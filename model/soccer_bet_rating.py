"""Soccer bet rating + ledger — the traceability/accountability spine.

Every bet recommendation (first scorer, outright, group winner) flows through
``record_bet`` here, which:
  1. converts market odds → decimal + vig-free implied probability,
  2. computes EV and edge vs our model probability,
  3. assigns a deterministic 1–5 star rating (documented constants below),
  4. upserts the bet into ``soccer_bets`` (one row per selection per model),
  5. appends an immutable row to ``soccer_bet_snapshots`` (audit trail),
  6. LOCKS the row once the event has started, so the backtest always uses the
     closing recommendation we actually committed to — never a post-hoc edit.

The star thresholds are constants so the rating is reproducible and tunable;
``model_version`` is stamped on every row so a model change never silently mixes
old and new recommendations in the backtest.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# ── Star rubric (deterministic) ───────────────────────────────────────────────
# Market-based (EV in ROI units, edge in probability points).
_MARKET_TIERS = [
    (5, 0.20, 0.04),
    (4, 0.10, 0.025),
    (3, 0.03, None),
    (2, -0.03, None),
]  # (stars, min_ev, min_edge_or_None); anything below tier-2 EV → 1 star.
# No-market (group winner): edge over the naive baseline + confidence floor.
_NOMARKET_TIERS = [
    (5, 0.45, 0.15),
    (4, 0.32, 0.08),
    (3, None, 0.03),
    (2, None, -0.03),
]  # (stars, min_prob_or_None, min_edge); below tier-2 edge → 1 star.
_LONGSHOT_PROB = 0.02   # below this our_prob, cap stars at 3 (tail noise)
_LONGSHOT_CAP = 3
# For efficient single-game markets (moneyline/total/first-scorer), a tiny model
# edge on a big longshot creates huge but illusory EV.  When longshot_odds_cap is
# on, cap stars by the offered price.  NOT applied to futures, where longshots are
# the legitimate value plays.
_ODDS_CAP_TIERS = [(21.0, 2), (11.0, 3)]   # (min decimal odds, max stars)


def american_to_decimal(ml: int) -> float:
    """American moneyline → decimal odds (gross payout per 1 unit, incl. stake)."""
    return 1.0 + (ml / 100.0 if ml > 0 else 100.0 / abs(ml))


def american_to_prob(ml: int) -> float:
    """American moneyline → implied probability (with vig)."""
    return 1.0 / american_to_decimal(ml)


def prob_to_american(p: float) -> int:
    """Implied probability → American odds (inverse of american_to_prob).

    Use this to average odds in probability space — averaging American odds
    arithmetically is invalid (you cannot mean +100 and -120).
    """
    p = min(max(p, 1e-6), 0.999999)
    dec = 1.0 / p
    if dec >= 2.0:
        return round((dec - 1.0) * 100)
    return -round(100.0 / (dec - 1.0))


def rate_market(
    our_prob: float,
    decimal_odds: float,
    ref_prob: float,
    longshot_odds_cap: bool = False,
) -> tuple[int, float, float]:
    """Return (stars, ev, edge) for a bet with a market line.

    ref_prob is the vig-free market probability; edge = our_prob − ref_prob.
    EV is expected ROI per unit staked.  When longshot_odds_cap is set, long
    prices have their star ceiling lowered (efficient single-game markets).
    """
    ev = our_prob * decimal_odds - 1.0
    edge = our_prob - ref_prob
    stars = 1
    for tier_stars, min_ev, min_edge in _MARKET_TIERS:
        if ev >= min_ev and (min_edge is None or edge >= min_edge):
            stars = tier_stars
            break
    if our_prob < _LONGSHOT_PROB:
        stars = min(stars, _LONGSHOT_CAP)
    if longshot_odds_cap:
        for min_dec, max_stars in _ODDS_CAP_TIERS:
            if decimal_odds >= min_dec:
                stars = min(stars, max_stars)
                break
    return stars, ev, edge


def rate_no_market(our_prob: float, baseline_prob: float) -> tuple[int, None, float]:
    """Return (stars, None, edge) for a bet with no market (e.g. group winner)."""
    edge = our_prob - baseline_prob
    stars = 1
    for tier_stars, min_prob, min_edge in _NOMARKET_TIERS:
        prob_ok = min_prob is None or our_prob >= min_prob
        edge_ok = min_edge is None or edge >= min_edge
        if prob_ok and edge_ok:
            stars = tier_stars
            break
    if our_prob < _LONGSHOT_PROB:
        stars = min(stars, _LONGSHOT_CAP)
    return stars, None, edge


def new_capture_key() -> str:
    """A timestamp key tying one ingestion/rating run together (traceability)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def record_bet(
    db: DatabaseManager,
    *,
    model_version: str,
    bet_type: str,
    scope: str,
    selection_label: str,
    our_prob: float,
    capture_key: str,
    market_odds: int | None = None,
    market_prob: float | None = None,   # vig-free reference prob when a market exists
    book: str | None = None,
    baseline_prob: float | None = None,  # used when there is no market
    matchup_id: int | None = None,
    subject_team_id: int | None = None,
    event_commence: datetime | None = None,
    inputs: dict | None = None,
    longshot_odds_cap: bool = False,
) -> int | None:
    """Rate and persist one bet recommendation.  Returns the bet id, or None if
    the row is locked (event already started) and was therefore left untouched.
    """
    if market_odds is not None and market_prob is not None:
        decimal_odds = american_to_decimal(market_odds)
        stars, ev, edge = rate_market(our_prob, decimal_odds, market_prob, longshot_odds_cap)
    else:
        decimal_odds = None
        ref = baseline_prob if baseline_prob is not None else 0.0
        stars, ev, edge = rate_no_market(our_prob, ref)

    now = datetime.now(timezone.utc)
    locked = event_commence is not None and now >= event_commence

    row = db.execute_one(
        """
        INSERT INTO soccer_bets (
            model_version, bet_type, scope, matchup_id, subject_team_id,
            selection_label, market_odds, market_decimal, market_prob, book,
            our_prob, edge, ev, stars, inputs_json, event_commence, locked, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (bet_type, scope, selection_label, model_version) DO UPDATE SET
            matchup_id      = EXCLUDED.matchup_id,
            subject_team_id = EXCLUDED.subject_team_id,
            market_odds     = EXCLUDED.market_odds,
            market_decimal  = EXCLUDED.market_decimal,
            market_prob     = EXCLUDED.market_prob,
            book            = EXCLUDED.book,
            our_prob        = EXCLUDED.our_prob,
            edge            = EXCLUDED.edge,
            ev              = EXCLUDED.ev,
            stars           = EXCLUDED.stars,
            inputs_json     = EXCLUDED.inputs_json,
            event_commence  = EXCLUDED.event_commence,
            locked          = EXCLUDED.locked,
            updated_at      = NOW()
        WHERE soccer_bets.locked = FALSE
          AND soccer_bets.status = 'pending'
        RETURNING id
        """,
        (
            model_version, bet_type, scope, matchup_id, subject_team_id,
            selection_label, market_odds, decimal_odds, market_prob, book,
            our_prob, edge, ev, stars, json.dumps(inputs or {}), event_commence, locked,
        ),
    )
    if not row:
        return None  # locked — left as the frozen closing recommendation

    bet_id = row["id"]
    db.execute(
        """
        INSERT INTO soccer_bet_snapshots
            (bet_id, capture_key, our_prob, market_prob, market_odds, edge, ev, stars)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (bet_id, capture_key, our_prob, market_prob, market_odds, edge, ev, stars),
    )
    return bet_id
