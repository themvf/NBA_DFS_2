"""Rate + settle MLB moneyline and totals bets into the accountability ledger.

Consumes the model numbers already stored on ``mlb_matchups``
(``our_total_pred`` from the totals model, ``our_prob_home`` from the moneyline
model) and records one rated, lock-at-first-pitch bet per market into
``mlb_bets`` via ``model.mlb_bet_rating.record_bet`` — the MLB analog of
``model/soccer_game_bets.py``.

  * **Moneyline** — bet the side our win-prob favours vs the vig-free line; odds
    = home_ml/away_ml.  (The market is efficient, so most rate 1–2★ — that fade
    is the honest signal, exactly like soccer first-scorer.)
  * **Total (O/U)** — the side comes from the model's own skew-aware predictive
    distribution (``p_over`` vs ``p_under``, frozen in the prediction snapshot),
    priced at an exact paired book quote.

    NEVER pick the side with ``our_total_pred > line``.  The totals model
    regresses ``actual_total − vegas_total`` under squared-error loss, so it
    predicts the conditional **mean**; books set the line at the **median**.
    League-wide those differ by half a run (mean miss +0.51, median miss 0.00 —
    right skew from blowouts), so a point comparison says Over on 74–87% of all
    games forever, and Over only hits 46.5% vs a ~52.4% breakeven.  That single
    category error cost −41.9u of v4's −50.3u.  See CLAUDE.md, "MLB totals
    mean-vs-median".

Usage:
    python -m model.mlb_game_bets                       # rate today's slate
    python -m model.mlb_game_bets --date 2026-06-18
    python -m model.mlb_game_bets --backfill 2026-03-20 2026-06-17
    python -m model.mlb_game_bets --settle
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os

from config import load_config
from db.database import DatabaseManager
from ingest.mlb_odds_policy import MlbOddsPolicyError, validate_american_price
from model.mlb_bet_rating import american_to_prob, new_capture_key, record_bet
from model.mlb_prediction_provenance import (
    PROSPECTIVE,
    RETROSPECTIVE_BACKFILL,
    latest_prediction_snapshot_id,
)

logger = logging.getLogger(__name__)

MODEL_VERSION = "mlb-gameline-v5"

_DEFAULT_EXECUTABLE_BOOKS = (
    "draftkings",
    "fanduel",
    "betmgm",
    "williamhill_us",
    "fanatics",
    "betrivers",
)
# Hard star cap (2026-07-02) — same honesty rule as soccer game markets and
# tennis ML. The models' own holdout evals show neither beats the market:
#   moneyline (mlb_ml_v1_eval.json):  our logloss .6772 vs market .6717,
#     our Brier .2424 vs .2411; the 5pp-edge sim ran −17.7% ROI.
#   totals (mlb_total_v1_eval.json):  our MAE 3.36 vs Vegas 3.31 (worse);
#     O/U side accuracy .549 on 215 games is noise vs the .524 breakeven,
#     and the ledger's 4-5★ totals tiers claim .60-.70 but realize ~.54.
# (The ledger's apparent ML profits were an artifact of the arithmetic
# American-odds averaging bug fixed 2026-07-02 — fictional payouts.)
# 2★ = "neutral — no demonstrated edge": every bet is still rated and locked
# each slate for the ledger/CLV record, but the panel never advertises a play.
_GAMELINE_MAX_STARS = 2

# Market anchor — shrink our_prob toward the vig-free market line before rating.
# v1 had NO anchor and was systematically overconfident (realized win% ~5.5pp
# below claimed across every tier/month), which manufactured fake high-star bets
# and made the ledger a "bet every game" longshot-variance machine. Offline
# walk-forward eval: anchoring closes the calibration gap (−6.6pp → −2.0pp at
# w=0.5, −0.1pp at 0.35) and flips the low-variance favorite side from −8% to
# breakeven+. w = fraction of our deviation from market we KEEP (0 = trust market
# fully, 1 = no anchor = v1). 0.5 is a conservative 50/50 prior, not tuned to the
# exact data minimum; matches the market-anchoring philosophy used in soccer.
_MARKET_ANCHOR_W = 0.5


def _anchor(our_prob: float, market_prob: float, w: float = _MARKET_ANCHOR_W) -> float:
    """Shrink our probability toward the vig-free market line: keep `w` of our
    deviation from market. Corrects the model's systematic overconfidence."""
    return market_prob + w * (our_prob - market_prob)


def _configured_books() -> tuple[str, ...]:
    raw = os.getenv("MLB_EXECUTABLE_BOOKS", "")
    configured = tuple(key.strip().lower() for key in raw.split(",") if key.strip())
    return configured or _DEFAULT_EXECUTABLE_BOOKS


def _decode_books(value: object) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return decoded
    return {}


def _vig_free_probability(selected_price: int, paired_price: int) -> float:
    selected_raw = american_to_prob(selected_price)
    paired_raw = american_to_prob(paired_price)
    return selected_raw / (selected_raw + paired_raw)


def select_exact_moneyline_quote(
    books: dict,
    *,
    side: str,
    allowed_books: tuple[str, ...] | None = None,
) -> dict | None:
    """Choose the best exact paired moneyline quote from configured books."""
    selected_key = "ml_home" if side == "home" else "ml_away"
    paired_key = "ml_away" if side == "home" else "ml_home"
    candidates = []
    for book_key in allowed_books or _configured_books():
        quote = books.get(book_key)
        if not isinstance(quote, dict):
            continue
        try:
            selected = validate_american_price(quote.get(selected_key))
            paired = validate_american_price(quote.get(paired_key))
        except MlbOddsPolicyError:
            continue
        candidates.append({
            "book": book_key,
            "price": selected,
            "paired_price": paired,
            "market_prob": _vig_free_probability(selected, paired),
            "bookmaker_updated_at": quote.get("last_update"),
        })
    return max(candidates, key=lambda row: row["price"], default=None)


def select_exact_total_quote(
    books: dict,
    *,
    side: str,
    line: float,
    allowed_books: tuple[str, ...] | None = None,
) -> dict | None:
    """Choose the best paired total price at the exact frozen proposition."""
    selected_key = "over" if side == "over" else "under"
    paired_key = "under" if side == "over" else "over"
    candidates = []
    for book_key in allowed_books or _configured_books():
        quote = books.get(book_key)
        if not isinstance(quote, dict):
            continue
        quote_line = quote.get("total_line")
        if not isinstance(quote_line, (int, float)) or not math.isfinite(float(quote_line)):
            continue
        if not math.isclose(float(quote_line), line, abs_tol=1e-9):
            continue
        try:
            selected = validate_american_price(quote.get(selected_key))
            paired = validate_american_price(quote.get(paired_key))
        except MlbOddsPolicyError:
            continue
        candidates.append({
            "book": book_key,
            "price": selected,
            "paired_price": paired,
            "line": float(quote_line),
            "market_prob": _vig_free_probability(selected, paired),
            "bookmaker_updated_at": quote.get("last_update"),
        })
    return max(candidates, key=lambda row: row["price"], default=None)


def _latest_prediction_quote_context(
    conn,
    *,
    matchup_id: int,
    market: str,
    event_commence,
    origin: str,
) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            s.id AS prediction_snapshot_id,
            s.odds_snapshot_id,
            s.raw_prediction,
            s.market_prob,
            s.market_line,
            s.feature_values,
            s.event_commence,
            h.captured_at AS odds_captured_at,
            h.books
        FROM mlb_game_prediction_snapshots s
        JOIN mlb_prediction_runs r ON r.id = s.run_id
        JOIN game_odds_history h ON h.id = s.odds_snapshot_id
        WHERE s.matchup_id = %s AND s.market = %s AND r.origin = %s
          AND s.event_commence = %s
          AND h.sport = 'mlb' AND h.matchup_id = s.matchup_id
          AND h.captured_at < s.event_commence
          AND s.created_at < s.event_commence
        ORDER BY s.created_at DESC, s.id DESC
        LIMIT 1
        """,
        (matchup_id, market, origin, event_commence),
    )
    row = cur.fetchone()
    if row:
        row = dict(row)
        row["books"] = _decode_books(row.get("books"))
        row["feature_values"] = _decode_books(row.get("feature_values"))
    return row


def frozen_total_distribution(feature_values: dict, line: float) -> dict | None:
    """Return the snapshot's skew-aware O/U distribution for this exact line.

    Fails closed (``None``) rather than falling back to a symmetric parametric
    distribution: a Poisson/normal treats mean == median, which is precisely the
    assumption that produced the permanent Over tilt.  A snapshot without a
    usable distribution is a bet we decline, not a bet we guess.
    """
    dist = feature_values.get("total_distribution")
    if not isinstance(dist, dict):
        return None
    if not math.isclose(float(dist.get("line", float("nan"))), line, abs_tol=1e-9):
        return None  # distribution was built at a different proposition
    try:
        p_over = float(dist["p_over"])
        p_under = float(dist["p_under"])
        p_push = float(dist.get("p_push", 0.0))
    except (KeyError, TypeError, ValueError):
        return None
    decided = p_over + p_under
    if decided <= 0.0:
        return None
    return {"p_over": p_over, "p_under": p_under, "p_push": p_push, "decided": decided}


def _fixtures(db: DatabaseManager, where: str, params: tuple) -> list[dict]:
    return db.execute(
        f"""
        SELECT m.id, m.game_id, m.commence_time,
               m.home_team_id, m.away_team_id,
               ht.abbreviation AS home, at.abbreviation AS away,
               m.vegas_total, m.our_total_pred,
               m.home_ml, m.away_ml, m.vegas_prob_home, m.our_prob_home
        FROM mlb_matchups m
        JOIN mlb_teams ht ON ht.team_id = m.home_team_id
        JOIN mlb_teams at ON at.team_id = m.away_team_id
        WHERE {where}
        ORDER BY m.commence_time ASC NULLS LAST, m.id ASC
        """,
        params,
    )


def _record_fixture(db, conn, fx: dict, capture_key: str, origin: str) -> int:
    """Write exact-book recommendations from their linked prediction snapshot."""
    written = 0
    scope = str(fx["id"])
    commence = fx["commence_time"]
    fixture_label = f"{fx['away']} @ {fx['home']}"

    ml_context = _latest_prediction_quote_context(
        conn, matchup_id=fx["id"], market="moneyline",
        event_commence=commence, origin=origin,
    )
    if ml_context and ml_context["market_prob"] is not None:
        op = float(ml_context["raw_prediction"])
        mp = float(ml_context["market_prob"])
        bet_home = op >= mp
        side = "home" if bet_home else "away"
        side_raw = op if bet_home else 1.0 - op
        reference_side = mp if bet_home else 1.0 - mp
        exact = select_exact_moneyline_quote(ml_context["books"], side=side)
        if exact:
            bet_id = record_bet(
                db,
                model_version=MODEL_VERSION,
                bet_type="moneyline",
                scope=scope,
                selection_label=fx["home"] if bet_home else fx["away"],
                our_prob=_anchor(side_raw, reference_side),
                capture_key=capture_key,
                market_odds=exact["price"],
                market_prob=exact["market_prob"],
                book=exact["book"],
                odds_snapshot_id=ml_context["odds_snapshot_id"],
                matchup_id=fx["id"],
                subject_team_id=fx["home_team_id"] if bet_home else fx["away_team_id"],
                event_commence=commence,
                prediction_snapshot_id=ml_context["prediction_snapshot_id"],
                origin=origin,
                longshot_odds_cap=True,
                max_stars=_GAMELINE_MAX_STARS,
                conn=conn,
                inputs={
                    "side": side,
                    "fixture": fixture_label,
                    "our_prob_home": round(op, 4),
                    "reference_market_prob_home": round(mp, 4),
                    "observed_book": exact["book"],
                    "observed_price": exact["price"],
                    "paired_price": exact["paired_price"],
                    "odds_snapshot_id": ml_context["odds_snapshot_id"],
                    "odds_captured_at": str(ml_context["odds_captured_at"]),
                    "bookmaker_updated_at": exact["bookmaker_updated_at"],
                    "anchor_w": _MARKET_ANCHOR_W,
                    "stars_capped_at": _GAMELINE_MAX_STARS,
                    "edge_status": "no_walkforward_edge",
                },
            )
            written += int(bet_id is not None)
        else:
            logger.warning("No exact paired moneyline quote for %s (%s)", fixture_label, side)

    total_context = _latest_prediction_quote_context(
        conn, matchup_id=fx["id"], market="total",
        event_commence=commence, origin=origin,
    )
    if total_context and total_context["market_line"] is not None:
        line = float(total_context["market_line"])
        lam = float(total_context["raw_prediction"])
        dist = frozen_total_distribution(total_context["feature_values"], line)
    else:
        dist = None
    if total_context and dist is None:
        logger.warning(
            "No skew-aware total distribution for %s at %.1f — declining the total "
            "(refusing the mean-vs-median point comparison)",
            fixture_label, float(total_context["market_line"] or 0.0),
        )
    if total_context and dist is not None:
        # Side comes from the predictive DISTRIBUTION, never `lam > line`.
        is_over = dist["p_over"] > dist["p_under"]
        side = "over" if is_over else "under"
        # Conditional-on-no-push, so it is directly comparable to the two-sided
        # vig-free book quote (which also excludes the push).
        side_raw = (dist["p_over"] if is_over else dist["p_under"]) / dist["decided"]
        exact = select_exact_total_quote(total_context["books"], side=side, line=line)
        if exact:
            bet_id = record_bet(
                db,
                model_version=MODEL_VERSION,
                bet_type="total",
                scope=scope,
                selection_label=f"Over {line}" if is_over else f"Under {line}",
                our_prob=_anchor(side_raw, exact["market_prob"]),
                capture_key=capture_key,
                market_odds=exact["price"],
                market_prob=exact["market_prob"],
                book=exact["book"],
                odds_snapshot_id=total_context["odds_snapshot_id"],
                market_line=line,
                matchup_id=fx["id"],
                event_commence=commence,
                prediction_snapshot_id=total_context["prediction_snapshot_id"],
                origin=origin,
                longshot_odds_cap=True,
                max_stars=_GAMELINE_MAX_STARS,
                conn=conn,
                inputs={
                    "line": line,
                    "side": side,
                    "our_total_pred": round(lam, 2),
                    "p_over": round(dist["p_over"], 4),
                    "p_under": round(dist["p_under"], 4),
                    "p_push": round(dist["p_push"], 4),
                    "side_selection": "empirical_distribution_v1",
                    "fixture": fixture_label,
                    "observed_book": exact["book"],
                    "observed_price": exact["price"],
                    "paired_price": exact["paired_price"],
                    "odds_snapshot_id": total_context["odds_snapshot_id"],
                    "odds_captured_at": str(total_context["odds_captured_at"]),
                    "bookmaker_updated_at": exact["bookmaker_updated_at"],
                    "anchor_w": _MARKET_ANCHOR_W,
                    "stars_capped_at": _GAMELINE_MAX_STARS,
                    "edge_status": "no_walkforward_edge",
                },
            )
            written += int(bet_id is not None)
        else:
            logger.warning("No exact paired total quote for %s (%s %.1f)", fixture_label, side, line)

    return written


def rate_slate(db: DatabaseManager, game_date: str | None = None) -> int:
    # Pre-game only: a recommendation created after first pitch is not a
    # recommendation (books void tickets placed post-start, and the 22:10 UTC
    # cron's ~65-min runtime meant rate_slate executed AFTER evening first
    # pitches, minting post-hoc "bets" nightly — 2026-07-08 incident; the
    # worst were rated on in-play odds that had leaked into mlb_matchups,
    # e.g. a +3300 in-play PHI line frozen as a 34.0-decimal "closing" price).
    where = "m.game_date = %s" if game_date else "m.game_date >= CURRENT_DATE"
    # Fail closed: unknown start times cannot prove a record is pregame.
    where += " AND m.commence_time IS NOT NULL AND m.commence_time > NOW()"
    params: tuple = (game_date,) if game_date else ()
    fixtures = _fixtures(db, where, params)
    if not fixtures:
        print("MLB bets: no fixtures to rate")
        return 0
    capture_key = new_capture_key()
    written = 0
    with db.connect() as conn:
        for fx in fixtures:
            written += _record_fixture(db, conn, fx, capture_key, PROSPECTIVE)
    print(f"MLB bets: {written} moneyline/total bets rated across {len(fixtures)} fixtures")
    return written


def backfill(db: DatabaseManager, start_date: str, end_date: str) -> int:
    """Build the historical ledger from the walk-forward predictions already
    stored on completed games (then call settle to grade them)."""
    fixtures = _fixtures(
        db,
        "m.game_date >= %s AND m.game_date <= %s "
        "AND (m.our_total_pred IS NOT NULL OR m.our_prob_home IS NOT NULL)",
        (start_date, end_date),
    )
    capture_key = new_capture_key()
    written = 0
    with db.connect() as conn:
        for fx in fixtures:
            written += _record_fixture(db, conn, fx, capture_key, RETROSPECTIVE_BACKFILL)
    print(f"MLB bets: backfilled {written} bets across {len(fixtures)} fixtures "
          f"({start_date} to {end_date})")
    return written


def settle(db: DatabaseManager) -> int:
    """Settle pending moneyline/total bets for games that now have a final."""
    # Void pass first: postponed/cancelled games never played as scheduled —
    # books void those tickets, and the makeup game (same gamePk, later date)
    # must not grade the original bets. game_status is stamped by
    # ingest.mlb_schedule.fetch_scores.
    voided = 0
    for g in db.execute(
        """
        SELECT m.id, m.game_status
        FROM mlb_matchups m
        WHERE m.game_status IN ('Postponed', 'Cancelled')
          AND EXISTS (SELECT 1 FROM mlb_bets b WHERE b.matchup_id = m.id AND b.status = 'pending')
        """,
    ):
        rows = db.execute(
            "UPDATE mlb_bets SET status = 'void', settled_at = NOW(), result_detail = %s "
            "WHERE matchup_id = %s AND status = 'pending' RETURNING id",
            (f"{g['game_status']} — game not played as scheduled; bets void", g["id"]),
        )
        voided += len(rows)
    if voided:
        print(f"MLB bets: {voided} bets voided (postponed/cancelled games)")

    games = db.execute(
        """
        SELECT m.id, m.home_score, m.away_score
        FROM mlb_matchups m
        WHERE m.home_score IS NOT NULL AND m.away_score IS NOT NULL
          AND EXISTS (SELECT 1 FROM mlb_bets b WHERE b.matchup_id = m.id AND b.status = 'pending')
        """,
    )
    settled = 0
    for g in games:
        hs, as_ = int(g["home_score"]), int(g["away_score"])
        total = hs + as_
        ml_winner = "home" if hs > as_ else "away"  # no draws in MLB
        bets = db.execute(
            "SELECT id, bet_type, selection_label, inputs_json FROM mlb_bets "
            "WHERE matchup_id = %s AND status = 'pending'",
            (g["id"],),
        )
        for b in bets:
            detail = f"Final {hs}-{as_}"
            if b["bet_type"] == "moneyline":
                side = (b["inputs_json"] or {}).get("side")
                status = "won" if side == ml_winner else "lost"
            else:  # total
                line = (b["inputs_json"] or {}).get("line")
                if line is None:
                    continue
                is_over = b["selection_label"].lower().startswith("over")
                if total == line:
                    status = "void"
                elif (total > line) == is_over:
                    status = "won"
                else:
                    status = "lost"
            db.execute(
                "UPDATE mlb_bets SET status = %s, settled_at = NOW(), result_detail = %s WHERE id = %s",
                (status, detail, b["id"]),
            )
            settled += 1
    if settled:
        print(f"MLB bets: {settled} bets settled")
    return settled


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Rate + settle MLB moneyline/total bets")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: upcoming)")
    parser.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                        help="Build the historical ledger from stored predictions")
    parser.add_argument("--settle", action="store_true", help="Settle finals and exit")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.settle:
        settle(db)
    elif args.backfill:
        backfill(db, args.backfill[0], args.backfill[1])
        settle(db)
    else:
        rate_slate(db, args.date)
        settle(db)
