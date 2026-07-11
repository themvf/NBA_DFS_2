"""Rate + settle MLB moneyline and totals bets into the accountability ledger.

Consumes the model numbers already stored on ``mlb_matchups``
(``our_total_pred`` from the totals model, ``our_prob_home`` from the moneyline
model) and records one rated, lock-at-first-pitch bet per market into
``mlb_bets`` via ``model.mlb_bet_rating.record_bet`` — the MLB analog of
``model/soccer_game_bets.py``.

  * **Moneyline** — bet the side our win-prob favours vs the vig-free line; odds
    = home_ml/away_ml.  (The market is efficient, so most rate 1–2★ — that fade
    is the honest signal, exactly like soccer first-scorer.)
  * **Total (O/U)** — bet Over/Under per our_total_pred vs the line; our_prob via
    Poisson(our_total_pred); priced at the standard −110.  Calibrated: ~1-run
    edges land 3★, big edges 5★.

Usage:
    python -m model.mlb_game_bets                       # rate today's slate
    python -m model.mlb_game_bets --date 2026-06-18
    python -m model.mlb_game_bets --backfill 2026-03-20 2026-06-17
    python -m model.mlb_game_bets --settle
"""

from __future__ import annotations

import argparse
import logging

from config import load_config
from db.database import DatabaseManager
from model.mlb_bet_rating import new_capture_key, record_bet
from model.soccer_game_bets import _over_under_probs  # Poisson P(over)/P(under)

logger = logging.getLogger(__name__)

MODEL_VERSION = "mlb-gameline-v3"
_STD_TOTAL_ODDS = -110          # MLB O/U is −110/−110; vig-free ref = 0.5
_STD_TOTAL_REF = 0.5

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


def _record_fixture(db, conn, fx: dict, capture_key: str) -> int:
    written = 0
    scope = str(fx["id"])  # stable per-game key; matchup_id also stored as FK
    commence = fx["commence_time"]
    fixture_label = f"{fx['away']} @ {fx['home']}"

    # ── Moneyline ──
    op = fx["our_prob_home"]
    mp = fx["vegas_prob_home"]
    if op is not None and mp is not None and fx["home_ml"] is not None and fx["away_ml"] is not None:
        op, mp = float(op), float(mp)
        # Pick the side on the RAW model edge, then anchor the bet probability
        # toward the (vig-free) market for that side before rating.
        bet_home = op >= mp
        side_raw = op if bet_home else 1.0 - op
        side_mkt = mp if bet_home else 1.0 - mp
        record_bet(
            db,
            model_version=MODEL_VERSION,
            bet_type="moneyline",
            scope=scope,
            selection_label=fx["home"] if bet_home else fx["away"],
            our_prob=_anchor(side_raw, side_mkt),
            capture_key=capture_key,
            market_odds=int(fx["home_ml"] if bet_home else fx["away_ml"]),
            market_prob=side_mkt,
            matchup_id=fx["id"],
            subject_team_id=fx["home_team_id"] if bet_home else fx["away_team_id"],
            event_commence=commence,
            longshot_odds_cap=True,
            max_stars=_GAMELINE_MAX_STARS,
            conn=conn,
            inputs={"side": "home" if bet_home else "away", "fixture": fixture_label,
                    "our_prob_home": round(op, 4), "market_prob_home": round(mp, 4),
                    "anchor_w": _MARKET_ANCHOR_W,
                    "stars_capped_at": _GAMELINE_MAX_STARS,
                    "edge_status": "no_walkforward_edge"},
        )
        written += 1

    # ── Total (O/U) ──
    line = fx["vegas_total"]
    lam = fx["our_total_pred"]
    if line is not None and lam is not None:
        line, lam = float(line), float(lam)
        p_over, p_under = _over_under_probs(line, lam)
        is_over = lam > line
        side_raw = p_over if is_over else p_under
        record_bet(
            db,
            model_version=MODEL_VERSION,
            bet_type="total",
            scope=scope,
            selection_label=f"Over {line}" if is_over else f"Under {line}",
            # Anchor toward the −110 vig-free coin-flip — tempers the totals
            # model's strong overconfidence (v1 5★ claimed 70%, hit 54%).
            our_prob=_anchor(side_raw, _STD_TOTAL_REF),
            capture_key=capture_key,
            market_odds=_STD_TOTAL_ODDS,
            market_prob=_STD_TOTAL_REF,
            matchup_id=fx["id"],
            event_commence=commence,
            longshot_odds_cap=True,
            max_stars=_GAMELINE_MAX_STARS,
            conn=conn,
            inputs={"line": line, "side": "over" if is_over else "under",
                    "our_total_pred": round(lam, 2), "fixture": fixture_label,
                    "anchor_w": _MARKET_ANCHOR_W,
                    "stars_capped_at": _GAMELINE_MAX_STARS,
                    "edge_status": "no_walkforward_edge"},
        )
        written += 1

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
            written += _record_fixture(db, conn, fx, capture_key)
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
            written += _record_fixture(db, conn, fx, capture_key)
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
