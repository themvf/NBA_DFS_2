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

MODEL_VERSION = "mlb-gameline-v1"
_STD_TOTAL_ODDS = -110          # MLB O/U is −110/−110; vig-free ref = 0.5
_STD_TOTAL_REF = 0.5


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
        bet_home = op >= mp
        record_bet(
            db,
            model_version=MODEL_VERSION,
            bet_type="moneyline",
            scope=scope,
            selection_label=fx["home"] if bet_home else fx["away"],
            our_prob=op if bet_home else 1.0 - op,
            capture_key=capture_key,
            market_odds=int(fx["home_ml"] if bet_home else fx["away_ml"]),
            market_prob=mp if bet_home else 1.0 - mp,
            matchup_id=fx["id"],
            subject_team_id=fx["home_team_id"] if bet_home else fx["away_team_id"],
            event_commence=commence,
            longshot_odds_cap=True,
            conn=conn,
            inputs={"side": "home" if bet_home else "away", "fixture": fixture_label,
                    "our_prob_home": round(op, 4), "market_prob_home": round(mp, 4)},
        )
        written += 1

    # ── Total (O/U) ──
    line = fx["vegas_total"]
    lam = fx["our_total_pred"]
    if line is not None and lam is not None:
        line, lam = float(line), float(lam)
        p_over, p_under = _over_under_probs(line, lam)
        is_over = lam > line
        record_bet(
            db,
            model_version=MODEL_VERSION,
            bet_type="total",
            scope=scope,
            selection_label=f"Over {line}" if is_over else f"Under {line}",
            our_prob=p_over if is_over else p_under,
            capture_key=capture_key,
            market_odds=_STD_TOTAL_ODDS,
            market_prob=_STD_TOTAL_REF,
            matchup_id=fx["id"],
            event_commence=commence,
            longshot_odds_cap=True,
            conn=conn,
            inputs={"line": line, "side": "over" if is_over else "under",
                    "our_total_pred": round(lam, 2), "fixture": fixture_label},
        )
        written += 1

    return written


def rate_slate(db: DatabaseManager, game_date: str | None = None) -> int:
    where = "m.game_date = %s" if game_date else "m.game_date >= CURRENT_DATE"
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
