"""Rate moneyline bets and write the tennis_bets ledger (both tours).

Reuses the soccer rating engine (rate_market, american_to_decimal) — the star
rubric is sport-agnostic.  Each side (home/away) is rated separately; the UI's
best-per-game picks the higher-rated side.

P3 (model/tennis_model.py) proved tennis moneyline has NO exploitable edge, so
model/tennis_predictions now sets our_prob = the vig-free market consensus. With
our_prob == market, edge ≈ 0 and EV ≈ −vig, so every bet honestly rates ≤2★:
the ledger becomes a calibration record, not an edge feed (see memory
tennis-moneyline-no-edge). Both tours are rated — the old ATP-only guard existed
only because WTA lacked ratings; that no longer gates anything when our number is
the market.

Moneyline is an efficient single-game market, so longshot_odds_cap stays ON.

Rows LOCK at event_commence (kickoff) so the backtest uses the closing
recommendation we committed to.  model_version = 'tennis-ml-v2'. Re-rating first
clears UNLOCKED pending rows from older versions (no double-count / orphans);
locked + settled rows are preserved as the committed audit trail — including the
v1 Elo-blend recommendations, whose settled results are themselves evidence of
the no-edge finding.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_decimal, rate_market

logger = logging.getLogger(__name__)

MODEL_VERSION = "tennis-ml-v2"


def _record(cur, *, match_id, side, label, our_prob, market_odds, market_prob,
            event_commence, inputs, capture_key) -> bool:
    """Upsert one rated moneyline bet (returns True if written, False if locked)."""
    decimal_odds = american_to_decimal(market_odds)
    stars, ev, edge = rate_market(our_prob, decimal_odds, market_prob, longshot_odds_cap=True)
    now = datetime.now(timezone.utc)
    locked = event_commence is not None and now >= event_commence
    cur.execute(
        """
        INSERT INTO tennis_bets (
            model_version, bet_type, match_id, side, selection_label,
            market_odds, market_decimal, market_prob, our_prob, edge, ev, stars,
            inputs_json, event_commence, locked, updated_at
        ) VALUES (%s, 'moneyline', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (bet_type, match_id, side, model_version) DO UPDATE SET
            selection_label = EXCLUDED.selection_label,
            market_odds = EXCLUDED.market_odds,
            market_decimal = EXCLUDED.market_decimal,
            market_prob = EXCLUDED.market_prob,
            our_prob = EXCLUDED.our_prob,
            edge = EXCLUDED.edge,
            ev = EXCLUDED.ev,
            stars = EXCLUDED.stars,
            inputs_json = EXCLUDED.inputs_json,
            event_commence = EXCLUDED.event_commence,
            locked = EXCLUDED.locked,
            updated_at = NOW()
        WHERE tennis_bets.locked = FALSE AND tennis_bets.status = 'pending'
        RETURNING id
        """,
        (MODEL_VERSION, match_id, side, label, market_odds, decimal_odds,
         market_prob, our_prob, edge, ev, stars, json.dumps(inputs), event_commence, locked),
    )
    row = cur.fetchone()
    if not row:
        return False  # locked — frozen closing recommendation
    # Append-only snapshot trail (mirrors soccer/mlb_bet_snapshots) — gives
    # tennis entry→close CLV measurement in model/clv_report.py.
    cur.execute(
        """
        INSERT INTO tennis_bet_snapshots
            (bet_id, capture_key, our_prob, market_prob, market_odds, edge, ev, stars)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (row["id"], capture_key, our_prob, market_prob, market_odds, edge, ev, stars),
    )
    return True


def rate_and_write(db: DatabaseManager, match_date: str | None = None) -> int:
    """Rate moneyline for both tours (our_prob = market → honest ≤2★). Returns bets written."""
    where = (
        "WHERE match_date = %s AND (commence_time IS NULL OR commence_time > NOW())"
        if match_date else
        "WHERE match_date >= CURRENT_DATE AND (commence_time IS NULL OR commence_time > NOW())"
    )
    params = (match_date,) if match_date else ()
    matches = db.execute(
        f"""SELECT id, tour, commence_time, home_player, away_player,
                   home_ml, away_ml, home_win_prob, away_win_prob,
                   our_prob_home, our_prob_away
            FROM tennis_matches {where}""",
        params,
    )

    written = 0
    capture_key = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with db.connect() as conn:
        cur = conn.cursor()
        # Drop stale UNLOCKED pending rows from older model versions so a version
        # bump never double-counts a still-open match (locked/settled preserved).
        cur.execute(
            "DELETE FROM tennis_bets WHERE bet_type='moneyline' AND status='pending' "
            "AND locked = FALSE AND model_version <> %s",
            (MODEL_VERSION,),
        )
        for m in matches:
            if m["our_prob_home"] is None or m["home_ml"] is None or m["away_ml"] is None:
                continue
            if m["home_win_prob"] is None or m["away_win_prob"] is None:
                continue

            inputs = {"home_player": m["home_player"], "away_player": m["away_player"],
                      "model_version": MODEL_VERSION}
            if _record(cur, match_id=m["id"], side="home", label=m["home_player"],
                       our_prob=m["our_prob_home"], market_odds=m["home_ml"],
                       market_prob=m["home_win_prob"], event_commence=m["commence_time"],
                       inputs=inputs, capture_key=capture_key):
                written += 1
            if _record(cur, match_id=m["id"], side="away", label=m["away_player"],
                       our_prob=m["our_prob_away"], market_odds=m["away_ml"],
                       market_prob=m["away_win_prob"], event_commence=m["commence_time"],
                       inputs=inputs, capture_key=capture_key):
                written += 1

    print(f"Tennis bets: {written} moneyline selections rated ({MODEL_VERSION})")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Rate tennis moneyline bets")
    parser.add_argument("--date", help="Match date YYYY-MM-DD (default: all upcoming)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    rate_and_write(db, args.date)
