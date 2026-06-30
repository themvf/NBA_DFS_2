"""Rate moneyline bets and write the tennis_bets ledger (ATP, V1).

Reuses the soccer rating engine (rate_market, american_to_decimal) — the star
rubric is sport-agnostic.  Only matches with a real Elo signal (both players
rated) are recorded, so the ledger isn't padded with zero-edge market-fallback
rows.  Each side (home/away) is rated separately; the UI's best-per-game picks
the higher-rated side.

Moneyline is an efficient single-game market, so longshot_odds_cap is ON: a tiny
model edge on a big price can't manufacture a fake 5★ (same guard as soccer ML).

Rows LOCK at event_commence (kickoff) so the backtest uses the closing
recommendation we committed to.  model_version = 'tennis-ml-v1'.
"""

from __future__ import annotations

import argparse
import json
import logging
import unicodedata
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_decimal, rate_market

logger = logging.getLogger(__name__)

MODEL_VERSION = "tennis-ml-v1"


def _normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _record(cur, *, match_id, side, label, our_prob, market_odds, market_prob,
            event_commence, inputs) -> bool:
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
        """,
        (MODEL_VERSION, match_id, side, label, market_odds, decimal_odds,
         market_prob, our_prob, edge, ev, stars, json.dumps(inputs), event_commence, locked),
    )
    return cur.rowcount > 0


def rate_and_write(db: DatabaseManager, match_date: str | None = None) -> int:
    """Rate moneyline for ATP matches with an Elo signal. Returns bets written."""
    rated = {r["norm_name"] for r in db.execute(
        "SELECT norm_name FROM tennis_player_ratings WHERE tour = 'ATP'")}

    where = "WHERE match_date = %s" if match_date else "WHERE match_date >= CURRENT_DATE"
    params = (match_date,) if match_date else ()
    matches = db.execute(
        f"""SELECT id, tour, commence_time, home_player, away_player,
                   home_ml, away_ml, home_win_prob, away_win_prob,
                   our_prob_home, our_prob_away
            FROM tennis_matches {where}""",
        params,
    )

    written = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for m in matches:
            # Only ATP matches where BOTH players are rated (real signal).
            if m["tour"] != "ATP":
                continue
            if _normalize_name(m["home_player"]) not in rated:
                continue
            if _normalize_name(m["away_player"]) not in rated:
                continue
            if m["our_prob_home"] is None or m["home_ml"] is None or m["away_ml"] is None:
                continue
            if m["home_win_prob"] is None or m["away_win_prob"] is None:
                continue

            inputs = {"home_player": m["home_player"], "away_player": m["away_player"],
                      "model_version": MODEL_VERSION}
            if _record(cur, match_id=m["id"], side="home", label=m["home_player"],
                       our_prob=m["our_prob_home"], market_odds=m["home_ml"],
                       market_prob=m["home_win_prob"], event_commence=m["commence_time"],
                       inputs=inputs):
                written += 1
            if _record(cur, match_id=m["id"], side="away", label=m["away_player"],
                       our_prob=m["our_prob_away"], market_odds=m["away_ml"],
                       market_prob=m["away_win_prob"], event_commence=m["commence_time"],
                       inputs=inputs):
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
