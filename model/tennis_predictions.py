"""Write our match-win predictions into tennis_matches (both tours).

P3 walk-forward (``model/tennis_model.py``, 66k matches) proved that a fitted,
market-anchored Elo model CANNOT beat the closing moneyline — on either tour, on
any surface, including grass (the Wimbledon target). Conditional on the vig-free
market prob, the Elo coefficient is even slightly negative: the market already
fully prices Elo, so a blend only adds noise (see memory tennis-moneyline-no-edge).

So our official prediction IS the vig-free market consensus: honest calibration,
zero claimed edge. This deliberately replaces the earlier 50/50 Elo↔market blend,
which pulled ``our_prob`` away from a sharp line and manufactured −EV "edges" in
the bet layer.

The Elo ratings (``ingest/tennis_history``) remain for display/context — they are
just not a betting signal. If a future feature is ever shown to beat the line
out-of-sample in ``tennis_model.py``, re-introduce a blend here with the *fitted*
weight, not a hand-picked one.
"""

from __future__ import annotations

import argparse
import logging

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)


def predict_and_write(db: DatabaseManager, match_date: str | None = None) -> int:
    """Set our_prob_home/away = vig-free market consensus. Returns rows updated."""
    where = "WHERE match_date = %s" if match_date else "WHERE match_date >= CURRENT_DATE"
    params = (match_date,) if match_date else ()
    matches = db.execute(
        f"""SELECT id, home_win_prob, away_win_prob
            FROM tennis_matches {where}""",
        params,
    )

    updated = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for m in matches:
            mh, ma = m["home_win_prob"], m["away_win_prob"]
            if mh is None or ma is None:
                continue  # no market line → nothing to anchor to
            cur.execute(
                "UPDATE tennis_matches SET our_prob_home = %s, our_prob_away = %s WHERE id = %s",
                (round(mh, 4), round(ma, 4), m["id"]),
            )
            updated += 1

    print(f"Tennis predictions: {updated} matches set to vig-free market prob "
          f"(no edge over the line -- see tennis_model.py P3)")
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Write tennis match predictions")
    parser.add_argument("--date", help="Match date YYYY-MM-DD (default: all upcoming)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    predict_and_write(db, args.date)
