"""Write our match-win predictions into tennis_matches (ATP only, V1).

Elo-based, market-anchored — value comes from DISAGREEMENT with a sharp market,
not from fighting it (same philosophy as the soccer model's LineStar/market
anchor).  For each ATP match with both players rated:

    rating = grass_elo                      if grass_matches >= 20  (grass specialist sample)
           = 0.5*overall + 0.5*grass        if 0 < grass_matches < 20
           = overall_elo                    if no grass history
    model_prob_home = 1/(1+10^((rating_away-rating_home)/400))
    our_prob_home   = w*model_prob + (1-w)*market_fair_home      (w = MARKET_ANCHOR)

Players with no rating (young qualifiers) and ALL WTA matches fall back to the
vig-free market prob → zero edge, so the bet model produces no false signal for
them.  Totals/handicap predictions are intentionally NOT produced in V1.
"""

from __future__ import annotations

import argparse
import logging
import unicodedata

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

MARKET_ANCHOR = 0.5     # weight on the market fair prob (1-MARKET_ANCHOR on Elo)
_GRASS_FULL = 20        # grass matches at/above which we trust grass_elo outright


def _normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _blended_elo(overall: float, grass: float, grass_matches: int) -> float:
    if grass_matches >= _GRASS_FULL:
        return grass
    if grass_matches > 0:
        return 0.5 * overall + 0.5 * grass
    return overall


def _expected(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def predict_and_write(db: DatabaseManager, match_date: str | None = None) -> int:
    """Compute our_prob_home/away for upcoming matches. Returns rows updated."""
    ratings = {
        r["norm_name"]: r
        for r in db.execute(
            "SELECT norm_name, overall_elo, grass_elo, grass_matches "
            "FROM tennis_player_ratings WHERE tour = 'ATP'"
        )
    }

    where = "WHERE match_date = %s" if match_date else "WHERE match_date >= CURRENT_DATE"
    params = (match_date,) if match_date else ()
    matches = db.execute(
        f"""SELECT id, tour, home_player, away_player, home_win_prob, away_win_prob
            FROM tennis_matches {where}""",
        params,
    )

    updated = 0
    modeled = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for m in matches:
            mh, ma = m["home_win_prob"], m["away_win_prob"]
            if mh is None or ma is None:
                continue  # no market line → nothing to anchor to

            rh = ratings.get(_normalize_name(m["home_player"])) if m["tour"] == "ATP" else None
            ra = ratings.get(_normalize_name(m["away_player"])) if m["tour"] == "ATP" else None

            if rh and ra:
                eh = _blended_elo(rh["overall_elo"], rh["grass_elo"], rh["grass_matches"])
                ea = _blended_elo(ra["overall_elo"], ra["grass_elo"], ra["grass_matches"])
                model_home = _expected(eh, ea)
                our_home = MARKET_ANCHOR * mh + (1 - MARKET_ANCHOR) * model_home
                modeled += 1
            else:
                our_home = mh  # fallback: market fair → zero edge

            our_home = round(min(max(our_home, 1e-4), 0.9999), 4)
            cur.execute(
                "UPDATE tennis_matches SET our_prob_home = %s, our_prob_away = %s WHERE id = %s",
                (our_home, round(1 - our_home, 4), m["id"]),
            )
            updated += 1

    print(f"Tennis predictions: {updated} matches updated ({modeled} with Elo signal)")
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Write tennis match predictions")
    parser.add_argument("--date", help="Match date YYYY-MM-DD (default: all upcoming)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    predict_and_write(db, args.date)
