"""Populate soccer_matchups.bracket_slot from the published knockout bracket.

The Odds API gives us the 16 Round-of-32 ties but NOT the bracket linkage (which
winners meet in R16, on up the tree). That structure is fixed once the R32 is set,
so we encode it here as an ordered list (top→bottom of the bracket). Consecutive
slots meet each round: slots (1,2)→an R16 match, (3,4)→the next, then (1-match,
2-match)→a QF, etc. — standard single-elimination adjacency.

Source: the official 2026 bracket as shown on the FOX bracket view (R32→R16
pairings confirmed by the "RD32 W#" feeder labels; QF+ by standard adjacency).
Re-run safely — it matches ties by the unordered pair of team names and upserts
the slot. If FIFA re-seeds (it won't post-draw), edit BRACKET_ORDER and re-run.

Usage:
    python -m ingest.soccer_bracket            # populate from BRACKET_ORDER
    python -m ingest.soccer_bracket --show     # print current slots, no writes
"""

from __future__ import annotations

import argparse
import logging
import unicodedata

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# Bracket order, top→bottom. Each entry is the (team_a, team_b) of one R32 tie in
# its bracket position (slot = index + 1). Consecutive slots pair up each round.
BRACKET_ORDER: list[tuple[str, str]] = [
    ("Germany", "Paraguay"),            # 1  ┐ R16: 1v2
    ("France", "Sweden"),               # 2  ┘
    ("South Africa", "Canada"),         # 3  ┐ R16: 3v4
    ("Netherlands", "Morocco"),         # 4  ┘   (QF: (1v2) v (3v4))
    ("Portugal", "Croatia"),            # 5  ┐ R16: 5v6
    ("Spain", "Austria"),               # 6  ┘
    ("USA", "Bosnia & Herzegovina"),    # 7  ┐ R16: 7v8
    ("Belgium", "Senegal"),             # 8  ┘   (QF: (5v6) v (7v8))  → SF top half
    ("Brazil", "Japan"),                # 9  ┐ R16: 9v10
    ("Ivory Coast", "Norway"),          # 10 ┘
    ("Mexico", "Ecuador"),              # 11 ┐ R16: 11v12
    ("England", "DR Congo"),            # 12 ┘   (QF: (9v10) v (11v12))
    ("Argentina", "Cape Verde"),        # 13 ┐ R16: 13v14
    ("Australia", "Egypt"),             # 14 ┘
    ("Switzerland", "Algeria"),         # 15 ┐ R16: 15v16
    ("Colombia", "Ghana"),              # 16 ┘   (QF: (13v14) v (15v16)) → SF bottom half
]


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def populate(db: DatabaseManager, show_only: bool = False) -> int:
    # Map normalized team name → team_id once.
    teams = db.execute("SELECT team_id, name FROM soccer_teams")
    name_to_id = {_norm(t["name"]): t["team_id"] for t in teams}

    if show_only:
        rows = db.execute(
            """
            SELECT sm.bracket_slot AS slot, h.name AS home, a.name AS away
            FROM soccer_matchups sm
            JOIN soccer_teams h ON h.team_id = sm.home_team_id
            JOIN soccer_teams a ON a.team_id = sm.away_team_id
            WHERE sm.bracket_slot IS NOT NULL
            ORDER BY sm.bracket_slot
            """,
        )
        for r in rows:
            print(f"  slot {r['slot']:2d}: {r['home']} vs {r['away']}")
        print(f"{len(rows)} ties slotted")
        return len(rows)

    updated = 0
    missing: list[tuple[str, str]] = []
    for idx, (team_a, team_b) in enumerate(BRACKET_ORDER):
        slot = idx + 1
        id_a = name_to_id.get(_norm(team_a))
        id_b = name_to_id.get(_norm(team_b))
        if not id_a or not id_b:
            missing.append((team_a, team_b))
            continue
        # Match the tie in EITHER orientation (home/away), unplayed knockout only.
        res = db.execute(
            """
            UPDATE soccer_matchups
            SET bracket_slot = %s
            WHERE home_score IS NULL
              AND ((home_team_id = %s AND away_team_id = %s)
                OR (home_team_id = %s AND away_team_id = %s))
            RETURNING id
            """,
            (slot, id_a, id_b, id_b, id_a),
        )
        if res:
            updated += len(res)
        else:
            missing.append((team_a, team_b))

    print(f"Bracket: {updated}/16 ties slotted")
    if missing:
        print("  NOT matched (check team names / fixtures loaded):")
        for a, b in missing:
            print(f"    {a} vs {b}")
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Populate knockout bracket slots")
    parser.add_argument("--show", action="store_true", help="Print current slots, no writes")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    populate(db, show_only=args.show)
