"""Populate soccer_matchups.bracket_round/bracket_slot through the whole tree.

The Odds API gives us the 16 Round-of-32 ties but NOT the bracket linkage (which
winners meet in R16, on up the tree). That structure is fixed once the R32 is set,
so R32 is hardcoded here as an ordered list (top→bottom). Consecutive slots meet
each round: slots (1,2)→an R16 match, (3,4)→the next, then (R16 match 1, R16
match 2)→a QF, etc. — standard single-elimination adjacency.

R16 onward CANNOT be hardcoded by team name (we don't know winners in advance),
so cascade() derives the next round's pairing dynamically from the CURRENT
round's resolved winners (winner_team_id, or a score comparison fallback) and
matches it to the real fixture already sitting in soccer_matchups (loaded by
the normal schedule ingest once the Odds API knows the pairing) by team-pair —
purely additive tagging, no fixture creation. Re-run safely, any time: it only
tags rows that both (a) have a resolved previous round to derive from and
(b) aren't already tagged for the target round.

Source: the official 2026 bracket as shown on the FOX bracket view (R32→R16
pairings confirmed by the "RD32 W#" feeder labels; QF+ by standard adjacency).

Usage:
    python -m ingest.soccer_bracket            # tag R32, then cascade R16→Final
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

# (round, size) in tree order; ROUND_AFTER[r] cascades into r+1.
ROUND_SIZES: list[tuple[str, int]] = [
    ("r32", 16), ("r16", 8), ("qf", 4), ("sf", 2), ("final", 1),
]


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _winner_team_id(tie: dict) -> int | None:
    """Resolved winner for a tie, or None if not yet decided."""
    if tie.get("winner_team_id"):
        return tie["winner_team_id"]
    if tie.get("home_score") is None or tie.get("away_score") is None:
        return None
    hs, as_ = tie["home_score"], tie["away_score"]
    if hs > as_:
        return tie["home_team_id"]
    if as_ > hs:
        return tie["away_team_id"]
    return None  # draw with no winner_team_id — unresolved (e.g. pens not yet recorded)


def assign_r32(db: DatabaseManager) -> int:
    """Tag the 16 hardcoded R32 ties by team name. Idempotent."""
    teams = db.execute("SELECT team_id, name FROM soccer_teams")
    name_to_id = {_norm(t["name"]): t["team_id"] for t in teams}

    updated = 0
    missing: list[tuple[str, str]] = []
    for idx, (team_a, team_b) in enumerate(BRACKET_ORDER):
        slot = idx + 1
        id_a, id_b = name_to_id.get(_norm(team_a)), name_to_id.get(_norm(team_b))
        if not id_a or not id_b:
            missing.append((team_a, team_b))
            continue
        res = db.execute(
            """
            UPDATE soccer_matchups
            SET bracket_slot = %s, bracket_round = 'r32'
            WHERE (bracket_round IS NULL OR bracket_round = 'r32')
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

    print(f"Bracket r32: {updated}/16 ties slotted")
    if missing:
        print("  NOT matched (check team names / fixtures loaded):")
        for a, b in missing:
            print(f"    {a} vs {b}")
    return updated


def cascade(db: DatabaseManager, from_round: str, from_size: int, to_round: str) -> int:
    """Derive to_round's slot assignment from from_round's RESOLVED winners.

    Adjacent slots i*2-1, i*2 in from_round feed to_round slot i. A pair only
    cascades once BOTH ties in it have a resolved winner — the real fixture
    for that slot must already exist in soccer_matchups (normal schedule
    ingest loads it once the Odds API/schedule knows the pairing); this only
    TAGS it, never creates a fixture. Skips slots already tagged for to_round.
    """
    ties = db.execute(
        "SELECT id, bracket_slot, home_team_id, away_team_id, home_score, away_score, "
        "winner_team_id FROM soccer_matchups WHERE bracket_round = %s ORDER BY bracket_slot",
        (from_round,),
    )
    by_slot = {t["bracket_slot"]: t for t in ties}

    already = {r["bracket_slot"] for r in db.execute(
        "SELECT bracket_slot FROM soccer_matchups WHERE bracket_round = %s", (to_round,))}

    tagged = 0
    pending = []
    for i in range(from_size // 2):
        to_slot = i + 1
        if to_slot in already:
            continue
        a, b = by_slot.get(2 * i + 1), by_slot.get(2 * i + 2)
        if not a or not b:
            continue
        wa, wb = _winner_team_id(a), _winner_team_id(b)
        if not wa or not wb:
            pending.append(to_slot)
            continue
        res = db.execute(
            """
            UPDATE soccer_matchups
            SET bracket_slot = %s, bracket_round = %s
            WHERE bracket_round IS NULL
              AND ((home_team_id = %s AND away_team_id = %s)
                OR (home_team_id = %s AND away_team_id = %s))
            RETURNING id
            """,
            (to_slot, to_round, wa, wb, wb, wa),
        )
        if res:
            tagged += 1
        else:
            pending.append(to_slot)  # winners known, real fixture not loaded yet

    msg = f"Bracket {to_round}: {tagged} newly slotted"
    if pending:
        msg += f" ({len(pending)} pending — winners undecided or fixture not yet loaded)"
    print(msg)
    return tagged


def populate(db: DatabaseManager, show_only: bool = False) -> int:
    if show_only:
        rows = db.execute(
            """
            SELECT sm.bracket_round AS round, sm.bracket_slot AS slot, h.name AS home, a.name AS away
            FROM soccer_matchups sm
            JOIN soccer_teams h ON h.team_id = sm.home_team_id
            JOIN soccer_teams a ON a.team_id = sm.away_team_id
            WHERE sm.bracket_slot IS NOT NULL
            ORDER BY CASE COALESCE(sm.bracket_round, 'r32')
                       WHEN 'r32' THEN 0 WHEN 'r16' THEN 1 WHEN 'qf' THEN 2
                       WHEN 'sf' THEN 3 WHEN 'final' THEN 4 END,
                     sm.bracket_slot
            """,
        )
        for r in rows:
            print(f"  {r['round'] or 'r32':<6} slot {r['slot']:2d}: {r['home']} vs {r['away']}")
        print(f"{len(rows)} ties slotted")
        return len(rows)

    total = assign_r32(db)
    for i in range(len(ROUND_SIZES) - 1):
        from_round, from_size = ROUND_SIZES[i]
        to_round, _ = ROUND_SIZES[i + 1]
        total += cascade(db, from_round, from_size, to_round)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Populate knockout bracket slots (all rounds)")
    parser.add_argument("--show", action="store_true", help="Print current slots, no writes")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    populate(db, show_only=args.show)
