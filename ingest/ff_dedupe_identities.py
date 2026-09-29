"""Deactivate FantasyPros-only player rows that have a canonical gsis twin.

A FantasyPros-only `ff_players` row (no gsis_id, no sleeper id) that shares
season, normalized name, position AND team with a row that DOES carry a
gsis_id is a duplicate identity: the nflverse-backed row is canonical, DK
slates resolve to it by gsis, and the FantasyPros-only row only exists to be
matched by name. Two active rows for one player let a name-matched projection
land on the wrong id (the review found exactly one such pair in run 75dabb4a:
Puka Nacua id 34 vs 560) and break any "no duplicate identity" assertion.

The team is part of the match. Name and position alone describe two
different people often enough in the NFL (two Mike Williamses, two Josh
Allens) that deactivating on them would retire a real player's only row.
Teams are compared through `model.nfl_team_aliases`, because FantasyPros and
nflverse spell four clubs differently (the Trevor Lawrence pair is JAC vs
JAX); a row with no team never matches.

Idempotent and repeatable: deactivates (never deletes) and prints each
change. Run before the projection build; `build_week` asserts afterwards.

Usage:
    python -m ingest.ff_dedupe_identities [--season 2026] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_weekly import target_season
from model.nfl_team_aliases import normalize_team

FIND = """
SELECT a.id, a.canonical_name, a.position, a.team_abbrev, a.fantasypros_player_id,
       b.id AS twin_id, b.gsis_id AS twin_gsis, b.team_abbrev AS twin_team
FROM ff_players a
JOIN ff_players b
  ON b.season = a.season AND b.normalized_name = a.normalized_name
 AND b.position = a.position AND b.gsis_id IS NOT NULL AND b.active AND b.id <> a.id
WHERE a.season = %s AND a.active AND a.gsis_id IS NULL AND a.sleeper_player_id IS NULL
ORDER BY a.canonical_name
"""


def same_team(left, right) -> bool:
    a, b = normalize_team(left), normalize_team(right)
    return a is not None and a == b


def dedupe(db: DatabaseManager, season: int, *, dry_run: bool = False) -> list[dict]:
    rows = [dict(r) for r in db.execute(FIND, (season,)) if same_team(r["team_abbrev"], r["twin_team"])]
    if rows and not dry_run:
        db.execute_many("UPDATE ff_players SET active = FALSE WHERE id = %s AND gsis_id IS NULL",
                        [(r["id"],) for r in rows])
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, help="Default: the current NFL season (January-March = last year).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    season = target_season(args.season, datetime.now(timezone.utc))
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    rows = dedupe(db, season, dry_run=args.dry_run)
    print(json.dumps({"season": season, "dry_run": args.dry_run, "deactivated": rows}, default=str, indent=2))


if __name__ == "__main__":
    main()
