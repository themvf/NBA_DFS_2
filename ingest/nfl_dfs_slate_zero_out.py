"""Zero the stored projection of every DK-flagged OUT slate row (repeatable).

Until 2026-09-22 the slate write copied the projection run's number for a
player DraftKings had already flagged OUT/IR/PUP; only the web read layer
zeroed him. The stored row is a display cache (`nfl_dfs_slate_players`),
so correcting it is legitimate; the immutable projection run row is untouched
and `identity_evidence.projection_row_id` still points at the original
number. Idempotent: a second run changes nothing.

Usage:
    python -m ingest.nfl_dfs_slate_zero_out [--dry-run]
"""

from __future__ import annotations

import argparse
import json

from config import load_config
from db.database import DatabaseManager

FIND = """
SELECT sp.id, sp.upload_id, sp.name, sp.position, sp.dk_status, sp.projection_status, sp.our_proj
FROM nfl_dfs_slate_players sp
WHERE sp.is_out AND (
    sp.projection_status IS DISTINCT FROM 'out'
    OR COALESCE(sp.our_proj, 0) <> 0 OR COALESCE(sp.floor_fpts, 0) <> 0
    OR COALESCE(sp.median_fpts, 0) <> 0 OR COALESCE(sp.ceiling_fpts, 0) <> 0
    OR COALESCE(sp.boom_rate, 0) <> 0)
ORDER BY sp.upload_id, sp.name
"""

ZERO = """
UPDATE nfl_dfs_slate_players
SET projection_status = 'out', our_proj = 0, floor_fpts = 0, median_fpts = 0,
    ceiling_fpts = 0, boom_rate = 0, updated_at = NOW()
WHERE id = %s
"""


def zero_out(db: DatabaseManager, *, dry_run: bool = False) -> list[dict]:
    rows = [dict(r) for r in db.execute(FIND)]
    if rows and not dry_run:
        db.execute_many(ZERO, [(r["id"],) for r in rows])
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    rows = zero_out(db, dry_run=args.dry_run)
    by_upload: dict[str, int] = {}
    for r in rows:
        by_upload[r["upload_id"]] = by_upload.get(r["upload_id"], 0) + 1
    print(json.dumps({"dry_run": args.dry_run, "zeroed": len(rows), "by_upload": by_upload}, indent=2))


if __name__ == "__main__":
    main()
