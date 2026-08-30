"""One-off backfill for `ff_player_week_stats`.

The weekly table is populated by the normal refresh (`ingest.ff_independent`)
from now on. This script exists so an existing database can be filled without
waiting for the next scheduled run, and without rebuilding the ranking sets --
a board rebuild is a much larger side effect than this table needs.

It calls the same `save_weekly_history` the refresh calls, against a player
universe read back out of `ff_players`, so it exercises the real code path
rather than a parallel copy of it.

    python -m ingest.ff_backfill_week_stats --season 2025
"""

from __future__ import annotations

import argparse

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_independent import (
    NFLVERSE_WEEKLY_STATS_URL,
    WEEKLY_STAT_POSITIONS,
    _fetch_csv,
    save_weekly_history,
)


def backfill(season: int) -> int:
    config = load_config()
    # Same pattern as ff_independent.run(): DatabaseManager exists only to
    # apply the schema (RefreshDatabase does not), so a first run creates
    # ff_player_week_stats before anything tries to write to it.
    DatabaseManager(config.database_url)
    db = RefreshDatabase(config.database_url)
    try:
        universe = [
            {
                "player_id": row["id"],
                "gsis_id": row["gsis_id"],
                "name": row["canonical_name"],
                "position": row["position"],
            }
            for row in db.execute(
                """SELECT id, gsis_id, canonical_name, position
                   FROM ff_players WHERE position = ANY(%s)""",
                (list(WEEKLY_STAT_POSITIONS),),
            )
        ]
        if not universe:
            raise RuntimeError("ff_players holds no weekly-eligible players; run the refresh first")

        url = NFLVERSE_WEEKLY_STATS_URL.format(season=season)
        frame, _ = _fetch_csv(url)
        if len(frame) < 5000:
            raise RuntimeError(f"nflverse {season} weekly stats returned {len(frame)} rows; expected thousands")

        written = save_weekly_history(db, universe, season, frame)
        db.conn.commit()
        print(f"{written} player-weeks stored for {season} across {len(universe)} known players")
        return written
    finally:
        db.conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True, help="completed season to backfill, e.g. 2025")
    backfill(parser.parse_args().season)
