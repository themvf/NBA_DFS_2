"""Backfill `ff_team_byes` for seasons the current refresh will not touch.

`ingest.ff_independent` writes this table for the season it is building, so
from now on the live season maintains itself. This script fills the earlier
seasons the week-by-week grid can display, without rebuilding any ranking set.

It calls the same `compute_bye_weeks` and `save_team_byes` the refresh calls,
against the same nflverse schedule, so it exercises the real code path rather
than a parallel copy of it. Like the refresh, it refuses to write a partial
slate: a season that does not resolve to exactly 32 teams is skipped loudly.

    python -m ingest.ff_backfill_team_byes --seasons 2023 2024 2025 2026
"""

from __future__ import annotations

import argparse

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_independent import (
    NFLVERSE_SCHEDULE_URL,
    _fetch_csv,
    compute_bye_weeks,
    save_team_byes,
)

EXPECTED_TEAMS = 32


def backfill(seasons: list[int]) -> dict[int, int]:
    config = load_config()
    DatabaseManager(config.database_url)
    db = RefreshDatabase(config.database_url)
    written: dict[int, int] = {}
    try:
        schedule, _ = _fetch_csv(NFLVERSE_SCHEDULE_URL)
        for season in seasons:
            bye_weeks = compute_bye_weeks(schedule, season)
            if len(bye_weeks) != EXPECTED_TEAMS:
                print(f"  {season}: SKIPPED -- {len(bye_weeks)} teams resolved, expected {EXPECTED_TEAMS}")
                continue
            written[season] = save_team_byes(db, season, bye_weeks)
            print(f"  {season}: {written[season]} team byes stored")
        db.close()
    except Exception:
        db.close(error=True)
        raise
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seasons", type=int, nargs="+", required=True)
    backfill(parser.parse_args().seasons)
