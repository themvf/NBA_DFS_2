"""Guarded live smoke test for the NFL odds pipeline.

This command writes schedule and odds captures to the configured database and
spends Odds API quota. It therefore requires an explicit --confirm-write flag.

Usage:
    python -m ingest.nfl_smoke_test --confirm-write
    python -m ingest.nfl_smoke_test --confirm-write --date 2026-09-13
"""

from __future__ import annotations

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_schedule import collect_nfl_data_health, fetch_odds, fetch_scores
from model.line_alerts import scan, settle


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a live NFL ingestion smoke test")
    parser.add_argument("--date", default=None, help="Eastern game date YYYY-MM-DD")
    parser.add_argument(
        "--confirm-write",
        action="store_true",
        help="Acknowledge database writes and Odds API quota usage",
    )
    args = parser.parse_args()
    if not args.confirm_write:
        parser.error("--confirm-write is required because this smoke test mutates the configured database")

    config = load_config()
    if not config.database_url:
        parser.error("DATABASE_URL is not configured")
    if not config.odds_api.api_key:
        parser.error("ODDS_API_KEY is not configured")

    target = args.date or datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    db = DatabaseManager(config.database_url)
    captured = fetch_odds(db, config.odds_api.api_key, target)
    scores = fetch_scores(db, config.odds_api.api_key, 3)
    alerts = scan(db, "nfl")
    grades = settle(db, "nfl")
    health = collect_nfl_data_health(db, target)
    print({
        "date": target,
        "captures": captured,
        "scores": scores,
        "new_alerts": alerts,
        "grades": grades,
        "health": health,
    })
    return 0 if health["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
