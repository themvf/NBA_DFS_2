"""Orchestrate the NFL Vegas schedule, odds, scores, alerts, and health pass."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from datetime import datetime
from typing import TypeVar
from zoneinfo import ZoneInfo

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_schedule import (
    collect_nfl_data_health,
    fetch_events,
    fetch_odds,
    fetch_scores,
    verify_fresh_upcoming_odds,
)
from ingest.nfl_teams import seed_teams

logger = logging.getLogger(__name__)
T = TypeVar("T")


def _refresh_pickem_probabilities(db: DatabaseManager, target_date: str) -> dict:
    """Use today's captured odds without buying another Odds API response."""
    from ingest.nfl_season_schedule import load_season, fetch_schedule
    from model.nfl_survivor_model import compute_and_store, fit_spread_prob, historical_games

    day = datetime.strptime(target_date, "%Y-%m-%d")
    season = day.year - 1 if day.month <= 3 else day.year
    upcoming = db.execute(
        """SELECT id FROM nfl_matchups
           WHERE game_date = %s AND season_type = 'regular'
             AND commence_time > NOW() AND NOT completed
             AND COALESCE(game_status, '') NOT IN ('Postponed', 'Cancelled')""",
        (target_date,),
    )
    if not upcoming:
        return {"skipped": "no upcoming games"}
    # The daily event fetch may have created IDs since the weekly schedule ran.
    schedule = fetch_schedule()
    load_season(db, season, schedule)
    linked = db.execute(
        """SELECT id, matchup_id FROM nfl_season_games
           WHERE season = %s AND matchup_id = ANY(%s)""",
        (season, [row["id"] for row in upcoming]),
    )
    if {row["matchup_id"] for row in linked} != {row["id"] for row in upcoming}:
        raise RuntimeError("Upcoming NFL games are missing season-schedule links")
    fit = fit_spread_prob(historical_games(schedule))
    return compute_and_store(db, season, fit, game_ids={row["id"] for row in linked})


def _stage(label: str, fn: Callable[[], T]) -> tuple[bool, T | None]:
    try:
        result = fn()
        print(f"{label}: completed ({result})")
        return True, result
    except Exception as exc:  # noqa: BLE001 - each stage must be reported
        logger.exception("%s failed: %s", label, exc)
        print(f"{label}: FAILED ({type(exc).__name__}: {exc})")
        return False, None


def run_refresh(db: DatabaseManager, api_key: str, target_date: str) -> int:
    from model.line_alerts import scan, settle

    stages: list[tuple[str, bool, object | None]] = []
    for label, action in (
        ("nfl_teams", lambda: seed_teams(db)),
        ("nfl_events", lambda: fetch_events(db, api_key, target_date)),
        ("nfl_odds", lambda: fetch_odds(db, api_key, target_date)),
        ("nfl_scores", lambda: fetch_scores(db, api_key, 3)),
        ("nfl_alert_scan", lambda: scan(db, "nfl")),
        ("nfl_alert_settlement", lambda: settle(db, "nfl")),
        ("nfl_freshness", lambda: verify_fresh_upcoming_odds(db, target_date)),
        ("nfl_data_health", lambda: collect_nfl_data_health(db, target_date)),
    ):
        ok, result = _stage(label, action)
        if label == "nfl_freshness" and ok and result is False:
            ok = False
        if label == "nfl_data_health" and ok and isinstance(result, dict) and result.get("status") != "pass":
            ok = False
        stages.append((label, ok, result))
    # A failed capture must not make old odds look newly computed. Alert or
    # settlement failures, however, do not block independent probabilities.
    required = {"nfl_events", "nfl_odds", "nfl_freshness"}
    if all(ok for label, ok, _ in stages if label in required):
        ok, result = _stage("nfl_pickem_probabilities", lambda: _refresh_pickem_probabilities(db, target_date))
        stages.append(("nfl_pickem_probabilities", ok, result))
    else:
        print("nfl_pickem_probabilities: SKIPPED (events, odds, or freshness failed)")
    failures = [label for label, ok, _ in stages if not ok]
    if failures:
        print(f"NFL Vegas refresh finished with failures: {', '.join(failures)}")
        return 1
    print("NFL Vegas refresh finished successfully")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh NFL Vegas data")
    parser.add_argument("--date", default=None, help="Eastern game date YYYY-MM-DD")
    args = parser.parse_args()
    target = args.date or datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    config = load_config()
    return run_refresh(DatabaseManager(config.database_url), config.odds_api.api_key, target)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
