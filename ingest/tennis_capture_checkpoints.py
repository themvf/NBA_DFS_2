"""Quota-aware sportsbook captures at decision-useful pre-match checkpoints.

This module is polled by the 15-minute Tennis settlement workflow.  It makes
no paid request unless at least one US Open match is missing a required
sportsbook snapshot.  One tournament refresh covers every match in that tour,
so simultaneous session matches do not multiply API cost.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_schedule import discover_tournaments, fetch_tournament
from ingest.tennis_us_open_preflight import active_us_open_tournaments

TOURNAMENT_PATTERN = "%us open%"
CHECKPOINTS = (
    ("open", 12 * 60, 72 * 60),
    ("t_minus_6h", 5 * 60, 7 * 60),
    ("t_minus_90m", 60, 120),
    ("t_minus_20m", 5, 35),
)


def due_checkpoints(db: DatabaseManager, now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(timezone.utc)
    due: list[dict] = []
    for name, min_lead_minutes, max_lead_minutes in CHECKPOINTS:
        rows = db.execute(
            """
            SELECT tm.id, tm.tour, tm.commence_time
            FROM tennis_matches tm
            WHERE tm.tournament ILIKE %s
              AND tm.commence_time > %s
              AND EXTRACT(EPOCH FROM (tm.commence_time - %s)) / 60.0
                    BETWEEN %s AND %s
              AND NOT EXISTS (
                SELECT 1 FROM game_odds_history h
                WHERE h.sport='tennis' AND h.matchup_id=tm.id
                  AND h.captured_at <= tm.commence_time
                  AND NOT (h.books ? 'polymarket')
                  AND EXTRACT(EPOCH FROM (tm.commence_time - h.captured_at)) / 60.0
                        BETWEEN %s AND %s
              )
            ORDER BY tm.commence_time
            """,
            (TOURNAMENT_PATTERN, now, now, min_lead_minutes, max_lead_minutes,
             min_lead_minutes, max_lead_minutes),
        )
        due.extend({"checkpoint": name, **dict(row)} for row in rows)
    return due


def capture_due_checkpoints(db: DatabaseManager, api_key: str, *, dry_run: bool = False) -> dict:
    due = due_checkpoints(db)
    tours = sorted({row["tour"] for row in due})
    result = {"due_matches": len(due), "due_tours": tours,
              "checkpoints": sorted({row["checkpoint"] for row in due}),
              "captured_events": 0, "dry_run": dry_run}
    if not due or dry_run:
        return result
    active = active_us_open_tournaments(discover_tournaments(api_key))
    active_by_tour = {tour: (key, title) for tour, key, title in active}
    missing = [tour for tour in tours if tour not in active_by_tour]
    if missing:
        raise RuntimeError(f"US Open checkpoint due but provider tour is unavailable: {missing}")
    for tour in tours:
        sport_key, title = active_by_tour[tour]
        result["captured_events"] += fetch_tournament(
            db, api_key, tour, sport_key, title, game_date=None,
        )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Capture due US Open sportsbook checkpoints")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--existing-schema", action="store_true",
                        help="Use an already migrated database without running global DDL")
    args = parser.parse_args()
    config = load_config()
    output = capture_due_checkpoints(
        DatabaseManager(config.database_url, initialize_schema=not args.existing_schema),
        config.odds_api.api_key,
        dry_run=args.dry_run,
    )
    print(json.dumps(output, indent=2, default=str))
