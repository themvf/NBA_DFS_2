"""Verify provider coverage and stored fixture readiness for the 2026 US Open.

The check is intentionally executable in CI. It enriches deterministic Slam
metadata, reconciles the 128-player first-round draw (64 matches per tour),
and fails loudly when odds capture or identity coverage is incomplete.
"""

from __future__ import annotations

import argparse
import json

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_schedule import discover_tournaments

FIRST_ROUND_EXPECTED_PER_TOUR = 64
TOURNAMENT_PATTERN = "%us open%"


def is_us_open(sport_key: str, title: str) -> bool:
    """Identify ATP/WTA US Open provider keys without relying on one title form."""
    normalized = f"{sport_key} {title}".lower().replace("_", " ")
    return "us open" in normalized


def active_us_open_tournaments(tournaments: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    return [item for item in tournaments if is_us_open(item[1], item[2])]


def enrich_us_open_metadata(db: DatabaseManager) -> int:
    """Fill deterministic singles metadata without inventing player features."""
    rows = db.execute(
        """
        WITH first_start AS (
          SELECT MIN(scheduled_at) AS at
          FROM tennis_events
          WHERE canonical_tournament ILIKE %s AND scheduled_at IS NOT NULL
        )
        UPDATE tennis_events e
        SET surface='hard', indoor=FALSE,
            best_of=CASE WHEN e.tour='ATP' THEN 5 WHEN e.tour='WTA' THEN 3 ELSE e.best_of END,
            round=CASE
              WHEN e.round IS NULL AND e.scheduled_at <= first_start.at + INTERVAL '3 days'
              THEN 'R128' ELSE e.round END,
            updated_at=NOW()
        FROM first_start
        WHERE e.canonical_tournament ILIKE %s
          AND (e.surface IS DISTINCT FROM 'hard' OR e.indoor IS DISTINCT FROM FALSE
               OR e.best_of IS NULL
               OR (e.round IS NULL
                   AND e.scheduled_at <= first_start.at + INTERVAL '3 days'))
        RETURNING e.id
        """,
        (TOURNAMENT_PATTERN, TOURNAMENT_PATTERN),
    )
    return len(rows)


def preflight(db: DatabaseManager, api_key: str, *, enrich: bool = False) -> dict:
    active = active_us_open_tournaments(discover_tournaments(api_key))
    enriched = enrich_us_open_metadata(db) if enrich else 0
    coverage_rows = db.execute(
        """
        SELECT tm.tour, COUNT(*) FILTER (WHERE e.round='R128') AS fixtures,
               COUNT(*) FILTER (WHERE e.round='R128' AND tm.home_ml IS NOT NULL
                                 AND tm.away_ml IS NOT NULL) AS priced,
               COUNT(*) FILTER (WHERE e.round='R128' AND tm.canonical_event_id IS NOT NULL) AS canonicalized
        FROM tennis_matches tm
        LEFT JOIN tennis_events e ON e.id=tm.canonical_event_id
        WHERE tm.tournament ILIKE %s
        GROUP BY tm.tour ORDER BY tm.tour
        """,
        (TOURNAMENT_PATTERN,),
    )
    coverage = [
        {key: (int(row[key]) if key != "tour" else row[key])
         for key in ("tour", "fixtures", "priced", "canonicalized")}
        for row in coverage_rows
    ]
    metadata = db.execute_one(
        """
        SELECT COUNT(*) FILTER (WHERE round='R128') AS first_round_events,
               COUNT(*) FILTER (WHERE round='R128' AND surface='hard') AS surface_known,
               COUNT(*) FILTER (WHERE round='R128' AND best_of IS NOT NULL) AS best_of_known,
               COUNT(*) FILTER (WHERE round='R128' AND indoor=FALSE) AS outdoor_known
        FROM tennis_events WHERE canonical_tournament ILIKE %s
        """,
        (TOURNAMENT_PATTERN,),
    ) or {}
    captures = db.execute_one(
        """
        SELECT COUNT(*) AS matches,
               COUNT(*) FILTER (WHERE sportsbook_caps >= 2) AS with_two_sportsbook_captures,
               ROUND(AVG(sportsbook_caps), 2) AS avg_sportsbook_captures
        FROM (
          SELECT tm.id,
                 COUNT(g.id) FILTER (WHERE NOT (g.books ? 'polymarket')) AS sportsbook_caps
          FROM tennis_matches tm
          LEFT JOIN tennis_events e ON e.id=tm.canonical_event_id
          LEFT JOIN game_odds_history g
            ON g.sport='tennis' AND g.matchup_id=tm.id AND g.captured_at<=tm.commence_time
          WHERE tm.tournament ILIKE %s AND e.round='R128'
          GROUP BY tm.id
        ) capture_counts
        """,
        (TOURNAMENT_PATTERN,),
    ) or {}
    issues: list[str] = []
    if not active:
        issues.append("The Odds API does not advertise an active US Open tournament")
    by_tour = {row["tour"]: row for row in coverage}
    for tour in ("ATP", "WTA"):
        row = by_tour.get(tour, {})
        fixtures = int(row.get("fixtures", 0))
        priced = int(row.get("priced", 0))
        canonicalized = int(row.get("canonicalized", 0))
        if fixtures != FIRST_ROUND_EXPECTED_PER_TOUR:
            issues.append(f"{tour} first-round draw has {fixtures}/64 fixtures")
        if priced != fixtures:
            issues.append(f"{tour} has {priced}/{fixtures} priced first-round fixtures")
        if canonicalized != fixtures:
            issues.append(f"{tour} has {canonicalized}/{fixtures} canonical first-round fixtures")
    expected_total = FIRST_ROUND_EXPECTED_PER_TOUR * 2
    for field in ("first_round_events", "surface_known", "best_of_known", "outdoor_known"):
        if int(metadata.get(field, 0)) != expected_total:
            issues.append(f"metadata {field}={int(metadata.get(field, 0))}/{expected_total}")

    return {
        "ready": not issues,
        "provider_us_open_active": bool(active),
        "provider_tournaments": [
            {"tour": tour, "sport_key": key, "title": title} for tour, key, title in active
        ],
        "enriched_events": enriched,
        "stored_coverage": coverage,
        "metadata": {k: int(v or 0) for k, v in metadata.items()},
        "capture_coverage": {
            k: (float(v) if k == "avg_sportsbook_captures" and v is not None else int(v or 0))
            for k, v in captures.items()
        },
        "issues": issues,
        "rating_policy": "moneyline calibration only; capped at 2 stars",
        "derivative_policy": "book-rule settlement; unresolved rules require manual review",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="US Open tennis readiness preflight")
    parser.add_argument("--enrich", action="store_true", help="Fill deterministic Slam metadata")
    parser.add_argument("--fail-on-unready", action="store_true", help="Exit non-zero when a readiness gate fails")
    args = parser.parse_args()
    config = load_config()
    result = preflight(
        DatabaseManager(config.database_url), config.odds_api.api_key, enrich=args.enrich,
    )
    print(json.dumps(result, indent=2))
    if args.fail_on_unready and not result["ready"]:
        raise SystemExit(2)
