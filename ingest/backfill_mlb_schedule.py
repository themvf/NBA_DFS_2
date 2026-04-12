"""Backfill mlb_matchups (schedule + scores) for a date range.

Uses the free MLB Stats API — no IP blocks, no auth required.

Usage:
    python -m ingest.backfill_mlb_schedule --start 2026-03-27 --end 2026-04-12
    python -m ingest.backfill_mlb_schedule --start 2026-03-27 --end 2026-04-12 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, timedelta

from config import load_config
from db.database import DatabaseManager
from ingest.mlb_schedule import fetch_schedule, fetch_scores

logger = logging.getLogger(__name__)

SLEEP_BETWEEN_DATES = 1.0  # MLB Stats API is generous, but be polite


def _all_dates(start: str, end: str) -> list[str]:
    d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    out = []
    while d <= end_d:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _dates_already_complete(db: DatabaseManager, start: str, end: str) -> set[str]:
    rows = db.execute(
        """
        SELECT game_date::text AS game_date
        FROM mlb_matchups
        WHERE game_date BETWEEN %s AND %s
        GROUP BY game_date
        HAVING COUNT(*) > 0
          AND COUNT(*) FILTER (WHERE home_score IS NULL OR away_score IS NULL) = 0
        """,
        (start, end),
    )
    return {r["game_date"] for r in rows}


def _dates_with_matchups(db: DatabaseManager, start: str, end: str) -> set[str]:
    rows = db.execute(
        """
        SELECT DISTINCT game_date::text AS game_date
        FROM mlb_matchups
        WHERE game_date BETWEEN %s AND %s
        """,
        (start, end),
    )
    return {r["game_date"] for r in rows}


def backfill(db: DatabaseManager, start: str, end: str, dry_run: bool = False) -> None:
    all_dates = _all_dates(start, end)
    complete = _dates_already_complete(db, start, end)
    has_matchups = _dates_with_matchups(db, start, end)

    need_schedule = [d for d in all_dates if d not in has_matchups]
    need_scores = [
        d for d in all_dates
        if d in has_matchups and d not in complete
        and date.fromisoformat(d) < date.today()
    ]

    print(
        f"MLB Schedule backfill | {start} to {end}\n"
        f"  Total dates      : {len(all_dates)}\n"
        f"  Need schedule    : {len(need_schedule)}\n"
        f"  Need scores only : {len(need_scores)}\n"
        f"  Already complete : {len(complete)}"
    )

    if dry_run:
        if need_schedule:
            print(f"\nWould fetch schedule for: {need_schedule[0]} ... {need_schedule[-1]}")
        if need_scores:
            print(f"Would fetch scores for: {need_scores[:5]}{'...' if len(need_scores) > 5 else ''}")
        return

    total_games = 0
    skipped = 0
    for i, d in enumerate(need_schedule):
        try:
            games = fetch_schedule(db, d)
        except Exception as exc:
            logger.warning("Skipping %s — schedule fetch failed: %s", d, exc)
            skipped += 1
            time.sleep(SLEEP_BETWEEN_DATES)
            continue
        if games:
            try:
                fetch_scores(db, d)
            except Exception as exc:
                logger.warning("Scores failed for %s: %s", d, exc)
            total_games += len(games)
        if i < len(need_schedule) - 1:
            time.sleep(SLEEP_BETWEEN_DATES)

    if skipped:
        print(f"  {skipped} dates skipped due to API errors")

    scores_updated = 0
    for i, d in enumerate(need_scores):
        try:
            n = fetch_scores(db, d)
            scores_updated += n
        except Exception as exc:
            logger.warning("Scores failed for %s: %s", d, exc)
        if i < len(need_scores) - 1:
            time.sleep(SLEEP_BETWEEN_DATES)

    print(
        f"\nDone: {total_games} games ingested across {len(need_schedule)} dates, "
        f"{scores_updated} score updates across {len(need_scores)} dates"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Backfill MLB schedule + scores")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="End date   YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    backfill(db, args.start, args.end, dry_run=args.dry_run)
