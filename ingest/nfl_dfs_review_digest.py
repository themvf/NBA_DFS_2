"""Print the latest stored weekly report card as Markdown, for a CI run summary.

Read-only. Renders the report card the daily job already persisted; it never
recomputes accuracy, so this and the Weekly Player Review cannot disagree.

Usage:
    python -m ingest.nfl_dfs_review_digest [--season 2026] [--week 1] [--variant production]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_review_digest import render


def target_season(value: int | None, now: datetime) -> int:
    """Same rule as the weekly pipeline. Duplicated deliberately rather than
    importing `ingest.nfl_dfs_weekly`, which pulls in pandas and runs schema
    DDL on construction — neither belongs in a read-only reporter."""
    return value or (now.year - 1 if now.month <= 3 else now.year)


_PAYLOAD_SQL = """SELECT payload FROM nfl_dfs_weekly_report_cards
    WHERE season=%s AND week=%s ORDER BY created_at DESC, report_digest DESC LIMIT 1"""


def latest_report(db: DatabaseManager, season: int, week: int | None, lookback: int = 4):
    """The most recent week that actually has scored rows.

    MAX(week) is the wrong default in season: the upcoming week's report card
    exists from the moment projections are frozen, so it would always win and
    the digest would report an unplayed week every time. Walk back from the
    newest week to the first with a scored row, and fall back to the newest if
    nothing is scored yet (preseason, or a week still in progress).
    """
    if week is not None:
        rows = db.execute(_PAYLOAD_SQL, (season, week))
        return (rows[0]["payload"] if rows else None), week
    weeks = [r["week"] for r in db.execute(
        "SELECT DISTINCT week FROM nfl_dfs_weekly_report_cards WHERE season=%s ORDER BY week DESC LIMIT %s",
        (season, lookback))]
    newest = None
    for candidate in weeks:
        rows = db.execute(_PAYLOAD_SQL, (season, candidate))
        if not rows:
            continue
        payload = rows[0]["payload"]
        newest = newest or (payload, candidate)
        if any(r.get("actual") is not None for r in payload.get("rows", [])):
            return payload, candidate
    return newest if newest else (None, None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--variant", default="production")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    season = target_season(args.season, datetime.now(timezone.utc))
    # Read-only: no schema DDL, so this can never contend for table locks.
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    report, week = latest_report(db, season, args.week)
    if report is None:
        # Say which week is missing rather than emitting an empty-looking report.
        print(f"## NFL DFS — {season}\n\n_No stored report card"
              f"{'' if week is None else f' for week {week}'}. The weekly report-card step may not have run._")
        return 0
    print(render(report, variant=args.variant, limit=args.limit))
    return 0


if __name__ == "__main__":
    sys.exit(main())
