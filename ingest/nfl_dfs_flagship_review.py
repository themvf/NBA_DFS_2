"""Weekly: import the week's free flagship contests from stat-api and review our build against the winners.

Runs from the post-week workflow (Monday and Tuesday). For the latest NFL
week with a finished game day it finds, on each FINISHED game day (every game
final and six hours past its last kickoff), the biggest-prize contest of
every DraftKings slate stat-api lists, asks stat-api whether that contest is a
flagship (the Millionaire, the biggest Thursday and Monday showdown: those
answer in full with no key), imports each flagship through
`ingest.statapi_contest` (full standings, the top 100 users' every lineup),
and then runs the top-builds review (`research.nfl_top_builds`) against the
optimizer run saved for that slate. The review's numbers are stored in
`nfl_dfs_field_build_reviews`, one row per contest, so the winners-versus-us
comparison accumulates week by week instead of living in a terminal.

A week with no saved build for a slate still imports and reviews the winners;
the row says `ours: null` and why. A contest stat-api does not serve in full
is skipped and named, never stored as a preview. Nothing here needs
`STAT_API_KEY`; with one set, the contest listing is simply deeper.

Usage:
    python -m ingest.nfl_dfs_flagship_review                      # latest completed week
    python -m ingest.nfl_dfs_flagship_review --season 2026 --week 4
    python -m ingest.nfl_dfs_flagship_review --season 2026 --week 4 --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from ingest.statapi_contest import Client, StatApiError, import_contest, open_rows_only, usable
from model import statapi_contest as m
from research.nfl_top_builds import review_contest

VERSION = "nfl-dfs-flagship-review-v1"
TOP_USERS = 100
TOP_REVIEW = 5


# A game day is finished when every game on it is final and its last kickoff
# is six hours old. Monday's run then reviews Thursday and Sunday (Sunday night
# included) while Monday night is still to play; Tuesday's adds Monday.
_FINISHED_DAY = "bool_and(completed) AND max(kickoff) < NOW() - INTERVAL '6 hours'"


def latest_completed_week(db: DatabaseManager, season: int | None) -> tuple[int, int] | None:
    """The latest regular-season week with at least one finished game day."""
    rows = db.execute(
        f"""SELECT season, week FROM (
              SELECT season, week, gameday FROM nfl_season_games
               WHERE game_type = 'REG' AND (%s IS NULL OR season = %s)
               GROUP BY season, week, gameday HAVING {_FINISHED_DAY}) d
            ORDER BY season DESC, week DESC LIMIT 1""", (season, season))
    return (int(rows[0]["season"]), int(rows[0]["week"])) if rows else None


def game_days(db: DatabaseManager, season: int, week: int, *, finished_only: bool = False) -> list[str]:
    having = f"HAVING {_FINISHED_DAY}" if finished_only else ""
    rows = db.execute(
        f"""SELECT gameday FROM nfl_season_games WHERE season = %s AND week = %s AND game_type = 'REG'
            GROUP BY gameday {having} ORDER BY 1""",
        (season, week))
    return [str(r["gameday"]) for r in rows]


def flagship_candidates(client: Client, date: str) -> list[dict]:
    """The biggest-prize contest of each DraftKings NFL slate that day, with stat-api's access plan for it."""
    out = []
    slates = client.slates(date, "nfl").get("slates") or []
    for slate in slates:
        if "In-Game" in str(slate.get("name") or ""):
            continue
        listing = m.listing_contests(client.contests(int(slate["id"])).get("contests") or [], slate)
        listing = [c for c in listing if c.get("lineups_status") in (None, "available")]
        if not listing:
            continue
        best = max(listing, key=lambda c: ((c["prize_pool"] or 0), c["entry_count"]))
        try:
            probe = client.standings(best["statapi_id"], 1, 1)
        except StatApiError as exc:
            out.append({**best, "plan": f"error: {exc}"})
            continue
        access = probe.get("access") or {}
        out.append({**best, "plan": access.get("plan"), "full": bool(access.get("full")),
                    "usable": usable(access), "open_rows": open_rows_only(access),
                    "external_id": (probe.get("contest") or {}).get("external_id") or best.get("external_id")})
    return out


def persist_review(db: DatabaseManager, review: dict) -> None:
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO nfl_dfs_field_build_reviews
                     (contest_id, review_version, season, week, run_id, top_n, winners, ours, computed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                   ON CONFLICT (contest_id, review_version) DO UPDATE SET
                     season = EXCLUDED.season, week = EXCLUDED.week, run_id = EXCLUDED.run_id,
                     top_n = EXCLUDED.top_n, winners = EXCLUDED.winners, ours = EXCLUDED.ours, computed_at = NOW()""",
                (review["contest_id"], VERSION, review["season"], review["week"], review["run_id"], review["top_n"],
                 Json(review["winners"]), Json(review["ours"])))


def run(db: DatabaseManager | None, client: Client, season: int, week: int, days: list[str], *, dry_run: bool) -> int:
    print(f"{season} week {week}: game days {', '.join(days)}")
    imported, skipped = [], []
    for day in days:
        for c in flagship_candidates(client, day):
            label = f"{day} {c['name'][:60]} ({c['entry_count']:,} entries, ${c['prize_pool'] or 0:,.0f})"
            if c.get("plan") == "flagship" and c.get("usable"):
                print(f"  FLAGSHIP {label}" + (f" (top {c['open_rows']} rows open, no score curve)"
                                               if c.get("open_rows") else ""))
                if dry_run:
                    imported.append(c)
                    continue
                args = SimpleNamespace(standings_rows=1000, all_standings=True, top_users=TOP_USERS, keep_top=100,
                                       ownership_min_coverage=0.95, upload=None)
                try:
                    text = import_contest(args, client, db, c["statapi_id"])
                except StatApiError as exc:
                    print(f"    import failed: {exc}")
                    skipped.append((label, str(exc)))
                    continue
                print("    " + text.splitlines()[-2].strip() if text else "")
                imported.append(c)
            else:
                skipped.append((label, f"plan {c.get('plan')}" + ("" if c.get("full") else ", not served in full")))
    for label, why in skipped:
        print(f"  skipped {label}: {why}")
    if dry_run or db is None:
        print(f"dry run: {len(imported)} flagship contest(s) found, nothing written")
        return 0

    for c in imported:
        contest_id = str(c["external_id"])
        try:
            review = review_contest(db, contest_id, top=TOP_REVIEW, run_id=None)
        except LookupError as exc:
            print(f"  review of DK {contest_id}: {exc}")
            continue
        review.update({"season": season, "week": week})
        persist_review(db, review)
        ours = review["ours"]
        if not ours:
            detail = "ours: no saved build for this slate"
        elif ours["best_points"] is None:
            detail = f"ours run {ours['run_id'][:8]}: no lineup scored (no points for {', '.join(ours['players_without_points'])})"
        else:
            rank = ours["best_rank_estimate"]
            detail = (f"ours run {ours['run_id'][:8]} best {ours['best_points']} "
                      + (f"(~#{rank:,})" if rank is not None else "(rank unknown: no score curve)"))
            if ours["lineups_unscored"]:
                detail += (f"; {ours['lineups_unscored']} lineup(s) not scored, no points for "
                           f"{', '.join(ours['players_without_points'])}")
        print(f"  review stored for DK {contest_id}: winners {len(review['winners'])}, {detail}")
    print(f"{client.calls} requests to stat-api")
    return 0


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--dry-run", action="store_true", help="discover and name the flagship contests, write nothing")
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=not args.dry_run)
    if args.week and not args.season:
        parser.error("--week needs --season")
    if args.season and args.week:
        season, week = args.season, args.week
    else:
        found = latest_completed_week(db, args.season)
        if not found:
            print("no completed regular-season week on the schedule; nothing to review")
            return
        season, week = found
    # An explicit --week reviews every day asked for; the default only finished ones.
    days = game_days(db, season, week, finished_only=not (args.season and args.week))
    if not days:
        sys.exit(f"{season} week {week}: no game days on the schedule")
    import os
    client = Client(api_key=os.environ.get("STAT_API_KEY") or None)
    sys.exit(run(None if args.dry_run else db, client, season, week, days, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
