"""Backfill WC 2026 first-scorer results from TheSportsDB.

Fetches the goal timeline for every completed WC 2026 match and stores the
first non-own-goal scorer in soccer_match_scorers.  Idempotent — already-stored
games are skipped unless --force is passed.

Also used to pull ALL goal events (not just first scorer) for future analysis
of goal-timing distributions.

Usage:
    python -m ingest.soccer_scorers_history           # backfill all completed games
    python -m ingest.soccer_scorers_history --force   # re-fetch even if already stored
    python -m ingest.soccer_scorers_history --dry-run # print results, don't write DB
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import unicodedata

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import upsert_soccer_match_goal, upsert_soccer_match_scorer
from ingest.soccer_results import _tsdb_find_event, _tsdb_first_scorer, TSDB_BASE

sys.stdout.reconfigure(encoding="utf-8")
logger = logging.getLogger(__name__)


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return " ".join(text.lower().split())


def fetch_all_goals(tsdb_event_id: str, api_key: str) -> list[dict]:
    """Return all goal events from a TheSportsDB match timeline (non-own-goals only)."""
    try:
        r = requests.get(
            f"{TSDB_BASE}/{api_key}/lookuptimeline.php",
            params={"id": tsdb_event_id},
            timeout=15,
        )
        r.raise_for_status()
        timeline = r.json().get("timeline") or []
    except requests.RequestException as e:
        logger.warning("TheSportsDB lookuptimeline failed for %s: %s", tsdb_event_id, e)
        return []

    goals = []
    for ev in timeline:
        if ev.get("strTimeline") != "Goal":
            continue
        detail = (ev.get("strTimelineDetail") or "").lower()
        if "own" in detail:
            continue
        try:
            minute = int(ev.get("intTime") or 999)
        except (TypeError, ValueError):
            minute = 999
        assist = ev.get("strAssist") or None
        # idAssist="0" means no assist recorded
        if assist and ev.get("idAssist") in ("0", 0):
            assist = None
        goals.append({
            "player": ev.get("strPlayer", ""),
            "team": ev.get("strTeam", ""),
            "assist": assist,
            "minute": minute,
            "detail": ev.get("strTimelineDetail", ""),
        })

    goals.sort(key=lambda x: x["minute"])
    return goals


def backfill(db: DatabaseManager, api_key: str = "123", force: bool = False, dry_run: bool = False) -> int:
    """Fetch and store first scorers for all completed WC 2026 games.

    Returns the number of games processed.
    """
    # Find completed games (score known).
    games = db.execute(
        """
        SELECT sm.game_id, sm.game_date,
               ht.name AS home_name, at.name AS away_name,
               sm.home_score, sm.away_score
        FROM soccer_matchups sm
        JOIN soccer_teams ht ON ht.team_id = sm.home_team_id
        JOIN soccer_teams at ON at.team_id = sm.away_team_id
        WHERE sm.home_score IS NOT NULL AND sm.away_score IS NOT NULL
        ORDER BY sm.game_date, sm.commence_time
        """
    )
    if not games:
        print("No completed games found.")
        return 0

    # Skip games whose stored timeline is COMPLETE unless --force.  "Present" is
    # not enough: a timeline fetched while the match was live (or before TSDB
    # finished publishing) is partial — Belgium-Senegal froze at 2 of 5 goals —
    # so completeness is judged by goal count vs the final score.  Games with an
    # own goal never look complete (own goals are excluded from
    # soccer_match_goals) and get harmlessly re-fetched each pass.
    if not force:
        goal_counts = {r["game_id"]: r["n"] for r in db.execute(
            "SELECT game_id, COUNT(*) AS n FROM soccer_match_goals GROUP BY game_id")}
        goalless_stored = {r["game_id"] for r in db.execute(
            "SELECT game_id FROM soccer_match_scorers WHERE scorer_name = '[none]'")}
    else:
        goal_counts = {}
        goalless_stored = set()

    processed = 0
    skipped = 0
    no_scorer = 0

    for g in games:
        gid = g["game_id"]
        total = (g["home_score"] or 0) + (g["away_score"] or 0)
        complete = (goal_counts.get(gid, 0) == total and total > 0) or \
                   (total == 0 and gid in goalless_stored)
        if complete:
            skipped += 1
            continue

        game_date = str(g["game_date"])[:10]
        home, away = g["home_name"], g["away_name"]

        tsdb_id = _tsdb_find_event(game_date, home, away, api_key)
        if not tsdb_id:
            logger.warning("TheSportsDB: no event for %s vs %s on %s", home, away, game_date)
            continue

        goals = fetch_all_goals(tsdb_id, api_key)
        if not goals:
            # 0-0 draw or all own goals.
            no_scorer += 1
            if dry_run:
                print(f"  {game_date} {home} {g['home_score']}-{g['away_score']} {away}: NO SCORER")
            else:
                upsert_soccer_match_scorer(
                    db, game_id=gid, game_date=game_date,
                    scorer_name="[none]", scorer_team=None,
                    goal_minute=None, tsdb_event_id=tsdb_id,
                )
        else:
            first = goals[0]
            if dry_run:
                print(
                    f"  {game_date} {home} {g['home_score']}-{g['away_score']} {away}: "
                    f"{first['player']} ({first['team']}, {first['minute']}') — "
                    f"{len(goals)} total goals"
                )
            else:
                # Store first scorer summary (existing behavior).
                upsert_soccer_match_scorer(
                    db, game_id=gid, game_date=game_date,
                    scorer_name=first["player"], scorer_team=first["team"],
                    goal_minute=first["minute"], tsdb_event_id=tsdb_id,
                )
                # Replace, don't merge: a stale partial row with a divergent
                # minute would survive the upsert as a duplicate and the goal
                # count would never match the final score again.
                db.execute("DELETE FROM soccer_match_goals WHERE game_id = %s", (gid,))
                # Store all goals so the UI can show per-player goal counts.
                for goal in goals:
                    upsert_soccer_match_goal(
                        db, game_id=gid, game_date=game_date,
                        player_name=goal["player"], player_team=goal["team"],
                        assist_name=goal.get("assist"),
                        goal_minute=goal["minute"],
                        is_first_goal=(goal is first),
                    )

        processed += 1
        time.sleep(0.3)  # gentle rate limit — free tier has no stated limit but be courteous

    action = "would process" if dry_run else "processed"
    print(
        f"Soccer scorers: {action} {processed} games "
        f"({skipped} already stored, {no_scorer} with no scorer)"
    )
    return processed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill WC 2026 first-scorer results")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if already stored")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write DB")
    # default MUST be None: with default="123" the `args.api_key or getenv(...)`
    # below is always truthy "123", so the premium THESPORTSDB_API_KEY env is
    # never consulted — which silently ran first-scorer backfill on the
    # rate-limited free tier (429s) even with a paid key configured.
    parser.add_argument("--api-key", default=None, help="TheSportsDB API key (default: env or free '123')")
    args = parser.parse_args()

    import os
    api_key = args.api_key or os.getenv("THESPORTSDB_API_KEY", "123")

    config = load_config()
    db = DatabaseManager(config.database_url)
    backfill(db, api_key=api_key, force=args.force, dry_run=args.dry_run)
