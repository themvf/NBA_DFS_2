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

    # Skip games already stored unless --force.
    if not force:
        stored = {r["game_id"] for r in db.execute("SELECT game_id FROM soccer_match_scorers")}
        # Also skip if we already have full goals data for that game.
        stored |= {r["game_id"] for r in db.execute("SELECT DISTINCT game_id FROM soccer_match_goals")}
    else:
        stored = set()

    processed = 0
    skipped = 0
    no_scorer = 0

    for g in games:
        gid = g["game_id"]
        if gid in stored:
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
    parser.add_argument("--api-key", default="123", help="TheSportsDB API key (default: free '123')")
    args = parser.parse_args()

    import os
    api_key = args.api_key or os.getenv("THESPORTSDB_API_KEY", "123")

    config = load_config()
    db = DatabaseManager(config.database_url)
    backfill(db, api_key=api_key, force=args.force, dry_run=args.dry_run)
