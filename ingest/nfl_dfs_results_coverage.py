"""Say plainly which completed NFL games still cannot be scored.

The post-week review used to finish green whether or not it had scored the
week: on 2026-09-29 the Tuesday pass re-scored stored rows, inserted nothing,
and Monday night's PHI@CHI had zero result rows. The only trace was one line
from the slate report card ("not scorable yet (pending_game 46)").

This check reads the schedule and the realized-results ledger after the
results phase has run and names every completed game whose player feed has
not landed, using the same skill-coverage rule as the slate report card
(`model.nfl_dfs_slate_reportcard.skill_coverage`): a game counts only when
each of its teams has an exact QB/RB/WR/TE result under the current scoring
version. DST rows do not count; they come from a different nflverse feed.

Outcomes:
  - a completed game kicked off less than GRACE_HOURS ago and not scorable:
    a warning. nflverse publishes after the game; observed 2026-09-29, the
    Monday-night stats landed at 11:34 UTC Tuesday, after the 10:07 slot.
  - a completed game older than GRACE_HOURS and still not scorable: an error,
    and the command exits 1. That is a real gap, not a publishing delay.
  - a game that kicked off more than SCHEDULE_LAG_HOURS ago but the schedule
    still calls incomplete: a warning (a postponed or suspended game would
    look the same, so it does not fail the run).

Usage:
    python -m ingest.nfl_dfs_results_coverage [--season 2026]
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_results import SCORING_VERSION
from ingest.nfl_dfs_weekly import target_season
from model.nfl_dfs_slate_reportcard import latest_exact_results, skill_coverage

GRACE_HOURS = 30
SCHEDULE_LAG_HOURS = 8


def assess(games: list[dict], results: list[dict], now: datetime, *,
           grace_hours: float = GRACE_HOURS, schedule_lag_hours: float = SCHEDULE_LAG_HOURS) -> dict:
    """Pure: classify every game that has kicked off."""
    exact = latest_exact_results(results, now)
    out = {"checked": 0, "scorable": 0, "awaiting_recent": [], "missing_overdue": [], "not_marked_completed": []}
    for game in sorted(games, key=lambda g: (g["kickoff"], g["id"])):
        kickoff = game["kickoff"]
        if kickoff is None or kickoff > now:
            continue
        age = (now - kickoff).total_seconds() / 3600
        label = {"game_id": game["id"], "week": game.get("week"), "game": game["game_key"],
                 "kickoff": kickoff.isoformat(), "hours_since_kickoff": round(age, 1)}
        if not game.get("completed"):
            if age > schedule_lag_hours:
                out["not_marked_completed"].append(label)
            continue
        out["checked"] += 1
        coverage = skill_coverage(game, exact.get(game["id"]))
        if coverage["covered"]:
            out["scorable"] += 1
            continue
        label["teams_missing_skill_results"] = coverage["teams_missing_skill_results"]
        (out["missing_overdue"] if age >= grace_hours else out["awaiting_recent"]).append(label)
    return out


def load(db: DatabaseManager, season: int) -> tuple[list[dict], list[dict]]:
    games = [dict(r) for r in db.execute(
        """SELECT g.id, g.week, g.kickoff, g.completed, a.abbreviation || '@' || h.abbreviation AS game_key
           FROM nfl_season_games g
           JOIN nfl_teams h ON h.team_id = g.home_team_id
           JOIN nfl_teams a ON a.team_id = g.away_team_id
           WHERE g.season = %s AND g.game_type = 'REG'""",
        (season,),
    )]
    results = [dict(r) for r in db.execute(
        """SELECT id, player_id, game_id, position, team, actual_dk_fpts, scoring_status, computed_at
           FROM nfl_dfs_player_week_results
           WHERE season = %s AND scoring_version = %s AND game_id IS NOT NULL""",
        (season, SCORING_VERSION),
    )]
    return games, results


def _describe(game: dict) -> str:
    missing = game.get("teams_missing_skill_results")
    tail = f"; no player results for {', '.join(missing)}" if missing else ""
    return f"week {game['week']} {game['game']} (kicked off {game['hours_since_kickoff']}h ago{tail})"


def report(summary: dict, season: int) -> int:
    lines = [f"## Realized results coverage ({season}, {SCORING_VERSION})", "",
             f"{summary['scorable']} of {summary['checked']} completed games are scorable."]
    for game in summary["missing_overdue"]:
        message = f"Completed game still not scorable: {_describe(game)}. Its players are held back, not graded."
        print(f"::error title=NFL results missing::{message}")
        lines.append(f"- ERROR: {message}")
    for game in summary["awaiting_recent"]:
        message = (f"Completed game not scorable yet: {_describe(game)}. nflverse has not published its player "
                   f"stats; the next post-week pass will pick it up.")
        print(f"::warning title=NFL results pending::{message}")
        lines.append(f"- {message}")
    for game in summary["not_marked_completed"]:
        message = f"Schedule does not mark {_describe(game)} as completed."
        print(f"::warning title=NFL schedule lag::{message}")
        lines.append(f"- {message}")
    if not (summary["missing_overdue"] or summary["awaiting_recent"] or summary["not_marked_completed"]):
        lines.append("Every game that has kicked off is completed and scorable.")
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with Path(step_summary).open("a", encoding="utf-8") as stream:
            stream.write("\n".join(lines) + "\n\n")
    print(json.dumps(summary, indent=2, default=str))
    return 1 if summary["missing_overdue"] else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--grace-hours", type=float, default=GRACE_HOURS)
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    games, results = load(db, season)
    return report(assess(games, results, now, grace_hours=args.grace_hours), season)


if __name__ == "__main__":
    raise SystemExit(main())
