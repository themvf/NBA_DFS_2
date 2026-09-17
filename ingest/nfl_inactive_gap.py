"""Size the inactive gap from stored data. Read-only.

Answers one question before anyone builds an inactive feed: of the players
carrying a Questionable designation before kickoff, how many never recorded a
stat line, and how many projected points did we spend on them?

Usage:
    python -m ingest.nfl_inactive_gap [--season 2026] [--week 1]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from model.nfl_inactive_gap import render, status_before_kickoff, summarize

SKILL = ["QB", "RB", "WR", "TE"]


def target_season(value, now):
    return value or (now.year - 1 if now.month <= 3 else now.year)


def collect(db: DatabaseManager, season: int, week: int | None) -> tuple[list[dict], set[int]]:
    # Kickoff per player-week, from the canonical schedule via his team.
    kickoff_sql = """SELECT p.id player_id, g.week, MIN(g.kickoff) kickoff
            FROM ff_players p
            JOIN nfl_teams t ON t.abbreviation = p.team_abbrev
            JOIN nfl_season_games g
              ON g.season = %(season)s AND g.game_type = 'REG'
             AND (g.home_team_id = t.team_id OR g.away_team_id = t.team_id)
            WHERE p.season = %(season)s AND p.active AND p.position = ANY(%(skill)s)
              AND (%(week)s::int IS NULL OR g.week = %(week)s::int)
            GROUP BY p.id, g.week"""
    kickoffs = db.execute(kickoff_sql, {"season": season, "skill": SKILL, "week": week})

    statuses = db.execute(
        """SELECT o.player_id, o.normalized_status status, s.fetched_at captured_at,
                  s.dataset
           FROM ff_player_injury_observations o
           JOIN ff_source_snapshots s ON s.id = o.source_snapshot_id
           WHERE o.season = %s AND o.source = 'fantasypros'
             AND s.dataset LIKE %s""",
        (season, f"game-week-injuries-v2-{season}-%"),
    )
    captures: dict[tuple[int, int], list[dict]] = {}
    for row in statuses:
        # The week lives in the dataset name: game-week-injuries-v2-<season>-<week>
        tail = str(row["dataset"]).rsplit("-", 1)[-1]
        if not tail.isdigit():
            continue
        captures.setdefault((int(row["player_id"]), int(tail)), []).append(
            {"status": row["status"], "captured_at": row["captured_at"]})

    played = {(int(r["player_id"]), int(r["week"])) for r in db.execute(
        """SELECT DISTINCT player_id, week FROM nfl_dfs_player_week_results
           WHERE season = %s AND actual_dk_fpts IS NOT NULL""", (season,))}

    projected = {(int(r["player_id"]), int(r["week"])): float(r["model_proj_fpts"] or 0.0)
                 for r in db.execute(
        """SELECT DISTINCT ON (pp.player_id, r.week)
                  pp.player_id, r.week, pp.model_proj_fpts
           FROM nfl_dfs_player_projections pp
           JOIN nfl_dfs_projection_runs r ON r.run_id = pp.run_id
           WHERE r.season = %s
           ORDER BY pp.player_id, r.week, r.as_of_at DESC""", (season,))}

    rows, weeks = [], set()
    for entry in kickoffs:
        pid, wk = int(entry["player_id"]), int(entry["week"])
        status = status_before_kickoff(captures.get((pid, wk)), entry["kickoff"])
        if not status:
            continue
        weeks.add(wk)
        rows.append({"status": status, "week": wk, "played": (pid, wk) in played,
                     "projected": projected.get((pid, wk))})
    return rows, weeks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    args = parser.parse_args()
    season = target_season(args.season, datetime.now(timezone.utc))
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    rows, weeks = collect(db, season, args.week)
    print(render(summarize(rows), season, weeks))
    return 0


if __name__ == "__main__":
    sys.exit(main())
