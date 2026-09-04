"""Daily NFL DFS orchestration, including current-season realized inputs.

Uses free nflverse feeds only. Core tables must already be installed; this
runner creates only DFS tables and never runs unrelated global migrations.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from psycopg2.extras import execute_batch

from config import load_config
from db.database import DatabaseManager
from db.schema import TABLES
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_independent import (
    NFLVERSE_WEEKLY_STATS_URL, NFLVERSE_WEEKLY_TEAM_STATS_URL,
    WEEKLY_STAT_POSITIONS, _fetch_csv, _snapshot,
    save_weekly_history, save_dst_weekly_history,
)
from ingest.nfl_season_schedule import fetch_schedule, load_season, verify_season
from ingest.nfl_dfs_results import materialize, SCORING_FIELDS


DST_FIELDS = {"def_sacks", "def_interceptions", "fumble_recovery_opp", "def_safeties",
              "def_tds", "special_teams_tds", "def_fg_blocks", "def_pat_blocks",
              "def_punt_blocks", "def_2pt_made"}


class BatchedWeeklyWriter(RefreshDatabase):
    """Reuse source adapters while batching their independent weekly upserts."""
    def __init__(self, database_url):
        super().__init__(database_url)
        self.pending = {}

    def execute(self, statement, params=None):
        if "INSERT INTO ff_player_week_stats" in statement:
            self.pending.setdefault(statement, []).append(params)
            return []
        return super().execute(statement, params)

    def flush(self):
        with self.conn.cursor() as cursor:
            for statement, values in self.pending.items():
                execute_batch(cursor, statement, values, page_size=500)
        self.pending.clear()


class PipelineDatabase(DatabaseManager):
    def _ensure_schema(self):
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '15s'")
                for ddl in TABLES:
                    if "CREATE TABLE IF NOT EXISTS nfl_dfs_" in ddl:
                        cursor.execute(ddl)

    def execute_many(self, sql, params_list):
        with self.connect() as connection:
            with connection.cursor() as cursor:
                execute_batch(cursor, sql, params_list, page_size=500)


def target_season(value: int | None, now: datetime) -> int:
    return value or (now.year - 1 if now.month <= 3 else now.year)


def validate_partial_feed(frame: pd.DataFrame, season: int, *, team: bool) -> pd.DataFrame:
    required = {"season", "week", "season_type", "team", "opponent_team", "game_id"}
    required |= DST_FIELDS if team else {"player_id", "position", *SCORING_FIELDS["QB"], *SCORING_FIELDS["K"]}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"weekly feed missing columns: {sorted(missing)}")
    selected = frame[(frame["season"] == season) & (frame["season_type"] == "REG")].copy()
    if not team:
        selected = selected[selected["position"].isin(WEEKLY_STAT_POSITIONS)]
    if selected.empty:
        raise ValueError("weekly feed has no current-season regular-season rows")
    if not selected["week"].between(1, 18).all():
        raise ValueError("invalid regular-season week in feed")
    identity = ["team", "week"] if team else ["player_id", "week"]
    if selected[identity].isna().any().any() or selected.duplicated(identity).any():
        raise ValueError("missing or duplicate weekly feed identity")
    scored = selected if team else selected[selected["position"].isin(WEEKLY_STAT_POSITIONS)]
    fields = DST_FIELDS if team else set(SCORING_FIELDS["QB"]) | set(SCORING_FIELDS["K"])
    numbers = scored[list(fields)].apply(pd.to_numeric, errors="coerce")
    if numbers.isna().any().any() or numbers.isin([float("inf"), -float("inf")]).any().any():
        raise ValueError("missing or non-finite scoring components in weekly feed")
    return selected


def fetch_partial(url: str, season: int, *, team: bool):
    for attempt in range(3):
        try:
            frame, digest = _fetch_csv(url)
            return validate_partial_feed(frame, season, team=team), digest
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def refresh_results(db, season: int) -> dict:
    schedule = fetch_schedule()
    loaded = load_season(db, season, schedule)
    problems = verify_season(db, season)
    if problems:
        raise ValueError(f"schedule health gate failed: {problems}")
    games = schedule[(schedule["season"] == season) & (schedule["game_type"] == "REG")]
    completed = games[games["home_score"].notna() & games["away_score"].notna()]
    if completed.empty:
        return {"season": season, "games": loaded, "status": "awaiting_completed_games", "source_rows": 0}

    # Validate both before writing anything. Small Week 1 feeds are valid;
    # unavailable/malformed feeds fail visibly and are retried next day.
    player_url = NFLVERSE_WEEKLY_STATS_URL.format(season=season)
    team_url = NFLVERSE_WEEKLY_TEAM_STATS_URL.format(season=season)
    players, player_digest = fetch_partial(player_url, season, team=False)
    teams, team_digest = fetch_partial(team_url, season, team=True)
    players = players[players["game_id"].isin(completed["game_id"])]
    teams = teams[teams["game_id"].isin(completed["game_id"])]
    writer = BatchedWeeklyWriter(db.database_url)
    try:
        universe = [{"player_id": r["id"], "gsis_id": r["gsis_id"],
                     "name": r["canonical_name"], "position": r["position"], "team": r["team_abbrev"]}
                    for r in writer.execute("""SELECT id,gsis_id,canonical_name,position,team_abbrev
                        FROM ff_players WHERE season=%s AND position=ANY(%s)""",
                        (season, list(WEEKLY_STAT_POSITIONS) + ["DST"]))]
        if not universe:
            raise ValueError("No canonical current-season player universe")
        player_count = save_weekly_history(writer, universe, season, players)
        dst_count = save_dst_weekly_history(writer, universe, season, teams, schedule)
        if not player_count or not dst_count:
            raise ValueError("No matching skill-player or DST results; investigate identity coverage")
        writer.flush()
        for dataset, frame, digest, url in (
            ("player-week-stats", players, player_digest, player_url),
            ("team-week-stats", teams, team_digest, team_url),
        ):
            _snapshot(writer, source="nflverse", dataset=dataset, season=season,
                      digest=digest, row_count=len(frame), params={"url": url, "use": "daily DFS outcomes"})
        writer.close()
    except Exception:
        writer.close(error=True)
        raise
    return {"season": season, "status": "refreshed", "completed_games": len(completed),
            "player_weeks": player_count, "dst_weeks": dst_count,
            "unmatched_eligible_player_rows": int(players["position"].isin(WEEKLY_STAT_POSITIONS).sum()) - player_count,
            "completed_games_missing_team_feed": len(set(completed["game_id"]) - set(teams["game_id"])),
            "results": materialize(db, [season])}


def refresh_projections(db, season: int, week: int | None) -> dict:
    from ingest.nfl_dfs_projections import infer_target_week, build_week, persist_week
    if week is None:
        upcoming = db.execute_one("""SELECT COUNT(*)::int n FROM nfl_season_games
            WHERE season=%s AND game_type='REG' AND kickoff > NOW()""", (season,))
        if not upcoming["n"]:
            return {"status": "no_upcoming_regular_season_games", "season": season}
        week = infer_target_week(db, season)
    rows, manifest = build_week(db, season=season, week=week,
                               as_of_at=datetime.now(timezone.utc), seed=20260902)
    return {"run_id": persist_week(db, rows, manifest), "players": len(rows), "season": season, "week": week}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("results", "projections"), required=True)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    args = parser.parse_args()
    season = target_season(args.season, datetime.now(timezone.utc))
    db = PipelineDatabase(load_config().database_url)
    result = refresh_results(db, season) if args.phase == "results" else refresh_projections(db, season, args.week)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
