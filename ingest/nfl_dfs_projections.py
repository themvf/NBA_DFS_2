"""Build and persist the prop-free NFL DFS historical baseline.

This job projects canonical ``ff_players`` for one NFL week.  The later DK
salary upload resolves its rows to these canonical identities; unmatched and
ambiguous names are never guessed here.  Re-running identical inputs produces
the same artifact digest/run id.

    python -m ingest.nfl_dfs_projections --season 2026 --week 1
"""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json, execute_values

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_historical import (
    MODEL_CONFIG,
    MODEL_VERSION,
    HistoricalProjection,
    HistoricalWeek,
    ProjectionContext,
    artifact_digest,
    project_player,
)


RUN_NAMESPACE = uuid.UUID("8abcf42c-6a0d-49cc-9f34-1ad6f17b0d77")


def infer_target_week(db: DatabaseManager, season: int) -> int:
    """Select the next scheduled regular-season week from the canonical schedule."""
    rows = db.execute(
        """SELECT week, MIN(kickoff) first_kickoff
           FROM nfl_season_games
           WHERE season=%s AND game_type='REG'
             AND kickoff >= NOW() - INTERVAL '6 hours'
           GROUP BY week
           ORDER BY first_kickoff, week
           LIMIT 1""",
        (season,),
    )
    if not rows:
        raise ValueError(f"No current or upcoming regular-season week is loaded for {season}")
    return int(rows[0]["week"])


def _history(db: DatabaseManager, season: int, week: int | None) -> list[HistoricalWeek]:
    rows = db.execute(
        """SELECT w.player_id, p.gsis_id, p.canonical_name, p.position,
                  w.season, w.week, w.team, w.opponent, w.source_row
           FROM ff_player_week_stats w
           JOIN ff_players p ON p.id=w.player_id
           WHERE w.season_type='REG'
             AND (w.season < %s OR (w.season=%s AND %s IS NOT NULL AND w.week < %s))
           ORDER BY w.season,w.week,w.player_id""",
        (season, season, week, week),
    )
    return [HistoricalWeek(
        player_id=int(row["player_id"]),
        player_gsis_id=row["gsis_id"],
        player_name=row["canonical_name"],
        position=row["position"],
        season=int(row["season"]),
        week=int(row["week"]),
        team=row["team"],
        opponent=row["opponent"],
        stats=row["source_row"] or {},
    ) for row in rows]


def _slate_environment(db: DatabaseManager, season: int, week: int | None) -> dict[str, dict[str, Any]]:
    if week is None:
        raise ValueError("--week is required: nfl_season_games is the authoritative slate schedule")
    rows = db.execute(
        """SELECT home.abbreviation home_team, away.abbreviation away_team,
                  m.home_implied, m.away_implied,
                  COALESCE(m.vegas_total,g.quoted_total_line) vegas_total,
                  COALESCE(m.home_spread,g.quoted_spread_line) home_spread,
                  m.event_id, COALESCE(m.commence_time,g.kickoff) commence_time
           FROM nfl_season_games g
           JOIN nfl_teams home ON home.team_id=g.home_team_id
           JOIN nfl_teams away ON away.team_id=g.away_team_id
           LEFT JOIN nfl_matchups m ON m.id=g.matchup_id
           WHERE g.season=%s AND g.week=%s AND g.game_type='REG'""",
        (season, week),
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        home_implied = row["home_implied"]
        away_implied = row["away_implied"]
        if (home_implied is None or away_implied is None) and row["vegas_total"] is not None and row["home_spread"] is not None:
            home_implied = (float(row["vegas_total"]) - float(row["home_spread"])) / 2.0
            away_implied = float(row["vegas_total"]) - home_implied
        common = {"event_id": row["event_id"], "commence_time": row["commence_time"]}
        result[row["home_team"]] = {**common, "opponent": row["away_team"], "team_implied_total": home_implied}
        result[row["away_team"]] = {**common, "opponent": row["home_team"], "team_implied_total": away_implied}
    return result


def _players(db: DatabaseManager, season: int, teams: list[str]) -> list[dict[str, Any]]:
    if not teams:
        return []
    return db.execute(
        """SELECT id,gsis_id,canonical_name,normalized_name,position,team_abbrev
           FROM ff_players
           WHERE season=%s AND active AND team_abbrev=ANY(%s)
             AND position=ANY(%s)
           ORDER BY position,canonical_name""",
        (season, teams, ["QB", "RB", "WR", "TE", "K", "DST"]),
    )


def build_week(
    db: DatabaseManager,
    *,
    season: int,
    week: int | None,
    as_of_at: datetime,
    seed: int,
    config: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    model_config = {**MODEL_CONFIG, **(config or {})}
    history = _history(db, season, week)
    environment = _slate_environment(db, season, week)
    players = _players(db, season, sorted(environment))
    projections: list[dict[str, Any]] = []
    for player in players:
        env = environment[player["team_abbrev"]]
        projection: HistoricalProjection = project_player(
            player_id=int(player["id"]),
            player_gsis_id=player["gsis_id"],
            player_name=player["canonical_name"],
            position=player["position"],
            historical_rows=history,
            cutoff_season=season,
            cutoff_week=week,
            context=ProjectionContext(team_implied_total=env["team_implied_total"]),
            seed=seed,
            config=model_config,
        )
        projections.append({
            **projection.as_dict(),
            "normalized_name": player["normalized_name"],
            "team": player["team_abbrev"],
            "opponent": env["opponent"],
            "event_id": env["event_id"],
            "commence_time": env["commence_time"],
        })
    snapshot_rows = db.execute(
        """SELECT DISTINCT ON (season,dataset)
                  id,response_hash,season,dataset
           FROM ff_source_snapshots
           WHERE source='nflverse' AND season <= %s
             AND dataset IN ('weekly-player-stats','player-week-stats')
           ORDER BY season,dataset,fetched_at DESC,id DESC""",
        (season,),
    )
    source_evidence = [dict(row) for row in snapshot_rows]
    manifest = {
        "model_version": MODEL_VERSION,
        "season": season,
        "week": week,
        "seed": seed,
        "model_config": model_config,
        "history_rows": len(history),
        "source_evidence": source_evidence,
        "projections": projections,
        "prop_inputs": [],
    }
    manifest["artifact_digest"] = artifact_digest(manifest)
    manifest["as_of_at"] = as_of_at.isoformat()
    return projections, manifest


def persist_week(db: DatabaseManager, projections: list[dict[str, Any]], manifest: dict[str, Any]) -> str:
    digest = manifest["artifact_digest"]
    run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{MODEL_VERSION}:{digest}"))
    source_ids = [row["id"] for row in manifest["source_evidence"]]
    as_of_at = datetime.fromisoformat(manifest["as_of_at"])
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO nfl_dfs_projection_runs
               (run_id,model_version,scoring,season,week,as_of_at,seed,
                history_cutoff_season,history_cutoff_week,source_snapshot_ids,
                model_config,player_count,artifact_digest)
               VALUES (%s,%s,'DK',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(model_version,artifact_digest) DO NOTHING""",
            (run_id, MODEL_VERSION, manifest["season"], manifest["week"], as_of_at,
             manifest["seed"], manifest["season"], manifest["week"], Json(source_ids),
             Json(manifest["model_config"]), len(projections), digest),
        )
        values = []
        for row in projections:
            evidence = {
                "source_snapshot_ids": source_ids,
                "event_id": row["event_id"],
                "commence_time": row["commence_time"].isoformat() if row["commence_time"] else None,
                "prop_inputs": [],
            }
            values.append((
                run_id, row["player_id"], row["player_gsis_id"], row["player_name"],
                row["normalized_name"], row["team"], row["opponent"], row["position"],
                "gsis_id" if row["player_gsis_id"] else "exact_name_position_team",
                row["projection_status"], row["history_games"], row["prior_games"],
                row["model_proj_fpts"], row["baseline_fpts"], row["floor_fpts"],
                row["median_fpts"], row["ceiling_fpts"], row["boom_rate"], row["confidence"],
                Json(row["stat_means"]), Json(row["feature_snapshot"]), Json(evidence),
            ))
        if values:
            execute_values(cur, """INSERT INTO nfl_dfs_player_projections
                (run_id,player_id,player_gsis_id,player_name,normalized_name,team,opponent,
                 position,identity_method,projection_status,history_games,prior_games,
                 model_proj_fpts,baseline_fpts,floor_fpts,median_fpts,ceiling_fpts,
                 boom_rate,confidence,stat_means,feature_snapshot,source_evidence)
                VALUES %s ON CONFLICT(run_id,player_id) DO NOTHING""", values)
    return run_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--week", type=int)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--no-persist", action="store_true")
    args = parser.parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url)
    week = args.week if args.week is not None else infer_target_week(db, args.season)
    projections, manifest = build_week(
        db, season=args.season, week=week,
        as_of_at=datetime.now(timezone.utc), seed=args.seed,
    )
    run_id = None if args.no_persist else persist_week(db, projections, manifest)
    counts: dict[str, int] = {}
    for row in projections:
        counts[row["projection_status"]] = counts.get(row["projection_status"], 0) + 1
    print(json.dumps({
        "run_id": run_id,
        "artifact_digest": manifest["artifact_digest"],
        "season": args.season,
        "week": week,
        "players": len(projections),
        "status_counts": counts,
        "prop_inputs": [],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
