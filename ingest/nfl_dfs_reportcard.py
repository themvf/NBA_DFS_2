"""Persist auditable weekly production/shadow report cards, without reforecasting."""
import argparse
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone

from psycopg2.extras import Json
from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase, target_season
from model.nfl_dfs_historical import artifact_digest
from model.nfl_dfs_reportcard import build_report


def inputs(db, season, week):
    games = db.execute("""SELECT g.id,g.kickoff,g.completed,h.abbreviation home_team,a.abbreviation away_team
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
        JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.week=%s AND g.game_type='REG'""", (season, week))
    players = db.execute("""SELECT id player_id,gsis_id,canonical_name name,position,team_abbrev team
        FROM ff_players WHERE season=%s AND active AND position IN ('QB','RB','WR','TE','DST')""", (season,))
    production = db.execute("""SELECT p.*,r.model_version,r.model_config,r.seed,r.artifact_digest,
        GREATEST(p.created_at,r.created_at,r.as_of_at) captured_at
        FROM nfl_dfs_player_projections p JOIN nfl_dfs_projection_runs r ON r.run_id=p.run_id
        WHERE r.season=%s AND r.week=%s AND p.position IN ('QB','RB','WR','TE','DST')""", (season, week))
    forecasts = [{"player_id": p["player_id"], "forecast_id": str(p["id"]), "variant": "production",
        "name": p["player_name"], "team": p["team"], "position": p["position"], "captured_at": p["captured_at"],
        "mean": p["model_proj_fpts"], "median": p["median_fpts"], "p10": p["floor_fpts"], "p90": p["ceiling_fpts"],
        "boom_probability": p["boom_rate"], "history_games": p["history_games"], "stat_means": p["stat_means"],
        "model_version": p["model_version"], "run_id": str(p["run_id"]), "input_digest": p["artifact_digest"],
        "source_evidence": p["source_evidence"], "config": p["model_config"], "seed": p["seed"],
        "feature_snapshot": p["feature_snapshot"]} for p in production if p["player_id"] is not None]
    shadows = db.execute("""SELECT p.*,f.team_abbrev current_team FROM nfl_dfs_shadow_predictions p
        JOIN ff_players f ON f.id=p.player_id WHERE p.season=%s AND p.week=%s""", (season, week))
    for s in shadows:
        p = s["payload"]
        # Legacy payloads lacked team; kickoff + current team is a qualified
        # fallback, never an arbitrary pairing after a roster move.
        matches = [g for g in games if g["kickoff"] == s["kickoff"] and s["current_team"] in (g["home_team"], g["away_team"])]
        team = p.get("team") or (s["current_team"] if len(matches) == 1 else None)
        base = {"player_id": s["player_id"], "forecast_id": str(s["id"]), "name": p["player_name"],
            "team": team, "position": p["position"], "captured_at": s["captured_at"],
            "history_games": p["history_games"], "run_id": s["study_run_id"], "input_digest": s["input_digest"],
            "source_evidence": {"history_digest": p["history_digest"], "study_digest": p["source_study_digest"],
                                "identity": "frozen_team" if p.get("team") else "legacy_current_team_plus_exact_kickoff"},
            "seed": p.get("seed"), "config": p.get("baseline_config"),
            "model_version": p.get("shadow_version", "shadow-v1")}
        forecasts.append({**base, "variant": "shadow_baseline", "mean": p["baseline"], "median": p.get("median"),
            "p10": p["p10"], "p90": p["p90"], "boom_probability": p["boom_probability"], "stat_means": p.get("stat_means", {})})
        if p.get("candidate"):
            c = p["candidate"]
            forecasts.append({**base, "variant": "opportunity", "mean": c["prediction"], "median": c.get("median"),
                "p10": c["p10"], "p90": c["p90"], "boom_probability": c["boom_probability"],
                "stat_means": {}, "recipe_digest": c["recipe_digest"]})
    identities = {str(player["gsis_id"]): player["player_id"] for player in players if player.get("gsis_id")}
    identities.update({f"DST:{player['team']}": player["player_id"] for player in players if player["position"] == "DST"})
    efficiency_runs = db.execute("""SELECT run_digest,as_of_at,payload FROM nfl_dfs_efficiency_runs
        WHERE season=%s AND week=%s ORDER BY as_of_at,run_digest""", (season, week))
    for run in efficiency_runs:
        payload = run["payload"]
        for team_forecast in payload.get("forecasts", []):
            for projection in team_forecast.get("players", []):
                player_id = identities.get(str(projection.get("identity")))
                if player_id is None:
                    continue
                forecasts.append({
                    "player_id": player_id, "forecast_id": f"{run['run_digest']}:{projection.get('identity')}",
                    "variant": "efficiency_research", "name": projection["name"], "team": team_forecast["team"],
                    "position": projection["position"], "captured_at": run["as_of_at"],
                    "mean": projection["mean_fpts"], "median": projection["median_fpts"],
                    "p10": projection["p10_fpts"], "p90": projection["p90_fpts"],
                    "boom_probability": projection["boom_rate"], "history_games": projection["history_games"],
                    "stat_means": projection["stat_means"], "model_version": payload["version"],
                    "run_id": run["run_digest"], "input_digest": payload["dataset_digest"],
                    "source_evidence": {"workload_run_digest": payload["workload_run_digest"],
                        "coherence_scope": projection["coherence_scope"]}, "config": payload["config"],
                    "seed": projection["seed"],
                })
    results = db.execute("""SELECT * FROM nfl_dfs_player_week_results WHERE season=%s AND week=%s""", (season, week))
    return dict(games=games, players=players, forecasts=forecasts, results=results)


def persist(db, report):
    # A new observation records when coverage was checked, even if outcomes
    # are unchanged. Re-persisting this exact report is idempotent; the UI
    # reads one latest report per week, never sums observations as samples.
    digest = artifact_digest(report)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""INSERT INTO nfl_dfs_weekly_report_cards(report_digest,season,week,payload)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (digest, report["season"], report["week"], Json(report, dumps=lambda x: json.dumps(x, default=str))))
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    db = PipelineDatabase(load_config().database_url)
    weeks = [args.week] if args.week else [r["week"] for r in db.execute("""SELECT DISTINCT week FROM (
        SELECT week FROM nfl_dfs_projection_runs WHERE season=%s
        UNION SELECT week FROM nfl_dfs_shadow_predictions WHERE season=%s
        UNION SELECT week FROM nfl_season_games WHERE season=%s AND completed
        ) weeks WHERE week IS NOT NULL ORDER BY week""", (season, season, season))]
    for week in weeks:
        report = build_report(season=season, week=week, now=now, **inputs(db, season, week))
        report["implementation"] = {p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
                                    for p in ("model/nfl_dfs_reportcard.py", "ingest/nfl_dfs_reportcard.py")}
        digest = persist(db, report)
        print(json.dumps({"season": season, "week": week, "digest": digest, "summary": report["summary"]}))


if __name__ == "__main__":
    main()
