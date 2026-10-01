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
from model.nfl_dfs_context_variant_study import context_forecasts


def inputs(db, season, week, now):
    games = db.execute("""SELECT g.id,g.kickoff,g.completed,h.abbreviation home_team,a.abbreviation away_team
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
        JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.week=%s AND g.game_type='REG'""", (season, week))
    players = db.execute("""SELECT id player_id,gsis_id,canonical_name name,position,team_abbrev team
        FROM ff_players WHERE season=%s AND active AND position IN ('QB','RB','WR','TE','DST')""", (season,))
    # Only the forecast `build_report` keeps: the last capture before the
    # player's kickoff (ties broken by forecast id, as there). Loading every run
    # killed the runner from 2026-09-30: the 15-minute pre-kickoff cadence put
    # 88 runs (323 MB) in week 3 and 52 runs (1.5 GB) in week 4 by Wednesday.
    # Rows this drops are counted, so `rejected_non_pregame_snapshots` and the
    # report digest are unchanged; superseded pregame rows were never counted.
    production_sql = """WITH g AS (SELECT g.id game_id,g.kickoff,h.abbreviation home_team,a.abbreviation away_team
            FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
            JOIN nfl_teams a ON a.team_id=g.away_team_id
            WHERE g.season=%(season)s AND g.week=%(week)s AND g.game_type='REG'),
        f AS (SELECT p.id,p.player_id,p.team,GREATEST(p.created_at,r.created_at,r.as_of_at) captured_at
            FROM nfl_dfs_player_projections p JOIN nfl_dfs_projection_runs r ON r.run_id=p.run_id
            WHERE r.season=%(season)s AND r.week=%(week)s AND p.player_id IS NOT NULL
              AND p.position IN ('QB','RB','WR','TE','DST')),
        m AS (SELECT f.*,g.game_id,g.kickoff FROM f LEFT JOIN LATERAL (
            SELECT * FROM g WHERE f.team IN (g.home_team,g.away_team) ORDER BY g.kickoff,g.game_id LIMIT 1) g ON TRUE)"""
    rejected = db.execute(production_sql + """
        SELECT count(*) n FROM m WHERE game_id IS NULL OR captured_at IS NULL OR kickoff IS NULL
            OR captured_at >= kickoff OR captured_at > %(now)s""", {"season": season, "week": week, "now": now})
    production = db.execute(production_sql + """,
        pick AS (SELECT DISTINCT ON (player_id,game_id) id FROM m
            WHERE game_id IS NOT NULL AND captured_at < kickoff AND captured_at <= %(now)s
            ORDER BY player_id,game_id,captured_at DESC,id::text COLLATE "C" DESC)
        SELECT p.*,r.model_version,r.model_config,r.seed,r.artifact_digest,
            GREATEST(p.created_at,r.created_at,r.as_of_at) captured_at
        FROM pick JOIN nfl_dfs_player_projections p ON p.id=pick.id
        JOIN nfl_dfs_projection_runs r ON r.run_id=p.run_id""", {"season": season, "week": week, "now": now})
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
        forecasts.extend(context_forecasts(base, p))
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
    return dict(games=games, players=players, forecasts=forecasts, results=results,
                prior_rejected=int(rejected[0]["n"]))


def same_content(previous, report):
    """True when two reports differ only in when they were evaluated."""
    if previous is None:
        return False
    current = json.loads(json.dumps(report, default=str))
    return ({k: v for k, v in previous.items() if k != "evaluated_at"}
            == {k: v for k, v in current.items() if k != "evaluated_at"})


def persist(db, report):
    # The UI and review digest read one latest report per week. A report that
    # differs from the newest stored one only in `evaluated_at` is not written:
    # each is ~7 MB and the research job runs several times a day, so repeats
    # cost 80-200 MB a day by 2026-09-30. Returns the stored digest either way.
    digest = artifact_digest(report)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT report_digest,payload FROM nfl_dfs_weekly_report_cards
                WHERE season=%s AND week=%s ORDER BY created_at DESC, report_digest DESC LIMIT 1""",
                (report["season"], report["week"]))
            latest = cursor.fetchone()
            if latest and same_content(latest["payload"], report):
                return latest["report_digest"]
            cursor.execute("""INSERT INTO nfl_dfs_weekly_report_cards(report_digest,season,week,payload)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (digest, report["season"], report["week"], Json(report, dumps=lambda x: json.dumps(x, default=str))))
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--dry-run", action="store_true", help="read existing ledgers without schema changes or persistence")
    parser.add_argument("--output", type=Path, help="save the report locally (requires --week)")
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    if args.output and not args.week:
        parser.error("--output requires --week")
    db = PipelineDatabase(load_config().database_url, initialize_schema=not args.dry_run)
    weeks = [args.week] if args.week else [r["week"] for r in db.execute("""SELECT DISTINCT week FROM (
        SELECT week FROM nfl_dfs_projection_runs WHERE season=%s
        UNION SELECT week FROM nfl_dfs_shadow_predictions WHERE season=%s
        UNION SELECT week FROM nfl_season_games WHERE season=%s AND completed
        ) weeks WHERE week IS NOT NULL ORDER BY week""", (season, season, season))]
    for week in weeks:
        report = build_report(season=season, week=week, now=now, **inputs(db, season, week, now))
        report["implementation"] = {p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
                                    for p in ("model/nfl_dfs_reportcard.py", "ingest/nfl_dfs_reportcard.py")}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        digest = artifact_digest(report) if args.dry_run else persist(db, report)
        print(json.dumps({"season": season, "week": week, "digest": digest, "summary": report["summary"]}))


if __name__ == "__main__":
    main()
