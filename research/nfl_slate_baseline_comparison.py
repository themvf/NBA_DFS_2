"""Adapt a saved Classic/Showdown salary slate and exact baseline for scenario research.

This supplies no unqualified matchup adjustment. Live default uses the exact
slate projection run; --saved-optimizer-run uses its immutable pre-lock roster.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from config import load_config
from db.database import DatabaseManager
from research.nfl_archived_contests import lock_at
from model.nfl_pfr_supplement import team_code


def capture_comparison(db, *, upload_id, run_id=None, saved_optimizer_run=None):
    with db.reuse_connection():
        upload = db.execute("SELECT * FROM nfl_dfs_slate_uploads WHERE upload_id=%s", (upload_id,))[0]
        salaries = db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY dk_player_id", (upload_id,))
        saved = db.execute("SELECT * FROM nfl_dfs_optimizer_runs WHERE run_id=%s AND upload_id=%s", (saved_optimizer_run, upload_id))[0] if saved_optimizer_run else None
        run_id = run_id or (saved["projection_run_id"] if saved else upload["projection_run_id"])
        run = db.execute("SELECT * FROM nfl_dfs_projection_runs WHERE run_id=%s", (run_id,))[0]
        projections = {p["player_id"]: p for p in db.execute("SELECT * FROM nfl_dfs_player_projections WHERE run_id=%s", (run_id,))}
        games = db.execute("""SELECT g.nflverse_game_id game_id,h.abbreviation home_team,a.abbreviation away_team,g.kickoff
            FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
            WHERE g.season=%s AND g.week=%s""", (run["season"], run["week"]))
    frozen = {p["dkPlayerId"]: p for p in saved["input_snapshot"]} if saved else None
    if saved and saved["created_at"] >= lock_at(salaries):
        raise ValueError("Saved optimizer run was created after lock")
    if not saved and lock_at(salaries) <= datetime.now(timezone.utc):
        raise ValueError("No current pregame slate")
    game_map = {f"{team_code(g['away_team'])}@{team_code(g['home_team'])}": g for g in games}
    rows, missing, retained_salary = [], [], []
    for salary in salaries:
        snapshot = frozen.get(salary["dk_player_id"]) if frozen is not None else None
        if frozen is not None and snapshot is None:
            continue
        baseline = projections.get(snapshot.get("ffPlayerId") if snapshot else salary["ff_player_id"])
        game = game_map.get("@".join(team_code(t) for t in (salary.get("game_key") or "").split("@")))
        if not baseline or not game or baseline.get("model_proj_fpts") is None:
            missing.append({"dk_player_id": salary["dk_player_id"], "reason": "baseline_or_game_missing"})
            continue
        if snapshot:
            salary = {**salary, "is_out": snapshot["isOut"], "salary": snapshot["salary"], "captain_salary": snapshot["captainSalary"]}
        baseline = {**baseline, "is_out": salary["is_out"]}
        retained_salary.append(salary)
        rows.append({"player_id": baseline["player_id"], "name": salary["name"], "position": salary["position"], "team": salary["team"],
            "salary": salary["salary"], "dk_player_id": salary["dk_player_id"], "slate_player_id": salary["id"], "game_id": game["game_id"],
            "kickoff": game["kickoff"].isoformat(), "baseline": baseline,
            "shadow": {"status": "baseline_preserved", "ledger": [], "delta": 0, "candidate": {"mean": baseline["model_proj_fpts"]}}})
    result = {"version": "nfl-slate-baseline-scenario-adapter-v1", "season": run["season"], "week": run["week"],
        "upload_id": upload_id, "file_name": upload["file_name"], "format": upload["format"], "baseline_run_id": str(run_id),
        "baseline_version": run["model_version"], "baseline_as_of_at": run["as_of_at"], "as_of_at": datetime.now(timezone.utc).isoformat(),
        "salary_rows": len(salaries), "salary_snapshot": retained_salary, "players": rows, "skipped": missing,
        "production_changed": False, "forecast_state": "retrospective_development" if saved else "baseline_scenario_research",
        "saved_optimizer_run_id": saved_optimizer_run, "saved_decision_at": saved["created_at"] if saved else None}
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--upload", required=True)
    ap.add_argument("--run")
    ap.add_argument("--saved-optimizer-run")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    result = capture_comparison(db, upload_id=args.upload, run_id=args.run, saved_optimizer_run=args.saved_optimizer_run)
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, default=str, indent=2), encoding="utf-8")
    print(json.dumps({"format": result["format"], "players": len(result["players"]), "baseline": result["baseline_run_id"], "mode": result["forecast_state"]}))


if __name__ == "__main__":
    main()
