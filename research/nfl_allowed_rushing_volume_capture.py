"""Freeze the registered allowed-rushing-volume distribution for one saved slate.

This is an append-only experimental consumer artifact. It never edits the
historical projection or the compact context-variant study payload.
"""
from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from db.queries import DatabaseManager
from ingest.nfl_dfs_projections import _history
from ingest.nfl_dfs_workload import raw_history
from ingest.nfl_matchup_context import persist_forecasts
from model.nfl_context_engine import stable_digest
from model.nfl_dfs_environment_variants import rush_factor
from model.nfl_dfs_historical import draftkings_points
from model.nfl_dfs_workload_opponent import Prior, plays_faced
from model.nfl_matchup_projection import sample_baseline_draws, summarize_draws, reproduction_check, saved_summary

VERSION = "nfl-allowed-rushing-volume-v1"
SUPPORTED = {"QB", "RB", "WR", "TE"}
TEAM_ALIASES = {"WSH": "WAS", "LA": "LAR", "JAC": "JAX", "AZ": "ARI"}


def salary_team(value):
    return TEAM_ALIASES.get(value, value)


def frozen_volume_projection(player, draws, rush, game_id):
    position = player["position"]
    baseline = summarize_draws(position, draws)
    replay = reproduction_check(player, baseline)
    saved = saved_summary(player)
    result = {"version": VERSION, "status": "not_applied", "authority": "shadow_only",
              "baseline": saved, "candidate": saved, "delta": 0.0, "ledger": [],
              "reproduction": replay, "model_artifact_hash": stable_digest({"version": VERSION, "prior": "nfl-dfs-workload-opponent-study-v1", "weight": 0.5, "clamp": [0.5, 1.5]}),
              "matchup_manifest_hash": stable_digest({"game_id": game_id, "rush": rush})}
    reason = ("ineligible_or_out" if player.get("is_out") or player.get("projection_status") == "out" else
              "no_registered_effect_for_position" if position not in SUPPORTED else
              "no_baseline_draws" if not draws else
              "saved_baseline_not_reproduced_or_availability_adjusted" if not replay["passed"] else
              "missing_allowed_carries_history" if not rush else
              "zero_rushing_volume_effect" if rush["factor"] == 1 else None)
    if reason:
        result["reason"] = reason
        return result
    factor = rush["factor"]
    candidate_draws = deepcopy(draws)
    for draw in candidate_draws:
        for field in ("rushing_yards", "rushing_tds"):
            if field in draw:
                draw[field] *= factor
    candidate = summarize_draws(position, candidate_draws)
    result.update(status="under_evaluation", reason="prospective_gate_not_yet_scorable",
                  baseline=baseline, candidate=candidate, delta=candidate["mean"] - baseline["mean"])
    result["ledger"] = [{"component": "rushing_yards_and_tds", "factor": factor,
                         "points_delta": result["delta"], "inputs": rush,
                         "unchanged": "carries, passing, receiving, K, DST"}]
    return result


def capture(db, upload_id, now):
    upload = db.execute_one("SELECT * FROM nfl_dfs_slate_uploads WHERE upload_id=%s", (upload_id,))
    if not upload or not upload["projection_run_id"]:
        raise ValueError("A saved salary upload with a pinned projection run is required")
    run = db.execute_one("SELECT * FROM nfl_dfs_projection_runs WHERE run_id=%s", (upload["projection_run_id"],))
    if not run or run["model_version"] != "nfl-dfs-historical-v5":
        raise ValueError("Allowed rushing volume requires the exact historical-v5 baseline")
    salary = [dict(r) for r in db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY id", (upload_id,))]
    game_rows = db.execute("""SELECT g.season,g.week,g.kickoff,h.abbreviation home,a.abbreviation away
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
        JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.week=%s AND g.game_type='REG'""", (run["season"], run["week"]))
    games = {team: {"game_id": f'{g["season"]}_{g["week"]:02d}_{salary_team(g["away"])}_{salary_team(g["home"])}',
                    "kickoff": g["kickoff"], "opponent": salary_team(g["away"] if team == g["home"] else g["home"])}
             for g in game_rows for team in (g["home"], g["away"])}
    games = {salary_team(team): game for team, game in games.items()}
    invalid = [(s["name"], s["team"], games.get(s["team"], {}).get("kickoff")) for s in salary
               if s["team"] not in games or games[s["team"]]["kickoff"] <= now]
    if invalid:
        raise ValueError(f"Every salary game must be strictly pregame: {invalid[:5]}")
    projections = {int(r["player_id"]): dict(r) for r in db.execute(
        "SELECT * FROM nfl_dfs_player_projections WHERE run_id=%s", (run["run_id"],))}
    history = _history(db, run["season"], run["week"])
    _players, team_history = raw_history(db)
    prior = Prior(team_history, plays_faced(db), (run["season"], run["week"]))
    rush_by_team = {team: rush_factor(prior, team, game["opponent"]) for team, game in games.items()}
    players = []
    for s in salary:
        p = projections.get(s["ff_player_id"])
        if not p:
            continue
        game = games[s["team"]]
        p.update(is_out=s["is_out"], season=run["season"], week=run["week"])
        draws = sample_baseline_draws(p, history, config=run["model_config"], seed=run["seed"])
        shadow = frozen_volume_projection(p, draws, rush_by_team[s["team"]], game["game_id"])
        players.append({"player_id": p["player_id"], "dk_player_id": s["dk_player_id"],
                        "game_id": game["game_id"], "kickoff": game["kickoff"].isoformat(),
                        "baseline": p, "shadow": shadow})
    artifact = {"version": VERSION, "season": run["season"], "week": run["week"],
                "as_of_at": now.isoformat(), "upload_id": str(upload_id),
                "baseline_run_id": str(run["run_id"]), "baseline_version": run["model_version"],
                "baseline_config_hash": stable_digest({"model_version": run["model_version"], "model_config": run["model_config"]}),
                "source_manifest_hash": stable_digest({"team_rush": rush_by_team, "slate_player_ids": [s["id"] for s in salary]}),
                "players": players, "production_changed": False}
    return json.loads(json.dumps(artifact, default=str))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upload-id", required=True)
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        artifact = capture(db, args.upload_id, datetime.now(timezone.utc))
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
        result = persist_forecasts(db, artifact) if args.persist else None
    print(json.dumps({"players": len(artifact["players"]),
                      "adjusted": sum(p["shadow"]["status"] == "under_evaluation" for p in artifact["players"]),
                      "persisted": result}))


if __name__ == "__main__":
    main()
