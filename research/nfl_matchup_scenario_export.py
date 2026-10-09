"""Export audited v5 score marginals for a real saved slate (read-only DB).

This is an explicitly independent-player construction diagnostic. It never
claims coherent game scenarios or changes production projections. Original
source replays need the source ledger; a read of today's history is labeled
current_source_replay even when the frozen mean reproduces exactly.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import json
from copy import deepcopy

import numpy as np

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_projections import _history
from model.nfl_context_engine import stable_digest
from model.nfl_dfs_historical import draftkings_points
from model.nfl_matchup_projection import sample_baseline_draws

VERSION = "nfl-baseline-marginal-diagnostic-v1"


def build_score_banks(slate_players, projections, history, run, upload, *, captured_at, comparison=None):
    """Strict identity join and exact original-seed mean audit before inclusion."""
    by_id = {row["player_id"]: row for row in projections if row.get("player_id") is not None}
    config = run["model_config"]
    seeds = [int(run["seed"]), int(run["seed"]) ^ 0xA5A5A5A5]
    count = int(config["draws"])
    if count < 2 or count > 10000:
        raise ValueError("unsupported draw count")
    source_hash = stable_digest({"upload": upload, "players": slate_players, "projections": projections,
                                 "history": [asdict(row) for row in history], "config": config})
    scores = [{}, {}]
    shadow_scores = [{}, {}]
    shadow_rows = {str(row["player_id"]): row for row in (comparison or {}).get("players", [])}
    if comparison and str(comparison.get("baseline_run_id")) != str(run["run_id"]):
        raise ValueError("comparison baseline run differs")
    players, optimizer, audits, excluded = [], [], [], []
    for row in slate_players:
        player = by_id.get(row.get("ff_player_id"))
        why = None
        if not player:
            why = "unresolved_projection_identity"
        elif row.get("is_out") or player.get("projection_status") == "out":
            why = "ineligible_or_confirmed_out"
        elif player.get("model_proj_fpts") is None:
            why = "missing_baseline"
        elif int(player.get("history_games") or 0) < 2:
            why = "insufficient_own_history_for_research_candidate_pool"
        elif row["position"] != player["position"] or row["team"] != player["team"]:
            why = "identity_team_or_position_conflict"
        if why:
            excluded.append({"dkPlayerId": row["dk_player_id"], "name": row["name"], "reason": why})
            continue
        original = sample_baseline_draws(player, history, config=config, seed=seeds[0])
        original_scores = [draftkings_points(player["position"], d) for d in original]
        difference = abs(float(np.mean(original_scores)) - float(player["model_proj_fpts"])) if original_scores else float("inf")
        quantile_drift = max((abs(float(np.quantile(original_scores, q)) - float(player[key])) for q, key in
                              ((.1, "floor_fpts"), (.5, "median_fpts"), (.9, "ceiling_fpts")) if player.get(key) is not None), default=0) if original_scores else float("inf")
        stat_drift = max((abs(float(np.mean([d.get(key, 0) for d in original])) - float(value))
                          for key, value in player.get("stat_means", {}).items() if isinstance(value, (float, int))), default=0) if original_scores else float("inf")
        if not original_scores or any(player.get(key) is None for key in ("floor_fpts", "median_fpts", "ceiling_fpts")) or difference > .00011 or quantile_drift > .00011 or stat_drift > .00011:
            excluded.append({"dkPlayerId": row["dk_player_id"], "name": row["name"], "reason": "saved_baseline_not_reproduced", "meanDifference": difference if original_scores else None})
            continue
        second = sample_baseline_draws(player, history, config=config, seed=seeds[1])
        key = str(row["dk_player_id"])
        scores[0][key] = original_scores
        scores[1][key] = [draftkings_points(player["position"], d) for d in second]
        shadow = shadow_rows.get(str(player["player_id"]), {}).get("shadow") or {}
        ledger = shadow.get("ledger") or []
        if shadow.get("status") != "under_evaluation":
            ledger = []
        for i, base_draws in enumerate((original, second)):
            changed = deepcopy(base_draws)
            for step in ledger:
                field, factor = step["component"], float(step["factor"])
                if field not in ("passing_yards", "rushing_yards") or not .9 - 1e-12 <= factor <= 1.1 + 1e-12 or step.get("status") != "shadow_only":
                    raise ValueError("unsupported or unqualified shadow transformation")
                for draw in changed:
                    draw[field] *= factor
            shadow_scores[i][key] = [draftkings_points(player["position"], d) for d in changed]
        if ledger and abs(float(np.mean(shadow_scores[0][key])) - float(shadow["candidate"]["mean"])) > .001:
            raise ValueError("shadow ledger does not reproduce the frozen candidate mean")
        audits.append({"dkPlayerId": row["dk_player_id"], "playerId": player["player_id"], "name": row["name"], "position": row["position"],
                       "savedMean": player["model_proj_fpts"], "replayedMean": float(np.mean(original_scores)), "absoluteDifference": difference,
                       "maximumQuantileDifference": quantile_drift, "maximumStatDifference": stat_drift})
        captain = {"dkPlayerId": row["captain_dk_player_id"], "salary": row["captain_salary"]} if row.get("captain_dk_player_id") is not None and row.get("captain_salary") is not None else None
        players.append({"dkPlayerId": row["dk_player_id"], "name": row["name"], "position": row["position"],
                        "rosterPositions": row["roster_positions"], "teamAbbrev": row["team"], "opponent": row["opponent"],
                        "homeAway": None, "gameKey": row["game_key"], "gameInfo": row["game_info"], "salary": row["salary"],
                        "avgFptsDk": row["avg_fpts_dk"], "status": row["dk_status"], "isOut": False, "captain": captain})
        optimizer.append({"id": row["id"], "dkPlayerId": row["dk_player_id"], "captainDkPlayerId": row["captain_dk_player_id"],
                          "name": row["name"], "position": row["position"], "team": row["team"], "opponent": row["opponent"],
                          "gameKey": row["game_key"], "salary": row["salary"], "captainSalary": row["captain_salary"], "rosterPositions": row["roster_positions"],
                          "isOut": False, "projectionStatus": player["projection_status"], "historyGames": player["history_games"],
                          "ourProj": player["model_proj_fpts"], "floorFpts": player["floor_fpts"], "ceilingFpts": player["ceiling_fpts"], "boomRate": player["boom_rate"],
                          "avgFptsDk": row["avg_fpts_dk"], "fantasyprosProj": row["fantasypros_proj"], "linestarProj": row["linestar_proj"],
                          "linestarOwnPct": row["linestar_own_pct"], "customProj": row["custom_proj"]})
    if not players:
        raise ValueError("no eligible audited model marginals")
    snapshot_id = stable_digest({"source": source_hash, "captured_at": captured_at, "players": players})
    banks = []
    for i, stream in enumerate(("selection", "evaluation")):
        banks.append({"schemaVersion": "nfl-marginal-score-bank-v1",
                      "metadata": {"schemaVersion": 1, "runId": stable_digest({"snapshot": snapshot_id, "stream": stream, "seed": seeds[i]}),
                                   "modelVersion": VERSION, "snapshotId": snapshot_id, "decisionAt": captured_at, "inputsCapturedAt": captured_at,
                                   "source": "model", "sampling": "iid", "seed": seeds[i], "streamId": stream + ":" + snapshot_id},
                      "scenarioIds": [f"{stream}:{snapshot_id}:{n}" for n in range(count)], "scores": scores[i],
                      "provenance": {"generator": VERSION, "sourceManifestHash": source_hash, "productionRunId": str(run["run_id"]),
                                     "marginalAuditPassed": True, "historyMode": "current_source_replay", "maximumMeanDifference": max(a["absoluteDifference"] for a in audits)}})
    shadow_banks = []
    if comparison:
        for i, bank in enumerate(banks):
            candidate_bank = deepcopy(bank)
            candidate_bank["scores"] = shadow_scores[i]
            candidate_bank["metadata"]["modelVersion"] = VERSION + "+pfr-shadow"
            candidate_bank["metadata"]["runId"] = stable_digest({"baseline_bank": bank["metadata"]["runId"], "comparison": stable_digest(comparison)})
            candidate_bank["provenance"]["projectionAuthority"] = "shadow_only"
            candidate_bank["provenance"]["shadowComparisonDigest"] = stable_digest(comparison)
            shadow_banks.append(candidate_bank)
    return {"version": VERSION, "status": "construction_only_independent_diagnostic", "generatedAt": captured_at,
            "slate": {"format": upload["format"], "players": players, "games": upload["games"], "teams": upload["teams"], "warnings": []},
            "optimizerPlayers": optimizer, "selection": banks[0], "evaluation": banks[1],
            "challengerSelection": shadow_banks[0] if shadow_banks else None, "challengerEvaluation": shadow_banks[1] if shadow_banks else None,
            "audit": {"sourceManifestHash": source_hash, "productionRunId": str(run["run_id"]), "uploadId": str(upload["upload_id"]),
                      "inputPlayers": len(slate_players), "auditedPlayers": len(players), "marginals": audits, "excluded": excluded,
                      "coherentGameBankAvailable": False, "productionChanged": False,
                      "limitations": ["Player marginals are independently sampled; no calibrated game/stack/DST dependence.",
                                      "Current source replay, not a historical point-in-time claim.", "Audited research subset; excluded rows are not assigned zero forecasts.",
                                      "Marginal mean audit preserves baseline math but does not prove historical source identity."]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upload", required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--comparison", help="Frozen root matchup comparison; optional research ledger only")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        run = db.execute("SELECT * FROM nfl_dfs_projection_runs WHERE run_id=%s", (args.run,))[0]
        upload = db.execute("SELECT * FROM nfl_dfs_slate_uploads WHERE upload_id=%s", (args.upload,))[0]
        players = db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY dk_player_id", (args.upload,))
        projections = db.execute("SELECT * FROM nfl_dfs_player_projections WHERE run_id=%s ORDER BY player_id", (args.run,))
        history = _history(db, run["season"], run["week"])
    now = datetime.now(timezone.utc).isoformat()
    # Stable digests use JSON-normalized database timestamps and UUIDs.
    normalize = lambda value: json.loads(json.dumps(value, default=str))
    comparison = json.loads(Path(args.comparison).read_text(encoding="utf-8")) if args.comparison else None
    result = build_score_banks(normalize(players), normalize(projections), history, normalize(run), normalize(upload), captured_at=now, comparison=comparison)
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, separators=(",", ":"), allow_nan=False), encoding="utf-8")
    print(json.dumps({"output": str(target), "inputPlayers": result["audit"]["inputPlayers"], "auditedPlayers": result["audit"]["auditedPlayers"],
                      "maximumMeanDifference": result["selection"]["provenance"]["maximumMeanDifference"], "excluded": len(result["audit"]["excluded"]),
                      "status": result["status"]}))


if __name__ == "__main__":
    main()
