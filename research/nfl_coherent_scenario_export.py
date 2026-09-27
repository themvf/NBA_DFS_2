"""Read stored NFL inputs and freeze a separately named coherent research bank."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import json

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_efficiency import raw_history
from model.nfl_context_engine import stable_digest
from model.nfl_dfs_workload import build
from model.nfl_dfs_historical import BOOM_THRESHOLDS
from model.nfl_matchup_scenarios import build_coherent_banks, condition_starting_qbs
from model.nfl_pfr_supplement import team_code as pfr_team_code
from hashlib import sha256
import numpy as np


def team_code(value):
    """DK/canonical roster identity; PFR's LA alias must not leak into DK games."""
    code = pfr_team_code(value)
    return "LAR" if code == "LA" else code


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--comparison", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--draws", type=int, default=400)
    ap.add_argument("--retrospective", action="store_true", help="Labeled current-source mechanical replay; never forward qualification or live recommendations")
    ap.add_argument("--baseline-marginals", help="Verified original-seed baseline score bank for paired WIS grading; absence withholds paired grading")
    args = ap.parse_args()
    comparison_bytes = Path(args.comparison).read_bytes()
    comparison = json.loads(comparison_bytes)
    registration_path = Path("research/nfl_coherent_scenario_study.json")
    registration = json.loads(registration_path.read_text(encoding="utf-8"))
    marginal_bytes = Path(args.baseline_marginals).read_bytes() if args.baseline_marginals else None
    marginal_bank = json.loads(marginal_bytes) if marginal_bytes else None
    if marginal_bank and (str(marginal_bank["audit"]["productionRunId"]) != str(comparison["baseline_run_id"])
            or str(marginal_bank["audit"]["uploadId"]) != str(comparison["upload_id"])
            or not marginal_bank["selection"]["provenance"].get("marginalAuditPassed")):
        raise ValueError("Baseline grading bank has incompatible run or failed marginal audit")
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        history = raw_history(db)
        raw_teams = db.execute("""SELECT w.id,w.season,w.week,w.team,w.opponent,w.fetched_at,w.source_row->'raw_team_stats' stats
            FROM ff_player_week_stats w JOIN ff_players p ON p.id=w.player_id WHERE p.position='DST' AND w.source='nflverse'
            AND w.season_type='REG' AND w.season>=2023 AND (w.season<%s OR (w.season=%s AND w.week<%s)) ORDER BY w.season,w.week,w.team""",
            (comparison["season"], comparison["season"], comparison["week"]))
        salary = db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY dk_player_id", (comparison["upload_id"],))
        if comparison.get("salary_snapshot"):
            salary = comparison["salary_snapshot"]
        upload = db.execute("SELECT * FROM nfl_dfs_slate_uploads WHERE upload_id=%s", (comparison["upload_id"],))[0]
        baseline_run = db.execute("SELECT model_version,model_config FROM nfl_dfs_projection_runs WHERE run_id=%s", (comparison["baseline_run_id"],))[0]
        depth_rows = [] if args.retrospective else db.execute("""SELECT gsis_id identity,team_abbrev team,fetched_at,
            NULLIF(metadata->'sleeper'->>'depth_chart_order','')::int depth_order
            FROM ff_players WHERE season=%s AND position='QB' AND active=TRUE""", (comparison["season"],))
    for row in depth_rows:
        row["team"] = team_code(row["team"])
    for row in history:
        row["team"] = team_code(row["team"])
        row["opponent"] = team_code(row["opponent"])
    team_rows = []
    for row in raw_teams:
        if not isinstance(row["stats"], dict):
            continue
        team_rows.append({**dict(row), "team": team_code(row["team"]), "opponent": team_code(row["opponent"]),
                          "game_id": row["stats"].get("game_id")})
    roster, identities, baseline_means, factors, games = [], {}, {}, {}, {}
    mapped = {int(p["dk_player_id"]): p for p in comparison["players"]}
    slate_players, optimizer = [], []
    for row in salary:
        p = mapped.get(int(row["dk_player_id"]))
        if not p or p["baseline"].get("is_out") or row["is_out"] or p["baseline"]["projection_status"] == "out":
            continue
        baseline = p["baseline"]
        team = team_code(p["team"])
        identity = "DST:" + team if p["position"] == "DST" else baseline["player_gsis_id"]
        if not identity or (p["position"] == "K" and upload["format"] != "showdown"):
            continue
        identities[identity] = int(row["dk_player_id"])
        baseline_means[str(row["dk_player_id"])] = baseline["model_proj_fpts"]
        roster.append({"identity": identity, "position": p["position"], "name": p["name"], "team": team})
        away, home = row["game_key"].split("@")
        games[p["game_id"]] = {"game_id": p["game_id"], "season": comparison["season"], "week": comparison["week"],
                              "kickoff": p["kickoff"], "home_team": team_code(home), "away_team": team_code(away)}
        fs = baseline["feature_snapshot"]
        yard, td = float(fs.get("yardage_factor", 1)), float(fs.get("touchdown_factor", 1))
        player_factors = {}
        if p["position"] == "QB":
            player_factors = {"passing_yards_per_completion": yard, "passing_td_rate": td, "rushing_yards_per_carry": yard, "rushing_td_rate": td}
        elif p["position"] in ("RB", "WR", "TE"):
            player_factors = {"receiving_yards_per_reception": yard, "receiving_td_rate": td, "rushing_yards_per_carry": yard, "rushing_td_rate": td}
        for step in p["shadow"].get("ledger", []):
            key = "passing_yards_per_completion" if step["component"] == "passing_yards" else "rushing_yards_per_carry"
            if step.get("status") == "shadow_only":
                player_factors[key] *= float(step["factor"])
        factors[identity] = player_factors
        captain = {"dkPlayerId": row["captain_dk_player_id"], "salary": row["captain_salary"]} if row["captain_dk_player_id"] is not None else None
        slate_players.append({"dkPlayerId": row["dk_player_id"], "name": row["name"], "position": row["position"], "rosterPositions": row["roster_positions"],
                              "teamAbbrev": team, "opponent": team_code(row["opponent"]), "homeAway": None, "gameKey": row["game_key"], "gameInfo": row["game_info"],
                              "salary": row["salary"], "avgFptsDk": row["avg_fpts_dk"], "status": row["dk_status"], "isOut": False, "captain": captain})
        optimizer.append({"id": row["id"], "dkPlayerId": row["dk_player_id"], "captainDkPlayerId": row["captain_dk_player_id"], "name": row["name"],
                          "position": row["position"], "team": team, "opponent": team_code(row["opponent"]), "gameKey": row["game_key"], "salary": row["salary"],
                          "captainSalary": row["captain_salary"], "rosterPositions": row["roster_positions"], "isOut": False,
                          "projectionStatus": baseline["projection_status"], "historyGames": baseline["history_games"], "ourProj": baseline["model_proj_fpts"],
                          "floorFpts": baseline["floor_fpts"], "ceilingFpts": baseline["ceiling_fpts"], "boomRate": baseline["boom_rate"], "avgFptsDk": row["avg_fpts_dk"],
                          "fantasyprosProj": row["fantasypros_proj"], "linestarProj": row["linestar_proj"], "linestarOwnPct": row["linestar_own_pct"], "customProj": row["custom_proj"]})
    now = datetime.now(timezone.utc).isoformat()
    if not args.retrospective and any(datetime.fromisoformat(game["kickoff"].replace("Z", "+00:00")) <= datetime.fromisoformat(now) for game in games.values()):
        raise ValueError("a target game has already started; freeze must remain pregame")
    forecasts = build(team_rows, history, list(games.values()), roster, now)
    conditional_roles = condition_starting_qbs(forecasts, depth_rows, now)
    kicker_roles = {}
    kicker_audit = []
    for team in sorted({p["team"] for p in roster}):
        candidates = [p for p in roster if p["team"] == team and p["position"] == "K"]
        if len(candidates) == 1:
            kicker_roles[team] = candidates[0]["identity"]
        if candidates:
            kicker_audit.append({"team": team, "state": "conditional_unique_slate_kicker" if len(candidates) == 1 else "unallocated_ambiguous_kicker",
                "identities": [p["identity"] for p in candidates], "availability_probability": None})
    source_manifest = {"baseline_run_id": comparison["baseline_run_id"], "comparison_digest": stable_digest(comparison),
                       "comparison_file_sha256": sha256(comparison_bytes).hexdigest(),
                       "baseline_marginal_file_sha256": sha256(marginal_bytes).hexdigest() if marginal_bytes else None,
                       "team_rows_digest": stable_digest(json.loads(json.dumps(team_rows, default=str))), "history_digest": stable_digest(history),
                       "salary_digest": stable_digest(json.loads(json.dumps(salary, default=str))), "captured_at": now, "history_mode": "current_source_replay",
                       "registration": registration, "registration_sha256": sha256(registration_path.read_bytes()).hexdigest(),
                       "conditional_roles": conditional_roles, "depth_digest": stable_digest(json.loads(json.dumps(depth_rows, default=str))),
                       "kicker_roles": kicker_audit, "retrospective": args.retrospective,
                       "baseline_config_hash": stable_digest(dict(baseline_run)),
                       "baseline_config_hash_convention": "sha256_compact_sorted_json_model_version_and_model_config",
                       "implementation_hash_algorithm": "sha256_utf8_lf",
                       "implementation_hashes": {name: sha256(Path(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in
                            ("model/nfl_matchup_scenarios.py", "model/nfl_dfs_efficiency.py", "model/nfl_dfs_workload.py", "ingest/nfl_dfs_results.py", "research/nfl_coherent_scenario_export.py",
                             "model/nfl_matchup_projection.py", "model/nfl_dfs_historical.py", "research/nfl_matchup_scenario_export.py", "web/src/lib/nfl-dfs/scoring.ts",
                             "web/src/lib/nfl-dfs/scenarios.ts", "web/src/lib/nfl-dfs/portfolio-selection.ts", "web/src/lib/nfl-dfs/matchup-contest.ts", "web/src/app/dfs/nfl/nfl-optimizer-shadow.ts",
                             "model/nfl_dst_components.py", "model/nfl_team_aliases.py")}}
    if registration.get("implementation_hashes") != source_manifest["implementation_hashes"]:
        raise ValueError("Coherent implementation differs from the immutable registration")
    if args.draws != registration.get("draws"):
        raise ValueError("Draw count differs from the immutable registration")
    if not args.retrospective and source_manifest["baseline_config_hash"] != registration.get("baseline_config_hash"):
        raise ValueError("Baseline configuration differs from the registered candidate cohort")
    slate = {"format": upload["format"], "players": slate_players, "games": upload["games"], "teams": upload["teams"], "warnings": []}
    result = build_coherent_banks(slate=slate, forecasts=forecasts, history=history, team_rows=team_rows, identities=identities,
        baseline_means=baseline_means, source_manifest=source_manifest, decision_at=now, draws=args.draws, rate_factors=factors, kicker_roles=kicker_roles)
    if args.retrospective:
        result["status"] = "coherent_research_retrospective_unqualified"
        result["limitations"].append("Retrospective mechanical replay using a saved pre-lock salary/projection snapshot and currently captured pre-target history. Current depth roles and target-game outcomes are not used. This is not an untouched forward test or historically executable bank.")
    supported = {row["dkPlayerId"] for row in result["slate"]["players"]}
    result["optimizerPlayers"] = [row for row in optimizer if row["dkPlayerId"] in supported]
    result["audit"] = {"productionRunId": comparison["baseline_run_id"], "uploadId": comparison["upload_id"], "inputPlayers": len(salary),
                       "auditedPlayers": len(supported), "coverage": result["coverage"], "marginalsPreserved": False, "productionChanged": False}
    eval_marginals = {row["dkPlayerId"]: row for row in result["diagnostics"][1]["player_marginals"]}
    baseline_identities = {str(row["dkPlayerId"]): str(row["playerId"]) for row in (marginal_bank or {}).get("audit", {}).get("marginals", [])
        if all(isinstance(row.get(key), (float, int)) and 0 <= row[key] <= .00011 for key in ("absoluteDifference", "maximumQuantileDifference", "maximumStatDifference"))}
    result["paired_grading_rows"] = []
    for player in comparison["players"]:
        key = str(player["dk_player_id"])
        candidate = eval_marginals.get(int(key))
        baseline_scores = (marginal_bank or {}).get("selection", {}).get("scores", {}).get(key)
        paired = candidate is not None and bool(baseline_scores) and baseline_identities.get(key) == str(player["player_id"]) and not args.retrospective
        threshold = BOOM_THRESHOLDS.get(player["position"])
        result["paired_grading_rows"].append({"dk_player_id": int(key), "player_id": player["player_id"], "position": player["position"],
            "game_id": player["game_id"], "kickoff": player["kickoff"], "decision_at": now, "baseline_run_id": comparison["baseline_run_id"],
            "status": "paired" if paired else "excluded", "exclusion_reason": None if paired else "retrospective_development" if args.retrospective else "candidate_or_audited_baseline_draws_missing",
            "baseline": {"mean": float(np.mean(baseline_scores)), **{f"p{int(q * 100)}": float(np.quantile(baseline_scores, q)) for q in (.1, .25, .5, .75, .9)},
                "boom_threshold": threshold, "boom_probability": float(np.mean(np.array(baseline_scores) >= threshold))} if paired else None,
            "candidate": {"mean": candidate["researchMean"], **{f"p{q}": candidate[f"researchP{q}"] for q in (10, 25, 50, 75, 90)},
                "boom_threshold": candidate["boomThreshold"], "boom_probability": candidate["boomProbability"]} if candidate else None})
    by_dk = {int(p["dk_player_id"]): p["baseline"] for p in comparison["players"]}
    for diagnostic, bank in zip(result["diagnostics"], (result["selection"], result["evaluation"])):
        for row in diagnostic["player_marginals"]:
            old = by_dk[row["dkPlayerId"]]
            row.update(baselineP10=old["floor_fpts"], baselineP50=old["median_fpts"], baselineP90=old["ceiling_fpts"])
            row["p10Delta"] = row["researchP10"] - old["floor_fpts"] if old["floor_fpts"] is not None else None
            row["p50Delta"] = row["researchP50"] - old["median_fpts"] if old["median_fpts"] is not None else None
            row["p90Delta"] = row["researchP90"] - old["ceiling_fpts"] if old["ceiling_fpts"] is not None else None
            row["eventMeans"] = []
            for field, old_field in (("passTds", "passing_tds"), ("rushTds", "rushing_tds"), ("recTds", "receiving_tds"), ("interceptions", "passing_interceptions"), ("fumblesLost", "fumbles_lost_total"), ("sacks", "sacks"), ("dstInterceptions", "interceptions")):
                observations = [d["stats"][str(row["dkPlayerId"])][field] for d in bank["scenarios"] if field in d["stats"][str(row["dkPlayerId"])]]
                if observations:
                    old_mean = old["stat_means"].get(old_field)
                    row["eventMeans"].append({"stat": field, "researchMean": float(np.mean(observations)), "probabilityPositive": float(np.mean(np.array(observations) > 0)),
                                              "baselineMean": old_mean, "meanDelta": float(np.mean(observations)) - old_mean if old_mean is not None else None})
        diagnostic["sharedBudgetDependence"] = []
        for game_index, game in enumerate(diagnostic["event_ledgers"][0]):
            for key in ("attempts", "carries"):
                left = [draw[game_index]["teams"][0]["opportunities"][key] for draw in diagnostic["event_ledgers"]]
                right = [draw[game_index]["teams"][1]["opportunities"][key] for draw in diagnostic["event_ledgers"]]
                diagnostic["sharedBudgetDependence"].append({"game_id": game["game_id"], "component": key,
                    "correlation": float(np.corrcoef(left, right)[0, 1]) if np.std(left) > 0 and np.std(right) > 0 else None})
    # Large ledgers stay separate so the UI can consume compact diagnostics.
    result["limitations"].append("QB shares condition on a fresh unique listed starter playing normally; this is not a calibrated availability mixture. Unresolved roles retain historical allocation; role states are audited in the source manifest.")
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    for diagnostic in result["diagnostics"]:
        ledger_path = target.with_name(target.stem + "-" + diagnostic["stream"] + "-ledger.json")
        ledger_path.write_text(json.dumps(diagnostic.pop("event_ledgers"), separators=(",", ":"), allow_nan=False), encoding="utf-8")
        diagnostic["eventLedgerFile"] = str(ledger_path)
        diagnostic["eventLedgerHash"] = __import__("hashlib").sha256(ledger_path.read_bytes()).hexdigest()
    target.write_text(json.dumps(result, separators=(",", ":"), default=str, allow_nan=False), encoding="utf-8")
    summary = {key: result[key] for key in ("version", "authority", "productionChanged", "status", "manifest", "diagnostics", "coverage", "limitations", "audit", "paired_grading_rows")}
    target.with_name("coherent-scenario-summary.json" if not args.retrospective else "showdown-retrospective-summary.json").write_text(json.dumps(summary, indent=2, default=str, allow_nan=False), encoding="utf-8")
    print(json.dumps({"output": str(target), "model": result["version"], "coverage": result["coverage"], "historicalGames": result["manifest"]["history_games"], "productionChanged": False}))


if __name__ == "__main__":
    main()
