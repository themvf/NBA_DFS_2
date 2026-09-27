"""Read-only context grading/health and explicit matchup study registration.

Examples:
  python -m research.nfl_matchup_study context --season 2026
  python -m research.nfl_matchup_study health --season 2026
  python -m research.nfl_matchup_study validate --registry docs/nfl-matchup-studies.json
  python -m research.nfl_matchup_study grade --manifest registered.json --rows frozen-rows.json
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path

from model.nfl_dfs_context_variant_study import evaluate_context_variants, freeze_health, timestamp
from model.nfl_matchup_study import digest, evaluate_study, implementation_digest, validate_manifest


def ledger_inputs(db, season, now, study_run_id=None):
    games = db.execute("""SELECT g.id,g.season,g.week,g.kickoff,g.completed,
        h.abbreviation home_team,a.abbreviation away_team FROM nfl_season_games g
        JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.game_type='REG' ORDER BY g.week,g.kickoff""", (season,))
    records = db.execute("""SELECT p.*, CASE WHEN r.id IS NULL THEN NULL ELSE
        jsonb_build_object('actual',r.actual_dk_fpts,'scoring_status',r.scoring_status,
          'game_id',r.game_id,'computed_at',r.computed_at,'result_id',r.id,
          'scoring_version',r.scoring_version,'result_digest',r.input_digest) END outcome
        FROM nfl_dfs_shadow_predictions p
        LEFT JOIN LATERAL (SELECT r.* FROM nfl_dfs_player_week_results r
          JOIN nfl_season_games g ON g.id=r.game_id
          WHERE r.player_id=p.player_id AND r.season=p.season AND r.week=p.week
            AND g.completed AND g.kickoff=p.kickoff AND r.computed_at<=%s
          ORDER BY r.computed_at DESC,r.id DESC LIMIT 1) r ON TRUE
        WHERE p.season=%s AND p.captured_at<=%s AND (%s IS NULL OR p.study_run_id=%s)""", (now, season, now, study_run_id, study_run_id))
    grouped = defaultdict(list)
    for game in games:
        grouped[(game["season"], game["week"])].append(game)
    complete = [key for key, group in grouped.items() if all(g["completed"] and g["kickoff"] < now for g in group)]
    return games, records, complete


def context_report(db, season, now, config):
    pins = db.execute("SELECT report FROM nfl_dfs_research_runs WHERE run_id=%s", (config["study_run_id"],))
    if not pins or pins[0]["report"].get("output_digest") != config["output_digest"]:
        raise ValueError("Context study pin is absent or its registered output digest differs")
    games, records, complete = ledger_inputs(db, season, now)
    pin = config["study_run_id"]
    compatibility_path = Path("research/nfl_dfs_context_scoring_compatibility.json")
    compatibility = json.loads(compatibility_path.read_text()) if compatibility_path.exists() else None
    if compatibility:
        from ingest.nfl_dfs_results import SCORING_VERSION
        rule = compatibility["versions"].get(SCORING_VERSION)
        if not rule or outcome_implementation_errors({"outcome_implementation_hashes": rule["implementation_hashes"]}):
            raise ValueError("Context outcome scorer differs from its explicit compatibility declaration")
    report = evaluate_context_variants(records, pin, now, complete_weeks=complete,
                                      outcome_versions=compatibility["versions"] if compatibility else None)
    report["outcome_compatibility_hash"] = digest(compatibility) if compatibility else None
    report["pin_output_digest"] = config["output_digest"]
    report["pin_verified"] = True
    report["all_study_pins_present"] = sorted({r["study_run_id"] for r in records})
    report["dataset_digest"] = digest(records)
    pin_weeks = [(r["season"], r["week"]) for r in records if r["study_run_id"] == pin]
    first = min(pin_weeks) if pin_weeks else (season, min(config.get("forward_gate", {}).get("window_weeks", [1])))
    report["freeze_health"] = freeze_health([g for g in games if (g["season"], g["week"]) >= first], records, pin, now)
    return report


def prospective_reports(db, season, now, registry):
    """Join immutable matchup snapshots to latest exact, revisioned outcomes."""
    registered = [m for m in registry["studies"] if m.get("status") == "registered" and m["kind"].startswith("dfs_")]
    if not registered:
        return {"version": "nfl-matchup-forward-report-v1", "studies": [], "reason": "no registered DFS studies"}
    first_registered = min(timestamp(resolve_registration(m)["registered_at"]) for m in registered)
    records = db.execute("""SELECT p.player_id,p.game_id,p.kickoff,p.projection,
        f.run_id,f.season,f.week,f.created_at,f.as_of_at,f.manifest,
        b.model_version baseline_version,b.model_config baseline_config,b.created_at baseline_created_at,
        r.id result_id,r.actual_dk_fpts,r.scoring_status,r.scoring_version,r.input_digest result_digest,r.computed_at result_at
        FROM nfl_matchup_player_forecasts p JOIN nfl_matchup_forecast_runs f ON f.run_id=p.run_id
        JOIN nfl_dfs_projection_runs b ON b.run_id=f.baseline_run_id
        JOIN nfl_season_games g ON g.nflverse_game_id=p.game_id
        LEFT JOIN LATERAL (SELECT r.* FROM nfl_dfs_player_week_results r
          WHERE r.player_id=p.player_id AND r.game_id=g.id AND r.computed_at<=%s AND g.completed
          ORDER BY r.computed_at DESC,r.id DESC LIMIT 1) r ON TRUE
        WHERE f.season=%s AND f.created_at<=%s AND f.created_at>=%s""", (now, season, now, first_registered))
    completed = db.execute("""SELECT season,week FROM nfl_season_games WHERE season=%s AND game_type='REG'
        GROUP BY season,week HAVING bool_and(completed) AND max(kickoff)<%s""", (season, now))
    complete = [(r["season"], r["week"]) for r in completed]
    reports = []
    for manifest in registered:
        manifest = resolve_registration(manifest)
        outcome_pin_errors = outcome_implementation_errors(manifest)
        if outcome_pin_errors:
            rejected_report = evaluate_study(manifest, [], now=now, complete_weeks=complete)
            rejected_report.update(reason="registered outcome implementation differs from the checked-out scorer",
                                   outcome_implementation_errors=outcome_pin_errors)
            reports.append(rejected_report)
            continue
        rows = []
        for record in records:
            p, run = record["projection"], record["manifest"]
            if p["position"] not in manifest["affected_positions"]:
                continue
            shadow, original = p["shadow"], p["baseline"]
            available = [timestamp(record["baseline_created_at"])]
            for source in p.get("sources", []):
                for key in ("captured_at", "recorded_at"):
                    if source.get(key):
                        available.append(timestamp(source[key]))
                identity_at = (source.get("identity_manifest") or {}).get("captured_at")
                if identity_at:
                    available.append(timestamp(identity_at))
            family_hashes = {key: run.get("model_hashes", {}).get(key) for key in manifest["components"]}
            config_hash = next(iter(family_hashes.values())) if len(family_hashes) == 1 else digest(family_hashes)
            covered = shadow.get("status") == "under_evaluation"
            def distribution(value):
                return {"mean": value.get("mean"), "median": value.get("p50"), "p10": value.get("p10"),
                        "p90": value.get("p90"), "boom_probability": value.get("boom")}
            if covered:
                baseline, candidate = distribution(shadow["baseline"]), distribution(shadow["candidate"])
            else:
                baseline = {"mean": original.get("model_proj_fpts"), "median": original.get("median_fpts"),
                            "p10": original.get("floor_fpts"), "p90": original.get("ceiling_fpts"), "boom_probability": original.get("boom_rate")}
                candidate = dict(baseline)
            rows.append({"study_id": manifest["study_id"], "forecast_id": f"{record['run_id']}:{record['player_id']}",
                "game_id": record["game_id"], "player_id": record["player_id"], "season": record["season"], "week": record["week"],
                "position": p["position"], "captured_at": record["created_at"], "decision_cutoff": record["as_of_at"],
                "available_at": max(available), "kickoff": record["kickoff"],
                "input_manifest_hash": digest({"matchup": shadow["matchup_manifest_hash"], "baseline": original.get("id"), "run": record["run_id"]}),
                "candidate_config_hash": config_hash, "baseline_config_hash": digest({"model_version": record["baseline_version"], "model_config": record["baseline_config"]}),
                "implementation_hashes": run.get("implementation_hashes"),
                "scoring_version": record.get("scoring_version") or manifest["scoring_version"],
                "actual": record.get("actual_dk_fpts"), "scoring_status": record.get("scoring_status"),
                "result_id": record.get("result_id"), "result_at": record.get("result_at"), "result_digest": record.get("result_digest"),
                "baseline": baseline, "candidate": candidate, "covered": covered})
        evaluated = evaluate_study(manifest, rows, now=now, complete_weeks=complete)
        evaluated["outcome_implementation_hashes"] = manifest.get("outcome_implementation_hashes", {})
        evaluated["seen_candidate_config_hashes"] = sorted({r["candidate_config_hash"] for r in rows if r["candidate_config_hash"]})
        evaluated["seen_baseline_config_hashes"] = sorted({r["baseline_config_hash"] for r in rows})
        reports.append(evaluated)
    return {"version": "nfl-matchup-forward-report-v1", "evaluated_at": now.isoformat(), "season": season,
            "source_forecast_rows": len(records), "studies": reports, "production_promotion": False}


def resolve_registration(index_entry):
    """Resolve append-only pins/amendments; never reinterpret earlier freezes."""
    if not index_entry.get("registration_file"):
        return dict(index_entry)
    original = json.loads(Path(index_entry["registration_file"]).read_text())
    result = dict(original)
    pin_files = index_entry.get("implementation_pin_files") or ([index_entry["implementation_pin_file"]] if index_entry.get("implementation_pin_file") else [])
    previous_pin = None
    for path in pin_files:
        pin = json.loads(Path(path).read_text())
        if pin["registration_manifest_hash"] != digest(original):
            raise ValueError("Implementation pin does not match immutable study registration")
        if previous_pin and (pin.get("previous_pin_hash") != digest(previous_pin) or timestamp(pin["pinned_at"]) < timestamp(previous_pin["pinned_at"])):
            raise ValueError("Broken immutable implementation pin chain")
        result.update(implementation_hashes=pin["hashes"], implementation_pinned_at=pin["pinned_at"],
                      implementation_hash_algorithm=pin.get("hash_algorithm","sha256_raw_bytes"),
                      qualification_scope=pin.get("qualification_scope"))
        previous_pin = pin
    for path in index_entry.get("amendment_files", []):
        amendment = json.loads(Path(path).read_text())
        if amendment["registration_manifest_hash"] != digest(original) or amendment["previous_baseline_config_hash"] != result["baseline_config_hash"]:
            raise ValueError("Broken immutable baseline amendment chain")
        if timestamp(amendment["registered_at"]) < timestamp(result["registered_at"]):
            raise ValueError("Baseline amendment cannot be backdated")
        result.update(baseline_config_hash=amendment["baseline_config_hash"], registered_at=amendment["registered_at"],
                      holdout_start_at=amendment["registered_at"], baseline_amendment_hash=digest(amendment))
    previous_outcome_amendment = None
    for path in index_entry.get("outcome_amendment_files", []):
        amendment = json.loads(Path(path).read_text())
        if (amendment["registration_manifest_hash"] != digest(original)
                or amendment["previous_scoring_version"] != result["scoring_version"]
                or amendment.get("previous_amendment_hash") != previous_outcome_amendment):
            raise ValueError("Broken immutable outcome amendment chain")
        earliest = max(timestamp(result["registered_at"]), timestamp(result.get("implementation_pinned_at") or result["registered_at"]))
        if timestamp(amendment["registered_at"]) < earliest:
            raise ValueError("Outcome amendment cannot be backdated")
        if amendment.get("hash_algorithm") != "sha256_lf_normalized" or not amendment.get("implementation_hashes"):
            raise ValueError("Outcome amendment requires exact portable scorer implementation pins")
        result.update(scoring_version=amendment["scoring_version"], registered_at=amendment["registered_at"],
                      holdout_start_at=amendment["registered_at"], outcome_amendment_hash=digest(amendment),
                      outcome_implementation_hashes=amendment["implementation_hashes"])
        previous_outcome_amendment = digest(amendment)
    return result


def outcome_implementation_errors(manifest):
    """A named result version must not silently run a changed outcome scorer."""
    errors = []
    for path, expected in manifest.get("outcome_implementation_hashes", {}).items():
        source = Path(path)
        if not source.is_file() or implementation_digest(source.read_bytes()) != expected:
            errors.append(path)
    return errors


GAME_RESULT_DDL = """CREATE TABLE IF NOT EXISTS nfl_matchup_game_results (
 result_id TEXT PRIMARY KEY, game_id TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL,
 home_score INT NOT NULL, away_score INT NOT NULL, outcome TEXT NOT NULL,
 source_digest TEXT NOT NULL, scoring_version TEXT NOT NULL);"""


def capture_game_results(db, season, now):
    """Append observed result revisions; original forecasts are never rewritten."""
    db.execute(GAME_RESULT_DDL)
    games = db.execute("""SELECT nflverse_game_id game_id,home_score,away_score FROM nfl_season_games
        WHERE season=%s AND game_type='REG' AND completed AND kickoff<%s
          AND home_score IS NOT NULL AND away_score IS NOT NULL""", (season, now))
    count = 0
    with db.connect() as connection:
        with connection.cursor() as cursor:
            for game in games:
                if not game["game_id"]:
                    continue
                source = digest(dict(game))
                outcome = "home" if game["home_score"] > game["away_score"] else "away" if game["away_score"] > game["home_score"] else "tie"
                cursor.execute("""INSERT INTO nfl_matchup_game_results
                    (result_id,game_id,observed_at,home_score,away_score,outcome,source_digest,scoring_version)
                    SELECT %s,%s,%s,%s,%s,%s,%s,'nfl-game-outcome-three-class-v1'
                    WHERE (SELECT source_digest FROM nfl_matchup_game_results WHERE game_id=%s ORDER BY observed_at DESC,result_id DESC LIMIT 1)
                      IS DISTINCT FROM %s ON CONFLICT DO NOTHING RETURNING result_id""",
                    (digest({"game_id":game["game_id"],"source":source,"observed_at":now}),game["game_id"],now,game["home_score"],game["away_score"],outcome,source,game["game_id"],source))
                count += cursor.rowcount
    return count


def pickem_prospective_reports(db, season, now, registry):
    studies = [m for m in registry["studies"] if m.get("status") == "registered" and m["kind"] == "pickem"]
    if not studies:
        return []
    first_registered = min(timestamp(resolve_registration(m)["registered_at"]) for m in studies)
    records = db.execute("""SELECT f.*,g.season,g.week,g.kickoff,r.result_id,r.observed_at result_at,
        r.outcome,r.source_digest result_digest,r.scoring_version
        FROM nfl_pickem_matchup_forecasts f JOIN nfl_season_games g ON g.nflverse_game_id=f.game_id
        LEFT JOIN LATERAL (SELECT r.* FROM nfl_matchup_game_results r WHERE r.game_id=f.game_id
           AND r.observed_at<=%s ORDER BY r.observed_at DESC,r.result_id DESC LIMIT 1) r ON TRUE
        WHERE g.season=%s AND f.available_at<=%s AND f.available_at>=%s""", (now, season, now, first_registered))
    complete = [(r["season"],r["week"]) for r in db.execute("""SELECT season,week FROM nfl_season_games
        WHERE season=%s AND game_type='REG' GROUP BY season,week HAVING bool_and(completed) AND max(kickoff)<%s""", (season,now))]
    reports = []
    for index in studies:
        manifest = resolve_registration(index)
        rows = []
        for record in records:
            payload = record["payload"]
            if payload.get("study_id") != manifest["study_id"]:
                continue
            input_ = payload["input"]
            available = [timestamp(input_["baseline"]["marketCapturedAt"])] if input_["baseline"].get("marketCapturedAt") else [timestamp(record["decision_cutoff"])]
            for source in payload.get("featureManifest", {}).get("sources", []):
                for key in ("captured_at", "recorded_at"):
                    if source.get(key):
                        available.append(timestamp(source[key]))
                identity_at = (source.get("identity_manifest") or {}).get("captured_at")
                if identity_at:
                    available.append(timestamp(identity_at))
            rows.append({"study_id": manifest["study_id"], "forecast_id": record["forecast_id"], "game_id": record["game_id"],
                "season": record["season"], "week": record["week"], "kickoff": record["kickoff"],
                "captured_at": record["available_at"], "decision_cutoff": record["decision_cutoff"], "available_at": max(available),
                "input_manifest_hash": digest(input_), "candidate_config_hash": payload.get("candidate_config_hash"),
                "baseline_config_hash": payload.get("baseline_config_hash"), "implementation_hashes": payload.get("implementation_hashes"),
                "baseline": probability_arm(payload.get("baseline")), "candidate": probability_arm(payload.get("candidate")),
                "covered": payload.get("covered",False), "scoring_version": record.get("scoring_version") or manifest["scoring_version"],
                "scoring_status": "exact" if record.get("outcome") else "pending", "actual": record.get("outcome"),
                "result_id": record.get("result_id"), "result_at": record.get("result_at"), "result_digest": record.get("result_digest")})
        reports.append(evaluate_study(manifest, rows, now=now, complete_weeks=complete))
    return reports


def probability_arm(payload):
    """UI metadata such as homeConditional is not a fourth outcome class."""
    payload = payload or {}
    return {"probabilities": {k: payload[k] for k in ("home", "away", "tie") if k in payload}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("context", "health", "prospective", "coherent", "freeze-pickem", "validate", "grade", "register"))
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--config", type=Path, default=Path("artifacts/nfl_dfs_shadow_config.json"))
    parser.add_argument("--registry", type=Path, default=Path("docs/nfl-matchup-studies.json"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--rows", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--phase", choices=("promotion", "rollback"), default="promotion")
    parser.add_argument("--capture-results", action="store_true", help="append immutable game result observations before grading")
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc)
    args.season = args.season or (now.year-1 if now.month <= 3 else now.year)
    if args.action in ("context", "health", "prospective", "coherent", "freeze-pickem"):
        from config import load_config
        from ingest.nfl_dfs_weekly import PipelineDatabase
        db = PipelineDatabase(load_config().database_url, initialize_schema=False)
        config = json.loads(args.config.read_text())
        if args.action == "freeze-pickem":
            from research.nfl_pickem_matchup import freeze_forecasts, MODEL_PATH
            week_row = db.execute_one("SELECT min(week) week FROM nfl_season_games WHERE season=%s AND game_type='REG' AND kickoff>%s", (args.season, now))
            week = args.week or (week_row or {}).get("week")
            result = freeze_forecasts(db, json.loads(MODEL_PATH.read_text()), args.season, week, persist=True) if week else {"status":"no upcoming regular-season games"}
            args.output = args.output or Path("artifacts/nfl-matchup-implementation")/now.date().isoformat()/"pickem-shadow.json"
        elif args.action == "prospective":
            registry = json.loads(args.registry.read_text())
            captured = capture_game_results(db, args.season, now) if args.capture_results else None
            result = prospective_reports(db, args.season, now, registry)
            result["captured_game_result_revisions"] = captured
            if db.execute_one("SELECT to_regclass('nfl_matchup_game_results') available")["available"]:
                result["studies"].extend(pickem_prospective_reports(db, args.season, now, registry))
            else:
                result["pickem_outcomes"] = "result revision ledger not captured; run with --capture-results"
            from research.nfl_coherent_study import coherent_reports
            result["studies"].extend(coherent_reports(db,args.season,now))
        elif args.action == "coherent":
            from research.nfl_coherent_study import coherent_reports
            result={"version":"nfl-coherent-forward-report-v1","season":args.season,"evaluated_at":now.isoformat(),
                    "studies":coherent_reports(db,args.season,now),"production_promotion":False}
        else:
            result = context_report(db, args.season, now, config)
        if args.action == "health":
            result = {"checked_at": now.isoformat(), "study_run_id": config["study_run_id"],
                      "checks": [r for r in result["freeze_health"] if args.week is None or r["week"] == args.week]}
    elif args.action == "validate":
        registry = json.loads(args.registry.read_text())
        result = {"studies": [{"study_id": m["study_id"], "errors": validate_manifest(m)} for m in registry["studies"]]}
    elif args.action == "register":
        if not args.manifest or not args.output:
            parser.error("register requires --manifest and a new --output")
        result = json.loads(args.manifest.read_text())
        result.update(status="registered", registered_at=now.isoformat())
        errors = validate_manifest(result)
        if errors:
            raise ValueError("Cannot register: " + "; ".join(errors))
        # Immutable registration: a new study needs a new output path.
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2)
        print(json.dumps({"study_id": result["study_id"], "manifest_hash": digest(result), "registered": str(args.output)}))
        return 0
    else:
        if not args.manifest or not args.rows:
            parser.error("grade requires --manifest and --rows")
        manifest, payload = json.loads(args.manifest.read_text()), json.loads(args.rows.read_text())
        result = evaluate_study(manifest, payload["rows"], phase=args.phase, now=now,
                                complete_weeks=payload.get("complete_weeks", []))
    output = args.output or (Path("artifacts/nfl_dfs_context_variant_study.json") if args.action == "context" else None)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    printed = {"output":str(output),"forecasts":len(result.get("forecasts",[])),"covered":sum(f.get("covered",False) for f in result.get("forecasts",[])),"production_changed":False} if args.action == "freeze-pickem" else result
    if args.action in ("prospective","coherent") and output:
        printed = {**result, "output": str(output), "studies": [{key:value for key,value in study.items() if key != "forecast_evidence"} for study in result["studies"]]}
    print(json.dumps(printed, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
