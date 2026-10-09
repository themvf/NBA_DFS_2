"""Registered, paired matchup forecast grading. Verdicts never activate models."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from hashlib import sha256
import json
from math import isfinite, log
from pathlib import Path

from model.nfl_dfs_context_variant_study import paired_interval, timestamp

VERSION = "nfl-matchup-study-v1"
KINDS = ("pickem", "dfs_mean", "dfs_distribution")
FLOORS = {"pickem": (8, 100, 100), "dfs_mean": (8, 200, 50), "dfs_distribution": (8, 200, 50)}
REQUIRED = ("study_id", "hypothesis", "kind", "consumer", "cohort", "feature_versions",
            "source_versions", "candidate_config_hash", "baseline_config_hash", "registered_at",
            "development_start_at", "development_end_at", "tuning_mode", "holdout_start_at", "holdout_end_at", "family_size",
            "seed", "bootstrap_draws", "fallback", "rollback", "gates", "scoring_version",
            "calibration_bins", "affected_positions", "components", "status")


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False).encode()).hexdigest()


def implementation_digest(content):
    """Source identity is portable across Git LF/CRLF checkout conventions."""
    return sha256(content.replace(b"\r\n", b"\n")).hexdigest()


def validate_manifest(manifest):
    errors = [f"missing {key}" for key in REQUIRED if key not in manifest or manifest[key] is None]
    if errors:
        return errors
    if manifest["kind"] not in KINDS:
        errors.append("unsupported study kind")
        return errors
    if manifest["status"] != "registered":
        errors.append("study is not registered")
    for key in ("candidate_config_hash", "baseline_config_hash"):
        value = manifest[key]
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            errors.append(f"{key} must be a frozen SHA256")
    try:
        development_start, development_end, start, end, registered = (timestamp(manifest[k]) for k in
            ("development_start_at", "development_end_at", "holdout_start_at", "holdout_end_at", "registered_at"))
        if not development_start < development_end < start < end or registered > start:
            errors.append("registration and chronological development/holdout boundaries overlap")
        if manifest["tuning_mode"] != "fixed_prior_no_tuning":
            errors.append("v1 requires fixed prior settings with no invented tuning split")
    except (ValueError, TypeError):
        errors.append("invalid timezone-aware study boundaries")
    if not isinstance(manifest["family_size"], int) or manifest["family_size"] < 1:
        errors.append("positive preregistered multiplicity family_size required")
    if not isinstance(manifest["bootstrap_draws"], int) or manifest["bootstrap_draws"] < 2000:
        errors.append("at least 2000 bootstrap draws required")
    if not manifest["feature_versions"] or not manifest["source_versions"] or not manifest["components"]:
        errors.append("exact feature/source versions and candidate components are required")
    bins = manifest["calibration_bins"]
    if not isinstance(bins, list) or len(bins) < 3 or bins[0] != 0 or bins[-1] != 1 or any(a >= b for a, b in zip(bins, bins[1:])):
        errors.append("fixed increasing calibration bins must span 0 to 1")
    gates = manifest["gates"]
    floors = FLOORS[manifest["kind"]]
    for key, floor in zip(("min_weeks", "min_rows", "min_games"), floors):
        if not isinstance(gates.get(key), int) or gates[key] < floor:
            errors.append(f"{key} must be at least {floor}")
    required_gates = ("primary_materiality", "primary_metric")
    if manifest["kind"] == "pickem":
        required_gates += ("brier_harm", "calibration_harm", "tie_model_version")
    else:
        required_gates += ("mean_harm", "position_mean_harm", "p90_harm", "boom_thresholds", "boom_brier_harm", "coverage_harm", "width_harm", "interval_alphas")
        if not manifest["affected_positions"]:
            errors.append("affected DFS positions must be declared")
    errors.extend(f"missing gate {k}" for k in required_gates if k not in gates)
    minimum_materiality = .001 if manifest["kind"] == "pickem" else .01 if manifest["kind"] == "dfs_distribution" else .05
    if not _number(gates.get("primary_materiality")) or gates["primary_materiality"] < minimum_materiality:
        errors.append("primary materiality cannot relax the specification")
    margins = {"brier_harm":.002,"calibration_harm":.01} if manifest["kind"] == "pickem" else {"mean_harm":.10,"position_mean_harm":.10,"p90_harm":.05}
    for key, ceiling in margins.items():
        if not _number(gates.get(key)) or not 0 <= gates[key] <= ceiling:
            errors.append(f"{key} cannot relax the specification")
    if manifest["rollback"].get("weeks") != 8 or manifest["rollback"].get("rule") != "primary_delta_lower_ci_above_zero_or_guardrail_harm":
        errors.append("new-family rollback must preserve the eight-week fixed endpoint")
    if manifest["kind"] == "dfs_distribution" and gates.get("interval_alphas") != [.2]:
        errors.append("v1 scorer supports the registered central 80% interval only")
    if manifest["kind"] == "pickem" and gates.get("primary_metric") != "three_class_log_loss":
        errors.append("pickem primary must be three_class_log_loss")
    if manifest["kind"] == "dfs_mean" and gates.get("primary_metric") != "mean_absolute_error":
        errors.append("DFS mean primary must be mean_absolute_error")
    if manifest["kind"] == "dfs_distribution" and gates.get("primary_metric") != "weighted_interval_score":
        errors.append("DFS distribution primary must be weighted_interval_score")
    return errors


def _number(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and isfinite(value)


def _probabilities(values):
    if set(values) != {"home", "away", "tie"} or not all(_number(p) and 0 <= p <= 1 for p in values.values()):
        raise ValueError("invalid three-class probabilities")
    if abs(sum(values.values())-1) > 1e-8:
        raise ValueError("probabilities must sum to one")
    clipped = {k: max(1e-6, min(1-1e-6, v)) for k, v in values.items()}
    total = sum(clipped.values())
    return {k: v/total for k, v in clipped.items()}


def _ece(rows, arm, bins):
    errors = []
    for label in ("home", "away", "tie"):
        total = 0.
        for low, high in zip(bins, bins[1:]):
            group = [r for r in rows if low <= r[arm]["probabilities"][label] and
                     (r[arm]["probabilities"][label] < high or high == 1)]
            if group:
                total += abs(sum(r[arm]["probabilities"][label]-int(r["actual"] == label) for r in group))
        errors.append(total/len(rows))
    return sum(errors)/3


def _arm_metrics(arm, actual, kind, boom_threshold=None):
    if kind == "pickem":
        p = _probabilities(arm["probabilities"])
        if actual not in p:
            raise ValueError("unknown outcome class")
        return {"primary": -log(p[actual]), "brier": sum((p[k]-int(k == actual))**2 for k in p)}
    if not _number(actual) or not all(_number(arm.get(k)) for k in ("mean", "median", "p10", "p90", "boom_probability")):
        raise ValueError("missing exact actual or forecast distribution")
    if not arm["p10"] <= arm["median"] <= arm["p90"] or not 0 <= arm["boom_probability"] <= 1:
        raise ValueError("invalid interval or boom probability")
    error = actual-arm["p90"]
    alpha = .2
    width = arm["p90"]-arm["p10"]
    interval_score = width + 2/alpha*max(arm["p10"]-actual, 0) + 2/alpha*max(actual-arm["p90"], 0)
    wis = (.5*abs(actual-arm["median"]) + alpha/2*interval_score)/1.5
    return {"primary": abs(actual-arm["mean"]) if kind == "dfs_mean" else wis,
            "mae": abs(actual-arm["mean"]), "p90_pinball": max(.9*error, -.1*error),
            "coverage": float(arm["p10"] <= actual <= arm["p90"]), "width": width,
            "boom_brier": (arm["boom_probability"]-int(actual >= boom_threshold))**2}


def evaluate_study(manifest, records, *, phase="promotion", now=None, complete_weeks=()):
    """Rows share frozen baseline/candidate inputs; every rejection is counted.

    Required row envelope: study_id, forecast_id, game_id, player_id (DFS),
    season/week, position, captured_at, kickoff, available_at, decision_cutoff,
    input_manifest_hash, candidate_config_hash, baseline_config_hash,
    scoring_version, actual/scoring_status, baseline/candidate, covered.
    A complete-week list must come from the schedule/outcome settlement job.
    """
    errors = validate_manifest(manifest)
    report = {"version": VERSION, "study_id": manifest.get("study_id"), "manifest_hash": digest(manifest),
              "grader_implementation_hash": implementation_digest(Path(__file__).read_bytes()),
              "phase": phase, "verdict": "NO_VERDICT", "production_promotion": False,
              "registration_errors": errors, "rejected": {}, "n": 0, "weeks": 0}
    if errors:
        report["reason"] = "incomplete or unregistered study manifest"
        return report
    if phase not in ("promotion", "rollback"):
        raise ValueError("unknown phase")
    now = timestamp(now) if now else None
    start, end = timestamp(manifest["holdout_start_at"]), timestamp(manifest["holdout_end_at"])
    if phase == "rollback":
        if not manifest.get("promoted_at") or not manifest.get("rollback_end_at"):
            report["reason"] = "rollback requires frozen promotion and endpoint timestamps"
            return report
        start, end = timestamp(manifest["promoted_at"]), timestamp(manifest["rollback_end_at"])
    selected, rejected = {}, Counter()
    complete = {tuple(w) for w in complete_weeks}
    kind, gates = manifest["kind"], manifest["gates"]
    for raw in records:
        row = deepcopy(raw)
        try:
            if row["study_id"] != manifest["study_id"]:
                raise ValueError("other study")
            capture, cutoff, available, kick = (timestamp(row[k]) for k in ("captured_at", "decision_cutoff", "available_at", "kickoff"))
            if not available <= cutoff <= capture < kick or not start <= kick < end or (now and capture > now):
                raise ValueError("ineligible as-of window")
            if capture < timestamp(manifest["registered_at"]):
                raise ValueError("forecast preceded registration")
            if manifest.get("implementation_hashes"):
                if capture < timestamp(manifest["implementation_pinned_at"]) or row.get("implementation_hashes") != manifest["implementation_hashes"]:
                    raise ValueError("implementation pin mismatch or forecast preceded pin")
            if row.get("scoring_version") != manifest["scoring_version"]:
                raise ValueError("incompatible scoring")
            input_hash = row.get("input_manifest_hash", "")
            if len(input_hash) != 64 or any(c not in "0123456789abcdef" for c in input_hash) or any(row.get(k) != manifest[k] for k in ("candidate_config_hash", "baseline_config_hash")):
                raise ValueError("unfrozen input or config mismatch")
            if row.get("covered") is False and row["candidate"] != row["baseline"]:
                raise ValueError("missing-feature fallback changed baseline")
            if kind == "pickem":
                _probabilities(row["baseline"]["probabilities"])
                _probabilities(row["candidate"]["probabilities"])
                if row["baseline"]["probabilities"]["tie"] != row["candidate"]["probabilities"]["tie"]:
                    raise ValueError("tie component differs")
            key = (row["season"], row["week"], row["game_id"], row.get("player_id") if kind != "pickem" else None)
            rank = (capture, str(row["forecast_id"]))
            if key not in selected or rank > selected[key][0]:
                selected[key] = rank, row
        except (KeyError, ValueError, TypeError) as exc:
            rejected[str(exc)] += 1
    pairs = []
    for _, row in selected.values():
        try:
            if row.get("scoring_status") != "exact" or row.get("actual") is None:
                raise ValueError("unscored")
            if not row.get("result_at") or not row.get("result_digest") or row.get("result_id") is None:
                raise ValueError("missing immutable outcome evidence")
            if (row["season"], row["week"]) not in complete:
                raise ValueError("week not completely scorable")
            if now and timestamp(row["result_at"]) > now:
                raise ValueError("result not yet available")
            threshold = gates.get("boom_thresholds", {}).get(row.get("position"))
            if kind != "pickem" and not _number(threshold):
                raise ValueError("missing registered position/boom threshold")
            base = _arm_metrics(row["baseline"], row["actual"], kind, threshold)
            candidate = _arm_metrics(row["candidate"], row["actual"], kind, threshold)
            pairs.append({**row, "metrics": {k: candidate[k]-v for k, v in base.items()}, "base_metrics": base, "candidate_metrics": candidate})
        except (KeyError, ValueError, TypeError) as exc:
            rejected[str(exc)] += 1
    report.update(rejected=dict(rejected), n=len(pairs), weeks=len({(r["season"], r["week"]) for r in pairs}),
                  games=len({r["game_id"] for r in pairs}), covered_n=sum(r.get("covered") is True for r in pairs),
                  frozen_rows=len(selected), frozen_covered_rows=sum(r.get("covered") is True for _,r in selected.values()),
                  baseline_config_hash=manifest["baseline_config_hash"], candidate_config_hash=manifest["candidate_config_hash"])
    scored_ids = {str(row["forecast_id"]) for row in pairs}
    evidence = []
    for _, row in sorted(selected.values(), key=lambda item: str(item[1]["forecast_id"])):
        entry = {key: row.get(key) for key in ("forecast_id", "game_id", "player_id", "season", "week", "position",
            "input_manifest_hash", "covered", "result_id", "result_digest", "scoring_status")}
        entry.update({key: timestamp(row[key]).isoformat() if row.get(key) else None for key in
            ("captured_at", "decision_cutoff", "available_at", "kickoff", "result_at")})
        entry["scored_in_report"] = str(row["forecast_id"]) in scored_ids
        evidence.append(entry)
    report.update(forecast_evidence=evidence, population_digest=digest(evidence))
    alpha = .05/manifest["family_size"]
    def metric(name, sample=pairs):
        return paired_interval([{**r, "delta": r["metrics"][name]} for r in sample],
                               draws=manifest["bootstrap_draws"], seed=manifest["seed"], alpha=alpha)
    report["interval"] = {"unit": "season/week", "family_size": manifest["family_size"], "alpha": alpha,
                           "method": "percentile cluster bootstrap; Bonferroni familywise correction"}
    if not pairs:
        report["reason"] = "no eligible paired forward outcomes"
        return report
    report["metrics"] = {name: metric(name) for name in pairs[0]["metrics"]}
    report["covered_metrics"] = {name: metric(name, [r for r in pairs if r.get("covered") is True]) for name in pairs[0]["metrics"]}
    floor = report["weeks"] >= gates["min_weeks"] and report["n"] >= gates["min_rows"] and report["games"] >= gates["min_games"]
    if not floor:
        report["reason"] = f"no verdict: {report['weeks']}/{gates['min_weeks']} weeks, {report['n']}/{gates['min_rows']} pairs, {report['games']}/{gates['min_games']} games"
        return report
    # A fixed endpoint avoids re-testing every refresh until a lucky PASS appears.
    if now is None or now < end:
        report["reason"] = "awaiting preregistered fixed evaluation endpoint"
        return report
    primary = report["metrics"]["primary"]
    checks = {}
    if kind == "pickem":
        checks["brier"] = report["metrics"]["brier"]["ci"][1] < gates["brier_harm"]
        ece_delta = _ece(pairs, "candidate", manifest["calibration_bins"])-_ece(pairs, "baseline", manifest["calibration_bins"])
        report["calibration_error_delta"] = ece_delta
        checks["calibration"] = ece_delta <= gates["calibration_harm"]
    else:
        checks["p90"] = report["metrics"]["p90_pinball"]["ci"][1] < gates["p90_harm"]
        if kind == "dfs_distribution":
            checks["mean"] = report["metrics"]["mae"]["ci"][1] < gates["mean_harm"]
            checks["boom"] = report["metrics"]["boom_brier"]["ci"][1] < gates["boom_brier_harm"]
            base_coverage = sum(r["base_metrics"]["coverage"] for r in pairs)/len(pairs)
            candidate_coverage = sum(r["candidate_metrics"]["coverage"] for r in pairs)/len(pairs)
            checks["coverage"] = abs(candidate_coverage-.8)-abs(base_coverage-.8) <= gates["coverage_harm"]
            checks["width"] = report["metrics"]["width"]["ci"][1] <= gates["width_harm"]
        position_results = {}
        for position in manifest["affected_positions"]:
            cohort = [r for r in pairs if r.get("position") == position]
            stats = metric("mae", cohort)
            position_results[position] = stats
            if stats["n"] < 100 or stats["weeks"] < 6:
                report.update(reason=f"no verdict: insufficient affected cohort {position}", position_metrics=position_results)
                return report
            checks[f"position:{position}"] = stats["ci"][1] < gates["position_mean_harm"]
        report["position_metrics"] = position_results
    report["guardrails"] = checks
    materiality = gates["primary_materiality"]
    if kind == "dfs_distribution":
        base_loss = sum(r["base_metrics"]["primary"] for r in pairs)/len(pairs)
        materiality *= base_loss
    if phase == "rollback":
        harm = report["metrics"]["brier"]["ci"][0] > gates["brier_harm"] or not checks["calibration"] if kind == "pickem" else (
            report["metrics"]["p90_pinball"]["ci"][0] > gates["p90_harm"] or
            any(value["ci"][0] > gates["position_mean_harm"] for value in report["position_metrics"].values()))
        if kind == "dfs_distribution":
            harm = harm or report["metrics"]["mae"]["ci"][0] > gates["mean_harm"] or report["metrics"]["boom_brier"]["ci"][0] > gates["boom_brier_harm"] or not checks["coverage"] or report["metrics"]["width"]["ci"][0] > gates["width_harm"]
        report["verdict"] = "ROLLBACK" if primary["ci"][0] > 0 or harm else "HOLD"
        report["reason"] = "frozen eight-week rollback comparison; activation requires the policy owner"
    else:
        passed = primary["delta"] <= -materiality and primary["ci"][1] < 0 and all(checks.values())
        uncertainty_checks = [(primary, 0.)]
        if kind == "pickem":
            uncertainty_checks.append((report["metrics"]["brier"], gates["brier_harm"]))
            observed_failure = not checks["calibration"]
        else:
            uncertainty_checks.append((report["metrics"]["p90_pinball"], gates["p90_harm"]))
            uncertainty_checks.extend((value, gates["position_mean_harm"]) for value in report["position_metrics"].values())
            observed_failure = False
            if kind == "dfs_distribution":
                uncertainty_checks.extend((report["metrics"][name], gates[margin]) for name, margin in
                    (("mae", "mean_harm"), ("boom_brier", "boom_brier_harm"), ("width", "width_harm")))
                observed_failure = not checks["coverage"]
        demonstrated_harm = any(value["ci"][0] >= margin for value, margin in uncertainty_checks)
        uncertain = (primary["delta"] <= -materiality and not observed_failure and not demonstrated_harm and
                     any(value["ci"][0] < margin <= value["ci"][1] for value, margin in uncertainty_checks))
        report["verdict"] = "PASS" if passed else "NO_VERDICT" if uncertain else "FAIL"
        report["reason"] = "registered forecast gate passed; policy activation remains separate" if passed else "insufficient primary or guardrail precision at fixed endpoint; no promotion" if uncertain else "registered materiality/uncertainty/harm gate failed"
    return report
