"""Grade the frozen context variants without changing any production policy.

The 2026-09-22 registration is authoritative: last accepted pregame capture
per player/week within one explicit study pin, exact outcomes, eight complete
forward weeks, and 2,000 week-clustered bootstrap draws (seed 20260922).
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from math import isfinite

import numpy as np

VERSION = "nfl-dfs-context-variant-study-v1"
CONTEXT_VERSION = "nfl-dfs-context-variants-v1"
NAMES = ("env_baseline", "env_trailing", "opp_carries", "interval_rq", "prior8")
POSITIONS = ("QB", "RB", "WR", "TE")
SEED, DRAWS = 20260922, 2000


def timestamp(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timezone-aware timestamps are required")
    return parsed


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def context_forecasts(base, payload):
    """Report-card adapter; retain the selected shadow identity and evidence."""
    variants = payload.get("context_variants") or {}
    if variants.get("version") != CONTEXT_VERSION:
        return []
    out = []
    for name in NAMES:
        value = variants.get(name)
        if not isinstance(value, dict) or not finite(value.get("mean")):
            continue
        out.append({**base, "variant": f"context:{name}",
                    **{key: value.get(key) for key in ("mean", "median", "p10", "p90", "boom_probability")},
                    "stat_means": {}, "context_version": variants["version"],
                    "context_inputs": value.get("inputs"), "context_variant_payload": value})
    return out


def paired_interval(rows, value_key="delta", *, draws=DRAWS, seed=SEED, alpha=.05):
    """Resample whole weeks, retaining the player-weighted paired estimand."""
    groups = defaultdict(list)
    for row in rows:
        groups[(int(row["season"]), int(row["week"]))].append(float(row[value_key]))
    if not groups:
        return {"n": 0, "weeks": 0, "delta": None, "ci": None}
    values = list(groups.values())
    sums = np.array([sum(v) for v in values])
    counts = np.array([len(v) for v in values])
    rng = np.random.default_rng(seed)
    picks = rng.integers(0, len(values), size=(draws, len(values)))
    estimates = sums[picks].sum(axis=1) / counts[picks].sum(axis=1)
    return {"n": int(counts.sum()), "weeks": len(groups), "delta": float(sums.sum()/counts.sum()),
            "ci": [float(v) for v in np.quantile(estimates, [alpha/2, 1-alpha/2])]}


def selected_records(records, study_run_id, now):
    """Do not pool pins or recover an older variant missing from the last row."""
    selected, rejected = {}, Counter()
    now = timestamp(now)
    for row in records:
        if row.get("study_run_id") != study_run_id:
            rejected["other_study_pin"] += 1
            continue
        try:
            capture, kickoff = timestamp(row["captured_at"]), timestamp(row["kickoff"])
        except (ValueError, TypeError, KeyError):
            rejected["invalid_time"] += 1
            continue
        if capture >= kickoff or capture > now:
            rejected["not_available_pregame"] += 1
            continue
        key = (row["player_id"], int(row["season"]), int(row["week"]))
        rank = (capture, int(row.get("id", 0)))
        if key not in selected or rank > selected[key][0]:
            selected[key] = (rank, row)
    return [row for _, row in selected.values()], dict(rejected)


def _bucket(n):
    return "hist_2_5" if 2 <= n <= 5 else "hist_6_16" if 6 <= n <= 16 else "hist_17_plus" if n >= 17 else "excluded"


def evaluate_context_variants(records, study_run_id, now, *, complete_weeks=None, outcome_versions=None):
    if not study_run_id:
        raise ValueError("An explicit study pin is required")
    selected, rejected = selected_records(records, study_run_id, now)
    complete_weeks = set(tuple(w) for w in (complete_weeks or []))
    paired = defaultdict(list)
    coverage = Counter()
    outcome_versions = outcome_versions or {"nfl-dk-realized-v2": {"positions": [*POSITIONS, "DST"]}}
    scoring_coverage = Counter()
    for row in selected:
        payload = row.get("payload") or {}
        variants = payload.get("context_variants") or {}
        if variants.get("version") != CONTEXT_VERSION:
            coverage["missing_or_unknown_context_schema"] += 1
            continue
        for name in NAMES:
            if isinstance(variants.get(name), dict) and finite(variants[name].get("mean")):
                coverage[name] += 1
        outcome = row.get("outcome") or {}
        actual = outcome.get("actual")
        if not finite(actual) or outcome.get("scoring_status") != "exact" or timestamp(row["kickoff"]) >= timestamp(now):
            continue
        if outcome.get("computed_at") and timestamp(outcome["computed_at"]) > timestamp(now):
            continue
        scoring_version = outcome.get("scoring_version")
        scoring_rule = outcome_versions.get(scoring_version) or {}
        if payload.get("position") not in scoring_rule.get("positions", []):
            coverage["incompatible_context_outcome_version"] += 1
            continue
        if scoring_rule.get("effective_at") and (not outcome.get("computed_at") or timestamp(outcome["computed_at"]) < timestamp(scoring_rule["effective_at"])):
            coverage["outcome_preceded_compatibility_registration"] += 1
            continue
        scoring_coverage[str(scoring_version)] += 1
        base = variants.get("env_baseline") or {}
        if not finite(base.get("mean")):
            continue
        for name in NAMES[1:]:
            value = variants.get(name) or {}
            if not finite(value.get("mean")):
                continue
            paired[name].append({"season": int(row["season"]), "week": int(row["week"]),
                "player_id": row["player_id"], "position": payload["position"],
                "history_bucket": _bucket(int(payload.get("history_games", 0))),
                "actual": actual, "base": base, "candidate": value,
                "boom_threshold": payload.get("boom_threshold"),
                "delta": abs(value["mean"]-actual)-abs(base["mean"]-actual)})
    studies = {}
    for name in NAMES[1:]:
        rows = paired[name]
        scorable = sorted({(r["season"], r["week"]) for r in rows} & complete_weeks)
        # A completed partial week remains descriptive until the schedule closes.
        window = set(scorable[:8])
        gated = [r for r in rows if (r["season"], r["week"]) in window]
        cohorts = {p: paired_interval([r for r in gated if r["position"] == p]) for p in (*POSITIONS, "DST")}
        verdict, reason = "NO_VERDICT", f"no verdict: {len(scorable)} of 8 forward weeks scorable"
        floor = all(cohorts[p]["n"] >= 100 and cohorts[p]["weeks"] >= 6 for p in POSITIONS)
        if len(scorable) >= 8 and not floor:
            reason = "no verdict: position floors require 100 scored player-weeks and 6 distinct weeks"
        elif len(scorable) >= 8 and name in ("opp_carries", "env_trailing"):
            if name == "opp_carries":
                passed = cohorts["RB"]["ci"][1] < 0 and all(cohorts[p]["ci"][0] <= 0 for p in ("QB", "WR", "TE"))
            else:
                passed = all(cohorts[p]["ci"][1] < 0 for p in POSITIONS)
            verdict = "PASS" if passed else "FAIL"
            reason = "registered paired MAE gate passed" if passed else "registered kill/harm rule; variant closed"
        elif len(scorable) >= 8 and name == "prior8":
            buckets = {b: paired_interval([r for r in gated if r["history_bucket"] == b])
                       for b in ("hist_2_5", "hist_6_16", "hist_17_plus")}
            if all(c["n"] > 0 for c in buckets.values()):
                passed = buckets["hist_2_5"]["ci"][1] < 0 and all(c["ci"][0] <= 0 for c in buckets.values())
                verdict, reason = ("PASS", "registered history-bucket gate passed") if passed else ("FAIL", "registered history-bucket kill rule")
            else:
                reason = "no verdict: missing eligible history bucket"
            cohorts["history_buckets"] = buckets
        elif len(scorable) >= 8 and name == "interval_rq":
            sample = [r for r in gated if r["history_bucket"] == "hist_17_plus"]
            valid = [r for r in sample if finite(r["boom_threshold"]) and all(finite(r[a].get(k))
                     for a in ("base", "candidate") for k in ("p10", "p90", "boom_probability"))]
            if len(valid) == len(sample) and valid:
                measures = {}
                for arm in ("base", "candidate"):
                    measures[arm] = {"coverage": sum(r[arm]["p10"] <= r["actual"] <= r[arm]["p90"] for r in valid)/len(valid),
                        "boom_brier": sum((r[arm]["boom_probability"]-int(r["actual"] >= r["boom_threshold"]))**2 for r in valid)/len(valid)}
                same_means = all(r["base"]["mean"] == r["candidate"]["mean"] for r in gated)
                passed = same_means and abs(measures["candidate"]["coverage"]-.8) < abs(measures["base"]["coverage"]-.8) and measures["candidate"]["boom_brier"] <= measures["base"]["boom_brier"]
                verdict, reason = ("PASS", "registered interval gate passed") if passed else ("FAIL", "registered interval kill rule")
                cohorts["intervals"] = measures
                ordered = sorted(valid, key=lambda r: r["base"]["mean"])
                cohorts["mean_terciles"] = [{"n": len(group), **{a: sum(r[a]["p10"] <= r["actual"] <= r[a]["p90"] for r in group)/len(group)
                    for a in ("base", "candidate")}} for group in (list(g) for g in np.array_split(ordered, 3)) if group]
            else:
                reason = "no verdict: missing interval/boom observations"
        studies[name] = {"n": len(gated), "paired_available_n": len(rows), "scorable_weeks": [list(w) for w in scorable],
                         "evaluation_weeks": [list(w) for w in sorted(window)], "cohorts": cohorts,
                         "verdict": verdict, "reason": reason}
    return {"version": VERSION, "study_run_id": study_run_id, "evaluated_at": timestamp(now).isoformat(),
            "selected_player_weeks": len(selected), "coverage": dict(coverage), "rejected": rejected,
            "outcome_version_counts": dict(scoring_coverage), "outcome_versions": outcome_versions,
            "bootstrap": {"draws": DRAWS, "seed": SEED, "unit": "season/week", "confidence": .95},
            "studies": studies, "production_promotion": False}


def freeze_health(games, records, study_run_id, now):
    """Saturday 21:35 UTC deadline, plus explicit missed per-game freezes."""
    now = timestamp(now)
    selected, _ = selected_records(records, study_run_id, now)
    grouped = defaultdict(list)
    for game in games:
        grouped[(int(game["season"]), int(game["week"]))].append(game)
    checks = []
    for (season, week), slate in sorted(grouped.items()):
        sunday = next((timestamp(g["kickoff"]) for g in slate if timestamp(g["kickoff"]).weekday() == 6), None)
        anchor = sunday or max(timestamp(g["kickoff"]) for g in slate)
        saturday = anchor - timedelta(days=(anchor.weekday()-5) % 7)
        deadline = saturday.replace(hour=21, minute=35, second=0, microsecond=0)
        valid = [r for r in selected if (int(r["season"]), int(r["week"])) == (season, week)
                 and (r.get("payload", {}).get("context_variants") or {}).get("version") == CONTEXT_VERSION
                 and finite((r["payload"]["context_variants"].get("env_baseline") or {}).get("mean"))]
        missed = [g.get("id") for g in slate if timestamp(g["kickoff"]) <= now and not any(
            timestamp(r["kickoff"]) == timestamp(g["kickoff"]) and r["payload"].get("team") in (g["home_team"], g["away_team"]) for r in valid)]
        failure = now >= deadline and not valid
        checks.append({"key": "nfl_context_variant_freeze", "season": season, "week": week,
                       "deadline": deadline.isoformat(), "study_run_id": study_run_id, "eligible_player_weeks": len(valid),
                       "missing_started_game_ids": missed, "status": "failure" if failure else "warning" if missed else "healthy" if valid else "pending"})
    return checks
