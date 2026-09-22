"""Forward promotion gate for the shadow-only opportunity candidates (WP5).

`artifacts/nfl_dfs_shadow_config.json` held only `production_promotion: false`
and no numeric rule, so nothing could ever promote a candidate OR say it
failed. This module grades the five opportunity candidates against a frozen
rule, per position, on the forward shadow ledger:

    window       : 2026 weeks 3-12 (weeks 1-2 were inspected; excluded)
    per position : >= min_player_weeks scored player-weeks AND >= min_weeks
                   distinct weeks, else `insufficient`
    PASS         : weeks-clustered paired MAE (candidate - baseline) 95% CI
                   upper bound < 0
                   AND p10-p90 interval coverage not worse than baseline
                   AND boom Brier not worse than baseline
    graded once  : no verdict is emitted until every window week is scorable

The gate REPORTS. It never flips `production_promotion`; that remains a
deliberate config commit after the verdict is read, exactly as every other
promotion in this repo.
"""

from __future__ import annotations

import random
from collections import defaultdict

DEFAULT_GATE = {
    "season": 2026,
    "window_weeks": list(range(3, 13)),
    "inspected_weeks": [1, 2],
    "min_player_weeks_per_position": 150,
    "min_weeks": 5,
    "bootstrap_iters": 2000,
    "seed": 20260922,
    "coverage_tolerance": 0.0,
    "brier_tolerance": 0.0,
}
POSITIONS = ("QB", "RB", "WR", "TE", "DST")


def _pairs(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        p, o = r["payload"], r["outcome"]
        if not o or o.get("actual") is None or o.get("scoring_status", "exact") != "exact" or not p.get("candidate"):
            continue
        c = p["candidate"]
        actual = float(o["actual"])
        out.append({
            "week": int(p["week"]), "position": p["position"],
            "d_abs": abs(float(c["prediction"]) - actual) - abs(float(p["baseline"]) - actual),
            "cand_inside": float(c["p10"]) <= actual <= float(c["p90"]),
            "base_inside": float(p["p10"]) <= actual <= float(p["p90"]),
            "cand_brier": (float(c["boom_probability"]) - float(actual >= float(p["boom_threshold"]))) ** 2,
            "base_brier": (float(p["boom_probability"]) - float(actual >= float(p["boom_threshold"]))) ** 2,
        })
    return out


def _cell(pairs: list[dict], gate: dict) -> dict:
    by_week: dict[int, list[dict]] = defaultdict(list)
    for r in pairs:
        by_week[r["week"]].append(r)
    weeks = sorted(by_week)
    n = len(pairs)
    if not n:
        return {"n": 0, "weeks": 0}
    mean = lambda key, rs: sum(float(r[key]) for r in rs) / len(rs)
    ci = None
    if len(weeks) >= 2:
        rng = random.Random(int(gate.get("seed", DEFAULT_GATE["seed"])))
        draws = []
        for _ in range(int(gate.get("bootstrap_iters", DEFAULT_GATE["bootstrap_iters"]))):
            sample = [r for wk in (rng.choice(weeks) for _ in weeks) for r in by_week[wk]]
            if sample:
                draws.append(mean("d_abs", sample))
        if len(draws) >= 100:
            draws.sort()
            ci = [draws[int(0.025 * len(draws))], draws[int(0.975 * len(draws)) - 1]]
    return {
        "n": n, "weeks": len(weeks),
        "d_mae": mean("d_abs", pairs), "d_mae_ci": ci,
        "coverage_candidate": mean("cand_inside", pairs), "coverage_baseline": mean("base_inside", pairs),
        "boom_brier_candidate": mean("cand_brier", pairs), "boom_brier_baseline": mean("base_brier", pairs),
    }


def evaluate_forward_gate(rows: list[dict], gate: dict | None = None) -> dict:
    """rows: the deduplicated (payload, outcome) records `evaluation()` builds."""
    gate = {**DEFAULT_GATE, **(gate or {})}
    window = set(int(w) for w in gate["window_weeks"])
    pairs = [r for r in _pairs(rows) if r["week"] in window]
    scorable = {r["week"] for r in pairs}
    cells = {pos: _cell([r for r in pairs if r["position"] == pos], gate) for pos in POSITIONS}
    missing = sorted(window - scorable)
    result = {"gate_version": "nfl-dfs-shadow-forward-gate-v1", "gate": gate,
              "weeks_scorable": sorted(scorable), "cells": cells, "production_promotion": False}
    if missing:
        result["status"] = "no_verdict"
        result["reason"] = f"window incomplete; weeks not yet scorable: {missing}"
        return result
    verdicts = {}
    for pos, c in cells.items():
        if c["n"] < gate["min_player_weeks_per_position"] or c["weeks"] < gate["min_weeks"] or not c.get("d_mae_ci"):
            verdicts[pos] = {"verdict": "insufficient", "n": c["n"], "weeks": c["weeks"]}
            continue
        gain = c["d_mae_ci"][1] < 0
        coverage_ok = c["coverage_candidate"] >= c["coverage_baseline"] - gate["coverage_tolerance"]
        brier_ok = c["boom_brier_candidate"] <= c["boom_brier_baseline"] + gate["brier_tolerance"]
        verdicts[pos] = {"verdict": "PASS" if (gain and coverage_ok and brier_ok) else "FAIL",
                         "paired_mae_ci_below_zero": gain, "coverage_not_worse": coverage_ok,
                         "boom_brier_not_worse": brier_ok}
    result["status"] = "graded"
    result["verdicts"] = verdicts
    return result
