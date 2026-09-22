"""The forward promotion gate must refuse a verdict until its window is
complete, enforce per-position floors, require all three conditions, and
never flip production_promotion itself."""

from __future__ import annotations

import json
from pathlib import Path

from model.nfl_dfs_shadow_gate import DEFAULT_GATE, evaluate_forward_gate


def record(week, position, actual, base, cand, *, base_int=(0.0, 30.0), cand_int=(0.0, 30.0),
           base_boom=0.1, cand_boom=0.1, threshold=25.0):
    return {"payload": {"week": week, "season": 2026, "position": position, "baseline": base,
                        "p10": base_int[0], "p90": base_int[1], "boom_probability": base_boom,
                        "boom_threshold": threshold,
                        "candidate": {"prediction": cand, "p10": cand_int[0], "p90": cand_int[1],
                                      "boom_probability": cand_boom}},
            "outcome": {"actual": actual, "scoring_status": "exact"}}


def synthetic(weeks, per_week=20, cand_gain=2.0, position="RB"):
    rows = []
    for w in weeks:
        for i in range(per_week):
            actual = 10.0 + (i % 5)
            rows.append(record(w, position, actual, base=actual + 4.0, cand=actual + 4.0 - cand_gain))
    return rows


GATE = {**DEFAULT_GATE, "window_weeks": [3, 4, 5, 6, 7], "min_weeks": 3,
        "min_player_weeks_per_position": 40, "bootstrap_iters": 300}


def test_no_verdict_until_every_window_week_is_scorable():
    out = evaluate_forward_gate(synthetic([3, 4, 5]), GATE)
    assert out["status"] == "no_verdict" and "[6, 7]" in out["reason"]
    assert out["production_promotion"] is False
    assert "verdicts" not in out


def test_inspected_weeks_never_enter_the_window():
    rows = synthetic([1, 2]) + synthetic([3, 4, 5, 6, 7], cand_gain=-3.0)
    out = evaluate_forward_gate(rows, GATE)
    assert out["weeks_scorable"] == [3, 4, 5, 6, 7]
    assert out["cells"]["RB"]["n"] == 100                 # weeks 1-2 excluded
    assert out["verdicts"]["RB"]["verdict"] == "FAIL"     # the candidate is worse in-window


def test_pass_requires_all_three_conditions_and_floors():
    out = evaluate_forward_gate(synthetic([3, 4, 5, 6, 7]), GATE)
    assert out["status"] == "graded"
    assert out["verdicts"]["RB"]["verdict"] == "PASS"
    assert out["verdicts"]["QB"]["verdict"] == "insufficient"
    # Worse coverage flips it even with the MAE gain.
    rows = [record(w, "RB", 12.0, 16.0, 14.0, cand_int=(13.0, 30.0)) for w in [3, 4, 5, 6, 7] for _ in range(20)]
    out = evaluate_forward_gate(rows, GATE)
    v = out["verdicts"]["RB"]
    assert v["paired_mae_ci_below_zero"] and not v["coverage_not_worse"] and v["verdict"] == "FAIL"
    # Worse boom Brier flips it too.
    rows = [record(w, "RB", 12.0, 16.0, 14.0, cand_boom=0.9) for w in [3, 4, 5, 6, 7] for _ in range(20)]
    v = evaluate_forward_gate(rows, GATE)["verdicts"]["RB"]
    assert not v["boom_brier_not_worse"] and v["verdict"] == "FAIL"


def test_gate_never_promotes_and_config_records_the_inspected_weeks():
    out = evaluate_forward_gate(synthetic([3, 4, 5, 6, 7]), GATE)
    assert out["production_promotion"] is False
    config = json.loads(Path("artifacts/nfl_dfs_shadow_config.json").read_text())
    assert config["production_promotion"] is False
    gate = config["forward_gate"]
    assert gate["inspected_weeks"] == [1, 2] and gate["window_weeks"] == list(range(3, 13))
    assert gate["min_player_weeks_per_position"] == 150 and gate["min_weeks"] == 5
