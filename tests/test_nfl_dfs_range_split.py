import pytest

from research.nfl_dfs_range_split import (
    GATED,
    blind_rows,
    blind_verdict,
    forward_verdict,
    grade_rows,
    split_range,
)


def test_split_keeps_the_centre_and_takes_the_long_memory_shape():
    lo, hi = split_range(10.0, 12.0, 3.0, 24.0)
    assert lo == pytest.approx(2.5) and hi == pytest.approx(20.0)
    assert split_range(10.0, 0.0, 0.0, 5.0) is None
    k = 1.3  # the market anchor multiplies every quantile by k; the split commutes with it
    lo_k, hi_k = split_range(k * 10.0, 12.0, 3.0, 24.0)
    assert lo_k == pytest.approx(k * lo) and hi_k == pytest.approx(k * hi)


def test_blind_rows_fall_back_to_the_baseline_range():
    replay = {"rows": [
        {"season": 2020, "week": 5, "game_id": "g", "position": "RB", "actual": 8.0,
         "base": {"mean": 10.0, "p10": 4.0, "p90": 18.0},
         "half_life": {"17.0": {"mean": 0.0, "p10": 0.0, "p90": 0.0}}},
        {"season": 2020, "week": 5, "game_id": "g", "position": "WR", "actual": 8.0,
         "base": {"mean": 10.0, "p10": 4.0, "p90": 18.0},
         "half_life": {"17.0": {"mean": 8.0, "p10": 2.0, "p90": 16.0}}},
    ]}
    rows, counts = blind_rows(replay)
    assert counts["m17_not_positive"] == 1
    assert (rows[0]["cand_p10"], rows[0]["cand_p90"]) == (4.0, 18.0)
    assert rows[1]["cand_p10"] == pytest.approx(2.5) and rows[1]["cand_p90"] == pytest.approx(20.0)


def _g(hi, pos_mean=-0.01, base=(0.14, 0.12), cand=(0.11, 0.10), n=500):
    return {"n": n, "range_loss_delta": {"mean": hi - 0.02, "lo": hi - 0.04, "hi": hi},
            "coverage": {"baseline": {"below_p10": base[0], "above_p90": base[1]},
                         "candidate": {"below_p10": cand[0], "above_p90": cand[1]}},
            "by_position": {p: {"n": 10, "range_loss_delta": {"mean": pos_mean}} for p in GATED}}


def test_blind_verdict():
    assert blind_verdict({2020: _g(-0.01), 2021: _g(0.001)})["verdict"] == "KILL"
    assert blind_verdict({2020: _g(-0.01), 2021: _g(-0.02)})["verdict"] == "B_CLEARED"
    assert blind_verdict({2020: _g(-0.01, pos_mean=0.02), 2021: _g(-0.02)})["verdict"] == "INCONCLUSIVE"
    assert blind_verdict({2020: _g(-0.01, cand=(0.17, 0.12)), 2021: _g(-0.02)})["verdict"] == "INCONCLUSIVE"


def test_forward_verdict():
    assert forward_verdict("KILL", _g(0.1), 8, False)["verdict"] == "NOT_RUN"
    assert forward_verdict("B_CLEARED", _g(0.1, n=300), 8, False)["verdict"] == "WAIT"
    assert forward_verdict("B_CLEARED", _g(0.1, n=500), 5, False)["verdict"] == "WAIT"
    assert forward_verdict("B_CLEARED", _g(0.01, n=500), 8, False)["verdict"] == "PASS"  # point -0.01
    assert forward_verdict("B_CLEARED", _g(0.05, n=500), 8, False)["verdict"] == "INCONCLUSIVE"
    assert forward_verdict("B_CLEARED", _g(0.01, n=300), 8, True)["verdict"] == "PASS"  # final window grades anyway


def test_grade_rows_reads_both_arms():
    rows = [{"season": 2020, "week": w, "game_id": f"g{w}", "position": "RB", "actual": a,
             "base_p10": 5.0, "base_p90": 15.0, "cand_p10": 3.0, "cand_p90": 17.0}
            for w, a in [(1, 1.0), (2, 2.0), (3, 10.0), (4, 20.0)]]
    g = grade_rows(rows, "game_id")
    assert g["n"] == 4 and g["coverage"]["baseline"]["below_p10"] == 0.5 and g["coverage"]["candidate"]["below_p10"] == 0.5
