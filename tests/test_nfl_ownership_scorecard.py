"""Metric functions and label gating of the ownership scorecard grader (model/nfl_ownership_scorecard.py)."""

import math

import numpy as np
import pandas as pd
import pytest

from model import nfl_ownership_provenance as prov
from model import nfl_ownership_scorecard as sc


def test_spearman_averages_tied_ranks():
    # Ties in both vectors: average ranks give exactly 0.8 here; ordinal ranks would not.
    pred = [1, 2, 2, 3, 4]
    actual = [1, 3, 2, 3, 5]
    from scipy.stats import rankdata
    expect = np.corrcoef(rankdata(pred, "average"), rankdata(actual, "average"))[0, 1]
    assert sc.spearman_avg(pred, actual) == pytest.approx(expect)
    # A tie-blind (ordinal) ranking would differ on this input.
    ordinal = np.corrcoef(rankdata(pred, "ordinal"), rankdata(actual, "ordinal"))[0, 1]
    assert not math.isclose(expect, ordinal)


def test_spearman_undefined_cases_are_nan():
    assert math.isnan(sc.spearman_avg([1, 2], [1, 2]))
    assert math.isnan(sc.spearman_avg([1, 1, 1], [1, 2, 3]))


def test_mae_bias_rmse_sign_convention():
    m = sc.error_metrics([10, 0, 5], [7, 2, 5])
    assert m["n"] == 3
    assert m["mae"] == pytest.approx((3 + 2 + 0) / 3)
    assert m["bias"] == pytest.approx((3 - 2 + 0) / 3)          # predicted minus actual
    assert m["rmse"] == pytest.approx(math.sqrt((9 + 4) / 3))


def test_bootstrap_is_deterministic_with_a_seed_and_resamples_slates():
    rng = np.random.default_rng(1)
    samples = [sc.SlateSample(f"s{i}", rng.uniform(0, 30, 20), rng.uniform(0, 30, 20)) for i in range(5)]
    a = sc.cluster_bootstrap(samples, reps=300, seed=7)
    b = sc.cluster_bootstrap(samples, reps=300, seed=7)
    c = sc.cluster_bootstrap(samples, reps=300, seed=8)
    assert a == b
    assert a["mae"] != c["mae"]
    pooled = sc.pooled(samples)
    assert a["mae"][0] <= pooled["mae"] <= a["mae"][1]
    assert sc.cluster_bootstrap(samples[:1])["reps"] == 0          # one slate: no interval


def test_pooled_weights_players_and_averages_slate_spearman():
    s1 = sc.SlateSample("a", np.array([1.0, 2, 3]), np.array([1.0, 2, 3]))
    s2 = sc.SlateSample("b", np.array([0.0, 0, 0, 0, 0, 0]), np.array([1.0, 1, 1, 1, 1, 1]))
    p = sc.pooled([s1, s2])
    assert p["n"] == 9
    assert p["mae"] == pytest.approx(6 / 9)
    assert p["spearman_mean"] == pytest.approx(1.0)               # slate b's Spearman is undefined, skipped


def test_calibration_bins_and_top_overlap():
    bins = sc.calibration_bins([0.5, 3, 40], [0, 4, 30], sc.CALIBRATION_EDGES)
    assert [b["n"] for b in bins] == [1, 1, 0, 0, 0, 1]
    assert bins[-1]["bin"] == "35+"
    assert sc.top_k_overlap(list("abc"), [3, 2, 1], [1, 2, 3], k=2) == 1


def _record(**kw):
    base = dict(contest_id="c1", fmt="showdown", path="stat_api", full_standings=True,
                ownership={"alice": {"drafted_pct": 40.0, "drafted_by_slot": {"CPT": 10.0, "FLEX": 30.0}},
                           "bob": {"drafted_pct": 20.0, "drafted_by_slot": {"CPT": 0.0, "FLEX": 20.0}}},
                salary_pool=frozenset({"alice", "bob", "carl"}),
                captained=frozenset({"alice"}), flex_seen=frozenset({"alice", "bob"}))
    base.update(kw)
    return prov.ContestRecord(**base)


def test_excluded_classes_never_enter_metrics():
    # Mass below the floor: carl is unresolved; bob's captain zero is synthesized.
    p = prov.classify_contest(_record(), mass_floor=0.995)
    from research.nfl_ownership_scorecard import grade_contest, label_frame

    cpt = label_frame(p, "CPT")
    assert set(cpt.loc[cpt["label_class"] == "synthesized_zero", "normalized_name"]) == {"bob"}
    assert set(cpt.loc[cpt["label_class"] == "unresolved", "normalized_name"]) == {"carl"}
    df = pd.DataFrame({"normalized_name": ["alice", "bob", "carl"], "name": ["Alice", "Bob", "Carl"],
                       "position": ["QB", "WR", "WR"], "salary": [10000, 6000, 2000], "anchored": [True, False, False],
                       "is_out": [False, False, False]})
    fc = {"v": pd.DataFrame({"normalized_name": ["alice", "bob", "carl"], "CPT": [12.0, 50.0, 50.0],
                             "FLEX": [31.0, 18.0, 99.0]})}
    rows = grade_contest(df, fc, p, "showdown", diagnostics=False, relevant={"alice", "bob", "carl"})
    by = {(r["slot"], r["population"]): r for r in rows}
    cpt_row = by[("CPT", "full_pool")]
    assert cpt_row["metrics"]["n"] == 1                          # only alice: bob synthesized, carl unresolved
    assert cpt_row["metrics"]["mae"] == pytest.approx(2.0)
    flex_row = by[("FLEX", "full_pool")]
    assert flex_row["metrics"]["n"] == 2                         # carl unresolved, excluded
    assert flex_row["metrics"]["mae"] == pytest.approx((1 + 2) / 2)
    assert cpt_row["class_counts"]["synthesized_zero"] == 1 and cpt_row["class_counts"]["unresolved"] == 1


def test_out_players_are_not_graded():
    p = prov.classify_contest(_record(path="dk_export"), mass_floor=0.995)
    from research.nfl_ownership_scorecard import grade_contest

    df = pd.DataFrame({"normalized_name": ["alice", "bob", "carl"], "name": ["A", "B", "C"], "position": ["QB", "WR", "WR"],
                       "salary": [10000, 6000, 2000], "anchored": [False] * 3, "is_out": [False, False, True]})
    fc = {"v": pd.DataFrame({"normalized_name": ["alice", "bob", "carl"], "CPT": [10.0, 0.0, 0.0], "FLEX": [30.0, 20.0, 0.0]})}
    rows = grade_contest(df, fc, p, "showdown", diagnostics=False, relevant=set())
    flex = next(r for r in rows if r["slot"] == "FLEX" and r["population"] == "full_pool")
    assert flex["metrics"]["n"] == 2 and flex["metrics"]["mae"] == 0


def test_classic_port_budgets_and_cap():
    players = pd.DataFrame({
        "position": ["QB"] * 3 + ["TE"] * 2,
        "salary": [7000, 6000, 5000, 6000, 3000],
        "proj": [24.0, 20.0, 15.0, 14.0, 6.0], "dk_avg": [22.0, 19.0, 14.0, 12.0, 5.0],
        "dk_status": ["", "", "Q", "", ""], "is_out": [False, False, False, False, False]})
    v3, v4 = sc.prior_classic(players, "v3"), sc.prior_classic(players, "v4")
    assert v3[:3].sum() == pytest.approx(100, abs=0.02) and v3.max() <= 60
    assert v3[3:].sum() == pytest.approx(105, abs=0.02) and v4[3:].sum() == pytest.approx(120, abs=0.02)  # TE cap binds
    assert np.allclose(v3[:3], v4[:3])                          # v4 Classic differs from v3 only by budgets


def test_design_tags():
    assert sc.design_tag("v4", "classic", 5) == "design"
    assert sc.design_tag("v4", "showdown", 6) == "not_design"
    assert sc.design_tag("v3", "showdown", 3) == "design"
    assert sc.design_tag("v3", "classic", 3) == "not_design"
