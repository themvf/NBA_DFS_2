from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from model.mlb_game_total_model import (
    FEATURE_COLS as TOTAL_FEATURES,
    feature_group_availability as total_feature_group_availability,
    rolling_origin_total_residuals,
    total_distribution,
)
from model.mlb_moneyline_model import (
    FEATURE_COLS as ML_FEATURES,
    calibration_resamples,
    feature_group_availability as moneyline_feature_group_availability,
    rolling_origin_moneyline_calibration,
)
from model.mlb_validation import chronological_date_holdout, expanding_date_folds


def test_chronological_partitions_never_split_a_game_date() -> None:
    frame = pd.DataFrame({
        "id": range(12),
        "game_date": ["2026-06-01"] * 3 + ["2026-06-02"] * 3
        + ["2026-06-03"] * 3 + ["2026-06-04"] * 3,
    })
    train, test = chronological_date_holdout(frame, 0.25)
    assert set(train["game_date"]).isdisjoint(set(test["game_date"]))
    for fold_train, fold_test in expanding_date_folds(frame, folds=2):
        assert set(fold_train["game_date"]).isdisjoint(set(fold_test["game_date"]))
        assert max(fold_train["game_date"]) < min(fold_test["game_date"])


def test_moneyline_calibration_uses_out_of_fold_population_and_resamples() -> None:
    rng = np.random.default_rng(7)
    n = 620
    frame = pd.DataFrame({"id": np.arange(n), "game_date": pd.date_range("2024-01-01", periods=n)})
    for index, feature in enumerate(ML_FEATURES):
        frame[feature] = rng.normal(0, 1, n) if index else rng.uniform(0.35, 0.65, n)
    frame["home_win"] = rng.binomial(1, frame["market_home_prob"])
    calibrator, raw, outcomes = rolling_origin_moneyline_calibration(frame)
    assert 0 < len(raw) < len(frame)
    assert len(raw) == len(outcomes)
    calibrated = float(calibrator.predict([0.52])[0])
    assert 0 < calibrated < 1
    samples = calibration_resamples(raw, outcomes, 0.52, seed=12)
    assert len(samples) == 200
    assert all(0 < value < 1 for value in samples)


def test_total_distribution_is_line_specific_and_probability_complete() -> None:
    rng = np.random.default_rng(9)
    n = 620
    frame = pd.DataFrame({"id": np.arange(n), "game_date": pd.date_range("2024-01-01", periods=n)})
    for feature in TOTAL_FEATURES:
        frame[feature] = rng.normal(0, 1, n)
    frame["vegas_total"] = rng.choice([7.5, 8.0, 8.5, 9.0], n)
    frame["actual_total"] = np.maximum(0, np.rint(frame["vegas_total"] + rng.normal(0, 3.5, n)))
    residuals = rolling_origin_total_residuals(frame)
    assert 0 < len(residuals) < len(frame)
    distribution = total_distribution(8.7, 8.0, residuals, seed=15)
    assert distribution["line"] == 8.0
    assert distribution["p_over"] + distribution["p_push"] + distribution["p_under"] == pytest.approx(1)
    assert distribution["p_push"] > 0
    assert len(distribution["resamples"]) == 200
    assert all(
        sample["p_over"] + sample["p_push"] + sample["p_under"] == pytest.approx(1)
        for sample in distribution["resamples"]
    )


def test_constant_point_in_time_groups_are_not_retained() -> None:
    moneyline = pd.DataFrame({
        "market_home_prob": [0.45, 0.55], "sp_xfip_adv": [0.0, 0.0],
        "sp_k9_adv": [0.0, 0.0], "wrc_adv": [0.0, 0.0],
        "iso_adv": [0.0, 0.0], "bullpen_adv": [0.0, 0.0],
    })
    total = pd.DataFrame({
        "vegas_total": [8.0, 9.0], "home_implied": [4.0, 4.5],
        "away_implied": [4.0, 4.5], "abs_spread": [1.5, 2.0],
        "home_win_prob": [0.45, 0.55], "sp_xfip_avg": [4.2, 4.2],
        "sp_xfip_diff": [0.0, 0.0], "sp_k9_avg": [8.4, 8.4],
        "park_runs_factor": [0.95, 1.05], "temp_delta": [0.0, 0.0],
        "wind_component": [0.0, 0.0], "wrc_avg": [100.0, 100.0],
        "iso_avg": [0.165, 0.165], "bullpen_fip_avg": [4.2, 4.2],
    })

    ml_rows = {row["group"]: row for row in moneyline_feature_group_availability(moneyline, moneyline)}
    total_rows = {row["group"]: row for row in total_feature_group_availability(total, total)}

    assert ml_rows["market_baseline"]["retained"] is True
    assert ml_rows["starters"]["status"] == "not_evaluable"
    assert total_rows["park_weather"]["status"] == "not_evaluable"
    assert total_rows["lineup"]["retained"] is False
