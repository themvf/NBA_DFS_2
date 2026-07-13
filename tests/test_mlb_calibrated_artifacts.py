from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from model.mlb_game_total_model import (
    FEATURE_COLS as TOTAL_FEATURES,
    rolling_origin_total_residuals,
    total_distribution,
)
from model.mlb_moneyline_model import (
    FEATURE_COLS as ML_FEATURES,
    calibration_resamples,
    rolling_origin_moneyline_calibration,
)


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
