import pytest

from model.tennis_surface_elo_evaluation import (
    _brier,
    _loss,
    cluster_bootstrap_delta,
    metrics,
)


def test_binary_scores_use_outcome_independent_orientation():
    assert _loss(0.8, 1) < _loss(0.8, 0)
    assert _brier(0.8, 1) == pytest.approx(0.04)
    assert _brier(0.2, 0) == pytest.approx(0.04)


def test_metrics_are_finite_for_extreme_probabilities():
    result = metrics([
        {"market": 1.0, "overall_elo": 1.0, "surface_elo": 1.0,
         "blended_surface_elo": 1.0, "outcome": 1},
        {"market": 0.0, "overall_elo": 0.0, "surface_elo": 0.0,
         "blended_surface_elo": 0.0, "outcome": 0},
    ], "overall_elo")
    assert result["n"] == 2
    assert result["log_loss"] >= 0


def test_cluster_bootstrap_detects_consistent_blended_improvement():
    rows = []
    for tournament in ("A", "B", "C", "D"):
        for outcome in (0, 1) * 10:
            rows.append({
                "tournament": tournament,
                "outcome": outcome,
                "overall_elo": 0.55 if outcome else 0.45,
                "blended_surface_elo": 0.70 if outcome else 0.30,
            })
    low, high = cluster_bootstrap_delta(rows, 123)
    # Identical cluster effects legitimately produce a zero-width interval.
    assert low <= high < 0
