from datetime import date

from model.tennis_surface_elo import (
    PerformanceState,
    PRIOR,
    _eligible,
    blended_surface,
    expected,
    reliability,
    reliability_label,
    surface_bucket,
)
from ingest.repair_tennis_result_semantics import _status


def test_expected_probability_is_symmetric():
    p = expected(1600.0, 1500.0)
    assert 0.63 < p < 0.65
    assert abs(p + expected(1500.0, 1600.0) - 1.0) < 1e-12


def test_surface_reliability_and_blend_are_frozen():
    assert reliability(0) == 0.0
    assert reliability(20) == 0.5
    assert blended_surface(1600.0, 1400.0, 0) == 1600.0
    assert blended_surface(1600.0, 1400.0, 20) == 1500.0
    assert reliability_label(4) == "insufficient"
    assert reliability_label(5) == "developing"
    assert reliability_label(20) == "established"


def test_indoor_hard_updates_hard_bucket():
    assert surface_bucket("indoor_hard") == "hard"
    assert surface_bucket("grass") == "grass"


def test_walkovers_and_retirements_are_excluded():
    assert _eligible({"walkover": True, "retired": False, "completion_status": "walkover"}) == (False, "walkover")
    assert _eligible({"walkover": False, "retired": True, "completion_status": "retired"}) == (False, "retirement")
    assert _eligible({"walkover": False, "retired": False, "completion_status": "completed"}) == (True, None)


def test_prior_is_2023_only_neutral_rating():
    assert PRIOR == 1500.0


def test_tennis_data_comment_result_semantics():
    assert _status("Retired") == ("retired", True, False)
    assert _status("Walkover") == ("walkover", False, True)
    assert _status("Awarded") == ("awarded", False, False)
    assert _status("Completed") == ("completed", False, False)


def test_performance_state_is_weighted_and_trailing_365_days():
    state = PerformanceState()
    state.add((date(2024, 1, 1), 0.50, 100, 0.40, 80))
    state.add((date(2024, 6, 1), 0.75, 300, 0.60, 120))
    serve, ret, serve_n, return_n = state.view(date(2024, 6, 2))
    assert serve == 0.6875
    assert abs(ret - 0.52) < 1e-12
    assert (serve_n, return_n) == (400, 200)
    serve, ret, serve_n, return_n = state.view(date(2025, 1, 2))
    assert serve == 0.75
    assert ret == 0.60
    assert (serve_n, return_n) == (300, 120)
