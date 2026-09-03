from datetime import datetime, timedelta, timezone

import pytest

from model.cfb_historical_signals import (
    benjamini_hochberg,
    blend_feature,
    cohort_summary,
    grade_home,
    promotion_eligible,
    reliability_label,
    season_blend_weights,
    shrunk_rate,
    snapshot_is_point_in_time,
    spread_bucket,
    walk_forward_splits,
    wilson_interval,
)


def test_home_favorite_grading_and_half_point_has_no_push() -> None:
    assert grade_home(31, 17, -14.5) == ("win", "loss")
    assert grade_home(32, 17, -14.5) == ("win", "win")
    assert grade_home(28, 14, -14.0) == ("win", "push")


def test_exact_line_and_bucket_cohorts_are_distinct() -> None:
    rows = [
        {"season": 2023, "home_score": 35, "away_score": 20, "home_spread": -14.5},
        {"season": 2024, "home_score": 27, "away_score": 14, "home_spread": -14.5},
        {"season": 2025, "home_score": 38, "away_score": 21, "home_spread": -16.5},
        {"season": 2025, "home_score": 21, "away_score": 24, "home_spread": 3.0},
        {"season": 2025, "home_score": None, "away_score": None, "home_spread": -14.5},
    ]
    exact = cohort_summary(rows, exact_home_spread=-14.5)
    bucket = cohort_summary(rows, favorite_low=14.0, favorite_high=16.5)
    assert exact.ats.n == 2
    assert (exact.ats.wins, exact.ats.losses, exact.ats.pushes) == (1, 1, 0)
    assert bucket.ats.n == 3
    assert bucket.seasons == (2023, 2024, 2025)


def test_spread_buckets_preserve_key_number_boundaries() -> None:
    assert spread_bucket(-14.5) == (14.0, 16.5, "Favorite 14.0-16.5")
    assert spread_bucket(3.0) == (3.0, 6.5, "Favorite 3.0-6.5")
    assert spread_bucket(0.0) is None


def test_wilson_interval_and_empty_sample() -> None:
    low, high = wilson_interval(5, 10)
    assert low == pytest.approx(0.2366, abs=0.001)
    assert high == pytest.approx(0.7634, abs=0.001)
    assert wilson_interval(0, 0) == (None, None)


def test_team_rate_is_shrunk_toward_cohort() -> None:
    assert shrunk_rate(2, 0, 0.51) == pytest.approx((2 + 20 * 0.51) / 22)
    assert shrunk_rate(0, 0, 0.51) == 0.51
    assert reliability_label(2) == "VERY LOW"
    assert reliability_label(50) == "HIGH"


def test_first_week_blend_keeps_most_weight_on_prior() -> None:
    assert season_blend_weights(0) == (0, 1)
    assert season_blend_weights(1) == pytest.approx((0.2, 0.8))
    assert season_blend_weights(4) == pytest.approx((0.5, 0.5))
    assert blend_feature(10, 20, 1) == pytest.approx(18)
    assert blend_feature(None, 20, 1) == 20


def test_walk_forward_never_uses_future_season() -> None:
    splits = walk_forward_splits(range(2016, 2026))
    assert splits[0] == ((2016, 2017, 2018, 2019), 2020)
    assert splits[-1][1] == 2025
    assert all(max(train) < test for train, test in splits)


def test_benjamini_hochberg_preserves_order_and_monotonicity() -> None:
    assert benjamini_hochberg([0.01, 0.04, 0.03]) == pytest.approx([0.03, 0.04, 0.04])


def test_point_in_time_gate_rejects_late_snapshot() -> None:
    kickoff = datetime(2026, 9, 5, 16, tzinfo=timezone.utc)
    captured = kickoff - timedelta(hours=1)
    assert snapshot_is_point_in_time(captured - timedelta(minutes=1), captured, kickoff)
    assert not snapshot_is_point_in_time(kickoff, captured, kickoff)


def test_promotion_requires_every_frozen_prospective_gate() -> None:
    valid = dict(
        status="PROSPECTIVE_SHADOW", definition_frozen=True, holdout_passed=True,
        leakage_findings=0, prospective_n=100, required_prospective_n=100,
        requires_clv=True, avg_clv=0.2,
    )
    assert promotion_eligible(**valid)
    assert not promotion_eligible(**{**valid, "prospective_n": 99})
    assert not promotion_eligible(**{**valid, "avg_clv": -0.01})
    assert not promotion_eligible(**{**valid, "status": "BACKTESTED"})
