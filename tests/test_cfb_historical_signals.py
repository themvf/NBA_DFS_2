from datetime import datetime, timedelta, timezone

import pytest

from model.cfb_historical_signals import (
    opponent_adjusted_ratings,
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


def _chain() -> list[dict]:
    """A beats B by 10 at home, B beats C by 10 at home, A beats C by 20 neutral."""
    return [
        {"home_team_id": 1, "away_team_id": 2, "home_score": 20, "away_score": 10,
         "neutral_site": False},
        {"home_team_id": 2, "away_team_id": 3, "home_score": 20, "away_score": 10,
         "neutral_site": False},
        {"home_team_id": 1, "away_team_id": 3, "home_score": 30, "away_score": 10,
         "neutral_site": True},
    ]


def test_srs_orders_teams_through_a_transitive_chain() -> None:
    adjustment = opponent_adjusted_ratings(_chain())
    assert adjustment.converged
    assert adjustment.ratings[1] > adjustment.ratings[2] > adjustment.ratings[3]
    assert adjustment.games == 3
    assert adjustment.teams == 3


def test_srs_ratings_are_centred_on_zero() -> None:
    adjustment = opponent_adjusted_ratings(_chain())
    assert sum(adjustment.ratings.values()) == pytest.approx(0.0, abs=1e-9)


def test_srs_estimates_home_field_from_the_population_not_a_constant() -> None:
    # Both sited games are 10-point home wins, so the estimate is 10.
    assert opponent_adjusted_ratings(_chain()).home_field_advantage == pytest.approx(10.0)
    flat = [
        {"home_team_id": 1, "away_team_id": 2, "home_score": 20, "away_score": 20,
         "neutral_site": False},
    ]
    assert opponent_adjusted_ratings(flat).home_field_advantage == pytest.approx(0.0)


def test_srs_separates_record_from_schedule_strength() -> None:
    """Two unbeaten teams, one of which has played nobody."""
    games = [
        # Team 1 beats the two strongest opponents by a field goal each.
        {"home_team_id": 1, "away_team_id": 2, "home_score": 23, "away_score": 20,
         "neutral_site": True},
        {"home_team_id": 1, "away_team_id": 3, "home_score": 23, "away_score": 20,
         "neutral_site": True},
        # Team 4 beats the two weakest opponents by forty each.
        {"home_team_id": 4, "away_team_id": 5, "home_score": 50, "away_score": 10,
         "neutral_site": True},
        {"home_team_id": 4, "away_team_id": 6, "home_score": 50, "away_score": 10,
         "neutral_site": True},
        # Links that establish 2 and 3 are far better than 5 and 6.
        {"home_team_id": 2, "away_team_id": 5, "home_score": 45, "away_score": 10,
         "neutral_site": True},
        {"home_team_id": 3, "away_team_id": 6, "home_score": 45, "away_score": 10,
         "neutral_site": True},
    ]
    adjustment = opponent_adjusted_ratings(games)
    assert adjustment.strength_of_schedule[1] > adjustment.strength_of_schedule[4]
    assert adjustment.ratings[1] > adjustment.ratings[4]


def test_srs_ignores_games_without_a_final_score() -> None:
    games = _chain() + [
        {"home_team_id": 1, "away_team_id": 2, "home_score": None, "away_score": None,
         "neutral_site": False},
    ]
    assert opponent_adjusted_ratings(games).games == 3


def test_srs_on_an_empty_population_reports_nothing_rather_than_zeroes() -> None:
    adjustment = opponent_adjusted_ratings([])
    assert adjustment.ratings == {}
    assert adjustment.teams == 0
    assert adjustment.converged


def test_srs_reports_a_fragmented_early_season_schedule() -> None:
    """Ratings from unlinked components are not on one comparable scale."""
    linked = [
        {"home_team_id": 1, "away_team_id": 2, "home_score": 20, "away_score": 10},
        {"home_team_id": 2, "away_team_id": 3, "home_score": 20, "away_score": 10},
    ]
    assert opponent_adjusted_ratings(linked).components == 1
    isolated_pair = linked + [
        {"home_team_id": 8, "away_team_id": 9, "home_score": 30, "away_score": 0},
    ]
    assert opponent_adjusted_ratings(isolated_pair).components == 2
    assert opponent_adjusted_ratings([]).components == 0
