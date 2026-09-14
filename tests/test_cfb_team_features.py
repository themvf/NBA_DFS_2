from datetime import datetime, timezone

import pytest

from model.cfb_team_features import (
    adjustment_context,
    compose_team_feature,
    partition_fbs_games,
    season_adjustments,
    summarize_team_games,
)


def test_team_game_summary_orients_home_and_away() -> None:
    rows = [
        {"home_team_id": 1, "away_team_id": 2, "home_score": 35, "away_score": 14},
        {"home_team_id": 3, "away_team_id": 1, "home_score": 24, "away_score": 27},
    ]
    summary = summarize_team_games(rows, 1)
    assert summary["games"] == 2
    assert summary["points_for"] == 31
    assert summary["points_against"] == 19
    assert summary["margin"] == 12
    assert summary["win_rate"] == 1


def test_empty_team_summary_preserves_missingness() -> None:
    summary = summarize_team_games([], 1)
    assert summary["games"] == 0
    assert summary["margin"] is None
    assert summary["win_rate"] is None


def test_fbs_population_excludes_fcs_and_counts_unknown_separately() -> None:
    rows = [
        {"home_classification": "fbs", "away_classification": "fbs", "id": 1},
        {"home_classification": "FBS", "away_classification": "fbs", "id": 2},
        {"home_classification": "fbs", "away_classification": "fcs", "id": 3},
        {"home_classification": "fbs", "away_classification": None, "id": 4},
    ]
    partition = partition_fbs_games(rows)
    assert [row["id"] for row in partition["games"]] == [1, 2]
    assert partition["excluded_non_fbs"] == 1
    assert partition["excluded_unknown_classification"] == 1


def test_fcs_blowout_no_longer_inflates_a_team_summary() -> None:
    rows = [
        # The September tune-up that used to count as ordinary production.
        {"home_team_id": 1, "away_team_id": 9, "home_score": 63, "away_score": 3,
         "home_classification": "fbs", "away_classification": "fcs", "season": 2026},
        {"home_team_id": 1, "away_team_id": 2, "home_score": 20, "away_score": 17,
         "home_classification": "fbs", "away_classification": "fbs", "season": 2026},
    ]
    eligible = partition_fbs_games(rows)["games"]
    summary = summarize_team_games(eligible, 1)
    assert summary["games"] == 1
    assert summary["points_for"] == 20
    assert summary["margin"] == 3


def _srs_rows() -> list[dict]:
    return [
        {"season": 2026, "home_team_id": 1, "away_team_id": 2, "home_score": 21,
         "away_score": 14, "neutral_site": False},
        {"season": 2026, "home_team_id": 2, "away_team_id": 3, "home_score": 28,
         "away_score": 7, "neutral_site": False},
        {"season": 2025, "home_team_id": 1, "away_team_id": 3, "home_score": 10,
         "away_score": 24, "neutral_site": False},
    ]


def test_seasons_are_rated_independently() -> None:
    adjustments = season_adjustments(_srs_rows())
    assert sorted(adjustments) == [2025, 2026]
    assert adjustments[2026].games == 2
    assert adjustments[2025].games == 1


def test_adjustment_context_separates_current_season_from_prior_seasons() -> None:
    adjustments = season_adjustments(_srs_rows())
    current, prior = adjustment_context(adjustments, team_id=1, season=2026)
    assert current is not None and prior is not None
    assert current["rating"] == pytest.approx(adjustments[2026].ratings[1])
    assert prior["seasons"] == 1
    # A team absent from the current season still reports its prior context.
    missing_current, _ = adjustment_context(adjustments, team_id=99, season=2026)
    assert missing_current is None


def test_opponent_adjusted_margin_is_blended_and_population_is_summarized() -> None:
    adjustments = season_adjustments(_srs_rows())
    partition = partition_fbs_games([
        {"home_classification": "fbs", "away_classification": "fcs"},
    ])
    feature = compose_team_feature(
        game={"id": 500, "season": 2026}, team_id=1, opponent_team_id=2,
        as_of=datetime(2026, 9, 14, tzinfo=timezone.utc),
        current={"games": 1, "points_for": 21.0, "points_against": 14.0,
                 "margin": 7.0, "win_rate": 1.0},
        prior={"games": 1, "points_for": 10.0, "points_against": 24.0,
               "margin": -14.0, "win_rate": 0.0},
        roster=None, roster_confidence=0.0,
        adjustments=adjustments, population=partition,
    )
    payload = feature["features_json"]
    assert payload["blended"]["opponent_adjusted_margin"] is not None
    assert payload["opponent_adjusted"]["current_season"]["rating"] == pytest.approx(
        adjustments[2026].ratings[1]
    )
    assert payload["population"] == {
        "eligible_games": 0, "excluded_non_fbs": 1,
        "excluded_unknown_classification": 0, "definition": "fbs_versus_fbs",
    }
    # Three of four present: current season, prior window, opponent adjustment.
    # Only the roster snapshot is missing.
    assert feature["source_completeness"] == 0.75


def test_a_team_with_no_observed_history_is_not_reported_as_two_thirds_complete() -> None:
    empty = {"games": 0, "points_for": None, "points_against": None,
             "margin": None, "win_rate": None}
    feature = compose_team_feature(
        game={"id": 1, "season": 2026}, team_id=1, opponent_team_id=2,
        as_of=datetime(2026, 8, 30, tzinfo=timezone.utc),
        current=empty, prior=empty, roster=None, roster_confidence=0.0,
        adjustments={}, population=None,
    )
    assert feature["source_completeness"] == 0.0
    assert feature["feature_version"] == "cfb-team-context-v2"
