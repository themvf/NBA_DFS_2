import pytest

from model.cfb_team_features import summarize_team_games


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
