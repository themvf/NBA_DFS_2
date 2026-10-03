"""The possession challenger must keep future results out of pregame inputs."""

from research.cfb_forecast_v1 import replay_games
from tests.test_cfb_forecast_v1 import _fixture


def test_challenger_features_use_only_earlier_games():
    games, plays, drives = _fixture()
    before = replay_games(games, plays, drives)[0]
    games[2]["home_score"] = 99
    drives[(3, 1)] = 25
    after = replay_games(games, plays, drives)[0]
    assert before["home_v2_features"] == after["home_v2_features"]
    assert before["away_v2_features"] == after["away_v2_features"]
    assert before["home_expected_drives"] == after["home_expected_drives"]


def test_opponent_adjusted_ppa_is_frozen_before_kickoff():
    games, plays, drives = _fixture()
    before = replay_games(games, plays, drives)[0]
    assert before["home_v3_features"][2:4] != before["home_v2_features"][2:4]
    plays[(3, 1)]["off_ppa"] = 100
    games[2]["away_score"] = 99
    after = replay_games(games, plays, drives)[0]
    assert before["home_v3_features"] == after["home_v3_features"]
    assert before["away_v3_features"] == after["away_v3_features"]
