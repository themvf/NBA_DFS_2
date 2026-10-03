"""Leakage and missing-source checks for the research score forecast."""

from datetime import datetime, timedelta, timezone

from research.cfb_forecast_v1 import american_probability, replay_games


def _fixture():
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    games = []
    plays = {}
    drives = {}
    for index in range(3):
        game_id = index + 1
        games.append({
            "id": game_id, "season": 2026, "week": index + 1,
            "commence_time": start + timedelta(days=7 * index),
            "completed": True, "home_team_id": 1, "away_team_id": 2,
            "home_score": 20 + index, "away_score": 14 + index,
            "home_name": "Home", "away_name": "Away",
        })
        plays[(game_id, 1)] = {"ppa_plays": 70, "off_ppa": 0.2 + index}
        plays[(game_id, 2)] = {"ppa_plays": 65, "off_ppa": -0.1 - index}
        drives[(game_id, 1)] = 12
        drives[(game_id, 2)] = 11
    return games, plays, drives


def test_game_features_use_only_earlier_completed_games():
    games, plays, drives = _fixture()
    before = replay_games(games, plays, drives)
    assert len(before) == 1 and before[0]["id"] == 3
    games[2]["home_score"] = 100
    plays[(3, 1)]["off_ppa"] = 20.0
    after = replay_games(games, plays, drives)
    assert before[0]["home_features"] == after[0]["home_features"]
    assert before[0]["away_features"] == after[0]["away_features"]


def test_missing_play_feed_does_not_become_zero_efficiency():
    games, plays, drives = _fixture()
    del plays[(1, 1)]
    assert replay_games(games, plays, drives) == []


def test_moneyline_probability_requires_a_real_quote():
    assert american_probability(None) is None
    assert american_probability(0) is None
    assert round(american_probability(-150), 4) == 0.6
    assert round(american_probability(200), 4) == 0.3333
