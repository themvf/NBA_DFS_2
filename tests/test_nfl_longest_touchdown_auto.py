from datetime import datetime, timezone

import pytest

from research import nfl_longest_touchdown_auto as auto


NOW = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)


def test_january_games_use_the_previous_nfl_season():
    assert auto.current_nfl_season(datetime(2027, 1, 3, tzinfo=timezone.utc)) == 2026


def game(gid, kickoff, week=5):
    return {"game_id": gid, "season": 2026, "week": week, "kickoff": kickoff,
        "away": "PHI", "home": "JAX"}


def test_targets_next_upcoming_week_and_keeps_started_forecast():
    thursday = game("thursday", "2026-10-08T20:00:00+00:00")
    sunday = game("sunday", "2026-10-11T17:00:00+00:00")
    next_week = game("next", "2026-10-15T20:00:00+00:00", 6)
    snapshot = {"games": [thursday, sunday, next_week]}
    assert auto.target_week(snapshot, NOW, 2026) == 5
    prior = {"season": 2026, "weeks": [{"week": 5, "games": [
        {"game": thursday}, {"game": sunday}]}]}
    assert auto.prior_started(prior, 2026, 5, NOW) == [{"game": thursday}]
    assert auto.prior_started(prior, 2027, 5, NOW) == []
    assert auto.target_week(snapshot, datetime(2026, 10, 12, tzinfo=timezone.utc), 2026) == 6


def test_validation_rejects_missing_game_or_out_player():
    target = game("sunday", "2026-10-11T17:00:00+00:00")
    snapshot = {"games": [target]}
    forecast = {"game": target, "decisionAt": "2026-10-10T11:00:00+00:00",
        "players": [{"identity": "P1", "longestShare": .6}], "residual": [],
        "noTd": .4, "forecastRosterIds": ["P1"]}
    publication = {"season": 2026, "week": 5, "games": [forecast]}
    requests = {"sunday": {"players": [{"identity": "P1", "status": "out"}]}}
    with pytest.raises(ValueError, match="Out player"):
        auto.validate(publication, snapshot, requests, NOW)
    requests["sunday"]["players"][0]["status"] = "unresolved"
    auto.validate(publication, snapshot, requests, NOW)
    publication["games"] = []
    with pytest.raises(ValueError, match="every canonical game"):
        auto.validate(publication, snapshot, requests, NOW)
