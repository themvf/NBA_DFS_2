from copy import deepcopy

import pytest

from model.nfl_longest_touchdown import Settings
from research import nfl_longest_touchdown_weekly as weekly


def game(day, away, home):
    return {"game_id": f"2026_05_{away}_{home}", "season": 2026, "week": 5,
        "kickoff": f"2026-10-{day:02}T17:00:00+00:00", "away": away, "home": home}


def request(target):
    depth = {"season": 2026, "sources": {
        "sleeper": {"snapshot_fetched_at": "2026-10-10T10:00:00+00:00"},
        "fantasypros": {"retrieved_at": "2026-10-10T11:00:00+00:00"}}}
    return {"game": target, "decision_at": "2026-10-10T12:00:00+00:00",
        "dual_depth": {target["away"]: depth, target["home"]: depth},
        "week_injuries": [{"observed_at": "2026-10-10T11:00:00+00:00"}],
        "players": [{"identity": "P1", "name": "One", "team": target["away"],
            "position": "RB", "status": "out", "sleeper_status": "OUT",
            "fantasypros_injury_status": "OUT"},
            {"identity": "P2", "name": "Two", "team": target["home"],
            "position": "WR", "status": "unresolved", "sleeper_status": "ACTIVE",
            "fantasypros_injury_status": "UNKNOWN"}]}


def test_weekly_request_excludes_dual_out_but_keeps_unresolved_conditional():
    target = game(11, "A", "B")
    result = weekly.model_request(request(target), target)
    assert [p["status"] for p in result["players"]] == ["out", "active"]
    assert result["roster_evidence"]["availability_verified"] is False
    conflict = request(target)
    conflict["players"][0]["fantasypros_injury_status"] = "QUESTIONABLE"
    with pytest.raises(ValueError, match="Inconsistent Out"):
        weekly.model_request(conflict, target)
    healthy = request(target)
    healthy["week_injuries"] = []
    healthy["week_injury_snapshot"] = {"fetched_at": "2026-10-10T11:30:00+00:00"}
    assert weekly.model_request(healthy, target)["players"]
    official = request(target)
    official["players"][1]["status"] = "out"
    official["players"][1]["official_status"] = "INACTIVE"
    assert weekly.model_request(official, target)["players"][1]["status"] == "out"


def test_weekly_publication_requires_every_game_and_preserves_prior(monkeypatch):
    thursday, sunday = game(9, "T", "D"), game(11, "A", "B")
    prior = {"game": thursday, "decisionAt": "2026-10-08T12:00:00+00:00"}
    snapshot = {"games": [thursday, sunday]}
    requests = {sunday["game_id"]: request(sunday)}
    monkeypatch.setattr(weekly, "publish_game", lambda *args: {"game": sunday, "decisionAt": requests[sunday["game_id"]]["decision_at"]})
    result = weekly.build_week(snapshot, requests, 2026, 5, Settings(draws=100), [prior])
    assert result["coverage"] == {"scheduledGames": 2, "publishedGames": 2}
    assert result["games"][0] is prior
    with pytest.raises(ValueError, match="Missing pregame request"):
        weekly.build_week(snapshot, {}, 2026, 5, Settings(draws=100), [prior])
    wrong = deepcopy(prior)
    wrong["game"]["home"] = "X"
    with pytest.raises(ValueError, match="does not match"):
        weekly.build_week(snapshot, requests, 2026, 5, Settings(draws=100), [wrong])


def test_season_index_keeps_previous_weeks():
    first = {"season": 2026, "week": 5, "games": []}
    next_week = {"season": 2026, "week": 6, "games": []}
    result = weekly.season_index(next_week, weekly.season_index(first))
    assert [item["week"] for item in result["weeks"]] == [5, 6]
    assert weekly.season_index(first, result)["weeks"][0] == first
    new_season = {"season": 2027, "week": 1, "games": []}
    rolled = weekly.season_index(new_season, result)
    assert rolled["season"] == 2027
    assert [item["week"] for item in rolled["priorSeasons"][0]["weeks"]] == [5, 6]


def test_only_modeled_contributors_trigger_a_stale_forecast():
    publication = {"players": [
        {"identity": "P1", "anyTd": .1, "longestShare": .05},
        {"identity": "P2", "anyTd": 0, "longestShare": 0}],
        "residual": [{"identity": "OTHER:A", "anyTd": .2, "longestShare": .1},
            {"identity": "P3", "anyTd": .01, "longestShare": .005}]}
    assert weekly.modeled_roster_ids(publication) == ["P1", "P3"]
