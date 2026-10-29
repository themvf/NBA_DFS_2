from datetime import datetime, timedelta, timezone

from research import nfl_game_leaders_auto as auto

NOW = datetime(2026, 10, 15, 22, 0, tzinfo=timezone.utc)


def game(gid, hours, week=6):
    return {"game_id": gid, "season": 2026, "week": week, "home": "H", "away": "A",
            "kickoff": (NOW + timedelta(hours=hours)).isoformat()}


def entry(gid, hours, tag):
    return {"game": game(gid, hours), "decision_at": (NOW - timedelta(hours=30)).isoformat(),
            "tag": tag, "sources": {"snapshot": "s", "requests": "r"}}


def test_target_week_is_the_next_game_inside_seven_days():
    snap = {"games": [game("past", -5, 5), game("tnf", 2, 6), game("next", 170, 7)]}
    assert auto.target_week(snap, NOW, 2026) == 6
    assert auto.target_week({"games": [game("far", 24 * 8, 7)]}, NOW, 2026) is None


def test_started_game_keeps_its_frozen_entry_and_new_forecast_wins_otherwise():
    week = [game("tnf", -1), game("sun", 40)]
    prior = {"season": 2026, "week": 6, "games": [entry("tnf", -1, "frozen"), entry("sun", 40, "old")]}
    passed = {"tnf": entry("tnf", -1, "new-tnf"), "sun": entry("sun", 40, "new-sun")}
    games, skipped = auto.merge(week, prior, passed, {}, [], NOW)
    assert [g["tag"] for g in games] == ["frozen", "new-sun"]
    assert skipped == []


def test_failed_gate_keeps_previous_forecast_and_says_why():
    week = [game("sun", 40), game("mon", 70)]
    prior = {"season": 2026, "week": 6, "games": [entry("sun", 40, "old")]}
    games, skipped = auto.merge(week, prior, {}, {"sun": "unassigned 7%"},
                                [{"game_id": "mon", "reason": "capture failed"}], NOW)
    assert [g["tag"] for g in games] == ["old"]
    assert skipped == [{"game_id": "sun", "reason": "kept previous forecast: unassigned 7%"},
                       {"game_id": "mon", "reason": "capture failed"}]


def test_prior_page_from_another_week_is_never_reused():
    week = [game("sun", 40)]
    prior = {"season": 2026, "week": 5, "games": [entry("sun", 40, "last-week")]}
    games, skipped = auto.merge(week, prior, {}, {}, [], NOW)
    assert games == [] and skipped[0]["reason"] == "no forecast produced"


def test_gate_isolates_a_bad_game(monkeypatch):
    def fake_publish(batch, evaluations, sources):
        f = batch["forecasts"][0]
        if f["game"]["game_id"] == "bad":
            raise ValueError("unassigned leader share above 5%")
        return {"games": [{"game": f["game"], "sources": sources}]}
    monkeypatch.setattr(auto, "publish", fake_publish)
    batch = {"forecasts": [{"game": game("ok", 5)}, {"game": game("bad", 5)}]}
    passed, failed = auto.gate_games(batch, [], {"snapshot": "s", "requests": "r"})
    assert list(passed) == ["ok"] and "unassigned" in failed["bad"]


def test_validate_rejects_a_forecast_frozen_after_kickoff():
    late = entry("tnf", -1, "x")
    late["decision_at"] = NOW.isoformat()
    try:
        auto.validate({"games": [late]}, [game("tnf", -1)])
    except ValueError as exc:
        assert "before kickoff" in str(exc)
    else:
        raise AssertionError("expected a refusal")


def moved_request(share=0.0):
    players = [{"identity": "a1", "team": "A"}, {"identity": "h1", "team": "H"},
               {"identity": "moved", "team": "A"}]
    roles = {"H": {"carries": {"h1": 0.9, "moved": share, "NEW:H:0": 0.1},
                   "targets": {"h1": 1.0, "moved": share}},
             "A": {"carries": {"a1": 1.0, "moved": 0.0}}}
    return {"players": players, "role_scenarios": roles, "scenario_evidence": {"method": "m"}}


def test_player_who_moved_between_the_two_teams_leaves_his_old_teams_field():
    req = auto.drop_moved_players(moved_request())
    assert req["role_scenarios"]["H"] == {"carries": {"h1": 0.9, "NEW:H:0": 0.1}, "targets": {"h1": 1.0}}
    assert req["role_scenarios"]["A"]["carries"] == {"a1": 1.0, "moved": 0.0}
    assert [d["action"] for d in req["scenario_evidence"]["moved_players_dropped"]] == ["carries", "targets"]


def test_moved_player_with_a_share_fails_rather_than_handing_it_on():
    import pytest
    with pytest.raises(ValueError, match="holds 0.200 of H carries"):
        auto.drop_moved_players(moved_request(0.2))


def test_request_without_moved_players_is_unchanged():
    req = moved_request()
    req["players"] = req["players"][:2]
    req["role_scenarios"]["H"]["carries"].pop("moved")
    req["role_scenarios"]["H"]["targets"].pop("moved")
    assert "moved_players_dropped" not in auto.drop_moved_players(req)["scenario_evidence"]


def test_archive_page_keeps_one_file_per_week_and_an_ordered_index(tmp_path):
    from research.nfl_game_leaders_auto import archive_page
    from research.nfl_longest_touchdown import read
    page = lambda week, at, n: {"season": 2026, "week": week, "decision_at": at, "games": [{}] * n}
    archive_page(page(7, "2026-10-21T00:00:00Z", 14), tmp_path)
    archive_page(page(8, "2026-10-28T00:00:00Z", 13), tmp_path)
    archive_page(page(8, "2026-10-29T00:00:00Z", 14), tmp_path)
    index = read(tmp_path / "index.json")["weeks"]
    assert [w["key"] for w in index] == ["2026-w08", "2026-w07"]
    assert index[0]["games"] == 14 and index[0]["decision_at"] == "2026-10-29T00:00:00Z"
    assert read(tmp_path / "2026-w08.json")["decision_at"] == "2026-10-29T00:00:00Z"


def test_recent_played_skips_games_the_player_missed():
    from research.nfl_game_leaders_auto import attach_recent_played
    games = [{"game_id": f"g{i}", "kickoff": f"2026-09-{10 + i:02d}T17:00:00Z", "away": "NE", "home": "X",
              "completed": True} for i in range(7)]
    box = lambda gid, ident, tgt, yds: {"game_id": gid, "team": "NE", "identity": ident, "carries": 0,
                                         "targets": tgt, "receptions": 1, "rushing_yards": 0, "receiving_yards": yds}
    boxes = [box(f"g{i}", "teammate", 3, 20) for i in range(7)]
    boxes += [box("g0", "wr", 8, 90), box("g5", "wr", 10, 27), box("g6", "wr", 6, 48), box("g4", "wr", 0, 0)]
    entry = {"game": {"kickoff": "2026-10-01T17:00:00Z"},
             "metrics": {"receiving_yards": {"players": [
                 {"identity": "wr", "team": "NE", "residual": False},
                 {"identity": "OTHER:NE", "team": "NE", "residual": True}]}}}
    p = attach_recent_played(entry, {"games": games, "boxes": boxes})["metrics"]["receiving_yards"]["players"][0]
    # g0 falls outside the last six team games; g4 had no target, so it is not a game played.
    assert (p["recent_team_games"], p["recent_played_games"], p["recent_played_mean"]) == (6, 2, 37.5)
