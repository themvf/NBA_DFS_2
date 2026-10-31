from research.nba_points_workload_conflicts import decision_team, evaluate_candidate, grade
from ingest.nba_rosters import changes, parse_roster

TEAM = {"id": "28", "displayName": "Toronto Raptors", "abbreviation": "TOR"}


def athlete(aid, name, status="active"):
    return {"id": aid, "fullName": name, "status": {"type": status}}


def test_parse_roster_keys_and_skips_incomplete():
    rows = parse_roster(TEAM, {"athletes": [athlete("1", "Scottie Barnes"), {"id": "2"}]})
    assert len(rows) == 1 and rows[0]["player_key"] == "scottiebarnes" and rows[0]["team"] == "Toronto Raptors"


def test_changes_append_only_on_change_and_record_departures():
    cur = parse_roster(TEAM, {"athletes": [athlete("1", "Scottie Barnes"), athlete("2", "RJ Barrett")]})
    assert len(changes(cur, {})) == 2
    latest = {r["espn_athlete_id"]: r for r in cur}
    assert changes(cur, latest) == []
    moved = [dict(cur[0], espn_team_id="2", team="Boston Celtics")]
    diff = changes(moved, latest)
    assert {d["espn_athlete_id"]: d["team"] for d in diff} == {"1": "Boston Celtics", "2": None}
    assert [d["status"] for d in diff if d["team"] is None] == ["not_on_roster"]
    # a departure is recorded once, not every run
    latest2 = dict(latest, **{d["espn_athlete_id"]: d for d in diff})
    assert changes(moved, latest2) == []


def test_decision_team_states():
    assert decision_team(["Toronto Raptors"], None) == ("unverified", None)
    assert decision_team(["Toronto Raptors"], {"team": "Toronto Raptors"})[0] == "verified"
    assert decision_team(["Toronto Raptors"], {"team": "Boston Celtics"})[0] == "team_changed"
    assert decision_team(["Toronto Raptors", "Boston Celtics"], {"team": "Boston Celtics"})[0] == "team_changed"


def test_candidate_carries_roster_team():
    hist = [{"game_date": f"2026-10-2{i}", "source_game_id": f"g{i}", "fga": 18, "minutes": 30, "team": "Toronto Raptors"}
            for i in range(3)]
    c = {"player_key": "p", "opportunity_forecasts": {"fga": 12.0, "minutes": 30}, "history": hist}
    r = evaluate_candidate(c, "2026-10-30", roster={"p": {"team": "Toronto Raptors"}})
    assert r["decision_time_team"] == "verified" and r["roster_team"] == "Toronto Raptors"


def test_grade_summary_counts_ties_and_unresolved_separately():
    rec = {"game_date": "2026-10-30", "snapshot_sha256": "x", "conflicts": [
        {"player_key": "a", "forecast_fga": 12.0}, {"player_key": "b", "forecast_fga": 12.0},
        {"player_key": "c", "forecast_fga": 12.0}, {"player_key": "d", "forecast_fga": 12.0}]}
    g = grade(rec, {"a": {"played": True, "fga": 15}, "b": {"played": True, "fga": 12}, "c": None})
    assert (g["conflicts"], g["resolved"], g["above_forecast"]) == (4, 2, 1)
    assert [o["outcome"] for o in g["outcomes"]] == ["above_forecast", "tie", "unresolved", "unresolved"]
