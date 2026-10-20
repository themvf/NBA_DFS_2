"""Role-change tag on flagged prop unders (research/nfl_prop_under_role_change.py)."""
from datetime import datetime, timezone

from research import nfl_prop_under_role_change as rc

GAME = "2026_06_WAS_SF"


def _request(players):
    return {"decision_at": "2026-10-19T21:38:09+00:00", "players": players}


def _p(identity, name, team, position, status="unresolved"):
    return {"identity": identity, "name": name, "team": team, "position": position, "status": status}


def _usage(rows):
    usage = {}
    for team, week, pid, targets, carries in rows:
        usage.setdefault((team, week), {})[pid] = {"targets": targets, "carries": carries}
    return usage


BET = {"game_id": GAME, "identity": "white", "week": 6}


def test_fresh_absence_same_position_material_and_played_last_game():
    req = _request([_p("white", "Rachaad White", "WSH", "RB"),
                    _p("jcm", "Jacory Croskey-Merritt", "WSH", "RB", "out")])
    usage = _usage([("WAS", w, "jcm", 1, 13) for w in range(1, 6)] + [("WAS", 5, "white", 1, 7)])
    tag = rc.tag_bet(BET, req, usage)
    assert tag["tag"] == "fresh_absence"
    assert tag["fresh"][0]["name"] == "Jacory Croskey-Merritt"
    assert tag["window_weeks"] == [1, 2, 3, 4, 5]


def test_teammate_who_missed_last_game_is_continuing_not_fresh():
    req = _request([_p("white", "Rachaad White", "WSH", "RB"),
                    _p("jcm", "Jacory Croskey-Merritt", "WSH", "RB", "out")])
    usage = _usage([("WAS", w, "jcm", 1, 13) for w in (1, 2)] + [("WAS", w, "white", 1, 7) for w in (3, 4, 5)])
    assert rc.tag_bet(BET, req, usage)["tag"] == "continuing_absence"


def test_immaterial_or_active_teammate_does_not_tag():
    req = _request([_p("white", "Rachaad White", "WSH", "RB"),
                    _p("low", "Backup Back", "WSH", "RB", "out"),
                    _p("act", "Active Back", "WSH", "RB")])
    usage = _usage([("WAS", 5, "low", 1, 2), ("WAS", 5, "act", 2, 15), ("WAS", 5, "white", 1, 7)])
    assert rc.tag_bet(BET, req, usage)["tag"] == "none"


def test_other_position_absence_is_descriptive_only():
    bet = {"game_id": GAME, "identity": "kittle", "week": 6}
    req = _request([_p("kittle", "George Kittle", "SF", "TE"),
                    _p("deebo", "Deebo Samuel Sr.", "SF", "WR", "out")])
    usage = _usage([("SF", 5, "deebo", 4, 2), ("SF", 5, "kittle", 6, 0)])
    tag = rc.tag_bet(bet, req, usage)
    assert tag["tag"] == "none"
    assert tag["cross_position_fresh"][0]["name"] == "Deebo Samuel Sr."


def test_other_team_absence_is_ignored():
    req = _request([_p("white", "Rachaad White", "WSH", "RB"),
                    _p("cmc", "Christian McCaffrey", "SF", "RB", "out")])
    usage = _usage([("SF", 5, "cmc", 5, 15), ("WAS", 5, "white", 1, 7)])
    assert rc.tag_bet(BET, req, usage)["tag"] == "none"


def test_missing_request_and_quarterback_are_not_tagged():
    assert rc.tag_bet(BET, None, {})["tag"] == "unknown"
    req = _request([_p("white", "Jayden Daniels", "WSH", "QB")])
    assert rc.tag_bet(BET, req, {})["tag"] == "not_taggable"


def test_team_code_mapping_handles_wsh_and_was():
    req = _request([_p("a", "A", "WSH", "RB"), _p("b", "B", "SF", "WR")])
    assert rc.team_codes(GAME, req) == {"SF": "SF", "WSH": "WAS"}


def test_window_uses_only_completed_weeks_before_the_game():
    usage = _usage([("SF", w, "x", 1, 1) for w in range(1, 9)])
    assert rc.usage_window(usage, "SF", 6, 5) == [1, 2, 3, 4, 5]


def test_paired_interval_and_final_verdict():
    def bet(week, pnl, tag):
        return {"week": week, "pnl": pnl, "result": "win" if pnl > 0 else "loss",
                "game_id": f"g{week}", "role_change": {"tag": tag}}
    bets = []
    for w in range(1, 11):
        bets += [bet(w, -1.0, "fresh_absence")] * 13
        bets += [bet(w, 0.9, "none"), bet(w, -1.0, "none")] * 10
    out = rc.summarize(bets, final=True)
    assert out["floors_met"] is True
    assert out["difference_interval"][1] < 0
    assert out["verdict_if_first"] == "TAGGED_WORSE"
    assert rc.summarize(bets, final=False)["verdict_if_first"] is None
    assert rc.summarize(bets[:5], final=True)["verdict_if_first"] == "INCONCLUSIVE"


def test_parent_study_files_are_only_imported():
    """The parent's grader and pins are untouched: this study adds no amendment."""
    from research import nfl_prop_under_study as under
    parent = rc.under_registration()
    assert parent["study"] == "nfl-prop-under-v2"
    assert all(under.pins_hold(parent).values())
    assert rc.REGISTRATION["parent_registration"] == "docs/nfl-prop-under-study-v2.json"
    assert rc._ts("2026-10-19T21:38:09Z") == datetime(2026, 10, 19, 21, 38, 9, tzinfo=timezone.utc)


def test_registered_constants_are_frozen():
    """Registered 2026-10-20: changing any of these is a new study version."""
    r = rc.REGISTRATION
    assert r["study"] == "nfl-prop-under-role-change-v1"
    assert r["status"] == "registered"
    assert r["registered_at"] == "2026-10-20T11:21:02+00:00"
    assert r["parent_registration"] == "docs/nfl-prop-under-study-v2.json"
    assert r["tag"] == {"window_games": 5, "material_targets": 3.0, "material_carries": 5.0,
                        "positions": ["RB", "WR", "TE"], "fb_counts_as": "RB"}
    assert r["floors"] == {"tagged_bets": 120, "tagged_game_weeks": 8}
    assert r["bootstrap"] == {"resamples": 2000, "seed": 20261020, "cluster": "game_week", "interval": 0.95}
    assert r["verdict"]["last_week"] == 18
