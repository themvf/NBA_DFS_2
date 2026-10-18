from model import cfb_team_ats as ta


def _g(home, away, hs, as_, spread, total=50.0, *, hconf="SEC", aconf="SEC", aclass="fbs",
       neutral=False, conf_game=None, season_type="regular", hid=None, aid=None):
    return {
        "hname": home, "aname": away, "home_team_id": hid or hash(home) % 10000,
        "away_team_id": aid or hash(away) % 10000, "home_score": hs, "away_score": as_,
        "home_spread": spread, "vegas_total": total, "hclass": "fbs", "aclass": aclass,
        "hconf": hconf, "aconf": aconf, "neutral_site": neutral,
        "conference_game": (hconf == aconf) if conf_game is None else conf_game,
        "season_type": season_type,
    }


def _row(rows, team):
    return next(r for r in rows if r["Team"] == team)


def test_away_team_perspective_flips():
    # Home -7 wins by 3: home fails to cover, away covers as a 7-point dog.
    rows = ta.team_rows([_g("A", "B", 24, 21, -7.0)])
    a, b = _row(rows, "A"), _row(rows, "B")
    assert a["SU record"] == "1-0" and a["ATS record"] == "0-1-0" and a["Avg cover margin"] == -4.0
    assert b["SU record"] == "0-1" and b["ATS record"] == "1-0-0" and b["Avg cover margin"] == 4.0
    assert b["Underdog ATS"] == "1-0-0" and b["Away ATS"] == "1-0-0"
    assert a["Favorite ATS"] == "0-1-0" and a["Home ATS"] == "0-1-0"
    assert b["ATS units"] == round(100 / 110, 2) and a["ATS units"] == -1.0
    assert rows[0]["Team"] == "B"


def test_push_on_spread_and_total():
    rows = ta.team_rows([_g("A", "B", 27, 20, -7.0, total=47.0)])
    a = _row(rows, "A")
    assert a["ATS record"] == "0-0-1" and a["Covered (%)"] is None and a["ATS units"] == 0.0
    assert a["Over-under record"] == "0-0-1" and a["Overs (%)"] is None


def test_neutral_site_is_neither_home_nor_away():
    a = _row(ta.team_rows([_g("A", "B", 30, 10, -3.0, neutral=True)]), "A")
    assert a["Home games"] == 0 and a["Away games"] == 0 and a["Favorite games"] == 1


def test_fcs_opponent_counts_for_fbs_team_only():
    rows = ta.team_rows([_g("A", "Tiny", 45, 3, -30.0, aconf="Big Sky", aclass="fcs")])
    assert [r["Team"] for r in rows] == ["A"]
    assert rows[0]["Vs FCS"] == 1 and rows[0]["ATS record"] == "1-0-0"
    assert [c["Conference"] for c in ta.conference_rows([_g("A", "Tiny", 45, 3, -30.0, aconf="Big Sky", aclass="fcs")])] == ["SEC"]


def test_missing_spread_and_postseason_excluded():
    games = [_g("A", "B", 20, 10, None), _g("A", "B", 20, 10, -3.0, season_type="postseason")]
    assert ta.team_rows(games) == []


def test_conference_aggregation():
    games = [
        _g("A", "B", 30, 20, -3.0),                                  # in-conference: one cover, one loss
        _g("A", "X", 10, 20, -6.0, aconf="Big Ten"),                 # A (SEC) home fav loses; X road dog covers
        _g("Y", "B", 14, 24, 2.5, hconf="Big Ten"),                  # B (SEC) road fav covers; Y home dog fails
    ]
    conf = {c["Conference"]: c for c in ta.conference_rows(games)}
    sec, b10 = conf["SEC"], conf["Big Ten"]
    assert sec["Team-games"] == 4 and sec["ATS record"] == "2-2-0"
    assert sec["Non-conference ATS"] == "1-1-0" and sec["Non-conference games"] == 2
    assert sec["Road favorite ATS"] == "1-0-0"
    assert b10["Non-conference ATS"] == "1-1-0" and b10["Home underdog ATS"] == "0-1-0"
    assert b10["Teams"] == 2


def test_charts_return_false_on_empty(tmp_path):
    assert ta.team_ats_chart([], tmp_path / "a.png") is False
    assert ta.conference_chart([], tmp_path / "b.png") is False


def test_charts_write(tmp_path):
    games = [_g(f"T{i}", f"U{i}", 20 + i, 20, -1.5, hconf="SEC", aconf="ACC") for i in range(15)]
    assert ta.team_ats_chart(ta.team_rows(games), tmp_path / "a.png")
    assert ta.conference_chart(ta.conference_rows(games), tmp_path / "b.png")
    assert (tmp_path / "a.png").exists() and (tmp_path / "b.png").exists()
