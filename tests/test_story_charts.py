import pytest

from model import cfb_story_charts as cs
from model import nfl_market_scenarios as ns
from model import story_charts as sc


def _row(week=1, home="KC", away="DEN", hs=27, as_=20, spread=6.5, hml=-280, aml=230, total=44.5):
    return {"season": 2026, "week": week, "game_id": f"2026_{week}_{away}_{home}", "game_type": "REG",
            "home_team": home, "away_team": away, "home_score": hs, "away_score": as_, "spread_line": spread,
            "home_moneyline": hml, "away_moneyline": aml, "total_line": total, "location": "Home",
            "weekday": "Sunday", "gametime": "13:00", "roof": "outdoors", "div_game": 0,
            "home_rest": 7, "away_rest": 7, "gameday": f"2026-09-{10 + week:02d}"}


def _records(rows):
    return ns.eligible(ns.build_games(rows))


def test_bet_units_and_bankroll():
    # Home -6.5 favorite at -280 wins by 7: favorite wins both, over (47 > 44.5).
    r = _records([_row()])[0]
    u = sc.bet_units(r)
    assert u["Favorites, moneyline"] == pytest.approx(100 / 280)
    assert u["Underdogs, moneyline"] == -1.0
    assert u["Favorites, spread"] == pytest.approx(100 / 110) and u["Underdogs, spread"] == -1.0
    assert u["Overs"] == pytest.approx(100 / 110) and u["Unders"] == -1.0
    # Week 2: underdog wins outright at +230, under.
    weeks, series = sc.bankroll(_records([_row(), _row(week=2, hs=10, as_=20, total=40.5)]))
    assert weeks == [1, 2]
    assert series["Underdogs, moneyline"] == pytest.approx([-100, 130])
    assert series["Overs"][-1] == pytest.approx(100 / 110 * 100 - 100)


def test_fav_view_and_team_perspective():
    v = sc.fav_view(_records([_row()])[0])
    assert v["fav"] == "KC" and v["margin"] == 7 and v["given"] == 6.5
    t = sc.team_games(_records([_row()]))
    assert t["KC"][0]["cover"] == 1 and t["DEN"][0]["cover"] == -1 and t["DEN"][0]["opp"] == "KC"


def test_outcomes_and_calibration():
    rec = _records([_row(), _row(week=2, hs=26), _row(week=3, hs=10)])   # covered / won no cover / lost
    out = sc.outcome_rows(rec, ns.SPREAD_BANDS)
    band = next(o for o in out if o["band"] == "Spread 4 to 6.5")
    assert (band["n"], band["Won and covered"], band["Won, did not cover"], band["Lost"]) == (3, 1, 1, 1)
    cal = sc.calibration_rows(rec)
    assert len(cal) == 1 and cal[0]["n"] == 3 and cal[0]["actual"] == pytest.approx(2 / 3)
    assert cal[0]["ci"][0] < 2 / 3 < cal[0]["ci"][1]


def test_late_flip():
    base = {"home": "KC", "away": "DEN", "week": 1, "home_spread": -6.5, "vegas_total": 44.5}
    # KC led by 3 with 5:00 left (not covering), won by 10: a late cover; 27 points then 51 final, under -> over.
    x = sc.late_flip({**base, "home_score": 30, "away_score": 20, "late_home": 15, "late_away": 12})
    assert x["spread_flip"] and x["spread_late"] == -3.5 and x["spread_final"] == 3.5 and x["total_flip"]
    # Road favorite covering late, then not.
    y = sc.late_flip({**base, "home_spread": 3.0, "home_score": 21, "away_score": 20, "late_home": 14, "late_away": 20})
    assert y["fav"] == "DEN" and y["spread_late"] == 3 and y["spread_final"] == -4 and y["spread_flip"]
    assert not y["total_flip"]
    assert sc.late_flip({**base, "late_home": None, "home_score": 1, "away_score": 0, "late_away": None}) is None


def test_late_scores_from_pbp_and_cfb_steps():
    plays = [{"game_id": "g", "play_id": 1, "qtr": 4, "game_seconds_remaining": 400, "total_home_score": 10, "total_away_score": 7},
             {"game_id": "g", "play_id": 2, "qtr": 4, "game_seconds_remaining": 300, "total_home_score": 13, "total_away_score": 7},
             {"game_id": "g", "play_id": 3, "qtr": 4, "game_seconds_remaining": 120, "total_home_score": 20, "total_away_score": 7},
             {"game_id": "g", "play_id": 4, "qtr": 5, "game_seconds_remaining": 500, "total_home_score": 26, "total_away_score": 7}]
    assert ns.late_scores_from_pbp(plays) == {"g": (13, 7)}
    assert cs.late_score([(10.0, 7, 0), (55.0, 10, 0), (58.0, 17, 0)]) == (10, 0)
    assert cs.late_score([]) == (0, 0)


def test_spread_labels_keep_gap_and_order():
    ys = sc.spread_labels([0, 0.5, 1, 10], gap=2)
    assert all(b - a >= 2 - 1e-6 for a, b in zip(ys, ys[1:]))
    assert ys[-1] == pytest.approx(10)


def test_every_chart_writes_a_png(tmp_path):
    rows = [_row(week=w, home=h, away=a, hs=hs, as_=as_, spread=sp, total=t)
            for w in range(1, 5)
            for h, a, hs, as_, sp, t in (("KC", "DEN", 27, 20, 6.5, 44.5), ("BUF", "MIA", 17, 20, 3.0, 47.5),
                                         ("PHI", "DAL", 30, 10, 7.5, 41.5), ("SF", "LA", 24, 23, 2.5, 49.5))]
    rec = _records(rows)
    table = ns.breakdown_table(ns.build_games(rows))
    late = [{**g, "late_home": g["home_score"] - 7, "late_away": g["away_score"]} for g in ns.build_games(rows)]
    checks = {
        "a": sc.margin_chart(rec, tmp_path / "a.png", "NFL"),
        "b": sc.total_chart(rec, tmp_path / "b.png", "NFL"),
        "c": sc.line_miss_chart(rec, tmp_path / "c.png", "NFL"),
        "d": sc.bankroll_chart(rec, tmp_path / "d.png", "NFL"),
        "e": sc.slope_chart(table, table, tmp_path / "e.png", "NFL", "2023-25", min_games=1),
        "f": sc.calibration_chart(rec, rec, tmp_path / "f.png", "NFL", "2023-25"),
        "g": sc.team_calendar_chart(rec, tmp_path / "g.png", "NFL"),
        "h": sc.team_quadrant_chart(rec, tmp_path / "h.png", "NFL"),
        "i": sc.outcomes_chart(rec, ns.SPREAD_BANDS, tmp_path / "i.png", "NFL"),
        "j": sc.late_flips_chart(late, tmp_path / "j.png", "NFL"),
    }
    assert all(checks.values()), checks
    for name in checks:
        assert (tmp_path / f"{name}.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not sc.margin_chart([], tmp_path / "x.png", "NFL")
    assert not sc.late_flips_chart([], tmp_path / "y.png", "NFL")
