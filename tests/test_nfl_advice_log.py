import pytest

from research import nfl_advice_log as L


def test_game_id_and_prices():
    assert L.game_id(2026, 6, "KC@LV") == "2026_06_KC_LV"
    assert L.game_id(2026, 5, "BUF@LAR") == "2026_05_BUF_LA"
    assert L.decimal(-150) == pytest.approx(1 + 100 / 150) and L.decimal(156) == pytest.approx(2.56)
    assert L.implied(100) == pytest.approx(0.5)


def test_profit_including_dead_heat():
    assert L.profit("won", 150) == pytest.approx(1.5)
    assert L.profit("lost", -110) == -1.0
    assert L.profit("push", -110) == 0.0 and L.profit("void", 200) == 0.0
    # Two-way dead heat at +300: half the stake at 4.0 decimal returns 2.0, profit +1.0.
    assert L.profit("dead_heat", 300, 0.5) == pytest.approx(1.0)


def test_grading_rules():
    assert L.grade_prop("over", 45.5, 73, "player_reception_yds") == "won"
    assert L.grade_prop("under", 45.5, 73, "player_reception_yds") == "lost"
    assert L.grade_prop("over", 5.0, 5, "player_receptions") == "push"
    assert L.grade_prop("yes", None, 1, "player_anytime_td") == "won"
    assert L.grade_prop("no", None, 0, "player_anytime_td") == "won"
    assert L.grade_leader(100, [100, 80, 60]) == ("won", 1.0)
    assert L.grade_leader(80, [100, 80]) == ("lost", 1.0)
    assert L.grade_leader(100, [100, 100, 3]) == ("dead_heat", 0.5)


def test_settle_pick_matches_names_and_refuses_unknowns():
    box = [{"player_display_name": "Kenneth Walker III", "rushing_yards": 128.0, "receiving_yards": 11.0},
           {"player_display_name": "Xavier Worthy", "rushing_yards": None, "receiving_yards": 73.0},
           {"player_display_name": "Omarion Hampton", "rushing_yards": 41.0, "receiving_yards": 20.0}]
    got = L.settle_pick({"player": "Kenneth Walker", "market": "player_rush_yds", "selection": "over",
                         "line": 70.5}, box)
    assert got == {"result": "won", "share": 1.0, "actual": 128.0}
    lead = L.settle_pick({"player": "Kenneth Walker III", "market": "leader_total_yards", "selection": "yes",
                          "line": None}, box)
    assert lead["result"] == "won" and lead["actual"] == 139.0
    assert L.settle_pick({"player": "Nobody", "market": "player_rush_yds", "selection": "over", "line": 1}, box) is None
    assert L.settle_pick({"player": "Xavier Worthy", "market": "other", "selection": "yes", "line": None}, box) is None
    assert L.settle_pick({"player": "Xavier Worthy", "market": "player_rush_yds", "selection": "over",
                          "line": None}, box) is None


def test_report_excludes_late_and_void():
    picks = [{"late": False, "result": "won", "price_american": 100, "fair_prob": 0.55},
             {"late": False, "result": "lost", "price_american": -110, "fair_prob": 0.5},
             {"late": False, "result": "void", "price_american": -110, "fair_prob": None},
             {"late": True, "result": "won", "price_american": 500, "fair_prob": 0.3}]
    s = L.report_rows(picks)
    assert s["graded"] == 2 and s["wins"] == 1 and s["losses"] == 1 and s["late"] == 1
    assert s["units"] == pytest.approx(0.0)
    assert s["avg_fair"] == pytest.approx(0.525)
