import math

from model import nfl_yards_win_prob_chart as y


def _g(gid, week, home, away, hs, as_):
    return {"game_id": gid, "season": 2025, "week": week, "gameday": f"2025-09-{week:02d}", "home_team": home,
            "away_team": away, "home_score": hs, "away_score": as_, "home_moneyline": -150, "away_moneyline": 130}


def test_pregame_averages_use_only_earlier_games():
    sched = [_g("g1", 1, "A", "B", 20, 10), _g("g2", 2, "B", "A", 7, 14), _g("g3", 3, "A", "B", 3, 0)]
    yards = {"g1": {"A": 400.0, "B": 200.0}, "g2": {"A": 300.0, "B": 100.0}, "g3": {"A": 9999.0, "B": 0.0}}
    rows = {r["game_id"]: r for r in y.pregame_rows(sched, yards)}
    assert rows["g1"]["home_ypg"] is None and rows["g1"]["home_n"] == 0
    assert rows["g2"]["home_ypg"] == 200.0 and rows["g2"]["away_ypg"] == 400.0
    assert rows["g3"]["home_ypg"] == 350.0 and rows["g3"]["away_ypg"] == 150.0   # g3's own yards never leak in


def test_fit_and_predict_are_symmetric_and_directional():
    rows = []
    for i in range(200):
        d = (i % 21 - 10) * 10.0
        won_home = (i * 7919) % 100 < 100 / (1 + math.exp(-0.01 * d))
        rows.append({"home_ypg": 300 + d, "away_ypg": 300, "home_score": 1 if won_home else 0,
                     "away_score": 0 if won_home else 1})
    b = y.fit_slope(rows)
    assert 0 < b < 0.05
    assert y.predict(b, 350, 300) > 0.5 > y.predict(b, 300, 350)
    assert math.isclose(y.predict(b, 350, 300) + y.predict(b, 300, 350), 1.0)


def test_scores_skip_ties_and_chart_renders(tmp_path):
    rows = [{"home_ypg": 380, "away_ypg": 300, "home_score": 24, "away_score": 17, "home_moneyline": -200,
             "away_moneyline": 170},
            {"home_ypg": 300, "away_ypg": 300, "home_score": 20, "away_score": 20, "home_moneyline": -110,
             "away_moneyline": -110}]
    s = y.scores(0.01, rows)
    assert s["n"] == 1 and s["yards_right"] == 1.0
    games = [{"away": "KC", "home": "SEA", "market_home": 0.58, "model_home": 0.63, "yards_home": 0.31,
              "home_ypg": 333.0, "away_ypg": 416.0}]
    assert y.yards_chart(games, tmp_path / "c.png", "Sunday", 0.01, s, {**s, "weeks": "weeks 4-7"})
    assert not y.yards_chart([], tmp_path / "d.png", "", 0.01, s, s)
