import math

from model import nfl_yards_win_prob_chart as y


def _g(gid, week, home, away, hs, as_):
    return {"game_id": gid, "season": 2025, "week": week, "gameday": f"2025-09-{week:02d}", "home_team": home,
            "away_team": away, "home_score": hs, "away_score": as_, "home_moneyline": -150, "away_moneyline": 130}


def test_pregame_averages_use_only_earlier_games():
    sched = [_g("g1", 1, "A", "B", 20, 10), _g("g2", 2, "B", "A", 7, 14), _g("g3", 3, "A", "B", 3, 0)]
    stat = {"g1": {"A": 400.0, "B": 200.0}, "g2": {"A": 300.0, "B": 100.0}, "g3": {"A": 9999.0, "B": 0.0}}
    rows = {r["game_id"]: r for r in y.pregame_rows(sched, stat)}
    assert rows["g1"]["home_avg"] is None and rows["g1"]["home_n"] == 0
    assert rows["g2"]["home_avg"] == 200.0 and rows["g2"]["away_avg"] == 400.0
    assert rows["g3"]["home_avg"] == 350.0 and rows["g3"]["away_avg"] == 150.0   # g3's own numbers never leak in


def test_game_stats_counts_only_offensive_touchdowns_and_records_zero():
    pbp = [
        {"game_id": "g", "season_type": "REG", "posteam": "A", "play_type": "pass", "yards_gained": 40.0,
         "touchdown": 1.0, "td_team": "A"},
        {"game_id": "g", "season_type": "REG", "posteam": "A", "play_type": "pass", "yards_gained": 0.0,
         "touchdown": 1.0, "td_team": "B"},                                     # pick-six: not A's, not B's offense
        {"game_id": "g", "season_type": "REG", "posteam": "B", "play_type": "run", "yards_gained": 5.0,
         "touchdown": 0.0, "td_team": float("nan")},
        {"game_id": "g", "season_type": "REG", "posteam": "B", "play_type": "punt", "yards_gained": 0.0,
         "touchdown": 1.0, "td_team": "A"},                                     # return TD: not offense
        {"game_id": "p", "season_type": "PRE", "posteam": "A", "play_type": "run", "yards_gained": 99.0,
         "touchdown": 1.0, "td_team": "A"},
    ]
    yards, tds = y.game_stats(pbp)
    assert yards["g"] == {"A": 40.0, "B": 5.0}
    assert tds["g"] == {"A": 1.0, "B": 0.0}
    assert "p" not in yards


def test_fit_and_predict_are_symmetric_and_directional():
    rows = []
    for i in range(200):
        d = (i % 21 - 10) * 10.0
        won_home = (i * 7919) % 100 < 100 / (1 + math.exp(-0.01 * d))
        rows.append({"home_avg": 300 + d, "away_avg": 300, "home_score": 1 if won_home else 0,
                     "away_score": 0 if won_home else 1})
    b = y.fit_slope(rows)
    assert 0 < b < 0.05
    assert y.predict(b, 350, 300) > 0.5 > y.predict(b, 300, 350)
    assert math.isclose(y.predict(b, 350, 300) + y.predict(b, 300, 350), 1.0)


def test_pick_names_the_favorite_and_calls_a_tie_even():
    assert y._pick("KC", "SEA", 0.31) == ("KC", 0.69)
    assert y._pick("KC", "SEA", 0.58) == ("SEA", 0.58)
    assert y._pick("PIT", "NO", 0.5) == ("Even", 0.5)


def test_scores_skip_ties_and_chart_renders(tmp_path):
    rows = [{"home_avg": 380, "away_avg": 300, "home_score": 24, "away_score": 17, "home_moneyline": -200,
             "away_moneyline": 170},
            {"home_avg": 300, "away_avg": 300, "home_score": 20, "away_score": 20, "home_moneyline": -110,
             "away_moneyline": -110}]
    s = y.scores(0.01, rows)
    assert s["n"] == 1 and s["stat_right"] == 1.0
    ev = {"b": 0.01, "fit": s, "oos": s}
    games = [{"away": "KC", "home": "SEA", "market_home": 0.58, "model_home": 0.63, "yards_home": 0.31,
              "tds_home": 0.55, "home_ypg": 333.0, "away_ypg": 416.0, "home_tpg": 2.4, "away_tpg": 2.1}]
    assert y.picks_chart(games, tmp_path / "c.png", "Sunday", ev, ev)
    assert not y.picks_chart([], tmp_path / "d.png", "", ev, ev)
