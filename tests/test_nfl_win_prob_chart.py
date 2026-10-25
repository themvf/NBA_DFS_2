import pytest

from model import nfl_win_prob_chart as w


def test_home_win_removes_margin():
    assert w.home_win(-110, -110) == pytest.approx(0.5)
    p = w.home_win(-495, 376)
    assert 0.79 < p < 0.81
    assert w.home_win(150, -180) == pytest.approx(1 - w.home_win(-180, 150))


def test_game_rows_and_chart(tmp_path):
    raw = [{"home": "BAL", "away": "CIN", "home_ml": 131, "away_ml": -155, "open_home_ml": -183,
            "open_away_ml": 153, "spread": 3.0, "kickoff_label": "1:00 PM ET"},
           {"home": "HOU", "away": "NYG", "home_ml": -495, "away_ml": 376, "open_home_ml": None,
            "open_away_ml": None, "spread": -10.5, "kickoff_label": "1:00 PM ET"},
           {"home": "X", "away": "Y", "home_ml": None, "away_ml": None}]
    games = w.game_rows(raw)
    assert len(games) == 2
    assert games[0]["move"] == pytest.approx(w.home_win(131, -155) - w.home_win(-183, 153))
    assert games[1]["move"] is None
    assert w.win_prob_chart(games, tmp_path / "a.png", "Sunday", "now", " (Oct 18)")
    assert not w.win_prob_chart([], tmp_path / "b.png", "", "")


def test_model_rows_use_tie_free_chances_and_skip_uncovered(tmp_path):
    games = [{"home": "SEA", "away": "KC", "kickoff_label": "8:20 PM ET"},
             {"home": "LV", "away": "LAR", "kickoff_label": "4:25 PM ET"}]
    f = {"KC@SEA": {"baseline": {"home": 0.579, "away": 0.419, "tie": 0.002},
                    "candidate": {"home": 0.633, "away": 0.365, "tie": 0.002, "homeConditional": 0.634}}}
    rows = w.model_rows(games, f)
    assert len(rows) == 1
    r = rows[0]
    assert r["p_home"] == pytest.approx(0.634) and r["p_away"] == pytest.approx(0.366)
    assert r["market_home"] == pytest.approx(0.579 / 0.998)
    assert r["edge"] == pytest.approx(0.634 - 0.579 / 0.998)
    assert w.model_chart(rows, tmp_path / "m.png", "Sunday", "now")
    assert not w.model_chart([], tmp_path / "n.png", "", "")
