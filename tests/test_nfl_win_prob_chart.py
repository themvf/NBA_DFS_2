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
