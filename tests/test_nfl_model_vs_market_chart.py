import pytest

from model import nfl_model_vs_market_chart as c


def _g(home, away, hs, as_, model_home, market_home, week=5):
    return {"week": week, "home": home, "away": away, "home_score": hs, "away_score": as_,
            "model_home": model_home, "market_home": market_home}


def test_rows_orient_on_the_team_the_model_liked_more():
    rows = c.game_rows([_g("SEA", "KC", 20, 17, 0.63, 0.58),     # model likes home SEA more
                        _g("ATL", "SF", 31, 23, 0.36, 0.43),     # model likes away SF more
                        _g("X", "Y", None, None, 0.5, 0.5)])     # unfinished: dropped
    assert [r["team"] for r in rows] == ["SF", "SEA"]            # biggest move first
    sf, sea = rows
    assert sf["model"] == pytest.approx(0.64) and sf["market"] == pytest.approx(0.57)
    assert sf["result"] == "lost" and sf["score"] == "23-31" and not sf["at_home"]
    assert sea["result"] == "won" and sea["edge"] == pytest.approx(0.05)


def test_scorecard_leaves_ties_out_and_counts_leans():
    rows = c.game_rows([_g("A", "B", 20, 10, 0.70, 0.60),   # lean A +10, won
                        _g("C", "D", 10, 20, 0.55, 0.54),   # lean C +1 (not a lean), lost
                        _g("E", "F", 17, 17, 0.60, 0.50)])  # tie
    sc = c.scorecard(rows)
    assert sc["n"] == 2 and sc["ties"] == 1
    assert sc["market_picks"] == 1 and sc["model_picks"] == 1
    assert sc["lean_n"] == 1 and sc["lean_won"] == 1
    assert sc["model_brier"] == pytest.approx(((0.7 - 1) ** 2 + 0.55 ** 2) / 2)


def test_chart_renders(tmp_path):
    rows = c.game_rows([_g("A", "B", 20, 10, 0.70, 0.60), _g("C", "D", 10, 20, 0.45, 0.50)])
    assert c.chart(rows, tmp_path / "m.png", 2026, "weeks 4-5")
    assert not c.chart([], tmp_path / "n.png", 2026, "")
