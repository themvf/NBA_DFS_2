import pytest

from model import nfl_team_receiving_charts as c


def _r(pid, pos, rec, yds, td=0, rush=0, rush_td=0, team="CHI"):
    return {"player_id": pid, "player_display_name": pid.upper(), "position": pos, "team": team,
            "receptions": rec, "receiving_yards": yds, "receiving_tds": td, "rushing_yards": rush,
            "rushing_tds": rush_td}


def test_player_totals_split_points():
    rows = [_r("a", "WR", 5, 110, td=1), _r("a", "WR", 2, 20), _r("b", "RB", 3, 10, rush=100, rush_td=1),
            _r("q", "QB", 0, 0, rush=30), _r("c", "TE", 2, 15), _r("d", "WR", 9, 90, team="NE")]
    ps = c.player_totals(rows, "CHI")
    assert [p["name"] for p in ps] == ["A", "B"]
    a = ps[0]
    assert a["games"] == 2 and a["receptions"] == 7 and a["rec_yards"] == 130 and a["rec_tds"] == 1
    assert a["parts"]["rec"] == 7 and a["parts"]["yds"] == 13 and a["parts"]["td"] == 6
    assert a["parts"]["other"] == pytest.approx(3.0)          # the 100-yard receiving bonus
    assert sum(a["parts"].values()) == pytest.approx(a["dk"])
    assert ps[1]["parts"]["rush"] == pytest.approx(16.0) and ps[1]["parts"]["other"] == pytest.approx(3.0)


def test_chart_writes_png(tmp_path):
    rows = [_r(f"p{i}", "WR", 3 + i, 30 * i, td=i % 2, team=t) for i in range(5) for t in ("CHI", "NE")]
    data = {t: c.player_totals(rows, t) for t in ("CHI", "NE")}
    assert c.receiving_chart(data, tmp_path / "a.png", "weeks 1-6")
    assert not c.receiving_chart({"CHI": []}, tmp_path / "b.png", "")
