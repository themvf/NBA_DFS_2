import pytest

from model import nfl_receiving_yards_range_charts as c


def _r(pid, week, yds, rec=1, pos="WR", team="PIT"):
    return {"player_id": pid, "player_display_name": pid.upper(), "position": pos, "team": team, "week": week,
            "receiving_yards": yds, "receptions": rec}


def test_player_ranges():
    rows = [_r("a", w, y) for w, y in ((1, 10), (2, 40), (3, 70), (4, float("nan")))]
    rows += [_r("b", 1, 5, rec=2), _r("q", 1, 0, pos="QB", rec=5), _r("c", 1, 99, team="NO", rec=9)]
    ps = c.player_ranges(rows, "PIT")
    assert [p["name"] for p in ps] == ["A"]
    a = ps[0]
    assert a["games"] == 4 and a["low"] == 0 and a["high"] == 70 and a["avg"] == pytest.approx(30.0)
    assert a["q1"] <= a["avg"] <= a["q3"]


def test_chart_writes_png(tmp_path):
    rows = [_r(f"p{i}", w, 10 * i + w, team=t) for i in range(4) for w in range(1, 5) for t in ("PIT", "NO")]
    data = {t: c.player_ranges(rows, t) for t in ("PIT", "NO")}
    assert c.range_chart(data, tmp_path / "a.png", {"PIT": "weeks 1-4", "NO": "weeks 1-4"})
    assert not c.range_chart({"PIT": []}, tmp_path / "b.png", {"PIT": ""})
