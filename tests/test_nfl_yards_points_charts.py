import pytest

from model import nfl_yards_points_charts as y


def _p(gid, team, opp, yards, play="run", drive=1, yl=50, td=0, home=None, hs=21, as_=10, **kw):
    home = home or team
    away = opp if home == team else team
    row = {"game_id": gid, "week": 1, "season_type": "REG", "posteam": team, "home_team": home, "away_team": away,
           "home_score": hs, "away_score": as_, "play_type": play, "yards_gained": yards, "interception": 0,
           "fumble_lost": 0, "field_goal_attempt": 0, "yardline_100": yl, "fixed_drive": drive, "touchdown": td,
           "td_team": team if td else None}
    row.update(kw)
    return row


def test_team_offense_counts_yards_points_and_red_zone():
    pbp = [_p("g1", "KC", "LV", 30), _p("g1", "KC", "LV", 15, play="pass", yl=15, td=1),
           _p("g1", "KC", "LV", 5, play="penalty"),
           _p("g1", "KC", "LV", 0, play="field_goal", drive=2, yl=18, field_goal_attempt=1),
           _p("g1", "LV", "KC", -7, play="pass", home="KC", drive=3, interception=1),
           _p("g1", "LA", "KC", 9, season_type="POST")]
    t = y.team_offense(pbp)
    assert t["KC"]["yards"] == 45 and t["KC"]["points"] == 21 and t["LV"]["points"] == 10
    assert t["KC"]["rz_trips"] == 2 and t["KC"]["rz_td"] == 0.5 and t["KC"]["fgs"] == 1
    assert t["LV"]["yards"] == -7 and t["LV"]["turnovers"] == 1 and t["LV"]["rz_td"] is None
    assert t["KC"]["ypp"] == pytest.approx(45 / 21) and "LAR" not in t


def test_fit_and_gaps():
    teams = {"A": {"yards": 300, "points": 20}, "B": {"yards": 400, "points": 30}, "C": {"yards": 350, "points": 20}}
    a, b = y.fit(teams)
    g = y.gaps(teams)
    assert b > 0 and g["C"] < 0 and sum(g.values()) == pytest.approx(0)


def test_charts_write_pngs(tmp_path):
    teams = {f"T{i}": {"games": 6, "yards": 250 + 10 * i, "points": 15 + i * (0.7 + (i % 3) / 3), "turnovers": i % 3,
                       "fgs": 2.0, "rz_trips": 3.0, "rz_td": 0.5 + i / 40, "ypp": None} for i in range(10)}
    for v in teams.values():
        v["ypp"] = v["yards"] / v["points"]
    assert y.scatter_chart(teams, tmp_path / "a.png", "weeks 1-6")
    assert y.ranked_chart(teams, tmp_path / "b.png", "weeks 1-6")
    assert (tmp_path / "b.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not y.scatter_chart({}, tmp_path / "x.png", "")
