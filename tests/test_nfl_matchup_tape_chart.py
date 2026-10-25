from model import nfl_matchup_tape_chart as m


def _play(gid, team, yards, epa, td=0.0, yl=50.0, drive=1.0, pt="pass", inter=0.0):
    return {"game_id": gid, "week": 1, "season_type": "REG", "posteam": team, "play_type": pt, "yards_gained": yards,
            "epa": epa, "touchdown": td, "td_team": team if td else float("nan"), "interception": inter,
            "fumble_lost": 0.0, "yardline_100": yl, "fixed_drive": drive}


def test_team_table_counts_offense_defense_turnovers_and_red_zone():
    sched = [{"game_id": "g1", "week": 1, "gameday": "2026-09-10", "home_team": "A", "away_team": "B",
              "home_score": 14, "away_score": 3},
             {"game_id": "g2", "week": 2, "gameday": "2026-09-17", "home_team": "B", "away_team": "A",
              "home_score": None, "away_score": None}]
    pbp = [_play("g1", "A", 50, 0.5), _play("g1", "A", 10, 0.3, td=1.0, yl=10),
           _play("g1", "B", 20, -0.2, inter=1.0, drive=2.0), _play("g1", "B", 5, -0.1, yl=15, drive=3.0),
           _play("g1", "A", 0, 0.0, pt="punt")]
    t = m.team_table(pbp, sched, before_week=2)
    a, b = t["A"], t["B"]
    assert (a["wins"], a["losses"], b["losses"]) == (1, 0, 1)
    assert a["off_yds"] == 60 and a["def_yds"] == 25 and b["def_yds"] == 60
    assert a["off_td"] == 1 and a["to_margin"] == 1 and b["to_margin"] == -1
    assert a["rz_td"] == 1.0 and b["rz_td"] == 0.0
    assert abs(a["off_epa"] - 0.4) < 1e-9 and abs(a["def_epa"] + 0.15) < 1e-9
    r = m.ranks(t)
    assert r["A"]["pf"] == 1 and r["A"]["pa"] == 1 and r["B"]["def_yds"] == 2


def test_chart_renders(tmp_path):
    sched = [{"game_id": "g1", "week": 1, "gameday": "2026-09-10", "home_team": "A", "away_team": "B",
              "home_score": 14, "away_score": 3}]
    t = m.team_table([_play("g1", "A", 50, 0.5), _play("g1", "B", 20, -0.2)], sched, before_week=2)
    assert m.tape_chart("B", "A", t, m.ranks(t), tmp_path / "t.png", "context")
    assert not m.tape_chart("X", "A", t, m.ranks(t), tmp_path / "u.png", "")
