import math

from model import nfl_matchup_deep_charts as d

NAN = float("nan")


def _play(gid, off, dfn, yards, epa, *, dropback=0.0, sack=0.0, inter=0.0, fumble=0.0, rec=NAN, down=1.0,
          conv=0.0, fail=0.0, cpoe=NAN, passer=NAN, pt=None):
    return {"game_id": gid, "week": 1, "season_type": "REG", "posteam": off, "defteam": dfn,
            "play_type": pt or ("pass" if dropback else "run"), "yards_gained": yards, "epa": epa,
            "qb_dropback": dropback, "sack": sack, "interception": inter, "fumble": fumble,
            "fumble_recovery_1_team": rec, "third_down_converted": conv, "third_down_failed": fail, "down": down,
            "cpoe": cpoe, "passer_player_name": passer}


def _sched():
    return [{"game_id": "g1", "week": 1, "gameday": "2026-09-10", "home_team": "A", "away_team": "B",
             "home_score": 24, "away_score": 20},
            {"game_id": "g2", "week": 2, "gameday": "2026-09-17", "home_team": "B", "away_team": "A",
             "home_score": None, "away_score": None}]


def test_team_stats_units_luck_and_quarterbacks():
    pbp = [
        _play("g1", "A", "B", 25, 0.8, dropback=1.0, cpoe=10.0, passer="A.QB", down=3.0, conv=1.0),
        _play("g1", "A", "B", -7, -1.0, dropback=1.0, sack=1.0, passer="A.QB"),
        _play("g1", "A", "B", 4, 0.1, down=3.0, fail=1.0),
        _play("g1", "B", "A", 0, -2.0, dropback=1.0, inter=1.0, cpoe=-20.0, passer="B.QB"),
        _play("g1", "B", "A", 3, -0.5, fumble=1.0, rec="A"),
        _play("g1", "A", "B", 2, 0.2, fumble=1.0, rec="A"),
    ]
    t = d.team_stats(pbp, _sched(), before_week=2)
    a, b = t["A"], t["B"]
    assert (a["wins"], b["losses"]) == (1, 1)
    assert a["one_score"] == (1, 0, 0) and a["one_score_pct"] == 1.0
    assert a["fumbles"] == (2, 2) and b["fumbles"] == (0, 2) and b["fum_rec"] == 0.0
    assert math.isclose(a["pass_off"], -0.1) and math.isclose(b["pass_def"], -0.1)
    assert math.isclose(a["rush_off"], 0.15)
    assert a["sack_off"] == 0.5 and b["sack_def"] == 0.5
    assert a["expl_off"] == 1 / 3                       # sacks are not plays for explosive-rate purposes
    assert a["third_off"] == 0.5 and b["third_def"] == 0.5
    assert a["qb_name"] == "A.QB" and a["qb_cpoe"] == 10.0 and b["qb_int"] == 1.0
    assert a["pyth_pct"] > 0.5 > b["pyth_pct"]
    r = d.ranks(t)
    assert r["A"]["pass_off"] == 1 and r["A"]["def_epa"] == 1 and r["B"]["qb_int"] == 2


def test_charts_render(tmp_path):
    pbp = [_play("g1", "A", "B", 25, 0.8, dropback=1.0, passer="A.QB"),
           _play("g1", "B", "A", 5, -0.2, dropback=1.0, passer="B.QB")]
    t = d.team_stats(pbp, _sched(), before_week=2)
    r = d.ranks(t)
    assert d.luck_chart("B", "A", t, r, tmp_path / "l.png")
    assert d.edge_chart("B", "A", t, r, tmp_path / "e.png", [], [], "A")
    assert not d.luck_chart("X", "A", t, r, tmp_path / "x.png")
