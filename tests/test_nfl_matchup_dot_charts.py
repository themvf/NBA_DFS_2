from model import nfl_matchup_deep_charts as deep
from model import nfl_matchup_dot_charts as d
from model import nfl_matchup_tape_chart as tape

NAN = float("nan")


def _play(gid, week, off, dfn, yards, epa, *, dropback=0.0, passer=NAN, td=0.0):
    return {"game_id": gid, "week": week, "season_type": "REG", "posteam": off, "defteam": dfn,
            "play_type": "pass" if dropback else "run", "yards_gained": yards, "epa": epa, "qb_dropback": dropback,
            "sack": 0.0, "interception": 0.0, "fumble": 0.0, "fumble_recovery_1_team": NAN,
            "third_down_converted": 0.0, "third_down_failed": 0.0, "down": 1.0, "cpoe": NAN,
            "passer_player_name": passer, "touchdown": td, "td_team": off if td else NAN, "fumble_lost": 0.0,
            "yardline_100": 50.0, "fixed_drive": 1.0}


def _sched():
    return [{"game_id": "g1", "week": 1, "gameday": "2026-09-10", "home_team": "A", "away_team": "B",
             "home_score": 24, "away_score": 20},
            {"game_id": "g2", "week": 2, "gameday": "2026-09-17", "home_team": "B", "away_team": "A",
             "home_score": 10, "away_score": 13},
            {"game_id": "g3", "week": 3, "gameday": "2026-09-24", "home_team": "A", "away_team": "B",
             "home_score": None, "away_score": None}]


def _pbp():
    return [_play("g1", 1, "A", "B", 25, 0.8, dropback=1.0, passer="A.Starter"),
            _play("g1", 1, "A", "B", 5, 0.1, dropback=1.0, passer="A.Starter"),
            _play("g1", 1, "B", "A", 4, -0.2, dropback=1.0, passer="B.QB"),
            _play("g2", 2, "A", "B", 8, 0.3, dropback=1.0, passer="A.Backup"),
            _play("g2", 2, "B", "A", 3, -0.4, dropback=1.0, passer="B.QB")]


def test_quarterback_is_latest_games_starter_with_his_season_numbers():
    t = deep.team_stats(_pbp(), _sched(), before_week=3)
    assert t["A"]["qb_name"] == "A.Backup" and t["A"]["qb_dropbacks"] == 1
    assert abs(t["A"]["qb_epa"] - 0.3) < 1e-9
    assert t["B"]["qb_name"] == "B.QB" and t["B"]["qb_dropbacks"] == 2


def test_merged_ranks_put_better_further_right_whatever_the_direction():
    pbp, sched = _pbp(), _sched()
    table = d.merge(tape.team_table(pbp, sched, 3), deep.team_stats(pbp, sched, 3))
    rk = d.ranks(table)
    n = len(table)
    # A scored more and allowed less: best on both, so both sit at the right end.
    assert rk["A"]["pf"] == 1 and rk["A"]["pa"] == 1
    assert d.position(rk["A"]["pa"], n) == n and d.position(rk["B"]["pa"], n) == 1


def test_line_text_names_the_favourite_and_the_opener():
    assert d.line_text("DAL", "PHI", []) == "No line captured this week."
    assert d.line_text("DAL", "PHI", [(1, -4.5), (2, -4.0)]) == "Line: DAL -4 (opened DAL -4.5)."
    assert d.line_text("DAL", "PHI", [(1, 3.0), (2, 3.5)]) == "Line: PHI -3.5 (opened PHI -3)."


def test_charts_render(tmp_path):
    pbp, sched = _pbp(), _sched()
    table = d.merge(tape.team_table(pbp, sched, 3), deep.team_stats(pbp, sched, 3))
    rk = d.ranks(table)
    assert d.profile_chart("B", "A", table, rk, tmp_path / "p.png", "")
    assert d.matchup_chart("B", "A", table, rk, tmp_path / "m.png", [], "No line captured this week.")
    assert not d.profile_chart("X", "A", table, rk, tmp_path / "x.png", "")
