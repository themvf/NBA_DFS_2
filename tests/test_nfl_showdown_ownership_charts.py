from model import nfl_showdown_ownership_charts as s


def test_pick_flagships_keeps_biggest_full_contest_per_game():
    contests = [
        {"contest_id": "1", "contest_name": "NFL Showdown $1.5M Monday Night Showdown (WAS @ SF)", "week": 6,
         "entry_count": 87000, "mass": 599.2},
        {"contest_id": "2", "contest_name": "NFL Showdown $10K Small (WAS @ SF)", "week": 6, "entry_count": 900,
         "mass": 599.9},
        {"contest_id": "dfsdb-x", "contest_name": "NFL Showdown $300K (WAS @ SF)", "week": 6, "entry_count": 99999,
         "mass": 600.0},
        {"contest_id": "3", "contest_name": "NFL Showdown $1.5M Thursday Night Showdown (SEA @ DEN)", "week": 6,
         "entry_count": 84000, "mass": 500.0},
        {"contest_id": "4", "contest_name": "contest-standings-4", "week": 6, "entry_count": 50000, "mass": 600.0}]
    got = s.pick_flagships(contests)
    assert [g["contest_id"] for g in got] == ["1"]
    assert got[0]["label"] == "Wk 6 Mon WAS @ SF"


def test_players_and_summary():
    rows = [{"player_name": "A", "drafted_pct": 80.0, "drafted_by_slot": {"CPT": 30.0, "FLEX": 50.0}, "fpts": 10.0},
            {"player_name": "A", "drafted_pct": 70.0, "drafted_by_slot": '{"CPT": 20, "FLEX": 50}', "fpts": 10.0},
            {"player_name": "B", "drafted_pct": 5.0, "drafted_by_slot": {"CPT": 1.0, "FLEX": 4.0}, "fpts": 30.0},
            {"player_name": "C", "drafted_pct": 9.0, "drafted_by_slot": None, "fpts": None}]
    ps = s.players(rows)
    assert [p["name"] for p in ps] == ["A", "B"] and ps[0]["cpt"] == 30.0
    g = s.game_summary(ps)
    assert g["best"]["name"] == "B" and g["chalk_cpt"]["name"] == "A"


def test_charts_write_pngs(tmp_path):
    ps = [{"name": f"Player {i}", "own": float(i * 9), "cpt": float(i), "flex": float(i * 8), "pts": float(i * 3)}
          for i in range(10)]
    games = [{"label": f"Wk {w} Mon A @ B", "week": w, "entry_count": 1000, "players": ps} for w in (1, 2, 3)]
    assert s.by_game_chart(games, tmp_path / "a.png")
    assert s.pooled_chart(games, tmp_path / "b.png")
    assert s.captain_chart(games, tmp_path / "c.png")
    assert (tmp_path / "c.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not s.by_game_chart([], tmp_path / "x.png")
