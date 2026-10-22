from model import nfl_game_leader_history_charts as g


def _r(pid, team, opp, week, rush=0.0, rec=0.0):
    return {"player_id": pid, "player_display_name": pid.upper(), "team": team, "opponent_team": opp, "week": week,
            "rushing_yards": rush, "receiving_yards": rec}


def test_games_need_both_teams():
    rows = [_r("a", "KC", "LV", 1), _r("b", "LV", "KC", 1), _r("c", "NE", "NYJ", 1)]
    gs = g.games(rows)
    assert list(gs) == [(1, "KC", "LV")]


def test_leaders_split_ties_and_count_played():
    rows = [_r("a", "KC", "LV", 1, rush=100), _r("b", "LV", "KC", 1, rush=100, rec=50),
            _r("a", "KC", "DEN", 2, rush=80), _r("c", "DEN", "KC", 2, rush=20, rec=float("nan"))]
    rush = g.leaders(g.games(rows), "rushing_yards")
    assert rush["a"]["led"] == 1.5 and rush["a"]["played"] == 2 and rush["b"]["led"] == 0.5
    assert rush["a"]["lead_yards"] == [100.0, 80.0]
    rec = g.leaders(g.games(rows), "receiving_yards")
    assert rec["b"]["led"] == 1.0 and "a" in rec and rec["a"]["led"] == 0.0
    top = g.ranked(rush, 1)
    assert top[0]["id"] == "a"


def test_charts_write_pngs(tmp_path):
    rows = [_r(f"p{i}", "KC", "LV", w, rush=float(i * w), rec=float(w)) for i in range(5) for w in range(1, 5)]
    rows += [_r("q", "LV", "KC", w, rush=1.0, rec=float(w * 10)) for w in range(1, 5)]
    tables = {stat: g.leaders(g.games(rows), stat) for stat, _ in g.METRICS}
    assert g.count_chart(tables, tmp_path / "a.png", "Weeks 1-4", 4)
    assert g.rate_chart(tables, tmp_path / "b.png", "Weeks 1-4")
    assert (tmp_path / "b.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
