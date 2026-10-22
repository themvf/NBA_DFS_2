from model import nfl_unders_chart as u


def _g(week, away, home, line, hs, as_, season=2026, game_type="REG"):
    return {"season": season, "game_type": game_type, "week": week, "game_id": f"{season}_{week}_{away}_{home}",
            "away_team": away, "home_team": home, "total_line": line, "home_score": hs, "away_score": as_}


def test_finished_games_classifies_and_filters():
    rows = [_g(1, "NE", "SEA", 44.5, 13, 10), _g(1, "SF", "LA", 47.5, 30, 20), _g(1, "A", "B", 40.0, 20, 20),
            _g(2, "C", "D", 41.5, None, None), _g(1, "E", "F", 40.5, 10, 10, game_type="PRE"),
            _g(1, "G", "H", 40.5, 10, 10, season=2025), _g(1, "I", "J", None, 10, 10)]
    games = u.finished_games(rows)
    assert [g["result"] for g in games] == ["under", "over", "push"]
    assert games[0]["gap"] == -21.5 and games[1]["home"] == "LAR"
    s = u.week_summary(games)
    assert s == {1: {"games": 3, "under": 1, "over": 1, "push": 1}}


def test_chart_writes_png(tmp_path):
    rows = [_g(w, "A", "B", 44.5, 10 + i, 10) for w in (1, 2) for i in range(0, 40, 8)]
    games = u.finished_games(rows)
    assert u.unders_chart(games, tmp_path / "a.png")
    assert (tmp_path / "a.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not u.unders_chart([g for g in games if g["result"] != "under"], tmp_path / "b.png")


def test_overs_chart_writes_png(tmp_path):
    rows = [_g(w, "A", "B", 44.5, 30 + i, 20) for w in (1, 2) for i in range(0, 20, 5)]
    games = u.finished_games(rows)
    assert all(g["result"] == "over" for g in games)
    assert u.overs_chart(games, tmp_path / "o.png")
    assert not u.unders_chart(games, tmp_path / "u.png")
