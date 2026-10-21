from model import nfl_prop_history_charts as h


def test_history_features_use_only_earlier_weeks():
    games = {1: 50.0, 2: 70.0, 3: 90.0, 5: 10.0}
    f = h.history_features(games, [40.0, 60.0, 80.0, 100.0], week=4)
    assert f["n_prior"] == 3 and f["season_avg"] == 70.0 and f["last_game"] == 90.0
    assert f["last3"] == 70.0 and f["last_season"] == 70.0
    thin = h.history_features({1: 50.0}, [1.0], week=2)
    assert thin["season_avg"] is None and thin["last_season"] is None and thin["last_game"] == 50.0


def test_result_and_gap_bins():
    assert h.result(None, 50.5) is None
    assert h.result(60, 50.5) == "over" and h.result(40, 50.5) == "under" and h.result(50, 50) == "push"
    assert h.gap_bin(60, 100) == "30%+ below"
    assert h.gap_bin(95, 100) == "0-15% below"
    assert h.gap_bin(130, 100) == "15%+ above"
    assert h.gap_bin(50, None) is None and h.gap_bin(50, 0) is None


def test_line_changes_pair_consecutive_lines():
    rows = [{"market": "m", "player_key": "a", "week": 1, "line": 50.0, "actual": 80.0},
            {"market": "m", "player_key": "a", "week": 3, "line": 55.0, "actual": None},
            {"market": "m", "player_key": "a", "week": 4, "line": 52.0, "actual": 40.0}]
    out = h.line_changes(rows)
    assert out == [{"market": "m", "beat": 30.0, "move": 5.0, "gap_weeks": 2}]


def test_stats_helpers():
    assert h.pearson([1, 2, 3], [2, 4, 6]) == 1.0
    assert h.slope([0, 1, 2], [1, 3, 5]) == 2.0
    lo, hi = h.wilson(5, 10)
    assert 0.2 < lo < 0.5 < hi < 0.8


def test_charts_write_pngs(tmp_path):
    rows = []
    for i in range(30):
        for market in h.MARKETS:
            line = 10.0 + i
            rows.append({"week": 1 + i % 6, "player_key": f"p{i % 5}", "name": f"P{i % 5}", "market": market,
                         "line": line, "p_over": 0.5, "actual": line + (3 if i % 2 else -3),
                         "result": "over" if i % 2 else "under", "season_avg": line * 1.1, "last3": line,
                         "last_game": line + 1, "last_season": line * 0.9, "n_prior": 3,
                         "gap": h.gap_bin(line, line * 1.1)})
    assert h.line_vs_average_chart(rows, tmp_path / "a.png", "weeks 1-6")
    assert h.over_rate_by_gap_chart(rows, tmp_path / "b.png", "weeks 1-6")
    assert h.line_follows_chart(rows, tmp_path / "c.png", "weeks 1-6")
    assert h.after_big_game_chart(h.line_changes(rows), tmp_path / "d.png", "weeks 1-6")
    assert h.player_paths_chart(rows, tmp_path / "e.png", "weeks 1-6")
    for n in "abcde":
        assert (tmp_path / f"{n}.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not h.line_vs_average_chart([], tmp_path / "x.png", "")
