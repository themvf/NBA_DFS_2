import math

from model import nfl_defense_correlation_charts as c


def test_defense_stats_per_game():
    base = {"season_type": "REG", "home_team": "LA", "away_team": "SF"}
    pbp = [base | {"game_id": "g1", "week": 1, "defteam": "LA", "sack": 1, "interception": 0,
                   "home_score": 20, "away_score": 17},
           base | {"game_id": "g1", "week": 1, "defteam": "SF", "sack": 0, "interception": 1,
                   "home_score": 20, "away_score": 17},
           base | {"game_id": "g2", "week": 7, "defteam": "LA", "sack": 1, "interception": 1,
                   "home_score": 3, "away_score": 3}]
    s = c.defense_stats(pbp, {1})
    assert s["LAR"] == {"games": 1, "sacks": 1.0, "ints": 0.0, "points": 17.0}
    assert s["SF"]["points"] == 20.0 and s["SF"]["ints"] == 1.0


def test_fisher_ci_and_correlations():
    lo, hi = c.fisher_ci(0.5, 32)
    assert lo < 0.5 < hi and math.isclose(math.tanh(math.atanh(0.5)), 0.5)
    assert c.fisher_ci(None, 32) is None and c.fisher_ci(0.5, 3) is None
    effects = {pos: {f"T{i}": float(i) for i in range(6)} for pos, _ in c.ROWS}
    stats = {f"T{i}": {"sacks": float(i), "ints": float(-i), "points": 1.0 + (i % 2)} for i in range(6)}
    corr = c.correlations(effects, stats)
    assert math.isclose(corr[("QB", "sacks")]["r"], 1.0) and math.isclose(corr[("RB", "ints")]["r"], -1.0)
    assert corr[("WR", "points")]["n"] == 6


def test_charts_write_pngs(tmp_path):
    effects = {pos: {f"T{i}": float(i % 5) for i in range(10)} for pos, _ in c.ROWS}
    stats = {f"T{i}": {"sacks": float(i), "ints": float(i % 3), "points": 20.0 + i} for i in range(10)}
    corr = c.correlations(effects, stats)
    assert c.matrix_chart(corr, tmp_path / "a.png", "weeks 1-6")
    assert c.scatter_chart(corr, tmp_path / "b.png", "weeks 1-6")
    assert (tmp_path / "a.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
