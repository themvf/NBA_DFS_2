"""Synthetic checks for the projection compression study's machinery."""
import random

from model import nfl_dfs_projection_tier_study as study


def _row(pid, game, projected, actual, cohort="hist_6_plus", position="WR"):
    return {"ff_player_id": pid, "game_key": game, "projected": projected, "actual": actual,
            "cohort": cohort, "position": position, "name": pid}


def test_leakage_guard_and_latest_eligible_run():
    runs = {"early": {"history_cutoff_season": 2026, "history_cutoff_week": 2, "as_of_at": "2026-09-20T10:00:00"},
            "later": {"history_cutoff_season": 2026, "history_cutoff_week": 2, "as_of_at": "2026-09-24T10:00:00"},
            "leaky": {"history_cutoff_season": 2026, "history_cutoff_week": 4, "as_of_at": "2026-09-28T10:00:00"}}
    reports = [
        {"season": 2026, "week": 3, "projection_run_id": "early", "rows": [_row("a", "ATL@GB", 10, 12)]},
        {"season": 2026, "week": 3, "projection_run_id": "later", "rows": [_row("a", "ATL@GB", 11, 12)]},
        {"season": 2026, "week": 3, "projection_run_id": "leaky", "rows": [_row("a", "ATL@GB", 99, 12), _row("b", "ATL@GB", 5, 1)]},
    ]
    rows = study.eligible_rows(reports, runs)
    assert len(rows) == 1, "one row per player-game; the leaky run contributes nothing"
    assert rows[0]["projected"] == 11, "the latest eligible run wins"


def test_cutoff_is_exclusive():
    """A run with cutoff week 3 used weeks 1-2 only, so it is valid for week 3."""
    runs = {"r": {"history_cutoff_season": 2026, "history_cutoff_week": 3, "as_of_at": "x"}}
    reports = [{"season": 2026, "week": 3, "projection_run_id": "r", "rows": [_row("a", "g", 10, 12)]}]
    assert len(study.eligible_rows(reports, runs)) == 1


def test_out_and_nonpositive_rows_excluded():
    runs = {"r": {"history_cutoff_season": 2026, "history_cutoff_week": 2, "as_of_at": "x"}}
    reports = [{"season": 2026, "week": 2, "projection_run_id": "r", "rows": [
        _row("a", "g", 10, 0, cohort="out"), _row("b", "g", 0, 3), _row("c", "g", None, 3), _row("d", "g", 8, 9)]}]
    assert [r["ff_player_id"] for r in study.eligible_rows(reports, runs)] == ["d"]


def test_slope_recovers_known_compression():
    rng = random.Random(1)
    rows = []
    for g in range(40):
        for p in range(10):
            projected = rng.uniform(2, 25)
            rows.append(_row(f"{g}-{p}", f"G{g}", projected, 1.4 * projected - 4 + rng.gauss(0, 2)))
    result = study.analyze(rows, draws=400)
    assert abs(result["primary"]["slope"] - 1.4) < 0.05
    assert result["primary"]["verdict"] == "CONFIRMED"


def test_calibrated_projections_are_not_confirmed():
    rng = random.Random(2)
    rows = [_row(f"{g}-{p}", f"G{g}", x, x + rng.gauss(0, 6))
            for g in range(40) for p in range(10) for x in [rng.uniform(2, 25)]]
    assert study.analyze(rows, draws=400)["primary"]["verdict"] == "NOT CONFIRMED"


def test_small_samples_draw_no_verdict():
    rows = [_row(str(i), f"G{i % 5}", 10 + i % 7, 12) for i in range(100)]
    assert study.analyze(rows, draws=100)["primary"]["verdict"] == "INSUFFICIENT"


def test_tiers_cover_every_projection():
    assert [study.tier_of(v) for v in (25, 18, 17.9, 12, 6, 5.9, 0.1)] == \
        ["18+", "18+", "12-18", "12-18", "6-12", "under 6", "under 6"]
