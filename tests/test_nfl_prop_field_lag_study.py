"""nfl-prop-field-lag-v1 (model/nfl_prop_field_lag_study.py): the pure analysis on synthetic fields."""
import numpy as np

from model import nfl_prop_field_lag_study as m


def _field(seed: int, lag: float, weeks: int = 8, per_week: int = 100) -> list[dict]:
    """A synthetic flagship: ownership set from the Wednesday line (lag=1) or the close (lag=0)."""
    rng = np.random.default_rng(seed)
    rows = []
    for w in range(1, weeks + 1):
        for i in range(per_week):
            pos = m.POSITIONS[i % 4]
            salary = float(rng.integers(3000, 9500, endpoint=True) // 100 * 100)
            early = max(2.0, salary / 550 + rng.normal(0, 2.5))
            delta = rng.normal(0, 1.5)
            late = early + delta
            seen = late - lag * delta  # what the field priced ownership on
            own = max(0.0, 2.0 * seen - salary / 1000 + rng.normal(0, 2.0))
            rows.append({"week": w, "contest_id": f"c{w}", "player": f"p{w}-{i}", "position": pos, "salary": salary,
                         "early_pts": early, "late_pts": late, "delta": delta, "own": own,
                         "actual": late + rng.normal(0, 7.0)})
    return rows


def test_a_lagged_field_is_explained_better_by_the_wednesday_lines() -> None:
    rows = _field(1, lag=1.0)
    gap = m.mae_gap(rows)
    lo, hi = m.bootstrap_ci(rows, m.mae_gap, draws=200)
    assert gap < 0 and hi < 0
    w, maes = m.field_lag_weight(rows)
    assert w >= 0.8 and maes[1.0] < maes[0.0]


def test_a_field_that_updates_is_explained_better_by_the_close() -> None:
    rows = _field(2, lag=0.0)
    gap = m.mae_gap(rows)
    lo, hi = m.bootstrap_ci(rows, m.mae_gap, draws=200)
    assert gap > 0 and lo > 0
    w, _ = m.field_lag_weight(rows)
    assert w <= 0.2


def test_control_is_small_when_the_close_is_efficient() -> None:
    for seed in (3, 5, 9):
        rows = _field(seed, lag=1.0)
        corr, n = m.control_correlation(rows)
        assert n == len(rows) and abs(corr) < 0.1
    lo, hi = m.bootstrap_ci(_field(3, lag=1.0), lambda s: m.control_correlation(s)[0], draws=200)
    assert lo <= 0 <= hi


def test_bootstrap_takes_the_wider_of_cluster_and_row_intervals() -> None:
    rows = _field(4, lag=1.0, weeks=4, per_week=60)
    by_week = {}
    for r in rows:
        by_week.setdefault(r["week"], []).append(r)
    clustered = m._resample_ci([by_week[w] for w in sorted(by_week)], m.mae_gap, 100, 1)
    rowwise = m._resample_ci([[r] for r in rows], m.mae_gap, 100, 2)
    lo, hi = m.bootstrap_ci(rows, m.mae_gap, draws=100, seed=1)
    assert lo == min(clustered[0], rowwise[0]) and hi == max(clustered[1], rowwise[1])


def test_verdict_only_at_the_floors_and_terciles_are_descriptive() -> None:
    short = _field(4, lag=1.0, weeks=5, per_week=80)
    res = m.analyze(short, pulled=[], slates=[], draws=100)
    assert res["floors"]["met"] is False and "verdict" not in res and "descriptive" in res
    assert [t["tercile"] for t in res["descriptive"]["terciles"]] == [1, 2, 3]
    full = _field(5, lag=1.0, weeks=8, per_week=100)
    res = m.analyze(full, pulled=[], slates=[], draws=100)
    assert res["floors"]["met"] is True and res["verdict"]["h1_field_lag"] == "PASS"
    assert res["descriptive"]["field_lag_weight"] >= 0.8


def test_design_matrix_shape_and_rows_without_actuals_stay_in_the_ownership_model() -> None:
    rows = _field(6, lag=1.0, weeks=2, per_week=10)
    rows[0]["actual"] = None
    assert m.design(rows, "late").shape == (20, 6) and m.design(m.blended(rows, 0.5), "mix").shape == (20, 6)
    assert m.control_correlation(rows)[1] == 19
    assert len(m.target(rows)) == 20
