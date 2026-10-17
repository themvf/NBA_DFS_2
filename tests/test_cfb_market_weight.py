import numpy as np

from research import cfb_market_weight as mw


def _rows(w_true, n=1500, seed=1):
    """Synthetic games where actual - line = w_true * (model - line) + noise."""
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        line = rng.normal(0, 10)
        model = line + rng.normal(0, 6)
        actual = line + w_true * (model - line) + rng.normal(0, 12)
        total_line = 55 + rng.normal(0, 6)
        q = 1 / (1 + np.exp(-line / 7))
        p_model = 1 / (1 + np.exp(-model / 7))
        won = float(rng.random() < q)
        rows.append({"game_id": i, "season": 2023 + i % 3, "date": f"2024-09-{1 + i % 28:02d}",
                     "margin_line": line, "margin_model": model, "margin_actual": actual,
                     "total_line": total_line, "total_model": total_line + rng.normal(0, 6),
                     "total_actual": total_line + rng.normal(0, 12),
                     "q_home": q, "p_model": p_model, "home_won": won})
    return rows


def test_weight_through_origin_recovers_a_known_weight():
    rows = _rows(0.3)
    fit = mw.fit_market(rows, "margin")
    assert abs(fit["w"] - 0.3) < 0.1
    assert fit["ci95"][0] < 0.3 < fit["ci95"][1]
    assert fit["n"] == 1500 and fit["dates"] == 28


def test_zero_weight_interval_includes_zero_and_decision_says_so():
    rows = _rows(0.0)
    result = mw.run(rows)
    lo, hi = result["pooled"]["margin"]["ci95"]
    assert lo <= 0 <= hi
    assert result["decision_margin"] == "NO_RESIDUAL_SKILL"


def test_strong_weight_is_residual_skill():
    result = mw.run(_rows(0.6, n=3000))
    assert result["decision_margin"] == "RESIDUAL_SKILL"
    for fold in result["folds"].values():
        assert fold["margin"]["anchored_minus_line_mae"]["ci95"][1] < 0


def test_negative_weight_is_anti_informative():
    assert mw.run(_rows(-0.5, n=3000))["decision_margin"] == "ANTI_INFORMATIVE"


def test_theta_offset_is_zero_when_outcomes_follow_the_market():
    rows = _rows(0.0, n=4000, seed=3)
    theta = mw.theta_offset([r["q_home"] for r in rows], [r["p_model"] for r in rows],
                            [r["home_won"] for r in rows])
    assert abs(theta) < 0.15


def test_grade_uses_line_when_weight_is_zero():
    rows = _rows(0.0, n=200)
    graded = mw.grade_market(rows, "margin", 0.0)
    assert graded["mae"]["anchored"] == graded["mae"]["line"]
    assert graded["anchored_minus_line_mae"]["mean"] == 0.0


def test_missing_lines_are_excluded_not_zeroed():
    rows = _rows(0.0, n=100)
    for r in rows[:40]:
        r["margin_line"] = None
        r["q_home"] = None
    assert mw.fit_market(rows, "margin")["n"] == 60
    assert mw.fit_moneyline(rows)["n"] == 60
