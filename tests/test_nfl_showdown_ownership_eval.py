import numpy as np
import pandas as pd
import pytest

from model.nfl_showdown_ownership_eval import (
    build_lineup, fit_metrics, portfolio, prior_showdown, score_portfolio,
)


def pool():
    """The same pool web/scripts/test-nfl-ownership-prior.ts pins the TS prior to."""
    rows = [("QB", 10800, 18.6, 16200), ("RB", 9600, 15.6, 14400), ("WR", 10600, 14.1, 15900), ("RB", 200, 5.8, 300)]
    rows += [("WR", 3000 + i * 300, 3 + i * 0.4, round((3000 + i * 300) * 1.5)) for i in range(14)]
    df = pd.DataFrame(rows, columns=["position", "salary", "pts", "captain_salary"])
    df["dk_status"] = ""
    df["is_out"] = False
    return df


def test_prior_matches_the_typescript_numbers():
    flex, cap = prior_showdown(pool(), value_exponent=0.5)
    assert flex[:4] == pytest.approx([51.94, 70.68, 79.54, 25.07], abs=0.02)
    assert cap[:4] == pytest.approx([43.06, 24.32, 15.46, 1.93], abs=0.02)
    assert flex.sum() == pytest.approx(500.0) and cap.sum() == pytest.approx(100.0)


def test_budgets_caps_and_one_slot_per_player():
    flex, cap = prior_showdown(pool())
    assert flex.sum() == pytest.approx(500.0) and cap.sum() == pytest.approx(100.0)
    assert cap.max() <= 50.0 + 1e-9 and (flex + cap).max() <= 95.0 + 1e-9


def test_out_and_zero_points_draw_nothing():
    df = pool()
    df.loc[1, "is_out"] = True
    df.loc[2, "pts"] = 0
    flex, cap = prior_showdown(df)
    assert flex[1] == flex[2] == cap[1] == cap[2] == 0


def test_lower_value_exponent_shifts_ownership_to_the_high_points_players():
    hi_value, _ = prior_showdown(pool(), value_exponent=1.5)
    lo_value, _ = prior_showdown(pool(), value_exponent=0.5)
    assert lo_value[2] < hi_value[2]                # the priciest WR was over-credited for value
    assert lo_value[3] < hi_value[3]


def small_slate():
    names = [f"p{i}" for i in range(12)]
    df = pd.DataFrame({"normalized_name": names, "team": ["A"] * 6 + ["B"] * 6,
                       "salary": [9000, 8000, 7000, 6000, 5000, 4000] * 2,
                       "pts": [20, 16, 12, 9, 6, 3, 19, 15, 11, 8, 5, 2], "captain_salary": [1.5 * s for s in [9000, 8000, 7000, 6000, 5000, 4000] * 2],
                       "dk_status": "", "is_out": False})
    df["flex_act"] = 0.0
    df["cpt_act"] = 0.0
    return df


def test_build_lineup_respects_cap_slots_and_both_teams():
    df = small_slate()
    captain, flex = build_lineup(df, df.pts.to_numpy(float), 1.5 * df.pts.to_numpy(float))
    chosen = [captain, *flex]
    assert len(chosen) == len(set(chosen)) == 6
    sal = dict(zip(df.normalized_name, df.salary))
    assert 1.5 * sal[captain] + sum(sal[f] for f in flex) <= 50_000
    assert {df.set_index("normalized_name").team[n] for n in chosen} == {"A", "B"}


def test_leverage_moves_a_portfolio_off_the_chalk_and_costs_points():
    df = small_slate()
    chalk = np.zeros(len(df)); chalk[0] = 90.0            # p0 owned by nearly everyone
    free = portfolio(df, chalk, chalk, leverage=0.0, seed=3, lineups=20, noise=0.05)
    fade = portfolio(df, chalk, chalk, leverage=3.0, seed=3, lineups=20, noise=0.05)
    used = lambda lines: np.mean([("p0" in (c, *f)) for c, f in lines])
    assert used(free) > used(fade)
    assert score_portfolio(df, free, {})["proj_pts"] >= score_portfolio(df, fade, {})["proj_pts"]


def test_score_portfolio_counts_real_copies():
    df = small_slate()
    lines = [("p0", ("p1", "p2", "p3", "p6", "p7")), ("p6", ("p0", "p1", "p2", "p3", "p7"))]
    out = score_portfolio(df, lines, {lines[0]: 5})
    assert out["share_duplicated"] == 0.5 and out["mean_real_copies"] == 2.5 and out["max_real_copies"] == 5


def test_fit_metrics():
    df = small_slate()
    df["flex_act"] = np.arange(12, dtype=float)
    df["cpt_act"] = 0.0
    m = fit_metrics(df, df.flex_act.to_numpy(float), np.zeros(12))
    assert m["mae_flex"] == 0.0 and m["rho_chalk"] == pytest.approx(1.0)
