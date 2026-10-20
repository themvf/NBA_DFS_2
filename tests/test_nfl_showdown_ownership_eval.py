import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from model.nfl_showdown_ownership_eval import (
    build_lineup, field_points, fit_metrics, portfolio, prior_showdown, prior_showdown_v4, score_portfolio,
)

FIXTURE = Path(__file__).parent / "fixtures" / "nfl_ownership_prior_showdown_fixture.json"


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


def fixture_slate():
    """The shared one-game pool; `expected` in the file comes from the TS prior."""
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    df = pd.DataFrame(data["players"])
    df["pts"] = [field_points(p, a) or 0.0 for p, a in zip(df["projection"], df["dkAvg"])]
    df = df.rename(columns={"captainSalary": "captain_salary", "dkStatus": "dk_status", "isOut": "is_out"})
    return data, df


def _max_gap(expected_players, ids, flex, cap):
    by_id = {p["dkPlayerId"]: p for p in expected_players}
    return max(max(abs(f - by_id[i]["flexPct"]), abs(c - by_id[i]["captainPct"])) for i, f, c in zip(ids, flex, cap))


def test_v4_matches_the_typescript_on_the_shared_fixture():
    data, df = fixture_slate()
    assert len(data["expected"]) == len(data["scenarios"]) == 4
    for scenario, expected in zip(data["scenarios"], data["expected"]):
        assert expected["name"] == scenario["name"]
        if scenario["variant"] == "v3":
            flex, cap = prior_showdown(df, value_exponent=0.5)
        else:
            flex, cap, notes = prior_showdown_v4(df, scenario["favoriteTeam"], scenario["underdogTeam"])
            for key in ("puntTierPlayers", "gameScriptKnown"):
                assert notes[key] == expected["notes"][key], (scenario["name"], key)
            assert notes["puntTierPct"] == pytest.approx(expected["notes"]["puntTierPct"], abs=0.02)
            assert notes["unallocatedPct"] == pytest.approx(expected["unallocatedPct"], abs=0.02)
        gap = _max_gap(expected["players"], df["dkPlayerId"], flex, cap)
        assert gap < 0.02, f"{scenario['name']}: max |python - ts| = {gap:.4f}"


def test_v4_structure_on_the_fixture():
    _, df = fixture_slate()
    flex, cap, notes = prior_showdown_v4(df, "SF", "WAS")
    assert flex.sum() == pytest.approx(500.0, abs=0.01) and cap.sum() == pytest.approx(100.0, abs=0.01)
    assert (flex + cap).max() <= 95.0 + 1e-9
    row = dict(zip(df["name"], zip(flex, cap)))
    assert row["sf dst"][0] > row["was dst"][0]              # the favored defense drafts more
    assert row["sf rb3"] == (0.0, 0.0) and row["sf wr4"] == (0.0, 0.0)   # out; zero points
    assert notes["puntTierPct"] == pytest.approx(60.0)
    _, _, unknown = prior_showdown_v4(df)
    assert unknown["gameScriptKnown"] is False


def test_v4_without_punts_kickers_or_dst_equals_v3():
    df = pool()
    df = df[df["salary"] >= 3000].reset_index(drop=True)
    df["team"] = ["A", "B"] * (len(df) // 2) + ["A"] * (len(df) % 2)
    flex3, cap3 = prior_showdown(df, value_exponent=0.5)
    flex4, cap4, notes = prior_showdown_v4(df)
    assert notes["puntTierPlayers"] == 0
    assert np.allclose(flex3, flex4, atol=1e-6) and np.allclose(cap3, cap4, atol=1e-6)
