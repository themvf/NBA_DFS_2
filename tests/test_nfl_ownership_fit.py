"""`nfl-ownership-fit-v1` (model/nfl_ownership_fit.py): registered constants, features, fit and allocation."""
import numpy as np
import pandas as pd
import pytest

from model import nfl_ownership_fit as m


def test_registration_is_pinned():
    assert m.VERSION == "nfl-ownership-fit-v1"
    assert m.ALPHA == 1.0 and m.TARGET_OFFSET == 0.1 and m.CAP == 60.0
    assert m.FEATURES == ("log_salary", "proj", "proj_missing", "dk_avg", "dk_missing", "value",
                          "rank_proj", "rank_value", "rank_dk", "rank_salary",
                          "team_share", "game_rank", "team_pos_rank", "status_q", "status_d", "log_pool")


def slate(seed: int, n_games: int = 6) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    teams = [f"T{i}" for i in range(2 * n_games)]
    for g in range(n_games):
        home, away = teams[2 * g], teams[2 * g + 1]
        for team in (home, away):
            plan = [("QB", 2), ("RB", 4), ("WR", 5), ("TE", 3), ("DST", 1)]
            for pos, k in plan:
                for j in range(k):
                    salary = int(rng.integers(3000, 9500) // 100 * 100)
                    proj = max(0.5, salary / 1000 * rng.uniform(1.2, 3.2) * (1.0 if j == 0 else 0.5))
                    rows.append({"name": f"{team} {pos}{j}", "team": team, "position": pos, "salary": salary,
                                 "proj": proj, "dk_avg": proj * rng.uniform(0.7, 1.3), "dk_status": "",
                                 "game_key": f"{away}@{home}"})
    df = pd.DataFrame(rows)
    # the field drafts value and the top of each team; label in percentage points
    score = (df["proj"] / (df["salary"] / 1000)) ** 2 * (1 + rng.normal(0, 0.15, len(df)))
    df["actual"] = 0.0
    for pos, budget in (("QB", 100), ("RB", 230), ("WR", 335), ("TE", 135), ("DST", 100)):
        ix = df.position == pos
        df.loc[ix, "actual"] = (score[ix] / score[ix].sum() * budget).clip(upper=60)
    return df


def test_feature_frame_ranks_are_within_position_and_bounded():
    df = m.feature_frame(slate(1))
    for col in ("rank_proj", "rank_value", "rank_dk", "rank_salary", "game_rank"):
        assert df[col].between(0, 1).all()
    for pos, sub in df.groupby("position"):
        assert sub["rank_proj"].max() == 1.0 and sub["rank_proj"].min() == 0.0
    assert df["team_pos_rank"].min() == 1.0 and df["team_pos_rank"].max() == 4.0
    assert df[df.position == "DST"]["team_pos_rank"].eq(1.0).all()
    assert abs(df.groupby("team")["team_share"].first().sum() - 1.0) < 1e-9
    assert df["proj_missing"].eq(0).all() and df["dk_missing"].eq(0).all()


def test_lone_player_in_a_position_ranks_at_the_middle_and_missing_inputs_are_flagged():
    df = slate(2)
    df = df[~((df.position == "DST") & (df.team != "T0"))]
    df.loc[df.index[:3], "proj"] = None
    df.loc[df.index[:3], "dk_avg"] = None
    f = m.feature_frame(df)
    assert f[f.position == "DST"]["rank_proj"].iloc[0] == 0.5
    assert f["proj_missing"].sum() == 3 and f["dk_missing"].sum() == 3
    assert f.loc[f.proj_missing == 1, "proj"].eq(0).all()


def test_fit_predict_hits_position_budgets_and_the_cap_and_beats_a_flat_guess():
    frames = [(f"s{i}", m.feature_frame(slate(10 + i))) for i in range(5)]
    model = m.fit(frames[:4])
    assert set(model["groups"]) == set(m.POSITIONS) and model["training_slates"] == ["s0", "s1", "s2", "s3"]
    held = frames[4][1]
    pred = m.predict(model, held)
    assert pred.max() <= m.CAP + 1e-6
    for pos, budget in model["budgets"].items():
        assert abs(pred[(held.position == pos).to_numpy()].sum() - budget) < 0.05
    raw = m.predict_raw(model, held)
    assert abs(raw.sum() - 900.0) < 0.05
    corr = np.corrcoef(pred, held["actual"])[0, 1]
    assert corr > 0.8
    mae = np.abs(pred - held["actual"]).mean()
    flat = np.abs(held["actual"].mean() - held["actual"]).mean()
    assert mae < flat


def test_each_slate_counts_equally_regardless_of_size():
    big = m.feature_frame(pd.concat([slate(20), slate(21)], ignore_index=True).assign(game_key=lambda d: d.game_key + d.index.astype(str)))
    small = m.feature_frame(slate(22))
    model = m.fit([("big", big), ("small", small)])
    assert model["groups"]["QB"]["slates"] == 2
    assert model["groups"]["QB"]["rows"] == int((big.position == "QB").sum() + (small.position == "QB").sum())


def test_fit_refuses_a_single_slate():
    with pytest.raises(ValueError):
        m.fit([("only", m.feature_frame(slate(3)))])


def test_gate_requires_every_bar_including_beating_the_prior():
    assert m.gate(0.80, 1.4, 0.0, 7, 1.5)["verdict"] == "PASS"
    assert m.gate(0.80, 1.4, 0.0, 7, 1.3)["verdict"] == "FAIL"      # no better than the prior
    assert m.gate(0.80, 1.4, 0.0, 3, 1.5)["verdict"] == "FAIL"      # too few slates
    assert m.gate(0.69, 1.4, 0.0, 7, 1.5)["verdict"] == "FAIL"
    assert m.gate(0.80, 2.1, 0.0, 7, 2.5)["verdict"] == "FAIL"
    assert m.gate(0.80, 1.4, 0.6, 7, 1.5)["verdict"] == "FAIL"
