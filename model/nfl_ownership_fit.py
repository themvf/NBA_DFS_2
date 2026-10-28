"""Fitted NFL Classic field-ownership model (`nfl-ownership-fit-v1`).

Registered in docs/nfl-ownership-model.md ("Phase 2 fit") before the first fit.
Pure: frames in, model dict out, predictions out. One Ridge per position on
log(drafted_pct + 0.1) over pregame snapshot features, then a capped
allocation to each position's mean recorded total. The runner that reads the
database and grades the folds is research/nfl_ownership_fit.py.

A constant here is a registration. Changing one is a new version.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from model.nfl_ownership_scorecard import allocate_detailed

VERSION = "nfl-ownership-fit-v1"
POSITIONS = ("QB", "RB", "WR", "TE", "DST")
FEATURES = ("log_salary", "proj", "proj_missing", "dk_avg", "dk_missing", "value",
            "rank_proj", "rank_value", "rank_dk", "rank_salary",
            "team_share", "game_rank", "team_pos_rank", "status_q", "status_d", "log_pool")
ALPHA = 1.0
TARGET_OFFSET = 0.1
CAP = 60.0
VALUE_SALARY_FLOOR = 3000.0
TEAM_POS_RANK_CLIP = 4
CLASSIC_TOTAL = 900.0


def _pct_rank(values: pd.Series) -> pd.Series:
    """Tie-averaged percentile rank in [0, 1]; a lone value is 0.5."""
    n = len(values)
    if n <= 1:
        return pd.Series([0.5] * n, index=values.index, dtype=float)
    return (values.rank(method="average") - 1.0) / (n - 1.0)


def feature_frame(players: pd.DataFrame) -> pd.DataFrame:
    """Add the registered feature columns to one slate's ACTIVE players.

    Needs columns: name, team, position, salary, proj (None when absent), dk_avg
    (None when absent), dk_status, game_key. Rows are one slate; ranks and
    shares are computed within it.
    """
    df = players.copy()
    df["position"] = df["position"].astype(str).str.upper()
    df["salary"] = df["salary"].astype(float)
    proj = pd.to_numeric(df["proj"], errors="coerce")
    dk = pd.to_numeric(df["dk_avg"], errors="coerce")
    df["proj_missing"] = proj.isna().astype(float)
    df["dk_missing"] = dk.isna().astype(float)
    df["proj"] = proj.fillna(0.0).clip(lower=0.0)
    df["dk_avg"] = dk.fillna(0.0).clip(lower=0.0)
    df["log_salary"] = np.log(df["salary"].clip(lower=100.0) / 1000.0)
    df["value"] = df["proj"] / (df["salary"].clip(lower=VALUE_SALARY_FLOOR) / 1000.0)
    g = df.groupby("position", group_keys=False)
    df["rank_proj"] = g["proj"].transform(_pct_rank)
    df["rank_value"] = g["value"].transform(_pct_rank)
    df["rank_dk"] = g["dk_avg"].transform(_pct_rank)
    df["rank_salary"] = g["salary"].transform(_pct_rank)
    df["log_pool"] = np.log(g["proj"].transform("size").astype(float))
    slate_total = float(df["proj"].sum()) or 1.0
    df["team_share"] = df.groupby("team")["proj"].transform("sum") / slate_total
    game_total = df.groupby("game_key")["proj"].transform("sum")
    game_totals = df.groupby("game_key")["proj"].sum()
    game_rank = _pct_rank(game_totals)
    df["game_rank"] = df["game_key"].map(game_rank).astype(float)
    df["team_pos_rank"] = (df.groupby(["team", "position"])["proj"]
                           .rank(method="first", ascending=False).clip(upper=TEAM_POS_RANK_CLIP).astype(float))
    status = df["dk_status"].fillna("").astype(str).str.upper()
    df["status_q"] = (status == "Q").astype(float)
    df["status_d"] = (status == "D").astype(float)
    return df


def _ridge(x: np.ndarray, y: np.ndarray, w: np.ndarray, alpha: float) -> tuple[np.ndarray, float, np.ndarray, np.ndarray]:
    w = w / w.sum()
    mean = np.sum(x * w[:, None], axis=0)
    scale = np.sqrt(np.sum((x - mean) ** 2 * w[:, None], axis=0))
    scale[scale < 1e-9] = 1.0
    z = np.column_stack([np.ones(len(x)), (x - mean) / scale])
    penalty = np.eye(z.shape[1]) * alpha
    penalty[0, 0] = 0.0
    coef = np.linalg.solve(z.T @ (z * w[:, None]) + penalty, z.T @ (y * w))
    return coef[1:], float(coef[0]), mean, scale


def fit(slates: list[tuple[str, pd.DataFrame]]) -> dict:
    """Fit on feature frames (`feature_frame` output plus an `actual` column in
    percentage points). Each slate's rows share equal total weight."""
    if len(slates) < 2:
        raise ValueError("fit needs at least two slates")
    groups, budgets = {}, {}
    for pos in POSITIONS:
        xs, ys, ws, totals = [], [], [], []
        for slate_id, df in slates:
            sub = df[df["position"] == pos]
            if sub.empty:
                continue
            xs.append(sub[list(FEATURES)].to_numpy(float))
            ys.append(np.log(sub["actual"].to_numpy(float).clip(min=0.0) + TARGET_OFFSET))
            ws.append(np.full(len(sub), 1.0 / len(sub)))
            totals.append(float(sub["actual"].sum()))
        if not xs:
            continue
        x, y, w = np.vstack(xs), np.concatenate(ys), np.concatenate(ws)
        coef, intercept, mean, scale = _ridge(x, y, w, ALPHA)
        groups[pos] = {"coefficients": coef.tolist(), "intercept": intercept, "mean": mean.tolist(),
                       "scale": scale.tolist(), "rows": int(len(x)), "slates": len(xs)}
        budgets[pos] = float(np.mean(totals))
    return {"version": VERSION, "alpha": ALPHA, "target_offset": TARGET_OFFSET, "cap": CAP,
            "features": list(FEATURES), "training_slates": [s for s, _ in slates],
            "groups": groups, "budgets": budgets}


def _raw_scores(model: dict, df: pd.DataFrame) -> np.ndarray:
    out = np.zeros(len(df))
    pos = df["position"].to_numpy(str)
    x = df[list(FEATURES)].to_numpy(float)
    for p, group in model["groups"].items():
        ix = np.where(pos == p)[0]
        if len(ix) == 0:
            continue
        z = (x[ix] - np.array(group["mean"])) / np.array(group["scale"])
        yhat = group["intercept"] + z @ np.array(group["coefficients"])
        out[ix] = np.clip(np.exp(np.clip(yhat, -30, 30)) - model["target_offset"], 0.0, None)
    return out


def predict(model: dict, df: pd.DataFrame) -> np.ndarray:
    """Primary: raw scores allocated within each position to its mean recorded
    total, capped at CAP with the excess redistributed."""
    scores = _raw_scores(model, df)
    out = np.zeros(len(df))
    pos = df["position"].to_numpy(str)
    for p, budget in model["budgets"].items():
        ids = [int(i) for i in np.where(pos == p)[0]]
        if not ids:
            continue
        alloc = allocate_detailed(ids, {i: float(scores[i]) for i in ids}, budget, model["cap"])
        for i, v in alloc.items():
            out[i] = v
    return np.round(out, 2)


def predict_raw(model: dict, df: pd.DataFrame) -> np.ndarray:
    """Secondary, not gating: raw scores rescaled to the Classic total, no
    position budgets, no cap."""
    scores = _raw_scores(model, df)
    total = float(scores.sum())
    if total <= 0:
        return np.zeros(len(df))
    return np.round(scores / total * CLASSIC_TOTAL, 2)


def gate(spearman: float, mae: float, bias: float, slates: int, prior_mae: float) -> dict:
    checks = {"held_out_slates_ge_4": slates >= 4, "spearman_ge_0.70": spearman >= 0.70,
              "mae_le_2.0": mae <= 2.0, "abs_bias_le_0.5": abs(bias) <= 0.5,
              "beats_prior_mae": mae < prior_mae}
    return {"checks": checks, "verdict": "PASS" if all(checks.values()) else "FAIL"}
