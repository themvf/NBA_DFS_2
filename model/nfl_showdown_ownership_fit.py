"""Fitted NFL Showdown field-ownership model (`nfl-ownership-fit-showdown-v1`).

Owner's decision 2026-10-31, after the CAR @ GB Showdown: replace the stated v5
Showdown prior with a model fitted to every imported full-field Showdown
contest. Two Ridge regressions (flex and captain) on sqrt(ownership %) over
pregame features of one slate, then each slot's scores are allocated to its
budget (flex 500, captain 100) with the same caps the prior used.

Leave-one-slate-out on 17 slates (weeks 1-8, 654 player rows), against v5 on
the same rows: flex MAE 5.35 vs 6.16, Spearman 0.78 vs 0.64; captain MAE 1.04
vs 1.53, Spearman 0.79 vs 0.65; better captain MAE on 17 of 17 slates and flex
on 13 of 17. Chosen from 16 variants (ridge strength, target transform, feature
set, blend with v5) scored the same way, so the held-out figures are mildly
optimistic; the next slates are the real test.

Pure: frames in, model dict out. `research/nfl_showdown_ownership_fit.py` reads
the database and writes the model to web/src/data, where
web/src/lib/nfl-dfs/showdown-ownership-fit.ts applies it; the shared fixture in
tests/fixtures pins the two languages together.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

VERSION = "nfl-ownership-fit-showdown-v1"
POSITIONS = ("QB", "RB", "WR", "TE", "K", "DST")
FEATURES = ("pts", "logpts", "logsal", "value", "rank_pts", "share", "team_rank", "pos_rank", "punt", "q",
            *(f"is_{p}" for p in POSITIONS), *(f"r_{p}" for p in POSITIONS))
ALPHA = 3.0
VALUE_SALARY_FLOOR = 3000.0
PUNT_SALARY_MAX = 3000.0
RANK_PTS_CLIP, TEAM_RANK_CLIP, POS_RANK_CLIP = 20, 8, 5
FLEX_BUDGET, CAPTAIN_BUDGET = 500.0, 100.0
FLEX_MAX, CAPTAIN_MAX, TOTAL_MAX = 90.0, 50.0, 95.0


def field_points(projection, dk_avg) -> float:
    """The prior's 60/40 blend of our projection and DraftKings' average."""
    proj = None if projection is None or pd.isna(projection) else float(projection)
    avg = None if dk_avg is None or pd.isna(dk_avg) else float(dk_avg)
    if proj is None and avg is None:
        return 0.0
    if proj is None:
        return avg
    if avg is None:
        return proj
    return 0.6 * proj + 0.4 * avg


def features(slate: pd.DataFrame) -> pd.DataFrame:
    """Feature rows for the players who can be owned (pts > 0, not out).

    Needs columns pts, salary, team, position, dk_status. Ranks are descending
    with ties averaged, computed within the rows given (one slate)."""
    pts = slate["pts"].to_numpy(float)
    sal = slate["salary"].to_numpy(float)
    pos = slate["position"].fillna("").astype(str).str.upper()
    d = pd.DataFrame(index=slate.index)
    d["pts"] = pts
    d["logpts"] = np.log(pts + 1.0)
    d["logsal"] = np.log(np.maximum(sal, 100.0) / 1000.0)
    d["value"] = pts / (np.maximum(sal, VALUE_SALARY_FLOOR) / 1000.0)
    d["rank_pts"] = slate["pts"].rank(ascending=False, method="average").clip(upper=RANK_PTS_CLIP)
    d["share"] = pts / pts.sum()
    d["team_rank"] = slate.groupby("team")["pts"].rank(ascending=False, method="average").clip(upper=TEAM_RANK_CLIP)
    d["pos_rank"] = slate.assign(_p=pos).groupby("_p")["pts"].rank(ascending=False, method="average").clip(upper=POS_RANK_CLIP)
    d["punt"] = (sal < PUNT_SALARY_MAX).astype(float)
    d["q"] = (slate["dk_status"].fillna("").astype(str).str.strip().str.upper() == "Q").astype(float)
    for p in POSITIONS:
        d[f"is_{p}"] = (pos == p).astype(float)
    for p in POSITIONS:
        d[f"r_{p}"] = d[f"is_{p}"] * d["pos_rank"]
    return d[list(FEATURES)]


def _ridge(x: np.ndarray, y: np.ndarray, alpha: float) -> dict:
    mean = x.mean(axis=0)
    scale = x.std(axis=0)
    scale[scale < 1e-9] = 1.0
    z = np.column_stack([np.ones(len(x)), (x - mean) / scale])
    penalty = np.eye(z.shape[1]) * alpha
    penalty[0, 0] = 0.0
    coef = np.linalg.solve(z.T @ z + penalty, z.T @ y)
    return {"intercept": float(coef[0]), "coefficients": coef[1:].tolist(), "mean": mean.tolist(), "scale": scale.tolist()}


def fit(slates: list[pd.DataFrame]) -> dict:
    """Each frame is one slate's ownable players with flex_act and cpt_act (percent)."""
    if len(slates) < 2:
        raise ValueError("fit needs at least two slates")
    x = np.vstack([features(s).to_numpy(float) for s in slates])
    yf = np.concatenate([np.sqrt(s["flex_act"].to_numpy(float).clip(min=0)) for s in slates])
    yc = np.concatenate([np.sqrt(s["cpt_act"].to_numpy(float).clip(min=0)) for s in slates])
    return {"version": VERSION, "alpha": ALPHA, "features": list(FEATURES), "rows": int(len(x)), "slates": len(slates),
            "flex": _ridge(x, yf, ALPHA), "captain": _ridge(x, yc, ALPHA)}


def _scores(group: dict, x: np.ndarray) -> np.ndarray:
    z = (x - np.array(group["mean"])) / np.array(group["scale"])
    return np.clip(group["intercept"] + z @ np.array(group["coefficients"]), 0.0, None) ** 2


def allocate(scores: np.ndarray, budget: float, caps: np.ndarray) -> np.ndarray:
    """Scale to the budget; a capped player keeps the cap and the rest is re-shared."""
    out = np.zeros(len(scores))
    open_ = scores > 0
    remaining = budget
    for _ in range(16):
        total = scores[open_].sum()
        if not open_.any() or total <= 0:
            break
        share = np.where(open_, remaining * scores / total, 0.0)
        over = open_ & (share > caps)
        if not over.any():
            out[open_] = share[open_]
            break
        out[over] = caps[over]
        remaining -= caps[over].sum()
        open_ &= ~over
    return out


def predict(model: dict, slate: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """(flex %, captain %) per row of `slate`; rows with pts <= 0 or out get 0."""
    out = slate["is_out"].fillna(False).astype(bool).to_numpy() if "is_out" in slate else np.zeros(len(slate), bool)
    ok = (slate["pts"].to_numpy(float) > 0) & ~out & (slate["salary"].to_numpy(float) > 0)
    flex, cpt = np.zeros(len(slate)), np.zeros(len(slate))
    if not ok.any():
        return flex, cpt
    sub = slate[ok]
    x = features(sub).to_numpy(float)
    c = allocate(_scores(model["captain"], x), CAPTAIN_BUDGET, np.full(len(sub), CAPTAIN_MAX))
    f = allocate(_scores(model["flex"], x), FLEX_BUDGET, np.minimum(FLEX_MAX, TOTAL_MAX - c))
    flex[ok], cpt[ok] = f, c
    return flex, cpt
