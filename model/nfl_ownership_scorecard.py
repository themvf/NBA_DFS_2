"""Grading machinery for `nfl-ownership-scorecard-v3` (docs/nfl-ownership-scorecard-v1.md).

Pure: numpy/pandas/scipy, no database, no I/O. The driver is
research/nfl_ownership_scorecard.py.

What is here:

- The frozen scorecard constants (section "Status" of the scorecard).
- A Python port of `projectOwnershipPrior` for CLASSIC, variants v3, v4 and the
  drafted v5 (web/src/lib/nfl-dfs/ownership-prior.ts is the authority; pinned by
  tests/fixtures/nfl_ownership_prior_classic_fixture.json). Showdown uses the
  pinned mirrors in model/nfl_showdown_ownership_eval.py (`prior_showdown` with
  value exponent 0.5 = v3, `prior_showdown_v4` = v4, `prior_showdown_v5` = v5,
  value exponent 1.5 = v2).
- Metric functions: MAE, bias (predicted minus actual), RMSE, Spearman with
  AVERAGED tied ranks, Pearson, calibration bins, top-10 overlap, largest
  misses, and a slate-clustered bootstrap for pooled values.
- Label gating: only ``observed`` and ``reconciled_zero`` labels enter a metric
  (`GATE_CLASSES`); ``synthesized_zero`` and ``unresolved`` are counted, never
  scored.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
from scipy.stats import rankdata

from model.nfl_showdown_ownership_eval import field_points, prior_showdown, prior_showdown_v4, prior_showdown_v5

SCORECARD_VERSION = "nfl-ownership-scorecard-v3"
GRADER_VERSION = "nfl-ownership-scorecard-grader-v1"
# Scorecard v3 (2026-10-20, owner): the canonical forecast is the LATEST capture at or
# before first kickoff minus 20 minutes from ANY source (pool capture, owner
# optimizer run, ghost freeze). v1/v2 preferred the pool capture over later sources.
CANONICAL_SOURCE_RULE = "latest"

GATE_CLASSES = ("observed", "reconciled_zero")
EXCLUDED_CLASSES = ("synthesized_zero", "unresolved")

# Frozen constants (scorecard "Status" table).
N_RELEVANT_CLASSIC = {"QB": 12, "RB": 20, "WR": 30, "TE": 12, "DST": 12}
CHALK_THRESHOLD = {"classic": 15.0, "FLEX": 25.0, "CPT": 10.0}
CLASSIC_TIER_EDGES = [0, 3000, 4000, 5000, 6000, 7000, 8000, math.inf]
SHOWDOWN_TIER_EDGES = [0, 2500, 3000, 4000, 5000, 6000, 7000, 8000, math.inf]
CALIBRATION_EDGES = [0, 1, 5, 10, 20, 35, math.inf]
CALIBRATION_EDGES_SHOWDOWN_FLEX = [0, 1, 5, 10, 20, 35, 50, math.inf]
CAPS = {"classic": 60.0, "CPT": 50.0, "FLEX": 90.0}

# Design tags (handoff section 3 task 4, scorecard section 6). v4's constants were read
# off weeks 2-5 (both formats); v3's Showdown exponent off weeks 2-3.
V4_DESIGN_WEEKS = range(2, 6)
V3_SHOWDOWN_DESIGN_WEEKS = range(2, 4)
# v5's constants were chosen on every 2026 slate of weeks 2-6 (docs/nfl-ownership-prior-v5.md).
V5_DESIGN_WEEKS = range(2, 7)

# --------------------------------------------------------------------------
# Classic prior port (ownership-prior.ts)
# --------------------------------------------------------------------------

CLASSIC_BUDGETS = {
    "v3": {"QB": 100, "RB": 255, "WR": 340, "TE": 105, "DST": 100},
    "v4": {"QB": 100, "RB": 230, "WR": 335, "TE": 135, "DST": 100},
    "v5": {"QB": 100, "RB": 230, "WR": 335, "TE": 135, "DST": 100},
}
# v5 (drafted 2026-10-20): each Classic score is raised to this before allocation
# (ownership-prior.ts CLASSIC_FLATTEN_ALPHA_V5; docs/nfl-ownership-prior-v5.md).
CLASSIC_FLATTEN_ALPHA = {"v3": 1.0, "v4": 1.0, "v5": 0.75}
STATUS_MULTIPLIER = {"Q": 0.6, "D": 0.25}
VALUE_SALARY_FLOOR = 3000


def _score_classic(points: float | None, salary: float, status: str, is_out: bool) -> float:
    if is_out or not salary > 0 or points is None or not points > 0:
        return 0.0
    value = points / (max(salary, VALUE_SALARY_FLOOR) / 1000.0)
    mult = STATUS_MULTIPLIER.get((status or "").strip().upper(), 1.0)
    return (max(points, 0.5) ** 2) * (max(value, 0.2) ** 1.5) * mult


def allocate_detailed(ids: Sequence, scores: Mapping, budget: float, cap: float) -> dict:
    """Exact port of allocateBudgetDetailed (single cap): in-pass capping with a
    fixed total and a falling remainder, in key order."""
    out: dict = {}
    open_ = {i: scores[i] for i in ids if scores[i] > 0}
    remaining = float(budget)
    for _ in range(16):
        if not open_:
            break
        total = sum(open_.values())
        if total <= 0:
            break
        capped = False
        for i in list(open_):
            share = remaining * (open_[i] / total)
            if share > cap:
                out[i] = cap
                del open_[i]
                remaining -= cap
                capped = True
        if not capped:
            for i, s in open_.items():
                out[i] = remaining * (s / total)
            open_ = {}
    for i in ids:
        out.setdefault(i, 0.0)
    return out


def prior_classic(players: pd.DataFrame, variant: str, alpha: float | None = None,
                  budgets: Mapping[str, float] | None = None,
                  weight_multiplier: Sequence[float] | None = None) -> np.ndarray:
    """Total ownership % per row (rounded to 0.01 like the TS). Needs columns
    position, salary, proj, dk_avg, dk_status, is_out. v3's TS filters positions
    case-sensitively and v4 upper-cases; stored positions are upper case either way."""
    budgets = CLASSIC_BUDGETS[variant] if budgets is None else budgets
    a = CLASSIC_FLATTEN_ALPHA[variant] if alpha is None else alpha
    pts = [field_points(p, a_) for p, a_ in zip(players["proj"], players["dk_avg"])]
    scores = {i: _score_classic(pts[i], float(players["salary"].iloc[i] or 0), players["dk_status"].iloc[i] or "",
                                bool(players["is_out"].iloc[i]))
              for i in range(len(players))}
    if a != 1.0:
        scores = {i: (s ** a if s > 0 else 0.0) for i, s in scores.items()}
    if weight_multiplier is not None:
        # nfl-ownership-news-v1 candidate C2: per-row multiplier after flattening.
        scores = {i: s * float(weight_multiplier[i]) for i, s in scores.items()}
    own = np.zeros(len(players))
    pos = [str(p or "") if variant == "v3" else str(p or "").upper() for p in players["position"]]
    for position, budget in budgets.items():
        ids = [i for i in range(len(players)) if pos[i] == position]
        for i, v in allocate_detailed(ids, scores, budget, CAPS["classic"]).items():
            own[i] = v
    return np.round(own, 2)


def showdown_frame(players: pd.DataFrame) -> pd.DataFrame:
    """The column shape the Showdown mirrors expect."""
    pts = [field_points(p, a) for p, a in zip(players["proj"], players["dk_avg"])]
    return pd.DataFrame({
        "pts": [0.0 if v is None else float(v) for v in pts],
        "salary": players["salary"].astype(float).to_numpy(),
        "captain_salary": pd.to_numeric(players["captain_salary"], errors="coerce").to_numpy(float),
        "dk_status": players["dk_status"].fillna("").to_numpy(),
        "is_out": players["is_out"].astype(bool).to_numpy(),
        "position": players["position"].fillna("").to_numpy(),
        "team": players["team"].to_numpy(),
    })


def prior_showdown_version(players: pd.DataFrame, variant: str, favorite: str | None = None,
                           underdog: str | None = None,
                           weight_multiplier: Sequence[float] | None = None) -> tuple[np.ndarray, np.ndarray, dict]:
    """(flex %, captain %, notes), rounded to 0.01 like the TS. `weight_multiplier`
    (v5 only) is the nfl-ownership-news-v1 C2 per-row multiplier."""
    df = showdown_frame(players)
    notes: dict = {"variant": variant}
    if weight_multiplier is not None and variant != "v5":
        raise ValueError("weight_multiplier applies to v5 only")
    if variant == "v5":
        flex, cap, notes = prior_showdown_v5(df, weight_multiplier=weight_multiplier)
    elif variant == "v4":
        flex, cap, notes = prior_showdown_v4(df, favorite, underdog)
    elif variant == "v3":
        flex, cap = prior_showdown(df, value_exponent=0.5)
    elif variant == "v2":
        flex, cap = prior_showdown(df, value_exponent=1.5)
    else:
        raise ValueError(variant)
    return np.round(flex, 2), np.round(cap, 2), notes


# --------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------

def spearman_avg(pred: Sequence[float], actual: Sequence[float]) -> float:
    """Spearman with AVERAGED tied ranks (Pearson of average ranks); nan when undefined."""
    p, a = np.asarray(pred, float), np.asarray(actual, float)
    if len(p) < 3:
        return float("nan")
    return pearson(rankdata(p, method="average"), rankdata(a, method="average"))


def pearson(x: Sequence[float], y: Sequence[float]) -> float:
    x, y = np.asarray(x, float), np.asarray(y, float)
    if len(x) < 3 or np.std(x) == 0 or np.std(y) == 0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def error_metrics(pred: Sequence[float], actual: Sequence[float]) -> dict:
    p, a = np.asarray(pred, float), np.asarray(actual, float)
    if len(p) == 0:
        return {"n": 0, "mae": None, "bias": None, "rmse": None, "spearman": None, "pearson": None}
    d = p - a
    return {"n": int(len(p)), "mae": float(np.mean(np.abs(d))), "bias": float(np.mean(d)),
            "rmse": float(np.sqrt(np.mean(d * d))), "spearman": _f(spearman_avg(p, a)), "pearson": _f(pearson(p, a))}


def _f(v: float) -> float | None:
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)


def calibration_bins(pred: Sequence[float], actual: Sequence[float], edges: Sequence[float]) -> list[dict]:
    p, a = np.asarray(pred, float), np.asarray(actual, float)
    out = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        m = (p >= lo) & (p < hi)
        out.append({"bin": f"{lo:g}-{hi:g}" if math.isfinite(hi) else f"{lo:g}+", "n": int(m.sum()),
                    "mean_pred": _f(float(p[m].mean())) if m.any() else None,
                    "mean_actual": _f(float(a[m].mean())) if m.any() else None})
    return out


def top_k_overlap(names: Sequence[str], pred: Sequence[float], actual: Sequence[float], k: int = 10) -> int:
    names = list(names)
    if not names:
        return 0
    order_p = np.argsort(-np.asarray(pred, float), kind="stable")[:k]
    order_a = np.argsort(-np.asarray(actual, float), kind="stable")[:k]
    return len({names[i] for i in order_p} & {names[i] for i in order_a})


def largest_misses(frame: pd.DataFrame, k: int = 10) -> list[dict]:
    if frame.empty:
        return []
    d = frame.assign(err=frame["pred"] - frame["actual"])
    d = d.reindex(d["err"].abs().sort_values(ascending=False, kind="stable").index)[:k]
    return [{"name": r["name"], "position": r["position"], "salary": int(r["salary"]) if pd.notna(r["salary"]) else None,
             "pred": round(float(r["pred"]), 2), "actual": round(float(r["actual"]), 2)} for _, r in d.iterrows()]


def salary_tier(salary: float, fmt: str) -> str:
    edges = SHOWDOWN_TIER_EDGES if fmt == "showdown" else CLASSIC_TIER_EDGES
    for lo, hi in zip(edges[:-1], edges[1:]):
        if lo <= salary < hi:
            return f"{lo / 1000:g}k+" if not math.isfinite(hi) else f"{lo / 1000:g}-{hi / 1000:g}k"
    return "unknown"


def gate_rows(labels: pd.DataFrame) -> pd.DataFrame:
    """Only observed and reconciled_zero labels may be scored."""
    return labels[labels["label_class"].isin(GATE_CLASSES)]


def summarize(frame: pd.DataFrame, fmt: str, slot: str) -> dict:
    """Every metric of scorecard section 5 for one population. `frame` has columns
    name, position, salary, pred, actual, anchored (bool)."""
    m = error_metrics(frame["pred"], frame["actual"])
    edges = CALIBRATION_EDGES_SHOWDOWN_FLEX if (fmt == "showdown" and slot == "FLEX") else CALIBRATION_EDGES
    m["calibration"] = calibration_bins(frame["pred"], frame["actual"], edges)
    m["top10_overlap"] = top_k_overlap(frame["name"], frame["pred"], frame["actual"]) if len(frame) else None
    m["largest_misses"] = largest_misses(frame)
    alloc = {}
    for pos, g in frame.groupby("position"):
        alloc[str(pos)] = {"pred": round(float(g["pred"].sum()), 2), "actual": round(float(g["actual"].sum()), 2), "n": int(len(g))}
    m["allocation"] = alloc
    m["sum_pred"], m["sum_actual"] = round(float(frame["pred"].sum()), 2), round(float(frame["actual"].sum()), 2)
    cap = CAPS["classic"] if fmt == "classic" else CAPS[slot]
    m["above_cap"] = [{"name": r["name"], "actual": round(float(r["actual"]), 2), "pred": round(float(r["pred"]), 2)}
                      for _, r in frame[frame["actual"] > cap].iterrows()]
    if "anchored" in frame:
        m["anchored"] = int(frame["anchored"].sum())
        m["unanchored"] = int((~frame["anchored"].astype(bool)).sum())
    return m


# --------------------------------------------------------------------------
# Pooling
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class SlateSample:
    slate: str
    pred: np.ndarray
    actual: np.ndarray


def pooled(samples: Sequence[SlateSample]) -> dict:
    """Player-weighted MAE / bias / RMSE over the concatenated slates; Spearman and
    top-level rank quality as the MEAN of per-slate tied-rank Spearman (ranks are
    not comparable across slates)."""
    if not samples:
        return {"slates": 0, "n": 0, "mae": None, "bias": None, "rmse": None, "spearman_mean": None}
    p = np.concatenate([s.pred for s in samples])
    a = np.concatenate([s.actual for s in samples])
    d = p - a
    rhos = [spearman_avg(s.pred, s.actual) for s in samples]
    rhos = [r for r in rhos if not math.isnan(r)]
    return {"slates": len(samples), "n": int(len(p)), "mae": float(np.mean(np.abs(d))), "bias": float(np.mean(d)),
            "rmse": float(np.sqrt(np.mean(d * d))), "spearman_mean": float(np.mean(rhos)) if rhos else None}


def cluster_bootstrap(samples: Sequence[SlateSample], reps: int = 2000, seed: int = 20261020) -> dict:
    """95% interval of each pooled statistic, resampling SLATES with replacement."""
    if len(samples) < 2:
        return {"reps": 0, "seed": seed, "note": "fewer than 2 slates: no interval"}
    rng = np.random.default_rng(seed)
    # Per-slate sufficient statistics, so a replicate is a weighted sum (same result as
    # pooled() on the resampled list, without recomputing ranks each time).
    n = np.array([len(s.pred) for s in samples], float)
    d = [s.pred - s.actual for s in samples]
    s_abs = np.array([np.abs(x).sum() for x in d])
    s_sum = np.array([x.sum() for x in d])
    s_sq = np.array([(x * x).sum() for x in d])
    rho = np.array([spearman_avg(s.pred, s.actual) for s in samples])
    stats: dict[str, list[float]] = {"mae": [], "bias": [], "rmse": [], "spearman_mean": []}
    for _ in range(reps):
        idx = rng.integers(0, len(samples), len(samples))
        tot = n[idx].sum()
        if tot == 0:
            continue
        stats["mae"].append(float(s_abs[idx].sum() / tot))
        stats["bias"].append(float(s_sum[idx].sum() / tot))
        stats["rmse"].append(float(np.sqrt(s_sq[idx].sum() / tot)))
        r = rho[idx]
        r = r[~np.isnan(r)]
        if len(r):
            stats["spearman_mean"].append(float(r.mean()))
    out: dict = {"reps": reps, "seed": seed}
    for k, v in stats.items():
        out[k] = [float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5))] if v else None
    return out


def design_tag(version: str, fmt: str, week: int | None) -> str:
    """`design` when the version's constants were read off this slate's week."""
    if week is None:
        return "unknown_week"
    if version == "v4" and week in V4_DESIGN_WEEKS:
        return "design"
    if version == "v3" and fmt == "showdown" and week in V3_SHOWDOWN_DESIGN_WEEKS:
        return "design"
    if version == "v5" and week in V5_DESIGN_WEEKS:
        return "design"
    return "not_design"


def concentration(frame: pd.DataFrame, k: int = 5) -> dict:
    """Error of the top-k predicted players vs everyone else (question 4)."""
    if frame.empty:
        return {}
    order = frame.sort_values("pred", ascending=False, kind="stable")
    top, rest = order.iloc[:k], order.iloc[k:]
    return {"top_k": k, "top_pred_sum": round(float(top["pred"].sum()), 2), "top_actual_sum": round(float(top["actual"].sum()), 2),
            "top_bias_mean": round(float((top["pred"] - top["actual"]).mean()), 3),
            "rest_bias_mean": round(float((rest["pred"] - rest["actual"]).mean()), 3) if len(rest) else None,
            "rest_pred_sum": round(float(rest["pred"].sum()), 2), "rest_actual_sum": round(float(rest["actual"].sum()), 2)}


def iter_slots(fmt: str) -> Iterable[str]:
    return ("CPT", "FLEX") if fmt == "showdown" else ("overall",)
