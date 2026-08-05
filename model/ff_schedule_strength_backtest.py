"""Walk-forward validation of the RB schedule-strength feature (and the
documented null result for QB/WR/TE).

Question: does a team's defense-vs-position rating in season N predict its
OWN rating in season N+1? Tested via a pooled AR(1) fit (rating_next ~ b0 +
b1 * rating_prev) across the 2023->2024 and 2024->2025 transitions,
evaluated separately per position and separately for the raw vs
opponent-adjusted rating -- never pooled across positions, same discipline
as every other walk-forward study in this repo.

Kill criterion (pre-registered before this was run 2026-08-05): a position
only becomes an applied projection factor if the bootstrap 95% CI on the
AR(1) slope excludes zero. This is the evidence behind
SCHEDULE_ADJUSTED_POSITIONS = {"RB"} and SCHEDULE_AR1_COEFFICIENTS in
ingest/ff_independent.py.

Honest caveat: only 2 season-transitions exist in the current nflverse
history (2023-2025), so n=64 team-position pairs per position (32 teams x 2
transitions) is thin by this project's usual bar (MLB/tennis studies
require n>=150-200). Treat this as a first, directional read -- revisit once
2026's season completes and a third transition (2025->2026) exists.

Usage:
    python -m model.ff_schedule_strength_backtest
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from ingest.ff_defense_stats import SCHEDULE_URL, TRACKED_POSITIONS, WEEKLY_STATS_URL, compute_season_ratings, fetch_csv
from ingest.ff_independent import build_schedule_context

SEASONS = (2023, 2024, 2025)
TRANSITIONS = ((2023, 2024), (2024, 2025))
N_BOOT = 2000
EVAL_ARTIFACT_PATH = "model/ff_schedule_strength_eval.json"


def _fit_ar1(x: np.ndarray, y: np.ndarray, n_boot: int = N_BOOT, seed: int = 7) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    X = np.vstack([np.ones(len(x)), x]).T
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    b0, b1 = coef
    pred = X @ coef
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot else None
    n = len(x)
    boots: list[float] = []
    for _ in range(n_boot):
        idx = rng.integers(0, n, n)
        Xb, yb = X[idx], y[idx]
        try:
            cb, *_ = np.linalg.lstsq(Xb, yb, rcond=None)
            boots.append(float(cb[1]))
        except Exception:
            continue
    lo, hi = (float(v) for v in np.percentile(boots, [2.5, 97.5]))
    return {
        "b0": round(float(b0), 4), "b1": round(float(b1), 4),
        "r2": round(float(r2), 4) if r2 is not None else None,
        "b1_ci_lo": round(lo, 4), "b1_ci_hi": round(hi, 4),
        "n": n, "significant": bool(lo > 0 or hi < 0),
    }


def run() -> dict[str, Any]:
    schedule = fetch_csv(SCHEDULE_URL)
    ratings_by_season: dict[int, pd.DataFrame] = {}
    for season in SEASONS:
        weekly = fetch_csv(WEEKLY_STATS_URL.format(season=season))
        ctx = build_schedule_context(schedule, season)
        ratings_by_season[season] = compute_season_ratings(weekly, ctx, season)

    results: dict[str, Any] = {
        "transitions": [f"{a}->{b}" for a, b in TRANSITIONS],
        "min_ci_excludes_zero_required": True,
        "positions": {},
    }
    for position in TRACKED_POSITIONS:
        pos_result: dict[str, Any] = {}
        for label, col in (("raw", "fpts_allowed_ppr_pg"), ("adjusted", "fpts_allowed_ppr_pg_adj")):
            prev_values: list[float] = []
            next_values: list[float] = []
            for prev_season, next_season in TRANSITIONS:
                prev = ratings_by_season[prev_season]
                nxt = ratings_by_season[next_season]
                prev_map = prev[prev["position"] == position].set_index("team_abbrev")[col]
                next_map = nxt[nxt["position"] == position].set_index("team_abbrev")[col]
                joined = pd.concat([prev_map, next_map], axis=1, keys=["prev", "next"]).dropna()
                prev_values.extend(joined["prev"].tolist())
                next_values.extend(joined["next"].tolist())
            pos_result[label] = _fit_ar1(np.array(prev_values), np.array(next_values))
        results["positions"][position] = pos_result

    results["applied_positions"] = sorted(
        position for position, r in results["positions"].items() if r["adjusted"]["significant"]
    )
    with open(EVAL_ARTIFACT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    return results


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
