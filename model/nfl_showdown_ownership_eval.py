"""Judge a Showdown ownership forecast by what it does to the lineups we build.

Accuracy alone (MAE, rank correlation) does not say whether a better forecast
changes a decision. On the 2026 weeks 2-3 Showdown contests (docs/nfl-ownership-
model.md) a perfect forecast and the stated prior both cut real-field duplication
sharply versus no ownership at all; the difference was projection points kept at
equal uniqueness (about 4-8 per lineup). So the measure here is the trade-off:

    for the portfolio an ownership input produces --
        projected points per lineup           (what the leverage cost us)
        share of lineups a real entry also played, and mean real copies
                                              (what the leverage bought)

`real_counts` is the contest's own lineup tally (nfl_dfs_field_structure parses
the same files), so duplication is measured against the field that actually
showed up, not a model of it.

`prior_showdown` mirrors web/src/lib/nfl-dfs/ownership-prior.ts so a variant can
be scored here before it is ported; tests/test_nfl_showdown_ownership_eval.py and
web/scripts/test-nfl-ownership-prior.ts pin both to the same numbers.

Pure: numpy/pandas/scipy, no database, no I/O. The seed is explicit; portfolios
from one seed are noisy, so compare configurations over several.
"""

from __future__ import annotations

from typing import Mapping, Sequence

import numpy as np
import pandas as pd
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.stats import spearmanr

VERSION = "nfl-showdown-ownership-eval-v1"

SALARY_CAP = 50_000
CAPTAIN_MULTIPLIER = 1.5
STATUS_MULTIPLIER = {"Q": 0.6, "D": 0.25}
VALUE_SALARY_FLOOR = 3000


def _allocate(scores: np.ndarray, budget: float, caps: np.ndarray) -> np.ndarray:
    """Share `budget` in proportion to `scores`, holding each at its cap and
    re-sharing the excess among the rest (mirror of allocateBudgetDetailed)."""
    scores = np.asarray(scores, float)
    out = np.zeros(len(scores))
    open_ = scores > 0
    remaining = float(budget)
    for _ in range(16):
        if not open_.any():
            break
        total = scores[open_].sum()
        share = np.where(open_, remaining * scores / total, 0.0)
        over = open_ & (share > caps)
        if not over.any():
            out[open_] = share[open_]
            break
        out[over] = caps[over]
        remaining -= caps[over].sum()
        open_ = open_ & ~over
    return out


def prior_showdown(slate: pd.DataFrame, points_exponent: float = 2.0, value_exponent: float = 1.5,
                   captain_exponent: float = 1.5, captain_budget: float = 100.0, flex_budget: float = 500.0,
                   captain_max: float = 50.0, flex_max: float = 90.0, total_max: float = 95.0
                   ) -> tuple[np.ndarray, np.ndarray]:
    """(flex %, captain %) per row. Needs columns pts, salary, captain_salary,
    dk_status, is_out. `pts` is the number the field drafts on (<= 0 draws nothing)."""
    n = len(slate)
    pts = slate["pts"].to_numpy(float)
    status = [str(s or "").strip().upper() for s in slate["dk_status"]]
    out = slate["is_out"].to_numpy(bool)

    def score(salary: np.ndarray) -> np.ndarray:
        value = pts / (np.maximum(salary, VALUE_SALARY_FLOOR) / 1000.0)
        mult = np.array([STATUS_MULTIPLIER.get(s, 1.0) for s in status])
        s = (np.maximum(pts, 0.5) ** points_exponent) * (np.maximum(value, 0.2) ** value_exponent) * mult
        return np.where(out | (salary <= 0) | (pts <= 0), 0.0, s)

    flex_salary = slate["salary"].to_numpy(float)
    cap_salary = np.nan_to_num(slate["captain_salary"].to_numpy(float), nan=0.0)
    cap = _allocate(score(cap_salary) ** captain_exponent, captain_budget, np.full(n, captain_max))
    flex = _allocate(score(flex_salary), flex_budget, np.minimum(flex_max, total_max - cap))
    return flex, cap


def fit_metrics(slate: pd.DataFrame, flex: np.ndarray, cap: np.ndarray) -> dict[str, float]:
    """Forecast accuracy against the contest's own flex/captain ownership
    (columns flex_act, cpt_act). `rho_chalk` ranks only players the field used."""
    act = slate["flex_act"].to_numpy(float) + slate["cpt_act"].to_numpy(float)
    pred = flex + cap
    chalk = act >= 1
    return {
        "mae_flex": float(np.mean(np.abs(flex - slate["flex_act"].to_numpy(float)))),
        "mae_captain": float(np.mean(np.abs(cap - slate["cpt_act"].to_numpy(float)))),
        "rho_chalk": float(spearmanr(pred[chalk], act[chalk])[0]) if chalk.sum() > 4 else float("nan"),
    }


def build_lineup(slate: pd.DataFrame, objective: np.ndarray, captain_objective: np.ndarray) -> tuple[str, tuple[str, ...]] | None:
    """Best Showdown lineup (1 captain + 5 flex) under the cap, both teams
    represented. Returns (captain, sorted flex names) as `normalized_name`s."""
    n = len(slate)
    salary = slate["salary"].to_numpy(float)
    teams = slate["team"].to_numpy()
    rows, lo, hi = [], [], []

    def add(row, low, high):
        rows.append(row); lo.append(low); hi.append(high)

    add(np.r_[CAPTAIN_MULTIPLIER * salary, salary], 0, SALARY_CAP)
    add(np.r_[np.ones(n), np.zeros(n)], 1, 1)
    add(np.r_[np.zeros(n), np.ones(n)], 5, 5)
    for i in range(n):                                  # one slot per player
        row = np.zeros(2 * n); row[i] = 1; row[n + i] = 1
        add(row, 0, 1)
    for team in sorted(set(teams)):                     # at least one from each team
        mask = (teams == team).astype(float)
        add(np.r_[mask, mask], 1, 6)
    result = milp(-np.r_[captain_objective, objective], constraints=LinearConstraint(np.array(rows), lo, hi),
                  integrality=np.ones(2 * n), bounds=Bounds(0, 1))
    if result.x is None:
        return None
    chosen = result.x > 0.5
    names = slate["normalized_name"].to_numpy()
    return str(names[np.where(chosen[:n])[0][0]]), tuple(sorted(str(names[j]) for j in np.where(chosen[n:])[0]))


def portfolio(slate: pd.DataFrame, flex_own: np.ndarray, cpt_own: np.ndarray, leverage: float, seed: int,
              lineups: int = 80, noise: float = 0.2) -> list[tuple[str, tuple[str, ...]]]:
    """`lineups` lineups maximising projection x (1 - ownership)^leverage, each
    on projections perturbed by multiplicative lognormal noise (diversifies)."""
    rng = np.random.default_rng(seed)
    base = slate["pts"].to_numpy(float)
    flex_factor = (1 - np.clip(flex_own, 0, 95) / 100) ** leverage
    cpt_factor = (1 - np.clip(cpt_own, 0, 95) / 100) ** leverage
    out = []
    for _ in range(lineups):
        pts = base * np.exp(rng.normal(0, noise, len(slate)))
        built = build_lineup(slate, pts * flex_factor, CAPTAIN_MULTIPLIER * pts * cpt_factor)
        if built:
            out.append(built)
    return out


def score_portfolio(slate: pd.DataFrame, lineups: Sequence[tuple[str, tuple[str, ...]]],
                    real_counts: Mapping[tuple[str, tuple[str, ...]], int]) -> dict[str, float]:
    """Projected points kept, and how often the real field played the same lineup."""
    pts = dict(zip(slate["normalized_name"], slate["pts"].to_numpy(float)))
    copies = np.array([real_counts.get(lineup, 0) for lineup in lineups])
    projected = [CAPTAIN_MULTIPLIER * pts[c] + sum(pts[f] for f in flex) for c, flex in lineups]
    return {"lineups": len(lineups), "proj_pts": float(np.mean(projected)),
            "share_duplicated": float((copies > 0).mean()), "mean_real_copies": float(copies.mean()),
            "max_real_copies": int(copies.max())}
