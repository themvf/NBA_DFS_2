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

`prior_showdown` mirrors the v3 Showdown path of web/src/lib/nfl-dfs/ownership-prior.ts
and `prior_showdown_v4` its v4 default, so a variant can be scored here before it is
ported; tests/test_nfl_showdown_ownership_eval.py and web/scripts/test-nfl-ownership-
prior.ts pin both languages to the same numbers (the v4 pins on the shared pool in
tests/fixtures/nfl_ownership_prior_showdown_fixture.json).

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


# nfl-ownership-prior-v4 Showdown constants, mirrored from
# web/src/lib/nfl-dfs/ownership-prior.ts (the authority). All stated priors.
V4_VERSION = "nfl-ownership-prior-v4"
PUNT_SALARY_MAX = 3000
PUNT_FLEX_BUDGET = 60.0
PUNT_MAX_PCT = 30.0
KICKER_MULTIPLIER = 0.6
CAPTAIN_POSITION_MULTIPLIER = {"TE": 0.7, "K": 0.25, "DST": 0.25}
DST_GAME_SCRIPT_MULTIPLIER = {"favorite": 1.3, "underdog": 0.7}
PROJECTION_WEIGHT = 0.6


def field_points(projection: float | None, dk_avg: float | None) -> float | None:
    """The number the field drafts on (fieldPoints in the TS): 60/40 blend of our
    projection and DraftKings' average, either alone when the other is missing."""
    def ok(v):
        return v is not None and not (isinstance(v, float) and np.isnan(v)) and np.isfinite(v)
    proj = float(projection) if ok(projection) else None
    avg = float(dk_avg) if ok(dk_avg) else None
    if proj is None and avg is None:
        return None
    if proj is None:
        return avg
    if avg is None:
        return proj
    return PROJECTION_WEIGHT * proj + (1 - PROJECTION_WEIGHT) * avg


def _allocate_detailed(ids: Sequence, scores: Mapping, budget: float, cap_for) -> tuple[dict, float]:
    """Exact port of allocateBudgetDetailed, including its in-pass order: within a
    pass `total` is fixed while `remaining` falls as players are capped, in key
    order. Returns (shares by id, unallocated budget). `ids` fixes the order."""
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
            cap = max(0.0, cap_for(i))
            if share > cap:
                out[i] = cap
                del open_[i]
                remaining -= cap
                capped = True
        if not capped:
            for i, s in open_.items():
                out[i] = remaining * (s / total)
            remaining = 0.0
            open_ = {}
    for i in ids:
        out.setdefault(i, 0.0)
    return out, max(0.0, remaining)


def prior_showdown_v4(slate: pd.DataFrame, favorite_team: str | None = None, underdog_team: str | None = None,
                      ) -> tuple[np.ndarray, np.ndarray, dict]:
    """nfl-ownership-prior-v4 Showdown: (flex %, captain %, notes) per row.

    Needs columns pts (the field-drafted number, as for `prior_showdown`),
    salary, captain_salary, dk_status, is_out, position, team. Mirrors
    projectOwnershipPrior(..., "showdown") in the TS with `variant: "v4"`:
    captain first (v3 shape x captain position x DST game script), then a punt
    tier (salary < $3,000, not out) sharing 60 of the 500 flex points on points^1
    capped at 30 (and under the one-slot total), then everyone else sharing what
    the tier did not take on the v3 shape x kicker x DST game script. Values are
    unrounded; the TS rounds each row to 0.01."""
    n = len(slate)
    pts = slate["pts"].to_numpy(float)
    status = [str(s or "").strip().upper() for s in slate["dk_status"].fillna("")]
    out = slate["is_out"].to_numpy(bool)
    pos = [str(p or "").upper() for p in slate["position"].fillna("")]
    teams = [None if (t is None or (isinstance(t, float) and np.isnan(t)) or t == "") else str(t)
             for t in slate["team"]]
    salary = slate["salary"].to_numpy(float)
    cap_salary = np.nan_to_num(slate["captain_salary"].to_numpy(float), nan=0.0)
    fav, dog = favorite_team, underdog_team                  # TS: fav truthiness, dog == null

    def game_script(i: int) -> float:
        if pos[i] != "DST" or not teams[i] or not fav:
            return 1.0
        if teams[i] == fav:
            return DST_GAME_SCRIPT_MULTIPLIER["favorite"]
        if dog is None or teams[i] == dog:
            return DST_GAME_SCRIPT_MULTIPLIER["underdog"]
        return 1.0

    def positional(i: int) -> float:
        return (KICKER_MULTIPLIER if pos[i] == "K" else 1.0) * game_script(i)

    def score(i: int, sal: float) -> float:                     # ownershipScore, value exponent 0.5
        if out[i] or not sal > 0 or not pts[i] > 0:
            return 0.0
        value = pts[i] / (max(sal, VALUE_SALARY_FLOOR) / 1000.0)
        return (max(pts[i], 0.5) ** 2.0) * (max(value, 0.2) ** 0.5) * STATUS_MULTIPLIER.get(status[i], 1.0)

    ids = list(range(n))
    captain_scores = {i: (score(i, cap_salary[i]) ** 1.5) * CAPTAIN_POSITION_MULTIPLIER.get(pos[i], 1.0) * game_script(i)
                      for i in ids}
    captain, cap_unalloc = _allocate_detailed(ids, captain_scores, 100.0, lambda i: 50.0)

    def flex_cap(i: int) -> float:
        return min(90.0, 95.0 - captain.get(i, 0.0))

    is_punt = [salary[i] > 0 and salary[i] < PUNT_SALARY_MAX and not out[i] for i in ids]
    punt_ids = [i for i in ids if is_punt[i]]
    punt_scores = {i: (0.0 if not pts[i] > 0 else max(pts[i], 0.5) * STATUS_MULTIPLIER.get(status[i], 1.0) * positional(i))
                   for i in punt_ids}
    punt, punt_unalloc = _allocate_detailed(punt_ids, punt_scores, PUNT_FLEX_BUDGET,
                                            lambda i: min(PUNT_MAX_PCT, flex_cap(i)))
    punt_taken = PUNT_FLEX_BUDGET - punt_unalloc
    main_ids = [i for i in ids if not is_punt[i]]
    main_scores = {i: score(i, salary[i]) * positional(i) for i in main_ids}
    main, main_unalloc = _allocate_detailed(main_ids, main_scores, 500.0 - punt_taken, flex_cap)
    flex = np.array([{**punt, **main}.get(i, 0.0) for i in ids])
    notes = {"variant": "v4", "puntTierPlayers": len(punt_ids), "puntTierPct": punt_taken,
             "gameScriptKnown": favorite_team is not None, "unallocatedPct": main_unalloc + cap_unalloc}
    return flex, np.array([captain[i] for i in ids]), notes


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
