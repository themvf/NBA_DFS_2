"""Grading for the ghost-lineup factorial (`nfl-ghost-factorial-v1`).

docs/nfl-ghost-factorial-study.md registers the rules this module applies:

* Every subject (each arm, the house, and any owner build on the same slate)
  is scored with the flagship contest's own player points, placed against
  the contest's score curve (top 1% and top 0.1% thresholds) and paid from
  its payout table. A lineup with a player whose points are unknown is
  UNSCORED and counted as such, never summed as zero.
* Primary outcome per arm and slate: top-1% hits per built lineup.
  Secondary: return per dollar entered, and completion (built / requested).
* Factor effects: each arm's outcome is regressed on its factor levels
  (indicators against the house level) with the slate held fixed (within-
  slate demeaning), and the uncertainty comes from resampling whole slates.
  Effects are computed only at a registered milestone (8 slates per format);
  before that the report is descriptive and the effects are withheld.
* Screening: at the milestone, a factor level whose bootstrap two-sided p
  passes Benjamini-Hochberg at q = 0.10 becomes a CANDIDATE. A candidate
  changes nothing; it is re-tested on the next 8 fresh slates under a new
  registration before any default changes.
* Extra arms (amendment 2026-10-30, `nfl-ghost-extra-arms-v1`): arms above
  DESIGN_ARMS sit OUTSIDE the array (the house with one setting moved, today
  the GPP boom weight at 1 and 4 against 2). They never enter the factorial
  regression; each is graded as the paired within-slate difference against
  the house, with the same slate bootstrap, milestone and Benjamini-Hochberg
  rule, and the same confirmation requirement.

Pure: no database access, no I/O.
"""

from __future__ import annotations

import math
import random
from collections import defaultdict
from typing import Any, Iterable

GRADER_VERSION = "nfl-ghost-grade-v1"
DESIGN_ARMS = 32
MILESTONE_SLATES = 8
FDR_Q = 0.10
BOOTSTRAP = 2000
BOOTSTRAP_SEED = 20261016


def curve_at(curve: list, fraction: float, entries: int) -> float:
    """Score at a top fraction of the field from the stored [rank, score] curve."""
    rank = max(1, math.ceil(fraction * entries))
    best = None
    for r, score in curve:
        if r <= rank:
            best = score
    return float(best if best is not None else curve[0][1])


def payout_for(curve: dict | None, score: float | None) -> float | None:
    if curve is None or score is None:
        return None
    for min_score, payout in curve["breakpoints"]:
        if score >= min_score:
            return float(payout)
    return 0.0


def score_lineup(slots: list[dict], points_for) -> tuple[float | None, list[str]]:
    """(score, missing names). A captain counts 1.5x; any unknown player leaves the lineup unscored."""
    total, missing = 0.0, []
    for s in slots:
        pts = points_for(s)
        if pts is None:
            missing.append(str(s.get("name")))
            continue
        total += pts * (1.5 if str(s.get("slot")).upper() == "CPT" else 1.0)
    return (None, missing) if missing else (round(total, 2), [])


def grade_subject(lineups: list[list[dict]], requested: int, points_for, contest: dict) -> dict:
    """Metrics for one portfolio against one contest. `contest` carries entries, score_curve, payout_curve, entry_fee."""
    t1 = curve_at(contest["score_curve"], 0.01, contest["entries"])
    t01 = curve_at(contest["score_curve"], 0.001, contest["entries"])
    scores, unscored = [], []
    for slots in lineups:
        score, missing = score_lineup(slots, points_for)
        if score is None:
            unscored.extend(missing)
        else:
            scores.append(score)
    payouts = [payout_for(contest.get("payout_curve"), s) for s in scores]
    paid = [p for p in payouts if p is not None]
    fee = contest.get("entry_fee")
    return {
        "lineups": requested, "built": len(lineups), "scored": len(scores),
        "top1_hits": sum(1 for s in scores if s >= t1), "top01_hits": sum(1 for s in scores if s >= t01),
        "cashes": sum(1 for p in paid if p > 0) if paid else None,
        "payout": round(sum(paid), 2) if len(paid) == len(scores) and contest.get("payout_curve") else None,
        "entry_fee": fee,
        "best_score": max(scores) if scores else None,
        "mean_score": round(sum(scores) / len(scores), 2) if scores else None,
        "detail": {"top1_threshold": t1, "top01_threshold": t01, "unscored_players": sorted(set(unscored))[:20],
                   "unscored_lineups": len(lineups) - len(scores)},
    }


def outcomes(row: dict) -> dict[str, float | None]:
    """The registered outcomes for one arm-slate (None where undefined)."""
    built, scored = row.get("built") or 0, row.get("scored") or 0
    fee, payout = row.get("entry_fee"), row.get("payout")
    return {
        "top1_rate": (row["top1_hits"] / scored) if scored else None,
        "roi": (payout / (fee * scored) - 1) if scored and fee and payout is not None else None,
        "completion": (built / row["lineups"]) if row.get("lineups") else None,
    }


def _indicators(levels: dict, house: dict) -> dict[str, float]:
    """One indicator per non-house level of each factor: 1 when the arm sits at that level."""
    return {f"{k}={v}": 1.0 for k, v in levels.items() if house.get(k) != v}


def _ols(x: list[list[float]], y: list[float]) -> list[float] | None:
    """Least squares by normal equations with Gaussian elimination; None when singular."""
    p = len(x[0]) if x else 0
    if not p or len(x) <= p:
        return None
    xtx = [[sum(r[i] * r[j] for r in x) for j in range(p)] for i in range(p)]
    xty = [sum(r[i] * yy for r, yy in zip(x, y)) for i in range(p)]
    a = [row[:] + [b] for row, b in zip(xtx, xty)]
    for c in range(p):
        piv = max(range(c, p), key=lambda r: abs(a[r][c]))
        if abs(a[piv][c]) < 1e-10:
            return None
        a[c], a[piv] = a[piv], a[c]
        for r in range(p):
            if r != c:
                f = a[r][c] / a[c][c]
                a[r] = [u - f * v for u, v in zip(a[r], a[c])]
    return [a[i][p] / a[i][i] for i in range(p)]


def fit_effects(cells: list[dict], house: dict, outcome: str) -> tuple[list[str], list[float] | None]:
    """Within-slate OLS of `outcome` on level indicators. `cells`: {slate, levels, <outcome>}."""
    keys = sorted({k for c in cells for k in _indicators(c["levels"], house)})
    by_slate: dict[Any, list[dict]] = defaultdict(list)
    for c in cells:
        if c.get(outcome) is not None:
            by_slate[c["slate"]].append(c)
    xs, ys = [], []
    for group in by_slate.values():
        if len(group) < 2:
            continue
        mats = [[_indicators(c["levels"], house).get(k, 0.0) for k in keys] for c in group]
        means = [sum(m[j] for m in mats) / len(mats) for j in range(len(keys))]
        ybar = sum(c[outcome] for c in group) / len(group)
        for m, c in zip(mats, group):
            xs.append([v - mu for v, mu in zip(m, means)])
            ys.append(c[outcome] - ybar)
    return keys, _ols(xs, ys)


def benjamini_hochberg(pvalues: dict[str, float], q: float = FDR_Q) -> set[str]:
    ranked = sorted(pvalues.items(), key=lambda kv: kv[1])
    m, cut = len(ranked), 0
    for i, (_, p) in enumerate(ranked, 1):
        if p <= q * i / m:
            cut = i
    return {k for k, _ in ranked[:cut]}


def effects_report(cells: list[dict], house: dict, outcome: str, *, boots: int = BOOTSTRAP, seed: int = BOOTSTRAP_SEED) -> dict:
    """Point effects, slate-bootstrap 95% intervals and two-sided p, and the BH candidates."""
    keys, point = fit_effects(cells, house, outcome)
    slates = sorted({c["slate"] for c in cells})
    if point is None:
        return {"outcome": outcome, "slates": len(slates), "effects": None, "reason": "design matrix singular or too few cells"}
    rng = random.Random(seed)
    draws: list[list[float]] = []
    by = defaultdict(list)
    for c in cells:
        by[c["slate"]].append(c)
    for _ in range(boots):
        sample = []
        for i, s in enumerate(rng.choice(slates) for _ in slates):
            sample.extend({**c, "slate": (s, i)} for c in by[s])
        _, b = fit_effects(sample, house, outcome)
        if b is not None and len(b) == len(keys):
            draws.append(b)
    effects, pvals = [], {}
    for j, k in enumerate(keys):
        col = sorted(d[j] for d in draws)
        if not col:
            continue
        lo, hi = col[int(0.025 * len(col))], col[min(len(col) - 1, int(0.975 * len(col)))]
        p = min(1.0, 2 * min(sum(1 for v in col if v <= 0), sum(1 for v in col if v >= 0)) / len(col))
        pvals[k] = p
        effects.append({"level": k, "effect": round(point[j], 5), "ci95": [round(lo, 5), round(hi, 5)], "p": round(p, 4)})
    candidates = benjamini_hochberg(pvals) if pvals else set()
    for e in effects:
        e["candidate"] = e["level"] in candidates
    return {"outcome": outcome, "slates": len(slates), "bootstraps": len(draws), "effects": effects,
            "candidates": sorted(candidates), "fdr_q": FDR_Q}


def extra_arm_label(levels: dict, house: dict) -> str:
    """The one setting an extra arm moves, as `key=value` (every other level equals the house's)."""
    moved = sorted(f"{k}={v}" for k, v in levels.items() if house.get(k) != v)
    return ",".join(moved)


def paired_report(cells: list[dict], house: dict, outcome: str, *, boots: int = BOOTSTRAP, seed: int = BOOTSTRAP_SEED) -> dict:
    """Extra arms against the house: per level, the mean within-slate difference (arm minus house) over
    the slates where both are graded, a slate-bootstrap 95% interval and two-sided p, and the
    Benjamini-Hochberg candidates across the extra levels of this outcome. `cells` holds house and extra cells only."""
    house_by_slate = {c["slate"]: c.get(outcome) for c in cells if c["arm"] == 0}
    diffs: dict[str, dict[Any, float]] = defaultdict(dict)
    for c in cells:
        if c["arm"] == 0 or c.get(outcome) is None or house_by_slate.get(c["slate"]) is None:
            continue
        diffs[extra_arm_label(c["levels"], house)][c["slate"]] = c[outcome] - house_by_slate[c["slate"]]
    rng = random.Random(seed)
    levels, pvals = [], {}
    for label in sorted(diffs):
        d = diffs[label]
        slates = sorted(d)
        values = [d[s] for s in slates]
        mean = sum(values) / len(values)
        means = sorted(sum(rng.choice(values) for _ in values) / len(values) for _ in range(boots))
        lo, hi = means[int(0.025 * len(means))], means[min(len(means) - 1, int(0.975 * len(means)))]
        p = min(1.0, 2 * min(sum(1 for v in means if v <= 0), sum(1 for v in means if v >= 0)) / len(means))
        pvals[label] = p
        levels.append({"level": label, "slates": len(slates), "mean_diff": round(mean, 5), "ci95": [round(lo, 5), round(hi, 5)], "p": round(p, 4)})
    candidates = benjamini_hochberg(pvals) if pvals else set()
    for e in levels:
        e["candidate"] = e["level"] in candidates
    return {"outcome": outcome, "bootstraps": boots, "levels": levels, "candidates": sorted(candidates), "fdr_q": FDR_Q}


def report(format_: str, graded: Iterable[dict], house: dict) -> dict:
    """The weekly report for one format. `graded`: one row per (slate, arm) with levels and metrics.
    Arms above DESIGN_ARMS are extra arms: kept out of the factorial screen, reported as pairs against the house."""
    cells = []
    for g in graded:
        cells.append({"slate": g["freeze_id"], "arm": g["arm"], "levels": g["levels"], **outcomes(g)})
    design = [c for c in cells if c["arm"] <= DESIGN_ARMS]
    extra = [c for c in cells if c["arm"] > DESIGN_ARMS]
    slates = sorted({c["slate"] for c in design})
    milestone = len(slates) >= MILESTONE_SLATES
    out: dict[str, Any] = {"version": GRADER_VERSION, "format": format_, "slates": len(slates), "milestone_slates": MILESTONE_SLATES,
                           "milestone_reached": milestone}
    if milestone:
        out["screening"] = {o: effects_report(design, house, o) for o in ("top1_rate", "roi", "completion")}
    else:
        out["screening"] = None
        out["note"] = (f"{len(slates)} of {MILESTONE_SLATES} slates graded: factor effects are withheld until the "
                       "registered milestone; per-slate results are descriptive.")
    extra_slates = sorted({c["slate"] for c in extra})
    paired_cells = extra + [c for c in design if c["arm"] == 0 and c["slate"] in set(extra_slates)]
    out["extra_arms"] = {"slates": len(extra_slates), "milestone_reached": len(extra_slates) >= MILESTONE_SLATES,
                         "paired": ({o: paired_report(paired_cells, house, o) for o in ("top1_rate", "roi", "completion")}
                                    if len(extra_slates) >= MILESTONE_SLATES else None)}
    if len(extra_slates) < MILESTONE_SLATES:
        out["extra_arms"]["note"] = (f"{len(extra_slates)} of {MILESTONE_SLATES} slates carry the extra arms: paired differences "
                                     "against the house are withheld until the registered milestone.")
    return out
