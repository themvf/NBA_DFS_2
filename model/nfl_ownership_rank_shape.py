"""`nfl-ownership-rank-shape-v1`: a calibration layer over an ownership ORDERING.

Registered 2026-11-07 (docs/nfl-ownership-model.md, "Rank-shape calibration
layer"). The field's ownership-by-rank curve is close to a fixed function of
rank within a position (Classic) or slot (Showdown), while our forecasts' error
is mostly ordering. So the forecast for player i is

    own_i = sum_r P(rank_i = r) * curve_group(r),   rescaled to the group's mean total,

where P(rank) comes from perturbing each player's log forecast with Gaussian
noise of one frozen `sigma` per format and ranking the perturbed scores (an
average over `DRAWS` fixed-seed draws). sigma = 0 is the hard rank map, which
hands rank 1's whole curve value to whoever we rank first and was measured
WORSE than the base forecast on Classic; the soft map was better on 5 of 7
Classic slates and 11-13 of 14-16 Showdown slates, leave-one-slate-out.

The curves and group totals are frozen in `nfl_ownership_rank_shape_v1.json`
(written once by `research/nfl_ownership_rank_shape.py --freeze` from the
scorecard's complete-label reference contests of weeks 2-9). Nothing here is
fitted at run time; the module is pure and deterministic.
"""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

import numpy as np

VERSION = "nfl-ownership-rank-shape-v1"
BASE_VERSION = "nfl-ownership-prior-v5"  # the ordering this version is applied to in the scorecard
CURVES_PATH = Path(__file__).resolve().parent / "nfl_ownership_rank_shape_v1.json"

# Frozen 2026-11-07 from the leave-one-slate-out backtest on weeks 2-9 (the
# best of {0, 0.3, 0.6, 0.9, 1.2} per format). Not re-tuned without a new version.
SIGMA = {"classic": 0.6, "showdown": 0.3}
DRAWS = 200
SEED = 7
LOG_FLOOR = 0.05  # added before the log so a 0% forecast has a finite score
# Weeks whose contests supplied the frozen curves and the sigma choice.
DESIGN_WEEKS = range(2, 10)


def group_key(fmt: str, slot: str) -> str:
    """Classic curves are per position; Showdown curves are per slot over every player."""
    return "position" if fmt == "classic" else slot


@lru_cache(maxsize=1)
def load_curves(path: Path | str | None = None) -> dict:
    p = Path(path) if path else CURVES_PATH
    return json.loads(p.read_text(encoding="utf-8"))


def curves_digest(curves: dict) -> str:
    body = json.dumps(curves["curves"], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def hard_rank_shape(pred: np.ndarray, groups: np.ndarray, curve_set: dict) -> np.ndarray:
    """Map each group's ordering of `pred` through its curve, rescaled to the group's mean total.

    A group with no frozen curve keeps the base forecast. Ranks past the curve's
    length take its last value.
    """
    pred = np.asarray(pred, dtype=float)
    out = pred.copy()
    for grp in np.unique(groups):
        idx = np.flatnonzero(groups == grp)
        spec = curve_set.get(str(grp))
        if spec is None:
            continue
        curve = np.asarray(spec["curve"], dtype=float)
        order = np.argsort(-pred[idx], kind="stable")
        vals = np.array([curve[min(r, len(curve) - 1)] for r in range(len(idx))])
        s = vals.sum()
        if s > 0:
            vals = vals * float(spec["total"]) / s
        out[idx[order]] = vals
    return out


def soft_rank_shape(pred: np.ndarray, groups: np.ndarray, curve_set: dict, sigma: float, *,
                    draws: int = DRAWS, seed: int = SEED) -> np.ndarray:
    """Expected curve value over the rank distribution implied by log-normal score noise."""
    pred = np.asarray(pred, dtype=float)
    if sigma <= 0:
        return hard_rank_shape(pred, groups, curve_set)
    rng = np.random.default_rng(seed)
    base = np.log(np.clip(pred, 0.0, None) + LOG_FLOOR)
    acc = np.zeros(len(pred))
    for _ in range(draws):
        noisy = np.exp(base + rng.normal(0.0, sigma, len(base)))
        acc += hard_rank_shape(noisy, groups, curve_set)
    return acc / draws


def apply(pred, groups, fmt: str, slot: str, curves: dict | None = None) -> np.ndarray:
    """The registered forecast: `pred` is the base ordering (any version), `groups`
    the player's position (Classic) or any constant (Showdown)."""
    curves = curves or load_curves()
    key = group_key(fmt, slot)
    curve_set = curves["curves"][fmt][key]
    g = np.asarray(groups).astype(str) if fmt == "classic" else np.array(["all"] * len(pred))
    return soft_rank_shape(pred, g, curve_set, SIGMA[fmt])
