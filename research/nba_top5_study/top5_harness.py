"""Shared harness for the top-5-lineups study. Every approach is scored with this file.

Contest: an entry names three NBA players; its score is the sum of their actual points
(a player who sits scores 0); the single highest entry takes everything; ties split; the
field is 200+ entries and almost every entry holds the night's biggest-line player.

Data (built by build_dataset.py, nothing here touches the database):
  data/nights.json            each night replayed as it stood one minute before the first tip
  data/box_2026_27.json       this season's box scores (all players, all games so far)
  data/player_logs_2025_26.json, data/team_logs_2025_26.json   last season (regular + playoffs)
  data/calibration_rows.json  the nightly grade of the books' points lines (each game's last pre-tip capture)

Leakage rule: a selector receives ``pre_lock(night)`` (actual results removed) and
``History(night)`` (box scores and calibration rows strictly before the night's date, plus
all of last season). Anything fitted must be fitted only on that history (walk-forward).

Usage:
    from top5_harness import load_nights, evaluate, line_sum_selector
    report = evaluate(my_selector, label="mine")   # dict with per-night rows and a summary
"""
from __future__ import annotations
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[0])); import _paths

import copy
import itertools
import json
import math
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import numpy as np

HERE = Path(__file__).resolve().parent
DATA = _paths.DATA_ROOT / "data"
REPO = _paths.REPO
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

RESULT_FIELDS = ("actual", "minutes", "played", "starter")
FIELD_SIZE = 300
FIELDS = 2000


# ------------------------------------------------------------------ data

@lru_cache(maxsize=1)
def _nights_raw() -> tuple:
    return tuple(json.loads((DATA / "nights.json").read_text(encoding="utf-8")))


def load_nights(complete_only: bool = True) -> list[dict]:
    """Nights in date order. ``complete_only`` drops any night whose games are not all final."""
    return [copy.deepcopy(n) for n in _nights_raw() if n["complete"] or not complete_only]


@lru_cache(maxsize=1)
def _box() -> tuple:
    return tuple(json.loads((DATA / "box_2026_27.json").read_text(encoding="utf-8")))


@lru_cache(maxsize=1)
def last_season_logs() -> tuple:
    """2025-26 player game logs (regular season and playoffs). Entirely before every night here."""
    return tuple(json.loads((DATA / "player_logs_2025_26.json").read_text(encoding="utf-8")))


@lru_cache(maxsize=1)
def last_season_team_logs() -> tuple:
    return tuple(json.loads((DATA / "team_logs_2025_26.json").read_text(encoding="utf-8")))


@lru_cache(maxsize=1)
def _calibration() -> tuple:
    return tuple(json.loads((DATA / "calibration_rows.json").read_text(encoding="utf-8")))


class History:
    """What was known before a night: this season's box scores and line grades from earlier dates, all of last season."""

    def __init__(self, night: dict):
        self.date = night["date"]
        self.box = [r for r in _box() if str(r["game_date"]) < self.date]
        self.calibration = [r for r in _calibration() if str(r["game_date"]) < self.date]
        self.last_season = last_season_logs()
        self.last_season_teams = last_season_team_logs()
        self.prior_nights = [pre_lock_with_results(n) for n in _nights_raw() if n["date"] < self.date and n["complete"]]


def pre_lock(night: dict) -> dict:
    """The night as it stood at the lock: no actual points, minutes, starters or final scores."""
    v = copy.deepcopy(night)
    for p in v["players"]:
        for f in RESULT_FIELDS:
            p.pop(f, None)
    v.pop("slate", None)
    for g in v["games"]:
        g.pop("homeScore", None)
        g.pop("awayScore", None)
    return v


def pre_lock_with_results(night: dict) -> dict:
    """An EARLIER night, results included (for walk-forward fitting)."""
    return copy.deepcopy(night)


def pool(night: dict, n: int = 20) -> list[dict]:
    return sorted(night["players"], key=lambda p: p["lineRank"])[:n]


def to_pool_players(view: dict, n: int = 20):
    """The research module's PoolPlayer objects, for its simulate_points(pool, sims, rng, recal=...)."""
    from research.nba_wta_trios import PoolPlayer
    out = []
    for p in pool(view, n):
        out.append(PoolPlayer(key=p["key"], player=p["name"], event_id=p["eventId"], home_team=p["home"], away_team=p["away"],
                              team=p["team"], line=p["line"], p_over=p["pOver"], books=p["books"], ladder_books=p["ladderBooks"],
                              curve=[tuple(c) for c in p["curve"]], floor=p["floor"], ceiling=p["ceiling"], p30=p["p30"], p40=p["p40"]))
    return out


# ------------------------------------------------------------------ simulation helpers (walk-forward safe)
#
# The research module's RECAL_G was fitted on Oct 20 - Nov 5, i.e. on nights scored here. Do NOT use
# simulate_points(..., recal=True) on those nights; fit the correction from History with fit_recal().

RECAL_KNOTS = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0)


def pit_level(player: dict, points: float) -> float | None:
    """F-level (1 - survival) of ``points`` under the player's own ladder curve at the lock; None without a usable curve."""
    from research.nba_wta_trios import survival_knots
    knots = survival_knots([tuple(c) for c in player["curve"]])
    if knots is None:
        return None
    s_arr, x_arr = knots            # survival ascending, points descending
    s = float(np.interp(points, x_arr[::-1], s_arr[::-1]))
    return 1.0 - s


def fit_recal(prior_nights: list[dict], pseudo: float = 200.0, deep: int = 60) -> tuple:
    """Walk-forward version of the module's RECAL_G: realized CDF of each PLAYED player's PIT under his own
    curve at RECAL_KNOTS, shrunk toward the identity with ``pseudo`` rows. Uses only the nights passed in."""
    pits = []
    for n in prior_nights:
        for p in pool(n, deep):
            if p.get("played") and p.get("actual") is not None:
                v = pit_level(p, p["actual"])
                if v is not None:
                    pits.append(v)
    pits = np.array(pits)
    m = len(pits)
    g = [float(((pits <= k).sum() + pseudo * k) / (m + pseudo)) if 0 < k < 1 else float(k) for k in RECAL_KNOTS]
    for i in range(1, len(g)):
        g[i] = max(g[i], g[i - 1] + 1e-6) if i < len(g) - 1 else 1.0
    return tuple(g)


def simulate(view: dict, sims: int, rng: np.random.Generator, n: int = 20, recal_g: tuple | None = None,
             sit: list[float] | None = None, rho_teammate: float = -0.06, rho_opponent: float = 0.02) -> tuple[list[str], np.ndarray]:
    """(keys, sims x players) whole-point draws for the top-n lines of a pre-lock view: the books' curves
    through a Gaussian copula, optionally thinned at the top with a walk-forward ``recal_g`` from fit_recal().
    ``sit`` = per-player sitting chance (default: the view's sitProbLead)."""
    from scipy.special import ndtr, ndtri
    from research.nba_wta_trios import survival_knots
    players = pool(view, n)
    k = len(players)
    c = np.eye(k)
    for i, j in itertools.combinations(range(k), 2):
        a, b = players[i], players[j]
        if a["eventId"] == b["eventId"] and a["team"] and b["team"]:
            c[i, j] = c[j, i] = rho_teammate if a["team"] == b["team"] else rho_opponent
    w, v = np.linalg.eigh(c)
    if w.min() < 1e-9:
        w = np.clip(w, 1e-6, None)
        c = (v * w) @ v.T
        d = np.sqrt(np.diag(c))
        c = c / np.outer(d, d)
    z = rng.standard_normal((sims, k)) @ np.linalg.cholesky(c).T
    u = ndtr(-z)                                        # survival level; high z = high score
    if recal_g is not None:
        u = 1.0 - np.interp(1.0 - u, recal_g, RECAL_KNOTS)
        z = -ndtri(np.clip(u, 1e-12, 1 - 1e-12))
    out = np.zeros((sims, k))
    for i, p in enumerate(players):
        knots = survival_knots([tuple(cc) for cc in p["curve"]])
        if knots is None:
            sd = 3.9 + 0.18 * p["line"]
            out[:, i] = np.maximum(0.0, p["line"] + 0.5 + sd * z[:, i])
        else:
            s_arr, x_arr = knots
            out[:, i] = np.interp(np.clip(u[:, i], s_arr[0], s_arr[-1]), s_arr, x_arr)
        q = (sit[i] if sit is not None else p.get("sitProbLead") or p.get("sitProbBooks") or 0.0)
        out[rng.random(sims) < q, i] = 0.0
    return [p["key"] for p in players], np.rint(np.maximum(out, 0.0))


# ------------------------------------------------------------------ scoring

def _actual(night: dict) -> dict[str, float]:
    """Actual points by key for the deep pool; an unresolved player (None) counts 0 and is reported."""
    return {p["key"]: (p["actual"] if p["actual"] is not None else 0.0) for p in night["players"]}


def trio_total(night: dict, trio) -> float:
    a = _actual(night)
    return float(sum(a.get(k, 0.0) for k in trio))


def pool_trio_totals(night: dict, n: int = 20) -> np.ndarray:
    a = _actual(night)
    keys = [p["key"] for p in pool(night, n)]
    return np.array(sorted((a[i] + a[j] + a[k] for i, j, k in itertools.combinations(keys, 3)), reverse=True))


def slate_best(night: dict) -> float:
    pts = sorted((s["points"] for s in night["slate"]), reverse=True)
    return float(sum(pts[:3]))


SCENARIOS = {
    "v1": (0.10, 3.0, None),      # the tracked test's stated prior: 10% on the exact top-3 lines, rest ~ exp(line/3)
    "pileon": (0.10, 3.0, 0.85),  # the owner's report: ~87% of entries hold the biggest line
    "spread": (0.0, 5.0, None),   # a field that spreads its picks
}


def _field_picks(lines: np.ndarray, rng: np.random.Generator, scenario: str, fields: int, size: int) -> np.ndarray:
    """(fields x size x 3) pool indices. Pool sorted by line, index 0 = biggest line."""
    chalk_share, tau, top_share = SCENARIOS[scenario]
    n = len(lines)
    n_chalk = int(round(size * chalk_share))
    rest = size - n_chalk
    g = lines / tau + rng.gumbel(size=(fields, rest, n))
    if top_share is not None:
        hold = rng.random((fields, rest)) < top_share
        g[:, :, 0] = np.where(hold, np.inf, -np.inf)
    picks = np.argpartition(-g, 3, axis=2)[:, :, :3]
    chalk = np.broadcast_to(np.arange(3), (fields, n_chalk, 3))
    return np.concatenate([chalk, picks], axis=1)


def _seed(*parts) -> int:
    import zlib
    return zlib.crc32("|".join(map(str, parts)).encode())


def field_share(night: dict, entries: list, scenario: str, fields: int = FIELDS, size: int = FIELD_SIZE, seed: int = 0) -> float:
    """Share of the prize our entries win against simulated fields that pick from the top-20 lines, with ACTUAL points.
    Deterministic: the same night and scenario always draw the same fields, so two selectors face identical fields."""
    p20 = pool(night, 20)
    a = _actual(night)
    pts = np.array([a[p["key"]] for p in p20])
    lines = np.array([p["line"] for p in p20])
    ours = np.array([trio_total(night, t) for t in entries])
    top_ours = ours.max()
    n_top_ours = int((ours == top_ours).sum())
    rng = np.random.default_rng(_seed(night["date"], scenario, seed))
    total = 0.0
    chunk = 250
    for start in range(0, fields, chunk):
        f = min(chunk, fields - start)
        tot = pts[_field_picks(lines, rng, scenario, f, size)].sum(axis=2)     # (f x size)
        fmax = tot.max(axis=1)
        fcount = (tot == fmax[:, None]).sum(axis=1)
        total += float(np.where(top_ours > fmax, 1.0, np.where(top_ours == fmax, n_top_ours / (n_top_ours + fcount), 0.0)).sum())
    return total / fields


def _in_top5(night: dict, entries: list) -> int:
    """How many of our entries are among the night's five highest-scoring top-20-line trios (ties at 5th included)."""
    a = _actual(night)
    keys = [p["key"] for p in pool(night, 20)]
    tots = sorted(((a[i] + a[j] + a[k]), frozenset((i, j, k))) for i, j, k in itertools.combinations(keys, 3))
    tots.sort(key=lambda t: -t[0])
    cut = tots[4][0] if len(tots) >= 5 else tots[-1][0]
    top = {t for v, t in tots if v >= cut}
    return sum(1 for e in entries if frozenset(e) in top)


def score_entries(night: dict, entries: list, scenarios=("v1", "pileon", "spread"), fields: int = FIELDS) -> dict:
    """Field-free and field-based scores for one night's entries (each a 3-tuple of player keys)."""
    if len(entries) == 0:
        raise ValueError("no entries")
    for t in entries:
        if len(set(t)) != 3:
            raise ValueError(f"bad trio {t}")
    known = {p["key"] for p in night["players"]}
    for t in entries:
        for k in t:
            if k not in known:
                raise ValueError(f"{k} is not in the night's 60-line pool")
    totals = [trio_total(night, t) for t in entries]
    best = max(totals)
    pt = pool_trio_totals(night, 20)
    rank20 = 1 + int((pt > best).sum())
    pct20 = float(((pt < best).sum() + 0.5 * (pt == best).sum()) / len(pt))
    out = {"date": night["date"], "best": best, "sum": float(sum(totals)), "totals": totals,
           "bestPool20": float(pt[0]), "gapPool20": float(pt[0] - best), "rankPool20": rank20, "pctPool20": pct20,
           "slateBest": slate_best(night), "gapSlate": slate_best(night) - best,
           "top1": rank20 <= 1, "top5": rank20 <= 5, "top10": rank20 <= 10, "top1pct": pct20 >= 0.99,
           "inTop5Trios": _in_top5(night, entries),
           "unresolved": sum(1 for t in entries for k in t if next(p for p in night["players"] if p["key"] == k)["actual"] is None)}
    for s in scenarios:
        out[f"share_{s}"] = field_share(night, entries, s, fields=fields)
    return out


# ------------------------------------------------------------------ baselines and evaluation

def line_sum_selector(view: dict, hist: History | None = None, k: int = 5) -> list[tuple]:
    """The k trios with the largest line sums from the top-20 lines (the tracked test's baseline)."""
    p20 = pool(view, 20)
    trios = sorted(itertools.combinations(p20, 3),
                   key=lambda t: (-sum(p["line"] for p in t), -sum((p["pOver"] or 0.5) for p in t), tuple(p["key"] for p in t)))
    return [tuple(p["key"] for p in t) for t in trios[:k]]


def v1_selector(view: dict, hist: History | None = None, k: int = 5) -> list[tuple] | None:
    """The tracked test's frozen entries for the night (Nov 6 onward only)."""
    return [tuple(e) for e in view["v1"]["entries"][:k]] if view.get("v1") else None


def paired_ci(diffs: list[float], draws: int = 5000, seed: int = 0) -> dict:
    d = np.asarray(diffs, dtype=float)
    if len(d) == 0:
        return {"n": 0}
    rng = np.random.default_rng(seed)
    means = d[rng.integers(0, len(d), size=(draws, len(d)))].mean(axis=1)
    return {"n": len(d), "mean": float(d.mean()), "lo": float(np.percentile(means, 2.5)), "hi": float(np.percentile(means, 97.5)),
            "wins": int((d > 0).sum()), "losses": int((d < 0).sum())}


METRICS = ("best", "gapPool20", "pctPool20", "rankPool20", "gapSlate", "inTop5Trios", "share_v1", "share_pileon", "share_spread")


def evaluate(selector: Callable, label: str = "selector", k: int = 5, nights: list[dict] | None = None,
             fields: int = FIELDS, verbose: bool = False, history_fn: Callable | None = None,
             baseline: Callable | None = None) -> dict:
    """Run ``selector(pre_lock(night), History(night)) -> k trios`` on every complete night, walk-forward,
    and compare with the line-sum baseline on identical nights (paired, night-resampled 95% intervals)."""
    nights = nights if nights is not None else load_nights()
    history_fn = history_fn or History
    baseline = baseline or line_sum_selector
    rows, base_rows = [], []
    for night in nights:
        view, hist = pre_lock(night), history_fn(night)
        entries = selector(view, hist)
        if entries is None:
            continue
        entries = [tuple(t) for t in entries][:k]
        if len({frozenset(t) for t in entries}) != len(entries):
            raise ValueError(f"{night['date']}: duplicate trios")
        r = score_entries(night, entries, fields=fields)
        r["entries"] = entries
        b = score_entries(night, [tuple(t) for t in baseline(view, hist)][:k], fields=fields)
        rows.append(r)
        base_rows.append(b)
        if verbose:
            print(night["date"], f"best {r['best']:.0f} (base {b['best']:.0f}) gap20 {r['gapPool20']:.0f} rank {r['rankPool20']}"
                  f" pileon {r['share_pileon']:.3f} (base {b['share_pileon']:.3f})")
    summary = {"label": label, "nights": len(rows)}
    for m in METRICS:
        a = [r[m] for r in rows]
        bb = [b[m] for b in base_rows]
        summary[m] = {"mean": float(np.mean(a)) if a else None, "baseline": float(np.mean(bb)) if bb else None,
                      "diff": paired_ci([x - y for x, y in zip(a, bb)])}
    for flag in ("top1", "top5", "top10", "top1pct"):
        summary[flag] = {"count": sum(r[flag] for r in rows), "baseline": sum(b[flag] for b in base_rows)}
    summary["prizes_total"] = {s: {"ours": sum(r[f"share_{s}"] for r in rows), "baseline": sum(b[f"share_{s}"] for b in base_rows)}
                               for s in ("v1", "pileon", "spread")}
    return {"summary": summary, "rows": rows, "baseline_rows": base_rows}


if __name__ == "__main__":
    rep = evaluate(line_sum_selector, label="line_sum", verbose=True)
    print(json.dumps(rep["summary"], indent=1, default=str)[:3000])
