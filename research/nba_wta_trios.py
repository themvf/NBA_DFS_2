"""Winner-take-all top-3 scorer contest: entries chosen to beat a field, not to score well.

Contest (as the owner plays it): each entry names three players; its score is the
sum of their points that night; the highest total wins the whole prize; tied
winners split it; one person may submit many entries; the field is 200+ entries.

Why a separate builder. The three biggest lines are the most likely trio, but in
a winner-take-all field they are also the most duplicated, and a trio only pays
when it beats every other entry. On the 16 graded nights to 2026-11-05 the three
biggest lines averaged 86 points against 112 for the night's best trio, were the
best of the 1,140 trios from the top-20 lines once, and ranked a median 52nd.

Method (``nba-wta-trios-v1``, frozen; see docs/nba-wta-trios.md):

1. Pool: the 20 biggest consensus lines from the latest pre-tip capture of each
   game on the date (``prop_odds_history``), read with the same ladder reader as
   the Top Scorers page and the nightly grade (``model/nba_points_ladder.py``).
2. Each player's points: the books' own exceedance curve (main line plus the
   alternate ladder), extended past the last rung with an exponential tail, drawn
   through a Gaussian copula with the measured same-game dependence (teammates
   -0.06, opponents +0.02), rounded to whole points. A player sits (scores 0) with
   the measured rate for his book count. A player with no usable ladder gets a
   normal with the measured spread for his line.
3. Field: ``FIELD_SIZE`` entries; ``CHALK_SHARE`` of them enter the exact
   three-biggest-lines trio and the rest pick three players with probability
   proportional to exp(line / POPULARITY_TAU). Stated priors: no field data exists.
4. Each simulated night pays an entry 1 / (number of entries tied at the top) if
   its total is the night's highest, else 0. Our entries compete with each other
   too. Entries are chosen greedily, each adding the most expected prize share to
   the ones already chosen, so the first k entries are the best k-entry set.
5. Baseline: the ``N_ENTRIES`` trios with the largest line sums, scored the same.

Everything is frozen before the slate's first tip into ``nba_wta_portfolios`` and
graded each morning into ``nba_wta_grades`` against actual points.

Usage:
    python -m research.nba_wta_trios freeze [--date YYYY-MM-DD] [--apply]
    python -m research.nba_wta_trios grade [--date YYYY-MM-DD] [--days-back 3] [--apply]
    python -m research.nba_wta_trios report
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import itertools
import json
import math
import random
from typing import Any

import numpy as np
from scipy.special import ndtr, ndtri

VERSION = "nba-wta-trios-v1"
GRADE_VERSION = "nba-wta-grade-v1"

# Frozen at registration (2026-11-06). Changing any of these is a new VERSION.
POOL_SIZE = 20
N_ENTRIES = 10
PRIMARY_N = 5
FIELD_SIZE = 300            # the owner's contest: "200+" entries
CHALK_SHARE = 0.10          # stated prior: share of the field on the exact three biggest lines
POPULARITY_TAU = 3.0        # stated prior: field picks proportional to exp(line / tau)
SIMS = 6000
SIM_CHUNK = 500             # a fresh field sample per chunk
TEAMMATE_RHO = -0.06        # measured: standardized (actual - line), graded Oct 20 - Nov 5, 5,061 pairs
OPPONENT_RHO = 0.02         # measured: same, 5,802 pairs
# Measured Oct 20-31: share of players who did not play, by how many books quoted the line.
DNP_BY_BOOKS = ((1, 0.35), (3, 0.03), (10**6, 0.002))
# Measured spread of (actual - line) by line, for the rare player with no usable ladder.
FALLBACK_SD_INTERCEPT = 3.9
FALLBACK_SD_SLOPE = 0.18
TAIL_FLOOR = 0.0005         # survival level where the extended tail stops
# nba-points-sim-recal-v1 (2026-11-06, opt-in; the frozen v1 entries never use it). The
# books' curves overstate each player's top end: over 1,688 played player-games (Oct 20 -
# Nov 5) line+10 was stated 10.5% and happened 6.3%, line+15 4.6% vs 1.9%. RECAL_G is the
# realized CDF of each player's PIT under his own curve at RECAL_KNOTS, shrunk toward the
# identity with 200 pseudo-rows; a draw's level u maps to G^-1(u) before the curve is
# inverted. Leave-one-night-out on 123 games it cut per-game leader log loss by 0.033
# (95% CI 0.012-0.053, nights resampled). docs/nba-game-leaders.md.
RECAL_VERSION = "nba-points-sim-recal-v1"
RECAL_KNOTS = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0)
RECAL_G = (0.0, 0.0964, 0.2309, 0.3347, 0.4317, 0.5254, 0.6276, 0.7368, 0.8379, 0.9306, 0.9762, 0.9974, 1.0)
# nba-points-sit-lead-v1 (2026-11-12, opt-in; the frozen v1 entries never use it). DNP_BY_BOOKS
# was measured from each game's LAST capture before its own tip. The owner's contest locks at
# the night's first tip, so the lines he builds from are hours ahead of the later games, and a
# player quoted then sits far more often. Every pre-tip capture Oct 20 - Nov 11 (10,720
# player-captures, 22 nights), by books at the line and hours from the capture to the player's
# own tip: research/nba_sit_lead.py prints the table these come from.
SIT_VERSION = "nba-points-sit-lead-v1"
SIT_LEAD_HOURS = (1.5, 6.0)   # under 1.5 h, 1.5 to 6 h, 6 h or more
# 1 book 49/171 (too few to split); 2-3 books 4/404, 5/209, 10/223; 4+ books 8/3,999, 76/3,603, 82/2,111.
SIT_BY_BOOKS_LEAD = ((1, (0.29, 0.29, 0.29)), (3, (0.010, 0.024, 0.045)), (10**6, (0.002, 0.021, 0.039)))
CONTENTION_TOP = 0.01       # grade: a best entry in the top 1% of the night's pool trios is "in contention"
REVIEW_AFTER_NIGHTS = 30


# ------------------------------------------------------------------ pure model


@dataclass
class PoolPlayer:
    key: str
    player: str
    event_id: str
    home_team: str
    away_team: str
    team: str | None            # one of the two event teams, or None when unknown
    line: float
    p_over: float | None
    books: int
    ladder_books: int
    curve: list[tuple[float, float]]   # (points, P(score above)), non-increasing; empty = no ladder
    floor: float | None
    ceiling: float | None
    p30: float | None
    p40: float | None

    @property
    def dnp_prob(self) -> float:
        for upper, p in DNP_BY_BOOKS:
            if self.books <= upper:
                return p
        return DNP_BY_BOOKS[-1][1]


def survival_knots(curve: list[tuple[float, float]]) -> tuple[np.ndarray, np.ndarray] | None:
    """(survival levels ascending, points) for inverse sampling; None when the curve is unusable.

    The books' curve is kept as is inside its range. Below the first rung the survival
    rises linearly to 0.995 at zero points; past the last rung it decays exponentially
    at the rate of the curve's last segment (bounded), until TAIL_FLOOR.
    """
    pts = [(float(x), float(p)) for x, p in curve if x is not None and p is not None and 0 < p < 1]
    pts.sort()
    if len(pts) < 2:
        return None
    xs, ss = [], []
    first_x, first_p = pts[0]
    if first_x > 0 and first_p < 0.995:
        xs.append(0.0)
        ss.append(0.995)
    last_s = 1.0
    for x, p in pts:
        p = min(p, last_s)
        if xs and x <= xs[-1]:
            continue
        xs.append(x)
        ss.append(p)
        last_s = p
    if len(xs) < 2:
        return None
    (x1, p1), (x2, p2) = (xs[-2], ss[-2]), (xs[-1], ss[-1])
    rate = math.log(p1 / p2) / (x2 - x1) if p1 > p2 > 0 and x2 > x1 else 0.0
    rate = min(1.0, max(0.05, rate))
    x, p = x2, p2
    while p > TAIL_FLOOR:
        x += 1.0
        p = p2 * math.exp(-rate * (x - x2))
        xs.append(x)
        ss.append(max(p, TAIL_FLOOR * 0.5))
    # strictly decreasing survival for interpolation
    out_s, out_x = [], []
    for x, s in zip(xs, ss):
        if out_s and s >= out_s[-1]:
            s = out_s[-1] - 1e-9
        out_s.append(s)
        out_x.append(x)
    s_arr = np.array(out_s[::-1])
    x_arr = np.array(out_x[::-1])
    return s_arr, x_arr


def correlation_matrix(pool: list[PoolPlayer]) -> np.ndarray:
    n = len(pool)
    c = np.eye(n)
    for i, j in itertools.combinations(range(n), 2):
        a, b = pool[i], pool[j]
        if a.event_id != b.event_id or a.team is None or b.team is None:
            continue
        rho = TEAMMATE_RHO if a.team == b.team else OPPONENT_RHO
        c[i, j] = c[j, i] = rho
    # The measured values are small; a non-PSD matrix would mean bad team tags. Repair by eigenvalue clip.
    w, v = np.linalg.eigh(c)
    if w.min() < 1e-9:
        w = np.clip(w, 1e-6, None)
        c = (v * w) @ v.T
        d = np.sqrt(np.diag(c))
        c = c / np.outer(d, d)
    return c


def recalibrate_survival(u: np.ndarray) -> np.ndarray:
    """Survival level -> recalibrated survival level (``RECAL_VERSION``): F-level u becomes G^-1(u)."""
    return 1.0 - np.interp(1.0 - np.asarray(u, dtype=float), RECAL_G, RECAL_KNOTS)


def simulate_points(pool: list[PoolPlayer], sims: int, rng: np.random.Generator, recal: bool = False,
                    sit: list[float] | None = None) -> np.ndarray:
    """(sims x players) whole points. A player who sits scores 0.

    ``recal`` thins each player's top end (``RECAL_VERSION``); the registered v1 entries use False.
    ``sit`` gives each player's sitting chance (``SIT_VERSION``); the registered v1 entries leave it
    out and use the book-count rate.
    """
    n = len(pool)
    z = rng.standard_normal((sims, n)) @ np.linalg.cholesky(correlation_matrix(pool)).T
    # survival level = 1 - Phi(z), so a high z is a high score
    u = ndtr(-z)
    if recal:
        u = recalibrate_survival(u)
        z = -ndtri(np.clip(u, 1e-12, 1 - 1e-12))   # the no-ladder fallback reads z
    out = np.zeros((sims, n))
    for i, p in enumerate(pool):
        knots = survival_knots(p.curve)
        if knots is None:
            sd = FALLBACK_SD_INTERCEPT + FALLBACK_SD_SLOPE * p.line
            out[:, i] = np.maximum(0.0, p.line + 0.5 + sd * z[:, i])
        else:
            s_arr, x_arr = knots
            out[:, i] = np.interp(np.clip(u[:, i], s_arr[0], s_arr[-1]), s_arr, x_arr)
        sat = rng.random(sims) < (p.dnp_prob if sit is None else sit[i])
        out[sat, i] = 0.0
    return np.rint(np.maximum(out, 0.0))


def candidate_trios(n: int) -> list[tuple[int, int, int]]:
    return list(itertools.combinations(range(n), 3))


def field_max(points: np.ndarray, pool: list[Any], rng: np.random.Generator, field_size: int = FIELD_SIZE,
              chalk_share: float = CHALK_SHARE, tau: float = POPULARITY_TAU) -> tuple[np.ndarray, np.ndarray]:
    """Per simulated night: the field's best total and how many field entries share it."""
    sims, n = points.shape
    lines = np.array([p.line for p in pool])
    chalk = np.argsort(-lines, kind="stable")[:3]
    n_chalk = int(round(field_size * chalk_share))
    logw = lines / tau
    best = np.zeros(sims)
    count = np.zeros(sims)
    for start in range(0, sims, SIM_CHUNK):
        stop = min(sims, start + SIM_CHUNK)
        # Gumbel-top-3: three distinct picks proportional to the weights
        g = logw + rng.gumbel(size=(field_size - n_chalk, n))
        picks = np.argpartition(-g, 3, axis=1)[:, :3]
        picks = np.vstack([np.tile(chalk, (n_chalk, 1)), picks])
        totals = points[start:stop][:, picks].sum(axis=2)          # (chunk x field)
        m = totals.max(axis=1)
        best[start:stop] = m
        count[start:stop] = (totals == m[:, None]).sum(axis=1)
    return best, count


def trio_totals(points: np.ndarray, trios: list[tuple[int, int, int]]) -> np.ndarray:
    idx = np.array(trios)
    return points[:, idx].sum(axis=2)   # (sims x trios)


def build_portfolio(totals: np.ndarray, fmax: np.ndarray, fcount: np.ndarray, n_entries: int,
                    order: list[int] | None = None) -> list[dict[str, Any]]:
    """Greedy: each pick adds the most expected prize share given the entries already chosen.

    With ``order`` the picks are fixed (used to score the baseline the same way).
    Returns, per pick, its trio index, the marginal share it added, the portfolio's
    cumulative share, and its standalone share against the field alone.
    """
    sims = totals.shape[0]
    top = fmax.copy()
    cf = fcount.copy()
    cp = np.zeros(sims)
    current = 0.0
    chosen: list[int] = []
    out = []
    standalone_all = np.where(totals > fmax[:, None], 1.0,
                              np.where(totals == fmax[:, None], 1.0 / (fcount[:, None] + 1.0), 0.0)).mean(axis=0)
    for step in range(n_entries):
        old = np.where(cp > 0, cp / (cf + cp), 0.0)
        if order is not None:
            if step >= len(order):
                break
            k = order[step]
            t = totals[:, k]
            new = np.where(t > top, 1.0, np.where(t == top, (cp + 1.0) / (cf + cp + 1.0), old))
            gain = float((new - old).mean())
        else:
            new = np.where(totals > top[:, None], 1.0,
                           np.where(totals == top[:, None], (cp[:, None] + 1.0) / (cf[:, None] + cp[:, None] + 1.0),
                                    old[:, None]))
            gains = (new - old[:, None]).mean(axis=0)
            if chosen:
                gains[chosen] = -np.inf
            k = int(np.argmax(gains))
            gain = float(gains[k])
            t = totals[:, k]
        chosen.append(k)
        higher = t > top
        tie = t == top
        cf = np.where(higher, 0.0, cf)
        cp = np.where(higher, 1.0, np.where(tie, cp + 1.0, cp))
        top = np.where(higher, t, top)
        current += gain
        out.append({"trio": k, "marginalShare": gain, "portfolioShare": current, "standaloneShare": float(standalone_all[k])})
    return out


def baseline_order(pool: list[PoolPlayer], trios: list[tuple[int, int, int]], n: int) -> list[int]:
    """The n trios with the largest line sums (ties: higher P(over) sum, then player keys)."""
    def key(k: int):
        t = trios[k]
        return (-sum(pool[i].line for i in t), -sum((pool[i].p_over or 0.5) for i in t), tuple(pool[i].key for i in t))
    return sorted(range(len(trios)), key=key)[:n]


def seed_for(game_date: date, pool: list[PoolPlayer]) -> int:
    h = hashlib.sha256((game_date.isoformat() + "|" + "|".join(f"{p.key}:{p.line}" for p in pool)).encode()).hexdigest()
    return int(h[:8], 16)


def select(game_date: date, pool: list[PoolPlayer], *, field_size: int = FIELD_SIZE, sims: int = SIMS) -> dict[str, Any]:
    """Deterministic for a given pool: the frozen entries, the baseline and the assumptions behind them."""
    if len(pool) < 3:
        raise ValueError("fewer than three players in the pool")
    seed = seed_for(game_date, pool)
    rng = np.random.default_rng(seed)
    points = simulate_points(pool, sims, rng)
    trios = candidate_trios(len(pool))
    totals = trio_totals(points, trios)
    fmax, fcount = field_max(points, pool, rng, field_size)
    picks = build_portfolio(totals, fmax, fcount, min(N_ENTRIES, len(trios)))
    base = build_portfolio(totals, fmax, fcount, min(N_ENTRIES, len(trios)),
                           order=baseline_order(pool, trios, min(N_ENTRIES, len(trios))))

    def entry(rank: int, p: dict[str, Any]) -> dict[str, Any]:
        t = trios[p["trio"]]
        return {"rank": rank, "players": [pool[i].key for i in t], "names": [pool[i].player for i in t],
                "lineSum": round(sum(pool[i].line for i in t), 1),
                "simMean": round(float(totals[:, p["trio"]].mean()), 2),
                "simP90": float(np.percentile(totals[:, p["trio"]], 90)),
                "marginalShare": round(p["marginalShare"], 6), "portfolioShare": round(p["portfolioShare"], 6),
                "standaloneShare": round(p["standaloneShare"], 6)}

    ownership = np.zeros(len(pool))
    # analytic-ish field ownership for display: chalk share plus the Gumbel-top-3 marginal, estimated by sampling
    g = np.array([p.line for p in pool]) / POPULARITY_TAU + rng.gumbel(size=(20000, len(pool)))
    top3 = np.argpartition(-g, 3, axis=1)[:, :3]
    for i in range(len(pool)):
        ownership[i] = (top3 == i).any(axis=1).mean() * (1 - CHALK_SHARE)
    for i in np.argsort(-np.array([p.line for p in pool]), kind="stable")[:3]:
        ownership[i] += CHALK_SHARE
    win_line = float(np.percentile(fmax, 50))
    return {
        "version": VERSION, "gameDate": game_date.isoformat(), "seed": seed, "sims": sims,
        "assumptions": {"fieldSize": field_size, "chalkShare": CHALK_SHARE, "popularityTau": POPULARITY_TAU,
                        "teammateRho": TEAMMATE_RHO, "opponentRho": OPPONENT_RHO,
                        "dnpByBooks": [list(x) for x in DNP_BY_BOOKS], "poolSize": POOL_SIZE},
        "simulatedWinningScore": {"p25": float(np.percentile(fmax, 25)), "median": win_line,
                                  "p75": float(np.percentile(fmax, 75))},
        "pool": [{"key": p.key, "player": p.player, "eventId": p.event_id, "team": p.team, "homeTeam": p.home_team,
                  "awayTeam": p.away_team, "line": p.line, "pOver": p.p_over, "books": p.books,
                  "ladderBooks": p.ladder_books, "floor": p.floor, "ceiling": p.ceiling, "p30": p.p30, "p40": p.p40,
                  "dnpProb": p.dnp_prob, "fieldOwnership": round(float(ownership[i]), 4),
                  "simMean": round(float(points[:, i].mean()), 2), "simP90": float(np.percentile(points[:, i], 90))}
                 for i, p in enumerate(pool)],
        "entries": [entry(r + 1, p) for r, p in enumerate(picks)],
        "baseline": [entry(r + 1, p) for r, p in enumerate(base)],
    }


# ------------------------------------------------------------------ grading (pure)


GRADE_FIELDS = 2000        # simulated fields per graded night for the realized prize share


def realized_share(payload: dict[str, Any], pts: dict[str, float], entries: list[dict[str, Any]], n: int) -> float:
    """Prize share the first n entries would have won with the actual points, against fields drawn
    from the frozen field model (seeded by the freeze, so a re-grade gives the same number)."""
    from types import SimpleNamespace
    keys = [p["key"] for p in payload["pool"]]
    pool = [SimpleNamespace(line=float(p["line"])) for p in payload["pool"]]
    a = payload.get("assumptions", {})
    rng = np.random.default_rng(int(payload.get("seed", 0)) + 1)
    fixed = np.array([[pts[k] for k in keys]], dtype=float).repeat(GRADE_FIELDS, axis=0)
    fmax, fcount = field_max(fixed, pool, rng, int(a.get("fieldSize", FIELD_SIZE)),
                             float(a.get("chalkShare", CHALK_SHARE)), float(a.get("popularityTau", POPULARITY_TAU)))
    tot = np.array([[sum(pts[k] for k in e["players"]) for e in entries[:n]]], dtype=float).repeat(GRADE_FIELDS, axis=0)
    return float(build_portfolio(tot, fmax, fcount, n, order=list(range(n)))[-1]["portfolioShare"])


def grade_portfolio(payload: dict[str, Any], actual: dict[str, float | None]) -> dict[str, Any] | None:
    """Score a frozen portfolio against actual points (a player who sat scores 0).

    ``actual`` maps every pool key to points, 0 for a player who did not play, or None
    when his game has no final box score yet; any None leaves the night ungraded.
    The yardstick is every trio from the frozen pool, so the field's real picks are
    not needed: a best entry ranked near the top of all pool trios is a contender.
    """
    keys = [p["key"] for p in payload["pool"]]
    if any(actual.get(k) is None for k in keys):
        return None
    pts = {k: float(actual[k]) for k in keys}
    all_totals = sorted((sum(pts[k] for k in t) for t in itertools.combinations(keys, 3)), reverse=True)
    n_trios = len(all_totals)
    contention_at = all_totals[max(0, math.ceil(n_trios * CONTENTION_TOP) - 1)]

    def pct(total: float) -> float:
        below = sum(1 for v in all_totals if v < total)
        equal = sum(1 for v in all_totals if v == total)
        return (below + 0.5 * equal) / n_trios

    out: dict[str, Any] = {"bestPoolTrio": all_totals[0], "contentionAt": contention_at, "poolTrios": n_trios, "byN": {}}
    for label, entries in (("ours", payload["entries"]), ("baseline", payload["baseline"])):
        out[label] = [sum(pts[k] for k in e["players"]) for e in entries]
    for n in (PRIMARY_N, N_ENTRIES):
        cell = {}
        for label in ("ours", "baseline"):
            best = max(out[label][:n])
            cell[label] = {"best": best, "percentile": round(pct(best), 6), "inContention": best >= contention_at,
                           "gapToBest": all_totals[0] - best,
                           "realizedShare": round(realized_share(payload, pts, payload[label if label == "baseline" else "entries"], n), 6)}
        out["byN"][str(n)] = cell
    return out


def _paired(diffs: list[float], draws: int, seed: int) -> list[float]:
    rng = random.Random(seed)
    boots = sorted(sum(rng.choice(diffs) for _ in diffs) / len(diffs) for _ in range(draws))
    return [boots[int(0.025 * draws)], boots[int(0.975 * draws) - 1]]


def summarize(grades: list[dict[str, Any]], n: int = PRIMARY_N, draws: int = 5000) -> dict[str, Any]:
    """Paired comparison over graded nights, each with a night-resampled 95% interval.

    Primary: realized prize share (actual points, frozen field model). Guard: the field-free
    percentile of the best entry among all pool trios."""
    cells = [g["byN"][str(n)] for g in grades if str(n) in g.get("byN", {})]
    if not cells:
        return {"nights": 0}
    diffs = [c["ours"]["percentile"] - c["baseline"]["percentile"] for c in cells]
    shares = [c for c in cells if "realizedShare" in c["ours"]]
    share_diffs = [c["ours"]["realizedShare"] - c["baseline"]["realizedShare"] for c in shares]
    return {
        "nights": len(cells),
        "oursShare": sum(c["ours"]["realizedShare"] for c in shares),
        "baselineShare": sum(c["baseline"]["realizedShare"] for c in shares),
        "shareMeanDiff": sum(share_diffs) / len(share_diffs) if share_diffs else None,
        "shareCi95": _paired(share_diffs, draws, 20261107) if share_diffs else None,
        "oursMeanPercentile": sum(c["ours"]["percentile"] for c in cells) / len(cells),
        "baselineMeanPercentile": sum(c["baseline"]["percentile"] for c in cells) / len(cells),
        "meanDiff": sum(diffs) / len(diffs),
        "ci95": _paired(diffs, draws, 20261106),
        "oursContention": sum(c["ours"]["inContention"] for c in cells),
        "baselineContention": sum(c["baseline"]["inContention"] for c in cells),
        "oursBetter": sum(d > 0 for d in diffs), "baselineBetter": sum(d < 0 for d in diffs),
        "reviewAfter": REVIEW_AFTER_NIGHTS,
    }


# ------------------------------------------------------------------ fixture for the web port

FIXTURE_PATH = "tests/fixtures/nba_wta_fixture.json"


def fixture_payload() -> dict[str, Any]:
    """The deterministic pieces the browser optimizer (web/src/lib/nba-contest-optimizer.ts) must match.

    The simulation itself is not pinned: numpy's and the browser's random streams differ, so the
    web port is checked on these pieces exactly and on the simulation statistically.
    """
    curves = [
        [(10.5, 0.8), (19.5, 0.5), (29.5, 0.12)],
        [(4.5, 0.93), (9.5, 0.62), (14.5, 0.31), (19.5, 0.11), (24.5, 0.04)],
        [(0.0, 0.99), (25.5, 0.55), (30.5, 0.3), (30.5, 0.3), (40.5, 0.07)],
        [(22.5, 0.6), (22.5, 0.5)],
        [(20.5, 0.5)],
    ]
    knots = []
    for c in curves:
        k = survival_knots(c)
        knots.append({"curve": [list(x) for x in c],
                      "s": None if k is None else [float(v) for v in k[0]],
                      "x": None if k is None else [float(v) for v in k[1]]})
    spec = [("e1", "A"), ("e1", "A"), ("e1", "B"), ("e2", "A"), ("e1", None), ("e2", "B")]
    pool = [PoolPlayer(key=f"p{i}", player=f"P{i}", event_id=e, home_team="A", away_team="B", team=t,
                       line=30.0 - 2 * i, p_over=0.5 - 0.01 * i, books=6, ladder_books=4, curve=[],
                       floor=None, ceiling=None, p30=None, p40=None) for i, (e, t) in enumerate(spec)]
    rng = np.random.default_rng(11)
    totals = rng.integers(40, 90, size=(60, 12)).astype(float)
    fmax = rng.integers(60, 85, size=60).astype(float)
    fcount = rng.integers(1, 4, size=60).astype(float)
    trios = candidate_trios(len(pool))
    return {
        "version": VERSION,
        "constants": {"POOL_SIZE": POOL_SIZE, "N_ENTRIES": N_ENTRIES, "FIELD_SIZE": FIELD_SIZE, "CHALK_SHARE": CHALK_SHARE,
                      "POPULARITY_TAU": POPULARITY_TAU, "SIMS": SIMS, "SIM_CHUNK": SIM_CHUNK, "TEAMMATE_RHO": TEAMMATE_RHO,
                      "OPPONENT_RHO": OPPONENT_RHO, "DNP_BY_BOOKS": [list(x) for x in DNP_BY_BOOKS],
                      "FALLBACK_SD_INTERCEPT": FALLBACK_SD_INTERCEPT, "FALLBACK_SD_SLOPE": FALLBACK_SD_SLOPE,
                      "TAIL_FLOOR": TAIL_FLOOR, "RECAL_KNOTS": list(RECAL_KNOTS), "RECAL_G": list(RECAL_G)},
        "recal": {"u": [0.0, 0.001, 0.01, 0.05, 0.2, 0.5, 0.77, 0.999, 1.0],
                  "out": [float(v) for v in recalibrate_survival(np.array([0.0, 0.001, 0.01, 0.05, 0.2, 0.5, 0.77, 0.999, 1.0]))]},
        "knots": knots,
        "dnp": {str(b): _dnp(b) for b in (1, 2, 3, 4, 6)},
        "sit": {"version": SIT_VERSION, "leadHours": list(SIT_LEAD_HOURS),
                "byBooksLead": [[upper, list(rates)] for upper, rates in SIT_BY_BOOKS_LEAD],
                "cases": [[b, h, sit_prob(b, h)] for b in (1, 2, 3, 4, 6) for h in (None, 0.0, 1.49, 1.5, 3.0, 5.99, 6.0, 11.0)]},
        "correlation": {"spec": [list(x) for x in spec], "matrix": correlation_matrix(pool).tolist()},
        "trios": [list(t) for t in trios],
        "baselineOrder": {"lines": [p.line for p in pool], "pOver": [p.p_over for p in pool],
                          "keys": [p.key for p in pool], "order": baseline_order(pool, trios, 5)},
        "portfolio": {"totals": totals.tolist(), "fmax": fmax.tolist(), "fcount": fcount.tolist(),
                      "greedy": build_portfolio(totals, fmax, fcount, 5),
                      "ordered": build_portfolio(totals, fmax, fcount, 4, order=[3, 0, 7, 1])},
    }


def _dnp(books: int) -> float:
    for upper, p in DNP_BY_BOOKS:
        if books <= upper:
            return p
    return DNP_BY_BOOKS[-1][1]


def sit_prob(books: int, lead_hours: float | None) -> float:
    """Chance a quoted player does not play (``SIT_VERSION``): by books at his line and the hours from
    the capture to his own tip. With no lead time it is the registered ``DNP_BY_BOOKS`` rate."""
    if lead_hours is None or not math.isfinite(lead_hours):
        return _dnp(books)
    band = sum(1 for upper in SIT_LEAD_HOURS if lead_hours >= upper)
    for upper, rates in SIT_BY_BOOKS_LEAD:
        if books <= upper:
            return rates[band]
    return SIT_BY_BOOKS_LEAD[-1][1][band]


# ------------------------------------------------------------------ database


def _now() -> datetime:
    return datetime.now(timezone.utc)


def load_pool(db, target: date, now: datetime) -> tuple[list[PoolPlayer], dict[str, Any]]:
    """The 20 biggest lines from each game's latest capture before ``now`` (all games still to tip)."""
    from db.database import fetch_rows
    from model.nba_points_ladder import name_key, player_key, read_player, team_key
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """WITH latest AS (
                   SELECT event_id, market, max(captured_at) AS captured_at FROM prop_odds_history
                   WHERE sport = 'nba' AND game_date = %s AND market IN ('player_points', 'player_points_alternate')
                     AND captured_at <= %s AND commence_time IS NOT NULL
                   GROUP BY event_id, market)
               SELECT p.event_id, p.market, p.commence_time, p.captured_at, p.home_team_name, p.away_team_name,
                      p.player, p.books
               FROM prop_odds_history p JOIN latest l
                 ON l.event_id = p.event_id AND l.market = p.market AND l.captured_at = p.captured_at
               WHERE p.sport = 'nba'""",
            (target, now),
        )
        rows = fetch_rows(cur)
        cur.execute(
            """SELECT DISTINCT ON (player_key) player, team FROM nba_box_score_players
               WHERE game_date < %s AND game_date >= %s AND team IS NOT NULL
               ORDER BY player_key, game_date DESC, id DESC""",
            (target, date(target.year if target.month >= 8 else target.year - 1, 8, 1)),
        )
        teams = {player_key(r["player"]): str(r["team"]) for r in fetch_rows(cur)}
    commences = [r["commence_time"] for r in rows]
    meta = {"games": len({r["event_id"] for r in rows}), "firstTip": min(commences) if commences else None,
            "capturedAt": max((r["captured_at"] for r in rows), default=None)}
    ladders = {(str(r["event_id"]), name_key(r["player"])): (r["books"] if isinstance(r["books"], dict) else json.loads(r["books"]))
               for r in rows if r["market"] == "player_points_alternate"}
    readings: dict[str, PoolPlayer] = {}
    duplicates: set[str] = set()
    for r in rows:
        if r["market"] != "player_points":
            continue
        books = r["books"] if isinstance(r["books"], dict) else json.loads(r["books"])
        reading = read_player(books, ladders.get((str(r["event_id"]), name_key(r["player"]))))
        if reading is None:
            continue
        key = player_key(r["player"])
        if key in readings:
            duplicates.add(key)
            continue
        d = reading.distribution
        home, away = str(r["home_team_name"] or ""), str(r["away_team_name"] or "")
        team = teams.get(key)
        side = team if team and team_key(team) in (team_key(home), team_key(away)) else None
        readings[key] = PoolPlayer(
            key=key, player=str(r["player"]), event_id=str(r["event_id"]), home_team=home, away_team=away,
            team=(home if side and team_key(side) == team_key(home) else away if side else None),
            line=reading.line, p_over=reading.p_over, books=len(reading.lines),
            ladder_books=d.ladder_books if d else 0,
            curve=[(c.point, c.p) for c in d.curve] if d else [],
            floor=d.floor if d else None, ceiling=d.ceiling if d else None,
            p30=d.at_least.get(30) if d else None, p40=d.at_least.get(40) if d else None)
    for k in duplicates:          # one name in two games: place neither
        readings.pop(k, None)
    pool = sorted(readings.values(), key=lambda p: (-p.line, -(p.p_over or 0.5), p.key))[:POOL_SIZE]
    meta["duplicatesDropped"] = sorted(duplicates)
    meta["quoted"] = len(readings)
    return pool, meta


def freeze(db, target: date, *, apply: bool, now: datetime | None = None) -> dict[str, Any]:
    from db.database import fetch_rows
    now = now or _now()
    pool, meta = load_pool(db, target, now)
    if meta["firstTip"] is None:
        return {"date": target.isoformat(), "status": "no_lines"}
    if meta["firstTip"] <= now:
        return {"date": target.isoformat(), "status": "after_first_tip", "firstTip": meta["firstTip"].isoformat()}
    if len(pool) < 3:
        return {"date": target.isoformat(), "status": "pool_too_small", "pool": len(pool)}
    payload = select(target, pool)
    payload["inputs"] = {"capturedAt": meta["capturedAt"].isoformat(), "firstTip": meta["firstTip"].isoformat(),
                         "games": meta["games"], "quotedPlayers": meta["quoted"],
                         "duplicatesDropped": meta["duplicatesDropped"]}
    text = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha256(text.encode()).hexdigest()
    result = {"date": target.isoformat(), "status": "frozen" if apply else "dry_run", "sha256": digest,
              "firstTip": meta["firstTip"].isoformat(), "capturedAt": meta["capturedAt"].isoformat(),
              "pool": len(pool), "entries": [" + ".join(e["names"]) for e in payload["entries"][:PRIMARY_N]],
              "portfolioShare5": payload["entries"][PRIMARY_N - 1]["portfolioShare"] if len(payload["entries"]) >= PRIMARY_N else None,
              "baselineShare5": payload["baseline"][PRIMARY_N - 1]["portfolioShare"] if len(payload["baseline"]) >= PRIMARY_N else None}
    if apply:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO nba_wta_portfolios (version, game_date, first_tip, inputs_captured_at, payload, payload_sha256)
                   VALUES (%s, %s, %s, %s, %s::jsonb, %s)
                   ON CONFLICT (version, game_date, payload_sha256) DO NOTHING RETURNING id""",
                (VERSION, target, meta["firstTip"], meta["capturedAt"], text, digest),
            )
            inserted = fetch_rows(cur)
            conn.commit()
        result["id"] = inserted[0]["id"] if inserted else None
        result["status"] = "frozen" if inserted else "unchanged"
    return result


def load_actuals(db, payload: dict[str, Any], target: date) -> dict[str, float | None]:
    """Actual points per pool key from the ESPN box scores; 0 when his final box has no row for him."""
    from model.nba_points_calibration import load_box_scores, match_game
    from model.nba_points_ladder import player_key
    games = load_box_scores(db, target)
    out: dict[str, float | None] = {}
    for p in payload["pool"]:
        g = match_game(p["homeTeam"], p["awayTeam"], games)
        if g is None:
            out[p["key"]] = None
            continue
        hits = [b for b in g.players if player_key(b.player) == p["key"]]
        if len(hits) > 1:
            out[p["key"]] = None
        elif not hits or not hits[0].played:
            out[p["key"]] = 0.0
        else:
            out[p["key"]] = float(hits[0].points or 0.0)
    return out


def grade(db, target: date, *, apply: bool) -> dict[str, Any]:
    from db.database import fetch_rows
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, payload FROM nba_wta_portfolios WHERE version = %s AND game_date = %s
               ORDER BY created_at DESC, id DESC LIMIT 1""", (VERSION, target))
        rows = fetch_rows(cur)
    if not rows:
        return {"date": target.isoformat(), "status": "no_portfolio"}
    pid = rows[0]["id"]
    payload = rows[0]["payload"] if isinstance(rows[0]["payload"], dict) else json.loads(rows[0]["payload"])
    actual = load_actuals(db, payload, target)
    result = grade_portfolio(payload, actual)
    if result is None:
        return {"date": target.isoformat(), "status": "pending", "portfolioId": pid,
                "missing": sorted(k for k, v in actual.items() if v is None)}
    result["actual"] = actual
    if apply:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO nba_wta_grades (portfolio_id, version, grade_version, game_date, payload)
                   VALUES (%s, %s, %s, %s, %s::jsonb)
                   ON CONFLICT (portfolio_id, grade_version)
                   DO UPDATE SET payload = EXCLUDED.payload, graded_at = now()""",
                (pid, VERSION, GRADE_VERSION, target, json.dumps(result, sort_keys=True, default=str)),
            )
            conn.commit()
    c = result["byN"][str(PRIMARY_N)]
    return {"date": target.isoformat(), "status": "graded" if apply else "dry_run", "portfolioId": pid,
            "bestPoolTrio": result["bestPoolTrio"], "contentionAt": result["contentionAt"],
            "ours5": c["ours"], "baseline5": c["baseline"]}


def all_grades(db) -> list[dict[str, Any]]:
    from db.database import fetch_rows
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT g.game_date, g.payload FROM nba_wta_grades g
                       WHERE g.version = %s AND g.grade_version = %s ORDER BY g.game_date""", (VERSION, GRADE_VERSION))
        rows = fetch_rows(cur)
    return [r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"]) for r in rows]


def main() -> int:
    from config import load_config
    from db.database import DatabaseManager
    from ingest.nba_box_scores import eastern_date
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["freeze", "grade", "report", "fixture"])
    parser.add_argument("--date", help="Eastern date (YYYY-MM-DD); default today ET for freeze, yesterday for grade")
    parser.add_argument("--days-back", type=int, default=1, help="grade: also grade this many earlier dates")
    parser.add_argument("--apply", action="store_true", help="write rows; without it, print only")
    args = parser.parse_args()
    if args.command == "fixture":
        with open(FIXTURE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(fixture_payload(), f, indent=1)
            f.write("\n")
        print(f"wrote {FIXTURE_PATH}")
        return 0
    db = DatabaseManager(load_config().database_url)
    db.require_tables(["prop_odds_history", "nba_box_score_players", "nba_wta_portfolios", "nba_wta_grades"])
    today = eastern_date(_now())
    if args.command == "freeze":
        target = date.fromisoformat(args.date) if args.date else today
        print(json.dumps(freeze(db, target, apply=args.apply), indent=2, default=str))
        return 0
    if args.command == "grade":
        last = date.fromisoformat(args.date) if args.date else today - timedelta(days=1)
        out = [grade(db, last - timedelta(days=b), apply=args.apply) for b in range(max(1, args.days_back) - 1, -1, -1)]
        print(json.dumps(out, indent=2, default=str))
        return 0
    grades = all_grades(db)
    print(json.dumps({"version": VERSION, "primaryN": PRIMARY_N, "summary": summarize(grades),
                      "summary10": summarize(grades, N_ENTRIES)}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
