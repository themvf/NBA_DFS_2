"""Market-anchored game leaders, `nfl-leaders-market-anchored-v1` (docs/nfl-leaders-anchored-test.md).

A third number beside our own leader share and the DK-implied share. It does
not change the game-leaders model: it takes that model's simulated games and
reshapes each listed player's distribution so it matches DraftKings' main line
and every rung of the alternate ladder, then re-runs who leads.

* Anchors per player and market: the main line's de-vigged P(over), and each
  ladder rung's P(over) with the main line's margin removed (ladders are
  one-sided). Rungs outside 3%-97% are dropped; P(over) is forced to fall as
  the line rises.
* The reshaping is a quantile map: a draw at our own cumulative probability u
  goes to DraftKings' value at u, interpolated between anchors; below the
  lowest and above the highest anchor our draws keep their own spacing. The map
  is monotone per player (tied draws are spread across their block with a fixed
  seed), so the joint pattern of the simulation, teammates competing for one
  ball, is kept. Below the lowest anchor draws are scaled toward zero, so a
  simulated zero stays zero unless DraftKings' lowest rung says he plays more.
* Receptions are rounded back to whole catches. Total yards is rushing plus
  receiving after both are reshaped, then reshaped again to DraftKings'
  rush+receiving line when one exists.
* Players without a DraftKings line keep our draws. No temperature is applied.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import numpy as np

VERSION = "nfl-leaders-market-anchored-v1"
# metric -> (main market key, alternate market key, draw field(s))
MARKETS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "rushing_yards": ("player_rush_yds", "player_rush_yds_alternate", ("rushYds",)),
    "receiving_yards": ("player_reception_yds", "player_reception_yds_alternate", ("recYds",)),
    "receptions": ("player_receptions", "player_receptions_alternate", ("receptions",)),
    "total_yards": ("player_rush_reception_yds", "player_rush_reception_yds_alternate", ("rushYds", "recYds")),
}
INTEGER_METRICS = {"receptions"}
P_MIN, P_MAX = 0.03, 0.97
TIE_SEED = 20261017


def american_to_prob(price: float) -> float:
    return 100 / (price + 100) if price > 0 else -price / (-price + 100)


def anchors(main: Mapping[str, Any] | None, alternate: Mapping[str, Any] | None) -> list[tuple[float, float]]:
    """(line, P(over)) pairs from one book's main line and ladder, sorted by line.
    Empty when there is no paired main line (the ladder cannot be de-vigged)."""
    if not main or any(main.get(k) is None for k in ("line", "over", "under")):
        return []
    raw_o, raw_u = american_to_prob(main["over"]), american_to_prob(main["under"])
    margin = raw_o + raw_u
    points = {float(main["line"]): raw_o / margin}
    for rung in (alternate or {}).get("ladder") or []:
        if rung.get("side") != "over" or rung.get("point") is None or rung.get("price") is None:
            continue
        x = float(rung["point"])
        if x in points:
            continue
        p = american_to_prob(rung["price"]) / margin
        if P_MIN <= p <= P_MAX:
            points[x] = p
    out, running = [], 1.0
    for x in sorted(points):
        running = min(running, points[x])
        out.append((x, running))
    return out


def quantile_map(draws: np.ndarray, points: Sequence[tuple[float, float]], integer: bool = False) -> np.ndarray:
    """Map draws so the share above each anchor line equals its P(over)."""
    d = np.asarray(draws, dtype=float)
    if not points or d.size == 0:
        return d.copy()
    n = d.size
    # Each draw's own cumulative probability. Ties are spread evenly across their
    # block (fixed seed), otherwise a block of equal counts, say 30% of games at
    # three catches, maps to one value and the anchor cannot be hit.
    tie_break = np.random.default_rng(TIE_SEED).random(n)
    order = np.lexsort((tie_break, d))
    u = np.empty(n)
    u[order] = (np.arange(n) + 0.5) / n
    xs = np.array([x for x, _ in points])
    fs = np.array([1 - p for _, p in points])          # target CDF at each line
    # Strictly increasing CDF knots for interpolation.
    fs = np.maximum.accumulate(fs + np.arange(len(fs)) * 1e-9)
    qs = np.quantile(d, fs)                              # our own value at those CDF levels
    t = np.interp(u, fs, xs)
    low, high = u < fs[0], u > fs[-1]
    # Below the lowest anchor, scale toward zero instead of shifting, so a game
    # our simulation scores at zero (or below, for rushing) stays there.
    scale = xs[0] / qs[0] if qs[0] > 0 else 1.0
    t[low] = np.where(d[low] > 0, d[low] * scale, d[low])
    # Above the highest anchor every draw must clear that line, even one tied at
    # our own quantile there.
    t[high] = xs[-1] + np.maximum(d[high] - qs[-1], 0) + 1e-6
    if integer:
        t = np.maximum(np.round(t), 0)
    return t


def anchor_players(players: Sequence[Mapping[str, Any]], quotes: Mapping[tuple[str, str], list[tuple[float, float]]],
                   name_key) -> tuple[dict[str, list[np.ndarray]], dict[str, dict[str, Any]]]:
    """Reshaped draws per metric (same order as `players`) and per-player diagnostics.
    `quotes[(metric, normalized_name)]` holds that player's anchors."""
    arrays: dict[str, list[np.ndarray]] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    mapped_parts: dict[str, list[np.ndarray]] = {}
    for metric in ("rushing_yards", "receiving_yards", "receptions"):
        field = MARKETS[metric][2][0]
        out = []
        for p in players:
            d = np.asarray(p["draws"][field], dtype=float)
            pts = [] if p.get("residual") else quotes.get((metric, name_key(p["name"])), [])
            out.append(quantile_map(d, pts, metric in INTEGER_METRICS) if pts else d.copy())
            if pts:
                diagnostics.setdefault(p["identity"], {})[metric] = {"anchors": [[x, round(q, 4)] for x, q in pts],
                                                                     "mean_before": round(float(d.mean()), 2),
                                                                     "mean_after": round(float(out[-1].mean()), 2)}
        arrays[metric] = out
        mapped_parts[field] = out
    totals = []
    for j, p in enumerate(players):
        total = mapped_parts["rushYds"][j] + mapped_parts["recYds"][j]
        pts = [] if p.get("residual") else quotes.get(("total_yards", name_key(p["name"])), [])
        if pts:
            before = float(total.mean())
            total = quantile_map(total, pts)
            diagnostics.setdefault(p["identity"], {})["total_yards"] = {"anchors": [[x, round(q, 4)] for x, q in pts],
                                                                        "mean_before": round(before, 2),
                                                                        "mean_after": round(float(total.mean()), 2)}
        totals.append(total)
    arrays["total_yards"] = totals
    return arrays, diagnostics


def leader_shares(arrays: Sequence[np.ndarray]) -> np.ndarray:
    """Win share per player: ties split equally."""
    matrix = np.vstack(arrays)
    best = matrix.max(axis=0)
    tied = matrix == best
    return (tied / tied.sum(axis=0)).mean(axis=1)


def grouped_shares(players: Sequence[Mapping[str, Any]], shares: Iterable[float]) -> dict[str, float]:
    """Residual slots grouped as OTHER:<team>, the same identities the published page uses."""
    out: dict[str, float] = {}
    for p, s in zip(players, shares):
        key = f"OTHER:{p['team']}" if p.get("residual") else p["identity"]
        out[key] = out.get(key, 0.0) + float(s)
    return out


def leader_log_loss(shares: Mapping[str, float], winners: Mapping[str, float]) -> float:
    """Log loss of the actual leader; `winners` maps identity to its tie-split credit."""
    return -sum(credit * np.log(max(shares.get(i, 0.0), 1e-4)) for i, credit in winners.items())
