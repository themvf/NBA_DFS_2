"""Walk-forward backtest for kicker projections.

Unlike DST (see model/ff_dst_projection_backtest.py, where history LOST to a
flat constant), kicker history carries real signal -- both history methods beat
a flat constant by a wide margin. Two findings drive the shipped model:

1. The 3-YEAR WEIGHTED BLEND BEATS PRIOR-SEASON-ONLY (held-out MAE 22.1 vs
   23.1, pooled Spearman 0.18 vs 0.17). So kickers keep the standard multi-year
   regression rather than the prior-season carry-forward used for DST.
2. The default shrinkage was too weak. Three full seasons against
   REGRESSION_PRIOR_GAMES=4 leaves an effective carry-forward weight of 0.93,
   but the tuning period fits 0.58. POSITION_REGRESSION_PRIOR_GAMES["K"]=37
   reproduces 0.58 at a 51-game sample; held out on 2025 that improves MAE
   23.2 -> 22.1.

Scoring note: kicker points use Yahoo's DISTANCE-TIERED field goals (0-39 = 3,
40-49 = 4, 50+ = 5, PAT = 1), verified against Yahoo's own express-settings
default page. The previous flat 3-per-field-goal formula undercounted kickers
by ~15 points a season (max 34) and changed the relative rank of 22 of 42
kickers, so this backtest would be measuring the wrong target without it.

Usage:
    python -m model.ff_kicker_projection_backtest
"""

from __future__ import annotations

import statistics
from typing import Any

import pandas as pd

from ingest.ff_independent import (
    BASELINE_GAMES,
    NFLVERSE_STATS_URL,
    POSITION_PRIOR_PPG,
    POSITION_REGRESSION_PRIOR_GAMES,
    REGRESSION_PRIOR_GAMES,
    SEASON_WEIGHTS,
    yahoo_kicker_points,
)

HISTORY_SEASONS = range(2020, 2026)
TARGET_SEASONS = (2023, 2024, 2025)
TUNING_TARGETS = (2023, 2024)
HELD_OUT_TARGET = 2025
# A real starting kicker, not a one-week injury fill-in whose season total is
# noise. Applied to the TARGET season only; history seasons just need >0 games.
MIN_TARGET_GAMES = 8


def load_kicker_seasons() -> tuple[dict[int, dict[str, float]], dict[int, dict[str, int]]]:
    points: dict[int, dict[str, float]] = {}
    games: dict[int, dict[str, int]] = {}
    for season in HISTORY_SEASONS:
        frame = pd.read_csv(NFLVERSE_STATS_URL.format(season=season), low_memory=False)
        kickers = frame[frame["position"] == "K"].fillna(0)
        points[season], games[season] = {}, {}
        for _, row in kickers.iterrows():
            player_id = str(row.get("player_id") or "").strip()
            played = int(row.get("games") or 0)
            if not player_id or played < 1:
                continue
            points[season][player_id] = yahoo_kicker_points(row.to_dict())
            games[season][player_id] = played
    return points, games


def weighted_history_points(
    player_id: str, target: int, points: dict[int, dict[str, float]], games: dict[int, dict[str, int]]
) -> float | None:
    numerator = denominator = 0.0
    for index, season in enumerate(range(target - 3, target)):
        if player_id not in points.get(season, {}):
            return None
        numerator += (points[season][player_id] / games[season][player_id]) * SEASON_WEIGHTS[index]
        denominator += SEASON_WEIGHTS[index]
    return (numerator / denominator) * BASELINE_GAMES if denominator else None


def prior_season_points(
    player_id: str, target: int, points: dict[int, dict[str, float]], games: dict[int, dict[str, int]]
) -> float | None:
    previous = target - 1
    if player_id not in points.get(previous, {}):
        return None
    return points[previous][player_id] / games[previous][player_id] * BASELINE_GAMES


def mae(rows: list[dict[str, Any]], field: str, lam: float, prior: float) -> float:
    return statistics.mean(abs((prior + lam * (row[field] - prior)) - row["actual"]) for row in rows)


def spearman(xs: list[float], ys: list[float]) -> float:
    def rank(values: list[float]) -> list[float]:
        order = sorted(range(len(values)), key=lambda index: values[index])
        ranks = [0.0] * len(values)
        for position, index in enumerate(order):
            ranks[index] = position + 1
        return ranks

    xs, ys = rank(xs), rank(ys)
    mean_x, mean_y = statistics.mean(xs), statistics.mean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator = (sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys)) ** 0.5
    return numerator / denominator if denominator else float("nan")


def run() -> dict[str, Any]:
    points, games = load_kicker_seasons()
    prior = POSITION_PRIOR_PPG["PPR"]["K"] * BASELINE_GAMES

    rows: list[dict[str, Any]] = []
    for target in TARGET_SEASONS:
        for player_id, actual in points[target].items():
            if games[target][player_id] < MIN_TARGET_GAMES:
                continue
            three_year = weighted_history_points(player_id, target, points, games)
            carry = prior_season_points(player_id, target, points, games)
            if three_year is None or carry is None:
                continue
            rows.append({"season": target, "actual": actual, "three_year": three_year, "carry": carry})

    tune = [row for row in rows if row["season"] in TUNING_TARGETS]
    test = [row for row in rows if row["season"] == HELD_OUT_TARGET]
    actuals = [row["actual"] for row in rows]

    print("Kicker projection backtest (walk-forward, no leakage)")
    print(f"  targets {TARGET_SEASONS}, n={len(rows)} kicker-seasons (>= {MIN_TARGET_GAMES} games)")
    print(f"  tuned on {TUNING_TARGETS} (n={len(tune)}), held out {HELD_OUT_TARGET} (n={len(test)})\n")
    print(f"  {'Predictor':<28}{'best lambda':>13}{'held-out MAE':>15}{'rank rho':>11}")

    result: dict[str, Any] = {"n_total": len(rows), "n_tuning": len(tune), "n_held_out": len(test)}
    for field, label in (("three_year", "3-year weighted (shipped)"), ("carry", "prior season only")):
        best = min((index / 100 for index in range(101)), key=lambda lam: mae(tune, field, lam, prior))
        held_out = mae(test, field, best, prior)
        rho = spearman([row[field] for row in rows], actuals)
        result[f"{field}_best_lambda"] = round(best, 2)
        result[f"{field}_held_out_mae"] = round(held_out, 1)
        result[f"{field}_spearman"] = round(rho, 2)
        print(f"  {label:<28}{best:>13.2f}{held_out:>15.1f}{rho:>11.2f}")

    flat = mae(test, "three_year", 0.0, prior)
    result["flat_held_out_mae"] = round(flat, 1)
    print(f"  {'flat constant':<28}{0.0:>13.2f}{flat:>15.1f}{'n/a':>11}")

    shipped_prior_games = POSITION_REGRESSION_PRIOR_GAMES.get("K", REGRESSION_PRIOR_GAMES)
    full_sample_games = 3 * BASELINE_GAMES
    shipped_lambda = full_sample_games / (full_sample_games + shipped_prior_games)
    default_lambda = full_sample_games / (full_sample_games + REGRESSION_PRIOR_GAMES)
    result["shipped_effective_lambda"] = round(shipped_lambda, 2)
    print(
        f"\n  Shipped POSITION_REGRESSION_PRIOR_GAMES['K']={shipped_prior_games:.0f}"
        f" -> effective lambda {shipped_lambda:.2f} at a {full_sample_games:.0f}-game sample"
        f"\n  (the {REGRESSION_PRIOR_GAMES:.0f}-game default would leave {default_lambda:.2f} -- too little shrinkage)"
    )
    print(
        "\n  VERDICT: kicker history is genuinely predictive (both history methods"
        "\n  beat the flat constant), and the 3-year weighted blend beats"
        "\n  prior-season-only. Kickers therefore keep the standard multi-year"
        "\n  regression -- do NOT switch them to the DST-style carry-forward."
    )
    return result


if __name__ == "__main__":
    run()
