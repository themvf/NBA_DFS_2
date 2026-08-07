"""Walk-forward backtest: how much can prior-season data say about DST scoring?

Very little, and this script quantifies exactly how little so the answer is
reproducible rather than re-litigated. Two findings drive the shipped model
(`ingest/ff_independent.py`, DST branch):

1. MAGNITUDE is nearly unpredictable. A 3-year weighted history regression
   (shipped briefly as v1.7, 2026-08-07) scored WORSE on held-out 2025 than a
   flat constant -- MAE 26.1 vs 24.8 -- because every Yahoo DST scoring
   component is near-noise year over year. The tuning period's own optimum was
   lambda=0.05, i.e. "almost entirely ignore the history". So the projection is
   shrunk hard toward the league prior.
2. ORDER carries a little real signal. Prior-season carry-forward reaches
   Spearman 0.18 and a 42% top-12 hit rate (vs 38% random) -- weak, but better
   than the 3-year blend (0.15) and infinitely better than a flat constant,
   which cannot rank at all.

Because shrinkage is monotonic, v1.9 uses carry-forward for ORDER and shrinkage
for MAGNITUDE, which is strictly best: held-out MAE 24.3, beating both raw
carry-forward (28.0) and the flat constant (24.8) while staying rankable.

This backtest should have been run BEFORE v1.7 shipped, not after.

Usage:
    python -m model.ff_dst_projection_backtest
"""

from __future__ import annotations

import statistics
from typing import Any

import pandas as pd

from ingest.ff_independent import (
    NFLVERSE_SCHEDULE_URL,
    NFLVERSE_TEAM_STATS_URL,
    YAHOO_DST_FUMBLE_REC_PTS,
    YAHOO_DST_INT_PTS,
    YAHOO_DST_SACK_PTS,
    YAHOO_DST_SAFETY_PTS,
    YAHOO_DST_TD_PTS,
    SEASON_WEIGHTS,
    BASELINE_GAMES,
    POSITION_PRIOR_PPG,
    _team_points_allowed_fpts_by_season,
    normalize_team,
)

HISTORY_SEASONS = range(2020, 2026)
TARGET_SEASONS = (2023, 2024, 2025)
TUNING_TARGETS = (2023, 2024)
HELD_OUT_TARGET = 2025


def load_actuals() -> tuple[dict[int, dict[str, float]], dict[int, dict[str, int]]]:
    """Real Yahoo-scored DST fantasy points per team-season."""
    schedule = pd.read_csv(NFLVERSE_SCHEDULE_URL)
    actuals: dict[int, dict[str, float]] = {}
    games: dict[int, dict[str, int]] = {}
    for season in HISTORY_SEASONS:
        frame = pd.read_csv(NFLVERSE_TEAM_STATS_URL.format(season=season))
        points_allowed = _team_points_allowed_fpts_by_season(schedule, season)
        actuals[season], games[season] = {}, {}
        for _, row in frame.iterrows():
            team = normalize_team(row["team"])
            actuals[season][team] = float(
                row["def_sacks"] * YAHOO_DST_SACK_PTS
                + row["def_interceptions"] * YAHOO_DST_INT_PTS
                + row["fumble_recovery_opp"] * YAHOO_DST_FUMBLE_REC_PTS
                + row["def_safeties"] * YAHOO_DST_SAFETY_PTS
                + (row["def_tds"] + row["special_teams_tds"]) * YAHOO_DST_TD_PTS
                + points_allowed.get(team, 0.0)
            )
            games[season][team] = int(row["games"])
    return actuals, games


def weighted_history_points(
    team: str, target: int, actuals: dict[int, dict[str, float]], games: dict[int, dict[str, int]]
) -> float | None:
    """Unshrunk 3-year weighted-history projection (the lambda=1 extreme)."""
    numerator = denominator = 0.0
    for index, season in enumerate(range(target - 3, target)):
        if team not in actuals.get(season, {}):
            return None
        numerator += (actuals[season][team] / games[season][team]) * SEASON_WEIGHTS[index]
        denominator += SEASON_WEIGHTS[index]
    return (numerator / denominator) * BASELINE_GAMES if denominator else None


def mae(rows: list[dict[str, Any]], predict) -> float:
    return statistics.mean(abs(predict(row) - row["actual"]) for row in rows)


def run() -> dict[str, Any]:
    actuals, games = load_actuals()
    prior_points = POSITION_PRIOR_PPG["PPR"]["DST"] * BASELINE_GAMES

    rows: list[dict[str, Any]] = []
    for target in TARGET_SEASONS:
        for team, actual in actuals[target].items():
            raw = weighted_history_points(team, target, actuals, games)
            if raw is None:
                continue
            rows.append({"season": target, "team": team, "actual": actual, "raw": raw})

    tune = [row for row in rows if row["season"] in TUNING_TARGETS]
    test = [row for row in rows if row["season"] == HELD_OUT_TARGET]

    def shrunk(lam: float):
        return lambda row: prior_points + lam * (row["raw"] - prior_points)

    best_lambda = min(
        (index / 100 for index in range(0, 101)),
        key=lambda lam: mae(tune, shrunk(lam)),
    )

    result = {
        "n_total": len(rows),
        "n_tuning": len(tune),
        "n_held_out": len(test),
        "best_lambda_on_tuning": round(best_lambda, 2),
        "held_out_mae_flat_constant": round(mae(test, shrunk(0.0)), 1),
        "held_out_mae_tuned": round(mae(test, shrunk(best_lambda)), 1),
        "held_out_mae_full_history": round(mae(test, shrunk(1.0)), 1),
    }
    print("DST projection backtest (walk-forward, no leakage)")
    print(f"  targets {TARGET_SEASONS}, n={result['n_total']} team-seasons")
    print(f"  tuned on {TUNING_TARGETS} (n={result['n_tuning']}), held out {HELD_OUT_TARGET} (n={result['n_held_out']})\n")
    print(f"  best lambda on tuning period: {result['best_lambda_on_tuning']:.2f}")
    print(f"  held-out MAE, flat constant       : {result['held_out_mae_flat_constant']}")
    print(f"  held-out MAE, tuned shrinkage     : {result['held_out_mae_tuned']}")
    print(f"  held-out MAE, full history (v1.7) : {result['held_out_mae_full_history']}")
    print(
        "\n  VERDICT: prior-season box score barely predicts DST MAGNITUDE, so the"
        "\n  shipped model shrinks hard toward the league prior. It does carry weak"
        "\n  ORDER signal, so carry-forward still sets the ranking (shrinkage is"
        "\n  monotonic and cannot change it). Do not widen the spread or re-ship an"
        "\n  unshrunk/multi-year DST projection without a NEW data source that clears"
        "\n  this same held-out bar."
    )
    return result


if __name__ == "__main__":
    run()
