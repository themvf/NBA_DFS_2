"""Walk-forward backtest: can prior-season box-score data predict DST fantasy points?

Answer, on 2023/2024/2025 target seasons: no. Every history-based predictor
tested is WORSE than a flat league-average constant. This script exists so
that verdict is reproducible and so the next person (or the next model
revision) does not rebuild the same thing without new evidence.

Context: `ingest/ff_independent.py` briefly shipped a real history-regression
DST model (v1.7, 2026-08-07) to replace a flat 105.0 placeholder. This
backtest -- which should have been run BEFORE that shipped, not after --
showed it made accuracy worse, and it was reverted in v1.8.

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
        "\n  VERDICT: prior-season box score does not predict DST fantasy points."
        "\n  A flat league-average constant is at or better than every history-based"
        "\n  predictor tested. Do not re-ship a history-regression DST projection"
        "\n  without a NEW data source that clears this same held-out bar."
    )
    return result


if __name__ == "__main__":
    run()
