"""Walk-forward screen for the QB/RB/WR/TE history-weighting constants.

`SEASON_WEIGHTS` (0.05/0.20/0.75) and `REGRESSION_PRIOR_GAMES` (4.0) in
`ingest/ff_independent.py` were set by judgment, never fitted. That is exactly
the situation `model/ff_kicker_projection_backtest.py` found for kickers, where
the default shrinkage turned out far too weak (effective carry-forward weight
0.93 against a fitted 0.58) and correcting it improved held-out MAE 23.2 -> 22.1.
This script asks the same question for the four skill positions, using the same
walk-forward discipline: tune on early targets, grade once on a season never
looked at during tuning.

WHAT IS AND IS NOT UNDER TEST
-----------------------------
Under test: the weighted-history blend and the shrinkage toward the position
prior -- everything that turns past seasons into `regressed_ppg`.

Deliberately held constant, so this measures one thing:
  * `role_factor` is pinned to 1.0. It depends on the CURRENT depth chart, which
    is not reconstructible for past seasons -- a live Sleeper depth order says
    nothing about who was RB2 in 2022. Including it would inject an
    unmeasurable term into every residual.
  * The target is POINTS PER GAME, not season totals. The model projects a fixed
    17-game baseline by design (availability is modelled separately and must not
    silently reduce that baseline), so grading against raw season totals would
    mostly measure injury prediction rather than the rate these constants
    control.
  * Players need `MIN_TARGET_GAMES` in the target season, so a two-game actual
    cannot masquerade as a per-game rate.

Usage:
    python -m model.ff_skill_constant_screen
"""

from __future__ import annotations

import random
import statistics
from typing import Any

import pandas as pd

from ingest.ff_independent import (
    NFLVERSE_STATS_URL,
    POSITION_PRIOR_PPG,
    REGRESSION_PRIOR_GAMES,
    SEASON_WEIGHTS,
)

POSITIONS = ("QB", "RB", "WR", "TE")
SCORING = "PPR"
HISTORY_SEASONS = range(2019, 2026)
TUNING_TARGETS = (2022, 2023, 2024)
HELD_OUT_TARGET = 2025
MIN_TARGET_GAMES = 6

# Candidate grids. Weights are (T-3, T-2, T-1) and get normalised, so only their
# ratio matters -- the grid spans "career-ish" (flat) through "last season only".
WEIGHT_CANDIDATES: tuple[tuple[float, float, float], ...] = (
    (0.33, 0.33, 0.34),
    (0.20, 0.30, 0.50),
    (0.15, 0.25, 0.60),
    (0.10, 0.25, 0.65),
    (0.05, 0.20, 0.75),  # current default
    (0.05, 0.15, 0.80),
    (0.00, 0.15, 0.85),
    (0.00, 0.10, 0.90),
    (0.00, 0.00, 1.00),
)
PRIOR_GAMES_CANDIDATES = (0.0, 2.0, 4.0, 8.0, 12.0, 16.0, 24.0, 32.0, 48.0, 64.0)

# Below this, a fitted improvement is not worth shipping: 0.05 PPG is under one
# point across a whole 17-game season, and re-tuning constants on noise is how
# the v1.7 DST regression happened.
MIN_MEANINGFUL_MAE_GAIN = 0.05

# A 90-point grid searched against a single held-out season can find an
# improvement by luck, so the held-out delta gets a bootstrap CI and only counts
# when the interval excludes zero. Same discipline the betting specs in
# CLAUDE.md apply to every ROI/CLV claim.
BOOTSTRAP_SAMPLES = 2000
BOOTSTRAP_SEED = 20260822


def load_seasons() -> dict[int, dict[str, dict[str, Any]]]:
    """{season: {player_id: {position, games, ppg}}} for regular-season play."""
    out: dict[int, dict[str, dict[str, Any]]] = {}
    for season in HISTORY_SEASONS:
        frame = pd.read_csv(NFLVERSE_STATS_URL.format(season=season))
        if "season_type" in frame.columns:
            frame = frame[frame["season_type"] == "REG"]
        rows: dict[str, dict[str, Any]] = {}
        for _, row in frame.iterrows():
            position = str(row.get("position") or "")
            games = int(row.get("games") or 0)
            if position not in POSITIONS or games <= 0:
                continue
            column = "fantasy_points_ppr" if SCORING == "PPR" else "fantasy_points"
            points = float(row.get(column) or 0.0)
            rows[str(row["player_id"])] = {
                "position": position,
                "games": games,
                "ppg": points / games,
            }
        out[season] = rows
    return out


def build_rows(seasons: dict[int, dict[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    """One row per (player, target season), using the model's own history window."""
    rows: list[dict[str, Any]] = []
    for target in (*TUNING_TARGETS, HELD_OUT_TARGET):
        for player_id, actual in seasons.get(target, {}).items():
            if actual["games"] < MIN_TARGET_GAMES:
                continue
            history = []
            for slot, season in enumerate(range(target - 3, target)):
                past = seasons.get(season, {}).get(player_id)
                if past:
                    history.append({"slot": slot, "games": past["games"], "ppg": past["ppg"]})
            if not history:
                continue
            rows.append({
                "target": target,
                "position": actual["position"],
                "actual_ppg": actual["ppg"],
                "history": history,
            })
    return rows


def predict(row: dict[str, Any], weights: tuple[float, float, float], prior_games: float) -> float:
    """Mirrors project_player()'s history branch with role_factor pinned to 1.0."""
    weighted = weight_total = 0.0
    history_games = 0
    for entry in row["history"]:
        weight = weights[entry["slot"]]
        if weight <= 0:
            continue
        weighted += entry["ppg"] * weight
        weight_total += weight
        history_games += entry["games"]
    prior_ppg = POSITION_PRIOR_PPG[SCORING][row["position"]]
    if not weight_total:
        return prior_ppg
    raw_ppg = weighted / weight_total
    return (raw_ppg * history_games + prior_ppg * prior_games) / (history_games + prior_games)


def mae(rows: list[dict[str, Any]], weights, prior_games: float) -> float:
    return statistics.mean(abs(predict(r, weights, prior_games) - r["actual_ppg"]) for r in rows)


def _rank(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: values[index])
    out = [0.0] * len(values)
    for position, index in enumerate(order):
        out[index] = position
    return out


def spearman(rows: list[dict[str, Any]], weights, prior_games: float) -> float:
    xs = _rank([predict(r, weights, prior_games) for r in rows])
    ys = _rank([r["actual_ppg"] for r in rows])
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    den = (sum((a - mx) ** 2 for a in xs) ** 0.5) * (sum((b - my) ** 2 for b in ys) ** 0.5)
    return num / den if den else float("nan")


def bootstrap_delta_ci(
    test: list[dict[str, Any]], weights, prior_games: float
) -> tuple[float, float]:
    """95% CI on (fitted MAE - current MAE), resampling held-out players."""
    rng = random.Random(BOOTSTRAP_SEED)
    per_row = [
        (
            abs(predict(r, weights, prior_games) - r["actual_ppg"])
            - abs(predict(r, SEASON_WEIGHTS, REGRESSION_PRIOR_GAMES) - r["actual_ppg"])
        )
        for r in test
    ]
    deltas = []
    size = len(per_row)
    for _ in range(BOOTSTRAP_SAMPLES):
        sample = [per_row[rng.randrange(size)] for _ in range(size)]
        deltas.append(statistics.mean(sample))
    deltas.sort()
    return deltas[int(0.025 * BOOTSTRAP_SAMPLES)], deltas[int(0.975 * BOOTSTRAP_SAMPLES)]


def run() -> dict[str, Any]:
    seasons = load_seasons()
    rows = build_rows(seasons)
    results: dict[str, Any] = {}

    print("Skill-position constant screen (walk-forward, PPR, points per game)")
    print(f"  tuned on {TUNING_TARGETS}, held out {HELD_OUT_TARGET}")
    print(f"  shipped defaults: weights={SEASON_WEIGHTS}, prior_games={REGRESSION_PRIOR_GAMES}\n")
    header = (
        f"{'pos':<5}{'n tune':>7}{'n test':>7}{'fitted weights':>20}{'prior':>7}"
        f"{'MAE now':>9}{'MAE fit':>9}{'delta':>8}{'95% CI':>18}{'rho fit':>9}"
    )
    print(header)
    print("-" * len(header))

    for position in POSITIONS:
        tune = [r for r in rows if r["position"] == position and r["target"] in TUNING_TARGETS]
        test = [r for r in rows if r["position"] == position and r["target"] == HELD_OUT_TARGET]
        if len(tune) < 50 or len(test) < 20:
            print(f"{position:<5}{len(tune):>7}{len(test):>7}   insufficient sample -- no verdict")
            continue

        best_weights, best_prior = min(
            ((w, p) for w in WEIGHT_CANDIDATES for p in PRIOR_GAMES_CANDIDATES),
            key=lambda combo: mae(tune, combo[0], combo[1]),
        )
        mae_now = mae(test, SEASON_WEIGHTS, REGRESSION_PRIOR_GAMES)
        mae_fit = mae(test, best_weights, best_prior)
        rho_now = spearman(test, SEASON_WEIGHTS, REGRESSION_PRIOR_GAMES)
        rho_fit = spearman(test, best_weights, best_prior)
        low, high = bootstrap_delta_ci(test, best_weights, best_prior)
        excludes_zero = high < 0
        weights_text = "/".join(f"{value:.2f}" for value in best_weights)
        ci_text = f"[{low:+.3f},{high:+.3f}]"
        print(
            f"{position:<5}{len(tune):>7}{len(test):>7}{weights_text:>20}{best_prior:>7.0f}"
            f"{mae_now:>9.3f}{mae_fit:>9.3f}{mae_fit - mae_now:>+8.3f}{ci_text:>18}{rho_fit:>9.3f}"
        )
        results[position] = {
            "n_tune": len(tune),
            "n_test": len(test),
            "fitted_weights": best_weights,
            "fitted_prior_games": best_prior,
            "held_out_mae_current": round(mae_now, 3),
            "held_out_mae_fitted": round(mae_fit, 3),
            "held_out_mae_delta": round(mae_fit - mae_now, 3),
            "held_out_spearman_current": round(rho_now, 3),
            "held_out_spearman_fitted": round(rho_fit, 3),
            "delta_ci_low": round(low, 3),
            "delta_ci_high": round(high, 3),
            "ci_excludes_zero": excludes_zero,
            "worth_shipping": (mae_now - mae_fit) >= MIN_MEANINGFUL_MAE_GAIN and excludes_zero,
        }

    shippable = [position for position, r in results.items() if r["worth_shipping"]]
    print(
        "\n  A negative delta means the fitted constants beat the shipped ones on a"
        f"\n  season never used for tuning. Gains under {MIN_MEANINGFUL_MAE_GAIN} PPG"
        "\n  are not worth shipping -- that is under one point across a 17-game"
        "\n  season, and re-tuning constants on noise is how v1.7's DST regression"
        "\n  happened."
    )
    print(f"\n  Positions clearing the bar: {', '.join(shippable) if shippable else 'NONE'}")
    return results


if __name__ == "__main__":
    run()
