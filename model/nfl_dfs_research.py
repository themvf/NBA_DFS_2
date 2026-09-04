"""Pure, versioned NFL DFS research: chronological fits and promotion gates.

2025 is a retrospective diagnostic (previously inspected), never a clean
holdout. Historical closing-reference features cannot graduate to shadow.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd

VERSION = "nfl-dfs-research-v1"
SEED = 20260903
POSITIONS = ("QB", "RB", "WR", "TE", "DST")
FEATURES = {
    "opportunity": ("baseline", "history_games", "prior_opportunity"),
    "closing_exploratory": (
        "baseline", "history_games", "prior_opportunity",
        "team_implied", "opponent_implied", "team_spread",
    ),
}
ALPHAS = (0.1, 1.0, 10.0, 100.0)


def implied_totals(total: float, source_home_favored_spread: float) -> tuple[float, float]:
    """nflverse spread is POSITIVE when home is favored, unlike Odds API."""
    return (total + source_home_favored_spread) / 2, (total - source_home_favored_spread) / 2


def fit(rows: list[dict], feature_names: tuple[str, ...], alpha: float) -> dict:
    if not rows:
        raise ValueError("empty training cohort")
    x = np.array([[r[k] for k in feature_names] for r in rows], dtype=float)
    if not np.isfinite(x).all():
        raise ValueError("non-finite training features")
    center, scale = x.mean(axis=0), x.std(axis=0)
    scale[scale < 1e-8] = 1
    z = np.column_stack([np.ones(len(x)), (x - center) / scale])
    y = np.array([r["actual"] - r["baseline"] for r in rows])
    penalty = np.eye(z.shape[1]) * alpha
    penalty[0, 0] = 0
    coef = np.linalg.solve(z.T @ z + penalty, z.T @ y)
    residuals = np.array([r["actual"] for r in rows]) - (np.array([r["baseline"] for r in rows]) + z @ coef)
    return {
        "features": list(feature_names), "center": center.tolist(), "scale": scale.tolist(),
        "coefficients": coef.tolist(), "alpha": alpha, "training_rows": len(rows),
        "training_seasons": sorted({r["season"] for r in rows}),
        "residuals": sorted(residuals.tolist()),
    }


def predict(recipe: dict, row: dict) -> float:
    x = np.array([row[k] for k in recipe["features"]], dtype=float)
    z = (x - recipe["center"]) / recipe["scale"]
    return float(row["baseline"] + recipe["coefficients"][0] + z @ recipe["coefficients"][1:])


def metrics(rows: list[dict], key: str = "prediction") -> dict:
    if not rows:
        return {"n": 0}
    y = np.array([r["actual"] for r in rows])
    pred = np.array([r[key] for r in rows])
    groups = defaultdict(list)
    for r in rows:
        groups[(r.get("season"), r.get("week"), r.get("position"))].append(r)
    top_hits, top_count = 0, 0
    for cohort in groups.values():
        n = max(1, int(np.ceil(len(cohort)*0.1)))
        # Index breaks ties deterministically without inventing slate identity.
        actual_top = set(sorted(range(len(cohort)), key=lambda i: (-cohort[i]["actual"], i))[:n])
        predicted_top = set(sorted(range(len(cohort)), key=lambda i: (-cohort[i][key], i))[:n])
        top_hits += len(actual_top & predicted_top)
        top_count += n
    rank_corr = float(pd.Series(y).rank().corr(pd.Series(pred).rank())) if np.std(y)>0 and np.std(pred)>0 else None
    return {
        "n": len(rows), "mae": float(np.mean(abs(pred-y))),
        "rmse": float(np.sqrt(np.mean((pred-y)**2))), "bias": float(np.mean(pred-y)),
        "p10_coverage": float(np.mean([r["actual"] <= r["p10"] for r in rows])),
        "p90_coverage": float(np.mean([r["actual"] <= r["p90"] for r in rows])),
        "boom_brier": float(np.mean([(r["boom_probability"] - float(r["actual"] >= r["boom_threshold"]))**2 for r in rows])),
        "spearman": rank_corr,
        "top_decile_precision_within_position_week": top_hits/top_count,
        "top_decile_definition": "recorded-stat weekly positional cohort, NOT a DraftKings slate",
        "boom_calibration": [
            {"lower": i/10, "n": len(bucket),
             "predicted": float(np.mean([r["boom_probability"] for r in bucket])) if bucket else None,
             "observed": float(np.mean([r["actual"] >= r["boom_threshold"] for r in bucket])) if bucket else None}
            for i in range(10)
            for bucket in [[r for r in rows if i/10 <= r["boom_probability"] and
                            (r["boom_probability"] < (i+1)/10 or i == 9)]]
        ],
    }


def clustered_mae_delta(rows: list[dict], repeats: int = 500) -> dict:
    """Paired bootstrap of entire games, not correlated individual players."""
    clusters: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        clusters[r["game_id"]].append(abs(r["prediction"]-r["actual"])-abs(r["baseline"]-r["actual"]))
    if len(clusters) < 2:
        return {"games": len(clusters), "delta": None, "ci95": None}
    values = list(clusters.values())
    sums = np.array([sum(v) for v in values])
    counts = np.array([len(v) for v in values])
    rng = np.random.default_rng(SEED)
    idx = rng.integers(0, len(values), size=(repeats, len(values)))
    draws = sums[idx].sum(axis=1) / counts[idx].sum(axis=1)
    return {"games": len(values), "delta": float(sums.sum()/counts.sum()),
            "ci95": np.quantile(draws, [0.025, 0.975]).tolist()}


def candidate_rows(recipe: dict, rows: list[dict]) -> list[dict]:
    residuals = np.array(recipe["residuals"])
    q10, q90 = np.quantile(residuals, [0.1, 0.9])
    return [{**r, "prediction": (p := predict(recipe, r)),
             "p10": p + float(q10), "p90": p + float(q90),
             "boom_probability": float(np.mean(residuals+p >= r["boom_threshold"]))}
            for r in rows]


def evaluate(samples: list[dict]) -> tuple[dict, list[dict]]:
    report: dict[str, Any] = {
        "version": VERSION, "split": {"fit": [2023], "select": [2024], "retrospective": [2025], "fresh_forward": [2026]},
        "2025_is_untouched": False, "production_promotion": False,
        "salary_adjusted": "unavailable: no historical DK slate salaries",
        "ownership_adjusted": "unavailable: no historical contest ownership",
        "benchmark": "production historical v2 algorithm with market inputs disabled; not archived production forecasts",
        "shadow_gate": "opportunity-only; validation MAE improves; retrospective game-clustered CI upper < 0; >=1% retrospective MAE gain; no worse boom Brier; >=100 rows per split; forward evaluation required before any promotion",
        "baseline": {}, "candidates": {},
    }
    saved_predictions: list[dict] = []
    for position in POSITIONS:
        cohort = [r for r in samples if r["position"] == position]
        report["baseline"][position] = {}
        for season in (2023, 2024, 2025):
            season_rows = [r for r in cohort if r["season"] == season]
            report["baseline"][position][str(season)] = metrics(season_rows, "baseline")
            if season_rows:
                simple = [abs(r["own_history_baseline"]-r["actual"]) for r in season_rows if r.get("own_history_baseline") is not None]
                report["baseline"][position][str(season)]["own_recency_mean_mae"] = float(np.mean(simple)) if simple else None
            saved_predictions.extend({**r, "model": "baseline", "prediction": r["baseline"]} for r in season_rows)
        for name, features in FEATURES.items():
            valid = [r for r in cohort if all(r.get(k) is not None for k in features)]
            train = [r for r in valid if r["season"] == 2023]
            validation = [r for r in valid if r["season"] == 2024]
            diagnostic = [r for r in valid if r["season"] == 2025]
            key = f"{position}:{name}"
            if min(len(train), len(validation), len(diagnostic)) < 100:
                report["candidates"][key] = {"status": "insufficient_samples", "counts": list(map(len, (train, validation, diagnostic)))}
                continue
            trials = []
            for alpha in ALPHAS:
                recipe = fit(train, features, alpha)
                trials.append((metrics(candidate_rows(recipe, validation))["mae"], alpha, recipe))
            _, alpha, selected = min(trials, key=lambda t: (t[0], t[1]))
            val_predictions = candidate_rows(selected, validation)
            frozen = fit(train+validation, features, alpha)
            test_predictions = candidate_rows(frozen, diagnostic)
            val_delta = clustered_mae_delta(val_predictions)
            test_delta = clustered_mae_delta(test_predictions)
            val_metrics, test_metrics = metrics(val_predictions), metrics(test_predictions)
            baseline_test = metrics(diagnostic, "baseline")
            eligible = bool(
                name == "opportunity" and val_delta["delta"] is not None and test_delta["ci95"] is not None
                and val_delta["delta"] < 0 and test_delta["ci95"][1] < 0
                and test_metrics["mae"] <= baseline_test["mae"] * 0.99
                and test_metrics["boom_brier"] <= baseline_test["boom_brier"]
            )
            report["candidates"][key] = {
                "status": "eligible_for_shadow_only" if eligible else "not_eligible",
                "market_timestamp_verified": False if name == "closing_exploratory" else None,
                "selected_alpha": alpha, "validation_trials": [{"mae": t[0], "alpha": t[1]} for t in trials],
                "validation": val_metrics, "validation_mae_delta": val_delta,
                "retrospective_2025": test_metrics, "retrospective_mae_delta": test_delta,
                "paired_baseline_2025": baseline_test, "recipe": frozen,
            }
            saved_predictions.extend({**r, "model": name} for r in val_predictions+test_predictions)
    return report, saved_predictions
