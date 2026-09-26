"""Held-out attribution study for PBP descriptors and NFL opening spreads."""

from __future__ import annotations

import numpy as np
import pandas as pd


STUDY_VERSION = "nfl-market-context-attribution-v1"
BASELINE = ("margin_difference", "scoring_environment")
PBP = (
    "epa_difference",
    "success_difference",
    "explosive_difference",
    "pass_rate_difference",
    "combined_pace_seconds",
    "turnover_difference",
    "sack_difference",
)


def market_pregame_features(
    team_games: pd.DataFrame, *, lookback: int = 6, minimum: int = 4
) -> pd.DataFrame:
    """Collapse two strictly prior team histories into one pregame matchup row."""
    if lookback < minimum or minimum < 1:
        raise ValueError("lookback must be at least the positive minimum")
    ordered = team_games.sort_values(["team", "season", "week", "game_id"]).copy()
    ordered["margin"] = ordered["points_for"] - ordered["points_against"]
    ordered["points_total"] = ordered["points_for"] + ordered["points_against"]
    ordered["interval_seconds"] = (
        ordered["interval_seconds_sum"] / ordered["interval_count"].replace(0, np.nan)
    )
    ordered["turnovers"] = ordered["turnover_rate"] + ordered["fumble_lost_rate"]
    metrics = (
        "margin", "points_total", "offensive_epa", "success_rate",
        "explosive_rate", "pass_rate", "interval_seconds", "turnovers", "sack_rate",
    )
    groups = ordered.groupby(["team", "season"], sort=False)
    for name in metrics:
        ordered[f"prior_{name}"] = groups[name].transform(
            lambda values: values.shift(1).rolling(lookback, min_periods=minimum).mean()
        )
    home = ordered[ordered["team"].eq(ordered["home_team"])].copy()
    away = ordered[ordered["team"].eq(ordered["away_team"])].copy()
    keep = ["game_id"] + [f"prior_{name}" for name in metrics]
    away = away[keep].rename(
        columns={column: f"away_{column}" for column in keep if column != "game_id"}
    )
    matchups = home.merge(away, on="game_id", how="inner", validate="one_to_one")
    matchups["margin_difference"] = matchups["prior_margin"] - matchups["away_prior_margin"]
    matchups["scoring_environment"] = (
        matchups["prior_points_total"] + matchups["away_prior_points_total"]
    ) / 2
    for metric, output in (
        ("offensive_epa", "epa_difference"),
        ("success_rate", "success_difference"),
        ("explosive_rate", "explosive_difference"),
        ("pass_rate", "pass_rate_difference"),
        ("turnovers", "turnover_difference"),
        ("sack_rate", "sack_difference"),
    ):
        matchups[output] = matchups[f"prior_{metric}"] - matchups[f"away_prior_{metric}"]
    matchups["combined_pace_seconds"] = (
        matchups["prior_interval_seconds"] + matchups["away_prior_interval_seconds"]
    ) / 2
    return matchups.dropna(subset=list(BASELINE + PBP)).reset_index(drop=True)


def _fit(
    train: pd.DataFrame, test: pd.DataFrame, features: tuple[str, ...]
) -> tuple[np.ndarray, np.ndarray]:
    x = train[list(features)].to_numpy(dtype=float)
    means, scales = x.mean(axis=0), x.std(axis=0)
    scales[scales == 0] = 1
    design = np.column_stack([np.ones(len(train)), (x - means) / scales])
    coefficients = np.linalg.lstsq(
        design, train["home_spread"].to_numpy(dtype=float), rcond=None
    )[0]
    test_design = np.column_stack([
        np.ones(len(test)),
        (test[list(features)].to_numpy(dtype=float) - means) / scales,
    ])
    return test_design @ coefficients, coefficients


def _score(actual: np.ndarray, prediction: np.ndarray) -> dict[str, float | int]:
    residual = prediction - actual
    return {
        "n": int(len(actual)),
        "mae": float(np.abs(residual).mean()),
        "rmse": float(np.sqrt(np.mean(residual**2))),
        "bias": float(residual.mean()),
    }


def evaluate_market_attribution(samples: pd.DataFrame) -> dict[str, object]:
    seasons = sorted(int(value) for value in samples["season"].unique())
    folds: list[dict[str, object]] = []
    for index, season in enumerate(seasons):
        train_seasons = seasons[:index]
        if len(train_seasons) < 2:
            continue
        train = samples[samples["season"].isin(train_seasons)]
        test = samples[samples["season"].eq(season)]
        if train.empty or test.empty:
            continue
        baseline, _ = _fit(train, test, BASELINE)
        challenger, _ = _fit(train, test, BASELINE + PBP)
        actual = test["home_spread"].to_numpy(dtype=float)
        baseline_score = _score(actual, baseline)
        challenger_score = _score(actual, challenger)
        folds.append({
            "season": season,
            "trainSeasons": train_seasons,
            "baseline": baseline_score,
            "pbpAttribution": challenger_score,
            "maeGain": baseline_score["mae"] - challenger_score["mae"],
        })
    if not folds:
        raise ValueError("market attribution needs at least three seasons")
    _, coefficients = _fit(samples, samples, BASELINE + PBP)
    associations = sorted(
        (
            {"feature": feature, "standardizedPoints": float(value)}
            for feature, value in zip(BASELINE + PBP, coefficients[1:], strict=True)
        ),
        key=lambda row: abs(row["standardizedPoints"]),
        reverse=True,
    )
    return {
        "studyVersion": STUDY_VERSION,
        "question": "Which pregame PBP descriptors are associated with the opening home spread?",
        "interpretation": (
            "Associational reconstruction of a consensus market number; coefficients do not "
            "prove sportsbook intent or causation."
        ),
        "baselineFeatures": list(BASELINE),
        "pbpFeatures": list(PBP),
        "folds": folds,
        "fullSampleStandardizedAssociations": associations,
        "decisionEffect": "none",
    }
