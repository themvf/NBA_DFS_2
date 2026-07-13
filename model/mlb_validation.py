"""Leakage-safe chronological partition helpers for MLB model artifacts."""

from __future__ import annotations

import numpy as np
import pandas as pd


def chronological_date_holdout(data: pd.DataFrame, test_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split on complete game dates so a date never appears on both sides."""
    ordered = data.sort_values(["game_date", "id"]).copy()
    normalized_dates = pd.to_datetime(ordered["game_date"]).dt.normalize()
    dates = np.asarray(sorted(normalized_dates.unique()))
    if len(dates) < 2:
        raise ValueError("at least two game dates are required for a chronological holdout")
    split_index = min(max(1, int(len(dates) * (1 - test_fraction))), len(dates) - 1)
    first_test_date = dates[split_index]
    train = ordered.loc[normalized_dates < first_test_date].reset_index(drop=True)
    test = ordered.loc[normalized_dates >= first_test_date].reset_index(drop=True)
    return train, test


def expanding_date_folds(data: pd.DataFrame, *, folds: int) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    """Return expanding folds whose test dates are never present in training."""
    ordered = data.sort_values(["game_date", "id"]).copy()
    normalized_dates = pd.to_datetime(ordered["game_date"]).dt.normalize()
    dates = np.asarray(sorted(normalized_dates.unique()))
    initial_date_count = max(1, len(dates) // 2)
    remaining_dates = dates[initial_date_count:]
    if len(remaining_dates) < folds:
        raise ValueError("insufficient chronological date population")

    result = []
    for fold_dates in np.array_split(remaining_dates, folds):
        if len(fold_dates) == 0:
            continue
        first_test_date = fold_dates[0]
        train = ordered.loc[normalized_dates < first_test_date].reset_index(drop=True)
        test = ordered.loc[normalized_dates.isin(fold_dates)].reset_index(drop=True)
        result.append((train, test))
    return result
