#!/usr/bin/env python3
"""Offline NBA feature exploration with XGBoost.

This script is for research, not production inference.

It answers three questions:
1. Do odds and props add signal beyond the current NBA live projection?
2. Are they more useful for direct FPTS prediction or residual correction?
3. Which features matter most on the holdout slates?

By default it trains on older NBA result slates and holds out the most recent
two slate dates. It compares four feature sets:
  - baseline
  - baseline + props
  - baseline + odds
  - full

Targets:
  - actual_fpts
  - residual_fpts = actual_fpts - live_proj
  - hit_5x classification
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, roc_auc_score

try:
    from xgboost import XGBClassifier, XGBRegressor

    HAS_XGBOOST = True
except Exception:  # pragma: no cover - optional dependency
    XGBClassifier = None
    XGBRegressor = None
    HAS_XGBOOST = False

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from config import DATA_DIR, load_config
from db.database import DatabaseManager

MODEL_VERSION = "nba_feature_explorer_v2"
DEFAULT_OUTPUT = DATA_DIR / "reports" / "nba_xgboost_exploration.json"

BASELINE_FEATURES = [
    "salary",
    "salary_k",
    "live_proj",
    "our_proj",
    "linestar_proj",
    "avg_fpts_dk",
    "proj_floor",
    "proj_ceiling",
    "boom_rate",
    "primary_position",
    "is_home",
]

PROP_FEATURES = [
    "prop_pts",
    "prop_reb",
    "prop_ast",
    "prop_blk",
    "prop_stl",
    "prop_pra",
    "prop_fpts_proxy",
    "prop_count",
    "pts_prop_minus_live",
    "pra_prop_minus_live",
]

ODDS_FEATURES = [
    "vegas_total",
    "team_implied_total",
    "home_win_prob",
    "abs_spread",
    "vegas_bucket",
    "implied_bucket",
]


@dataclass
class RegressionSummary:
    n_train: int
    n_test: int
    mae: float | None
    rmse: float | None
    corr: float | None
    comparison_n: int | None = None
    comparison_mae: float | None = None
    comparison_rmse: float | None = None
    comparison_corr: float | None = None
    baseline_mae: float | None = None
    baseline_rmse: float | None = None
    baseline_corr: float | None = None
    mae_delta_vs_baseline: float | None = None


@dataclass
class ClassificationSummary:
    n_train: int
    n_test: int
    auc: float | None
    baseline_auc: float | None = None
    auc_delta_vs_baseline: float | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explore NBA odds/prop features with XGBoost.")
    parser.add_argument("--backend", choices=["auto", "xgboost", "forest"], default="auto")
    parser.add_argument("--holdout-slates", type=int, default=2, help="Number of most-recent slate dates to hold out.")
    parser.add_argument(
        "--min-train-slates",
        type=int,
        default=3,
        help="Minimum number of prior imported slates required before a slate is eligible for walk-forward evaluation.",
    )
    parser.add_argument(
        "--min-cohort-rows",
        type=int,
        default=25,
        help="Minimum comparable rows before a cohort is marked as meaningful in the report.",
    )
    parser.add_argument("--min-date", type=str, default=None, help="Optional YYYY-MM-DD lower bound.")
    parser.add_argument("--max-date", type=str, default=None, help="Optional YYYY-MM-DD upper bound.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--top-features", type=int, default=15)
    return parser.parse_args()


def choose_backend(requested: str) -> str:
    if requested == "xgboost":
        if not HAS_XGBOOST:
            raise RuntimeError("xgboost is not installed in this environment.")
        return "xgboost"
    if requested == "forest":
        return "forest"
    return "xgboost" if HAS_XGBOOST else "forest"


def ml_to_prob(ml: int | float | None) -> float | None:
    if ml is None or not math.isfinite(float(ml)):
        return None
    ml = float(ml)
    return 100 / (ml + 100) if ml >= 0 else abs(ml) / (abs(ml) + 100)


def compute_team_implied_total(
    vegas_total: float | None,
    home_ml: float | None,
    away_ml: float | None,
    is_home: bool,
) -> float | None:
    if vegas_total is None:
        return None
    if home_ml is None or away_ml is None:
        return vegas_total / 2.0
    raw_home = ml_to_prob(home_ml)
    raw_away = ml_to_prob(away_ml)
    if raw_home is None or raw_away is None:
        return vegas_total / 2.0
    clean_home = raw_home / (raw_home + raw_away)
    implied_spread = max(-15.0, min(15.0, (clean_home - 0.5) / 0.025))
    home_implied = vegas_total / 2.0 + implied_spread / 2.0
    return home_implied if is_home else vegas_total - home_implied


def primary_position(eligible_positions: str | None) -> str:
    if not eligible_positions:
        return "UNK"
    for token in str(eligible_positions).split("/"):
        token = token.strip().upper()
        if token in {"PG", "SG", "SF", "PF", "C"}:
            return token
    return str(eligible_positions).split("/")[0].strip().upper() or "UNK"


def bucket_numeric(value: float | None, boundaries: list[float], labels: list[str]) -> str:
    if value is None or not math.isfinite(value):
        return "missing"
    for index, upper in enumerate(boundaries):
        if value < upper:
            return labels[index]
    return labels[-1]


def load_dataset(db: DatabaseManager, min_date: str | None, max_date: str | None) -> pd.DataFrame:
    clauses = [
        "ds.sport = 'nba'",
        "dp.actual_fpts IS NOT NULL",
        "COALESCE(dp.is_out, false) = false",
        "COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) IS NOT NULL",
    ]
    params: list[Any] = []
    if min_date:
        clauses.append("ds.slate_date >= %s")
        params.append(min_date)
    if max_date:
        clauses.append("ds.slate_date <= %s")
        params.append(max_date)

    sql = f"""
        SELECT
            ds.slate_date::text AS slate_date,
            ds.id AS slate_id,
            dp.id AS player_row_id,
            dp.dk_player_id,
            dp.name,
            dp.team_abbrev,
            dp.team_id,
            dp.eligible_positions,
            dp.salary,
            dp.avg_fpts_dk,
            dp.linestar_proj,
            dp.our_proj,
            dp.live_proj,
            dp.proj_floor,
            dp.proj_ceiling,
            dp.boom_rate,
            dp.proj_own_pct,
            dp.our_own_pct,
            dp.live_own_pct,
            dp.our_leverage,
            dp.live_leverage,
            dp.prop_pts,
            dp.prop_reb,
            dp.prop_ast,
            dp.prop_blk,
            dp.prop_stl,
            dp.actual_fpts,
            m.vegas_total,
            m.home_ml,
            m.away_ml,
            m.home_spread,
            m.home_team_id
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        LEFT JOIN nba_matchups m ON m.id = dp.matchup_id
        WHERE {" AND ".join(clauses)}
        ORDER BY ds.slate_date ASC, ds.id ASC, dp.id ASC
    """
    rows = db.execute(sql, tuple(params))
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    numeric_cols = [
        "salary",
        "avg_fpts_dk",
        "linestar_proj",
        "our_proj",
        "live_proj",
        "proj_floor",
        "proj_ceiling",
        "boom_rate",
        "proj_own_pct",
        "our_own_pct",
        "live_own_pct",
        "our_leverage",
        "live_leverage",
        "prop_pts",
        "prop_reb",
        "prop_ast",
        "prop_blk",
        "prop_stl",
        "actual_fpts",
        "vegas_total",
        "home_ml",
        "away_ml",
        "home_spread",
        "team_id",
        "home_team_id",
    ]
    for col in numeric_cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

    frame["salary_k"] = frame["salary"] / 1000.0
    frame["is_home"] = (frame["team_id"].notna() & frame["home_team_id"].notna() & (frame["team_id"] == frame["home_team_id"])).astype(int)
    frame["home_win_prob"] = frame["home_ml"].apply(ml_to_prob)
    frame["team_implied_total"] = [
        compute_team_implied_total(
            None if pd.isna(vegas_total) else float(vegas_total),
            None if pd.isna(home_ml) else float(home_ml),
            None if pd.isna(away_ml) else float(away_ml),
            bool(is_home),
        )
        for vegas_total, home_ml, away_ml, is_home in zip(
            frame["vegas_total"],
            frame["home_ml"],
            frame["away_ml"],
            frame["is_home"],
            strict=False,
        )
    ]
    frame["abs_spread"] = frame["home_spread"].abs()
    frame["primary_position"] = frame["eligible_positions"].map(primary_position)
    frame["prop_pra"] = frame[["prop_pts", "prop_reb", "prop_ast"]].fillna(0).sum(axis=1)
    frame.loc[
        frame[["prop_pts", "prop_reb", "prop_ast"]].isna().all(axis=1),
        "prop_pra",
    ] = np.nan
    frame["prop_fpts_proxy"] = (
        frame["prop_pts"].fillna(0) * 1.0
        + frame["prop_reb"].fillna(0) * 1.25
        + frame["prop_ast"].fillna(0) * 1.5
        + frame["prop_blk"].fillna(0) * 2.0
        + frame["prop_stl"].fillna(0) * 2.0
    )
    frame.loc[
        frame[["prop_pts", "prop_reb", "prop_ast", "prop_blk", "prop_stl"]].isna().all(axis=1),
        "prop_fpts_proxy",
    ] = np.nan
    frame["prop_count"] = frame[["prop_pts", "prop_reb", "prop_ast", "prop_blk", "prop_stl"]].notna().sum(axis=1)
    frame["pts_prop_minus_live"] = frame["prop_pts"] - frame["live_proj"]
    frame["pra_prop_minus_live"] = frame["prop_pra"] - frame["live_proj"]
    frame["actual_fpts"] = frame["actual_fpts"].astype(float)
    frame["residual_fpts"] = frame["actual_fpts"] - frame["live_proj"]
    frame["hit_5x"] = (frame["actual_fpts"] >= (frame["salary"] / 200.0)).astype(int)
    frame["vegas_bucket"] = frame["vegas_total"].apply(
        lambda value: bucket_numeric(value, [225, 232, 240], ["sub_225", "225_232", "232_240", "240_plus"])
    )
    frame["implied_bucket"] = frame["team_implied_total"].apply(
        lambda value: bucket_numeric(value, [112, 117, 122], ["sub_112", "112_117", "117_122", "122_plus"])
    )
    return frame


def split_train_test(frame: pd.DataFrame, holdout_slates: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    slate_dates = sorted(frame["slate_date"].dropna().unique().tolist())
    if len(slate_dates) <= holdout_slates:
        raise RuntimeError(f"Need more than {holdout_slates} slate dates; found {len(slate_dates)}.")
    holdout_dates = slate_dates[-holdout_slates:]
    train = frame[~frame["slate_date"].isin(holdout_dates)].copy()
    test = frame[frame["slate_date"].isin(holdout_dates)].copy()
    if train.empty or test.empty:
        raise RuntimeError("Train/test split is empty after applying holdout logic.")
    return train, test, holdout_dates


def prepare_features(
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    combined = pd.concat([train[features], test[features]], axis=0, ignore_index=True)
    encoded = pd.get_dummies(combined, columns=[col for col in features if combined[col].dtype == "object"], dummy_na=True)
    x_train = encoded.iloc[: len(train)].copy()
    x_test = encoded.iloc[len(train) :].copy()
    medians = x_train.median(numeric_only=True)
    x_train = x_train.fillna(medians).fillna(0.0)
    x_test = x_test.fillna(medians).fillna(0.0)
    return x_train.astype(float), x_test.astype(float)


def build_regressor(backend: str):
    if backend == "xgboost":
        return XGBRegressor(
            n_estimators=400,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=2.0,
            random_state=42,
            objective="reg:squarederror",
            n_jobs=4,
        )
    return RandomForestRegressor(
        n_estimators=300,
        max_depth=10,
        min_samples_leaf=4,
        random_state=42,
        n_jobs=-1,
    )


def build_classifier(backend: str):
    if backend == "xgboost":
        return XGBClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=2.0,
            random_state=42,
            objective="binary:logistic",
            eval_metric="logloss",
            n_jobs=4,
        )
    return RandomForestClassifier(
        n_estimators=300,
        max_depth=10,
        min_samples_leaf=4,
        random_state=42,
        n_jobs=-1,
    )


def regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> tuple[float | None, float | None, float | None]:
    y_true_arr = np.asarray(y_true, dtype=float)
    y_pred_arr = np.asarray(y_pred, dtype=float)
    mask = np.isfinite(y_true_arr) & np.isfinite(y_pred_arr)
    if mask.sum() == 0:
        return None, None, None
    y_true_arr = y_true_arr[mask]
    y_pred_arr = y_pred_arr[mask]
    mae = float(mean_absolute_error(y_true_arr, y_pred_arr))
    rmse = float(math.sqrt(mean_squared_error(y_true_arr, y_pred_arr)))
    corr = None
    if len(y_true_arr) > 1:
        y_true_std = float(np.std(y_true_arr))
        y_pred_std = float(np.std(y_pred_arr))
        if y_true_std > 0 and y_pred_std > 0:
            corr = float(np.corrcoef(y_true_arr, y_pred_arr)[0, 1])
    return mae, rmse, corr


def feature_importance(model: Any, columns: list[str], top_n: int) -> list[dict[str, Any]]:
    raw = getattr(model, "feature_importances_", None)
    if raw is None:
        return []
    pairs = [
        {"feature": column, "importance": float(importance)}
        for column, importance in zip(columns, raw, strict=False)
        if float(importance) > 0
    ]
    pairs.sort(key=lambda entry: (-entry["importance"], entry["feature"]))
    return pairs[:top_n]


def scenario_feature_sets() -> dict[str, list[str]]:
    return {
        "baseline": BASELINE_FEATURES,
        "baseline_plus_props": BASELINE_FEATURES + PROP_FEATURES,
        "baseline_plus_odds": BASELINE_FEATURES + ODDS_FEATURES,
        "full": BASELINE_FEATURES + PROP_FEATURES + ODDS_FEATURES,
    }


def regression_evaluation(y_true: pd.Series | np.ndarray, y_pred: pd.Series | np.ndarray) -> dict[str, Any]:
    y_true_arr = np.asarray(y_true, dtype=float)
    y_pred_arr = np.asarray(y_pred, dtype=float)
    mask = np.isfinite(y_true_arr) & np.isfinite(y_pred_arr)
    if mask.sum() == 0:
        return {"n": 0, "mae": None, "rmse": None, "corr": None}
    mae, rmse, corr = regression_metrics(y_true_arr[mask], y_pred_arr[mask])
    return {"n": int(mask.sum()), "mae": mae, "rmse": rmse, "corr": corr}


def classification_evaluation(y_true: pd.Series | np.ndarray, y_score: pd.Series | np.ndarray) -> dict[str, Any]:
    y_true_arr = np.asarray(y_true, dtype=float)
    y_score_arr = np.asarray(y_score, dtype=float)
    mask = np.isfinite(y_true_arr) & np.isfinite(y_score_arr)
    if mask.sum() == 0:
        return {"n": 0, "auc": None}
    y_true_arr = y_true_arr[mask].astype(int)
    y_score_arr = y_score_arr[mask]
    if len(np.unique(y_true_arr)) < 2:
        return {"n": int(mask.sum()), "auc": None}
    return {"n": int(mask.sum()), "auc": float(roc_auc_score(y_true_arr, y_score_arr))}


def regression_comparison(
    y_true: pd.Series | np.ndarray,
    scenario_pred: pd.Series | np.ndarray,
    baseline_pred: pd.Series | np.ndarray,
) -> dict[str, Any]:
    y_true_arr = np.asarray(y_true, dtype=float)
    scenario_arr = np.asarray(scenario_pred, dtype=float)
    baseline_arr = np.asarray(baseline_pred, dtype=float)
    mask = np.isfinite(y_true_arr) & np.isfinite(scenario_arr) & np.isfinite(baseline_arr)
    if mask.sum() == 0:
        return {"n": 0, "baselineMae": None, "baselineRmse": None, "baselineCorr": None, "maeDelta": None}
    scenario_metrics = regression_evaluation(y_true_arr[mask], scenario_arr[mask])
    baseline_metrics = regression_evaluation(y_true_arr[mask], baseline_arr[mask])
    return {
        "n": int(mask.sum()),
        "baselineMae": baseline_metrics["mae"],
        "baselineRmse": baseline_metrics["rmse"],
        "baselineCorr": baseline_metrics["corr"],
        "maeDelta": None if scenario_metrics["mae"] is None or baseline_metrics["mae"] is None else float(baseline_metrics["mae"]) - float(scenario_metrics["mae"]),
    }


def classification_comparison(
    y_true: pd.Series | np.ndarray,
    scenario_score: pd.Series | np.ndarray,
    baseline_score: pd.Series | np.ndarray,
) -> dict[str, Any]:
    y_true_arr = np.asarray(y_true, dtype=float)
    scenario_arr = np.asarray(scenario_score, dtype=float)
    baseline_arr = np.asarray(baseline_score, dtype=float)
    mask = np.isfinite(y_true_arr) & np.isfinite(scenario_arr) & np.isfinite(baseline_arr)
    if mask.sum() == 0:
        return {"n": 0, "baselineAuc": None, "aucDelta": None}
    y_true_arr = y_true_arr[mask].astype(int)
    if len(np.unique(y_true_arr)) < 2:
        return {"n": int(mask.sum()), "baselineAuc": None, "aucDelta": None}
    scenario_metrics = classification_evaluation(y_true_arr, scenario_arr[mask])
    baseline_metrics = classification_evaluation(y_true_arr, baseline_arr[mask])
    return {
        "n": int(mask.sum()),
        "baselineAuc": baseline_metrics["auc"],
        "aucDelta": None if scenario_metrics["auc"] is None or baseline_metrics["auc"] is None else float(scenario_metrics["auc"]) - float(baseline_metrics["auc"]),
    }


def summarize_slate_coverage(frame: pd.DataFrame) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    grouped = frame.groupby("slate_date", dropna=True)
    for slate_date, group in grouped:
        rows = int(len(group))
        live_count = int(np.isfinite(group["live_proj"].astype(float)).sum())
        our_count = int(np.isfinite(group["our_proj"].astype(float)).sum())
        linestar_count = int(np.isfinite(group["linestar_proj"].astype(float)).sum())
        prop_count = int(group["prop_count"].gt(0).sum())
        odds_count = int(group["vegas_total"].notna().sum())
        summaries.append(
            {
                "slateDate": str(slate_date),
                "rows": rows,
                "liveProjRows": live_count,
                "liveProjPct": round(live_count / rows * 100, 2) if rows else 0.0,
                "ourProjRows": our_count,
                "ourProjPct": round(our_count / rows * 100, 2) if rows else 0.0,
                "linestarProjRows": linestar_count,
                "linestarProjPct": round(linestar_count / rows * 100, 2) if rows else 0.0,
                "propRows": prop_count,
                "propPct": round(prop_count / rows * 100, 2) if rows else 0.0,
                "oddsRows": odds_count,
                "oddsPct": round(odds_count / rows * 100, 2) if rows else 0.0,
            }
        )
    return summaries


def slate_coverage_warnings(coverage: dict[str, Any], min_cohort_rows: int) -> list[str]:
    warnings: list[str] = []
    slate_date = coverage["slateDate"]
    if coverage["liveProjRows"] == 0:
        warnings.append(
            f"Imported slate {slate_date} has 0 live projections; direct comparisons vs live are unavailable."
        )
    elif coverage["liveProjRows"] < min_cohort_rows:
        warnings.append(
            f"Imported slate {slate_date} has only {coverage['liveProjRows']} live-projection rows; live cohort is below the minimum row threshold."
        )
    if coverage["propRows"] == 0:
        warnings.append(
            f"Imported slate {slate_date} has 0 prop rows; prop uplift cannot be measured on this slate."
        )
    elif coverage["propRows"] < min_cohort_rows:
        warnings.append(
            f"Imported slate {slate_date} has only {coverage['propRows']} prop rows; prop cohort is below the minimum row threshold."
        )
    if coverage["oddsRows"] == 0:
        warnings.append(
            f"Imported slate {slate_date} has 0 odds rows; odds uplift cannot be measured on this slate."
        )
    elif coverage["oddsRows"] < min_cohort_rows:
        warnings.append(
            f"Imported slate {slate_date} has only {coverage['oddsRows']} odds rows; odds cohort is below the minimum row threshold."
        )
    return warnings


def holdout_warnings(test: pd.DataFrame, holdout_dates: list[str]) -> list[str]:
    warnings: list[str] = []
    coverage = {entry["slateDate"]: entry for entry in summarize_slate_coverage(test)}
    for slate_date in holdout_dates:
        entry = coverage.get(slate_date)
        if not entry:
            warnings.append(f"Holdout date {slate_date} has no rows after filtering.")
            continue
        warnings.extend(slate_coverage_warnings(entry, min_cohort_rows=1))
    return warnings


def fit_regression_predictions(
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: list[str],
    target: str,
    backend: str,
) -> dict[str, Any]:
    train_filtered = train[np.isfinite(train[target].astype(float))].copy()
    test_filtered = test[np.isfinite(test[target].astype(float))].copy()
    prediction_series = pd.Series(np.nan, index=test.index, dtype=float)
    if train_filtered.empty or test_filtered.empty:
        return {
            "predictions": prediction_series,
            "n_train": len(train_filtered),
            "n_test": len(test_filtered),
        }

    x_train, x_test = prepare_features(train_filtered, test_filtered, features)
    model = build_regressor(backend)
    model.fit(x_train, train_filtered[target].astype(float))
    prediction_series.loc[test_filtered.index] = model.predict(x_test)
    return {
        "predictions": prediction_series,
        "n_train": len(train_filtered),
        "n_test": len(test_filtered),
    }


def fit_classification_scores(
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: list[str],
    backend: str,
) -> dict[str, Any]:
    train_filtered = train[np.isfinite(train["live_proj"].astype(float))].copy()
    test_filtered = test[np.isfinite(test["live_proj"].astype(float))].copy()
    score_series = pd.Series(np.nan, index=test.index, dtype=float)
    y_train = train_filtered["hit_5x"].astype(int) if not train_filtered.empty else pd.Series(dtype=int)
    y_test = test_filtered["hit_5x"].astype(int) if not test_filtered.empty else pd.Series(dtype=int)
    if train_filtered.empty or test_filtered.empty or y_train.nunique() < 2 or y_test.nunique() < 2:
        return {
            "scores": score_series,
            "n_train": len(train_filtered),
            "n_test": len(test_filtered),
        }

    x_train, x_test = prepare_features(train_filtered, test_filtered, features)
    model = build_classifier(backend)
    model.fit(x_train, y_train)
    score_series.loc[test_filtered.index] = model.predict_proba(x_test)[:, 1]
    return {
        "scores": score_series,
        "n_train": len(train_filtered),
        "n_test": len(test_filtered),
    }


def summarize_regression_cohort(
    test: pd.DataFrame,
    predictions: dict[str, pd.Series],
    mask: pd.Series,
    baseline_predictions: dict[str, pd.Series],
    min_cohort_rows: int,
) -> dict[str, Any]:
    index = test.index[mask.reindex(test.index).fillna(False).astype(bool)]
    result: dict[str, Any] = {
        "rowCount": int(len(index)),
        "meetsMinimumRows": int(len(index)) >= min_cohort_rows,
        "baselines": {},
        "scenarios": {},
    }
    if len(index) == 0:
        return result

    y_true = test.loc[index, "actual_fpts"].astype(float)
    for baseline_name, baseline_pred in baseline_predictions.items():
        metrics = regression_evaluation(y_true, baseline_pred.reindex(index).to_numpy(dtype=float))
        result["baselines"][baseline_name] = metrics

    for scenario_name, scenario_pred in predictions.items():
        metrics = regression_evaluation(y_true, scenario_pred.reindex(index).to_numpy(dtype=float))
        if baseline_predictions:
            metrics["baselineComparisons"] = {
                baseline_name: regression_comparison(
                    y_true,
                    scenario_pred.reindex(index).to_numpy(dtype=float),
                    baseline_pred.reindex(index).to_numpy(dtype=float),
                )
                for baseline_name, baseline_pred in baseline_predictions.items()
            }
        result["scenarios"][scenario_name] = metrics
    return result


def summarize_classification_cohort(
    test: pd.DataFrame,
    scores: dict[str, pd.Series],
    mask: pd.Series,
    baseline_scores: dict[str, pd.Series],
    min_cohort_rows: int,
) -> dict[str, Any]:
    index = test.index[mask.reindex(test.index).fillna(False).astype(bool)]
    result: dict[str, Any] = {
        "rowCount": int(len(index)),
        "meetsMinimumRows": int(len(index)) >= min_cohort_rows,
        "baselines": {},
        "scenarios": {},
    }
    if len(index) == 0:
        return result

    y_true = test.loc[index, "hit_5x"].astype(int)
    for baseline_name, baseline_score in baseline_scores.items():
        metrics = classification_evaluation(y_true, baseline_score.reindex(index).to_numpy(dtype=float))
        result["baselines"][baseline_name] = metrics

    for scenario_name, scenario_score in scores.items():
        metrics = classification_evaluation(y_true, scenario_score.reindex(index).to_numpy(dtype=float))
        if baseline_scores:
            metrics["baselineComparisons"] = {
                baseline_name: classification_comparison(
                    y_true,
                    scenario_score.reindex(index).to_numpy(dtype=float),
                    baseline_score.reindex(index).to_numpy(dtype=float),
                )
                for baseline_name, baseline_score in baseline_scores.items()
            }
        result["scenarios"][scenario_name] = metrics
    return result


def init_regression_aggregate(scenario_names: list[str], baseline_names: list[str]) -> dict[str, Any]:
    return {
        "rowCount": 0,
        "scenarios": {name: {"y_true": [], "y_pred": []} for name in scenario_names},
        "baselines": {name: {"y_true": [], "y_pred": []} for name in baseline_names},
        "scenarioComparisons": {
            scenario_name: {
                baseline_name: {"y_true": [], "scenario_pred": [], "baseline_pred": []}
                for baseline_name in baseline_names
            }
            for scenario_name in scenario_names
        },
    }


def append_regression_aggregate(
    aggregate: dict[str, Any],
    y_true: pd.Series,
    predictions: dict[str, pd.Series],
    baselines: dict[str, pd.Series],
) -> None:
    aggregate["rowCount"] += int(len(y_true))
    y_true_arr = np.asarray(y_true, dtype=float)
    for name, pred in predictions.items():
        pred_arr = np.asarray(pred, dtype=float)
        mask = np.isfinite(y_true_arr) & np.isfinite(pred_arr)
        if mask.any():
            aggregate["scenarios"][name]["y_true"].extend(y_true_arr[mask].tolist())
            aggregate["scenarios"][name]["y_pred"].extend(pred_arr[mask].tolist())
    for name, pred in baselines.items():
        pred_arr = np.asarray(pred, dtype=float)
        mask = np.isfinite(y_true_arr) & np.isfinite(pred_arr)
        if mask.any():
            aggregate["baselines"][name]["y_true"].extend(y_true_arr[mask].tolist())
            aggregate["baselines"][name]["y_pred"].extend(pred_arr[mask].tolist())
    for scenario_name, scenario_pred in predictions.items():
        scenario_arr = np.asarray(scenario_pred, dtype=float)
        for baseline_name, baseline_pred in baselines.items():
            baseline_arr = np.asarray(baseline_pred, dtype=float)
            mask = np.isfinite(y_true_arr) & np.isfinite(scenario_arr) & np.isfinite(baseline_arr)
            if mask.any():
                aggregate["scenarioComparisons"][scenario_name][baseline_name]["y_true"].extend(y_true_arr[mask].tolist())
                aggregate["scenarioComparisons"][scenario_name][baseline_name]["scenario_pred"].extend(scenario_arr[mask].tolist())
                aggregate["scenarioComparisons"][scenario_name][baseline_name]["baseline_pred"].extend(baseline_arr[mask].tolist())


def finalize_regression_aggregate(aggregate: dict[str, Any], min_cohort_rows: int) -> dict[str, Any]:
    result: dict[str, Any] = {
        "rowCount": int(aggregate["rowCount"]),
        "meetsMinimumRows": int(aggregate["rowCount"]) >= min_cohort_rows,
        "baselines": {},
        "scenarios": {},
    }
    for baseline_name, payload in aggregate["baselines"].items():
        metrics = regression_evaluation(payload["y_true"], payload["y_pred"])
        result["baselines"][baseline_name] = metrics
    for scenario_name, payload in aggregate["scenarios"].items():
        metrics = regression_evaluation(payload["y_true"], payload["y_pred"])
        if aggregate["baselines"]:
            metrics["baselineComparisons"] = {}
            for baseline_name, comparison_payload in aggregate["scenarioComparisons"][scenario_name].items():
                metrics["baselineComparisons"][baseline_name] = regression_comparison(
                    comparison_payload["y_true"],
                    comparison_payload["scenario_pred"],
                    comparison_payload["baseline_pred"],
                )
        result["scenarios"][scenario_name] = metrics
    return result


def init_classification_aggregate(scenario_names: list[str], baseline_names: list[str]) -> dict[str, Any]:
    return {
        "rowCount": 0,
        "scenarios": {name: {"y_true": [], "y_score": []} for name in scenario_names},
        "baselines": {name: {"y_true": [], "y_score": []} for name in baseline_names},
        "scenarioComparisons": {
            scenario_name: {
                baseline_name: {"y_true": [], "scenario_score": [], "baseline_score": []}
                for baseline_name in baseline_names
            }
            for scenario_name in scenario_names
        },
    }


def append_classification_aggregate(
    aggregate: dict[str, Any],
    y_true: pd.Series,
    scores: dict[str, pd.Series],
    baselines: dict[str, pd.Series],
) -> None:
    aggregate["rowCount"] += int(len(y_true))
    y_true_arr = np.asarray(y_true, dtype=float)
    for name, score in scores.items():
        score_arr = np.asarray(score, dtype=float)
        mask = np.isfinite(y_true_arr) & np.isfinite(score_arr)
        if mask.any():
            aggregate["scenarios"][name]["y_true"].extend(y_true_arr[mask].tolist())
            aggregate["scenarios"][name]["y_score"].extend(score_arr[mask].tolist())
    for name, score in baselines.items():
        score_arr = np.asarray(score, dtype=float)
        mask = np.isfinite(y_true_arr) & np.isfinite(score_arr)
        if mask.any():
            aggregate["baselines"][name]["y_true"].extend(y_true_arr[mask].tolist())
            aggregate["baselines"][name]["y_score"].extend(score_arr[mask].tolist())
    for scenario_name, scenario_score in scores.items():
        scenario_arr = np.asarray(scenario_score, dtype=float)
        for baseline_name, baseline_score in baselines.items():
            baseline_arr = np.asarray(baseline_score, dtype=float)
            mask = np.isfinite(y_true_arr) & np.isfinite(scenario_arr) & np.isfinite(baseline_arr)
            if mask.any():
                aggregate["scenarioComparisons"][scenario_name][baseline_name]["y_true"].extend(y_true_arr[mask].tolist())
                aggregate["scenarioComparisons"][scenario_name][baseline_name]["scenario_score"].extend(scenario_arr[mask].tolist())
                aggregate["scenarioComparisons"][scenario_name][baseline_name]["baseline_score"].extend(baseline_arr[mask].tolist())


def finalize_classification_aggregate(aggregate: dict[str, Any], min_cohort_rows: int) -> dict[str, Any]:
    result: dict[str, Any] = {
        "rowCount": int(aggregate["rowCount"]),
        "meetsMinimumRows": int(aggregate["rowCount"]) >= min_cohort_rows,
        "baselines": {},
        "scenarios": {},
    }
    for baseline_name, payload in aggregate["baselines"].items():
        metrics = classification_evaluation(payload["y_true"], payload["y_score"])
        result["baselines"][baseline_name] = metrics
    for scenario_name, payload in aggregate["scenarios"].items():
        metrics = classification_evaluation(payload["y_true"], payload["y_score"])
        if aggregate["baselines"]:
            metrics["baselineComparisons"] = {}
            for baseline_name, comparison_payload in aggregate["scenarioComparisons"][scenario_name].items():
                metrics["baselineComparisons"][baseline_name] = classification_comparison(
                    comparison_payload["y_true"],
                    comparison_payload["scenario_score"],
                    comparison_payload["baseline_score"],
                )
        result["scenarios"][scenario_name] = metrics
    return result


def evaluate_regression_scenario(
    name: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: list[str],
    target: str,
    backend: str,
    baseline_pred: pd.Series | np.ndarray,
    top_n: int,
) -> dict[str, Any]:
    train_filtered = train[np.isfinite(train[target].astype(float))].copy()
    test_filtered = test[np.isfinite(test[target].astype(float))].copy()
    x_train, x_test = prepare_features(train_filtered, test_filtered, features)
    y_train = train_filtered[target].astype(float)
    y_test = test_filtered[target].astype(float)
    model = build_regressor(backend)
    model.fit(x_train, y_train)
    pred = model.predict(x_test)

    mae, rmse, corr = regression_metrics(y_test, pred)
    baseline_series = pd.Series(np.asarray(baseline_pred, dtype=float), index=test.index)
    baseline_eval = baseline_series.loc[test_filtered.index].to_numpy(dtype=float)
    comparison_mask = np.isfinite(baseline_eval)
    comparison_n = int(comparison_mask.sum())

    comparison_mae = None
    comparison_rmse = None
    comparison_corr = None
    baseline_mae = None
    baseline_rmse = None
    baseline_corr = None
    if comparison_mask.any():
        comparison_mae, comparison_rmse, comparison_corr = regression_metrics(
            y_test.iloc[comparison_mask],
            pred[comparison_mask],
        )
        baseline_mae, baseline_rmse, baseline_corr = regression_metrics(
            y_test.iloc[comparison_mask],
            baseline_eval[comparison_mask],
        )

    summary = RegressionSummary(
        n_train=len(train_filtered),
        n_test=len(test_filtered),
        mae=mae,
        rmse=rmse,
        corr=corr,
        comparison_n=comparison_n,
        comparison_mae=comparison_mae,
        comparison_rmse=comparison_rmse,
        comparison_corr=comparison_corr,
        baseline_mae=baseline_mae,
        baseline_rmse=baseline_rmse,
        baseline_corr=baseline_corr,
        mae_delta_vs_baseline=(
            None if comparison_mae is None or baseline_mae is None else baseline_mae - comparison_mae
        ),
    )
    return {
        "scenario": name,
        "features": features,
        "summary": asdict(summary),
        "top_features": feature_importance(model, list(x_train.columns), top_n),
    }


def evaluate_classification_scenario(
    name: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: list[str],
    backend: str,
    baseline_scores: pd.Series | np.ndarray,
    top_n: int,
) -> dict[str, Any]:
    train_filtered = train[np.isfinite(train["live_proj"].astype(float))].copy()
    test_filtered = test[np.isfinite(test["live_proj"].astype(float))].copy()
    y_train = train_filtered["hit_5x"].astype(int)
    y_test = test_filtered["hit_5x"].astype(int)
    if y_train.nunique() < 2 or y_test.nunique() < 2:
        summary = ClassificationSummary(n_train=len(train_filtered), n_test=len(test_filtered), auc=None, baseline_auc=None, auc_delta_vs_baseline=None)
        return {
            "scenario": name,
            "features": features,
            "summary": asdict(summary),
            "top_features": [],
        }

    x_train, x_test = prepare_features(train_filtered, test_filtered, features)
    model = build_classifier(backend)
    model.fit(x_train, y_train)
    pred_scores = model.predict_proba(x_test)[:, 1]
    baseline_series = pd.Series(np.asarray(baseline_scores, dtype=float), index=test.index)
    baseline_arr = baseline_series.loc[test_filtered.index].to_numpy(dtype=float)
    mask = np.isfinite(baseline_arr)
    auc = float(roc_auc_score(y_test, pred_scores))
    baseline_auc = float(roc_auc_score(y_test[mask], baseline_arr[mask])) if mask.any() and y_test[mask].nunique() >= 2 else None
    summary = ClassificationSummary(
        n_train=len(train_filtered),
        n_test=len(test_filtered),
        auc=auc,
        baseline_auc=baseline_auc,
        auc_delta_vs_baseline=None if baseline_auc is None else auc - baseline_auc,
    )
    return {
        "scenario": name,
        "features": features,
        "summary": asdict(summary),
        "top_features": feature_importance(model, list(x_train.columns), top_n),
    }


def run_walk_forward_analysis(
    frame: pd.DataFrame,
    backend: str,
    min_train_slates: int,
    min_cohort_rows: int,
) -> dict[str, Any]:
    slate_dates = sorted(frame["slate_date"].dropna().unique().tolist())
    coverage_by_slate = {entry["slateDate"]: entry for entry in summarize_slate_coverage(frame)}
    scenarios = scenario_feature_sets()
    scenario_names = list(scenarios.keys())

    actual_aggregates = {
        "allRows": init_regression_aggregate(scenario_names, []),
        "liveProjectionRows": init_regression_aggregate(scenario_names, ["liveProjection", "baselineModel"]),
        "oddsRows": init_regression_aggregate(scenario_names, ["baselineModel"]),
        "propRows": init_regression_aggregate(scenario_names, ["baselineModel"]),
    }
    residual_aggregates = {
        "liveProjectionRows": init_regression_aggregate(scenario_names, ["liveProjection"]),
    }
    classification_aggregates = {
        "liveProjectionRows": init_classification_aggregate(scenario_names, ["liveProjection"]),
    }

    per_slate: list[dict[str, Any]] = []
    skipped_slates: list[dict[str, Any]] = []

    for slate_index, slate_date in enumerate(slate_dates):
        if slate_index < min_train_slates:
            skipped_slates.append(
                {
                    "slateDate": slate_date,
                    "reason": f"Requires at least {min_train_slates} prior imported slates before walk-forward evaluation.",
                    "trainSlateCount": slate_index,
                }
            )
            continue

        train_dates = slate_dates[:slate_index]
        train = frame[frame["slate_date"].isin(train_dates)].copy()
        test = frame[frame["slate_date"] == slate_date].copy()
        if train.empty or test.empty:
            skipped_slates.append(
                {
                    "slateDate": slate_date,
                    "reason": "Train or test split was empty for this imported slate.",
                    "trainSlateCount": slate_index,
                }
            )
            continue

        live_baseline = test["live_proj"].astype(float)
        hit5x_baseline = (test["live_proj"] / (test["salary"] / 200.0)).clip(lower=0.0, upper=2.0) / 2.0

        actual_predictions: dict[str, pd.Series] = {}
        residual_predictions: dict[str, pd.Series] = {}
        classification_scores: dict[str, pd.Series] = {}
        actual_train_rows: dict[str, int] = {}
        residual_train_rows: dict[str, int] = {}
        classification_train_rows: dict[str, int] = {}

        for scenario_name, features in scenarios.items():
            actual_fit = fit_regression_predictions(train, test, features, "actual_fpts", backend)
            actual_predictions[scenario_name] = actual_fit["predictions"]
            actual_train_rows[scenario_name] = int(actual_fit["n_train"])

            residual_fit = fit_regression_predictions(train, test, features, "residual_fpts", backend)
            residual_train_rows[scenario_name] = int(residual_fit["n_train"])
            residual_predictions[scenario_name] = live_baseline + residual_fit["predictions"]

            classification_fit = fit_classification_scores(train, test, features, backend)
            classification_train_rows[scenario_name] = int(classification_fit["n_train"])
            classification_scores[scenario_name] = classification_fit["scores"]

        all_mask = pd.Series(True, index=test.index)
        live_mask = test["live_proj"].notna()
        odds_mask = test["vegas_total"].notna()
        prop_mask = test["prop_count"].gt(0)

        slate_result = {
            "slateDate": slate_date,
            "trainSlateCount": slate_index,
            "trainRows": int(len(train)),
            "testRows": int(len(test)),
            "coverage": coverage_by_slate[slate_date],
            "warnings": slate_coverage_warnings(coverage_by_slate[slate_date], min_cohort_rows),
            "trainingRowsByTask": {
                "actualFptsRegression": actual_train_rows,
                "residualRegression": residual_train_rows,
                "hit5xClassification": classification_train_rows,
            },
            "actualFptsRegression": {
                "allRows": summarize_regression_cohort(
                    test,
                    actual_predictions,
                    all_mask,
                    {},
                    min_cohort_rows,
                ),
                "liveProjectionRows": summarize_regression_cohort(
                    test,
                    actual_predictions,
                    live_mask,
                    {
                        "liveProjection": live_baseline,
                        "baselineModel": actual_predictions["baseline"],
                    },
                    min_cohort_rows,
                ),
                "oddsRows": summarize_regression_cohort(
                    test,
                    actual_predictions,
                    odds_mask,
                    {"baselineModel": actual_predictions["baseline"]},
                    min_cohort_rows,
                ),
                "propRows": summarize_regression_cohort(
                    test,
                    actual_predictions,
                    prop_mask,
                    {"baselineModel": actual_predictions["baseline"]},
                    min_cohort_rows,
                ),
            },
            "residualRegression": {
                "liveProjectionRows": summarize_regression_cohort(
                    test,
                    residual_predictions,
                    live_mask,
                    {"liveProjection": live_baseline},
                    min_cohort_rows,
                ),
            },
            "hit5xClassification": {
                "liveProjectionRows": summarize_classification_cohort(
                    test,
                    classification_scores,
                    live_mask,
                    {"liveProjection": hit5x_baseline},
                    min_cohort_rows,
                ),
            },
        }
        per_slate.append(slate_result)

        all_index = test.index[all_mask]
        append_regression_aggregate(
            actual_aggregates["allRows"],
            test.loc[all_index, "actual_fpts"].astype(float),
            {name: pred.reindex(all_index) for name, pred in actual_predictions.items()},
            {},
        )

        live_index = test.index[live_mask]
        if len(live_index) > 0:
            append_regression_aggregate(
                actual_aggregates["liveProjectionRows"],
                test.loc[live_index, "actual_fpts"].astype(float),
                {name: pred.reindex(live_index) for name, pred in actual_predictions.items()},
                {
                    "liveProjection": live_baseline.reindex(live_index),
                    "baselineModel": actual_predictions["baseline"].reindex(live_index),
                },
            )
            append_regression_aggregate(
                residual_aggregates["liveProjectionRows"],
                test.loc[live_index, "actual_fpts"].astype(float),
                {name: pred.reindex(live_index) for name, pred in residual_predictions.items()},
                {"liveProjection": live_baseline.reindex(live_index)},
            )
            append_classification_aggregate(
                classification_aggregates["liveProjectionRows"],
                test.loc[live_index, "hit_5x"].astype(int),
                {name: score.reindex(live_index) for name, score in classification_scores.items()},
                {"liveProjection": hit5x_baseline.reindex(live_index)},
            )

        odds_index = test.index[odds_mask]
        if len(odds_index) > 0:
            append_regression_aggregate(
                actual_aggregates["oddsRows"],
                test.loc[odds_index, "actual_fpts"].astype(float),
                {name: pred.reindex(odds_index) for name, pred in actual_predictions.items()},
                {"baselineModel": actual_predictions["baseline"].reindex(odds_index)},
            )

        prop_index = test.index[prop_mask]
        if len(prop_index) > 0:
            append_regression_aggregate(
                actual_aggregates["propRows"],
                test.loc[prop_index, "actual_fpts"].astype(float),
                {name: pred.reindex(prop_index) for name, pred in actual_predictions.items()},
                {"baselineModel": actual_predictions["baseline"].reindex(prop_index)},
            )

    return {
        "evaluationUnit": "imported_slate",
        "minimumTrainSlates": int(min_train_slates),
        "minimumCohortRows": int(min_cohort_rows),
        "evaluatedSlateCount": int(len(per_slate)),
        "skippedSlates": skipped_slates,
        "perSlate": per_slate,
        "aggregate": {
            "actualFptsRegression": {
                cohort_name: finalize_regression_aggregate(aggregate, min_cohort_rows)
                for cohort_name, aggregate in actual_aggregates.items()
            },
            "residualRegression": {
                cohort_name: finalize_regression_aggregate(aggregate, min_cohort_rows)
                for cohort_name, aggregate in residual_aggregates.items()
            },
            "hit5xClassification": {
                cohort_name: finalize_classification_aggregate(aggregate, min_cohort_rows)
                for cohort_name, aggregate in classification_aggregates.items()
            },
        },
    }


def run_analysis(
    frame: pd.DataFrame,
    backend: str,
    holdout_slates: int,
    top_n: int,
    min_train_slates: int,
    min_cohort_rows: int,
) -> dict[str, Any]:
    train, test, holdout_dates = split_train_test(frame, holdout_slates)

    scenarios = scenario_feature_sets()

    live_baseline = test["live_proj"].astype(float)
    residual_baseline = np.zeros(len(test), dtype=float)
    hit5x_baseline = (test["live_proj"] / (test["salary"] / 200.0)).clip(lower=0.0, upper=2.0) / 2.0

    actual_runs = [
        evaluate_regression_scenario(name, train, test, features, "actual_fpts", backend, live_baseline, top_n)
        for name, features in scenarios.items()
    ]

    residual_runs = []
    for name, features in scenarios.items():
        result = evaluate_regression_scenario(
            name,
            train,
            test,
            features,
            "residual_fpts",
            backend,
            residual_baseline,
            top_n,
        )
        # Residual models are most useful if translated back into FPTS.
        adjusted_summary = result["summary"]
        train_filtered = train[np.isfinite(train["residual_fpts"].astype(float))].copy()
        test_filtered = test[np.isfinite(test["residual_fpts"].astype(float))].copy()
        x_train, x_test = prepare_features(train_filtered, test_filtered, features)
        model = build_regressor(backend)
        model.fit(x_train, train_filtered["residual_fpts"].astype(float))
        residual_pred = model.predict(x_test)
        baseline_subset = live_baseline.loc[test_filtered.index].to_numpy(dtype=float)
        adjusted_pred = baseline_subset + residual_pred
        mae, rmse, corr = regression_metrics(test_filtered["actual_fpts"].astype(float), adjusted_pred)
        baseline_mae, baseline_rmse, baseline_corr = regression_metrics(test_filtered["actual_fpts"].astype(float), baseline_subset)
        adjusted_summary["adjusted_mae"] = mae
        adjusted_summary["adjusted_rmse"] = rmse
        adjusted_summary["adjusted_corr"] = corr
        adjusted_summary["adjusted_mae_delta_vs_live"] = None if mae is None or baseline_mae is None else baseline_mae - mae
        result["summary"] = adjusted_summary
        result["top_features"] = feature_importance(model, list(x_train.columns), top_n)
        residual_runs.append(result)

    classification_runs = [
        evaluate_classification_scenario(name, train, test, features, backend, hit5x_baseline, top_n)
        for name, features in scenarios.items()
    ]

    baseline_mask = np.isfinite(test["actual_fpts"].astype(float)) & np.isfinite(live_baseline.astype(float))
    baseline_mae, baseline_rmse, baseline_corr = regression_metrics(
        test.loc[baseline_mask, "actual_fpts"].astype(float),
        live_baseline.loc[baseline_mask].to_numpy(dtype=float),
    )

    slate_dates = sorted(frame["slate_date"].dropna().unique().tolist())
    walk_forward = run_walk_forward_analysis(
        frame,
        backend=backend,
        min_train_slates=min_train_slates,
        min_cohort_rows=min_cohort_rows,
    )

    return {
        "modelVersion": MODEL_VERSION,
        "backend": backend,
        "evaluation": {
            "unit": "imported_slate",
            "description": "All walk-forward metrics are evaluated over imported slates present in the dataset, not calendar days.",
            "importedSlateCount": int(len(slate_dates)),
            "importedSlateDates": slate_dates,
            "minimumTrainSlates": int(min_train_slates),
            "minimumCohortRows": int(min_cohort_rows),
            "legacyTerminalHoldoutSlates": int(holdout_slates),
        },
        "holdoutDates": holdout_dates,
        "dataCoverageBySlate": summarize_slate_coverage(frame),
        "holdoutWarnings": holdout_warnings(test, holdout_dates),
        "sample": {
            "rows": int(len(frame)),
            "trainRows": int(len(train)),
            "testRows": int(len(test)),
            "slateDates": sorted(frame["slate_date"].dropna().unique().tolist()),
            "propCoveragePct": round(float(frame["prop_count"].gt(0).mean() * 100), 2),
            "oddsCoveragePct": round(float(frame["vegas_total"].notna().mean() * 100), 2),
        },
        "baselines": {
            "liveProjTestMAE": round(baseline_mae, 4) if baseline_mae is not None else None,
            "liveProjTestRMSE": round(baseline_rmse, 4) if baseline_rmse is not None else None,
            "liveProjTestCorr": round(baseline_corr, 4) if baseline_corr is not None else None,
            "hit5xRateTest": round(float(test["hit_5x"].mean()), 4) if len(test) > 0 else None,
        },
        "actualFptsRegression": actual_runs,
        "residualRegression": residual_runs,
        "hit5xClassification": classification_runs,
        "walkForward": walk_forward,
    }


def ensure_output_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
    args = parse_args()
    backend = choose_backend(args.backend)
    config = load_config()
    db = DatabaseManager(config.database_url)
    frame = load_dataset(db, args.min_date, args.max_date)
    if frame.empty:
        raise RuntimeError("No NBA result rows matched the requested filters.")

    report = run_analysis(
        frame,
        backend=backend,
        holdout_slates=args.holdout_slates,
        top_n=args.top_features,
        min_train_slates=args.min_train_slates,
        min_cohort_rows=args.min_cohort_rows,
    )
    ensure_output_dir(args.output)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")

    walk_forward_live = report["walkForward"]["aggregate"]["actualFptsRegression"]["liveProjectionRows"]["scenarios"]
    best_walk_forward_live = min(
        walk_forward_live.items(),
        key=lambda item: float("inf") if item[1]["mae"] is None else item[1]["mae"],
    )[0]

    print(json.dumps(
        {
            "output": str(args.output),
            "backend": backend,
            "holdoutDates": report["holdoutDates"],
            "baselineLiveProjMAE": report["baselines"]["liveProjTestMAE"],
            "walkForwardEvaluatedSlates": report["walkForward"]["evaluatedSlateCount"],
            "bestWalkForwardLiveScenario": best_walk_forward_live,
            "bestActualScenario": min(
                report["actualFptsRegression"],
                key=lambda entry: float("inf") if entry["summary"]["mae"] is None else entry["summary"]["mae"],
            )["scenario"],
            "bestResidualAdjustedScenario": min(
                report["residualRegression"],
                key=lambda entry: float("inf") if entry["summary"]["adjusted_mae"] is None else entry["summary"]["adjusted_mae"],
            )["scenario"],
            "bestHit5xScenario": max(
                report["hit5xClassification"],
                key=lambda entry: float("-inf") if entry["summary"]["auc"] is None else entry["summary"]["auc"],
            )["scenario"],
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
