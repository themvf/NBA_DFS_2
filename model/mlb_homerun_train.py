"""Train and evaluate a baseball-only MLB home run model.

The training set intentionally excludes DraftKings pools and HR odds. It uses
the baseball scenario rows from mlb_homerun_training_games and predicts whether
a hitter records at least one HR in that game.

Default split:
  train = 2024 + 2025 games before 2025-07-01
  test  = 2025 games on/after 2025-07-01

Usage:
    python -m model.mlb_homerun_train
    python -m model.mlb_homerun_train --test-start 2025-07-01
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from config import load_config
from db.database import DatabaseManager

MODEL_VERSION = "mlb_homerun_v1"
DEFAULT_OUTPUT = Path(__file__).resolve().with_name(f"{MODEL_VERSION}.json")
DEFAULT_TEST_START = "2025-07-01"
RANDOM_STATE = 42
PERMUTATION_SAMPLE_ROWS = 15000

ORDER_PA_FACTOR = {
    1: 1.08,
    2: 1.12,
    3: 1.10,
    4: 1.04,
    5: 1.00,
    6: 0.96,
    7: 0.93,
    8: 0.90,
    9: 0.88,
}

BASE_FEATURES = [
    "is_home",
    "batting_order",
    "hitter_games",
    "hitter_pa_pg",
    "hitter_hr_pg",
    "hitter_iso",
    "hitter_slg",
    "hitter_wrc_plus",
    "hitter_split_wrc_plus",
    "pitcher_games",
    "pitcher_ip_pg",
    "pitcher_hr_per_9",
    "pitcher_hr_fb_pct",
    "pitcher_xfip",
    "pitcher_fip",
    "pitcher_k_per_9",
    "pitcher_bb_per_9",
    "pitcher_whip",
    "pitcher_era",
    "park_runs_factor",
    "park_hr_factor",
]

ENGINEERED_FEATURES = [
    "has_batting_order",
    "order_pa_factor",
    "is_order_1",
    "is_order_2",
    "is_order_3",
    "is_order_4",
    "is_top3_order",
    "is_bottom3_order",
    "pitcher_hand_known",
    "vs_lhp",
    "vs_rhp",
    "hitter_power_available",
    "pitcher_power_allowed_available",
    "split_wrc_ratio",
    "hitter_hr_x_park",
    "hitter_iso_x_park",
    "hitter_hr_x_order",
    "hitter_hr_x_pitcher_hr9",
    "hitter_iso_x_pitcher_hr9",
    "pitcher_hr9_x_park",
    "pitcher_xfip_x_park",
]

FEATURES = BASE_FEATURES + ENGINEERED_FEATURES

FEATURE_GROUPS = {
    "hitter_power": [
        "hitter_hr_pg",
        "hitter_iso",
        "hitter_slg",
        "hitter_wrc_plus",
        "hitter_split_wrc_plus",
        "split_wrc_ratio",
        "hitter_power_available",
        "hitter_hr_x_park",
        "hitter_iso_x_park",
        "hitter_hr_x_order",
        "hitter_hr_x_pitcher_hr9",
        "hitter_iso_x_pitcher_hr9",
    ],
    "lineup_position": [
        "batting_order",
        "has_batting_order",
        "order_pa_factor",
        "is_order_1",
        "is_order_2",
        "is_order_3",
        "is_order_4",
        "is_top3_order",
        "is_bottom3_order",
    ],
    "pitcher_context": [
        "pitcher_games",
        "pitcher_ip_pg",
        "pitcher_hr_per_9",
        "pitcher_hr_fb_pct",
        "pitcher_xfip",
        "pitcher_fip",
        "pitcher_k_per_9",
        "pitcher_bb_per_9",
        "pitcher_whip",
        "pitcher_era",
        "pitcher_power_allowed_available",
        "pitcher_hand_known",
        "vs_lhp",
        "vs_rhp",
        "hitter_hr_x_pitcher_hr9",
        "hitter_iso_x_pitcher_hr9",
        "pitcher_hr9_x_park",
        "pitcher_xfip_x_park",
    ],
    "park_environment": [
        "is_home",
        "park_runs_factor",
        "park_hr_factor",
        "hitter_hr_x_park",
        "hitter_iso_x_park",
        "pitcher_hr9_x_park",
        "pitcher_xfip_x_park",
    ],
}


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _cap(value: Any, low: float, high: float) -> float:
    parsed = _safe_float(value)
    if parsed is None:
        return 1.0
    return max(low, min(high, parsed))


def _finite_metric(value: Any) -> float | None:
    parsed = _safe_float(value)
    return round(parsed, 6) if parsed is not None else None


def load_training_rows(db: DatabaseManager) -> list[dict[str, Any]]:
    return db.execute(
        """
        SELECT
            season,
            game_date::text AS game_date,
            game_id,
            hitter_mlb_id,
            hitter_name,
            hitter_team_abbrev,
            opponent_team_abbrev,
            is_home,
            ballpark,
            batting_order,
            opposing_sp_hand,
            hitter_games,
            hitter_pa_pg,
            hitter_hr_pg,
            hitter_iso,
            hitter_slg,
            hitter_wrc_plus,
            hitter_split_wrc_plus,
            pitcher_games,
            pitcher_ip_pg,
            pitcher_hr_per_9,
            pitcher_hr_fb_pct,
            pitcher_xfip,
            pitcher_fip,
            pitcher_k_per_9,
            pitcher_bb_per_9,
            pitcher_whip,
            pitcher_era,
            park_runs_factor,
            park_hr_factor,
            actual_hr,
            hit_hr_1plus
        FROM mlb_homerun_training_games
        WHERE season IN ('2024', '2025')
        ORDER BY game_date, game_id, hitter_mlb_id
        """
    )


def prepare_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame["game_date"] = pd.to_datetime(frame["game_date"])
    frame["target"] = frame["hit_hr_1plus"].astype(bool).astype(int)
    frame["is_home"] = frame["is_home"].fillna(False).astype(bool).astype(float)

    numeric_columns = [
        "batting_order",
        "hitter_games",
        "hitter_pa_pg",
        "hitter_hr_pg",
        "hitter_iso",
        "hitter_slg",
        "hitter_wrc_plus",
        "hitter_split_wrc_plus",
        "pitcher_games",
        "pitcher_ip_pg",
        "pitcher_hr_per_9",
        "pitcher_hr_fb_pct",
        "pitcher_xfip",
        "pitcher_fip",
        "pitcher_k_per_9",
        "pitcher_bb_per_9",
        "pitcher_whip",
        "pitcher_era",
        "park_runs_factor",
        "park_hr_factor",
        "actual_hr",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    order = frame["batting_order"]
    frame["has_batting_order"] = order.notna().astype(float)
    frame["order_pa_factor"] = order.map(ORDER_PA_FACTOR)
    for slot in (1, 2, 3, 4):
        frame[f"is_order_{slot}"] = (order == slot).astype(float)
    frame["is_top3_order"] = order.isin([1, 2, 3]).astype(float)
    frame["is_bottom3_order"] = order.isin([7, 8, 9]).astype(float)

    hand = frame["opposing_sp_hand"].astype(str).str.upper()
    frame["pitcher_hand_known"] = hand.isin(["L", "R"]).astype(float)
    frame["vs_lhp"] = (hand == "L").astype(float)
    frame["vs_rhp"] = (hand == "R").astype(float)

    frame["hitter_power_available"] = (
        frame[["hitter_hr_pg", "hitter_iso", "hitter_slg"]].notna().any(axis=1)
    ).astype(float)
    frame["pitcher_power_allowed_available"] = (
        frame[["pitcher_hr_per_9", "pitcher_xfip", "pitcher_fip", "pitcher_era"]].notna().any(axis=1)
    ).astype(float)

    frame["split_wrc_ratio"] = np.where(
        (frame["hitter_wrc_plus"] > 0) & frame["hitter_split_wrc_plus"].notna(),
        frame["hitter_split_wrc_plus"] / frame["hitter_wrc_plus"],
        np.nan,
    )
    frame["hitter_hr_x_park"] = frame["hitter_hr_pg"] * frame["park_hr_factor"]
    frame["hitter_iso_x_park"] = frame["hitter_iso"] * frame["park_hr_factor"]
    frame["hitter_hr_x_order"] = frame["hitter_hr_pg"] * frame["order_pa_factor"]
    frame["hitter_hr_x_pitcher_hr9"] = frame["hitter_hr_pg"] * frame["pitcher_hr_per_9"]
    frame["hitter_iso_x_pitcher_hr9"] = frame["hitter_iso"] * frame["pitcher_hr_per_9"]
    frame["pitcher_hr9_x_park"] = frame["pitcher_hr_per_9"] * frame["park_hr_factor"]
    frame["pitcher_xfip_x_park"] = frame["pitcher_xfip"] * frame["park_hr_factor"]

    for feature in FEATURES:
        frame[feature] = pd.to_numeric(frame[feature], errors="coerce")
    return frame


def split_train_test(frame: pd.DataFrame, test_start: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    test_start_dt = pd.Timestamp(test_start)
    train = frame.loc[frame["game_date"] < test_start_dt].copy()
    test = frame.loc[(frame["season"].astype(str) == "2025") & (frame["game_date"] >= test_start_dt)].copy()
    return train, test


def make_logistic_model() -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=2000, random_state=RANDOM_STATE)),
        ]
    )


def make_hgb_model() -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
            ("model", HistGradientBoostingClassifier(
                max_iter=220,
                learning_rate=0.035,
                max_leaf_nodes=24,
                l2_regularization=0.04,
                random_state=RANDOM_STATE,
            )),
        ]
    )


def heuristic_probabilities(frame: pd.DataFrame) -> np.ndarray:
    hr_pg = pd.to_numeric(frame["hitter_hr_pg"], errors="coerce").fillna(0.0).clip(lower=0.0)
    park = pd.to_numeric(frame["park_hr_factor"], errors="coerce").fillna(1.0).map(lambda value: _cap(value, 0.7, 1.5))
    order = pd.to_numeric(frame["order_pa_factor"], errors="coerce").fillna(1.0).map(lambda value: _cap(value, 0.8, 1.2))

    pitcher_hr9 = pd.to_numeric(frame["pitcher_hr_per_9"], errors="coerce")
    pitcher_xfip = pd.to_numeric(frame["pitcher_xfip"], errors="coerce")
    pitcher_fip = pd.to_numeric(frame["pitcher_fip"], errors="coerce")
    pitcher_era = pd.to_numeric(frame["pitcher_era"], errors="coerce")
    run_prevention = pitcher_xfip.combine_first(pitcher_fip).combine_first(pitcher_era).fillna(4.30)
    hr9_factor = pitcher_hr9.fillna(1.10).map(lambda value: _cap(value / 1.10, 0.6, 1.8))
    run_factor = run_prevention.map(lambda value: _cap(value / 4.30, 0.6, 1.8))
    pitcher_factor = (0.65 * hr9_factor) + (0.35 * run_factor)

    split_ratio = pd.to_numeric(frame["split_wrc_ratio"], errors="coerce").fillna(1.0).map(lambda value: _cap(value, 0.6, 1.5))
    expected_hr = hr_pg * park * order * pitcher_factor * split_ratio
    return (1.0 - np.exp(-expected_hr.to_numpy(dtype=float))).clip(0.0001, 0.9999)


def evaluate_predictions(y_true: np.ndarray, y_prob: np.ndarray) -> dict[str, float | None]:
    y_prob = np.clip(y_prob.astype(float), 0.000001, 0.999999)
    result: dict[str, float | None] = {
        "rows": int(y_true.size),
        "positiveRows": int(y_true.sum()),
        "positiveRate": _finite_metric(float(y_true.mean()) if y_true.size else None),
        "brier": _finite_metric(brier_score_loss(y_true, y_prob)),
        "logLoss": _finite_metric(log_loss(y_true, y_prob, labels=[0, 1])),
        "averagePrecision": _finite_metric(average_precision_score(y_true, y_prob)),
    }
    try:
        result["rocAuc"] = _finite_metric(roc_auc_score(y_true, y_prob))
    except ValueError:
        result["rocAuc"] = None
    return result


def calibration_table(y_true: np.ndarray, y_prob: np.ndarray) -> list[dict[str, Any]]:
    bins = [0.0, 0.025, 0.05, 0.075, 0.10, 0.125, 0.15, 0.20, 0.25, 1.0]
    labels = ["0-2.5%", "2.5-5%", "5-7.5%", "7.5-10%", "10-12.5%", "12.5-15%", "15-20%", "20-25%", "25%+"]
    bucketed = pd.DataFrame({"target": y_true, "prob": y_prob})
    bucketed["bucket"] = pd.cut(bucketed["prob"], bins=bins, labels=labels, include_lowest=True)
    rows: list[dict[str, Any]] = []
    for bucket, group in bucketed.groupby("bucket", observed=False):
        if group.empty:
            continue
        rows.append({
            "bucket": str(bucket),
            "rows": int(len(group)),
            "avgPredictedPct": _finite_metric(float(group["prob"].mean() * 100.0)),
            "hitRatePct": _finite_metric(float(group["target"].mean() * 100.0)),
            "brier": _finite_metric(brier_score_loss(group["target"], group["prob"])),
        })
    return rows


def top_k_metrics(test_frame: pd.DataFrame, y_prob: np.ndarray, k: int = 15) -> dict[str, float | int | None]:
    if test_frame.empty:
        return {"dates": 0}
    scored = test_frame[["game_date", "target"]].copy()
    scored["prob"] = y_prob
    total_top_rows = 0
    total_top_hr = 0
    total_hr = int(scored["target"].sum())
    captures: list[float] = []
    for _, group in scored.groupby("game_date"):
        top = group.sort_values("prob", ascending=False).head(k)
        date_hr = int(group["target"].sum())
        top_hr = int(top["target"].sum())
        total_top_rows += len(top)
        total_top_hr += top_hr
        if date_hr > 0:
            captures.append(top_hr / date_hr)

    overall_rate = float(scored["target"].mean()) if len(scored) else 0.0
    top_rate = total_top_hr / total_top_rows if total_top_rows else 0.0
    return {
        "dates": int(scored["game_date"].nunique()),
        "k": int(k),
        "topRows": int(total_top_rows),
        "topHrRows": int(total_top_hr),
        "topHitRatePct": _finite_metric(top_rate * 100.0),
        "overallHitRatePct": _finite_metric(overall_rate * 100.0),
        "liftVsOverall": _finite_metric(top_rate / overall_rate if overall_rate > 0 else None),
        "totalHrRows": int(total_hr),
        "captureRatePct": _finite_metric((total_top_hr / total_hr) * 100.0 if total_hr > 0 else None),
        "avgDateCapturePct": _finite_metric(float(np.mean(captures) * 100.0) if captures else None),
        "avgTopHrPerDate": _finite_metric(total_top_hr / scored["game_date"].nunique()),
    }


def model_probabilities(model: Pipeline, frame: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(frame[FEATURES])[:, 1]


def fit_models(train: pd.DataFrame) -> dict[str, Pipeline]:
    x_train = train[FEATURES]
    y_train = train["target"].to_numpy(dtype=int)
    models = {
        "logistic": make_logistic_model(),
        "histGradientBoosting": make_hgb_model(),
    }
    for model in models.values():
        model.fit(x_train, y_train)
    return models


def summarize_model(
    name: str,
    model: Pipeline | None,
    test: pd.DataFrame,
    probabilities: np.ndarray,
) -> dict[str, Any]:
    y_test = test["target"].to_numpy(dtype=int)
    summary = evaluate_predictions(y_test, probabilities)
    summary["top15"] = top_k_metrics(test, probabilities, k=15)
    summary["calibration"] = calibration_table(y_test, probabilities)
    if model is not None:
        summary["modelType"] = model.named_steps["model"].__class__.__name__
    else:
        summary["modelType"] = name
    return summary


def logistic_coefficients(model: Pipeline) -> list[dict[str, Any]]:
    logistic = model.named_steps["model"]
    coefs = logistic.coef_[0]
    rows = [
        {
            "feature": feature,
            "coefficient": round(float(coef), 6),
            "absCoefficient": round(abs(float(coef)), 6),
        }
        for feature, coef in zip(FEATURES, coefs, strict=True)
    ]
    return sorted(rows, key=lambda row: row["absCoefficient"], reverse=True)


def permutation_importance_summary(model: Pipeline, test: pd.DataFrame) -> list[dict[str, Any]]:
    if test.empty:
        return []
    sample = test
    if len(sample) > PERMUTATION_SAMPLE_ROWS:
        sample = sample.sample(PERMUTATION_SAMPLE_ROWS, random_state=RANDOM_STATE)
    result = permutation_importance(
        model,
        sample[FEATURES],
        sample["target"].to_numpy(dtype=int),
        scoring="average_precision",
        n_repeats=5,
        random_state=RANDOM_STATE,
        n_jobs=1,
    )
    rows = [
        {
            "feature": feature,
            "importanceMean": round(float(mean), 6),
            "importanceStd": round(float(std), 6),
        }
        for feature, mean, std in zip(FEATURES, result.importances_mean, result.importances_std, strict=True)
    ]
    return sorted(rows, key=lambda row: row["importanceMean"], reverse=True)


def grouped_permutation_importance(model: Pipeline, test: pd.DataFrame) -> list[dict[str, Any]]:
    if test.empty:
        return []
    sample = test
    if len(sample) > PERMUTATION_SAMPLE_ROWS:
        sample = sample.sample(PERMUTATION_SAMPLE_ROWS, random_state=RANDOM_STATE)
    baseline = average_precision_score(sample["target"], model_probabilities(model, sample))
    rows: list[dict[str, Any]] = []
    rng = np.random.default_rng(RANDOM_STATE)
    for group_name, features in FEATURE_GROUPS.items():
        shuffled = sample.copy()
        permutation = rng.permutation(len(shuffled))
        for feature in features:
            if feature in shuffled:
                shuffled[feature] = shuffled[feature].to_numpy()[permutation]
        score = average_precision_score(shuffled["target"], model_probabilities(model, shuffled))
        rows.append({
            "group": group_name,
            "averagePrecisionDrop": _finite_metric(baseline - score),
            "baselineAveragePrecision": _finite_metric(baseline),
            "shuffledAveragePrecision": _finite_metric(score),
            "features": features,
        })
    return sorted(rows, key=lambda row: row["averagePrecisionDrop"] or 0.0, reverse=True)


def deployable_logistic_artifact(model: Pipeline) -> dict[str, Any]:
    imputer = model.named_steps["imputer"]
    scaler = model.named_steps["scaler"]
    logistic = model.named_steps["model"]
    return {
        "kind": "standardized_logistic_regression",
        "features": FEATURES,
        "imputerMedians": [float(value) for value in imputer.statistics_.tolist()],
        "scalerMean": [float(value) for value in scaler.mean_.tolist()],
        "scalerScale": [float(value if value != 0 else 1.0) for value in scaler.scale_.tolist()],
        "coefficients": [float(value) for value in logistic.coef_[0].tolist()],
        "intercept": float(logistic.intercept_[0]),
        "probabilityBounds": [0.0001, 0.9999],
    }


def build_report(output: Path, test_start: str) -> dict[str, Any]:
    config = load_config()
    if not config.database_url:
        raise RuntimeError("DATABASE_URL is required.")

    db = DatabaseManager(config.database_url)
    frame = prepare_frame(load_training_rows(db))
    if frame.empty:
        raise RuntimeError("No HR training rows found.")

    train, test = split_train_test(frame, test_start)
    if train.empty or test.empty:
        raise RuntimeError(f"Invalid split: train={len(train)} test={len(test)}")

    models = fit_models(train)
    y_test = test["target"].to_numpy(dtype=int)
    baseline_constant = np.full(len(test), train["target"].mean(), dtype=float)
    baseline_heuristic = heuristic_probabilities(test)

    metrics = {
        "constantTrainRate": summarize_model("constantTrainRate", None, test, baseline_constant),
        "currentHeuristic": summarize_model("currentHeuristic", None, test, baseline_heuristic),
    }
    for name, model in models.items():
        metrics[name] = summarize_model(name, model, test, model_probabilities(model, test))

    full_logistic = make_logistic_model()
    full_logistic.fit(frame[FEATURES], frame["target"].to_numpy(dtype=int))
    full_hgb = make_hgb_model()
    full_hgb.fit(frame[FEATURES], frame["target"].to_numpy(dtype=int))

    report = {
        "modelVersion": MODEL_VERSION,
        "trainedAt": datetime.now(UTC).isoformat(),
        "data": {
            "sourceTable": "mlb_homerun_training_games",
            "featureSource": "season_aggregate",
            "leakageWarning": (
                "Current features are season aggregates, so this report is a first-pass feature analysis. "
                "A production model should retrain on pre-game rolling/prior-season features."
            ),
            "rows": int(len(frame)),
            "positiveRows": int(frame["target"].sum()),
            "positiveRatePct": _finite_metric(float(frame["target"].mean() * 100.0)),
            "trainRows": int(len(train)),
            "trainPositiveRows": int(train["target"].sum()),
            "testRows": int(len(test)),
            "testPositiveRows": int(test["target"].sum()),
            "testStart": test_start,
            "trainMinDate": train["game_date"].min().date().isoformat(),
            "trainMaxDate": train["game_date"].max().date().isoformat(),
            "testMinDate": test["game_date"].min().date().isoformat(),
            "testMaxDate": test["game_date"].max().date().isoformat(),
        },
        "features": FEATURES,
        "featureGroups": FEATURE_GROUPS,
        "metrics": metrics,
        "featureAnalysis": {
            "logisticStandardizedCoefficients": logistic_coefficients(models["logistic"]),
            "histGradientBoostingPermutationImportance": permutation_importance_summary(models["histGradientBoosting"], test),
            "groupedPermutationImportance": grouped_permutation_importance(models["histGradientBoosting"], test),
        },
        "deployableModel": deployable_logistic_artifact(full_logistic),
        "fullSampleReference": {
            "histGradientBoostingModelType": full_hgb.named_steps["model"].__class__.__name__,
            "note": "Tree model is evaluated for feature analysis; deployable artifact is logistic JSON for easier TS porting.",
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and evaluate MLB home run model.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--test-start", default=DEFAULT_TEST_START)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_report(args.output, args.test_start)
    metrics = report["metrics"]
    hgb = metrics["histGradientBoosting"]
    heuristic = metrics["currentHeuristic"]
    top_group = report["featureAnalysis"]["groupedPermutationImportance"][0]
    print(f"Wrote {args.output}")
    print(
        "Split: "
        f"{report['data']['trainRows']} train rows through {report['data']['trainMaxDate']}, "
        f"{report['data']['testRows']} test rows from {report['data']['testMinDate']}"
    )
    print(
        "HGB: "
        f"AP {hgb['averagePrecision']}, Brier {hgb['brier']}, "
        f"Top15 hit {hgb['top15']['topHitRatePct']}%, capture {hgb['top15']['captureRatePct']}%"
    )
    print(
        "Heuristic: "
        f"AP {heuristic['averagePrecision']}, Brier {heuristic['brier']}, "
        f"Top15 hit {heuristic['top15']['topHitRatePct']}%, capture {heuristic['top15']['captureRatePct']}%"
    )
    print(
        "Top feature group: "
        f"{top_group['group']} (AP drop {top_group['averagePrecisionDrop']})"
    )


if __name__ == "__main__":
    main()
