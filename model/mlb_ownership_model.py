"""Train and apply a reusable MLB ownership model.

V1 keeps the model intentionally simple:
- separate hitter and pitcher models
- linear Ridge regression on log ownership
- slate-relative rank features
- lineup and basic matchup context when available

The trained artifact is consumed by both Python ingest and the Next.js DFS
server actions. If the artifact is missing, callers should fall back to the
legacy heuristic ownership model.

Usage:
    python -m model.mlb_ownership_model
    python -m model.mlb_ownership_model --holdout-slates 4
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
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from config import load_config
from db.database import DatabaseManager

MODEL_VERSION = "mlb_ownership_v1"
DEFAULT_OUTPUT = Path(__file__).resolve().with_name(f"{MODEL_VERSION}.json")
DEFAULT_HOLDOUT_SLATES = 3
TARGET_OFFSET = 0.1
MIN_SCORE = 0.05

ROLE_CONFIGS: dict[str, dict[str, Any]] = {
    "hitter": {
        "budget": 800.0,
        "alpha": 8.0,
        "feature_order": [
            "baseline_own",
            "baseline_own_rank",
            "projection",
            "salary_k",
            "value_x",
            "projection_rank",
            "salary_rank",
            "value_rank",
            "lineup_order_norm",
            "has_lineup_order",
            "lineup_confirmed",
            "is_top4",
            "is_leadoff",
            "team_implied",
            "team_implied_rank",
            "vegas_total",
            "is_home",
            "pos_c",
            "pos_1b",
            "pos_2b",
            "pos_3b",
            "pos_ss",
            "pos_of",
        ],
    },
    "pitcher": {
        "budget": 200.0,
        "alpha": 12.0,
        "feature_order": [
            "baseline_own",
            "baseline_own_rank",
            "projection",
            "salary_k",
            "value_x",
            "projection_rank",
            "salary_rank",
            "value_rank",
            "opp_implied",
            "opp_implied_rank",
            "team_win_prob",
            "team_win_prob_rank",
            "vegas_total",
            "is_home",
        ],
    },
}

_ARTIFACT_CACHE: dict[Path, dict[str, Any] | None] = {}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _safe_int(value: Any) -> int | None:
    numeric = _safe_float(value)
    return int(numeric) if numeric is not None else None


def _safe_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def _sanitize_ownership_pct(value: Any) -> float | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return max(0.0, min(100.0, numeric))


def _is_pitcher(eligible_positions: str | None) -> bool:
    if not eligible_positions:
        return False
    return any(token.strip().upper() in {"SP", "RP", "P"} for token in str(eligible_positions).split("/"))


def _primary_hitter_position(eligible_positions: str | None) -> str:
    if not eligible_positions:
        return "UNK"
    for token in str(eligible_positions).split("/"):
        pos = token.strip().upper()
        if pos not in {"SP", "RP", "P"}:
            return pos or "UNK"
    return "UNK"


def _moneyline_to_prob(moneyline: Any) -> float | None:
    numeric = _safe_float(moneyline)
    if numeric is None:
        return None
    if numeric >= 0:
        return 100.0 / (numeric + 100.0)
    value = abs(numeric)
    return value / (value + 100.0)


def _first_projection(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _safe_float(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _normalize_scores(scores: list[tuple[int, float]], budget: float) -> dict[int, float]:
    valid = [(idx, score) for idx, score in scores if math.isfinite(score) and score > 0]
    total = sum(score for _, score in valid)
    if total <= 0:
        return {}
    result: dict[int, float] = {}
    for idx, score in valid:
        own_pct = round((score / total) * budget, 3)
        sanitized = _sanitize_ownership_pct(own_pct)
        if sanitized is not None:
            result[idx] = sanitized
    return result


def _correlation(y_true: np.ndarray, y_pred: np.ndarray) -> float | None:
    if y_true.size < 2 or y_pred.size < 2:
        return None
    if np.allclose(y_true, y_true[0]) or np.allclose(y_pred, y_pred[0]):
        return None
    corr = float(np.corrcoef(y_true, y_pred)[0, 1])
    return round(corr, 4) if math.isfinite(corr) else None


def _metric_summary(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | None]:
    if y_true.size == 0 or y_pred.size == 0:
        return {"mae": None, "bias": None, "corr": None}
    mae = float(np.mean(np.abs(y_pred - y_true)))
    bias = float(np.mean(y_pred - y_true))
    return {
        "mae": round(mae, 4),
        "bias": round(bias, 4),
        "corr": _correlation(y_true, y_pred),
    }


def _add_rank_features(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame.copy()
    grouped = result.groupby("slate_id", group_keys=False)
    for column in columns:
        rank_col = f"{column}_rank"
        result[rank_col] = grouped[column].rank(method="average", pct=True)
        result[rank_col] = result[rank_col].astype(float).fillna(0.5)
    return result


def _prepare_base_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame["actual_own_pct"] = frame["actual_own_pct"].map(_sanitize_ownership_pct)
    frame["baseline_proj_own_pct"] = frame["baseline_proj_own_pct"].map(_sanitize_ownership_pct)
    frame["baseline_own"] = frame["baseline_proj_own_pct"]
    frame["projection"] = frame.apply(
        lambda row: _first_projection(
            row,
            "linestar_proj",
            "our_proj",
            "avg_fpts_dk",
        ),
        axis=1,
    )
    frame["salary_k"] = pd.to_numeric(frame["salary"], errors="coerce") / 1000.0
    frame["value_x"] = frame["projection"] / frame["salary_k"].replace(0, np.nan)
    frame["lineup_order_norm"] = frame["dk_starting_lineup_order"].apply(
        lambda value: (10.0 - float(value)) / 9.0 if _safe_int(value) else None
    )
    frame["has_lineup_order"] = frame["dk_starting_lineup_order"].apply(lambda value: 1.0 if _safe_int(value) else 0.0)
    frame["lineup_confirmed"] = frame["dk_team_lineup_confirmed"].apply(lambda value: 1.0 if _safe_bool(value) else 0.0)
    frame["is_top4"] = frame["dk_starting_lineup_order"].apply(
        lambda value: 1.0 if (_safe_int(value) is not None and int(value) <= 4) else 0.0
    )
    frame["is_leadoff"] = frame["dk_starting_lineup_order"].apply(
        lambda value: 1.0 if _safe_int(value) == 1 else 0.0
    )
    frame["team_win_prob"] = frame["team_ml"].apply(_moneyline_to_prob)
    frame["vegas_total"] = pd.to_numeric(frame["vegas_total"], errors="coerce")
    frame["team_implied"] = pd.to_numeric(frame["team_implied"], errors="coerce")
    frame["opp_implied"] = pd.to_numeric(frame["opp_implied"], errors="coerce")
    frame["is_home"] = frame["is_home"].apply(lambda value: 1.0 if _safe_bool(value) else 0.0)
    frame["primary_pos"] = frame["eligible_positions"].apply(_primary_hitter_position)
    frame = _add_rank_features(
        frame,
        ["baseline_own", "projection", "salary_k", "value_x", "team_implied", "opp_implied", "team_win_prob"],
    )
    for pos in ("C", "1B", "2B", "3B", "SS", "OF"):
        frame[f"pos_{pos.lower()}"] = frame["primary_pos"].apply(lambda value, target=pos: 1.0 if value == target else 0.0)
    return frame


def load_training_rows(db: DatabaseManager) -> list[dict[str, Any]]:
    return db.execute(
        """
        SELECT
            ds.id AS slate_id,
            ds.slate_date::text AS slate_date,
            ds.game_count,
            ds.field_size,
            dp.id AS player_id,
            dp.name,
            dp.eligible_positions,
            dp.salary,
            dp.linestar_proj,
            COALESCE(dp.linestar_own_pct, dp.proj_own_pct) AS baseline_proj_own_pct,
            dp.our_proj,
            dp.avg_fpts_dk,
            dp.actual_own_pct,
            dp.dk_starting_lineup_order,
            dp.dk_team_lineup_confirmed,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_implied
                WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_implied
                ELSE NULL
            END AS team_implied,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
                WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
                ELSE NULL
            END AS opp_implied,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
                WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
                ELSE NULL
            END AS team_ml,
            mm.vegas_total,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
                WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
                ELSE NULL
            END AS is_home
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        WHERE ds.sport = 'mlb'
          AND ds.contest_type = 'main'
          AND ds.contest_format = 'gpp'
          AND dp.actual_own_pct IS NOT NULL
          AND COALESCE(dp.is_out, false) = false
          AND dp.salary > 0
        ORDER BY ds.slate_date ASC, ds.id ASC, dp.id ASC
        """
    )


def _frame_for_role(base_frame: pd.DataFrame, role: str) -> pd.DataFrame:
    if base_frame.empty:
        columns = ["eligible_positions", "baseline_own", "slate_id", *ROLE_CONFIGS[role]["feature_order"]]
        return pd.DataFrame(columns=columns)
    is_pitcher = role == "pitcher"
    mask = base_frame["eligible_positions"].apply(_is_pitcher) if is_pitcher else ~base_frame["eligible_positions"].apply(_is_pitcher)
    frame = base_frame.loc[mask].copy()
    frame = frame.loc[frame["baseline_own"].notna()].copy()
    feature_order = ROLE_CONFIGS[role]["feature_order"]
    for column in feature_order:
        if column not in frame.columns:
            frame[column] = np.nan
    return frame


def _fit_role_model(frame: pd.DataFrame, role: str, holdout_slates: int) -> tuple[dict[str, Any], dict[str, Any]]:
    config = ROLE_CONFIGS[role]
    feature_order = config["feature_order"]
    unique_dates = sorted(frame["slate_date"].dropna().unique().tolist())
    holdout_count = min(max(1, holdout_slates), max(1, len(unique_dates) - 1))
    holdout_dates = set(unique_dates[-holdout_count:])
    train_frame = frame.loc[~frame["slate_date"].isin(holdout_dates)].copy()
    if train_frame.empty:
        train_frame = frame.copy()
        holdout_dates = set()
    test_frame = frame.loc[frame["slate_date"].isin(holdout_dates)].copy() if holdout_dates else frame.iloc[0:0].copy()

    fill_values = {
        feature: float(pd.to_numeric(train_frame[feature], errors="coerce").median())
        if pd.to_numeric(train_frame[feature], errors="coerce").notna().any()
        else 0.0
        for feature in feature_order
    }

    def prepare_features(source: pd.DataFrame) -> pd.DataFrame:
        prepared = source.copy()
        for feature in feature_order:
            prepared[feature] = pd.to_numeric(prepared[feature], errors="coerce").fillna(fill_values[feature]).astype(float)
        return prepared

    prepared_train = prepare_features(train_frame)
    scaler = StandardScaler()
    x_train = scaler.fit_transform(prepared_train[feature_order].to_numpy(dtype=float))
    baseline_train = prepared_train["baseline_own"].to_numpy(dtype=float)
    y_train = (
        np.log(prepared_train["actual_own_pct"].to_numpy(dtype=float) + TARGET_OFFSET)
        - np.log(baseline_train + TARGET_OFFSET)
    )
    sample_weight = 1.0 + np.clip(prepared_train["actual_own_pct"].to_numpy(dtype=float), 0, 30) / 10.0
    model = Ridge(alpha=float(config["alpha"]))
    model.fit(x_train, y_train, sample_weight=sample_weight)

    evaluation = {
        "holdoutSlateDates": sorted(holdout_dates),
        "nTrainRows": int(len(train_frame)),
        "nHoldoutRows": int(len(test_frame)),
        "nHoldoutSlates": int(len(holdout_dates)),
    }
    if not test_frame.empty:
        predicted = predict_role_frame(test_frame, role, {
            "featureOrder": feature_order,
            "intercept": float(model.intercept_),
            "coefficients": [float(value) for value in model.coef_.tolist()],
            "means": [float(value) for value in scaler.mean_.tolist()],
            "scales": [float(value if value != 0 else 1.0) for value in scaler.scale_.tolist()],
            "fillValues": fill_values,
            "budget": float(config["budget"]),
            "minScore": MIN_SCORE,
        })
        actual = test_frame["actual_own_pct"].to_numpy(dtype=float)
        evaluation.update(_metric_summary(actual, predicted))
        baseline = test_frame["baseline_proj_own_pct"].to_numpy(dtype=float)
        baseline_mask = np.isfinite(baseline)
        if baseline_mask.any():
            baseline_metrics = _metric_summary(actual[baseline_mask], baseline[baseline_mask])
            evaluation["baselineMae"] = baseline_metrics["mae"]
            evaluation["baselineBias"] = baseline_metrics["bias"]
            evaluation["baselineCorr"] = baseline_metrics["corr"]
            mae = evaluation.get("mae")
            baseline_mae = evaluation.get("baselineMae")
            evaluation["maeDeltaVsBaseline"] = (
                round(float(mae) - float(baseline_mae), 4)
                if mae is not None and baseline_mae is not None
                else None
            )
        else:
            evaluation["baselineMae"] = None
            evaluation["baselineBias"] = None
            evaluation["baselineCorr"] = None
            evaluation["maeDeltaVsBaseline"] = None

    full_prepared = prepare_features(frame)
    full_scaler = StandardScaler()
    x_full = full_scaler.fit_transform(full_prepared[feature_order].to_numpy(dtype=float))
    baseline_full = full_prepared["baseline_own"].to_numpy(dtype=float)
    y_full = (
        np.log(full_prepared["actual_own_pct"].to_numpy(dtype=float) + TARGET_OFFSET)
        - np.log(baseline_full + TARGET_OFFSET)
    )
    full_sample_weight = 1.0 + np.clip(full_prepared["actual_own_pct"].to_numpy(dtype=float), 0, 30) / 10.0
    final_model = Ridge(alpha=float(config["alpha"]))
    final_model.fit(x_full, y_full, sample_weight=full_sample_weight)

    coefficients = [float(value) for value in final_model.coef_.tolist()]
    top_features = sorted(
        (
            {
                "feature": feature,
                "coefficient": round(coeff, 6),
                "absCoefficient": round(abs(coeff), 6),
            }
            for feature, coeff in zip(feature_order, coefficients, strict=True)
        ),
        key=lambda entry: entry["absCoefficient"],
        reverse=True,
    )[:8]

    artifact = {
        "featureOrder": feature_order,
        "intercept": float(final_model.intercept_),
        "coefficients": coefficients,
        "means": [float(value) for value in full_scaler.mean_.tolist()],
        "scales": [float(value if value != 0 else 1.0) for value in full_scaler.scale_.tolist()],
        "fillValues": fill_values,
        "budget": float(config["budget"]),
        "minScore": MIN_SCORE,
        "rowCount": int(len(frame)),
        "slateCount": int(frame["slate_id"].nunique()),
        "topFeatures": top_features,
        "evaluation": evaluation,
    }
    return artifact, evaluation


def predict_role_frame(frame: pd.DataFrame, role: str, artifact: dict[str, Any]) -> np.ndarray:
    if frame.empty:
        return np.array([], dtype=float)
    feature_order: list[str] = list(artifact["featureOrder"])
    fill_values: dict[str, float] = {key: float(value) for key, value in artifact.get("fillValues", {}).items()}
    prepared = frame.copy()
    for feature in feature_order:
        fill = fill_values.get(feature, 0.0)
        prepared[feature] = pd.to_numeric(prepared.get(feature), errors="coerce").fillna(fill).astype(float)

    means = np.array(artifact["means"], dtype=float)
    scales = np.array(artifact["scales"], dtype=float)
    scales[scales == 0] = 1.0
    coeffs = np.array(artifact["coefficients"], dtype=float)
    intercept = float(artifact["intercept"])
    x = prepared[feature_order].to_numpy(dtype=float)
    scaled = (x - means) / scales
    raw = intercept + scaled.dot(coeffs)
    baseline = prepared["baseline_own"].to_numpy(dtype=float)
    raw_scores = np.maximum(
        float(artifact.get("minScore", MIN_SCORE)),
        np.exp(raw) * (baseline + TARGET_OFFSET) - TARGET_OFFSET,
    )

    budget = float(artifact["budget"])
    normalized = np.zeros(len(prepared), dtype=float)
    for slate_id, slate_idx in prepared.groupby("slate_id").groups.items():
        slate_positions = [(int(index), float(raw_scores[pos])) for pos, index in enumerate(prepared.index) if index in slate_idx]
        normalized_map = _normalize_scores(slate_positions, budget)
        for pos, index in enumerate(prepared.index):
            if index in normalized_map:
                normalized[pos] = normalized_map[index]
    return normalized


def train_model(output_path: Path = DEFAULT_OUTPUT, holdout_slates: int = DEFAULT_HOLDOUT_SLATES) -> dict[str, Any]:
    config = load_config()
    if not config.database_url:
        raise RuntimeError("DATABASE_URL is required.")
    db = DatabaseManager(config.database_url)
    rows = load_training_rows(db)
    base_frame = _prepare_base_frame(rows)
    if base_frame.empty:
        raise RuntimeError("No historical MLB ownership rows found.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "modelVersion": MODEL_VERSION,
        "trainedAt": datetime.now(UTC).isoformat(),
        "trainingScope": {
            "sport": "mlb",
            "contestType": "main",
            "contestFormat": "gpp",
            "target": "actual_own_pct",
            "holdoutSlates": int(holdout_slates),
            "targetOffset": TARGET_OFFSET,
        },
        "roles": {},
    }

    for role in ("hitter", "pitcher"):
        role_frame = _frame_for_role(base_frame, role)
        if role_frame.empty:
            raise RuntimeError(f"No rows available for role={role}.")
        role_artifact, _ = _fit_role_model(role_frame, role, holdout_slates)
        artifact["roles"][role] = role_artifact

    output_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    _ARTIFACT_CACHE[output_path.resolve()] = artifact
    return artifact


def load_model_artifact(path: Path = DEFAULT_OUTPUT) -> dict[str, Any] | None:
    resolved = path.resolve()
    if resolved in _ARTIFACT_CACHE:
        return _ARTIFACT_CACHE[resolved]
    if not resolved.exists():
        _ARTIFACT_CACHE[resolved] = None
        return None
    try:
        artifact = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception:
        _ARTIFACT_CACHE[resolved] = None
        return None
    _ARTIFACT_CACHE[resolved] = artifact
    return artifact


def _build_player_feature_rows(players: list[dict[str, Any]], role: str, projection_mode: str) -> tuple[pd.DataFrame, list[int]]:
    active_rows: list[dict[str, Any]] = []
    original_indices: list[int] = []
    baseline_key = "our_own_pct" if projection_mode == "our" else "linestar_own_pct"
    for idx, player in enumerate(players):
        if _safe_bool(player.get("is_out")):
            continue
        if (player.get("salary") or 0) <= 0:
            continue
        pitcher_flag = _is_pitcher(player.get("eligible_positions"))
        if role == "pitcher" and not pitcher_flag:
            continue
        if role == "hitter" and pitcher_flag:
            continue
        row = {
            "slate_id": 1,
            "eligible_positions": player.get("eligible_positions"),
            "salary": player.get("salary"),
            "projection": _first_projection(
                player,
                "our_proj" if projection_mode == "our" else "linestar_proj",
                "linestar_proj" if projection_mode == "our" else "our_proj",
                "avg_fpts_dk",
            ),
            "dk_starting_lineup_order": player.get("dk_starting_lineup_order"),
            "dk_team_lineup_confirmed": player.get("dk_team_lineup_confirmed"),
            "team_implied": player.get("team_implied"),
            "opp_implied": player.get("opp_implied"),
            "team_ml": player.get("team_ml"),
            "vegas_total": player.get("vegas_total"),
            "is_home": player.get("is_home"),
            "actual_own_pct": 0.0,
            "baseline_proj_own_pct": _sanitize_ownership_pct(player.get(baseline_key)),
        }
        active_rows.append(row)
        original_indices.append(idx)
    frame = _prepare_base_frame(active_rows)
    return _frame_for_role(frame, role), original_indices


def predict_pool_ownership(
    players: list[dict[str, Any]],
    projection_mode: str = "field",
    artifact_path: Path = DEFAULT_OUTPUT,
) -> dict[int, float]:
    artifact = load_model_artifact(artifact_path)
    if not artifact:
        return {}

    result: dict[int, float] = {}
    role_mapping = {"hitter": "hitter", "pitcher": "pitcher"}
    for role in ("hitter", "pitcher"):
        role_artifact = artifact.get("roles", {}).get(role)
        if not role_artifact:
            continue
        role_frame, original_indices = _build_player_feature_rows(players, role, projection_mode)
        if role_frame.empty:
            continue
        predicted = predict_role_frame(role_frame, role_mapping[role], role_artifact)
        for offset, value in enumerate(predicted):
            sanitized = _sanitize_ownership_pct(value)
            if sanitized is not None:
                result[original_indices[offset]] = sanitized
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train MLB ownership model V1.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--holdout-slates", type=int, default=DEFAULT_HOLDOUT_SLATES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact = train_model(output_path=args.output, holdout_slates=max(1, args.holdout_slates))
    hitters = artifact["roles"]["hitter"]
    pitchers = artifact["roles"]["pitcher"]
    print(f"Wrote {args.output}")
    print(
        "Hitters: "
        f"{hitters['rowCount']} rows / {hitters['slateCount']} slates, "
        f"holdout MAE {hitters['evaluation'].get('mae')}, "
        f"baseline MAE {hitters['evaluation'].get('baselineMae')}"
    )
    print(
        "Pitchers: "
        f"{pitchers['rowCount']} rows / {pitchers['slateCount']} slates, "
        f"holdout MAE {pitchers['evaluation'].get('mae')}, "
        f"baseline MAE {pitchers['evaluation'].get('baselineMae')}"
    )


if __name__ == "__main__":
    main()
