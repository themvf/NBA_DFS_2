"""Leakage-safe, baseball-only MLB moneyline candidate model.

This candidate deliberately excludes sportsbook prices from its features. Raw
official outcomes are converted to rolling features using only earlier game
dates; same-date results never enter another game's feature row. The market is
used solely as the untouched holdout benchmark and the candidate is not
promoted unless it improves both log loss and Brier score.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from config import load_config
from db.database import DatabaseManager
from model.mlb_validation import chronological_date_holdout, expanding_date_folds

MODEL_VERSION = "mlb-ml-independent-v2-candidate"
DEFAULT_OUTPUT = Path(__file__).resolve().with_name(f"{MODEL_VERSION}.json")
TEAM_WINDOW = 30
STARTER_WINDOW = 10
MIN_TEAM_GAMES = 10
MIN_STARTS = 3

FEATURE_GROUPS = {
    "offense": [
        "offense_woba_adv", "offense_iso_adv", "offense_k_adv",
        "offense_bb_adv", "runs_per_game_adv",
    ],
    "starters": ["starter_fip_adv", "starter_k9_adv", "starter_bb9_adv"],
    "bullpen": ["bullpen_fip_adv"],
    "rest": ["rest_days_adv"],
}
FEATURE_COLS = [feature for features in FEATURE_GROUPS.values() for feature in features]


def load_outcomes(db: DatabaseManager) -> list[dict]:
    """Load the latest immutable official revision for each team/game."""
    return db.execute(
        """
        WITH latest AS (
            SELECT DISTINCT ON (o.game_id, o.team_id) o.*
            FROM mlb_team_game_outcomes o
            ORDER BY o.game_id, o.team_id, o.fetched_at DESC, o.id DESC
        )
        SELECT
            o.*,
            m.vegas_prob_home,
            m.home_ml,
            m.away_ml,
            park.runs_factor AS park_runs_factor
        FROM latest o
        JOIN mlb_matchups m ON m.id = o.matchup_id
        LEFT JOIN LATERAL (
            SELECT runs_factor
            FROM mlb_park_factors p
            WHERE p.team_id = CASE WHEN o.is_home THEN o.team_id ELSE o.opponent_team_id END
            ORDER BY p.season DESC, p.id DESC
            LIMIT 1
        ) park ON TRUE
        ORDER BY o.game_date, o.game_id, o.is_home DESC
        """
    )


def _rate(numerator: float, denominator: float) -> float | None:
    return float(numerator / denominator) if denominator > 0 else None


def _team_metrics(history: list[dict]) -> dict[str, float] | None:
    sample = history[-TEAM_WINDOW:]
    if len(sample) < MIN_TEAM_GAMES:
        return None
    hits = sum(int(row["hits"]) for row in sample)
    doubles = sum(int(row["doubles"]) for row in sample)
    triples = sum(int(row["triples"]) for row in sample)
    homers = sum(int(row["home_runs"]) for row in sample)
    singles = max(0, hits - doubles - triples - homers)
    walks = sum(int(row["walks"]) for row in sample)
    hbp = sum(int(row["hit_by_pitch"]) for row in sample)
    strikeouts = sum(int(row["strikeouts"]) for row in sample)
    at_bats = sum(int(row["at_bats"]) for row in sample)
    plate_appearances = sum(int(row["plate_appearances"]) for row in sample)
    runs = sum(int(row["runs"]) for row in sample)
    total_bases = singles + 2 * doubles + 3 * triples + 4 * homers
    woba_numerator = 0.69 * walks + 0.72 * hbp + 0.88 * singles + 1.24 * doubles + 1.56 * triples + 2.01 * homers
    return {
        "woba": _rate(woba_numerator, plate_appearances),
        "iso": (_rate(total_bases, at_bats) or 0.0) - (_rate(hits, at_bats) or 0.0),
        "k_pct": _rate(strikeouts, plate_appearances),
        "bb_pct": _rate(walks + hbp, plate_appearances),
        "runs_per_game": runs / len(sample),
    }


def _starter_metrics(history: list[dict]) -> dict[str, float] | None:
    sample = history[-STARTER_WINDOW:]
    if len(sample) < MIN_STARTS:
        return None
    outs = sum(int(row["starter_outs"]) for row in sample)
    innings = outs / 3.0
    if innings <= 0:
        return None
    homers = sum(int(row["starter_home_runs"]) for row in sample)
    walks = sum(int(row["starter_walks"]) + int(row["starter_hit_batters"]) for row in sample)
    strikeouts = sum(int(row["starter_strikeouts"]) for row in sample)
    return {
        "fip": (13 * homers + 3 * walks - 2 * strikeouts) / innings + 3.1,
        "k9": 9 * strikeouts / innings,
        "bb9": 9 * walks / innings,
    }


def _bullpen_metrics(history: list[dict]) -> dict[str, float] | None:
    sample = history[-TEAM_WINDOW:]
    if len(sample) < MIN_TEAM_GAMES:
        return None
    outs = sum(max(0, int(row["team_pitching_outs"]) - int(row["starter_outs"])) for row in sample)
    innings = outs / 3.0
    if innings <= 0:
        return None
    homers = sum(max(0, int(row["team_pitching_home_runs"]) - int(row["starter_home_runs"])) for row in sample)
    walks = sum(
        max(0, int(row["team_pitching_walks"]) - int(row["starter_walks"]))
        + max(0, int(row["team_pitching_hit_batters"]) - int(row["starter_hit_batters"]))
        for row in sample
    )
    strikeouts = sum(max(0, int(row["team_pitching_strikeouts"]) - int(row["starter_strikeouts"])) for row in sample)
    return {"fip": (13 * homers + 3 * walks - 2 * strikeouts) / innings + 3.1}


def _advantage(home: dict | None, away: dict | None, key: str, *, lower_is_better: bool = False) -> float | None:
    if home is None or away is None or home.get(key) is None or away.get(key) is None:
        return None
    return float(away[key] - home[key] if lower_is_better else home[key] - away[key])


def build_point_in_time_features(outcomes: list[dict]) -> pd.DataFrame:
    """Build one home-win row per game using strictly earlier game dates."""
    frame = pd.DataFrame(outcomes)
    if frame.empty:
        return frame
    frame["game_date"] = pd.to_datetime(frame["game_date"]).dt.normalize()
    frame = frame.sort_values(["game_date", "game_id", "is_home"], ascending=[True, True, False])

    team_history: dict[int, list[dict]] = defaultdict(list)
    starter_history: dict[int, list[dict]] = defaultdict(list)
    result: list[dict] = []

    for game_date, day in frame.groupby("game_date", sort=True):
        # Compute every game first, then append the day's outcomes. This makes
        # doubleheaders and all other same-date games incapable of leaking.
        for game_id, game_rows in day.groupby("game_id", sort=True):
            home_rows = game_rows[game_rows["is_home"].astype(bool)]
            away_rows = game_rows[~game_rows["is_home"].astype(bool)]
            if len(home_rows) != 1 or len(away_rows) != 1:
                continue
            home = home_rows.iloc[0].to_dict()
            away = away_rows.iloc[0].to_dict()
            home_team, away_team = int(home["team_id"]), int(away["team_id"])
            home_off = _team_metrics(team_history[home_team])
            away_off = _team_metrics(team_history[away_team])
            home_sp = _starter_metrics(starter_history[int(home["starter_id"])]) if pd.notna(home.get("starter_id")) else None
            away_sp = _starter_metrics(starter_history[int(away["starter_id"])]) if pd.notna(away.get("starter_id")) else None
            home_bp = _bullpen_metrics(team_history[home_team])
            away_bp = _bullpen_metrics(team_history[away_team])
            home_rest = (game_date - team_history[home_team][-1]["game_date"]).days if team_history[home_team] else None
            away_rest = (game_date - team_history[away_team][-1]["game_date"]).days if team_history[away_team] else None
            result.append({
                "id": int(home["matchup_id"]),
                "game_id": str(game_id),
                "game_date": game_date.strftime("%Y-%m-%d"),
                "home_win": int(int(home["runs"]) > int(away["runs"])),
                "market_home_prob": float(home["vegas_prob_home"]) if pd.notna(home.get("vegas_prob_home")) else None,
                "home_ml": int(home["home_ml"]) if pd.notna(home.get("home_ml")) else None,
                "away_ml": int(home["away_ml"]) if pd.notna(home.get("away_ml")) else None,
                "offense_woba_adv": _advantage(home_off, away_off, "woba"),
                "offense_iso_adv": _advantage(home_off, away_off, "iso"),
                "offense_k_adv": _advantage(home_off, away_off, "k_pct", lower_is_better=True),
                "offense_bb_adv": _advantage(home_off, away_off, "bb_pct"),
                "runs_per_game_adv": _advantage(home_off, away_off, "runs_per_game"),
                "starter_fip_adv": _advantage(home_sp, away_sp, "fip", lower_is_better=True),
                "starter_k9_adv": _advantage(home_sp, away_sp, "k9"),
                "starter_bb9_adv": _advantage(home_sp, away_sp, "bb9", lower_is_better=True),
                "bullpen_fip_adv": _advantage(home_bp, away_bp, "fip", lower_is_better=True),
                "rest_days_adv": float(home_rest - away_rest) if home_rest is not None and away_rest is not None else None,
            })

        for row in day.to_dict("records"):
            row["game_date"] = game_date
            team_history[int(row["team_id"])].append(row)
            if pd.notna(row.get("starter_id")):
                starter_history[int(row["starter_id"])].append(row)

    return pd.DataFrame(result).sort_values(["game_date", "id"]).reset_index(drop=True)


def _pipeline() -> Pipeline:
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(C=0.5, max_iter=1000)),
    ])


def _cv_logloss(train: pd.DataFrame, features: list[str], *, folds: int = 4) -> float:
    values: list[float] = []
    for fold_train, fold_test in expanding_date_folds(train, folds=folds):
        if len(fold_train) < 200:
            continue
        if features:
            model = _pipeline().fit(fold_train[features], fold_train["home_win"])
            probability = model.predict_proba(fold_test[features])[:, 1]
        else:
            probability = np.repeat(np.clip(fold_train["home_win"].mean(), 0.01, 0.99), len(fold_test))
        values.append(float(log_loss(fold_test["home_win"], probability)))
    return float(np.mean(values))


def select_feature_groups(train: pd.DataFrame, *, minimum_improvement: float = 0.0005) -> dict:
    """Forward-select groups using training-only chronological folds."""
    selected_groups: list[str] = []
    selected_features: list[str] = []
    current_score = _cv_logloss(train, [])
    audit: list[dict] = []
    remaining = dict(FEATURE_GROUPS)
    while remaining:
        candidates = []
        for name, features in remaining.items():
            score = _cv_logloss(train, selected_features + features)
            candidates.append((score, name, features))
        score, name, features = min(candidates)
        improvement = current_score - score
        retained = improvement >= minimum_improvement
        audit.append({
            "group": name,
            "candidate_cv_logloss": round(score, 6),
            "prior_cv_logloss": round(current_score, 6),
            "improvement": round(improvement, 6),
            "retained": retained,
        })
        remaining.pop(name)
        if not retained:
            for other_name, other_features in remaining.items():
                other_score = _cv_logloss(train, selected_features + other_features)
                audit.append({
                    "group": other_name,
                    "candidate_cv_logloss": round(other_score, 6),
                    "prior_cv_logloss": round(current_score, 6),
                    "improvement": round(current_score - other_score, 6),
                    "retained": False,
                })
            break
        selected_groups.append(name)
        selected_features.extend(features)
        current_score = score
    return {
        "baseline_cv_logloss": round(_cv_logloss(train, []), 6),
        "selected_cv_logloss": round(current_score, 6),
        "selected_groups": selected_groups,
        "selected_features": selected_features,
        "audit": audit,
        "minimum_improvement": minimum_improvement,
    }


def _rolling_calibrator(train: pd.DataFrame, features: list[str]) -> tuple[IsotonicRegression, int]:
    raw_parts: list[np.ndarray] = []
    y_parts: list[np.ndarray] = []
    for fold_train, fold_test in expanding_date_folds(train, folds=5):
        if len(fold_train) < 200:
            continue
        model = _pipeline().fit(fold_train[features], fold_train["home_win"])
        raw_parts.append(model.predict_proba(fold_test[features])[:, 1])
        y_parts.append(fold_test["home_win"].to_numpy(dtype=int))
    raw = np.concatenate(raw_parts)
    y = np.concatenate(y_parts)
    calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.01, y_max=0.99)
    calibrator.fit(raw, y)
    return calibrator, len(y)


def evaluate(db: DatabaseManager, *, test_fraction: float = 0.20) -> dict:
    features = build_point_in_time_features(load_outcomes(db))
    data = features[features["market_home_prob"].notna()].reset_index(drop=True)
    if len(data) < 500:
        return {"model_version": MODEL_VERSION, "promoted": False, "reason": "fewer than 500 benchmarkable games", "n": len(data)}
    train, test = chronological_date_holdout(data, test_fraction)
    selection = select_feature_groups(train)
    selected_features = selection["selected_features"]
    if not selected_features:
        return {
            "model_version": MODEL_VERSION,
            "promoted": False,
            "reason": "no baseball feature group improved training-only chronological validation",
            "feature_group_selection": selection,
        }
    model = _pipeline().fit(train[selected_features], train["home_win"])
    calibrator, calibration_n = _rolling_calibrator(train, selected_features)
    raw = model.predict_proba(test[selected_features])[:, 1]
    calibrated = calibrator.predict(raw)
    market = test["market_home_prob"].to_numpy(dtype=float)
    y = test["home_win"].to_numpy(dtype=int)
    market_logloss = float(log_loss(y, market))
    model_logloss = float(log_loss(y, calibrated))
    market_brier = float(brier_score_loss(y, market))
    model_brier = float(brier_score_loss(y, calibrated))
    promoted = model_logloss < market_logloss and model_brier < market_brier and len(test) >= 200
    missingness = {feature: round(float(data[feature].isna().mean()), 6) for feature in FEATURE_COLS}
    variation = {
        feature: {
            "non_missing": int(data[feature].notna().sum()),
            "unique": int(data[feature].nunique()),
            "std": round(float(data[feature].std()), 6),
        }
        for feature in FEATURE_COLS
    }
    result = {
        "model_version": MODEL_VERSION,
        "origin": "retrospective_backfill",
        "feature_cutoff": "strictly_prior_game_date",
        "market_used_as_feature": False,
        "training_start": str(train["game_date"].min()),
        "training_cutoff": str(train["game_date"].max()),
        "final_window_start": str(test["game_date"].min()),
        "final_window_end": str(test["game_date"].max()),
        "n_train": int(len(train)),
        "n_test": int(len(test)),
        "calibration_oof_n": int(calibration_n),
        "market_logloss": round(market_logloss, 6),
        "model_logloss": round(model_logloss, 6),
        "market_brier": round(market_brier, 6),
        "model_brier": round(model_brier, 6),
        "missingness": missingness,
        "variation": variation,
        "feature_groups": FEATURE_GROUPS,
        "feature_group_selection": selection,
        "promoted": promoted,
        "promotion_policy": "beats same-time market on final-window log loss and Brier with n_test >= 200",
        "reason": "promotion criteria passed" if promoted else "candidate did not beat the market on both proper scoring rules",
    }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate independent point-in-time MLB moneyline candidate")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    database = DatabaseManager(load_config().database_url)
    report = evaluate(database)
    output = Path(args.output)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {output}")
