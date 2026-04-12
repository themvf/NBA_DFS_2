#!/usr/bin/env python3
"""NBA game-total (O/U) prediction model.

Predicts where Vegas is most likely to miss on game totals.

Target:
    miss = (home_score + away_score) - vegas_total
    positive = actual went OVER  |  negative = actual went UNDER

Three models are compared:
    0. Vegas baseline — always predict 0 (trust Vegas completely)
    1. Ridge regression — linear, interpretable, fast
    2. XGBoost — captures non-linear interactions

Key features engineered:
    - Rest / back-to-back flags (computed from game dates)
    - Pace mismatch (fast vs slow team → total variance)
    - Defensive rating (both teams — determines scoring ceiling)
    - Rolling per-team Vegas bias (has this team historically beaten their implied?)
    - Game context (spread size, win probability, total tier)

Evaluation uses a chronological train/test split (no look-ahead).

Usage:
    python -m model.nba_game_total_model
    python -m model.nba_game_total_model --test-fraction 0.25 --top-features 15
    python -m model.nba_game_total_model --output reports/game_total_model.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, roc_auc_score
from sklearn.preprocessing import StandardScaler

try:
    from xgboost import XGBClassifier, XGBRegressor
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from config import load_config
from db.database import DatabaseManager

# ── Defaults ────────────────────────────────────────────────────
DEFAULT_TEST_FRACTION = 0.20   # last 20% of games chronologically
DEFAULT_MIN_GAMES     = 50     # skip model if fewer games available
DEFAULT_TOP_FEATURES  = 15


# ── Data Loading ─────────────────────────────────────────────────

def load_games(db: DatabaseManager) -> pd.DataFrame:
    """Pull all NBA games that have both odds and final scores."""
    rows = db.execute("""
        SELECT
            nm.id,
            nm.game_date::text         AS game_date,
            nm.home_team_id,
            nm.away_team_id,
            ht.abbreviation            AS home_abbrev,
            at.abbreviation            AS away_abbrev,
            nm.vegas_total,
            nm.home_ml,
            nm.away_ml,
            nm.home_spread,
            nm.vegas_prob_home         AS home_win_prob,
            nm.home_implied,
            nm.away_implied,
            nm.home_score,
            nm.away_score
        FROM nba_matchups nm
        JOIN teams ht ON ht.team_id = nm.home_team_id
        JOIN teams at ON at.team_id = nm.away_team_id
        WHERE nm.vegas_total IS NOT NULL
          AND nm.home_score  IS NOT NULL
          AND nm.away_score  IS NOT NULL
        ORDER BY nm.game_date
    """)
    df = pd.DataFrame(rows)
    df["game_date"] = pd.to_datetime(df["game_date"])
    for col in ["vegas_total", "home_ml", "away_ml", "home_spread",
                "home_win_prob", "home_implied", "away_implied",
                "home_score", "away_score"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_team_stats(db: DatabaseManager) -> pd.DataFrame:
    """Pull team pace/ratings by season."""
    rows = db.execute("""
        SELECT team_id, season, pace, off_rtg, def_rtg
        FROM nba_team_stats
    """)
    df = pd.DataFrame(rows)
    for col in ["pace", "off_rtg", "def_rtg"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── Feature Engineering ──────────────────────────────────────────

def _game_date_to_season(dt: pd.Timestamp) -> str:
    year, month = dt.year, dt.month
    return f"{year}-{str(year + 1)[-2:]}" if month >= 10 else f"{year - 1}-{str(year)[-2:]}"


def add_rest_features(df: pd.DataFrame) -> pd.DataFrame:
    """Days of rest and back-to-back flags for both home and away teams."""
    home = df[["game_date", "home_team_id"]].rename(columns={"home_team_id": "team_id"})
    away = df[["game_date", "away_team_id"]].rename(columns={"away_team_id": "team_id"})
    all_ap = pd.concat([home, away]).drop_duplicates().sort_values(["team_id", "game_date"])
    all_ap["prev_date"] = all_ap.groupby("team_id")["game_date"].shift(1)
    all_ap["days_rest"] = (
        (all_ap["game_date"] - all_ap["prev_date"]).dt.days - 1
    ).clip(lower=0).fillna(3.0)
    all_ap["is_b2b"] = (all_ap["days_rest"] == 0).astype(int)

    rest_home = all_ap.rename(columns={
        "team_id": "home_team_id", "days_rest": "days_rest_home", "is_b2b": "is_b2b_home",
    })[["game_date", "home_team_id", "days_rest_home", "is_b2b_home"]]
    rest_away = all_ap.rename(columns={
        "team_id": "away_team_id", "days_rest": "days_rest_away", "is_b2b": "is_b2b_away",
    })[["game_date", "away_team_id", "days_rest_away", "is_b2b_away"]]

    df = df.merge(rest_home, on=["game_date", "home_team_id"], how="left")
    df = df.merge(rest_away, on=["game_date", "away_team_id"], how="left")
    for col in ["days_rest_home", "days_rest_away"]:
        df[col] = df[col].fillna(3.0)
    for col in ["is_b2b_home", "is_b2b_away"]:
        df[col] = df[col].fillna(0).astype(int)
    df["rest_diff"] = df["days_rest_home"] - df["days_rest_away"]
    df["b2b_net"] = df["is_b2b_away"].astype(int) - df["is_b2b_home"].astype(int)
    return df


def add_team_stats_features(df: pd.DataFrame, team_stats: pd.DataFrame) -> pd.DataFrame:
    """Join team pace/ratings for both home and away teams."""
    df["season"] = df["game_date"].apply(_game_date_to_season)

    for side, id_col in [("home", "home_team_id"), ("away", "away_team_id")]:
        ts = team_stats.rename(columns={
            "team_id": id_col,
            "pace": f"{side}_pace",
            "off_rtg": f"{side}_off_rtg",
            "def_rtg": f"{side}_def_rtg",
        })[[id_col, "season", f"{side}_pace", f"{side}_off_rtg", f"{side}_def_rtg"]]
        df = df.merge(ts, on=[id_col, "season"], how="left")

    # Derived matchup features
    df["pace_avg"]    = (df["home_pace"]    + df["away_pace"])    / 2
    df["pace_diff"]   = (df["home_pace"]    - df["away_pace"]).abs()
    df["def_rtg_avg"] = (df["home_def_rtg"] + df["away_def_rtg"]) / 2
    df["def_rtg_diff"]= (df["home_def_rtg"] - df["away_def_rtg"]).abs()
    df["net_rtg_home"]= df["home_off_rtg"]  - df["home_def_rtg"]
    df["net_rtg_away"]= df["away_off_rtg"]  - df["away_def_rtg"]
    df["net_rtg_diff"]= df["net_rtg_home"]  - df["net_rtg_away"]
    return df


def add_rolling_team_bias(df: pd.DataFrame) -> pd.DataFrame:
    """Rolling mean of (actual_score - implied_total) per team, no look-ahead.

    Captures: 'historically, does Vegas underestimate/overestimate this team?'
    Uses shift(1) before expanding mean so current game is excluded.
    """
    df = df.sort_values("game_date").copy()

    # One row per (game_date, team_id) with (score - implied)
    home_ap = df[["game_date", "home_team_id", "home_score", "home_implied"]].rename(
        columns={"home_team_id": "team_id", "home_score": "score", "home_implied": "implied"}
    )
    away_ap = df[["game_date", "away_team_id", "away_score", "away_implied"]].rename(
        columns={"away_team_id": "team_id", "away_score": "score", "away_implied": "implied"}
    )
    all_ap = pd.concat([home_ap, away_ap], ignore_index=True)
    all_ap = all_ap.drop_duplicates(subset=["game_date", "team_id"]).sort_values(
        ["team_id", "game_date"]
    )
    all_ap["score_vs_implied"] = all_ap["score"] - all_ap["implied"]
    all_ap["team_rolling_bias"] = all_ap.groupby("team_id")["score_vs_implied"].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    all_ap["team_rolling_bias"] = all_ap["team_rolling_bias"].fillna(0.0)

    home_bias = all_ap.rename(columns={
        "team_id": "home_team_id", "team_rolling_bias": "home_team_rolling_bias"
    })[["game_date", "home_team_id", "home_team_rolling_bias"]]
    away_bias = all_ap.rename(columns={
        "team_id": "away_team_id", "team_rolling_bias": "away_team_rolling_bias"
    })[["game_date", "away_team_id", "away_team_rolling_bias"]]

    df = df.merge(home_bias, on=["game_date", "home_team_id"], how="left")
    df = df.merge(away_bias, on=["game_date", "away_team_id"], how="left")
    df["home_team_rolling_bias"] = df["home_team_rolling_bias"].fillna(0.0)
    df["away_team_rolling_bias"] = df["away_team_rolling_bias"].fillna(0.0)
    # Combined: if both teams are chronically under-implied, expect over
    df["total_team_rolling_bias"] = df["home_team_rolling_bias"] + df["away_team_rolling_bias"]
    return df


def add_context_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calendar and game-context features."""
    df["month"]       = df["game_date"].dt.month
    df["day_of_week"] = df["game_date"].dt.dayofweek  # Mon=0, Sun=6
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(int)
    df["abs_spread"]  = df["home_spread"].abs()
    # Target columns
    df["actual_total"]= df["home_score"] + df["away_score"]
    df["miss"]        = df["actual_total"] - df["vegas_total"]
    df["went_over"]   = (df["miss"] > 0).astype(int)
    return df


def build_features(df: pd.DataFrame, team_stats: pd.DataFrame) -> pd.DataFrame:
    """Run full feature engineering pipeline."""
    df = add_rest_features(df)
    df = add_team_stats_features(df, team_stats)
    df = add_rolling_team_bias(df)
    df = add_context_features(df)
    return df.sort_values("game_date").reset_index(drop=True)


# ── Feature list ─────────────────────────────────────────────────

FEATURE_COLS = [
    # Vegas line inputs
    "vegas_total",
    "home_implied",
    "away_implied",
    "abs_spread",
    "home_win_prob",
    # Rest
    "days_rest_home",
    "days_rest_away",
    "rest_diff",
    "is_b2b_home",
    "is_b2b_away",
    "b2b_net",
    # Pace
    "home_pace",
    "away_pace",
    "pace_avg",
    "pace_diff",
    # Ratings
    "home_off_rtg",
    "away_off_rtg",
    "home_def_rtg",
    "away_def_rtg",
    "def_rtg_avg",
    "def_rtg_diff",
    "net_rtg_home",
    "net_rtg_away",
    "net_rtg_diff",
    # Rolling team bias (no look-ahead)
    "home_team_rolling_bias",
    "away_team_rolling_bias",
    "total_team_rolling_bias",
    # Calendar
    "month",
    "is_weekend",
]


# ── Model evaluation ─────────────────────────────────────────────

def time_split(df: pd.DataFrame, test_fraction: float):
    """Chronological train/test split."""
    n = len(df)
    split = int(n * (1 - test_fraction))
    return df.iloc[:split], df.iloc[split:]


def regression_metrics(y_true, y_pred, label: str) -> dict:
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    if len(y_true) > 2 and np.std(y_pred) > 0:
        corr = float(np.corrcoef(y_true, y_pred)[0, 1])
    else:
        corr = 0.0
    return {"label": label, "mae": round(mae, 3), "rmse": round(rmse, 3), "corr": round(corr, 3)}


def over_under_auc(y_true_over, y_prob) -> float | None:
    if len(set(y_true_over)) < 2:
        return None
    try:
        return round(float(roc_auc_score(y_true_over, y_prob)), 3)
    except Exception:
        return None


def get_feature_importance(model, feature_names: list[str], top_n: int) -> list[dict]:
    if hasattr(model, "feature_importances_"):
        imps = model.feature_importances_
    elif hasattr(model, "coef_"):
        imps = np.abs(model.coef_)
    else:
        return []
    ranked = sorted(zip(feature_names, imps), key=lambda x: x[1], reverse=True)
    return [{"feature": f, "importance": round(float(v), 5)} for f, v in ranked[:top_n]]


def run_models(
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_cols: list[str],
    top_n: int,
) -> dict:
    # Drop rows missing any feature
    all_data = pd.concat([train, test])
    valid_cols = [c for c in feature_cols if c in all_data.columns]
    missing = [c for c in feature_cols if c not in all_data.columns]
    if missing:
        print(f"  [warn] Missing features: {missing}")

    train_clean = train.dropna(subset=valid_cols + ["miss", "went_over"])
    test_clean  = test.dropna(subset=valid_cols + ["miss", "went_over"])

    X_train = train_clean[valid_cols].values.astype(float)
    y_train = train_clean["miss"].values.astype(float)
    y_train_cls = train_clean["went_over"].values.astype(int)

    X_test = test_clean[valid_cols].values.astype(float)
    y_test  = test_clean["miss"].values.astype(float)
    y_test_cls = test_clean["went_over"].values.astype(int)

    results: dict = {
        "n_train": int(len(train_clean)),
        "n_test":  int(len(test_clean)),
        "date_range_train": (
            str(train_clean["game_date"].min().date()),
            str(train_clean["game_date"].max().date()),
        ),
        "date_range_test": (
            str(test_clean["game_date"].min().date()),
            str(test_clean["game_date"].max().date()),
        ),
        "models": [],
        "over_under_auc": {},
    }

    # 0. Vegas baseline
    vegas_metrics = regression_metrics(y_test, np.zeros(len(y_test)), "Vegas (predict 0)")
    results["models"].append(vegas_metrics)
    over_rate = float(y_test_cls.mean())
    results["baseline_over_rate"] = round(over_rate, 4)

    # 1. Ridge regression
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s  = scaler.transform(X_test)
    ridge = Ridge(alpha=1.0)
    ridge.fit(X_train_s, y_train)
    ridge_pred = ridge.predict(X_test_s)
    results["models"].append(regression_metrics(y_test, ridge_pred, "Ridge regression"))
    ridge_prob = 1 / (1 + np.exp(-ridge_pred / 5))  # soft sigmoid for AUC
    results["over_under_auc"]["ridge"] = over_under_auc(y_test_cls, ridge_prob)
    results["feature_importance_ridge"] = get_feature_importance(ridge, valid_cols, top_n)

    # 2. XGBoost
    if HAS_XGBOOST:
        xgb_reg = XGBRegressor(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=2.0,
            random_state=42,
            verbosity=0,
        )
        xgb_reg.fit(X_train, y_train)
        xgb_pred = xgb_reg.predict(X_test)
        results["models"].append(regression_metrics(y_test, xgb_pred, "XGBoost regressor"))
        results["feature_importance_xgb"] = get_feature_importance(xgb_reg, valid_cols, top_n)

        xgb_cls = XGBClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=2.0,
            random_state=42,
            verbosity=0,
        )
        xgb_cls.fit(X_train, y_train_cls)
        xgb_prob = xgb_cls.predict_proba(X_test)[:, 1]
        results["over_under_auc"]["xgboost"] = over_under_auc(y_test_cls, xgb_prob)
    else:
        print("  [info] xgboost not available — skipping XGBoost models")

    return results


# ── Team-level summary ────────────────────────────────────────────

def team_summary(df: pd.DataFrame) -> list[dict]:
    """Per-team Vegas accuracy: bias, over-implied rate, ATS rate."""
    home = df[["home_abbrev", "home_team_id", "home_implied", "home_score",
               "home_spread", "away_score"]].copy()
    home.columns = ["abbrev", "team_id", "implied", "score", "home_spread", "opp_score"]
    home["is_home"] = 1

    away = df[["away_abbrev", "away_team_id", "away_implied", "away_score",
               "home_spread", "home_score"]].copy()
    away.columns = ["abbrev", "team_id", "implied", "score", "home_spread", "opp_score"]
    away["is_home"] = 0

    all_ap = pd.concat([home, away], ignore_index=True)
    all_ap = all_ap.dropna(subset=["implied", "score"])
    all_ap["bias"]          = all_ap["score"] - all_ap["implied"]
    all_ap["beat_implied"]  = (all_ap["bias"] > 0).astype(int)
    all_ap["covered"] = np.where(
        all_ap["home_spread"].notna() & all_ap["opp_score"].notna(),
        np.where(
            all_ap["is_home"] == 1,
            (all_ap["score"] - all_ap["opp_score"]) > -all_ap["home_spread"],
            (all_ap["score"] - all_ap["opp_score"]) > all_ap["home_spread"],
        ),
        np.nan,
    )

    grp = all_ap.groupby("abbrev")
    summary = pd.DataFrame({
        "n":              grp["score"].count(),
        "avg_implied":    grp["implied"].mean(),
        "avg_actual":     grp["score"].mean(),
        "bias":           grp["bias"].mean(),        # + = Vegas underestimates
        "over_imp_rate":  grp["beat_implied"].mean(),
        "ats_cover_rate": grp.apply(lambda g: g["covered"].dropna().mean(), include_groups=False),
        "ats_n":          grp.apply(lambda g: g["covered"].notna().sum(), include_groups=False),
    }).reset_index()
    summary = summary.sort_values("bias", ascending=False)

    return summary.round(3).to_dict(orient="records")


# -- Report printing -──────────

def print_report(results: dict, team_stats_summary: list[dict], top_n: int) -> None:
    print("\n" + "=" * 60)
    print("NBA GAME TOTAL PREDICTION - MODEL REPORT")
    print("=" * 60)
    print(f"\nTrain: {results['n_train']} games  "
          f"({results['date_range_train'][0]} -> {results['date_range_train'][1]})")
    print(f"Test:  {results['n_test']} games  "
          f"({results['date_range_test'][0]} -> {results['date_range_test'][1]})")
    print(f"Baseline over rate: {results['baseline_over_rate'] * 100:.1f}%\n")

    print("-- Regression (predicting miss = actual - vegas_total) --")
    print(f"{'Model':<28} {'MAE':>7} {'RMSE':>7} {'Corr':>7}")
    print("-" * 52)
    for m in results["models"]:
        print(f"{m['label']:<28} {m['mae']:>7.3f} {m['rmse']:>7.3f} {m['corr']:>7.3f}")

    print("\n-- Over/Under Classification AUC -------------------------")
    for name, auc in results.get("over_under_auc", {}).items():
        label = "Ridge" if name == "ridge" else "XGBoost"
        auc_str = f"{auc:.3f}" if auc is not None else "n/a"
        bench = " <- beat 0.5 = better than coin flip" if auc and auc > 0.5 else ""
        print(f"  {label:<10}: {auc_str}{bench}")

    if "feature_importance_xgb" in results:
        print(f"\n-- XGBoost Top {top_n} Features (by importance) -------------")
        for i, item in enumerate(results["feature_importance_xgb"], 1):
            bar = "#" * int(item["importance"] * 200)
            print(f"  {i:>2}. {item['feature']:<30} {item['importance']:.4f}  {bar}")

    elif "feature_importance_ridge" in results:
        print(f"\n-- Ridge Top {top_n} Features (|coefficient|) -----------────")
        for i, item in enumerate(results["feature_importance_ridge"], 1):
            print(f"  {i:>2}. {item['feature']:<30} {item['importance']:.4f}")

    print("\n-- Team Vegas Bias (most underestimated -> most overestimated) --")
    print(f"  {'Team':<6} {'G':>4} {'AvgImp':>8} {'AvgAct':>8} "
          f"{'Bias':>7} {'>Imp%':>7} {'ATS%':>7} {'ATS n':>6}")
    print("  " + "-" * 58)
    for row in team_stats_summary:
        bias_str = f"{row['bias']:+.2f}" if row.get("bias") is not None else "-"
        over_str = f"{row['over_imp_rate'] * 100:.0f}%" if row.get("over_imp_rate") is not None else "-"
        ats_str  = f"{row['ats_cover_rate'] * 100:.0f}%" if row.get("ats_cover_rate") is not None else "-"
        print(f"  {row['abbrev']:<6} {int(row['n']):>4} "
              f"{row['avg_implied']:>8.1f} {row['avg_actual']:>8.1f} "
              f"{bias_str:>7} {over_str:>7} {ats_str:>7} {int(row.get('ats_n', 0)):>6}")

    print("\n")


# ── CLI ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NBA game-total prediction model")
    parser.add_argument("--test-fraction", type=float, default=DEFAULT_TEST_FRACTION,
                        help="Fraction of games to hold out for testing (chronological tail)")
    parser.add_argument("--min-games", type=int, default=DEFAULT_MIN_GAMES,
                        help="Minimum games required to run models")
    parser.add_argument("--top-features", type=int, default=DEFAULT_TOP_FEATURES,
                        help="Number of top features to display")
    parser.add_argument("--output", type=Path, default=None,
                        help="Optional path to write JSON results")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url)

    print("Loading data...")
    games = load_games(db)
    team_stats = load_team_stats(db)
    print(f"  {len(games)} games with odds + scores | {len(team_stats)} team-season rows")

    if len(games) < args.min_games:
        print(f"Insufficient data ({len(games)} < {args.min_games}). Exiting.")
        return

    print("Engineering features...")
    df = build_features(games, team_stats)

    # Drop rows still missing critical features after engineering
    df = df.dropna(subset=["miss", "vegas_total"]).reset_index(drop=True)
    print(f"  {len(df)} games after feature engineering")

    train_df, test_df = time_split(df, args.test_fraction)
    print(f"  Train: {len(train_df)} | Test: {len(test_df)}")

    print("Training models...")
    results = run_models(train_df, test_df, FEATURE_COLS, args.top_features)

    team_summary_data = team_summary(df)
    print_report(results, team_summary_data, args.top_features)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "model_results": results,
            "team_summary": team_summary_data,
        }
        args.output.write_text(json.dumps(payload, indent=2, default=str))
        print(f"Results written to {args.output}")


if __name__ == "__main__":
    main()
