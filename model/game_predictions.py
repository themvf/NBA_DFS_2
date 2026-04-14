"""NBA game-total prediction model — trains on completed games, predicts upcoming.

Uses Ridge regression with team-efficiency + Vegas features.  Called from
ingest/nba_schedule.py after Vegas odds are fetched for the day.

Writes our_game_total_pred to nba_matchups for upcoming games.
After games complete, the stored prediction can be compared against
vegas_total and the actual total (home_score + away_score) to track
whether our model improves on Vegas over time.

Usage (standalone):
    python -m model.game_predictions --date 2026-04-15
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

SEASON = "2025-26"

FEATURE_COLS = [
    "vegas_total",
    "home_implied",
    "away_implied",
    "abs_spread",
    "home_win_prob",
    "home_pace",
    "away_pace",
    "pace_avg",
    "pace_diff",
    "home_off_rtg",
    "away_off_rtg",
    "home_def_rtg",
    "away_def_rtg",
    "def_rtg_avg",
    "def_rtg_diff",
    "net_rtg_home",
    "net_rtg_away",
    "net_rtg_diff",
    "days_rest_home",
    "days_rest_away",
    "rest_diff",
    "is_b2b_home",
    "is_b2b_away",
    "month",
    "is_weekend",
]

# League averages used as fill values when team stats are missing
_LEAGUE_AVG_PACE = 100.0
_LEAGUE_AVG_RTG = 114.5


def load_game_data(db: DatabaseManager, season: str = SEASON) -> pd.DataFrame:
    """Load all NBA matchups with team stats for the season.

    Returns rows for both completed games (home_score not null) and
    upcoming games (home_score null) so the caller can split them.
    """
    rows = db.execute(
        """
        SELECT
            nm.id,
            nm.game_date::TEXT            AS game_date,
            nm.vegas_total,
            nm.home_implied,
            nm.away_implied,
            nm.home_spread,
            nm.vegas_prob_home,
            nm.home_score,
            nm.away_score,
            t_home.abbreviation           AS home_abbrev,
            t_away.abbreviation           AS away_abbrev,
            ts_home.pace                  AS home_pace,
            ts_home.off_rtg               AS home_off_rtg,
            ts_home.def_rtg               AS home_def_rtg,
            ts_away.pace                  AS away_pace,
            ts_away.off_rtg               AS away_off_rtg,
            ts_away.def_rtg               AS away_def_rtg
        FROM nba_matchups nm
        JOIN teams t_home ON t_home.team_id = nm.home_team_id
        JOIN teams t_away ON t_away.team_id = nm.away_team_id
        LEFT JOIN nba_team_stats ts_home
               ON ts_home.team_id = nm.home_team_id AND ts_home.season = %s
        LEFT JOIN nba_team_stats ts_away
               ON ts_away.team_id = nm.away_team_id AND ts_away.season = %s
        WHERE nm.vegas_total IS NOT NULL
        ORDER BY nm.game_date ASC
        """,
        (season, season),
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _add_rest_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute days-rest for home and away teams (look-ahead safe).

    We iterate chronologically and track the last game date per team.
    A team's rest for the current game is based only on prior games.
    Teams with no prior game in the dataset get a default of 5 rest days.
    """
    df = df.copy()
    df = df.sort_values("game_date").reset_index(drop=True)

    last_game: dict[str, pd.Timestamp] = {}
    days_rest_home: list[int] = []
    days_rest_away: list[int] = []

    for _, row in df.iterrows():
        gd = row["game_date"]
        ha, aa = row["home_abbrev"], row["away_abbrev"]

        lh = last_game.get(ha)
        la = last_game.get(aa)
        days_rest_home.append(int((gd - lh).days) if lh is not None else 5)
        days_rest_away.append(int((gd - la).days) if la is not None else 5)

        last_game[ha] = gd
        last_game[aa] = gd

    df["days_rest_home"] = days_rest_home
    df["days_rest_away"] = days_rest_away
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute derived columns required by FEATURE_COLS."""
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])

    # Fill missing team stats with league averages
    for col, fill in [
        ("home_pace", _LEAGUE_AVG_PACE),
        ("away_pace", _LEAGUE_AVG_PACE),
        ("home_off_rtg", _LEAGUE_AVG_RTG),
        ("away_off_rtg", _LEAGUE_AVG_RTG),
        ("home_def_rtg", _LEAGUE_AVG_RTG),
        ("away_def_rtg", _LEAGUE_AVG_RTG),
        ("home_implied", None),
        ("away_implied", None),
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if fill is not None:
                df[col] = df[col].fillna(fill)

    # Fill implied totals from vegas_total / 2 when missing
    half_total = df["vegas_total"] / 2
    df["home_implied"] = df["home_implied"].fillna(half_total)
    df["away_implied"] = df["away_implied"].fillna(half_total)

    # Vegas-derived
    df["abs_spread"] = pd.to_numeric(df["home_spread"], errors="coerce").abs().fillna(0.0)
    df["home_win_prob"] = pd.to_numeric(df["vegas_prob_home"], errors="coerce").fillna(0.5)

    # Pace features
    df["pace_avg"] = (df["home_pace"] + df["away_pace"]) / 2
    df["pace_diff"] = df["home_pace"] - df["away_pace"]

    # Rating features
    df["def_rtg_avg"] = (df["home_def_rtg"] + df["away_def_rtg"]) / 2
    df["def_rtg_diff"] = df["home_def_rtg"] - df["away_def_rtg"]
    df["net_rtg_home"] = df["home_off_rtg"] - df["away_def_rtg"]
    df["net_rtg_away"] = df["away_off_rtg"] - df["home_def_rtg"]
    df["net_rtg_diff"] = df["net_rtg_home"] - df["net_rtg_away"]

    # Rest features (look-ahead safe)
    df = _add_rest_features(df)
    df["rest_diff"] = df["days_rest_home"] - df["days_rest_away"]
    df["is_b2b_home"] = (df["days_rest_home"] == 1).astype(int)
    df["is_b2b_away"] = (df["days_rest_away"] == 1).astype(int)

    # Calendar
    df["month"] = df["game_date"].dt.month
    df["is_weekend"] = df["game_date"].dt.dayofweek.isin([5, 6]).astype(int)

    return df


def predict_and_write(
    db: DatabaseManager,
    season: str = SEASON,
    game_date: str | None = None,
) -> int:
    """Train Ridge on completed games; write predictions for upcoming games.

    - Target: actual_total - vegas_total  (the Vegas miss)
    - Prediction: vegas_total + predicted_miss  (our adjusted total)
    - Only writes to games on game_date that have no score yet.

    Returns number of matchups updated.
    """
    target_date = game_date or date.today().isoformat()

    df = load_game_data(db, season)
    if df.empty:
        logger.info("No game data available — skipping predictions")
        return 0

    df = build_features(df)

    # Actual total for completed games
    df["actual_total"] = pd.to_numeric(df["home_score"], errors="coerce") + \
                         pd.to_numeric(df["away_score"], errors="coerce")

    # Split: completed games with all features present (for training)
    completed = df.dropna(subset=["actual_total", "vegas_total"] + FEATURE_COLS)

    # Upcoming games on target_date without a score
    upcoming = df[
        (df["game_date"].dt.strftime("%Y-%m-%d") == target_date) &
        df["actual_total"].isna() &
        df[FEATURE_COLS].notna().all(axis=1)
    ]

    if completed.shape[0] < 20:
        logger.info("Only %d completed games — too few to train reliably", completed.shape[0])
        return 0

    if upcoming.empty:
        logger.info("No upcoming games to predict for %s", target_date)
        return 0

    # Train Ridge: predict the Vegas miss (actual - vegas)
    X_train = completed[FEATURE_COLS].values.astype(float)
    y_train = (completed["actual_total"] - completed["vegas_total"]).values.astype(float)

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)

    model = Ridge(alpha=1.0)
    model.fit(X_train_s, y_train)

    # Predict upcoming games
    X_pred = upcoming[FEATURE_COLS].values.astype(float)
    X_pred_s = scaler.transform(X_pred)
    miss_pred = model.predict(X_pred_s)
    our_totals = upcoming["vegas_total"].values.astype(float) + miss_pred

    # Write predictions
    updated = 0
    for matchup_id, pred in zip(upcoming["id"].values, our_totals):
        db.execute(
            "UPDATE nba_matchups SET our_game_total_pred = %s WHERE id = %s",
            (float(round(float(pred), 1)), int(matchup_id)),
        )
        updated += 1

    logger.info(
        "Game total predictions written: %d games for %s (trained on %d completed games)",
        updated, target_date, len(completed),
    )
    return updated


def evaluate(db: DatabaseManager, season: str = SEASON) -> None:
    """Print model MAE vs Vegas baseline on completed games (holdout = last 20%)."""
    df = load_game_data(db, season)
    if df.empty:
        print("No data.")
        return

    df = build_features(df)
    df["actual_total"] = pd.to_numeric(df["home_score"], errors="coerce") + \
                         pd.to_numeric(df["away_score"], errors="coerce")

    data = df.dropna(subset=["actual_total", "vegas_total"] + FEATURE_COLS).copy()
    if len(data) < 30:
        print(f"Only {len(data)} complete games — not enough for holdout evaluation.")
        return

    split = int(len(data) * 0.80)
    train, test = data.iloc[:split], data.iloc[split:]

    X_tr = train[FEATURE_COLS].values.astype(float)
    y_tr = (train["actual_total"] - train["vegas_total"]).values.astype(float)
    X_te = test[FEATURE_COLS].values.astype(float)
    y_te = test["actual_total"].values.astype(float)
    v_te = test["vegas_total"].values.astype(float)

    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_tr)
    X_te_s = scaler.transform(X_te)

    model = Ridge(alpha=1.0)
    model.fit(X_tr_s, y_tr)
    our_pred = v_te + model.predict(X_te_s)

    vegas_mae = float(np.mean(np.abs(v_te - y_te)))
    our_mae   = float(np.mean(np.abs(our_pred - y_te)))
    vegas_bias = float(np.mean(v_te - y_te))
    our_bias   = float(np.mean(our_pred - y_te))

    print(f"\n-- Game Total Model Evaluation (n={len(test)} holdout games) --")
    print(f"  Vegas baseline  MAE: {vegas_mae:.2f}  Bias: {vegas_bias:+.2f}")
    print(f"  Our model       MAE: {our_mae:.2f}  Bias: {our_bias:+.2f}")
    print(f"  Improvement:    {vegas_mae - our_mae:+.2f} pts/game")

    # Top coefficient features
    coefs = sorted(
        zip(FEATURE_COLS, model.coef_),
        key=lambda x: abs(x[1]),
        reverse=True,
    )[:8]
    print("\n  Top features (Ridge coef):")
    for fname, coef in coefs:
        print(f"    {fname:<25} {coef:+.4f}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="NBA game-total prediction model")
    parser.add_argument("--date",     help="Game date YYYY-MM-DD (default: today)")
    parser.add_argument("--season",   default=SEASON, help="Season string")
    parser.add_argument("--evaluate", action="store_true",
                        help="Print holdout evaluation instead of writing predictions")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.evaluate:
        evaluate(db, args.season)
    else:
        n = predict_and_write(db, args.season, args.date)
        print(f"Predictions written: {n} games")
