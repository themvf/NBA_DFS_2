"""MLB moneyline model — our independent P(home win) vs the market.

Market-anchored logistic regression: predicts the home team's win probability
from the vig-free market line PLUS run-environment/strength differentials the
market may under- or over-weight (starter xFIP/K9, team wRC+/ISO, bullpen FIP).
Writes ``our_prob_home`` to ``mlb_matchups``; the edge vs the vig-free market
drives the page's moneyline recommendation and an edge-tier backtest.

The moneyline is the sharpest MLB market, so the honest expectation is a small
edge at best — the backtest is built to *prove* whether any edge survives out of
sample rather than to assume one. Same philosophy and machinery as
``model/mlb_game_total_model.py`` (the totals residual model).

Anchoring note: including the vig-free market prob as a feature means the model
starts from the line and learns residual adjustments, rather than fighting an
efficient market from scratch.

Usage:
    python -m model.mlb_moneyline_model                 # train + write today
    python -m model.mlb_moneyline_model --date 2026-06-18
    python -m model.mlb_moneyline_model --backfill 2026-03-20 2026-06-17
    python -m model.mlb_moneyline_model --evaluate
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from model.mlb_pregame import eligible_pregame_matchup_ids
from model.mlb_prediction_provenance import (
    PROSPECTIVE,
    create_prediction_run,
    record_prediction_snapshot,
)

from config import load_config
from db.database import DatabaseManager
from model.mlb_game_total_model import (
    MLB_LEAGUE_AVG_BULLPEN_FIP,
    MLB_LEAGUE_AVG_ISO,
    MLB_LEAGUE_AVG_K9,
    MLB_LEAGUE_AVG_WRC,
    load_game_data,
    snapshot_starter_context,
    snapshot_bullpen_context,
    snapshot_weather_context,
)
from model.mlb_projections import MLB_LEAGUE_AVG_XFIP

logger = logging.getLogger(__name__)

MODEL_VERSION = "mlb-ml-v1"

# Home-oriented differentials: positive = favors the home team.
FEATURE_COLS = [
    "market_home_prob",   # vig-free market anchor
    "sp_xfip_adv",        # away_xfip − home_xfip (home SP better → +)
    "sp_k9_adv",          # home_k9 − away_k9
    "wrc_adv",            # home_wrc − away_wrc
    "iso_adv",            # home_iso − away_iso
    "bullpen_adv",        # away_bullpen_fip − home_bullpen_fip
]

_MIN_TRAIN_GAMES = 80


def build_ml_features(df: pd.DataFrame) -> pd.DataFrame:
    """Home-oriented strength differentials + market anchor + win target."""
    df = df.copy()
    num = lambda c: pd.to_numeric(df[c], errors="coerce")

    df["market_home_prob"] = num("vegas_prob_home")

    h_xfip = num("home_sp_xfip").fillna(MLB_LEAGUE_AVG_XFIP)
    a_xfip = num("away_sp_xfip").fillna(MLB_LEAGUE_AVG_XFIP)
    df["sp_xfip_adv"] = a_xfip - h_xfip            # home SP better (lower) → +

    h_k9 = num("home_sp_k9").fillna(MLB_LEAGUE_AVG_K9)
    a_k9 = num("away_sp_k9").fillna(MLB_LEAGUE_AVG_K9)
    df["sp_k9_adv"] = h_k9 - a_k9

    h_wrc = num("home_wrc").fillna(MLB_LEAGUE_AVG_WRC)
    a_wrc = num("away_wrc").fillna(MLB_LEAGUE_AVG_WRC)
    df["wrc_adv"] = h_wrc - a_wrc

    h_iso = num("home_iso").fillna(MLB_LEAGUE_AVG_ISO)
    a_iso = num("away_iso").fillna(MLB_LEAGUE_AVG_ISO)
    df["iso_adv"] = h_iso - a_iso

    h_bp = num("home_bullpen_fip").fillna(MLB_LEAGUE_AVG_BULLPEN_FIP)
    a_bp = num("away_bullpen_fip").fillna(MLB_LEAGUE_AVG_BULLPEN_FIP)
    df["bullpen_adv"] = a_bp - h_bp                # home bullpen better (lower) → +

    hs = num("home_score")
    as_ = num("away_score")
    df["home_win"] = np.where(hs.notna() & as_.notna(), (hs > as_).astype("float"), np.nan)
    return df


def _fit(completed: pd.DataFrame) -> tuple[LogisticRegression, StandardScaler]:
    X = completed[FEATURE_COLS].values.astype(float)
    y = completed["home_win"].values.astype(int)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    model = LogisticRegression(C=1.0, max_iter=1000)
    model.fit(Xs, y)
    return model, scaler


def _predict_home_prob(model, scaler, rows: pd.DataFrame) -> np.ndarray:
    Xs = scaler.transform(rows[FEATURE_COLS].values.astype(float))
    return model.predict_proba(Xs)[:, 1]


def predict_and_write(db: DatabaseManager, game_date: str | None = None) -> int:
    """Train on completed games; write our_prob_home for a date's upcoming games."""
    target_date = game_date or date.today().isoformat()
    df = build_ml_features(load_game_data(db))
    if df.empty:
        logger.info("No MLB game data — skipping moneyline predictions")
        return 0
    df["game_date"] = df["game_date"].astype(str)

    feat_ok = df[FEATURE_COLS].notna().all(axis=1)
    completed = df[feat_ok & df["home_win"].notna()]
    eligible_ids = eligible_pregame_matchup_ids(db, target_date)
    upcoming = df[
        feat_ok
        & df["home_win"].isna()
        & (df["game_date"] == target_date)
        & df["id"].isin(eligible_ids)
    ]

    if len(completed) < _MIN_TRAIN_GAMES:
        logger.info("Only %d completed MLB games — too few to train moneyline", len(completed))
        return 0
    if upcoming.empty:
        logger.info("No upcoming MLB games to predict for %s", target_date)
        return 0

    model, scaler = _fit(completed)
    probs = _predict_home_prob(model, scaler, upcoming)
    scaled_upcoming = scaler.transform(upcoming[FEATURE_COLS].values.astype(float))
    trained_through = str(completed["game_date"].max()) if not completed.empty else None
    run_id = create_prediction_run(
        db,
        model_version=MODEL_VERSION,
        trained_through=trained_through,
        origin=PROSPECTIVE,
        source="predict_and_write",
        config={
            "features": FEATURE_COLS,
            "logistic_c": 1.0,
            "training_games": len(completed),
            "missingness_policy": "source-aware-v1",
            "standardized_coefficients": dict(zip(FEATURE_COLS, model.coef_[0].tolist())),
        },
    )
    updated = 0
    for (_, feature_row), p, scaled_row in zip(upcoming.iterrows(), probs, scaled_upcoming):
        mid = int(feature_row["id"])
        prediction = float(round(float(p), 4))
        home_ml = feature_row.get("home_ml")
        missingness = {
            "market_home_prob": bool(pd.isna(feature_row.get("vegas_prob_home"))),
            "sp_xfip_adv": bool(
                pd.isna(feature_row.get("home_sp_xfip"))
                or pd.isna(feature_row.get("away_sp_xfip"))
            ),
            "sp_k9_adv": bool(
                pd.isna(feature_row.get("home_sp_k9"))
                or pd.isna(feature_row.get("away_sp_k9"))
            ),
            "wrc_adv": bool(
                pd.isna(feature_row.get("home_wrc"))
                or pd.isna(feature_row.get("away_wrc"))
            ),
            "iso_adv": bool(
                pd.isna(feature_row.get("home_iso"))
                or pd.isna(feature_row.get("away_iso"))
            ),
            "bullpen_adv": bool(
                pd.isna(feature_row.get("home_bullpen_fip"))
                or pd.isna(feature_row.get("away_bullpen_fip"))
            ),
            "starter_workload": bool(
                pd.isna(feature_row.get("home_expected_innings"))
                or pd.isna(feature_row.get("away_expected_innings"))
            ),
        }
        feature_values = {col: float(feature_row[col]) for col in FEATURE_COLS}
        feature_values["starter_context"] = snapshot_starter_context(feature_row)
        feature_values["bullpen_context"] = snapshot_bullpen_context(feature_row)
        feature_values["weather_context"] = snapshot_weather_context(feature_row)
        feature_values["contributions"] = {
            col: float(coef * value)
            for col, coef, value in zip(FEATURE_COLS, model.coef_[0], scaled_row)
        }
        record_prediction_snapshot(
            db,
            run_id=run_id,
            matchup_id=mid,
            market="moneyline",
            feature_values=feature_values,
            raw_prediction=prediction,
            calibrated_probability=prediction,
            market_odds=int(home_ml) if pd.notna(home_ml) else None,
            market_prob=float(feature_row["vegas_prob_home"]),
            missingness=missingness,
        )
        db.execute(
            "UPDATE mlb_matchups SET our_prob_home = %s WHERE id = %s",
            (prediction, mid),
        )
        updated += 1

    print(f"MLB moneyline model: wrote our_prob_home for {updated} games on {target_date} "
          f"(trained on {len(completed)} completed games)")
    return updated


def backfill_predictions(db: DatabaseManager, start_date: str, end_date: str) -> int:
    """Walk-forward backfill of our_prob_home (train on strictly-prior games only)."""
    df = build_ml_features(load_game_data(db))
    df["game_date"] = df["game_date"].astype(str)
    feat_ok = df[FEATURE_COLS].notna().all(axis=1)
    has_actual = df["home_win"].notna()

    dates = sorted(d for d in df.loc[(df["game_date"] >= start_date)
                                     & (df["game_date"] <= end_date), "game_date"].unique())
    written = 0
    for d in dates:
        train = df[has_actual & feat_ok & (df["game_date"] < d)]
        if len(train) < _MIN_TRAIN_GAMES:
            continue
        targets = df[feat_ok & (df["game_date"] == d)]
        if targets.empty:
            continue
        model, scaler = _fit(train)
        probs = _predict_home_prob(model, scaler, targets)
        for mid, p in zip(targets["id"].values, probs):
            db.execute(
                "UPDATE mlb_matchups SET our_prob_home = %s WHERE id = %s",
                (float(round(float(p), 4)), int(mid)),
            )
            written += 1

    print(f"MLB moneyline model: walk-forward backfill wrote {written} predictions "
          f"across {len(dates)} dates ({start_date} to {end_date})")
    return written


def evaluate(db: DatabaseManager, test_fraction: float = 0.20) -> dict:
    """Holdout — our win-prob calibration + edge-bet ROI vs the market."""
    df = build_ml_features(load_game_data(db))
    data = df[df[FEATURE_COLS].notna().all(axis=1) & df["home_win"].notna()].reset_index(drop=True)
    if len(data) < 120:
        print(f"Only {len(data)} complete MLB games — not enough for holdout.")
        return {}

    split = int(len(data) * (1 - test_fraction))
    train, test = data.iloc[:split], data.iloc[split:]
    model, scaler = _fit(train)

    our = _predict_home_prob(model, scaler, test)
    mkt = test["market_home_prob"].values.astype(float)
    y = test["home_win"].values.astype(int)

    def log_loss(p, y_):
        p = np.clip(p, 1e-6, 1 - 1e-6)
        return float(-np.mean(y_ * np.log(p) + (1 - y_) * np.log(1 - p)))

    def brier(p, y_):
        return float(np.mean((p - y_) ** 2))

    # Edge-bet sim: bet the side where our prob beats the vig-free market by ≥ thr.
    home_ml = pd.to_numeric(test.get("home_ml"), errors="coerce").values
    away_ml = pd.to_numeric(test.get("away_ml"), errors="coerce").values

    def _profit(ml, won):
        if not won:
            return -100.0
        return float(ml) if ml >= 0 else 10000.0 / abs(float(ml))

    sims = {}
    for thr in (0.03, 0.05):
        bets = wins = 0
        profit = 0.0
        for i in range(len(test)):
            home_edge = our[i] - mkt[i]
            away_edge = (1 - our[i]) - (1 - mkt[i])  # = -home_edge
            if home_edge >= thr and not np.isnan(home_ml[i]):
                won = y[i] == 1
                profit += _profit(home_ml[i], won); bets += 1; wins += int(won)
            elif away_edge >= thr and not np.isnan(away_ml[i]):
                won = y[i] == 0
                profit += _profit(away_ml[i], won); bets += 1; wins += int(won)
        sims[f"edge_{int(thr*100)}pp"] = {
            "bets": bets,
            "win_rate": round(wins / bets, 4) if bets else None,
            "roi": round(profit / (bets * 100), 4) if bets else None,
        }

    result = {
        "model_version": MODEL_VERSION,
        "n_train": int(len(train)),
        "n_test": int(len(test)),
        "market_logloss": round(log_loss(mkt, y), 4),
        "our_logloss": round(log_loss(our, y), 4),
        "market_brier": round(brier(mkt, y), 4),
        "our_brier": round(brier(our, y), 4),
        "edge_sims": sims,
        "coefs": sorted(
            ({"feature": f, "coef": round(float(c), 4)} for f, c in zip(FEATURE_COLS, model.coef_[0])),
            key=lambda d: abs(d["coef"]), reverse=True,
        ),
    }
    print(f"\n-- MLB Moneyline Model ({MODEL_VERSION}) — holdout n={result['n_test']} --")
    print(f"  Market  logloss {result['market_logloss']:.4f}  brier {result['market_brier']:.4f}")
    print(f"  Our     logloss {result['our_logloss']:.4f}  brier {result['our_brier']:.4f}")
    print("  Edge-bet sims (bet side where our prob beats vig-free market):")
    for k, v in sims.items():
        wr = f"{v['win_rate']*100:.1f}%" if v["win_rate"] is not None else "—"
        roi = f"{v['roi']*100:+.1f}%" if v["roi"] is not None else "—"
        print(f"    {k}: {v['bets']} bets  win {wr}  ROI {roi}")
    print("  Coefs (standardized):")
    for item in result["coefs"]:
        print(f"    {item['feature']:<18} {item['coef']:+.4f}")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="MLB moneyline (win prob) model")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: today)")
    parser.add_argument("--evaluate", action="store_true", help="Print holdout evaluation and exit")
    parser.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                        help="Walk-forward backfill our_prob_home over a date range")
    parser.add_argument("--output", help="Optional path to write evaluation JSON")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.evaluate:
        res = evaluate(db)
        if args.output and res:
            from pathlib import Path
            Path(args.output).write_text(json.dumps(res, indent=2))
            print(f"Wrote {args.output}")
    elif args.backfill:
        backfill_predictions(db, args.backfill[0], args.backfill[1])
    else:
        predict_and_write(db, args.date)
