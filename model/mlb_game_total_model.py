"""MLB game-total (O/U) model — our own number vs the market.

Trains a Ridge regression on completed games to predict the **Vegas miss**
(actual_total − vegas_total) from run-environment features, then writes
``our_total_pred = vegas_total + predicted_miss`` to ``mlb_matchups`` for
upcoming games.  Value comes from *disagreement* with the line, not from
fighting an efficient market — the same residual-over-Vegas philosophy as
``model/game_predictions.py`` (NBA) and ``model/soccer_predictions.py``.

This replaces the page's prior hand-weighted O/U heuristic (~52% accuracy)
with an independent, auditable number, and pulls in run-environment context
that was sitting unused in the DB: starter xFIP/K9, park run factor, weather,
**team wRC+/ISO offense strength, and bullpen ERA/FIP**.

Features (all available pre-game):
    Line inputs    : vegas_total, home_implied, away_implied, abs_spread, home_win_prob
    Starters       : avg/diff xFIP, avg K/9 (run suppression)
    Park + weather : park_runs_factor, temp delta, directional wind component
    Offense        : home/away team_wrc_plus, team_iso, combined
    Bullpen        : home/away bullpen_fip (relief run prevention)

Usage:
    python -m model.mlb_game_total_model               # train + write today's preds
    python -m model.mlb_game_total_model --date 2026-06-18
    python -m model.mlb_game_total_model --evaluate    # holdout MAE vs Vegas baseline
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from model.mlb_pregame import eligible_pregame_matchup_ids
from model.mlb_prediction_provenance import (
    PROSPECTIVE,
    create_prediction_run,
    record_prediction_snapshot,
)

from config import load_config
from db.database import DatabaseManager
from model.mlb_projections import (
    MLB_LEAGUE_AVG_XFIP,
    _wind_directional_lean,
)

logger = logging.getLogger(__name__)

MODEL_VERSION = "mlb-total-v1"
MLB_LEAGUE_AVG_K9 = 8.4    # starter K/9 league baseline (2026)
MLB_LEAGUE_AVG_WRC = 100.0
MLB_LEAGUE_AVG_ISO = 0.165
MLB_LEAGUE_AVG_BULLPEN_FIP = 4.20

FEATURE_COLS = [
    # Vegas line inputs
    "vegas_total",
    "home_implied",
    "away_implied",
    "abs_spread",
    "home_win_prob",
    # Starting pitcher run-suppression
    "sp_xfip_avg",
    "sp_xfip_diff",
    "sp_k9_avg",
    # Park + weather
    "park_runs_factor",
    "temp_delta",
    "wind_component",
    # Offense strength (was unused by the old O/U score)
    "wrc_avg",
    "iso_avg",
    # Bullpen run prevention (was unused)
    "bullpen_fip_avg",
]

# Minimum completed games before we trust the model enough to write predictions.
_MIN_TRAIN_GAMES = 60


def snapshot_starter_context(row: pd.Series) -> dict:
    keys = (
        "home_sp_hand", "away_sp_hand", "home_sp_bb9", "away_sp_bb9",
        "home_season_ip_per_start", "away_season_ip_per_start",
        "home_pitches_last_start", "away_pitches_last_start",
        "home_avg_pitches_last_3", "away_avg_pitches_last_3",
        "home_expected_innings", "away_expected_innings",
        "home_starter_days_rest", "away_starter_days_rest",
        "home_offense_split_players", "away_offense_split_players",
    )
    result = {}
    for key in keys:
        value = row.get(key)
        result[key] = None if value is None or pd.isna(value) else value
    return result


def snapshot_bullpen_context(row: pd.Series) -> dict:
    keys = (
        "home_bullpen_era", "away_bullpen_era", "home_bullpen_fip", "away_bullpen_fip",
        "home_bullpen_k_pct", "away_bullpen_k_pct", "home_bullpen_bb_pct", "away_bullpen_bb_pct",
        "home_bullpen_pitches_1d", "away_bullpen_pitches_1d",
        "home_bullpen_pitches_3d", "away_bullpen_pitches_3d",
        "home_relievers_back_to_back", "away_relievers_back_to_back",
        "home_bullpen_quality_outs", "away_bullpen_quality_outs",
    )
    result = {}
    for key in keys:
        value = row.get(key)
        result[key] = None if value is None or pd.isna(value) else value
    return result


def _season_of(game_date: str) -> str:
    """MLB season is the calendar year."""
    return game_date[:4]


def load_game_data(db: DatabaseManager) -> pd.DataFrame:
    """Load all MLB matchups joined with SP + team-stat context AS OF each
    game's own date.

    Returns both completed (home_score not null) and upcoming games so the
    caller can split them.  Starter stats are joined by id with a name
    fallback; team offense/bullpen come from mlb_team_stats_history.

    Point-in-time note (fixed 2026-07-05): mlb_team_stats/mlb_pitcher_stats
    are single current-state rows per (team/player, season), overwritten
    daily — joining them directly leaks future-in-season stats into
    predictions for earlier games (see CLAUDE.md "MLB Moneyline —
    Point-in-Time Leak Finding"). This joins the append-only
    mlb_team_stats_history/mlb_pitcher_stats_history tables via LATERAL,
    picking the latest snapshot at or before the game's own date. No
    snapshot exists for dates before the history tables started being
    populated, so those rows correctly come back NULL rather than leaking.
    """
    rows = db.execute(
        """
        WITH latest_park AS (
            SELECT DISTINCT ON (team_id) team_id, runs_factor
            FROM mlb_park_factors
            ORDER BY team_id, season DESC, id DESC
        )
        SELECT
            m.id,
            m.game_date::TEXT          AS game_date,
            m.commence_time,
            m.vegas_total,
            m.home_implied,
            m.away_implied,
            m.home_spread,
            m.home_ml,
            m.away_ml,
            m.vegas_prob_home,
            m.home_score,
            m.away_score,
            m.ballpark,
            m.weather_temp,
            m.wind_speed,
            m.wind_direction,
            park.runs_factor           AS park_runs_factor,
            COALESCE(hsp_id.xfip, hsp_nm.xfip)                               AS home_sp_xfip,
            COALESCE(asp_id.xfip, asp_nm.xfip)                               AS away_sp_xfip,
            COALESCE(hsp_id.k_per_9, hsp_nm.k_per_9)                          AS home_sp_k9,
            COALESCE(asp_id.k_per_9, asp_nm.k_per_9)                          AS away_sp_k9,
            COALESCE(hsp_id.bb_per_9, hsp_nm.bb_per_9)                        AS home_sp_bb9,
            COALESCE(asp_id.bb_per_9, asp_nm.bb_per_9)                        AS away_sp_bb9,
            COALESCE(hsp_id.hand, hsp_nm.hand)                                AS home_sp_hand,
            COALESCE(asp_id.hand, asp_nm.hand)                                AS away_sp_hand,
            COALESCE(hsp_id.ip_per_start, hsp_nm.ip_per_start)                AS home_season_ip_per_start,
            COALESCE(asp_id.ip_per_start, asp_nm.ip_per_start)                AS away_season_ip_per_start,
            hwl.pitches_last_start AS home_pitches_last_start,
            awl.pitches_last_start AS away_pitches_last_start,
            hwl.avg_pitches_last_3 AS home_avg_pitches_last_3,
            awl.avg_pitches_last_3 AS away_avg_pitches_last_3,
            hwl.expected_innings AS home_expected_innings,
            awl.expected_innings AS away_expected_innings,
            hwl.days_rest AS home_starter_days_rest,
            awl.days_rest AS away_starter_days_rest,
            COALESCE(
              CASE COALESCE(asp_id.hand, asp_nm.hand)
                WHEN 'L' THEN hos.wrc_plus_vs_l WHEN 'R' THEN hos.wrc_plus_vs_r END,
              hts.team_wrc_plus
            ) AS home_wrc,
            COALESCE(
              CASE COALESCE(hsp_id.hand, hsp_nm.hand)
                WHEN 'L' THEN aos.wrc_plus_vs_l WHEN 'R' THEN aos.wrc_plus_vs_r END,
              ats.team_wrc_plus
            ) AS away_wrc,
            CASE COALESCE(asp_id.hand, asp_nm.hand)
              WHEN 'L' THEN hos.players_vs_l WHEN 'R' THEN hos.players_vs_r END AS home_offense_split_players,
            CASE COALESCE(hsp_id.hand, hsp_nm.hand)
              WHEN 'L' THEN aos.players_vs_l WHEN 'R' THEN aos.players_vs_r END AS away_offense_split_players,
            hts.team_iso               AS home_iso,
            ats.team_iso               AS away_iso,
            hbp.reliever_era AS home_bullpen_era,
            abp.reliever_era AS away_bullpen_era,
            hbp.reliever_fip AS home_bullpen_fip,
            abp.reliever_fip AS away_bullpen_fip,
            hbp.reliever_k_pct AS home_bullpen_k_pct,
            abp.reliever_k_pct AS away_bullpen_k_pct,
            hbp.reliever_bb_pct AS home_bullpen_bb_pct,
            abp.reliever_bb_pct AS away_bullpen_bb_pct,
            hbp.pitches_1d AS home_bullpen_pitches_1d,
            abp.pitches_1d AS away_bullpen_pitches_1d,
            hbp.pitches_3d AS home_bullpen_pitches_3d,
            abp.pitches_3d AS away_bullpen_pitches_3d,
            hbp.relievers_back_to_back AS home_relievers_back_to_back,
            abp.relievers_back_to_back AS away_relievers_back_to_back,
            hbp.quality_outs AS home_bullpen_quality_outs,
            abp.quality_outs AS away_bullpen_quality_outs
        FROM mlb_matchups m
        LEFT JOIN LATERAL (
            SELECT k_per_9, bb_per_9, xfip, hand, ip_per_start FROM mlb_pitcher_stats_history h
            WHERE h.player_id = m.home_sp_id AND h.available_at < m.commence_time
              AND h.stats_through_at < m.commence_time
            ORDER BY h.available_at DESC, h.id DESC LIMIT 1
        ) hsp_id ON TRUE
        LEFT JOIN LATERAL (
            SELECT k_per_9, bb_per_9, xfip, hand, ip_per_start FROM mlb_pitcher_stats_history h
            WHERE h.player_id = m.away_sp_id AND h.available_at < m.commence_time
              AND h.stats_through_at < m.commence_time
            ORDER BY h.available_at DESC, h.id DESC LIMIT 1
        ) asp_id ON TRUE
        LEFT JOIN LATERAL (
            SELECT k_per_9, bb_per_9, xfip, hand, ip_per_start FROM mlb_pitcher_stats_history h
            WHERE LOWER(h.name) = LOWER(m.home_sp_name) AND h.available_at < m.commence_time
              AND h.stats_through_at < m.commence_time
            ORDER BY h.available_at DESC, h.id DESC LIMIT 1
        ) hsp_nm ON TRUE
        LEFT JOIN LATERAL (
            SELECT k_per_9, bb_per_9, xfip, hand, ip_per_start FROM mlb_pitcher_stats_history h
            WHERE LOWER(h.name) = LOWER(m.away_sp_name) AND h.available_at < m.commence_time
              AND h.stats_through_at < m.commence_time
            ORDER BY h.available_at DESC, h.id DESC LIMIT 1
        ) asp_nm ON TRUE
        LEFT JOIN latest_park park ON park.team_id = m.home_team_id
        LEFT JOIN LATERAL (
            SELECT team_wrc_plus, team_iso, bullpen_fip FROM mlb_team_stats_history h
            WHERE h.team_id = m.home_team_id AND h.season = LEFT(m.game_date::TEXT, 4)
              AND h.available_at < m.commence_time AND h.stats_through_at < m.commence_time
            ORDER BY h.available_at DESC, h.id DESC LIMIT 1
        ) hts ON TRUE
        LEFT JOIN LATERAL (
            SELECT team_wrc_plus, team_iso, bullpen_fip FROM mlb_team_stats_history h
            WHERE h.team_id = m.away_team_id AND h.season = LEFT(m.game_date::TEXT, 4)
              AND h.available_at < m.commence_time AND h.stats_through_at < m.commence_time
            ORDER BY h.available_at DESC, h.id DESC LIMIT 1
        ) ats ON TRUE
        LEFT JOIN LATERAL (
            SELECT wrc_plus_vs_l, wrc_plus_vs_r, players_vs_l, players_vs_r
            FROM mlb_team_offense_split_snapshots s
            WHERE s.team_id = m.home_team_id AND s.season = LEFT(m.game_date::TEXT, 4)
              AND s.available_at < m.commence_time AND s.stats_through_at < m.commence_time
            ORDER BY s.available_at DESC, s.id DESC LIMIT 1
        ) hos ON TRUE
        LEFT JOIN LATERAL (
            SELECT wrc_plus_vs_l, wrc_plus_vs_r, players_vs_l, players_vs_r
            FROM mlb_team_offense_split_snapshots s
            WHERE s.team_id = m.away_team_id AND s.season = LEFT(m.game_date::TEXT, 4)
              AND s.available_at < m.commence_time AND s.stats_through_at < m.commence_time
            ORDER BY s.available_at DESC, s.id DESC LIMIT 1
        ) aos ON TRUE
        LEFT JOIN LATERAL (
            SELECT pitches_last_start, avg_pitches_last_3, expected_innings, days_rest
            FROM mlb_starter_workload_snapshots w
            WHERE w.matchup_id = m.id AND w.side = 'home' AND w.available_at < m.commence_time
            ORDER BY w.available_at DESC, w.id DESC LIMIT 1
        ) hwl ON TRUE
        LEFT JOIN LATERAL (
            SELECT pitches_last_start, avg_pitches_last_3, expected_innings, days_rest
            FROM mlb_starter_workload_snapshots w
            WHERE w.matchup_id = m.id AND w.side = 'away' AND w.available_at < m.commence_time
            ORDER BY w.available_at DESC, w.id DESC LIMIT 1
        ) awl ON TRUE
        LEFT JOIN LATERAL (
            SELECT reliever_era, reliever_fip, reliever_k_pct, reliever_bb_pct,
                   pitches_1d, pitches_3d, relievers_back_to_back, quality_outs
            FROM mlb_bullpen_snapshots b
            WHERE b.matchup_id = m.id AND b.team_id = m.home_team_id
              AND b.available_at < m.commence_time
            ORDER BY b.available_at DESC, b.id DESC LIMIT 1
        ) hbp ON TRUE
        LEFT JOIN LATERAL (
            SELECT reliever_era, reliever_fip, reliever_k_pct, reliever_bb_pct,
                   pitches_1d, pitches_3d, relievers_back_to_back, quality_outs
            FROM mlb_bullpen_snapshots b
            WHERE b.matchup_id = m.id AND b.team_id = m.away_team_id
              AND b.available_at < m.commence_time
            ORDER BY b.available_at DESC, b.id DESC LIMIT 1
        ) abp ON TRUE
        WHERE m.vegas_total IS NOT NULL
        ORDER BY m.game_date ASC
        """,
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the FEATURE_COLS, filling missing context with league averages."""
    df = df.copy()

    num = lambda c: pd.to_numeric(df[c], errors="coerce")

    # Preserve source availability before deterministic research fallbacks are
    # applied. Prediction snapshots use these flags so the decision UI cannot
    # mistake a league-average substitute for observed pregame information.
    df["_missing_home_implied"] = num("home_implied").isna()
    df["_missing_away_implied"] = num("away_implied").isna()
    df["_missing_home_spread"] = num("home_spread").isna()
    df["_missing_home_win_prob"] = num("vegas_prob_home").isna()
    df["_missing_park_runs_factor"] = num("park_runs_factor").isna()

    df["vegas_total"] = num("vegas_total")
    half = df["vegas_total"] / 2.0
    df["home_implied"] = num("home_implied").fillna(half)
    df["away_implied"] = num("away_implied").fillna(half)
    df["abs_spread"] = num("home_spread").abs().fillna(0.0)
    df["home_win_prob"] = num("vegas_prob_home").fillna(0.5)

    # Starter run-suppression — xFIP (lower = better), fill league avg.
    h_xfip = num("home_sp_xfip").fillna(MLB_LEAGUE_AVG_XFIP)
    a_xfip = num("away_sp_xfip").fillna(MLB_LEAGUE_AVG_XFIP)
    df["sp_xfip_avg"] = (h_xfip + a_xfip) / 2.0
    df["sp_xfip_diff"] = (h_xfip - a_xfip).abs()
    h_k9 = num("home_sp_k9").fillna(MLB_LEAGUE_AVG_K9)
    a_k9 = num("away_sp_k9").fillna(MLB_LEAGUE_AVG_K9)
    df["sp_k9_avg"] = (h_k9 + a_k9) / 2.0

    # Park + weather.
    df["park_runs_factor"] = num("park_runs_factor").fillna(1.0).clip(0.70, 1.30)
    temp = num("weather_temp")
    df["temp_delta"] = ((temp - 72.0) / 18.0).clip(-1.0, 1.0).fillna(0.0)

    # Directional wind: signed component (+out / −in) scaled by speed.
    wind_speed = num("wind_speed").fillna(0.0)
    leans = [
        _wind_directional_lean(bp, wd)
        for bp, wd in zip(df["ballpark"], df["wind_direction"])
    ]
    df["wind_component"] = (
        pd.Series(leans, index=df.index).astype(float)
        * (wind_speed.clip(0, 20) / 20.0)
    )

    # Offense strength (previously unused by the O/U score).
    h_wrc = num("home_wrc").fillna(MLB_LEAGUE_AVG_WRC)
    a_wrc = num("away_wrc").fillna(MLB_LEAGUE_AVG_WRC)
    df["wrc_avg"] = (h_wrc + a_wrc) / 2.0
    h_iso = num("home_iso").fillna(MLB_LEAGUE_AVG_ISO)
    a_iso = num("away_iso").fillna(MLB_LEAGUE_AVG_ISO)
    df["iso_avg"] = (h_iso + a_iso) / 2.0

    # Bullpen run prevention (previously unused).
    h_bp = num("home_bullpen_fip").fillna(MLB_LEAGUE_AVG_BULLPEN_FIP)
    a_bp = num("away_bullpen_fip").fillna(MLB_LEAGUE_AVG_BULLPEN_FIP)
    df["bullpen_fip_avg"] = (h_bp + a_bp) / 2.0

    df["actual_total"] = num("home_score") + num("away_score")
    return df


def _fit(completed: pd.DataFrame) -> tuple[Ridge, StandardScaler]:
    """Fit Ridge on the Vegas miss."""
    X = completed[FEATURE_COLS].values.astype(float)
    y = (completed["actual_total"] - completed["vegas_total"]).values.astype(float)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    model = Ridge(alpha=2.0)
    model.fit(Xs, y)
    return model, scaler


def predict_and_write(db: DatabaseManager, game_date: str | None = None) -> int:
    """Train on completed games; write our_total_pred for upcoming games on a date."""
    target_date = game_date or date.today().isoformat()
    df = load_game_data(db)
    if df.empty:
        logger.info("No MLB game data — skipping total predictions")
        return 0

    df = build_features(df)
    eligible_ids = eligible_pregame_matchup_ids(db, target_date)
    completed = df.dropna(subset=["actual_total", "vegas_total"] + FEATURE_COLS)
    upcoming = df[
        (df["game_date"] == target_date)
        & df["id"].isin(eligible_ids)
        & df["actual_total"].isna()
        & df[FEATURE_COLS].notna().all(axis=1)
    ]

    if len(completed) < _MIN_TRAIN_GAMES:
        logger.info("Only %d completed MLB games — too few to train", len(completed))
        return 0
    if upcoming.empty:
        logger.info("No upcoming MLB games to predict for %s", target_date)
        return 0

    model, scaler = _fit(completed)
    X_pred = scaler.transform(upcoming[FEATURE_COLS].values.astype(float))
    # Clamp the miss so a noisy feature row can't produce an absurd total.
    miss_pred = np.clip(model.predict(X_pred), -3.0, 3.0)
    our_totals = upcoming["vegas_total"].values.astype(float) + miss_pred

    trained_through = str(completed["game_date"].max()) if not completed.empty else None
    run_id = create_prediction_run(
        db,
        model_version=MODEL_VERSION,
        trained_through=trained_through,
        origin=PROSPECTIVE,
        source="predict_and_write",
        config={
            "features": FEATURE_COLS,
            "ridge_alpha": 2.0,
            "training_games": len(completed),
            "missingness_policy": "source-aware-v1",
            "standardized_coefficients": dict(zip(FEATURE_COLS, model.coef_.tolist())),
        },
    )
    updated = 0
    for (_, feature_row), pred, scaled_row in zip(upcoming.iterrows(), our_totals, X_pred):
        matchup_id = int(feature_row["id"])
        prediction = float(round(float(pred), 2))
        missingness = {
            "vegas_total": bool(pd.isna(feature_row.get("vegas_total"))),
            "home_implied": bool(feature_row.get("_missing_home_implied")),
            "away_implied": bool(feature_row.get("_missing_away_implied")),
            "abs_spread": bool(feature_row.get("_missing_home_spread")),
            "home_win_prob": bool(feature_row.get("_missing_home_win_prob")),
            "sp_xfip_avg": bool(
                pd.isna(feature_row.get("home_sp_xfip"))
                or pd.isna(feature_row.get("away_sp_xfip"))
            ),
            "sp_xfip_diff": bool(
                pd.isna(feature_row.get("home_sp_xfip"))
                or pd.isna(feature_row.get("away_sp_xfip"))
            ),
            "sp_k9_avg": bool(
                pd.isna(feature_row.get("home_sp_k9"))
                or pd.isna(feature_row.get("away_sp_k9"))
            ),
            "park_runs_factor": bool(feature_row.get("_missing_park_runs_factor")),
            "temp_delta": bool(pd.isna(feature_row.get("weather_temp"))),
            "wind_component": bool(
                pd.isna(feature_row.get("wind_speed"))
                or pd.isna(feature_row.get("wind_direction"))
            ),
            "wrc_avg": bool(
                pd.isna(feature_row.get("home_wrc"))
                or pd.isna(feature_row.get("away_wrc"))
            ),
            "iso_avg": bool(
                pd.isna(feature_row.get("home_iso"))
                or pd.isna(feature_row.get("away_iso"))
            ),
            "bullpen_fip_avg": bool(
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
        feature_values["contributions"] = {
            col: float(coef * value)
            for col, coef, value in zip(FEATURE_COLS, model.coef_, scaled_row)
        }
        record_prediction_snapshot(
            db,
            run_id=run_id,
            matchup_id=matchup_id,
            market="total",
            feature_values=feature_values,
            raw_prediction=prediction,
            market_line=float(feature_row["vegas_total"]),
            market_prob=0.5,
            missingness=missingness,
        )
        db.execute(
            "UPDATE mlb_matchups SET our_total_pred = %s WHERE id = %s",
            (prediction, matchup_id),
        )
        updated += 1

    logger.info(
        "MLB total predictions: %d games for %s (trained on %d completed)",
        updated, target_date, len(completed),
    )
    print(f"MLB total model: wrote our_total_pred for {updated} games on {target_date} "
          f"(trained on {len(completed)} completed games)")
    return updated


def backfill_predictions(db: DatabaseManager, start_date: str, end_date: str) -> int:
    """Walk-forward backfill of our_total_pred over a historical date range.

    For each game date in [start_date, end_date], the model is trained ONLY on
    games that completed strictly before that date, then predicts that date's
    games.  This is look-ahead-safe, so the resulting ``our_total_pred`` on
    already-completed games forms an honest out-of-sample track record for the
    backtest (no future information leaks into any prediction).

    Returns the number of games written.
    """
    df = build_features(load_game_data(db))
    df["game_date"] = df["game_date"].astype(str)
    feat_ok = df[FEATURE_COLS].notna().all(axis=1)
    has_actual = df["actual_total"].notna() & df["vegas_total"].notna()

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
        miss = np.clip(model.predict(scaler.transform(targets[FEATURE_COLS].values.astype(float))), -3.0, 3.0)
        preds = targets["vegas_total"].values.astype(float) + miss
        for mid, pred in zip(targets["id"].values, preds):
            db.execute(
                "UPDATE mlb_matchups SET our_total_pred = %s WHERE id = %s",
                (float(round(float(pred), 2)), int(mid)),
            )
            written += 1

    print(f"MLB total model: walk-forward backfill wrote {written} predictions "
          f"across {len(dates)} dates ({start_date} to {end_date})")
    return written


def evaluate(db: DatabaseManager, test_fraction: float = 0.20) -> dict:
    """Chronological holdout — our MAE/bias vs the Vegas baseline."""
    df = build_features(load_game_data(db))
    data = df.dropna(subset=["actual_total", "vegas_total"] + FEATURE_COLS).reset_index(drop=True)
    if len(data) < 80:
        print(f"Only {len(data)} complete MLB games — not enough for holdout.")
        return {}

    split = int(len(data) * (1 - test_fraction))
    train, test = data.iloc[:split], data.iloc[split:]
    model, scaler = _fit(train)

    y_te = test["actual_total"].values.astype(float)
    v_te = test["vegas_total"].values.astype(float)
    our = v_te + np.clip(model.predict(scaler.transform(test[FEATURE_COLS].values.astype(float))), -3.0, 3.0)

    vegas_mae = float(np.mean(np.abs(v_te - y_te)))
    our_mae = float(np.mean(np.abs(our - y_te)))
    # O/U side accuracy: did we pick the correct side of the line vs the actual?
    our_side = np.sign(our - v_te)
    actual_side = np.sign(y_te - v_te)
    decided = actual_side != 0
    ou_acc = float(np.mean(our_side[decided] == actual_side[decided])) if decided.any() else float("nan")

    result = {
        "model_version": MODEL_VERSION,
        "n_train": int(len(train)),
        "n_test": int(len(test)),
        "vegas_mae": round(vegas_mae, 3),
        "our_mae": round(our_mae, 3),
        "mae_improvement": round(vegas_mae - our_mae, 3),
        "vegas_bias": round(float(np.mean(v_te - y_te)), 3),
        "our_bias": round(float(np.mean(our - y_te)), 3),
        "ou_side_accuracy": round(ou_acc, 4),
        "top_features": sorted(
            ({"feature": f, "coef": round(float(c), 4)} for f, c in zip(FEATURE_COLS, model.coef_)),
            key=lambda d: abs(d["coef"]), reverse=True,
        )[:8],
    }
    print(f"\n-- MLB Game Total Model ({MODEL_VERSION}) — holdout n={result['n_test']} --")
    print(f"  Vegas baseline  MAE {result['vegas_mae']:.2f}  bias {result['vegas_bias']:+.2f}")
    print(f"  Our model       MAE {result['our_mae']:.2f}  bias {result['our_bias']:+.2f}")
    print(f"  Improvement     {result['mae_improvement']:+.2f} runs/game")
    print(f"  O/U side accuracy: {result['ou_side_accuracy'] * 100:.1f}%  (>50% beats the line)")
    print("  Top features (Ridge coef):")
    for item in result["top_features"]:
        print(f"    {item['feature']:<18} {item['coef']:+.4f}")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="MLB game-total (O/U) model")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: today)")
    parser.add_argument("--evaluate", action="store_true", help="Print holdout evaluation and exit")
    parser.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                        help="Walk-forward backfill our_total_pred over a date range (look-ahead safe)")
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
