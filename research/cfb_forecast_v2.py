"""Opponent-adjusted, possession-based CFB score challenger.

All adjustments use games completed before the forecasted kickoff. This model
is published beside v1 for prospective grading, never as a betting signal.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
from scipy.stats import norm
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from config import load_config
from db.database import DatabaseManager
from ingest.cfb_plays import football_season_year
from research.cfb_forecast_v1 import (
    american_probability, load_rows, predict as predict_v1,
    replay_games, train as train_v1,
)

VERSION = "cfb-score-possession-v2"


def train(rows: list[dict], start: int, end: int, feature_key: str = "v2"):
    eligible = [row for row in rows if start <= row["season"] <= end
                and row["completed"] and row["home_actual_drives"]
                and row["away_actual_drives"]]
    if len(eligible) < 300:
        raise RuntimeError(f"Only {len(eligible)} complete training games")
    x = [row[f"home_{feature_key}_features"] for row in eligible] + [row[f"away_{feature_key}_features"] for row in eligible]
    y = ([float(row["home_score"]) / row["home_actual_drives"] for row in eligible]
         + [float(row["away_score"]) / row["away_actual_drives"] for row in eligible])
    model = make_pipeline(StandardScaler(), Ridge(alpha=100.0))
    model.fit(x, y)
    margins = []
    for row in eligible:
        home, away = score(row, model, feature_key)
        margins.append(float(row["home_score"] - row["away_score"]) - (home - away))
    margin_sd = float(np.std(margins, ddof=1))
    return model, margin_sd, len(eligible)


def score(row: dict, model, feature_key: str = "v2") -> tuple[float, float]:
    home_rate, away_rate = model.predict([row[f"home_{feature_key}_features"], row[f"away_{feature_key}_features"]])
    return (max(0.0, float(home_rate) * row["home_expected_drives"]),
            max(0.0, float(away_rate) * row["away_expected_drives"]))


def predict(row: dict, model, margin_sd: float, feature_key: str = "v2") -> dict:
    home, away = score(row, model, feature_key)
    return {
        "home_points": home, "away_points": away,
        "home_margin": home - away, "total": home + away,
        "home_win_probability": float(norm.cdf((home - away) / max(1.0, margin_sd))),
    }


def explain(row: dict, forecast: dict) -> dict:
    return {
        "home_features": [round(float(value), 4) for value in row["home_v2_features"]],
        "away_features": [round(float(value), 4) for value in row["away_v2_features"]],
        "feature_order": ["opponent_adjusted_off_ppd", "opponent_adjusted_def_ppd",
                          "off_ppa", "opponent_def_ppa", "home_field"],
        "home_expected_drives": round(row["home_expected_drives"], 2),
        "away_expected_drives": round(row["away_expected_drives"], 2),
        "home_adjusted_off_ppd": round(row["home_v2_features"][0], 3),
        "away_adjusted_off_ppd": round(row["away_v2_features"][0], 3),
        "home_adjusted_def_ppd": round(row["away_v2_features"][1], 3),
        "away_adjusted_def_ppd": round(row["home_v2_features"][1], 3),
        "home_predicted_ppd": round(forecast["home_points"] / row["home_expected_drives"], 3),
        "away_predicted_ppd": round(forecast["away_points"] / row["away_expected_drives"], 3),
    }


def evaluate(rows: list[dict], model, margin_sd: float, markets: dict,
             baseline_model=None, baseline_sd=None) -> dict:
    errors = defaultdict(list)
    scored = 0
    matched = 0
    for row in rows:
        if not row["completed"] or row["home_score"] is None or row["away_score"] is None:
            continue
        scored += 1
        actual_margin = float(row["home_score"] - row["away_score"])
        actual_total = float(row["home_score"] + row["away_score"])
        candidate = predict(row, model, margin_sd)
        errors["model_margin_error"].append(abs(candidate["home_margin"] - actual_margin))
        errors["model_total_error"].append(abs(candidate["total"] - actual_total))
        errors["model_brier"].append((candidate["home_win_probability"] - float(actual_margin > 0)) ** 2)
        if baseline_model is not None:
            baseline = predict_v1(row, baseline_model, baseline_sd)
            errors["baseline_margin_error"].append(abs(baseline["home_margin"] - actual_margin))
            errors["baseline_total_error"].append(abs(baseline["total"] - actual_total))
            errors["baseline_brier"].append((baseline["home_win_probability"] - float(actual_margin > 0)) ** 2)
        market = markets.get(row["id"])
        if market is None:
            continue
        matched += 1
        if market["home_spread"] is not None:
            errors["market_model_margin_error"].append(abs(candidate["home_margin"] - actual_margin))
            errors["market_margin_error"].append(abs(-float(market["home_spread"]) - actual_margin))
            if baseline_model is not None:
                errors["market_baseline_margin_error"].append(abs(baseline["home_margin"] - actual_margin))
        if market["vegas_total"] is not None:
            errors["market_model_total_error"].append(abs(candidate["total"] - actual_total))
            errors["market_total_error"].append(abs(float(market["vegas_total"]) - actual_total))
            if baseline_model is not None:
                errors["market_baseline_total_error"].append(abs(baseline["total"] - actual_total))
        home_p = american_probability(market["home_ml"])
        away_p = american_probability(market["away_ml"])
        if home_p is not None and away_p is not None:
            errors["market_model_brier"].append((candidate["home_win_probability"] - float(actual_margin > 0)) ** 2)
            errors["market_brier"].append((home_p / (home_p + away_p) - float(actual_margin > 0)) ** 2)
            if baseline_model is not None:
                errors["market_baseline_brier"].append((baseline["home_win_probability"] - float(actual_margin > 0)) ** 2)
    return {"games": scored, "matched_market_games": matched,
            "metrics": {key: {"n": len(value), "mean": round(float(np.mean(value)), 4)}
                        for key, value in errors.items() if value}}


def run(db: DatabaseManager) -> dict:
    season = football_season_year()
    games, plays, drives, markets = load_rows(db, season)
    rows = replay_games(games, plays, drives)
    start = max(2022, season - 4)
    holdout_model, holdout_sd, _ = train(rows, start, season - 2)
    holdout_baseline, holdout_baseline_sd, _ = train_v1(rows, start, season - 2)
    holdout = evaluate([row for row in rows if row["season"] == season - 1],
                       holdout_model, holdout_sd, {}, holdout_baseline, holdout_baseline_sd)
    model, margin_sd, trained = train(rows, start, season - 1)
    baseline, baseline_sd, _ = train_v1(rows, start, season - 1)
    forward = evaluate([row for row in rows if row["season"] == season],
                       model, margin_sd, markets, baseline, baseline_sd)
    upcoming = []
    explanations = {}
    now = datetime.now(timezone.utc)
    for row in rows:
        if row["season"] != season or row["completed"] or not now < row["kickoff"] <= now + timedelta(days=14):
            continue
        if row["home_ppa_plays"] < 100 or row["away_ppa_plays"] < 100:
            continue
        forecast = predict(row, model, margin_sd)
        upcoming.append({
            "game_id": row["id"], "kickoff": row["kickoff"].isoformat(),
            "home_current_games": row["home_current_games"],
            "away_current_games": row["away_current_games"],
            "home_ppa_plays": row["home_ppa_plays"],
            "away_ppa_plays": row["away_ppa_plays"],
            **{key: round(value, 4) for key, value in forecast.items()},
        })
        explanations[str(row["id"])] = explain(row, forecast)
    return {
        "version": VERSION, "season": season,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "RESEARCH_ONLY",
        "training": {"seasons": list(range(start, season)), "games": trained},
        "holdout": {"season": season - 1, **holdout},
        "forward": {"season": season, **forward},
        "prospective": {"season": season, **prospective_summary(db, season)},
        "explanations": explanations,
        "upcoming": upcoming,
    }


def prospective_summary(db: DatabaseManager, season: int) -> dict:
    rows = db.execute("""
        SELECT DISTINCT ON (f.game_id) m.id, m.home_score, m.away_score,
               f.home_points, f.away_points, f.home_win_probability,
               h.home_ml, h.away_ml, h.home_spread, h.vegas_total
        FROM cfb_game_forecasts f
        JOIN cfb_forecast_runs r ON r.id=f.run_id
        JOIN cfb_matchups m ON m.id=f.game_id
        LEFT JOIN LATERAL (
          SELECT home_ml, away_ml, home_spread, vegas_total
          FROM game_odds_history
          WHERE sport='cfb' AND matchup_id=m.id AND captured_at<=r.generated_at
            AND captured_at<m.commence_time
          ORDER BY captured_at DESC,id DESC LIMIT 1
        ) h ON TRUE
        WHERE m.season=%s AND m.completed=TRUE AND m.home_score IS NOT NULL
          AND m.away_score IS NOT NULL AND r.version=%s
          AND r.generated_at<m.commence_time
        ORDER BY f.game_id,r.generated_at DESC,f.id DESC
    """, (season, VERSION))
    errors = defaultdict(list)
    for row in rows:
        margin = float(row["home_score"] - row["away_score"])
        total = float(row["home_score"] + row["away_score"])
        if row["home_spread"] is not None:
            errors["model_margin_error"].append(abs(float(row["home_points"] - row["away_points"]) - margin))
            errors["market_margin_error"].append(abs(-float(row["home_spread"]) - margin))
        if row["vegas_total"] is not None:
            errors["model_total_error"].append(abs(float(row["home_points"] + row["away_points"]) - total))
            errors["market_total_error"].append(abs(float(row["vegas_total"]) - total))
        hp, ap = american_probability(row["home_ml"]), american_probability(row["away_ml"])
        if hp is not None and ap is not None:
            errors["model_brier"].append((float(row["home_win_probability"]) - float(margin > 0)) ** 2)
            errors["market_brier"].append((hp / (hp + ap) - float(margin > 0)) ** 2)
    return {"games": len(rows), "market_comparison": {
        key: {"n": len(errors[key]), "mean": round(float(np.mean(errors[key])), 4) if errors[key] else None}
        for key in ("model_margin_error", "market_margin_error", "model_total_error", "market_total_error", "model_brier", "market_brier")
    }}


def publish(db: DatabaseManager, report: dict) -> int:
    from psycopg2.extras import Json, execute_values
    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute("""INSERT INTO cfb_forecast_runs (version,generated_at,status,report_json)
                          VALUES (%s,%s,%s,%s) RETURNING id""",
                       (report["version"], report["generated_at"], report["status"],
                        Json({key: value for key, value in report.items() if key != "upcoming"})))
        run_id = int(cursor.fetchone()["id"])
        values = [(run_id, item["game_id"], item["kickoff"], item["home_points"],
                   item["away_points"], item["home_win_probability"],
                   item["home_current_games"], item["away_current_games"],
                   item["home_ppa_plays"], item["away_ppa_plays"])
                  for item in report["upcoming"]]
        if values:
            execute_values(cursor, """INSERT INTO cfb_game_forecasts
                (run_id,game_id,kickoff,home_points,away_points,home_win_probability,
                 home_current_games,away_current_games,home_ppa_plays,away_ppa_plays)
                VALUES %s""", values, page_size=500)
        from research.cfb_comparison import freeze_comparisons
        freeze_comparisons(cursor, run_id, report)
    return run_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "", initialize_schema=args.publish)
    report = run(db)
    if args.publish:
        report["published_run_id"] = publish(db, report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key not in ("upcoming", "explanations")}, indent=2))
    print(json.dumps({"upcoming_games": len(report["upcoming"]), "published_run_id": report.get("published_run_id")}))


if __name__ == "__main__":
    main()
