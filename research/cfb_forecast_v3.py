"""Research challenger: opponent-adjusted play PPA in the possession model.

The only feature change from v2 is the two play-value inputs. Each adjustment
uses opponents' records known before the forecasted kickoff.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from config import load_config
from db.database import DatabaseManager
from ingest.cfb_plays import football_season_year
from research.cfb_forecast_v1 import american_probability, load_rows, replay_games
from research.cfb_forecast_v2 import predict, publish, train

VERSION = "cfb-score-opponent-ppa-v3"


def evaluate(rows: list[dict], candidate, candidate_sd: float,
             baseline, baseline_sd: float, markets: dict) -> dict:
    errors = defaultdict(list)
    scored = matched = 0
    for row in rows:
        if not row["completed"] or row["home_score"] is None or row["away_score"] is None:
            continue
        scored += 1
        actual_margin = float(row["home_score"] - row["away_score"])
        actual_total = float(row["home_score"] + row["away_score"])
        actual_win = float(actual_margin > 0)
        new = predict(row, candidate, candidate_sd, "v3")
        old = predict(row, baseline, baseline_sd)
        for label, forecast in (("candidate", new), ("baseline", old)):
            errors[f"{label}_margin_error"].append(abs(forecast["home_margin"] - actual_margin))
            errors[f"{label}_total_error"].append(abs(forecast["total"] - actual_total))
            errors[f"{label}_brier"].append((forecast["home_win_probability"] - actual_win) ** 2)
        market = markets.get(row["id"])
        if market is None:
            continue
        matched += 1
        if market["home_spread"] is not None:
            errors["market_candidate_margin_error"].append(abs(new["home_margin"] - actual_margin))
            errors["market_baseline_margin_error"].append(abs(old["home_margin"] - actual_margin))
            errors["market_margin_error"].append(abs(-float(market["home_spread"]) - actual_margin))
        if market["vegas_total"] is not None:
            errors["market_candidate_total_error"].append(abs(new["total"] - actual_total))
            errors["market_baseline_total_error"].append(abs(old["total"] - actual_total))
            errors["market_total_error"].append(abs(float(market["vegas_total"]) - actual_total))
        hp, ap = american_probability(market["home_ml"]), american_probability(market["away_ml"])
        if hp is not None and ap is not None:
            errors["market_candidate_brier"].append((new["home_win_probability"] - actual_win) ** 2)
            errors["market_baseline_brier"].append((old["home_win_probability"] - actual_win) ** 2)
            errors["market_brier"].append((hp / (hp + ap) - actual_win) ** 2)
    return {"games": scored, "matched_market_games": matched,
            "metrics": {key: {"n": len(values), "mean": round(float(np.mean(values)), 4)}
                        for key, values in errors.items() if values}}


def run(db: DatabaseManager) -> dict:
    season = football_season_year()
    games, plays, drives, markets = load_rows(db, season)
    rows = replay_games(games, plays, drives)
    start = max(2022, season - 4)
    holdout_model, holdout_sd, _ = train(rows, start, season - 2, "v3")
    holdout_base, holdout_base_sd, _ = train(rows, start, season - 2)
    holdout = evaluate([row for row in rows if row["season"] == season - 1],
                       holdout_model, holdout_sd, holdout_base, holdout_base_sd, {})
    model, margin_sd, trained = train(rows, start, season - 1, "v3")
    baseline, baseline_sd, _ = train(rows, start, season - 1)
    forward = evaluate([row for row in rows if row["season"] == season],
                       model, margin_sd, baseline, baseline_sd, markets)
    now = datetime.now(timezone.utc)
    upcoming, explanations = [], {}
    for row in rows:
        if row["season"] != season or row["completed"] or not now < row["kickoff"] <= now + timedelta(days=14):
            continue
        if row["home_ppa_plays"] < 100 or row["away_ppa_plays"] < 100:
            continue
        forecast = predict(row, model, margin_sd, "v3")
        upcoming.append({
            "game_id": row["id"], "kickoff": row["kickoff"].isoformat(),
            "home_current_games": row["home_current_games"],
            "away_current_games": row["away_current_games"],
            "home_ppa_plays": row["home_ppa_plays"],
            "away_ppa_plays": row["away_ppa_plays"],
            **{key: round(value, 4) for key, value in forecast.items()},
        })
        explanations[str(row["id"])] = {
            "home_features": [round(float(value), 4) for value in row["home_v3_features"]],
            "away_features": [round(float(value), 4) for value in row["away_v3_features"]],
            "feature_order": ["opponent_adjusted_off_ppd", "opponent_adjusted_def_ppd",
                              "opponent_adjusted_off_ppa", "opponent_adjusted_def_ppa", "home_field"],
            "home_adjusted_off_ppa": round(row["home_v3_features"][2], 4),
            "home_adjusted_opponent_def_ppa": round(row["home_v3_features"][3], 4),
            "away_adjusted_off_ppa": round(row["away_v3_features"][2], 4),
            "away_adjusted_opponent_def_ppa": round(row["away_v3_features"][3], 4),
            "home_expected_drives": round(row["home_expected_drives"], 2),
            "away_expected_drives": round(row["away_expected_drives"], 2),
        }
    return {
        "version": VERSION, "season": season, "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "RESEARCH_ONLY",
        "training": {"seasons": list(range(start, season)), "games": trained},
        "holdout": {"season": season - 1, **holdout},
        "forward": {"season": season, **forward},
        "upcoming": upcoming, "explanations": explanations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "", initialize_schema=args.publish)
    report = run(db)
    if args.publish:
        report["published_run_id"] = publish(db, report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items()
                      if key not in ("upcoming", "explanations")}, indent=2))
    print(json.dumps({"upcoming_games": len(report["upcoming"])}))


if __name__ == "__main__":
    main()
