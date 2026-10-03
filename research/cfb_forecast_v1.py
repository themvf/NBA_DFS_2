"""Point-in-time CFB score forecast research.

Historical games are replayed in kickoff order.  A team's inputs for a game
contain only its previous completed FBS-vs-FBS games; the held-out 2026 season
is never used to fit the model.  Market comparison uses accepted pregame odds
captured before kickoff, not retrospective CFBD line references.

This is a score-forecast study, not a betting-signal registration.
"""

from __future__ import annotations

import argparse
import json
import math
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

VERSION = "cfb-score-context-v1"
PRIOR_GAMES = 4
MIN_CURRENT_GAMES = 2
DEFAULTS = {
    "points_for": 26.0,
    "points_against": 26.0,
    "off_ppa": 0.0,
    "def_ppa": 0.0,
    "off_drives": 12.0,
    "def_drives": 12.0,
}
METRICS = tuple(DEFAULTS)


def load_rows(db: DatabaseManager):
    games = [dict(row) for row in db.execute("""
        SELECT m.id, m.season, m.week, m.commence_time, m.completed,
               m.home_team_id, m.away_team_id, m.home_score, m.away_score,
               ht.name home_name, at.name away_name
        FROM cfb_matchups m
        JOIN cfb_teams ht ON ht.team_id=m.home_team_id
        JOIN cfb_teams at ON at.team_id=m.away_team_id
        WHERE m.season BETWEEN 2022 AND 2026
          AND LOWER(ht.classification)='fbs'
          AND LOWER(at.classification)='fbs'
          AND m.commence_time IS NOT NULL
        ORDER BY m.commence_time, m.id
    """)]
    plays = {
        (int(row["game_id"]), int(row["team_id"])): dict(row)
        for row in db.execute("""
            SELECT game_id, offense_team_id team_id,
                   COUNT(*) FILTER (WHERE ppa IS NOT NULL)::int ppa_plays,
                   AVG(ppa) off_ppa
            FROM cfb_plays
            WHERE season BETWEEN 2022 AND 2026 AND offense_team_id IS NOT NULL
            GROUP BY game_id, offense_team_id
        """)
    }
    drives = {
        (int(row["game_id"]), int(row["team_id"])): int(row["drives"])
        for row in db.execute("""
            SELECT game_id, offense_team_id team_id, COUNT(*)::int drives
            FROM cfb_drives
            WHERE season BETWEEN 2022 AND 2026 AND offense_team_id IS NOT NULL
            GROUP BY game_id, offense_team_id
        """)
    }
    markets = {
        int(row["id"]): dict(row)
        for row in db.execute("""
            SELECT DISTINCT ON (m.id) m.id, h.captured_at,
                   h.home_ml, h.away_ml, h.home_spread, h.vegas_total
            FROM cfb_matchups m
            JOIN game_odds_history h ON h.matchup_id=m.id
             AND h.sport='cfb' AND h.captured_at<m.commence_time
            WHERE m.season=2026
            ORDER BY m.id, h.captured_at DESC, h.id DESC
        """)
    }
    return games, plays, drives, markets


def team_profile(history: list[dict], season: int) -> dict:
    current = [row for row in history if row["season"] == season]
    prior = [row for row in history if row["season"] == season - 1]
    result = {"current_games": len(current), "prior_games": len(prior)}
    for metric, default in DEFAULTS.items():
        current_values = [row[metric] for row in current if row[metric] is not None]
        prior_values = [row[metric] for row in prior if row[metric] is not None]
        previous = float(np.mean(prior_values)) if prior_values else default
        if current_values:
            weight = len(current_values) / (len(current_values) + PRIOR_GAMES)
            result[metric] = weight * float(np.mean(current_values)) + (1 - weight) * previous
        else:
            result[metric] = previous
    result["current_ppa_plays"] = sum(row["ppa_plays"] for row in current)
    return result


def score_features(team: dict, opponent: dict, *, home: bool) -> list[float]:
    return [
        team["points_for"], opponent["points_against"],
        team["off_ppa"], opponent["def_ppa"],
        team["off_drives"], opponent["def_drives"],
        float(home),
    ]


def replay_games(games: list[dict], plays: dict, drives: dict) -> list[dict]:
    history: dict[int, list[dict]] = defaultdict(list)
    rows = []
    for game in games:
        season = int(game["season"])
        home_id, away_id = int(game["home_team_id"]), int(game["away_team_id"])
        home = team_profile(history[home_id], season)
        away = team_profile(history[away_id], season)
        if home["current_games"] >= MIN_CURRENT_GAMES and away["current_games"] >= MIN_CURRENT_GAMES:
            rows.append({
                "id": int(game["id"]), "season": season,
                "kickoff": game["commence_time"],
                "home_name": game["home_name"], "away_name": game["away_name"],
                "home_features": score_features(home, away, home=True),
                "away_features": score_features(away, home, home=False),
                "home_current_games": home["current_games"],
                "away_current_games": away["current_games"],
                "home_ppa_plays": home["current_ppa_plays"],
                "away_ppa_plays": away["current_ppa_plays"],
                "home_score": game["home_score"],
                "away_score": game["away_score"],
                "completed": bool(game["completed"]),
            })
        if not game["completed"] or game["home_score"] is None or game["away_score"] is None:
            continue
        home_play = plays.get((int(game["id"]), home_id))
        away_play = plays.get((int(game["id"]), away_id))
        home_drives = drives.get((int(game["id"]), home_id))
        away_drives = drives.get((int(game["id"]), away_id))
        # A missing feed must not be silently replaced with zero efficiency.
        if not home_play or not away_play or not home_drives or not away_drives:
            continue
        if home_play["ppa_plays"] < 30 or away_play["ppa_plays"] < 30:
            continue
        history[home_id].append({
            "season": season, "points_for": float(game["home_score"]),
            "points_against": float(game["away_score"]),
            "off_ppa": float(home_play["off_ppa"]),
            "def_ppa": float(away_play["off_ppa"]),
            "off_drives": float(home_drives), "def_drives": float(away_drives),
            "ppa_plays": int(home_play["ppa_plays"]),
        })
        history[away_id].append({
            "season": season, "points_for": float(game["away_score"]),
            "points_against": float(game["home_score"]),
            "off_ppa": float(away_play["off_ppa"]),
            "def_ppa": float(home_play["off_ppa"]),
            "off_drives": float(away_drives), "def_drives": float(home_drives),
            "ppa_plays": int(away_play["ppa_plays"]),
        })
    return rows


def train(rows: list[dict], through_season: int):
    eligible = [
        row for row in rows
        if row["season"] <= through_season and row["completed"]
        and row["home_score"] is not None and row["away_score"] is not None
    ]
    x = [row["home_features"] for row in eligible] + [row["away_features"] for row in eligible]
    y = [float(row["home_score"]) for row in eligible] + [float(row["away_score"]) for row in eligible]
    if len(eligible) < 300:
        raise RuntimeError(f"Only {len(eligible)} training games; need at least 300")
    model = make_pipeline(StandardScaler(), Ridge(alpha=100.0))
    model.fit(x, y)
    residual_sd = float(np.std(np.asarray(y) - model.predict(x), ddof=1))
    return model, residual_sd, len(eligible)


def predict(row: dict, model, residual_sd: float) -> dict:
    home_points, away_points = model.predict([row["home_features"], row["away_features"]])
    home_points, away_points = max(0.0, float(home_points)), max(0.0, float(away_points))
    margin = home_points - away_points
    # Two score errors contribute to the margin variance.  This is a
    # transparent distributional approximation, checked by Brier score below.
    win_probability = float(norm.cdf(margin / max(1.0, residual_sd * math.sqrt(2))))
    return {
        "home_points": home_points, "away_points": away_points,
        "home_margin": margin, "total": home_points + away_points,
        "home_win_probability": win_probability,
    }


def american_probability(value) -> float | None:
    if value is None:
        return None
    value = float(value)
    if value == 0:
        return None
    return 100.0 / (value + 100.0) if value > 0 else -value / (-value + 100.0)


def summarize(rows: list[dict], model, residual_sd: float, markets: dict) -> dict:
    scored, market_rows = [], []
    for row in rows:
        if not row["completed"] or row["home_score"] is None or row["away_score"] is None:
            continue
        forecast = predict(row, model, residual_sd)
        actual_margin = float(row["home_score"] - row["away_score"])
        actual_total = float(row["home_score"] + row["away_score"])
        scored.append({
            "margin_error": abs(forecast["home_margin"] - actual_margin),
            "total_error": abs(forecast["total"] - actual_total),
            "brier": (forecast["home_win_probability"] - float(actual_margin > 0)) ** 2,
        })
        market = markets.get(row["id"])
        if market:
            home_p = american_probability(market["home_ml"])
            away_p = american_probability(market["away_ml"])
            if home_p is not None and away_p is not None:
                market_p = home_p / (home_p + away_p)
            else:
                market_p = None
            market_rows.append({
                "model_margin_error": abs(forecast["home_margin"] - actual_margin)
                    if market["home_spread"] is not None else None,
                "market_margin_error": abs(-float(market["home_spread"]) - actual_margin)
                    if market["home_spread"] is not None else None,
                "model_total_error": abs(forecast["total"] - actual_total)
                    if market["vegas_total"] is not None else None,
                "market_total_error": abs(float(market["vegas_total"]) - actual_total)
                    if market["vegas_total"] is not None else None,
                "model_brier": (forecast["home_win_probability"] - float(actual_margin > 0)) ** 2
                    if market_p is not None else None,
                "market_brier": (market_p - float(actual_margin > 0)) ** 2
                    if market_p is not None else None,
            })
    def aggregate(items, key):
        values = [item[key] for item in items if item[key] is not None]
        return {"n": len(values), "mean": round(float(np.mean(values)), 4) if values else None}
    result = {
        "games": len(scored),
        "score": {key: aggregate(scored, key) for key in ("margin_error", "total_error", "brier")},
    }
    if market_rows:
        result["matched_market_games"] = len(market_rows)
        result["market_comparison"] = {
            key: aggregate(market_rows, key)
            for key in (
                "model_margin_error", "market_margin_error",
                "model_total_error", "market_total_error",
                "model_brier", "market_brier",
            )
        }
    return result


def run(db: DatabaseManager) -> dict:
    games, plays, drives, markets = load_rows(db)
    rows = replay_games(games, plays, drives)
    holdout_model, holdout_sd, holdout_train_games = train(rows, 2024)
    holdout = summarize(
        [row for row in rows if row["season"] == 2025], holdout_model, holdout_sd, {},
    )
    model, residual_sd, trained_games = train(rows, 2025)
    forward = summarize(
        [row for row in rows if row["season"] == 2026], model, residual_sd, markets,
    )
    upcoming = []
    now = datetime.now(timezone.utc)
    for row in rows:
        if row["season"] != 2026 or row["completed"]:
            continue
        if not now < row["kickoff"] <= now + timedelta(days=14):
            continue
        if row["home_ppa_plays"] < 100 or row["away_ppa_plays"] < 100:
            continue
        upcoming.append({
            "game_id": row["id"], "kickoff": row["kickoff"].isoformat(),
            "home": row["home_name"], "away": row["away_name"],
            "home_current_games": row["home_current_games"],
            "away_current_games": row["away_current_games"],
            "home_ppa_plays": row["home_ppa_plays"],
            "away_ppa_plays": row["away_ppa_plays"],
            **{k: round(v, 4) for k, v in predict(row, model, residual_sd).items()},
        })
    comparison = forward.get("market_comparison", {})
    metrics = ("margin_error", "total_error", "brier")
    benchmark_passed = all(
        comparison.get(f"model_{metric}", {}).get("n", 0) >= 200
        and comparison[f"model_{metric}"]["mean"] < comparison[f"market_{metric}"]["mean"]
        for metric in metrics
    )
    return {
        "version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "BENCHMARK_PASSED" if benchmark_passed else "RESEARCH_ONLY",
        "promotion_rule": "At least 200 matched 2026 games and lower model error than contemporaneous market for margin, total, and moneyline Brier score; passing still does not establish a betting edge.",
        "training": {"seasons": [2022, 2023, 2024, 2025], "games": trained_games},
        "holdout_2025": {"trained_games": holdout_train_games, **holdout},
        "forward_2026": forward,
        "upcoming": upcoming,
    }


def publish(db: DatabaseManager, report: dict) -> int:
    """Freeze the research result and all near-term forecasts before kickoff."""
    from psycopg2.extras import Json, execute_values

    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO cfb_forecast_runs
              (version, generated_at, status, report_json)
            VALUES (%s, %s, %s, %s) RETURNING id
            """,
            (
                report["version"], report["generated_at"],
                report["status"], Json({k: v for k, v in report.items() if k != "upcoming"}),
            ),
        )
        run_id = int(cursor.fetchone()["id"])
        values = [
            (
                run_id, item["game_id"], item["kickoff"], item["home_points"],
                item["away_points"], item["home_win_probability"],
                item["home_current_games"], item["away_current_games"],
                item["home_ppa_plays"], item["away_ppa_plays"],
            )
            for item in report["upcoming"]
        ]
        if values:
            execute_values(
                cursor,
                """
                INSERT INTO cfb_game_forecasts
                  (run_id, game_id, kickoff, home_points, away_points,
                   home_win_probability, home_current_games, away_current_games,
                   home_ppa_plays, away_ppa_plays)
                VALUES %s
                """,
                values,
                page_size=500,
            )
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
    print(json.dumps({k: v for k, v in report.items() if k != "upcoming"}, indent=2))
    print(json.dumps({
        "upcoming_games": len(report["upcoming"]),
        "ucf_houston": [
            item for item in report["upcoming"]
            if {item["home"], item["away"]} == {"UCF", "Houston"}
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
