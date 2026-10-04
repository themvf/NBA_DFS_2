"""Grade frozen CFB forecasts against finals and contemporaneous market quotes.

Latest pregame snapshots are selected per game/version. A strict paired cohort
also requires the same odds-history ID across v1, v2, and v3. No closing line,
final score, or later market capture is used as a forecast feature.
"""

from __future__ import annotations

import argparse
import json
import math
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from ingest.cfb_plays import football_season_year
from research.cfb_forecast_v1 import american_probability
from research.cfb_market_anchor import VERSION as ANCHOR_VERSION

VERSION = "cfb-prospective-evaluation-v1"
FORECAST_VERSIONS = (
    "cfb-score-context-v1", "cfb-score-possession-v2",
    "cfb-score-opponent-ppa-v3",
)
MARKETS = ("spread", "total", "moneyline")


def load_rows(db: DatabaseManager, season: int) -> list[dict]:
    return [dict(row) for row in db.execute("""
        SELECT DISTINCT ON (r.version,c.game_id)
               c.game_id,c.forecast_at,c.odds_history_id,c.evidence_json,
               r.version,m.game_date,m.commence_time,m.completed,
               m.home_score,m.away_score,
               h.name home_name,a.name away_name,
               vc.id close_id,vc.captured_at close_at,vc.history_id close_history_id,
               ch.home_spread close_home_spread,ch.vegas_total close_total,
               ch.home_ml close_home_ml,ch.away_ml close_away_ml
        FROM cfb_market_comparisons c
        JOIN cfb_game_forecasts f ON f.id=c.forecast_id
        JOIN cfb_forecast_runs r ON r.id=c.run_id
        JOIN cfb_matchups m ON m.id=c.game_id
        JOIN cfb_teams h ON h.team_id=m.home_team_id
        JOIN cfb_teams a ON a.team_id=m.away_team_id
        LEFT JOIN verified_clv_closes vc ON vc.sport='cfb'
          AND vc.matchup_id=m.id AND vc.scheduled_start_at=m.commence_time
        LEFT JOIN game_odds_history ch ON ch.id=vc.history_id
          AND ch.sport='cfb' AND ch.matchup_id=m.id
        WHERE m.season=%s AND r.version=ANY(%s)
          AND c.forecast_at<m.commence_time AND f.kickoff=m.commence_time
        ORDER BY r.version,c.game_id,c.forecast_at DESC,c.id DESC
    """, (season, list(FORECAST_VERSIONS)))]


def _final(row: dict) -> bool:
    return bool(row["completed"] and row["home_score"] is not None and row["away_score"] is not None)


def _close_values(row: dict) -> dict:
    home = american_probability(row["close_home_ml"])
    away = american_probability(row["close_away_ml"])
    return {
        "spread": -float(row["close_home_spread"]) if row["close_home_spread"] is not None else None,
        "total": float(row["close_total"]) if row["close_total"] is not None else None,
        "moneyline": home / (home + away) if home is not None and away is not None and home + away else None,
    }


def _actual(row: dict, market: str) -> float:
    margin = float(row["home_score"] - row["away_score"])
    if market == "spread":
        return margin
    if market == "total":
        return float(row["home_score"] + row["away_score"])
    return float(margin > 0)


def _forecast_value(evidence: dict, market: str) -> float:
    forecast = evidence["forecast"]
    if market == "spread":
        return float(forecast["home_points"] - forecast["away_points"])
    if market == "total":
        return float(forecast["home_points"] + forecast["away_points"])
    return float(forecast["home_win_probability"])


def _error(prediction: float, actual: float, market: str) -> float:
    return (prediction - actual) ** 2 if market == "moneyline" else abs(prediction - actual)


def _logloss(probability: float, outcome: float) -> float:
    p = min(1 - 1e-6, max(1e-6, probability))
    return -(outcome * math.log(p) + (1 - outcome) * math.log(1 - p))


def _cluster_interval(pairs: list[dict], field: str, *, seed: int) -> list[float] | None:
    by_date = defaultdict(list)
    for pair in pairs:
        if pair.get(field) is not None:
            by_date[pair["date"]].append(float(pair[field]))
    dates = sorted(by_date)
    if len(dates) < 4 or sum(map(len, by_date.values())) < 20:
        return None
    rng = random.Random(seed)
    samples = []
    for _ in range(2000):
        drawn = [rng.choice(dates) for _ in dates]
        samples.append(mean(value for date in drawn for value in by_date[date]))
    samples.sort()
    return [round(samples[49], 5), round(samples[1949], 5)]


def _summary(pairs: list[dict], market: str, seed: int) -> dict:
    if not pairs:
        return {"n": 0, "game_dates": 0, "model_error": None,
                "market_error": None, "model_minus_market": None,
                "model_minus_market_ci95": None, "anchor_n": 0}
    deltas = [{**pair, "delta": pair["model_error"] - pair["market_error"]}
              for pair in pairs]
    anchored = [{**pair, "anchor_delta": pair["anchor_error"] - pair["market_error"]}
                for pair in pairs if pair.get("anchor_error") is not None]
    result = {
        "n": len(pairs), "game_dates": len({pair["date"] for pair in pairs}),
        "model_error": round(mean(pair["model_error"] for pair in pairs), 5),
        "market_error": round(mean(pair["market_error"] for pair in pairs), 5),
        "model_minus_market": round(mean(pair["delta"] for pair in deltas), 5),
        "model_minus_market_ci95": _cluster_interval(deltas, "delta", seed=seed),
        "anchor_n": len(anchored),
    }
    if anchored:
        result["anchor_error"] = round(mean(pair["anchor_error"] for pair in anchored), 5)
        result["anchor_market_error"] = round(mean(pair["market_error"] for pair in anchored), 5)
        result["anchor_minus_market"] = round(mean(pair["anchor_delta"] for pair in anchored), 5)
        result["anchor_minus_market_ci95"] = _cluster_interval(anchored, "anchor_delta", seed=seed + 1)
    if market == "moneyline":
        result["model_logloss"] = round(mean(pair["model_logloss"] for pair in pairs), 5)
        result["market_logloss"] = round(mean(pair["market_logloss"] for pair in pairs), 5)
        result["model_calibration_bias"] = round(mean(pair["model_probability"] - pair["actual"] for pair in pairs), 5)
        result["market_calibration_bias"] = round(mean(pair["market_probability"] - pair["actual"] for pair in pairs), 5)
        if anchored:
            result["anchor_logloss"] = round(mean(pair["anchor_logloss"] for pair in anchored), 5)
            result["anchor_calibration_bias"] = round(mean(pair["anchor_probability"] - pair["actual"] for pair in anchored), 5)
    close_moves = [pair["directional_close_move"] for pair in pairs if pair["directional_close_move"] is not None]
    result["verified_close_n"] = len(close_moves)
    result["directional_close_move"] = round(mean(close_moves), 5) if close_moves else None
    return result


def evaluate(rows: list[dict], season: int, as_of: datetime) -> dict:
    by_game: dict[int, dict[str, dict]] = defaultdict(dict)
    coverage = {version: {"frozen": 0, "awaiting_final": 0, "final": 0,
                          "verified_close": 0, "final_without_verified_close": 0,
                          "eligible": Counter(), "excluded": Counter(),
                          "reasons": Counter()} for version in FORECAST_VERSIONS}
    pairs = {version: {market: [] for market in MARKETS} for version in FORECAST_VERSIONS}
    for row in rows:
        version = row["version"]
        if version not in coverage:
            continue
        evidence = row["evidence_json"]
        if isinstance(evidence, str):
            evidence = json.loads(evidence)
        if evidence.get("definition") != "cfb-comparison-v1":
            raise ValueError(f"Game {row['game_id']}: unknown frozen comparison definition")
        if row["odds_history_id"] is None and any(
            evidence["markets"][market]["eligible"] for market in MARKETS
        ):
            raise ValueError(f"Game {row['game_id']}: eligible market without source capture")
        row = {**row, "evidence_json": evidence}
        by_game[int(row["game_id"])][version] = row
        stats = coverage[version]
        stats["frozen"] += 1
        settled = _final(row)
        stats["final" if settled else "awaiting_final"] += 1
        if row["close_id"] is not None:
            stats["verified_close"] += 1
        elif settled:
            stats["final_without_verified_close"] += 1
        close = _close_values(row)
        for index, market in enumerate(MARKETS):
            quote = evidence["markets"][market]
            if not quote["eligible"]:
                stats["excluded"][market] += 1
                stats["reasons"].update(f"{market}:{reason}" for reason in quote["reasons"])
                continue
            stats["eligible"][market] += 1
            if not settled:
                continue
            actual = _actual(row, market)
            model = _forecast_value(evidence, market)
            observed = float(quote["value"])
            anchor = evidence.get("anchor", {})
            anchored = anchor.get("markets", {}).get(market) if anchor.get("version") == ANCHOR_VERSION else None
            directional_close_move = None
            if (close[market] is not None and row["close_at"] is not None
                    and row["close_at"] > row["forecast_at"] and model != observed):
                directional_close_move = math.copysign(1, model - observed) * (close[market] - observed)
            pair = {
                "game_id": int(row["game_id"]), "date": str(row["game_date"]),
                "odds_history_id": row["odds_history_id"],
                "actual": actual, "model_error": _error(model, actual, market),
                "market_error": _error(observed, actual, market),
                "anchor_error": _error(float(anchored), actual, market) if anchored is not None else None,
                "directional_close_move": directional_close_move,
            }
            if market == "moneyline":
                pair.update({
                    "model_probability": model, "market_probability": observed,
                    "model_logloss": _logloss(model, actual),
                    "market_logloss": _logloss(observed, actual),
                })
                if anchored is not None:
                    pair["anchor_probability"] = float(anchored)
                    pair["anchor_logloss"] = _logloss(float(anchored), actual)
            pairs[version][market].append(pair)

    # A head-to-head comparison can use only identical market evidence and a
    # narrow common decision window. Missing or mismatched sources stay out.
    strict = {market: [] for market in MARKETS}
    for game_versions in by_game.values():
        if any(version not in game_versions for version in FORECAST_VERSIONS):
            continue
        chosen = [game_versions[version] for version in FORECAST_VERSIONS]
        source_ids = {row["odds_history_id"] for row in chosen}
        if len(source_ids) != 1 or None in source_ids:
            continue
        times = [row["forecast_at"] for row in chosen]
        if max(times) - min(times) > timedelta(minutes=15) or not all(_final(row) for row in chosen):
            continue
        for market in MARKETS:
            if not all(row["evidence_json"]["markets"][market]["eligible"] for row in chosen):
                continue
            game_id = int(chosen[0]["game_id"])
            available = {version: next((pair for pair in pairs[version][market]
                        if pair["game_id"] == game_id), None) for version in FORECAST_VERSIONS}
            if any(pair is None for pair in available.values()):
                continue
            strict[market].append({
                "game_id": game_id, "date": str(chosen[0]["game_date"]),
                "source_id": chosen[0]["odds_history_id"],
                "market_error": available[FORECAST_VERSIONS[0]]["market_error"],
                **{version: available[version]["model_error"] for version in FORECAST_VERSIONS},
            })
    strict_summary = {}
    for index, market in enumerate(MARKETS):
        data = strict[market]
        result = {"n": len(data), "game_dates": len({item["date"] for item in data}),
                  "errors": {version: round(mean(item[version] for item in data), 5) if data else None
                             for version in FORECAST_VERSIONS},
                  "market_error": round(mean(item["market_error"] for item in data), 5) if data else None}
        deltas = [{**item, "delta": item[FORECAST_VERSIONS[2]] - item[FORECAST_VERSIONS[1]]}
                  for item in data]
        result["v3_minus_v2"] = round(mean(item["delta"] for item in deltas), 5) if deltas else None
        result["v3_minus_v2_ci95"] = _cluster_interval(deltas, "delta", seed=220 + index)
        strict_summary[market] = result

    compact_coverage = {
        version: {key: dict(value) if isinstance(value, Counter) else value
                  for key, value in stats.items()}
        for version, stats in coverage.items()
    }
    games = []
    for game_id, versions in by_game.items():
        row = versions.get(FORECAST_VERSIONS[2]) or next(iter(versions.values()))
        games.append({
            "id": game_id, "date": str(row["game_date"]),
            "kickoff": row["commence_time"].isoformat(),
            "game": f"{row['away_name']} at {row['home_name']}",
            "status": "final" if _final(row) else "awaiting_final" if row["commence_time"] <= as_of else "upcoming",
            "verified_close": row["close_id"] is not None,
            "captured": row["odds_history_id"] is not None,
            "eligible_markets": [market for market in MARKETS if row["evidence_json"]["markets"][market]["eligible"]],
        })
    games.sort(key=lambda row: (row["kickoff"], row["id"]), reverse=True)
    return {
        "version": VERSION, "season": season, "generated_at": as_of.isoformat(),
        "status": "RESEARCH_ONLY",
        "coverage": compact_coverage,
        "market_comparison": {
            version: {market: _summary(pairs[version][market], market, 100 + 10 * vi + mi)
                      for mi, market in enumerate(MARKETS)}
            for vi, version in enumerate(FORECAST_VERSIONS)
        },
        "strict_same_capture": strict_summary,
        "games": games,
        "interpretation": "Lower error is better. Negative differences favor the football model. "
                          "Intervals are game-date clustered and withheld for sparse samples. "
                          "Directional close movement is descriptive consensus movement, not executable CLV.",
    }


def publish(db: DatabaseManager, report: dict) -> int:
    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO cfb_prospective_evaluation_runs
              (version,season,generated_at,report_json)
            VALUES (%s,%s,%s,%s) RETURNING id
        """, (report["version"], report["season"], report["generated_at"], Json(report)))
        return int(cursor.fetchone()["id"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "", initialize_schema=args.publish)
    season = football_season_year()
    report = evaluate(load_rows(db, season), season, datetime.now(timezone.utc))
    if args.publish:
        report["published_run_id"] = publish(db, report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({
        "version": report["version"], "season": season,
        "frozen": {version: values["frozen"] for version, values in report["coverage"].items()},
        "final": {version: values["final"] for version, values in report["coverage"].items()},
        "strict": {market: values["n"] for market, values in report["strict_same_capture"].items()},
        "published_run_id": report.get("published_run_id"),
    }, indent=2))


if __name__ == "__main__":
    main()
