"""Ingest official reliever appearances and derive pregame bullpen snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import requests
from psycopg2.extras import execute_values

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)
MLB_BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
TRANSFORMATION_VERSION = "mlb-reliever-only-v1"


def _checksum(payload: object) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_relief_appearances(game: dict, boxscore: dict, *, captured_at: datetime) -> list[dict]:
    """Return every non-starter pitcher appearance from an official boxscore."""
    rows: list[dict] = []
    for side in ("home", "away"):
        team_box = (boxscore.get("teams") or {}).get(side) or {}
        pitcher_ids = team_box.get("pitchers") or []
        players = team_box.get("players") or {}
        team_id = int(game[f"{side}_team_id"])
        for order, pitcher_id in enumerate(pitcher_ids):
            player = players.get(f"ID{pitcher_id}") or {}
            stat = (player.get("stats") or {}).get("pitching") or {}
            if order == 0 or int(stat.get("gamesStarted") or 0) > 0:
                continue
            person = player.get("person") or {}
            payload = {
                "game_id": str(game["game_id"]), "team_id": team_id,
                "pitcher_id": int(pitcher_id), "appearance_order": order,
                "stat": stat,
            }
            rows.append({
                "matchup_id": int(game["id"]), "game_id": str(game["game_id"]),
                "game_date": str(game["game_date"]), "team_id": team_id,
                "pitcher_id": int(pitcher_id),
                "pitcher_name": str(person.get("fullName") or f"Pitcher {pitcher_id}"),
                "appearance_order": order, "outs": int(stat.get("outs") or 0),
                "pitches": int(stat["numberOfPitches"]) if stat.get("numberOfPitches") is not None else None,
                "batters_faced": int(stat.get("battersFaced") or 0),
                "hits": int(stat.get("hits") or 0),
                "earned_runs": int(stat.get("earnedRuns") or 0),
                "home_runs": int(stat.get("homeRuns") or 0),
                "walks": int(stat.get("baseOnBalls") or 0),
                "intentional_walks": int(stat.get("intentionalWalks") or 0),
                "hit_batters": int(stat.get("hitBatsmen") or 0),
                "strikeouts": int(stat.get("strikeOuts") or 0),
                "source_available_at": captured_at,
                "raw_checksum": _checksum(payload), "raw_json": payload,
            })
    return rows


def _fetch_game(game: dict, captured_at: datetime) -> list[dict]:
    try:
        response = requests.get(
            MLB_BOXSCORE_URL.format(game_id=game["game_id"]), timeout=20,
        )
        response.raise_for_status()
        return parse_relief_appearances(game, response.json() or {}, captured_at=captured_at)
    except requests.RequestException as exc:
        logger.warning("MLB boxscore failed for %s: %s", game["game_id"], exc)
        return []


def ingest_relief_appearances(db: DatabaseManager, start_date: str, end_date: str) -> int:
    games = [dict(row) for row in db.execute(
        """
        SELECT id, game_id, game_date, home_team_id, away_team_id
        FROM mlb_matchups
        WHERE game_id IS NOT NULL AND home_score IS NOT NULL AND away_score IS NOT NULL
          AND game_date BETWEEN %s AND %s
        ORDER BY game_date, id
        """,
        (start_date, end_date),
    )]
    captured_at = datetime.now(timezone.utc)
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_fetch_game, game, captured_at) for game in games]
        for future in as_completed(futures):
            rows.extend(future.result())
    if not rows:
        return 0
    values = [(
        row["matchup_id"], row["game_id"], row["game_date"], row["team_id"],
        row["pitcher_id"], row["pitcher_name"], row["appearance_order"], row["outs"],
        row["pitches"], row["batters_faced"], row["hits"], row["earned_runs"],
        row["home_runs"], row["walks"], row["intentional_walks"], row["hit_batters"],
        row["strikeouts"], "mlb_stats_api_boxscore", row["source_available_at"],
        row["raw_checksum"], json.dumps(row["raw_json"], sort_keys=True),
    ) for row in rows]
    with db.connect() as conn:
        execute_values(conn.cursor(), """
            INSERT INTO mlb_relief_appearances (
              matchup_id, game_id, game_date, team_id, pitcher_id, pitcher_name,
              appearance_order, outs, pitches, batters_faced, hits, earned_runs,
              home_runs, walks, intentional_walks, hit_batters, strikeouts,
              source, source_available_at, raw_checksum, raw_json
            ) VALUES %s
            ON CONFLICT (game_id, team_id, pitcher_id, raw_checksum) DO NOTHING
        """, values, page_size=500)
    return len(rows)


def derive_bullpen_metrics(appearances: list[dict], *, event_date: date) -> dict:
    quality = [row for row in appearances if (event_date - row["game_date"]).days <= 30]
    outs = sum(int(row.get("outs") or 0) for row in quality)
    innings = outs / 3.0
    bf = sum(int(row.get("batters_faced") or 0) for row in quality)
    er = sum(int(row.get("earned_runs") or 0) for row in quality)
    hr = sum(int(row.get("home_runs") or 0) for row in quality)
    bb = sum(int(row.get("walks") or 0) - int(row.get("intentional_walks") or 0) for row in quality)
    hbp = sum(int(row.get("hit_batters") or 0) for row in quality)
    strikeouts = sum(int(row.get("strikeouts") or 0) for row in quality)
    def window(days: int) -> list[dict]:
        return [row for row in appearances if 1 <= (event_date - row["game_date"]).days <= days]
    w1, w3, w7 = window(1), window(3), window(7)
    used_day1 = {int(row["pitcher_id"]) for row in w1}
    prior_day = {int(row["pitcher_id"]) for row in appearances if (event_date - row["game_date"]).days == 2}
    return {
        "quality_outs": outs, "quality_batters_faced": bf,
        "reliever_era": 9 * er / innings if innings > 0 else None,
        "reliever_fip": ((13 * hr + 3 * (bb + hbp) - 2 * strikeouts) / innings + 3.1) if innings > 0 else None,
        "reliever_k_pct": strikeouts / bf if bf > 0 else None,
        "reliever_bb_pct": (bb + hbp) / bf if bf > 0 else None,
        "pitches_1d": sum(int(row.get("pitches") or 0) for row in w1),
        "pitches_3d": sum(int(row.get("pitches") or 0) for row in w3),
        "pitches_7d": sum(int(row.get("pitches") or 0) for row in w7),
        "appearances_1d": len(w1), "appearances_3d": len(w3), "appearances_7d": len(w7),
        "relievers_used_1d": len(used_day1),
        "relievers_used_3d": len({int(row["pitcher_id"]) for row in w3}),
        "relievers_back_to_back": len(used_day1 & prior_day),
    }


def build_bullpen_snapshots(db: DatabaseManager, target_date: str) -> int:
    available_at = datetime.now(timezone.utc)
    matchups = db.execute(
        """SELECT id, game_date, commence_time, home_team_id, away_team_id
           FROM mlb_matchups WHERE game_date = %s AND game_id IS NOT NULL
             AND commence_time IS NOT NULL AND commence_time > %s""",
        (target_date, available_at),
    )
    written = 0
    for matchup in matchups:
        event_date = matchup["game_date"]
        for team_id in (matchup["home_team_id"], matchup["away_team_id"]):
            rows = [dict(row) for row in db.execute(
                """
                SELECT DISTINCT ON (game_id, team_id, pitcher_id) *
                FROM mlb_relief_appearances
                WHERE team_id = %s AND game_date >= %s AND game_date < %s
                  AND source_available_at < %s
                ORDER BY game_id, team_id, pitcher_id, captured_at DESC, id DESC
                """,
                (team_id, event_date - timedelta(days=30), event_date, matchup["commence_time"]),
            )]
            metrics = derive_bullpen_metrics(rows, event_date=event_date)
            payload = {"matchup_id": matchup["id"], "team_id": team_id, **metrics}
            row = db.execute_one(
                """
                INSERT INTO mlb_bullpen_snapshots (
                  matchup_id, team_id, event_commence, cutoff_at, quality_window_days,
                  quality_outs, quality_batters_faced, reliever_era, reliever_fip,
                  reliever_k_pct, reliever_bb_pct, pitches_1d, pitches_3d, pitches_7d,
                  appearances_1d, appearances_3d, appearances_7d, relievers_used_1d,
                  relievers_used_3d, relievers_back_to_back, source, available_at,
                  transformation_version, raw_checksum
                ) VALUES (%s,%s,%s,%s,30,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                          'mlb_stats_api_boxscore_derived',%s,%s,%s)
                ON CONFLICT (matchup_id, team_id, raw_checksum) DO NOTHING RETURNING id
                """,
                (matchup["id"], team_id, matchup["commence_time"], available_at,
                 metrics["quality_outs"], metrics["quality_batters_faced"], metrics["reliever_era"],
                 metrics["reliever_fip"], metrics["reliever_k_pct"], metrics["reliever_bb_pct"],
                 metrics["pitches_1d"], metrics["pitches_3d"], metrics["pitches_7d"],
                 metrics["appearances_1d"], metrics["appearances_3d"], metrics["appearances_7d"],
                 metrics["relievers_used_1d"], metrics["relievers_used_3d"],
                 metrics["relievers_back_to_back"], available_at, TRANSFORMATION_VERSION,
                 _checksum(payload)),
            )
            written += int(row is not None)
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest MLB reliever-only bullpen context")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--target-date", required=True)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url)
    appearances = ingest_relief_appearances(db, args.start, args.end)
    snapshots = build_bullpen_snapshots(db, args.target_date)
    print(f"MLB bullpen: {appearances} reliever appearances observed; {snapshots} snapshots written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
