"""Capture immutable official game outcomes for point-in-time moneyline training.

The stored rows are raw results, not pregame features. Offline feature builders
must use only outcomes from an earlier game date. Historical captures are marked
``retrospective_backfill`` so they cannot be confused with prospective evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from psycopg2.extras import execute_values

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)
MLB_BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"


def _integer(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _checksum(payload: object) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_team_game_outcomes(game: dict, boxscore: dict, *, fetched_at: datetime) -> list[dict]:
    """Return one official outcome row for each team in a completed game."""
    rows: list[dict] = []
    teams = boxscore.get("teams") or {}
    for side in ("home", "away"):
        opponent_side = "away" if side == "home" else "home"
        team_box = teams.get(side) or {}
        team_stats = team_box.get("teamStats") or {}
        batting = team_stats.get("batting") or {}
        pitching = team_stats.get("pitching") or {}
        pitcher_ids = team_box.get("pitchers") or []
        players = team_box.get("players") or {}
        starter_id = _integer(pitcher_ids[0]) if pitcher_ids else 0
        starter = players.get(f"ID{starter_id}") or {}
        starter_stats = (starter.get("stats") or {}).get("pitching") or {}
        starter_person = starter.get("person") or {}

        team_id = _integer(game[f"{side}_team_id"])
        opponent_team_id = _integer(game[f"{opponent_side}_team_id"])
        if not team_id or not opponent_team_id:
            continue

        payload = {
            "game_id": str(game["game_id"]),
            "team_id": team_id,
            "batting": batting,
            "pitching": pitching,
            "starter_id": starter_id or None,
            "starter": starter_stats,
        }
        rows.append({
            "matchup_id": int(game["id"]),
            "game_id": str(game["game_id"]),
            "game_date": str(game["game_date"]),
            "commence_time": game["commence_time"],
            "team_id": team_id,
            "opponent_team_id": opponent_team_id,
            "is_home": side == "home",
            "runs": _integer(batting.get("runs")),
            "hits": _integer(batting.get("hits")),
            "doubles": _integer(batting.get("doubles")),
            "triples": _integer(batting.get("triples")),
            "home_runs": _integer(batting.get("homeRuns")),
            "walks": _integer(batting.get("baseOnBalls")),
            "hit_by_pitch": _integer(batting.get("hitByPitch")),
            "strikeouts": _integer(batting.get("strikeOuts")),
            "at_bats": _integer(batting.get("atBats")),
            "plate_appearances": _integer(batting.get("plateAppearances")),
            "starter_id": starter_id or None,
            "starter_name": starter_person.get("fullName"),
            "starter_outs": _integer(starter_stats.get("outs")),
            "starter_hits": _integer(starter_stats.get("hits")),
            "starter_earned_runs": _integer(starter_stats.get("earnedRuns")),
            "starter_home_runs": _integer(starter_stats.get("homeRuns")),
            "starter_walks": _integer(starter_stats.get("baseOnBalls")),
            "starter_hit_batters": _integer(starter_stats.get("hitBatsmen")),
            "starter_strikeouts": _integer(starter_stats.get("strikeOuts")),
            "starter_air_outs": _integer(starter_stats.get("airOuts")),
            "starter_ground_outs": _integer(starter_stats.get("groundOuts")),
            "team_pitching_outs": _integer(pitching.get("outs")),
            "team_pitching_hits": _integer(pitching.get("hits")),
            "team_pitching_earned_runs": _integer(pitching.get("earnedRuns")),
            "team_pitching_home_runs": _integer(pitching.get("homeRuns")),
            "team_pitching_walks": _integer(pitching.get("baseOnBalls")),
            "team_pitching_hit_batters": _integer(pitching.get("hitBatsmen")),
            "team_pitching_strikeouts": _integer(pitching.get("strikeOuts")),
            "origin": "retrospective_backfill",
            "source": "mlb_stats_api_boxscore",
            "fetched_at": fetched_at,
            "raw_checksum": _checksum(payload),
            "raw_json": payload,
        })
    return rows


def _fetch_game(game: dict, fetched_at: datetime) -> list[dict]:
    try:
        response = requests.get(MLB_BOXSCORE_URL.format(game_id=game["game_id"]), timeout=20)
        response.raise_for_status()
        return parse_team_game_outcomes(game, response.json() or {}, fetched_at=fetched_at)
    except requests.RequestException as exc:
        logger.warning("MLB training boxscore failed for %s: %s", game["game_id"], exc)
        return []


def ingest_team_game_outcomes(db: DatabaseManager, start_date: str, end_date: str) -> dict[str, int]:
    games = [dict(row) for row in db.execute(
        """
        SELECT id, game_id, game_date, commence_time, home_team_id, away_team_id
        FROM mlb_matchups
        WHERE game_id IS NOT NULL
          AND home_score IS NOT NULL
          AND away_score IS NOT NULL
          AND game_date BETWEEN %s AND %s
        ORDER BY game_date, id
        """,
        (start_date, end_date),
    )]
    fetched_at = datetime.now(timezone.utc)
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_fetch_game, game, fetched_at) for game in games]
        for future in as_completed(futures):
            rows.extend(future.result())

    if not rows:
        return {"games": len(games), "rows": 0, "inserted": 0}

    columns = (
        "matchup_id", "game_id", "game_date", "commence_time", "team_id",
        "opponent_team_id", "is_home", "runs", "hits", "doubles", "triples",
        "home_runs", "walks", "hit_by_pitch", "strikeouts", "at_bats",
        "plate_appearances", "starter_id", "starter_name", "starter_outs",
        "starter_hits", "starter_earned_runs", "starter_home_runs", "starter_walks",
        "starter_hit_batters", "starter_strikeouts", "starter_air_outs",
        "starter_ground_outs", "team_pitching_outs", "team_pitching_hits",
        "team_pitching_earned_runs", "team_pitching_home_runs", "team_pitching_walks",
        "team_pitching_hit_batters", "team_pitching_strikeouts", "origin", "source",
        "fetched_at", "raw_checksum", "raw_json",
    )
    values = [tuple(json.dumps(row[name], sort_keys=True) if name == "raw_json" else row[name] for name in columns) for row in rows]
    before_row = db.execute_one("SELECT COUNT(*) AS n FROM mlb_team_game_outcomes") or {}
    before = int(before_row.get("n") or 0)
    with db.connect() as conn:
        cursor = conn.cursor()
        execute_values(
            cursor,
            f"INSERT INTO mlb_team_game_outcomes ({', '.join(columns)}) VALUES %s "
            "ON CONFLICT (game_id, team_id, raw_checksum) DO NOTHING",
            values,
            page_size=500,
        )
    after_row = db.execute_one("SELECT COUNT(*) AS n FROM mlb_team_game_outcomes") or {}
    inserted = max(0, int(after_row.get("n") or 0) - before)
    return {"games": len(games), "rows": len(rows), "inserted": inserted}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill official MLB moneyline training outcomes")
    parser.add_argument("--start", required=True, help="First completed game date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Last completed game date YYYY-MM-DD")
    args = parser.parse_args()

    config = load_config()
    database = DatabaseManager(config.database_url)
    print(ingest_team_game_outcomes(database, args.start, args.end))
