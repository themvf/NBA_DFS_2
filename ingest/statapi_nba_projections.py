"""Store stat-api's NBA player projections, as frozen at tip-off.

stat-api (``api.stat-api.com/api/v1/nba``) publishes its own pre-tip
projection for every player in every game: chance to play and to start,
minutes, points and the other stats, with P10/P50/P90 quantiles. Their docs:
"One current row per game, player and engine version; it updates until tip
and never changes after." So a row read after the game is over is the
version they had at tip, and this script only reads games that are final.

    python -m ingest.statapi_nba_projections --start 2026-10-20 --end 2026-11-12
    python -m ingest.statapi_nba_projections --days-back 3        # recent nights

Research only: manual, not scheduled, nothing reads these rows but
``research/nba_statapi_projection_compare.py``. Needs ``STAT_API_KEY`` (any
tier that returns full rows; the free tier returns a 5-row preview, which is
refused here rather than stored as if it were the whole game).

Rows are insert-only (``ON CONFLICT DO NOTHING``), so a rerun over the same
dates writes nothing new. Player names come from stat-api's own players table
(one paged read, then single lookups for anyone missing) and are keyed with
the same accent-free ``player_key`` the NBA points grade uses.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import json
import os
import time
from typing import Any

import requests

from config import load_config
from db.database import DatabaseManager
from db.statapi_nba_projections_schema import STATEMENTS
from ingest.nba_box_scores import eastern_date
from model.nba_points_ladder import player_key

BASE = "https://api.stat-api.com/api/v1/nba"
USER_AGENT = "NBADFS-v2 statapi-nba-projections/1.0 (personal research)"
FINAL_STATUSES = {"final", "f", "f/ot", "closed", "complete", "completed"}
SEASON_START = date(2026, 10, 20)


class StatApiError(RuntimeError):
    pass


class Client:
    def __init__(self, api_key: str, delay: float = 0.15, retries: int = 3, timeout: int = 60):
        if not api_key:
            raise StatApiError("STAT_API_KEY is not set")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json",
                                     "Authorization": f"Bearer {api_key}"})
        self.delay, self.retries, self.timeout = delay, retries, timeout
        self.calls = 0
        self.quota_remaining: str | None = None
        self._last = 0.0

    def get(self, path: str, params: dict | None = None) -> dict:
        url = BASE + path
        for attempt in range(self.retries + 1):
            wait = self.delay - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            self.calls += 1
            response = self.session.get(url, params=params, timeout=self.timeout)
            self.quota_remaining = response.headers.get("x-quota-remaining", self.quota_remaining)
            if response.status_code == 200:
                return response.json()
            if response.status_code in (429, 500, 502, 503, 504) and attempt < self.retries:
                retry_after = response.headers.get("Retry-After")
                time.sleep(float(retry_after) if retry_after and retry_after.isdigit() else 2 ** (attempt + 1))
                continue
            raise StatApiError(f"{url} {params}: HTTP {response.status_code} {response.text[:200]}")
        raise StatApiError(f"{url}: gave up")  # pragma: no cover


def _rows(payload: dict, key: str) -> list[dict]:
    rows = payload.get(key)
    if not isinstance(rows, list):
        raise StatApiError(f"response has no '{key}' list: {str(payload)[:200]}")
    for row in rows:
        meta = row.get("_metadata") if isinstance(row, dict) else None
        if meta:
            raise StatApiError(f"'{key}' came back as a preview ({meta.get('required_tier')} needed); refusing to store it")
    if payload.get("has_more"):
        raise StatApiError(f"'{key}' has more rows than one page; paging not implemented for this read")
    return rows


def games_on(client: Client, day: date) -> list[dict]:
    return _rows(client.get("/games", {"day": day.strftime("%Y%m%d"), "limit": 100}), "games")


def projections_for(client: Client, game_id: int) -> list[dict]:
    return _rows(client.get("/projected_player_stats", {"game_id": game_id, "limit": 500}), "projected_player_stats")


def load_players(client: Client) -> dict[int, dict]:
    players = _rows(client.get("/players", {"limit": 1000, "from_id": 0}), "players")
    return {int(p["id"]): p for p in players}


def player_record(client: Client, players: dict[int, dict], player_id: int) -> dict | None:
    if player_id not in players:
        try:
            players[player_id] = client.get(f"/players/{player_id}")
        except StatApiError:
            players[player_id] = {}
    return players[player_id] or None


def is_final(game: dict) -> bool:
    return str(game.get("status") or "").strip().lower() in FINAL_STATUSES


def _ts(value: Any) -> datetime | None:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None


def build_rows(game: dict, projections: list[dict], players: dict[int, dict], teams: dict[int, str],
               client: Client) -> list[tuple]:
    game_time = _ts(game.get("game_time"))
    game_date = eastern_date(game_time) if game_time else datetime.strptime(str(game["day"]), "%Y%m%d").date()
    out = []
    for p in projections:
        record = player_record(client, players, int(p["player_id"])) or {}
        name = record.get("full_name") or " ".join(x for x in (record.get("first_name"), record.get("last_name")) if x) or None
        quantiles = (p.get("stat_quantiles") or {}).get("pts") or {}
        out.append((
            int(game["id"]), int(p["player_id"]), int(p["projection_id"]), game_date, game_time,
            game.get("status"), game.get("home_team"), game.get("away_team"), teams.get(p.get("team_id")),
            name, player_key(name) if name else None, p.get("status"), p.get("status_source"),
            p.get("zero_reason"), p.get("play_probability"), p.get("start_probability"),
            p.get("expected_starter"), p.get("lineup_confirmed"), p.get("minutes_mean"), p.get("pts"),
            quantiles.get("p10"), quantiles.get("p50"), quantiles.get("p90"), _ts(p.get("inputs_as_of")),
            json.dumps(p),
        ))
    return out


INSERT = """INSERT INTO statapi_nba_player_projections
    (statapi_game_id, statapi_player_id, projection_id, game_date, game_time, game_status,
     home_team, away_team, team, player, player_key, status, status_source, zero_reason,
     play_probability, start_probability, expected_starter, lineup_confirmed, minutes_mean, pts,
     pts_p10, pts_p50, pts_p90, inputs_as_of, raw)
    VALUES %s
    ON CONFLICT (statapi_game_id, statapi_player_id, projection_id) DO NOTHING
    RETURNING 1"""


def persist(db: DatabaseManager, rows: list[tuple]) -> int:
    from psycopg2.extras import execute_values
    if not rows:
        return 0
    with db.connect() as conn:
        cur = conn.cursor()
        # fetch=True collects RETURNING rows from every page; cur.rowcount
        # would report only the last page (523 rows read as "23 new").
        return len(execute_values(cur, INSERT, rows, page_size=500, fetch=True))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--start", type=date.fromisoformat, help="first Eastern date (default: season start)")
    parser.add_argument("--end", type=date.fromisoformat, help="last Eastern date (default: yesterday ET)")
    parser.add_argument("--days-back", type=int, help="instead of --start: this many dates back from --end")
    parser.add_argument("--dry-run", action="store_true", help="fetch and count, write nothing")
    args = parser.parse_args()

    end = args.end or (eastern_date(datetime.now().astimezone()) - timedelta(days=1))
    start = end - timedelta(days=args.days_back - 1) if args.days_back else (args.start or SEASON_START)
    config = load_config()
    client = Client(os.environ.get("STAT_API_KEY", ""))
    db = DatabaseManager(config.database_url, initialize_schema=False)
    if not args.dry_run:
        for statement in STATEMENTS:
            db.execute(statement)

    teams = {int(t["id"]): t["abbreviation"] for t in _rows(client.get("/teams", {"limit": 100}), "teams")}
    players = load_players(client)
    total_rows = total_new = skipped_games = 0
    day = start
    while day <= end:
        games = games_on(client, day)
        finals = [g for g in games if is_final(g)]
        skipped = [g for g in games if not is_final(g)]
        skipped_games += len(skipped)
        day_rows: list[tuple] = []
        for game in finals:
            day_rows.extend(build_rows(game, projections_for(client, int(game["id"])), players, teams, client))
        new = 0 if args.dry_run else persist(db, day_rows)
        total_rows += len(day_rows)
        total_new += new
        note = f", {len(skipped)} not final ({', '.join(str(g.get('status')) for g in skipped)})" if skipped else ""
        print(f"{day}: {len(finals)} final games, {len(day_rows)} projection rows, {new} new{note}")
        day += timedelta(days=1)
    unnamed = sum(1 for p in players.values() if not p)
    print(f"done: {total_rows} rows read, {total_new} new, {skipped_games} games not final, "
          f"{unnamed} player ids with no name, {client.calls} requests, quota remaining {client.quota_remaining}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
