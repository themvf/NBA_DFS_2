"""CFBD player box scores -> per-player, per-game DraftKings points.

Fetches `/games/players` a whole week at a time (every FBS game in one call),
so a transfer's games at his old school come along without knowing where he
played. Two tables:

    cfb_player_game_boxes   raw CFBD payload per game, append-safe; lets a
                            parsing fix be re-run without the API key
    cfb_player_game_stats   one row per player-game with the stats DraftKings
                            CFB Classic scores and the points they earn

Scoring (DraftKings CFB Classic, offense): pass TD 4, 0.04/pass yd, 300+ yds +3,
INT -1, rush/rec TD 6, 0.1/yd, 100+ rush or rec yds +3, reception 1, kick/punt
return TD 6, fumble lost -1. 2-point conversions are not in CFBD box scores and
are left out (documented, never guessed).

    python -m ingest.cfb_player_games --season 2026 --weeks 1-4
    python -m ingest.cfb_player_games --reparse     # from stored raw, no key
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time

import requests
from psycopg2.extras import Json, execute_values

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)
CFBD_BASE = "https://api.collegefootballdata.com"
PARSER_VERSION = "cfb-player-games-v1"

DDL = [
    """CREATE TABLE IF NOT EXISTS cfb_player_game_boxes (
        cfbd_game_id BIGINT PRIMARY KEY,
        season INTEGER NOT NULL, week INTEGER NOT NULL, season_type TEXT NOT NULL,
        payload JSONB NOT NULL, fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS cfb_player_game_stats (
        cfbd_game_id BIGINT NOT NULL, cfbd_player_id TEXT NOT NULL,
        season INTEGER NOT NULL, week INTEGER NOT NULL, season_type TEXT NOT NULL,
        team TEXT NOT NULL, player_name TEXT NOT NULL,
        pass_att INTEGER, pass_cmp INTEGER, pass_yds DOUBLE PRECISION, pass_td INTEGER, interceptions INTEGER,
        rush_att INTEGER, rush_yds DOUBLE PRECISION, rush_td INTEGER,
        receptions INTEGER, rec_yds DOUBLE PRECISION, rec_td INTEGER,
        fumbles_lost INTEGER, return_td INTEGER,
        dk_points DOUBLE PRECISION NOT NULL, parser_version TEXT NOT NULL,
        PRIMARY KEY (cfbd_game_id, cfbd_player_id))""",
    "CREATE INDEX IF NOT EXISTS idx_cfb_player_game_stats_name ON cfb_player_game_stats (player_name)",
]


def _num(value) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def dk_points(s: dict) -> float:
    pts = (s["pass_yds"] * 0.04 + s["pass_td"] * 4 - s["interceptions"]
           + s["rush_yds"] * 0.1 + s["rush_td"] * 6
           + s["rec_yds"] * 0.1 + s["rec_td"] * 6 + s["receptions"]
           + s["return_td"] * 6 - s["fumbles_lost"])
    if s["pass_yds"] >= 300:
        pts += 3
    if s["rush_yds"] >= 100:
        pts += 3
    if s["rec_yds"] >= 100:
        pts += 3
    return round(pts, 2)


def parse_game(game: dict) -> list[dict]:
    """Every offensive contributor in one CFBD game, keyed by CFBD athlete id."""
    players: dict[str, dict] = {}

    def row(athlete: dict, team: str) -> dict:
        pid = str(athlete["id"])
        if pid not in players:
            players[pid] = {"cfbd_player_id": pid, "team": team, "player_name": athlete["name"],
                            "pass_att": 0, "pass_cmp": 0, "pass_yds": 0.0, "pass_td": 0, "interceptions": 0,
                            "rush_att": 0, "rush_yds": 0.0, "rush_td": 0, "receptions": 0, "rec_yds": 0.0,
                            "rec_td": 0, "fumbles_lost": 0, "return_td": 0}
        return players[pid]

    fields = {
        ("passing", "YDS"): "pass_yds", ("passing", "TD"): "pass_td", ("passing", "INT"): "interceptions",
        ("rushing", "CAR"): "rush_att", ("rushing", "YDS"): "rush_yds", ("rushing", "TD"): "rush_td",
        ("receiving", "REC"): "receptions", ("receiving", "YDS"): "rec_yds", ("receiving", "TD"): "rec_td",
        ("fumbles", "LOST"): "fumbles_lost",
    }
    for team in game.get("teams") or []:
        name = team.get("team") or ""
        for category in team.get("categories") or []:
            for stat_type in category.get("types") or []:
                cat, typ = category.get("name"), stat_type.get("name")
                for athlete in stat_type.get("athletes") or []:
                    if athlete.get("id") in (None, "", "-9999") or str(athlete.get("name", "")).upper() == "TEAM":
                        continue
                    if (cat, typ) == ("passing", "C/ATT"):
                        made, _, att = str(athlete.get("stat", "0/0")).partition("/")
                        r = row(athlete, name)
                        r["pass_cmp"] += int(_num(made)); r["pass_att"] += int(_num(att))
                    elif (cat, typ) in fields:
                        r = row(athlete, name)
                        r[fields[(cat, typ)]] += _num(athlete.get("stat"))
                    elif cat in ("kickReturns", "puntReturns") and typ == "TD":
                        # Only a return TD scores; only create a row when there is one.
                        if _num(athlete.get("stat")):
                            row(athlete, name)["return_td"] += int(_num(athlete.get("stat")))
    out = []
    for r in players.values():
        for key in ("pass_td", "interceptions", "rush_att", "rush_td", "receptions", "rec_td", "fumbles_lost"):
            r[key] = int(r[key])
        r["dk_points"] = dk_points(r)
        out.append(r)
    return out


def fetch_week(api_key: str, season: int, week: int, season_type: str) -> list[dict]:
    for attempt in range(4):
        try:
            response = requests.get(f"{CFBD_BASE}/games/players",
                                    params={"year": season, "week": week, "seasonType": season_type},
                                    headers={"Authorization": f"Bearer {api_key}"}, timeout=120)
            response.raise_for_status()
            logger.info("CFBD games/players %s wk%s %s: %s games; calls remaining %s", season, week, season_type,
                        len(response.json() or []), response.headers.get("X-CallLimit-Remaining"))
            return response.json() or []
        except requests.RequestException:
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)
    return []


STAT_COLUMNS = ("pass_att", "pass_cmp", "pass_yds", "pass_td", "interceptions", "rush_att", "rush_yds", "rush_td",
                "receptions", "rec_yds", "rec_td", "fumbles_lost", "return_td", "dk_points")


def store(db: DatabaseManager, games: list[dict], season: int, week: int, season_type: str, keep_raw: bool = True) -> int:
    """Batched upsert of one week: raw payloads and parsed player-game rows."""
    raw = [(int(g["id"]), season, week, season_type, Json(g)) for g in games]
    rows = [(int(g["id"]), r["cfbd_player_id"], season, week, season_type, r["team"], r["player_name"],
             *[r[c] for c in STAT_COLUMNS], PARSER_VERSION) for g in games for r in parse_game(g)]
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in ("team", "player_name", *STAT_COLUMNS, "parser_version"))
    with db.connect() as conn:
        cur = conn.cursor()
        for statement in DDL:
            cur.execute(statement)
        if keep_raw and raw:
            execute_values(cur, """INSERT INTO cfb_player_game_boxes (cfbd_game_id, season, week, season_type, payload)
                VALUES %s ON CONFLICT (cfbd_game_id) DO UPDATE SET payload = EXCLUDED.payload, fetched_at = NOW()""", raw)
        if rows:
            execute_values(cur, f"""INSERT INTO cfb_player_game_stats (cfbd_game_id, cfbd_player_id, season, week, season_type,
                team, player_name, {", ".join(STAT_COLUMNS)}, parser_version) VALUES %s
                ON CONFLICT (cfbd_game_id, cfbd_player_id) DO UPDATE SET {updates}""", rows, page_size=1000)
    return len(rows)


def _weeks(spec: str) -> list[int]:
    lo, _, hi = spec.partition("-")
    return list(range(int(lo), int(hi or lo) + 1))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, action="append")
    parser.add_argument("--weeks", default="1-16")
    parser.add_argument("--postseason", action="store_true")
    parser.add_argument("--reparse", action="store_true", help="re-derive stats from stored raw payloads")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    if args.reparse:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT season, week, season_type, payload FROM cfb_player_game_boxes")
            rows = cur.fetchall()
        groups: dict[tuple, list] = {}
        for r in rows:
            groups.setdefault((r["season"], r["week"], r["season_type"]), []).append(r["payload"])
        total = sum(store(db, games, *key, keep_raw=False) for key, games in groups.items())
        logger.info("Reparsed %s games into %s player-game rows", len(rows), total)
        return
    api_key = os.environ.get("CFBD_API_KEY")
    if not api_key:
        raise SystemExit("CFBD_API_KEY is required")
    for season in args.season or [2026]:
        plan = [(w, "regular") for w in _weeks(args.weeks)] + ([(1, "postseason")] if args.postseason else [])
        for week, season_type in plan:
            games = fetch_week(api_key, season, week, season_type)
            if not games:
                logger.info("%s wk%s %s: no games", season, week, season_type)
                continue
            logger.info("%s wk%s %s: %s player-game rows", season, week, season_type, store(db, games, season, week, season_type))


if __name__ == "__main__":
    main()
