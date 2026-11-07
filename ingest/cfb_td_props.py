"""DraftKings anytime-touchdown lines for upcoming college games (who the book expects to play).

Used by model/cfb_first_td.py only as an availability signal: a team the book lists
several players for, minus a player of ours it leaves off, suggests that player is not
expected to play. The prices themselves are not a model input.

One paid call per game: ``player_anytime_td`` from DraftKings alone (``bookmakers=``,
so markets x 1 = 1 credit). Games kicking off in the next few hours only, and a game
captured within ``SKIP_RECENT_HOURS`` is not bought again. Rows go to
``prop_odds_history`` (sport 'cfb'); every paid call is logged in ``odds_api_usage``.

    python -m ingest.cfb_td_props              # dry run: games and credits
    python -m ingest.cfb_td_props --apply
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_prop_odds import normalize_prop_payload

ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "americanfootball_ncaaf"
MARKET = "player_anytime_td"
BOOKMAKER = "draftkings"
WITHIN_HOURS = 4.0
MIN_HOURS = 0.25
SKIP_RECENT_HOURS = 3.0
MAX_CREDITS = 80

_UPCOMING = """
    SELECT m.odds_event_id, m.commence_time, h.name AS home, a.name AS away
    FROM cfb_matchups m JOIN cfb_teams h ON h.team_id = m.home_team_id
    JOIN cfb_teams a ON a.team_id = m.away_team_id
    WHERE m.odds_event_id IS NOT NULL AND m.home_implied IS NOT NULL
      AND m.commence_time > NOW() + %s * INTERVAL '1 hour'
      AND m.commence_time <= NOW() + %s * INTERVAL '1 hour'
      AND NOT EXISTS (SELECT 1 FROM prop_odds_history p
                      WHERE p.sport = 'cfb' AND p.event_id = m.odds_event_id AND p.market = %s
                        AND p.captured_at > NOW() - %s * INTERVAL '1 hour')
    ORDER BY m.commence_time"""


def select_games(db: DatabaseManager, within: float = WITHIN_HOURS, min_hours: float = MIN_HOURS) -> list[dict]:
    return [dict(r) for r in db.execute(_UPCOMING, (min_hours, within, MARKET, SKIP_RECENT_HOURS))]


def _number(response, name):
    try:
        return int(response.headers.get(name))
    except (TypeError, ValueError):
        return None


def persist(db: DatabaseManager, game: dict, payload: dict, captured: datetime) -> int:
    rows = [r for r in normalize_prop_payload(payload) if r["market"] == MARKET]
    key = captured.isoformat()
    with db.connect() as conn:
        cur = conn.cursor()
        for r in rows:
            cur.execute("""
                INSERT INTO prop_odds_history (sport, event_id, game_date, commence_time, home_team_name,
                    away_team_name, market, player, books, capture_key, captured_at)
                VALUES ('cfb', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sport, event_id, market, player, capture_key) DO NOTHING""",
                (game["odds_event_id"], game["commence_time"].date(), game["commence_time"], game["home"],
                 game["away"], MARKET, r["player"], json.dumps(r["books"]), key, captured))
    return len(rows)


def capture(db: DatabaseManager, api_key: str, apply: bool, max_credits: int = MAX_CREDITS) -> dict:
    games = select_games(db)
    if not apply:
        return {"games": len(games), "estimated_credits": len(games), "rows": 0}
    if not api_key:
        raise ValueError("ODDS_API_KEY is required")
    if len(games) > max_credits:
        raise ValueError(f"{len(games)} games would cost {len(games)} credits, above the {max_credits} ceiling")
    written = no_lines = 0
    for g in games:
        response = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/events/{g['odds_event_id']}/odds",
            params={"apiKey": api_key, "markets": MARKET, "bookmakers": BOOKMAKER,
                    "oddsFormat": "american", "dateFormat": "iso"},
            timeout=30)
        db.execute(
            """INSERT INTO odds_api_usage (sport, purpose, endpoint, event_count, markets, bookmakers,
                   requests_last, requests_used, requests_remaining, response_status, metadata)
               VALUES ('cfb', 'cfb_first_td_lines', %s, 1, %s, %s, %s, %s, %s, %s, %s::jsonb)""",
            (f"{ODDS_BASE}/sports/{SPORT_KEY}/events/{{id}}/odds", MARKET, BOOKMAKER,
             _number(response, "x-requests-last"), _number(response, "x-requests-used"),
             _number(response, "x-requests-remaining"), response.status_code,
             json.dumps({"event_ids": [g["odds_event_id"]]})))
        if response.status_code == 404:  # the provider no longer lists the event
            no_lines += 1
            continue
        response.raise_for_status()
        n = persist(db, g, response.json(), datetime.now(timezone.utc).replace(microsecond=0))
        written += n
        no_lines += n == 0
    return {"games": len(games), "estimated_credits": len(games), "rows": written, "games_without_lines": no_lines}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-credits", type=int, default=MAX_CREDITS)
    args = ap.parse_args(argv)
    cfg = load_config()
    db = DatabaseManager(cfg.database_url, initialize_schema=False)
    result = capture(db, os.environ.get("ODDS_API_KEY", ""), args.apply, args.max_credits)
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
