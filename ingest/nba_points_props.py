"""Capture NBA player-points prop lines for the top-scorers page.

The event listing is free. Each event's prop call costs credits on the shared
ODDS_API_KEY: with ``bookmakers=`` the price is markets x 1, so the two markets
(``player_points`` and its ``player_points_alternate`` ladder) over the six
configured books are 2 credits per game. A full 12-game night is about 24
credits a run.

The ladder (several over-only thresholds per player, "20+ points" and so on)
is what lets the page show a floor, a ceiling and a chance of 30+ instead of
one median line. normalize_prop_payload keeps every rung under ``ladder``.

Rows go to ``prop_odds_history`` (sport='nba', market='player_points'), one row
per (game, player, capture) with every book's line and over/under price, the
same shape the NFL and MLB prop captures write. ``game_date`` is the EASTERN
date of tip-off, so a 10:30pm ET game is not filed under tomorrow.

This is a dry run unless ``--apply`` is given.

Usage:
    python -m ingest.nba_points_props                       # dry run, today ET
    python -m ingest.nba_points_props --date 2026-10-21 --apply --max-credits 20
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import math
from typing import Any
from zoneinfo import ZoneInfo

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_prop_odds import normalize_prop_payload, require_credit_budget
from ingest.sportsbook_policy import BOOKMAKER_KEYS

ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT = "basketball_nba"
MARKETS: tuple[str, ...] = ("player_points", "player_points_alternate")
EASTERN = ZoneInfo("America/New_York")


def _start(event: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(str(event["commence_time"]).replace("Z", "+00:00"))


def eastern_date(at: datetime) -> date:
    return at.astimezone(EASTERN).date()


def select_events(events: list[dict[str, Any]], target: date, now: datetime) -> list[dict[str, Any]]:
    """Games tipping off on the Eastern date ``target`` that have not started."""
    chosen = [e for e in events if eastern_date(_start(e)) == target and _start(e) > now]
    return sorted(chosen, key=_start)


def estimated_credits(event_count: int) -> int:
    return event_count * len(MARKETS) * math.ceil(len(BOOKMAKER_KEYS) / 10)


def fetch_events(api_key: str) -> list[dict[str, Any]]:
    response = requests.get(f"{ODDS_BASE}/sports/{SPORT}/events", params={"apiKey": api_key}, timeout=20)
    response.raise_for_status()
    return list(response.json())


def _header_int(response: requests.Response, name: str) -> int | None:
    try:
        return int(response.headers.get(name))
    except (TypeError, ValueError):
        return None


def record_usage(db: DatabaseManager, response: requests.Response, event_id: str) -> None:
    """One odds_api_usage row per paid call, so /health's credit reading includes it."""
    db.execute(
        """INSERT INTO odds_api_usage (sport, purpose, endpoint, event_count, markets, bookmakers,
               requests_last, requests_used, requests_remaining, response_status, metadata)
           VALUES ('nba', 'nba_points_props', %s, 1, %s, %s, %s, %s, %s, %s, %s::jsonb)""",
        (f"{ODDS_BASE}/sports/{SPORT}/events/{{id}}/odds", ",".join(MARKETS), ",".join(BOOKMAKER_KEYS),
         _header_int(response, "x-requests-last"), _header_int(response, "x-requests-used"),
         _header_int(response, "x-requests-remaining"), response.status_code,
         json.dumps({"event_ids": [event_id]})),
    )


def persist_rows(db: DatabaseManager, event: dict[str, Any], payload: dict[str, Any], captured_at: datetime) -> int:
    captured = captured_at.astimezone(timezone.utc).replace(microsecond=0)
    capture_key = captured.isoformat()
    commence = _start(event)
    rows = [row for row in normalize_prop_payload(payload) if row["market"] in MARKETS]
    with db.connect() as conn:
        cursor = conn.cursor()
        for row in rows:
            cursor.execute(
                """
                INSERT INTO prop_odds_history
                    (sport, event_id, game_date, commence_time, home_team_name,
                     away_team_name, market, player, books, capture_key, captured_at)
                VALUES ('nba', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sport, event_id, market, player, capture_key) DO NOTHING
                """,
                (event["id"], eastern_date(commence), commence, event.get("home_team"),
                 event.get("away_team"), row["market"], row["player"], json.dumps(row["books"]),
                 capture_key, captured),
            )
    return len(rows)


def capture(
    *,
    db: DatabaseManager | None,
    api_key: str,
    target: date,
    apply: bool,
    max_credits: int = 0,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required")
    now = now or datetime.now(timezone.utc)
    selected = select_events(fetch_events(api_key), target, now)
    cost = estimated_credits(len(selected))
    summary: dict[str, Any] = {
        "date": target.isoformat(),
        "events": len(selected),
        "games": [f"{e.get('away_team')} @ {e.get('home_team')}" for e in selected],
        "estimatedCredits": cost,
        "rows": 0,
        "eventsWithoutLines": [],
    }
    if not apply or not selected:
        return summary
    require_credit_budget(estimated_credits=cost, max_credits=max_credits)
    if db is None:
        raise ValueError("database connection is required with --apply")
    for event in selected:
        response = requests.get(
            f"{ODDS_BASE}/sports/{SPORT}/events/{event['id']}/odds",
            params={
                "apiKey": api_key,
                "markets": ",".join(MARKETS),
                "bookmakers": ",".join(BOOKMAKER_KEYS),
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=30,
        )
        response.raise_for_status()
        record_usage(db, response, event["id"])
        written = persist_rows(db, event, response.json(), datetime.now(timezone.utc))
        if written == 0:
            # Books post player props late; an empty game is reported, never hidden.
            summary["eventsWithoutLines"].append(f"{event.get('away_team')} @ {event.get('home_team')}")
        summary["rows"] += written
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", help="Eastern date of the games (YYYY-MM-DD); default today ET")
    parser.add_argument("--apply", action="store_true", help="make paid prop calls and store them")
    parser.add_argument("--max-credits", type=int, default=0,
                        help="required hard ceiling with --apply; aborts before any paid call if exceeded")
    args = parser.parse_args()
    target = date.fromisoformat(args.date) if args.date else eastern_date(datetime.now(timezone.utc))
    config = load_config()
    db = None
    if args.apply:
        # Skip the per-run DDL pass: it waits on other writers' table locks and
        # times out (LockNotAvailable). This job only appends to existing tables.
        db = DatabaseManager(config.database_url, initialize_schema=False)
        db.require_tables(["prop_odds_history", "odds_api_usage"])
    result = capture(db=db, api_key=config.odds_api.api_key, target=target, apply=args.apply,
                     max_credits=max(0, args.max_credits))
    print(json.dumps({**result, "applied": args.apply}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
