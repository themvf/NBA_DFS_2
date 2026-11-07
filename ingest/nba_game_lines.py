"""NBA game spreads and totals, for the blowout question on the game-leaders panel.

One bulk ``/sports/basketball_nba/odds`` call per run, ``markets=spreads,totals``
with ``bookmakers=`` (six books, one region-equivalent): 2 credits whatever the
number of games. It runs inside ``ingest.nba_points_props`` so it shares that
job's three daily captures and its credit ceiling.

Rows go to ``nba_game_lines``: one row per game, written only when the game's
lines changed since its last stored row (``content_sha``), so a quiet day
writes nothing. The line in force at tip is the newest row captured before
``commence_time``. Every paid call still writes an ``odds_api_usage`` row
(purpose ``nba_game_lines``), which is what /health watches for freshness.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import statistics
from typing import Any
from zoneinfo import ZoneInfo

import requests

from db.database import DatabaseManager
from ingest.sportsbook_policy import BOOKMAKER_KEYS

ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT = "basketball_nba"
MARKETS: tuple[str, ...] = ("spreads", "totals")
CREDITS = len(MARKETS)  # bookmakers= with six books prices as markets x 1
EASTERN = ZoneInfo("America/New_York")


def _iso(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def book_lines(event: dict[str, Any]) -> dict[str, dict[str, float]]:
    """Per configured book: home spread, both spread prices, total and both total prices."""
    home, away = event.get("home_team"), event.get("away_team")
    out: dict[str, dict[str, float]] = {}
    for book in event.get("bookmakers") or []:
        if book.get("key") not in BOOKMAKER_KEYS:
            continue
        row: dict[str, float] = {}
        for market in book.get("markets") or []:
            outcomes = market.get("outcomes") or []
            if market.get("key") == "spreads":
                h = next((o for o in outcomes if o.get("name") == home), None)
                a = next((o for o in outcomes if o.get("name") == away), None)
                if h and a and h.get("point") is not None:
                    row.update(spread=float(h["point"]), home_price=h.get("price"), away_price=a.get("price"))
            elif market.get("key") == "totals":
                o = next((x for x in outcomes if x.get("name") == "Over"), None)
                u = next((x for x in outcomes if x.get("name") == "Under"), None)
                if o and u and o.get("point") is not None:
                    row.update(total=float(o["point"]), over_price=o.get("price"), under_price=u.get("price"))
        if row:
            out[str(book["key"])] = row
    return out


def summarize(event: dict[str, Any]) -> dict[str, Any] | None:
    """The row to store for one event, or None when no configured book quotes it."""
    books = book_lines(event)
    if not books:
        return None
    spreads = [b["spread"] for b in books.values() if "spread" in b]
    totals = [b["total"] for b in books.values() if "total" in b]
    content = {"books": books, "commence": event["commence_time"]}
    return {
        "event_id": str(event["id"]),
        "commence_time": _iso(event["commence_time"]),
        "home_team": event.get("home_team"),
        "away_team": event.get("away_team"),
        "home_spread": statistics.median(spreads) if spreads else None,
        "total": statistics.median(totals) if totals else None,
        "book_count": len(books),
        "books": books,
        "content_sha": hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest(),
    }


def select_events(events: list[dict[str, Any]], target: date, now: datetime) -> list[dict[str, Any]]:
    """Games tipping off on the Eastern date ``target`` that have not started."""
    return [e for e in events
            if _iso(e["commence_time"]).astimezone(EASTERN).date() == target and _iso(e["commence_time"]) > now]


def fetch(api_key: str, target: date, now: datetime) -> requests.Response:
    end = datetime.combine(target + timedelta(days=1), time(6), EASTERN).astimezone(timezone.utc)
    return requests.get(
        f"{ODDS_BASE}/sports/{SPORT}/odds",
        params={
            "apiKey": api_key, "markets": ",".join(MARKETS), "bookmakers": ",".join(BOOKMAKER_KEYS),
            "oddsFormat": "american", "dateFormat": "iso",
            "commenceTimeFrom": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "commenceTimeTo": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        timeout=30,
    )


INSERT_IF_CHANGED = """
INSERT INTO nba_game_lines (event_id, game_date, commence_time, home_team, away_team, home_spread, total,
                            book_count, books, content_sha, captured_at)
SELECT %(event_id)s, %(game_date)s, %(commence_time)s, %(home_team)s, %(away_team)s, %(home_spread)s, %(total)s,
       %(book_count)s, %(books)s::jsonb, %(content_sha)s, %(captured_at)s
WHERE (SELECT content_sha FROM nba_game_lines WHERE event_id = %(event_id)s
       ORDER BY captured_at DESC LIMIT 1) IS DISTINCT FROM %(content_sha)s
"""


def persist(db: DatabaseManager, rows: list[dict[str, Any]], captured_at: datetime) -> int:
    """Insert each game whose lines changed since its last stored row. Returns rows written."""
    written = 0
    stamp = captured_at.astimezone(timezone.utc).replace(microsecond=0)
    with db.connect() as conn:
        cursor = conn.cursor()
        for r in rows:
            cursor.execute(INSERT_IF_CHANGED, {
                **r, "books": json.dumps(r["books"], sort_keys=True), "captured_at": stamp,
                "game_date": r["commence_time"].astimezone(EASTERN).date(),
            })
            written += cursor.rowcount
    return written


def record_usage(db: DatabaseManager, response: requests.Response, events: int) -> None:
    def header(name: str) -> int | None:
        try:
            return int(response.headers.get(name))
        except (TypeError, ValueError):
            return None
    db.execute(
        """INSERT INTO odds_api_usage (sport, purpose, endpoint, event_count, markets, bookmakers,
               requests_last, requests_used, requests_remaining, response_status, metadata)
           VALUES ('nba', 'nba_game_lines', %s, %s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)""",
        (f"{ODDS_BASE}/sports/{SPORT}/odds", events, ",".join(MARKETS), ",".join(BOOKMAKER_KEYS),
         header("x-requests-last"), header("x-requests-used"), header("x-requests-remaining"), response.status_code),
    )


def capture(db: DatabaseManager, api_key: str, target: date, now: datetime) -> dict[str, Any]:
    response = fetch(api_key, target, now)
    response.raise_for_status()
    events = select_events(list(response.json()), target, now)
    record_usage(db, response, len(events))
    rows = [r for r in (summarize(e) for e in events) if r is not None]
    written = persist(db, rows, datetime.now(timezone.utc))
    return {"games": len(events), "quoted": len(rows), "changed": written}
