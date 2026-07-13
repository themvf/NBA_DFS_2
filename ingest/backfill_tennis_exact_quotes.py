"""Reconstruct exact Tennis quotes from the legacy per-book odds history.

The legacy ``game_odds_history.books`` payload retained exact bookmaker prices
and update timestamps, but did not normalize them into auditable quote rows.
This migration preserves that evidence in ``tennis_exact_quotes``.  It only
emits two-sided markets; old spread captures that lack the away line/price are
counted as unavailable and never inferred.

Usage:
    python -m ingest.backfill_tennis_exact_quotes
    python -m ingest.backfill_tennis_exact_quotes --matchup-id 123
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_foundation import ingest_live_event_quotes

logger = logging.getLogger(__name__)
RECONSTRUCTION_SOURCE = "legacy_odds_history_reconstruction"


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _decode_books(value: Any) -> dict[str, dict]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _market_payloads(book: dict, home: str, away: str) -> tuple[list[dict], int]:
    markets: list[dict] = []
    unavailable_spreads = 0

    if book.get("ml_home") is not None and book.get("ml_away") is not None:
        markets.append({
            "key": "h2h",
            "outcomes": [
                {"name": home, "price": book["ml_home"]},
                {"name": away, "price": book["ml_away"]},
            ],
        })

    if (book.get("total_line") is not None and book.get("over") is not None
            and book.get("under") is not None):
        line = float(book["total_line"])
        markets.append({
            "key": "totals",
            "outcomes": [
                {"name": "Over", "point": line, "price": book["over"]},
                {"name": "Under", "point": line, "price": book["under"]},
            ],
        })

    spread_home = book.get("spread_home")
    spread_away = book.get("spread_away")
    home_price = book.get("spread_home_price", book.get("spread_price"))
    away_price = book.get("spread_away_price")
    has_any_spread = any(value is not None for value in (
        spread_home, spread_away, home_price, away_price,
    ))
    complete_spread = all(value is not None for value in (
        spread_home, spread_away, home_price, away_price,
    ))
    if complete_spread and abs(float(spread_home) + float(spread_away)) < 1e-9:
        markets.append({
            "key": "spreads",
            "outcomes": [
                {"name": home, "point": float(spread_home), "price": home_price},
                {"name": away, "point": float(spread_away), "price": away_price},
            ],
        })
    elif has_any_spread:
        unavailable_spreads = 1

    return markets, unavailable_spreads


def _raw_event(row: dict) -> tuple[dict, int, int]:
    bookmakers: list[dict] = []
    incomplete_spreads = 0
    empty_books = 0
    for key, book in _decode_books(row["books"]).items():
        if not isinstance(book, dict):
            empty_books += 1
            continue
        markets, missing_spread = _market_payloads(
            book, row["home_player"], row["away_player"],
        )
        incomplete_spreads += missing_spread
        if not markets:
            empty_books += 1
            continue
        bookmakers.append({
            "key": key,
            "title": key.replace("_", " ").title(),
            "last_update": _iso(book.get("last_update") or row["captured_at"]),
            "markets": markets,
        })
    return ({
        "id": row["event_id"],
        "home_team": row["home_player"],
        "away_team": row["away_player"],
        "commence_time": _iso(row["commence_time"]),
        "bookmakers": bookmakers,
    }, incomplete_spreads, empty_books)


def run(db: DatabaseManager, matchup_id: int | None = None) -> dict:
    params: tuple[Any, ...] = ()
    filter_sql = ""
    if matchup_id is not None:
        filter_sql = "AND h.matchup_id = %s"
        params = (matchup_id,)
    rows = db.execute(
        f"""
        SELECT h.id, h.matchup_id, h.event_id, h.captured_at, h.books,
               tm.tour, tm.tournament, tm.commence_time,
               tm.home_player, tm.away_player
        FROM game_odds_history h
        JOIN tennis_matches tm ON tm.id = h.matchup_id
        WHERE h.sport = 'tennis'
          AND h.event_id IS NOT NULL
          AND h.books IS NOT NULL
          AND tm.commence_time IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM tennis_exact_quotes q
              WHERE q.provider_event_id = h.event_id
                AND q.captured_at = h.captured_at
                AND q.source = %s
          )
          {filter_sql}
        ORDER BY h.captured_at, h.id
        """,
        (RECONSTRUCTION_SOURCE, *params),
    )

    report = {
        "snapshots": len(rows),
        "processed": 0,
        "failed": 0,
        "quotes_inserted": 0,
        "quotes_rejected": 0,
        "incomplete_spread_books": 0,
        "books_without_complete_market": 0,
    }
    for row in rows:
        raw_event, incomplete_spreads, empty_books = _raw_event(row)
        report["incomplete_spread_books"] += incomplete_spreads
        report["books_without_complete_market"] += empty_books
        if not raw_event["bookmakers"]:
            continue
        try:
            result = ingest_live_event_quotes(
                db,
                tour=row["tour"],
                tournament=row["tournament"] or "Unknown",
                raw_event=raw_event,
                captured_at=row["captured_at"],
                quote_source=RECONSTRUCTION_SOURCE,
            )
            db.execute(
                """
                UPDATE tennis_matches SET
                    canonical_event_id=%s, event_revision_id=%s,
                    home_player_id=%s, away_player_id=%s
                WHERE id=%s
                """,
                (result["event_id"], result["event_revision_id"],
                 result["home_player_id"], result["away_player_id"], row["matchup_id"]),
            )
            report["processed"] += 1
            report["quotes_inserted"] += result["quotes_inserted"]
            report["quotes_rejected"] += result["quotes_rejected"]
        except Exception as exc:  # noqa: BLE001 -- report every bad legacy row
            report["failed"] += 1
            logger.exception("Exact-quote backfill failed for history row %s: %s", row["id"], exc)

    print(json.dumps(report, indent=2, default=str))
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill auditable Tennis exact-book quotes")
    parser.add_argument("--matchup-id", type=int)
    args = parser.parse_args()
    database = DatabaseManager(load_config().database_url)
    result = run(database, matchup_id=args.matchup_id)
    raise SystemExit(1 if result["failed"] else 0)
