"""Prospective NFL player-prop evidence capture.

The event listing is free.  Per-event prop calls consume Odds API credits, so
this command is a dry run unless ``--apply`` is supplied.  Every applied call
stores the complete provider payload in ``nfl_evidence_observations`` before
writing the normalized convenience rows in ``prop_odds_history``.

Usage:
    python -m ingest.nfl_prop_odds --event-limit 5
    python -m ingest.nfl_prop_odds --event-limit 5 --apply
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from typing import Any

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_prop_probe import MARKETS, ODDS_BASE, SPORT
from ingest.sportsbook_policy import BOOKMAKER_KEYS
from model.nfl_context_engine import stable_digest


def normalize_prop_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Preserve market shapes while providing a player/book convenience view."""
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for bookmaker in payload.get("bookmakers", []):
        book_key = str(bookmaker.get("key") or "")
        if not book_key:
            continue
        for market in bookmaker.get("markets", []):
            market_key = str(market.get("key") or "")
            if not market_key:
                continue
            for outcome in market.get("outcomes", []):
                description = str(outcome.get("description") or "").strip()
                outcome_name = str(outcome.get("name") or "").strip()
                side = outcome_name.lower()
                if description:
                    player = description
                elif side not in {"over", "under", "yes", "no"}:
                    # Yes-only markets commonly put the player in `name`.
                    player, side = outcome_name, "yes"
                else:
                    # An anonymous Over/Under cannot safely be assigned.
                    continue
                if not player:
                    continue
                row = grouped.setdefault(
                    (market_key, player),
                    {"market": market_key, "player": player, "books": {}},
                )
                book = row["books"].setdefault(
                    book_key,
                    {
                        "bookmakerTitle": bookmaker.get("title"),
                        "lastUpdate": market.get("last_update") or bookmaker.get("last_update"),
                        "outcomes": [],
                    },
                )
                preserved = {
                    "name": outcome_name,
                    "description": outcome.get("description"),
                    "price": outcome.get("price"),
                    "point": outcome.get("point"),
                    "side": side,
                }
                book["outcomes"].append(preserved)
                if side in {"over", "under", "yes", "no"}:
                    book[side] = outcome.get("price")
                if outcome.get("point") is not None:
                    book["line"] = outcome.get("point")
    return sorted(grouped.values(), key=lambda row: (row["market"], row["player"]))


def persist_event_payload(
    db: DatabaseManager,
    *,
    event: dict[str, Any],
    payload: dict[str, Any],
    captured_at: datetime,
) -> int:
    """Persist raw evidence first, then normalized rows, idempotently."""
    captured = captured_at.astimezone(timezone.utc).replace(microsecond=0)
    capture_key = captured.isoformat()
    raw_digest = stable_digest(payload)
    idempotency_key = stable_digest(
        {"source": "the_odds_api", "event": event["id"], "capture": capture_key, "payload": raw_digest}
    )
    observation_id = f"oddsapi-{idempotency_key}"
    commence = event.get("commence_time")
    rows = normalize_prop_payload(payload)
    with db.connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO nfl_evidence_observations
                (observation_id, source, source_record_key, event_occurred_at,
                 source_published_at, system_observed_at, raw_payload,
                 payload_digest, idempotency_key)
            VALUES (%s, 'the_odds_api', %s, %s, NULL, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (
                observation_id,
                f"nfl-props:{event['id']}",
                commence,
                captured,
                json.dumps(payload),
                raw_digest,
                idempotency_key,
            ),
        )
        for row in rows:
            cursor.execute(
                """
                INSERT INTO prop_odds_history
                    (sport, event_id, game_date, commence_time, home_team_name,
                     away_team_name, market, player, books, capture_key, captured_at)
                VALUES ('nfl', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sport, event_id, market, player, capture_key) DO NOTHING
                """,
                (
                    event["id"],
                    str(commence)[:10] if commence else None,
                    commence,
                    event.get("home_team"),
                    event.get("away_team"),
                    row["market"],
                    row["player"],
                    json.dumps(row["books"]),
                    capture_key,
                    captured,
                ),
            )
    return len(rows)


def fetch_events(api_key: str) -> list[dict[str, Any]]:
    response = requests.get(
        f"{ODDS_BASE}/sports/{SPORT}/events",
        params={"apiKey": api_key},
        timeout=20,
    )
    response.raise_for_status()
    return list(response.json())


def require_credit_budget(*, estimated_credits: int, max_credits: int) -> None:
    if max_credits <= 0:
        raise ValueError("paid prop capture requires a positive --max-credits budget")
    if estimated_credits > max_credits:
        raise ValueError(
            f"estimated prop cost {estimated_credits} exceeds --max-credits {max_credits}"
        )


def capture(
    *,
    db: DatabaseManager | None,
    api_key: str,
    event_limit: int,
    apply: bool,
    max_credits: int = 0,
) -> dict[str, int]:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required")
    events = fetch_events(api_key)
    now = datetime.now(timezone.utc)
    future = [
        event
        for event in events
        if datetime.fromisoformat(str(event["commence_time"]).replace("Z", "+00:00")) > now
    ]
    selected = future[:event_limit] if event_limit > 0 else future
    estimated_credits = len(selected) * len(MARKETS) * math.ceil(len(BOOKMAKER_KEYS) / 10)
    if not apply:
        return {"events": len(selected), "estimatedCredits": estimated_credits, "rows": 0}
    require_credit_budget(estimated_credits=estimated_credits, max_credits=max_credits)
    if db is None:
        raise ValueError("database connection is required with --apply")

    written = 0
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
        written += persist_event_payload(
            db,
            event=event,
            payload=response.json(),
            captured_at=datetime.now(timezone.utc),
        )
    return {"events": len(selected), "estimatedCredits": estimated_credits, "rows": written}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-limit", type=int, default=5, help="0 means every upcoming event")
    parser.add_argument("--apply", action="store_true", help="make paid prop calls and persist them")
    parser.add_argument(
        "--max-credits", type=int, default=0,
        help="required hard ceiling for an applied capture; paid calls abort before execution if exceeded",
    )
    args = parser.parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url) if args.apply else None
    result = capture(
        db=db,
        api_key=config.odds_api.api_key,
        event_limit=max(0, args.event_limit),
        apply=args.apply,
        max_credits=max(0, args.max_credits),
    )
    print(json.dumps({**result, "applied": args.apply}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
