"""Canonical integrity policy for MLB game-line odds ingestion.

All live MLB game-line writers must pass through these pure checks before a
matchup cache or odds-history row is updated. Historical backfills use the same
team/event/price rules with their historical snapshot timestamp.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable


class MlbOddsPolicyError(ValueError):
    """The event cannot be written safely and must fail closed."""


MAX_COMMENCE_DELTA = timedelta(hours=6)
_ATHLETICS_ALIASES = {
    "athletics",
    "oakland athletics",
    "sacramento athletics",
}


def normalize_team_name(name: str | None) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()
    return "athletics" if normalized in _ATHLETICS_ALIASES else normalized


def parse_utc(value: str | datetime | None, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MlbOddsPolicyError(f"invalid {field}: {value!r}") from exc
    else:
        raise MlbOddsPolicyError(f"missing {field}")
    if parsed.tzinfo is None:
        raise MlbOddsPolicyError(f"timezone-naive {field}")
    return parsed.astimezone(timezone.utc)


def validate_american_price(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MlbOddsPolicyError(f"non-numeric American price: {value!r}")
    numeric = float(value)
    if not math.isfinite(numeric) or not numeric.is_integer():
        raise MlbOddsPolicyError(f"non-integer American price: {value!r}")
    price = int(numeric)
    if -100 < price < 100:
        raise MlbOddsPolicyError(f"invalid American price inside (-100, +100): {price}")
    return price


def validate_event_prices(event: dict) -> None:
    """Reject the entire event when any advertised market price is invalid."""
    seen = 0
    for bookmaker in event.get("bookmakers") or []:
        for market in bookmaker.get("markets") or []:
            for outcome in market.get("outcomes") or []:
                if outcome.get("price") is None:
                    continue
                validate_american_price(outcome["price"])
                seen += 1
    if seen == 0:
        raise MlbOddsPolicyError("event has no priced outcomes")


def _american_probability(price: int) -> float:
    return 100.0 / (price + 100.0) if price > 0 else abs(price) / (abs(price) + 100.0)


def consensus_american(prices: Iterable[object]) -> int | None:
    """Average American odds in implied-probability space, never price space."""
    validated = [validate_american_price(price) for price in prices]
    if not validated:
        return None
    probability = sum(_american_probability(price) for price in validated) / len(validated)
    if not 0 < probability < 1:
        raise MlbOddsPolicyError(f"invalid consensus probability: {probability}")
    american = (
        -100.0 * probability / (1.0 - probability)
        if probability >= 0.5
        else 100.0 * (1.0 - probability) / probability
    )
    rounded = int(round(american))
    return -100 if rounded == 100 and probability >= 0.5 else rounded


def _candidate_identity_matches(event: dict, candidate: dict) -> bool:
    home = normalize_team_name(event.get("home_team"))
    away = normalize_team_name(event.get("away_team"))
    if not home or not away or home == away:
        raise MlbOddsPolicyError("event has missing or same-team identity")
    return (
        normalize_team_name(candidate.get("home_name")) == home
        and normalize_team_name(candidate.get("away_name")) == away
    )


def resolve_mlb_odds_event(
    event: dict,
    candidates: Iterable[dict],
    *,
    known_event_matchup_id: int | None = None,
    max_commence_delta: timedelta = MAX_COMMENCE_DELTA,
) -> dict:
    """Resolve by provider ID when known, otherwise exact teams + nearest time.

    Ambiguity, missing times, one-team matches, and excessive time deltas are
    rejected. This safely distinguishes split doubleheaders.
    """
    event_id = str(event.get("id") or "").strip()
    if not event_id:
        raise MlbOddsPolicyError("missing provider event id")
    event_commence = parse_utc(event.get("commence_time"), field="event commence_time")

    eligible: list[tuple[float, dict]] = []
    for candidate in candidates:
        if known_event_matchup_id is not None and int(candidate["id"]) != known_event_matchup_id:
            continue
        if not _candidate_identity_matches(event, candidate):
            continue
        candidate_commence = parse_utc(
            candidate.get("commence_time"),
            field="matchup commence_time",
        )
        delta = abs((candidate_commence - event_commence).total_seconds())
        if delta <= max_commence_delta.total_seconds():
            eligible.append((delta, candidate))

    if not eligible:
        suffix = f" mapped to matchup {known_event_matchup_id}" if known_event_matchup_id else ""
        raise MlbOddsPolicyError(f"no exact team/time match for event {event_id}{suffix}")

    eligible.sort(key=lambda item: item[0])
    if len(eligible) > 1 and math.isclose(eligible[0][0], eligible[1][0], abs_tol=60):
        raise MlbOddsPolicyError(f"ambiguous team/time match for event {event_id}")
    return eligible[0][1]


def require_pregame_capture(
    *,
    event_commence: str | datetime | None,
    matchup_commence: str | datetime | None,
    captured_at: datetime,
) -> None:
    captured = parse_utc(captured_at, field="captured_at")
    provider_start = parse_utc(event_commence, field="event commence_time")
    authoritative_start = parse_utc(matchup_commence, field="matchup commence_time")
    if captured >= provider_start or captured >= authoritative_start:
        raise MlbOddsPolicyError("capture is at or after first pitch")
