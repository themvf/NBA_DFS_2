"""Deterministic CFB market-movement context, contract revision 3 Appendix E.

The calculator is intentionally storage-agnostic.  It accepts normalized
capture dictionaries and returns either a complete measurement payload or a
bounded rejection record.  Persistence can therefore pin the exact endpoint
captures and configuration without recomputing a friendlier pair.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
import json
from math import floor, isfinite
from typing import Iterable, Mapping


DEFINITION_ID = "cfb_market_movement_context"
DEFINITION_VERSION = 1
MIN_START_AGE = timedelta(minutes=15)
MAX_START_AGE = timedelta(minutes=30)
MAX_QUOTE_AGE = timedelta(seconds=300)
MIN_COMMON_BOOKS = 4


def _dt(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def _decimal_from_american(value: object) -> float | None:
    price = _number(value)
    if price is None or price == 0:
        return None
    return 1 + (100 / abs(price) if price < 0 else price / 100)


def _price(quote: Mapping[str, object], decimal_key: str, american_key: str) -> float | None:
    direct = _number(quote.get(decimal_key))
    return direct if direct is not None else _decimal_from_american(quote.get(american_key))


def lower_median(values: Iterable[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("lower_median requires at least one value")
    return ordered[floor((len(ordered) - 1) / 2)]


@dataclass(frozen=True)
class MovementResult:
    status: str
    reason: str | None
    payload: dict


def _eligible_books(capture: Mapping[str, object], market: str, allowlist: frozenset[str]) -> tuple[dict, dict]:
    observed = _dt(capture.get("observed_at") or capture.get("captured_at"))
    accepted: dict[str, dict] = {}
    rejected: dict[str, str] = {}
    for book, raw_quote in (capture.get("books") or {}).items():
        if book not in allowlist:
            rejected[str(book)] = "not_allowlisted"
            continue
        quote = raw_quote if isinstance(raw_quote, Mapping) else {}
        update_keys = (("spread_home_updated_at", "spread_away_updated_at") if market == "spread"
                       else ("over_updated_at", "under_updated_at"))
        updates = [_dt(quote.get(key)) for key in update_keys]
        if not all(updates):
            fallback = _dt(quote.get("last_update") or quote.get("bookmaker_updated_at"))
            updates = [fallback, fallback]
        if observed is None or not all(updates):
            rejected[str(book)] = "missing_bookmaker_timestamp"
            continue
        ages = [observed - updated for updated in updates]
        if any(age < timedelta(0) for age in ages):
            rejected[str(book)] = "future_bookmaker_timestamp"
            continue
        if any(age > MAX_QUOTE_AGE for age in ages):
            rejected[str(book)] = "stale_quote"
            continue
        if market == "spread":
            line = _number(quote.get("spread_home"))
            other = _number(quote.get("spread_away"))
            home_price = _price(quote, "spread_home_decimal", "spread_home_price")
            away_price = _price(quote, "spread_away_decimal", "spread_away_price")
            if line is None or other is None or abs(line + other) > 1e-8:
                rejected[str(book)] = "incompatible_paired_sides"
                continue
            if home_price is None or away_price is None or min(home_price, away_price) <= 1:
                rejected[str(book)] = "invalid_price"
                continue
        elif market == "total":
            line = _number(quote.get("total_line"))
            over_price = _price(quote, "over_decimal", "over")
            under_price = _price(quote, "under_decimal", "under")
            if line is None:
                rejected[str(book)] = "missing_required_fields"
                continue
            if over_price is None or under_price is None or min(over_price, under_price) <= 1:
                rejected[str(book)] = "invalid_price"
                continue
        else:
            raise ValueError(f"unsupported market: {market}")
        accepted[str(book)] = {"line": line, "bookmaker_updated_at": [updated.isoformat() for updated in updates],
                               "quote_age_seconds": [age.total_seconds() for age in ages],
                               "quote_ids": list(quote.get(f"{market}_quote_ids") or quote.get("quote_ids") or [])}
    return accepted, rejected


def measure_movement(
    captures: Iterable[Mapping[str, object]], *, endpoint_capture_id: object,
    market: str, allowlist: Iterable[str], event_key: str,
    config_digest: str, scheduled_kickoff: datetime,
) -> MovementResult:
    """Measure the contract-defined interval ending at ``endpoint_capture_id``."""
    if market not in {"spread", "total"}:
        return MovementResult("rejected", "unsupported_market", {})
    ordered = sorted(
        captures,
        key=lambda row: (_dt(row.get("observed_at") or row.get("captured_at")) or datetime.min.replace(tzinfo=scheduled_kickoff.tzinfo), str(row.get("capture_id") or row.get("history_id"))),
    )
    endpoint = next((row for row in ordered if (row.get("capture_id") or row.get("history_id")) == endpoint_capture_id), None)
    if endpoint is None:
        return MovementResult("rejected", "missing_endpoint", {})
    end_time = _dt(endpoint.get("observed_at") or endpoint.get("captured_at"))
    if end_time is None or end_time >= scheduled_kickoff or endpoint.get("pregame_state", "pregame") != "pregame":
        return MovementResult("rejected", "temporal_or_origin_violation", {})
    revision = endpoint.get("schedule_revision_id")
    candidates = [
        row for row in ordered
        if row.get("schedule_revision_id") == revision
        and (_dt(row.get("observed_at") or row.get("captured_at")) is not None)
        and end_time - MAX_START_AGE <= _dt(row.get("observed_at") or row.get("captured_at")) <= end_time - MIN_START_AGE
    ]
    if not candidates:
        return MovementResult("rejected", "no_start_capture", {"endpoint_capture_id": str(endpoint_capture_id)})
    start_time = max(_dt(row.get("observed_at") or row.get("captured_at")) for row in candidates)
    start = sorted(
        (row for row in candidates if _dt(row.get("observed_at") or row.get("captured_at")) == start_time),
        key=lambda row: str(row.get("capture_id") or row.get("history_id")),
    )[0]
    allowed = frozenset(allowlist)
    start_books, start_rejected = _eligible_books(start, market, allowed)
    end_books, end_rejected = _eligible_books(endpoint, market, allowed)
    common = sorted(set(start_books) & set(end_books))
    if len(common) < MIN_COMMON_BOOKS:
        return MovementResult("rejected", "insufficient_book_intersection", {
            "start_capture_id": str(start.get("capture_id") or start.get("history_id")),
            "endpoint_capture_id": str(endpoint_capture_id), "common_books": common,
            "start_rejections": start_rejected, "endpoint_rejections": end_rejected,
        })
    deltas = {
        book: (start_books[book]["line"] - end_books[book]["line"] if market == "spread"
               else end_books[book]["line"] - start_books[book]["line"])
        for book in common
    }
    movement = lower_median(deltas.values())
    identity = {
        "definition_id": DEFINITION_ID, "definition_version": DEFINITION_VERSION,
        "event_key": event_key, "market": market,
        "start_capture_id": str(start.get("capture_id") or start.get("history_id")),
        "endpoint_capture_id": str(endpoint_capture_id),
        "schedule_revision_id": str(revision), "config_digest": config_digest,
    }
    payload = {
        **identity,
        "idempotency_key": sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
        "as_of_at": end_time.isoformat(), "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(), "interval_seconds": (end_time - start_time).total_seconds(),
        "scalar_value": movement, "unit": "line_points",
        "direction": "toward_home" if market == "spread" and movement > 0 else "toward_over" if market == "total" and movement > 0 else "neutral" if movement == 0 else "toward_away" if market == "spread" else "toward_under",
        "common_books": common, "common_book_count": len(common),
        "start_lower_median": lower_median(start_books[book]["line"] for book in common),
        "endpoint_lower_median": lower_median(end_books[book]["line"] for book in common),
        "book_deltas": {book: {"start": start_books[book]["line"], "end": end_books[book]["line"], "delta": deltas[book],
                                      "start_quote_ids": start_books[book]["quote_ids"], "endpoint_quote_ids": end_books[book]["quote_ids"],
                                      "start_quote_ages_seconds": start_books[book]["quote_age_seconds"],
                                      "endpoint_quote_ages_seconds": end_books[book]["quote_age_seconds"]} for book in common},
        "sign_counts": {"positive": sum(value > 0 for value in deltas.values()), "zero": sum(value == 0 for value in deltas.values()), "negative": sum(value < 0 for value in deltas.values())},
        "membership_only_books": sorted((set(start_books) ^ set(end_books))),
        "start_rejections": start_rejected, "endpoint_rejections": end_rejected,
        "continuous_path_claim": False,
    }
    if market == "total":
        payload["under_scalar_value"] = -movement
    return MovementResult("accepted", None, payload)
