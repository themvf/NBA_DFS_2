"""Prospective CFB moneyline movement with explicit observation-time evidence.

The v1 shared detector could call a 24-hour capture gap "steam" and a
two-point open/current comparison "walking". This classifier only emits those
labels when the stored path can support their timing claims. It makes no claim
about price changes between captures.
"""
from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite


VERSION = "cfb-moneyline-v2"
RETAIL = ("draftkings", "fanduel", "fanatics", "williamhill_us", "betmgm")
MIN_BOOKS = 3
MAX_QUOTE_AGE_MIN = 35
STEAM_MAX_GAP_MIN = 40
STEAM_MOVE_PP = 1.5
WALK_MIN_SPAN_MIN = 40
WALK_MAX_SPAN_MIN = 360
WALK_MAX_STEP_MIN = 90
WALK_MOVE_PP = 2.0
WALK_MIN_ACTIVE_STEP_PP = 0.25


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _implied(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not isfinite(value) or abs(value) < 100 or int(value) != value:
        return None
    return 100 / (100 + value) if value > 0 else -value / (100 - value)


def _home_fair(book: dict, captured_at: datetime) -> float | None:
    stamp = book.get("h2h_last_update") or book.get("last_update")
    if not stamp:
        return None
    try:
        age = (_utc(captured_at) - _utc(stamp)).total_seconds() / 60
    except (TypeError, ValueError):
        return None
    if not 0 <= age <= MAX_QUOTE_AGE_MIN:
        return None
    home, away = _implied(book.get("ml_home")), _implied(book.get("ml_away"))
    return home / (home + away) if home is not None and away is not None else None


def _book_probabilities(row: dict) -> dict[str, float]:
    books = row.get("books") or {}
    return {key: value for key in RETAIL
            if (value := _home_fair(books.get(key) or {}, row["captured_at"])) is not None}


def candidates(history: list[dict]) -> list[dict]:
    """Classify the newest capture; input is pregame and chronological."""
    if len(history) < 2:
        return []
    current, previous = history[-1], history[-2]
    current_at, previous_at = _utc(current["captured_at"]), _utc(previous["captured_at"])
    gap = (current_at - previous_at).total_seconds() / 60
    if gap <= 0:
        return []
    output = []
    current_probs = _book_probabilities(current)
    previous_probs = _book_probabilities(previous)
    matched = current_probs.keys() & previous_probs.keys()
    for side, direction in (("home", 1), ("away", -1)):
        supporting = sorted(key for key in matched
                            if (current_probs[key] - previous_probs[key]) * direction * 100 >= STEAM_MOVE_PP - 1e-9)
        if len(supporting) >= MIN_BOOKS:
            alert_type = "steam" if gap <= STEAM_MAX_GAP_MIN else "gap_repricing"
            output.append({"alert_type": alert_type, "side": side, "details": {
                    "market": "moneyline", "signal_version": VERSION, "detector_version": VERSION,
                    "trigger_history_id": current["history_id"],
                    "previous_history_id": previous["history_id"],
                    "trigger_capture_at": current_at.isoformat(),
                    "interval_minutes": round(gap, 2), "books_moved": len(supporting),
                    "timing_verified": alert_type == "steam",
                    "supporting_books": supporting,
                    "avg_move_pp": round(sum((current_probs[k] - previous_probs[k]) * direction * 100
                                             for k in supporting) / len(supporting), 3),
            }})
    # Walks need an observed, monotone path. A large unobserved interval is a
    # gap repricing, not evidence that the market moved gradually.
    window = [row for row in history
              if 0 <= (current_at - _utc(row["captured_at"])).total_seconds() / 60 <= WALK_MAX_SPAN_MIN]
    if len(window) < 3:
        return output
    times = [_utc(row["captured_at"]) for row in window]
    span = (times[-1] - times[0]).total_seconds() / 60
    steps = [(b - a).total_seconds() / 60 for a, b in zip(times, times[1:])]
    if not (WALK_MIN_SPAN_MIN <= span <= WALK_MAX_SPAN_MIN) or not all(0 < step <= WALK_MAX_STEP_MIN for step in steps):
        return output
    snapshots = [_book_probabilities(row) for row in window]
    matched = set.intersection(*(set(snapshot) for snapshot in snapshots))
    for side, direction in (("home", 1), ("away", -1)):
        supporting = []
        for key in matched:
            values = [snapshot[key] * direction for snapshot in snapshots]
            if (values[-1] - values[0]) * 100 >= WALK_MOVE_PP - 1e-9 and all(
                later >= earlier - 1e-9 for earlier, later in zip(values, values[1:])
            ) and sum((later - earlier) * 100 >= WALK_MIN_ACTIVE_STEP_PP - 1e-9
                      for earlier, later in zip(values, values[1:])) >= 2:
                supporting.append(key)
        if len(supporting) >= MIN_BOOKS:
            supporting.sort()
            output.append({"alert_type": "walking", "side": side, "details": {
                "market": "moneyline", "signal_version": VERSION, "detector_version": VERSION,
                "trigger_history_id": current["history_id"],
                "previous_history_id": previous["history_id"],
                "opening_history_id": window[0]["history_id"],
                "trigger_capture_at": current_at.isoformat(),
                "path_observations": len(window), "path_minutes": round(span, 2),
                "max_step_minutes": round(max(steps), 2), "overlap_books": len(supporting),
                "supporting_books": supporting,
                "drift_pp": round(sum((snapshots[-1][k] - snapshots[0][k]) * direction * 100
                                      for k in supporting) / len(supporting), 3),
            }})
    return output
