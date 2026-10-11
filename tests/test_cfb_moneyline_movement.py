"""CFB moneyline speed labels require observed timing and matched fresh books."""
from datetime import datetime, timedelta, timezone

from model.cfb_moneyline_movement import candidates


START = datetime(2026, 9, 26, 16, tzinfo=timezone.utc)
RETAIL = ("draftkings", "fanduel", "betmgm")


def snapshot(index: int, minute: int, home: int, away: int, *, books=RETAIL,
             quote_age_min: int = 0) -> dict:
    at = START + timedelta(minutes=minute)
    return {
        "history_id": index, "captured_at": at,
        "books": {key: {"ml_home": home, "ml_away": away,
                        "last_update": (at - timedelta(minutes=quote_age_min)).isoformat()}
                  for key in books},
    }


def test_24_hour_gap_is_not_steam_or_walking() -> None:
    # This is the shape of the Wyoming screenshot's first large repricing.
    history = [snapshot(1, 0, 135, -156), snapshot(2, 24 * 60, 112, -128)]
    found = candidates(history)
    assert len(found) == 1
    assert found[0]["alert_type"] == "gap_repricing"
    assert found[0]["details"]["interval_minutes"] == 1440
    assert found[0]["details"]["timing_verified"] is False


def test_three_matched_fresh_retail_books_qualify_as_observed_steam() -> None:
    history = [snapshot(1, 0, -110, -110), snapshot(2, 15, -125, 105)]
    found = candidates(history)
    assert len(found) == 1
    assert found[0]["alert_type"] == "steam"
    assert found[0]["side"] == "home"
    assert found[0]["details"]["signal_version"] == "cfb-moneyline-v2"
    assert found[0]["details"]["interval_minutes"] == 15
    assert found[0]["details"]["books_moved"] == 3


def test_stale_or_insufficient_book_support_cannot_claim_steam() -> None:
    old = snapshot(1, 0, -110, -110)
    stale = snapshot(2, 15, -125, 105, quote_age_min=36)
    two_books = snapshot(2, 15, -125, 105, books=RETAIL[:2])
    assert candidates([old, stale]) == []
    assert candidates([old, two_books]) == []


def test_walk_needs_multiple_active_steps_without_a_large_gap() -> None:
    gradual = [snapshot(1, 0, -110, -110), snapshot(2, 30, -120, 100),
               snapshot(3, 60, -130, 110)]
    walking = [item for item in candidates(gradual) if item["alert_type"] == "walking"]
    assert len(walking) == 1
    assert walking[0]["details"]["path_observations"] == 3
    assert walking[0]["details"]["max_step_minutes"] == 30
    one_jump = [snapshot(1, 0, -110, -110), snapshot(2, 30, -110, -110),
                snapshot(3, 60, -130, 110)]
    assert not any(item["alert_type"] == "walking" for item in candidates(one_jump))
    long_gap = [snapshot(1, 0, -110, -110), snapshot(2, 120, -120, 100),
                snapshot(3, 150, -130, 110)]
    assert not any(item["alert_type"] == "walking" for item in candidates(long_gap))
