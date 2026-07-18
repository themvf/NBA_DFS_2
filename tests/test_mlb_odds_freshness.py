from __future__ import annotations

from ingest.mlb_schedule import verify_fresh_upcoming_odds


class _FakeDatabase:
    def __init__(self, row: dict) -> None:
        self.row = row
        self.params = None

    def execute_one(self, _sql: str, params=None):
        self.params = params
        return self.row


def test_freshness_passes_when_no_upcoming_games_remain() -> None:
    db = _FakeDatabase({"upcoming_games": 0, "fresh_games": 0, "latest_capture": None})

    assert verify_fresh_upcoming_odds(db, "2026-07-17") is True


def test_freshness_fails_when_any_upcoming_game_has_no_recent_capture() -> None:
    db = _FakeDatabase({"upcoming_games": 4, "fresh_games": 3, "latest_capture": "now"})

    assert verify_fresh_upcoming_odds(db, "2026-07-17") is False
    assert db.params == (35, "2026-07-17")


def test_freshness_passes_after_any_upcoming_game_writes_a_recent_capture() -> None:
    db = _FakeDatabase({"upcoming_games": 4, "fresh_games": 4, "latest_capture": "now"})

    assert verify_fresh_upcoming_odds(db, "2026-07-17", max_age_minutes=20) is True
    assert db.params == (20, "2026-07-17")
