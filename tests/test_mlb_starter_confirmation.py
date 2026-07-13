from __future__ import annotations

from datetime import datetime, timezone

from ingest.mlb_schedule import _confirmed_starters_from_live_feed
from ingest import mlb_schedule


def test_live_feed_requires_both_official_lineups() -> None:
    payload = {
        "gameData": {"players": {"ID10": {"fullName": "Home SP"}}},
        "liveData": {"boxscore": {"teams": {
            "home": {"battingOrder": [1, 2, 3], "pitchers": [10]},
            "away": {"battingOrder": [], "pitchers": [20]},
        }}},
    }
    assert _confirmed_starters_from_live_feed(payload) is None


def test_live_feed_confirms_first_participating_pitcher_for_each_side() -> None:
    payload = {
        "gameData": {"players": {
            "ID10": {"fullName": "Home SP", "pitchHand": {"code": "R"}},
            "ID20": {"fullName": "Away SP", "pitchHand": {"code": "L"}},
        }},
        "liveData": {"boxscore": {"teams": {
            "home": {"battingOrder": [1, 2, 3], "pitchers": [10, 11]},
            "away": {"battingOrder": [4, 5, 6], "pitchers": [20, 21]},
        }}},
    }
    result = _confirmed_starters_from_live_feed(payload)
    assert result == {
        "home": {"id": 10, "name": "Home SP", "hand": "R", "status": "confirmed"},
        "away": {"id": 20, "name": "Away SP", "hand": "L", "status": "confirmed"},
    }


def test_confirmation_fetch_does_not_call_live_feed_outside_window(monkeypatch) -> None:
    monkeypatch.setattr(
        mlb_schedule.requests, "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected request")),
    )
    assert mlb_schedule._fetch_confirmed_starters(
        "123", "2026-07-17T23:00:00Z",
        now=datetime(2026, 7, 13, 1, tzinfo=timezone.utc),
    ) is None
