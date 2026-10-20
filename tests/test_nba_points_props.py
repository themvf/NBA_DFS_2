from datetime import date, datetime, timezone

import pytest

from ingest import nba_points_props as m


def _event(eid, commence):
    return {"id": eid, "commence_time": commence, "home_team": "H", "away_team": "A"}


def test_select_events_uses_eastern_date_and_skips_started_games():
    now = datetime(2026, 10, 21, 23, 45, tzinfo=timezone.utc)
    events = [
        _event("started", "2026-10-21T23:30:00Z"),       # 7:30pm ET, already tipped
        _event("late", "2026-10-22T02:30:00Z"),          # 10:30pm ET Oct 21, next day in UTC
        _event("tomorrow", "2026-10-22T23:30:00Z"),
    ]
    assert [e["id"] for e in m.select_events(events, date(2026, 10, 21), now)] == ["late"]


def test_eastern_date_of_a_late_tipoff():
    assert m.eastern_date(datetime(2026, 10, 22, 2, 30, tzinfo=timezone.utc)) == date(2026, 10, 21)


def test_credit_estimate_is_one_per_game_with_six_books():
    assert m.estimated_credits(12) == 12


def test_capture_refuses_without_budget(monkeypatch):
    future = "2099-01-01T23:00:00Z"
    monkeypatch.setattr(m, "fetch_events", lambda key: [_event("e", future)])
    with pytest.raises(ValueError):
        m.capture(db=None, api_key="k", target=date(2099, 1, 1), apply=True, max_credits=0)


def test_dry_run_makes_no_paid_call(monkeypatch):
    future = "2099-01-01T23:00:00Z"
    monkeypatch.setattr(m, "fetch_events", lambda key: [_event("e", future)])
    monkeypatch.setattr(m.requests, "get", lambda *a, **k: pytest.fail("paid call in dry run"))
    out = m.capture(db=None, api_key="k", target=date(2099, 1, 1), apply=False)
    assert out["events"] == 1 and out["estimatedCredits"] == 1 and out["rows"] == 0
