"""A failed MLB fetch is a failure, never an empty slate.

Until 2026-09-30 fetch_schedule / fetch_odds / fetch_scores / fetch_props
swallowed transport errors and returned [] or 0, which every caller read as an
off-day: an exhausted Odds API quota (401) looked like "0 matchups updated".
These tests pin the raise, and that one bad date does not stop settlement of
the others.
"""
from __future__ import annotations

import pytest
import requests

import ingest.mlb_prop_odds as props
import ingest.mlb_schedule as sched
import ingest.mlb_terminal_settlement as settlement


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"{status} error", response=response)


def _raising(exc):
    def get(*_args, **_kwargs):
        raise exc
    return get


class _UpcomingDb:
    """fetch_odds asks whether anything is upcoming before it pays."""
    def execute_one(self, *_args, **_kwargs):
        return {"scheduled": 3, "upcoming": 3}


def test_schedule_fetch_failure_raises_not_empty(monkeypatch):
    monkeypatch.setattr(sched.requests, "get", _raising(requests.ConnectionError("down")))
    with pytest.raises(sched.MlbStatsApiError, match="schedule request failed"):
        sched.fetch_schedule(object(), "2026-09-30")


def test_scores_fetch_failure_raises_not_zero(monkeypatch):
    monkeypatch.setattr(sched.requests, "get", _raising(requests.Timeout("slow")))
    with pytest.raises(sched.MlbStatsApiError, match="scores request failed"):
        sched.fetch_scores(object(), "2026-09-30")


def test_exhausted_quota_raises_with_its_status(monkeypatch):
    monkeypatch.setattr(sched.requests, "get", _raising(_http_error(401)))
    with pytest.raises(sched.OddsApiError, match="HTTP 401"):
        sched.fetch_odds(_UpcomingDb(), "key", "2026-09-30")


def test_new_errors_are_still_request_exceptions():
    # Callers that already isolate transport failures keep working.
    assert issubclass(sched.OddsApiError, requests.RequestException)
    assert issubclass(sched.MlbStatsApiError, requests.RequestException)


def test_prop_event_list_failure_raises(monkeypatch):
    monkeypatch.setattr(props.requests, "get", _raising(_http_error(401)))
    with pytest.raises(props.PropCaptureError, match="HTTP 401"):
        props.fetch_props(object(), "key")


def test_one_bad_date_does_not_stop_settlement(monkeypatch):
    calls = {"fetched": [], "settled": 0}

    class Db:
        def execute(self, *_args, **_kwargs):
            return [{"game_date": "2026-09-28"}, {"game_date": "2026-09-29"}]

    def fetch_scores(_db, game_date):
        calls["fetched"].append(game_date)
        if game_date == "2026-09-28":
            raise sched.MlbStatsApiError("scores request failed")
        return 4

    import model.line_alerts as line_alerts
    import model.mlb_terminal_signals as signals
    monkeypatch.setattr(settlement, "fetch_scores", fetch_scores)
    monkeypatch.setattr(line_alerts, "settle", lambda _db, _sport: calls.__setitem__("settled", 7) or 7)
    monkeypatch.setattr(signals, "run", lambda _db, settle_only: None)

    with pytest.raises(RuntimeError, match="2026-09-28"):
        settlement.settle_results(Db())
    assert calls["fetched"] == ["2026-09-28", "2026-09-29"], "the later date was still fetched"
    assert calls["settled"] == 7, "settlement ran despite the failed date"
