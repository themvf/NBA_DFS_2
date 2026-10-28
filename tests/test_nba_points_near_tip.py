from datetime import date, datetime, timedelta, timezone
import pytest
from ingest import nba_points_near_tip as m

NOW = datetime(2026, 10, 28, 23, 55, tzinfo=timezone.utc)
EVENT = {"id": "odds1", "home_team": "Boston Celtics", "away_team": "Brooklyn Nets",
         "commence_time": "2026-10-29T00:10:00Z"}
GAME = {"id": "espn1", "home": "Boston Celtics", "away": "Brooklyn Nets",
        "start": "2026-10-29T00:00:00Z", "state": "pre", "status": "STATUS_SCHEDULED"}


def test_official_earlier_start_wins_and_join_requires_both_teams():
    cutoff, error = m.game_cutoff(EVENT, [GAME])
    assert cutoff == NOW + timedelta(minutes=5) and error is None
    assert m.game_cutoff(EVENT, [GAME, GAME])[0] is None
    assert m.game_cutoff(EVENT, [{**GAME, "away": "New York Knicks"}])[0] is None
    assert m.game_cutoff(EVENT, [{**GAME, "state": "in"}])[0] is None
    assert m.game_cutoff(EVENT, [{**GAME, "status": "STATUS_POSTPONED"}])[0] is None


def test_separate_nonoverlapping_windows_and_restart_dedup():
    cutoff = NOW + timedelta(minutes=5)
    assert m.due_checkpoints(EVENT, cutoff, NOW, {}) == [("t_minus_5m", "odds1:t_minus_5m")]
    assert not m.due_checkpoints(EVENT, cutoff, NOW, {"odds1:t_minus_5m": {}})
    assert m.due_checkpoints(EVENT, cutoff, cutoff-timedelta(seconds=45), {}) == [("t_minus_1m", "odds1:t_minus_1m")]
    assert not m.due_checkpoints(EVENT, cutoff, cutoff-timedelta(seconds=5), {})
    assert not m.due_checkpoints(EVENT, cutoff, cutoff, {})


def test_late_payload_wrong_identity_and_future_provider_update_rejected():
    p = {**EVENT, "bookmakers": []}
    cutoff = NOW + timedelta(minutes=5)
    assert m.valid_payload(p, EVENT, cutoff, NOW)[0]
    assert not m.valid_payload(p, EVENT, cutoff, cutoff)[0]
    assert not m.valid_payload({**p, "id": "other"}, EVENT, cutoff, NOW)[0]
    assert not m.valid_payload({**p, "home_team": "New York Knicks"}, EVENT, cutoff, NOW)[0]
    assert not m.valid_payload({**p, "commence_time": NOW.isoformat()}, EVENT, cutoff, NOW)[0]
    assert not m.valid_payload({**p, "bookmakers": [{"last_update": cutoff.isoformat()}]}, EVENT, cutoff, NOW)[0]


class Response:
    def raise_for_status(self): pass
    def json(self):
        return {"events": [{"id": "espn1", "date": GAME["start"], "status": {"type": {"state": "pre", "name": "STATUS_SCHEDULED"}},
                 "competitions": [{"date": GAME["start"], "competitors": [
                     {"homeAway": "home", "team": {"displayName": GAME["home"]}},
                     {"homeAway": "away", "team": {"displayName": GAME["away"]}}]}]}]}


def setup_discovery(monkeypatch):
    monkeypatch.setattr(m, "fetch_events", lambda key: [EVENT])
    monkeypatch.setattr(m.requests, "get", lambda *args, **kwargs: Response())


def test_dry_run_no_paid_capture(tmp_path, monkeypatch):
    setup_discovery(monkeypatch)
    monkeypatch.setattr(m, "paid_capture", lambda *args: pytest.fail("paid in dry run"))
    state = {"attempts": {}, "reserved_credits": 0}
    m.run_once(target=date(2026,10,28),root=tmp_path,state=state,api_key="key",now=NOW)
    assert not state["attempts"] and not (tmp_path/'2026-10-28/state.json').exists()


def test_reserved_before_call_failure_not_retried_budget_enforced(tmp_path, monkeypatch):
    setup_discovery(monkeypatch)
    state = {"attempts": {}, "reserved_credits": 0}
    calls = []
    def paid(*args):
        assert (tmp_path/'2026-10-28/state.json').exists()
        assert state["reserved_credits"] == 1
        calls.append(1)
        raise TimeoutError()
    monkeypatch.setattr(m, "paid_capture", paid)
    for now in [NOW, NOW, NOW+timedelta(minutes=4)]:
        m.run_once(target=date(2026,10,28),root=tmp_path,state=state,api_key="key",
                   db=object(),apply=True,max_credits=1,now=now)
    assert len(calls) == 1
    assert state["attempts"]["odds1:t_minus_5m"]["status"] == "failed"
    assert state["plan"][0]["budget_blocked"]


def test_worker_lock_prevents_overlap_and_releases(tmp_path):
    with m.worker_lock(tmp_path/'lock'):
        with pytest.raises((OSError, BlockingIOError)):
            with m.worker_lock(tmp_path/'lock'): pass
    with m.worker_lock(tmp_path/'lock'): pass


def test_paid_call_only_main_market_preserves_raw_and_conservative_cutoff(tmp_path, monkeypatch):
    cutoff = NOW + timedelta(minutes=5)
    class PaidResponse:
        status_code = 200
        headers = {"x-requests-last": "1"}
        def json(self): return {**EVENT, "bookmakers": []}
    def request(url, **kwargs):
        assert kwargs['params']['markets'] == 'player_points'
        assert len(kwargs['params']['bookmakers'].split(',')) <= 10
        return PaidResponse()
    monkeypatch.setattr(m.requests, 'get', request)
    monkeypatch.setattr(m, 'utcnow', lambda: NOW)
    persisted = []
    monkeypatch.setattr(m, 'persist_rows', lambda db,event,payload,at: persisted.append((event,at)) or 2)
    class DB:
        def execute(self, *args): pass
    result = m.paid_capture(DB(), 'secret', EVENT, cutoff, tmp_path)
    assert result['rows'] == 2 and result['verified_closing_line'] is False
    assert persisted[0][0]['commence_time'] == cutoff.isoformat()
    assert 'secret' not in (tmp_path / PathName(result['raw_path'])).read_text()


def test_later_schedule_cannot_extend_an_original_cutoff(tmp_path, monkeypatch):
    setup_discovery(monkeypatch)
    earlier = NOW - timedelta(seconds=1)
    state = {"attempts": {}, "reserved_credits": 0,
             "games": {"odds1": {"cutoff": earlier.isoformat()}}}
    monkeypatch.setattr(m, "paid_capture", lambda *args: pytest.fail("extended earlier cutoff"))
    m.run_once(target=date(2026,10,28),root=tmp_path,state=state,api_key="key",
               apply=True,db=object(),max_credits=40,now=NOW)
    assert state['plan'][0]['cutoff'] == earlier.isoformat()
    assert not state['attempts']


def PathName(path):
    from pathlib import Path
    return Path(path).name
