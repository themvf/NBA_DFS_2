from __future__ import annotations

from datetime import datetime, timezone

from ingest import event_closing_lines as closes
from ingest import mlb_schedule, tennis_schedule
from model import clv_report


def test_close_quality_boundaries() -> None:
    assert closes.close_quality(0) == "A"
    assert closes.close_quality(300) == "A"
    assert closes.close_quality(301) == "B"
    assert closes.close_quality(900) == "B"
    assert closes.close_quality(901) == "C"
    assert closes.close_quality(1800) == "C"
    assert closes.close_quality(1801) == "stale"


def test_cfb_uses_early_and_late_market_checkpoints() -> None:
    checkpoints = closes.CHECKPOINTS_BY_SPORT["cfb"]
    assert ("t_minus_48h", 2880, 2520) in checkpoints
    assert ("t_minus_24h", 1440, 1200) in checkpoints
    assert ("t_minus_6h", 360, 330) in checkpoints
    assert ("t_minus_90m", 90, 60) in checkpoints
    assert ("t_minus_15m", 15, 5) in checkpoints
    assert ("t_minus_2m", 2, 0) in checkpoints


def test_nfl_calendar_cadence_for_sunday_early_game() -> None:
    kickoff = datetime(2026, 9, 13, 17, tzinfo=timezone.utc)  # 1:00 PM ET
    jobs = closes.nfl_checkpoint_schedule(kickoff)
    keyed = {job["checkpoint"]: job for job in jobs}

    assert len(jobs) == 32
    assert {f"d_minus_3_{hour:02d}" for hour in (0, 6, 12, 18)} <= set(keyed)
    assert {f"d_minus_2_{hour:02d}" for hour in (0, 6, 12, 18)} <= set(keyed)
    assert {f"d_minus_1_{hour:02d}" for hour in range(0, 24, 3)} <= set(keyed)
    assert {f"game_day_{hour:02d}" for hour in range(13)} <= set(keyed)
    assert keyed["game_day_12"]["target_at"] == datetime(2026, 9, 13, 16, tzinfo=timezone.utc)
    assert keyed["t_minus_30m"]["target_at"] == datetime(2026, 9, 13, 16, 30, tzinfo=timezone.utc)
    assert keyed["t_minus_15m"]["due_until"] == datetime(2026, 9, 13, 16, 55, tzinfo=timezone.utc)
    assert keyed["closing_candidate"]["due_until"] == kickoff


def test_nfl_calendar_cadence_respects_dst_offset() -> None:
    # DST ends on this Sunday: midnight is EDT while the 1 PM game is EST.
    jobs = closes.nfl_checkpoint_schedule(datetime(2026, 11, 1, 18, tzinfo=timezone.utc))
    keyed = {job["checkpoint"]: job for job in jobs}
    assert keyed["game_day_00"]["target_at"] == datetime(2026, 11, 1, 4, tzinfo=timezone.utc)
    assert keyed["game_day_12"]["target_at"] == datetime(2026, 11, 1, 17, tzinfo=timezone.utc)


def test_verified_cohort_boundary_is_machine_readable() -> None:
    assert closes.VERIFIED_CLV_START_AT == datetime(2026, 8, 31, 4, tzinfo=timezone.utc)
    actual = closes.classify_clv_cohort(
        scheduled_start=datetime(2026, 8, 31, 4, tzinfo=timezone.utc),
        quality="A", boundary_source="mlb_first_pitch",
    )
    assert actual["primary_clv_eligible"] is True
    assert actual["clv_cohort"] == "verified_clv_v1"
    assert actual["verification_level"] == "actual_start"


def test_stale_and_pre_boundary_closes_are_non_primary() -> None:
    stale = closes.classify_clv_cohort(
        scheduled_start=datetime(2026, 9, 1, tzinfo=timezone.utc),
        quality="stale", boundary_source="scheduled_provider",
    )
    historical = closes.classify_clv_cohort(
        scheduled_start=datetime(2026, 8, 31, 3, 59, 59, tzinfo=timezone.utc),
        quality="A", boundary_source="mlb_first_play",
    )
    assert stale["primary_clv_eligible"] is False
    assert stale["verification_level"] == "scheduled_boundary"
    assert historical["primary_clv_eligible"] is False


def test_parse_mlb_actual_start_prefers_first_pitch() -> None:
    payload = {
        "gameData": {
            "status": {"abstractGameState": "Live", "detailedState": "In Progress"},
            "datetime": {"firstPitch": "2026-08-31T23:07:12Z"},
        },
        "liveData": {"plays": {"allPlays": [{"about": {"startTime": "2026-08-31T23:08:00Z"}}]}},
    }
    boundary, source, evidence = closes.parse_mlb_actual_start(payload)
    assert boundary == datetime(2026, 8, 31, 23, 7, 12, tzinfo=timezone.utc)
    assert source == "mlb_first_pitch"
    assert evidence["abstract_state"] == "Live"


def test_parse_mlb_actual_start_falls_back_to_first_play() -> None:
    payload = {
        "gameData": {"status": {"abstractGameState": "Final"}},
        "liveData": {"plays": {"allPlays": [{"about": {"startTime": "2026-08-31T20:01:00Z"}}]}},
    }
    boundary, source, _ = closes.parse_mlb_actual_start(payload)
    assert boundary == datetime(2026, 8, 31, 20, 1, tzinfo=timezone.utc)
    assert source == "mlb_first_play"


def test_parse_mlb_preview_does_not_invent_boundary() -> None:
    boundary, source, evidence = closes.parse_mlb_actual_start({
        "gameData": {"status": {"abstractGameState": "Preview", "detailedState": "Delayed"}}
    })
    assert boundary is None
    assert source is None
    assert evidence["detailed_state"] == "Delayed"


def test_book_updates_must_be_before_boundary() -> None:
    boundary = datetime(2026, 8, 31, 20, tzinfo=timezone.utc)
    books = {
        "draftkings": {"last_update": "2026-08-31T19:59:00Z"},
        "fanduel": {"last_update": "2026-08-31T20:00:01Z"},
        "polymarket": {"last_update": "2026-08-31T19:58:00Z"},
    }
    assert closes._eligible_book_updates(books, boundary) == ["draftkings"]


class EmptyDb:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if "SELECT c.*" in sql:
            return []
        return []

    def execute_one(self, sql, params=None):
        self.calls.append((sql, params))
        return None


def test_no_due_checkpoint_makes_no_paid_request(monkeypatch) -> None:
    db = EmptyDb()
    monkeypatch.setattr(
        closes, "fetch_mlb_odds",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not call provider")),
    )
    monkeypatch.setattr(
        closes, "discover_tournaments",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not discover")),
    )
    result = closes.capture_due_checkpoints(
        db, "key", now=datetime(2026, 8, 31, 12, tzinfo=timezone.utc),
    )
    assert result["due"] == 0
    assert result["paid_requests"] == 0


def test_reconcile_supersedes_old_nfl_kickoff_jobs() -> None:
    db = EmptyDb()
    closes.reconcile_checkpoints(
        db, now=datetime(2026, 9, 10, 12, tzinfo=timezone.utc),
    )
    supersede_sql = db.calls[0][0]
    assert "superseded by kickoff reschedule" in supersede_sql
    assert "c.scheduled_start_at IS DISTINCT FROM m.commence_time" in supersede_sql
    assert "c.sport='nfl'" in supersede_sql


def test_cfb_due_games_share_one_paid_bulk_capture(monkeypatch) -> None:
    jobs = [
        {"id": 1, "sport": "cfb", "event_id": "a", "scheduled_start_at": "2026-09-05T16:00:00Z"},
        {"id": 2, "sport": "cfb", "event_id": "b", "scheduled_start_at": "2026-09-05T16:00:00Z"},
    ]
    db = EmptyDb()
    observed = {}
    monkeypatch.setattr(closes, "seed_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "reconcile_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "due_checkpoints", lambda *_args: jobs)
    monkeypatch.setattr(closes, "quota_allows", lambda *_args: (True, None))
    monkeypatch.setattr(closes, "_audit_usage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(closes, "_mark_attempt", lambda *_args: None)
    monkeypatch.setattr(closes, "_mark_failure", lambda *_args: None)

    def fake_fetch(_db, _key, *, event_ids, refresh_events, request_audit):
        observed.update(event_ids=event_ids, refresh_events=refresh_events)
        request_audit["requests_last"] = "3"
        return 2

    monkeypatch.setattr(closes, "fetch_cfb_odds", fake_fetch)
    result = closes.capture_due_checkpoints(
        db, "key", now=datetime(2026, 9, 5, 10, tzinfo=timezone.utc),
    )
    assert observed == {"event_ids": {"a", "b"}, "refresh_events": False}
    assert result["paid_requests"] == 1
    assert result["groups"] == 1


def test_nfl_due_games_share_one_targeted_bulk_capture(monkeypatch) -> None:
    jobs = [
        {"id": 11, "sport": "nfl", "event_id": "a", "season_type": "regular",
         "scheduled_start_at": "2026-09-13T17:00:00Z"},
        {"id": 12, "sport": "nfl", "event_id": "b", "season_type": "regular",
         "scheduled_start_at": "2026-09-13T17:00:00Z"},
    ]
    db = EmptyDb()
    observed = {}
    monkeypatch.setattr(closes, "seed_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "reconcile_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "due_checkpoints", lambda *_args: jobs)
    monkeypatch.setattr(closes, "quota_allows", lambda *_args: (True, None))
    monkeypatch.setattr(closes, "_audit_usage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(closes, "_mark_attempt", lambda *_args: None)
    monkeypatch.setattr(closes, "_mark_failure", lambda *_args: None)

    def fake_fetch(_db, _key, **kwargs):
        observed.update(kwargs)
        kwargs["request_audit"].update({"request_count": 1, "requests_last": 3})
        return 2

    monkeypatch.setattr(closes, "fetch_nfl_odds", fake_fetch)
    result = closes.capture_due_checkpoints(
        db, "key", now=datetime(2026, 9, 13, 12, tzinfo=timezone.utc),
    )
    assert observed["event_ids"] == {"a", "b"}
    assert observed["refresh_events"] is False
    assert observed["bookmakers"] == closes.BOOKMAKERS
    assert result["paid_requests"] == 1
    assert result["groups"] == 1


class QuotaDb:
    def __init__(self, credits_today: int, remaining: int | None) -> None:
        self.row = {"credits_today": credits_today, "remaining": remaining}

    def execute_one(self, _sql, _params=None):
        return self.row


def test_quota_guard_enforces_daily_cap(monkeypatch) -> None:
    monkeypatch.setattr(closes, "DAILY_CREDIT_CAP", 120)
    allowed, reason = closes.quota_allows(QuotaDb(118, 1000))
    assert allowed is False
    assert "daily" in str(reason)


def test_quota_guard_preserves_monthly_reserve(monkeypatch) -> None:
    monkeypatch.setattr(closes, "MIN_REMAINING_RESERVE", 250)
    allowed, reason = closes.quota_allows(QuotaDb(0, 252))
    assert allowed is False
    assert "reserve" in str(reason)


class RequestDb:
    def execute_one(self, _sql, _params=None):
        return {"scheduled": 1, "upcoming": 1}

    def execute(self, _sql, _params=None):
        return []


class EmptyOddsResponse:
    status_code = 200
    headers = {
        "x-requests-last": "3",
        "x-requests-used": "100",
        "x-requests-remaining": "19900",
    }
    url = "https://api.the-odds-api.com/v4/test?apiKey=hidden"

    def raise_for_status(self):
        return None

    def json(self):
        return []


def test_mlb_targeted_request_uses_event_ids_and_one_book_group(monkeypatch) -> None:
    observed = {}

    def fake_get(_url, *, params, timeout):
        observed.update(params)
        assert timeout == 20
        return EmptyOddsResponse()

    monkeypatch.setattr(mlb_schedule.requests, "get", fake_get)
    audit = {}
    mlb_schedule.fetch_odds(
        RequestDb(), "key", "2026-08-31", event_ids=["b", "a"],
        bookmakers=closes.BOOKMAKERS, request_audit=audit,
    )
    assert observed["eventIds"] == "a,b"
    assert observed["bookmakers"] == closes.BOOKMAKERS
    assert "regions" not in observed
    assert audit["requests_last"] == "3"


def test_tennis_targeted_request_uses_event_ids_and_one_book_group(monkeypatch) -> None:
    observed = {}

    def fake_get(_url, *, params, timeout):
        observed.update(params)
        assert timeout == 20
        return EmptyOddsResponse()

    monkeypatch.setattr(tennis_schedule.requests, "get", fake_get)
    audit = {}
    tennis_schedule.fetch_tournament(
        RequestDb(), "key", "ATP", "tennis_atp_us_open", "ATP US Open", None,
        event_ids=["two", "one"], bookmakers=closes.BOOKMAKERS,
        request_audit=audit,
    )
    assert observed["eventIds"] == "one,two"
    assert observed["bookmakers"] == closes.BOOKMAKERS
    assert "regions" not in observed
    assert audit["requests_remaining"] == "19900"


class QueryCaptureDb:
    def __init__(self) -> None:
        self.sql = ""

    def execute(self, sql, _params=None):
        self.sql = sql
        return []


def test_clv_report_defaults_to_verified_view() -> None:
    db = QueryCaptureDb()
    assert clv_report._collect(db, "tennis", None) == []
    assert "JOIN verified_clv_closes" in db.sql
    assert "LEFT JOIN verified_clv_closes" not in db.sql


def test_clv_report_requires_explicit_legacy_opt_in() -> None:
    db = QueryCaptureDb()
    assert clv_report._collect(db, "mlb", None, include_legacy=True) == []
    assert "LEFT JOIN event_closing_lines" in db.sql
