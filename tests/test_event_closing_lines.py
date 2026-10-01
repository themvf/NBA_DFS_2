from __future__ import annotations

from datetime import date, datetime, timezone

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
    assert ("cfb_t_minus_7d", 10080, 9720) in checkpoints
    assert ("cfb_t_minus_4d", 5760, 5400) in checkpoints
    assert ("t_minus_48h", 2880, 2520) in checkpoints
    assert ("t_minus_24h", 1440, 1200) in checkpoints
    assert ("t_minus_6h", 360, 330) in checkpoints
    assert ("t_minus_90m", 90, 60) in checkpoints
    assert ("t_minus_15m", 15, 5) in checkpoints
    assert ("t_minus_2m", 2, 0) in checkpoints
    assert ("closing_candidate", 5, 0) in checkpoints
    assert ("cfb_t_minus_720m", 720, 660) in checkpoints
    targets = sorted({target for _, target, _ in checkpoints if target <= 360})
    assert max(b - a for a, b in zip(targets, targets[1:])) <= 15
    assert len({name for name, _, _ in checkpoints}) == len(checkpoints)
    assert all(target > due >= 0 for _, target, due in checkpoints)


def test_cfb_early_pilot_is_limited_to_two_future_slates() -> None:
    calls: list[tuple[str, tuple]] = []

    class Db:
        def execute(self, sql, params=None):
            calls.append((sql, params))
            return []

    closes.seed_checkpoints(Db(), datetime(2026, 10, 1, tzinfo=timezone.utc))
    cfb_sql, params = next((sql, params) for sql, params in calls if params[0] == "cfb")
    assert "INTERVAL '8 days'" in cfb_sql
    assert "e.game_date BETWEEN %s AND %s" in cfb_sql
    assert params[-2:] == (closes.CFB_EARLY_PILOT_FIRST_GAME, closes.CFB_EARLY_PILOT_LAST_GAME)
    assert params[-2:] == (date(2026, 10, 8), date(2026, 10, 17))


def test_cfb_early_pilot_does_not_repeat_paid_attempts() -> None:
    class Db:
        def execute(self, sql, params=None):
            assert "c.checkpoint NOT IN ('cfb_t_minus_7d', 'cfb_t_minus_4d')" in sql
            assert "OR c.attempted_at IS NULL" in sql
            return []

    assert closes.due_checkpoints(Db(), datetime(2026, 10, 3, tzinfo=timezone.utc)) == []


def test_cfb_early_pilot_checkpoint_names_are_allowed_by_schema() -> None:
    from db.schema import CLOSE_CAPTURE_CONSTRAINT_DDLS, INDEXES, MIGRATIONS, TABLES

    definitions = "\n".join((*CLOSE_CAPTURE_CONSTRAINT_DDLS, *INDEXES, *MIGRATIONS, *TABLES))
    for name, _, _ in closes.CFB_EARLY_PILOT_CHECKPOINTS:
        assert definitions.count(f"'{name}'") >= 2


def test_tennis_dense_cadence_preserves_legacy_and_covers_final_thirty_minutes():
    checkpoints = closes.CHECKPOINTS_BY_SPORT["tennis"]
    assert set(closes.CORE_CHECKPOINTS) <= set(checkpoints)
    assert len({name for name, _, _ in checkpoints}) == len(checkpoints)
    assert all(target > due >= 0 for _, target, due in checkpoints)
    for lead in range(30, 0, -5):
        assert (f"tennis_t_minus_{lead}m", lead, lead-5) in checkpoints
    for low, high, interval in ((90,360,30),(30,90,15),(0,30,5)):
        targets = sorted({low, high} | {target for _,target,_ in checkpoints if low <= target <= high})
        assert max(b-a for a,b in zip(targets,targets[1:])) <= interval


def test_nfl_calendar_cadence_for_sunday_early_game() -> None:
    kickoff = datetime(2026, 9, 13, 17, tzinfo=timezone.utc)  # 1:00 PM ET
    jobs = closes.nfl_checkpoint_schedule(kickoff)
    keyed = {job["checkpoint"]: job for job in jobs}

    assert len(jobs) == 118
    for day in range(1, 8):
        assert keyed[f"d_minus_{day}_08"]["target_at"].astimezone(closes.EASTERN).hour == 8
    assert keyed["game_day_08"]["target_at"] == datetime(2026, 9, 13, 12, tzinfo=timezone.utc)
    for lead in range(120, 0, -5):
        job = keyed[f"nfl_t_minus_{lead}m"]
        assert (kickoff - job["target_at"]).total_seconds() == lead * 60
        assert (job["due_until"] - job["target_at"]).total_seconds() == 300
    assert "d_minus_7_00" in keyed
    assert {f"d_minus_3_{hour:02d}" for hour in range(0, 24, 3)} <= set(keyed)
    assert {f"d_minus_2_{hour:02d}" for hour in range(0, 24, 3)} <= set(keyed)
    assert {f"d_minus_1_{hour:02d}" for hour in range(24)} <= set(keyed)
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
    assert keyed["game_day_08"]["target_at"] == datetime(2026, 11, 1, 13, tzinfo=timezone.utc)
    assert keyed["t_minus_30m"]["target_at"] == datetime(2026, 11, 1, 17, 30, tzinfo=timezone.utc)


def test_every_nfl_checkpoint_is_allowed_by_schema_migration():
    import re
    from db.schema import INDEXES
    ddl = next(sql for sql in INDEXES if sql.startswith(
        "ALTER TABLE odds_capture_checkpoints ADD CONSTRAINT odds_capture_checkpoints_checkpoint_check"))
    patterns = re.findall(r"checkpoint ~ '([^']+)'", ddl)
    for job in closes.nfl_checkpoint_schedule(datetime(2026, 9, 13, 17, tzinfo=timezone.utc)):
        name = job["checkpoint"]
        assert f"'{name}'" in ddl or any(re.fullmatch(pattern, name) for pattern in patterns), name


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
    assert "c.sport='cfb'" in db.calls[1][0]
    assert "c.scheduled_start_at IS DISTINCT FROM m.commence_time" in db.calls[1][0]
    assert "c.sport='tennis'" in db.calls[2][0]
    assert "c.scheduled_start_at IS DISTINCT FROM m.commence_time" in db.calls[2][0]
    assert "c.status IN ('pending', 'attempted', 'failed')" in db.calls[2][0]


def test_cfb_due_games_share_one_paid_bulk_capture(monkeypatch) -> None:
    jobs = [
        {"id": 1, "sport": "cfb", "event_id": "a", "checkpoint": "cfb_t_minus_7d",
         "scheduled_start_at": "2026-10-10T16:00:00Z"},
        {"id": 2, "sport": "cfb", "event_id": "b", "checkpoint": "t_minus_48h",
         "scheduled_start_at": "2026-10-03T16:00:00Z"},
    ]
    db = EmptyDb()
    observed = {}
    monkeypatch.setattr(closes, "seed_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "reconcile_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "due_checkpoints", lambda *_args: jobs)
    monkeypatch.setattr(closes, "quota_allows", lambda *_args: (True, None))
    monkeypatch.setattr(closes, "_audit_usage", lambda *_args, **kwargs: observed.update(metadata=kwargs["metadata"]))
    monkeypatch.setattr(closes, "_mark_attempt", lambda *_args: None)
    monkeypatch.setattr(closes, "_mark_failure", lambda *_args: None)

    def fake_fetch(_db, _key, *, event_ids, refresh_events, request_audit):
        observed.update(event_ids=event_ids, refresh_events=refresh_events)
        request_audit["requests_last"] = "3"
        return 2

    monkeypatch.setattr(closes, "fetch_cfb_odds", fake_fetch)
    result = closes.capture_due_checkpoints(
        db, "key", now=datetime(2026, 10, 3, 16, tzinfo=timezone.utc),
    )
    assert observed["event_ids"] == {"a", "b"}
    assert observed["refresh_events"] is False
    assert observed["metadata"]["early_pilot_checkpoints"] == ["cfb_t_minus_7d"]
    assert observed["metadata"]["cadence_version"] == "cfb-early-pilot-v1"
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


def test_discovery_probe_is_bounded_and_skips_captured_or_distant_events():
    from datetime import timedelta
    now = datetime(2026, 9, 6, 15, tzinfo=timezone.utc)
    class Db:
        inserted = []
        def execute(self, sql, params=()):
            if "SELECT id AS matchup_id" in sql:
                return [dict(matchup_id=i, event_id=str(i), scheduled_start_at=now + timedelta(days=days), has_capture=captured)
                        for i, days, captured in [(1, 2, False), (2, 2, True), (3, 8, False), (4, -0.1, False)]]
            self.inserted = list(zip(*[iter(params)] * 7))
            assert "ON CONFLICT" in sql
            return []
    db = Db()
    closes._seed_nfl_checkpoints(db, now)
    probes = [r for r in db.inserted if r[3] == "nfl_first_observed"]
    assert len(probes) == 1 and probes[0][1] == 1
    assert probes[0][5] == now and probes[0][6] == now + timedelta(minutes=20)
    class DueDb:
        def execute(self, sql, params=()):
            assert "c.checkpoint <> 'nfl_first_observed' OR c.attempted_at IS NULL" in sql
            return []
    assert closes.due_checkpoints(DueDb(), now) == []
    from db.schema import INDEXES
    ddl = next(sql for sql in INDEXES if sql.startswith("ALTER TABLE odds_capture_checkpoints ADD CONSTRAINT odds_capture_checkpoints_checkpoint_check"))
    assert "checkpoint = 'nfl_first_observed'" in ddl


def test_nhl_cadence_covers_goalie_news_through_close() -> None:
    checkpoints = closes.CHECKPOINTS_BY_SPORT["nhl"]
    assert ("t_minus_6h", 360, 330) in checkpoints  # morning skate
    assert ("nhl_t_minus_45m", 45, 35) in checkpoints  # goalie confirmation
    assert ("closing_candidate", 5, 0) in checkpoints
    assert len({name for name, _, _ in checkpoints}) == len(checkpoints)
    assert all(target > due >= 0 for _, target, due in checkpoints)
    windows = sorted((due, target) for _, target, due in checkpoints if target <= 180)
    # No gap wider than 20 minutes in the final three hours.
    assert max(later[0] - earlier[1] for earlier, later in zip(windows, windows[1:])) <= 20


def _nhl_capture_setup(monkeypatch, fetch):
    jobs = [
        {"id": 21, "sport": "nhl", "event_id": "a", "scheduled_start_at": "2026-09-29T23:00:00Z"},
        {"id": 22, "sport": "nhl", "event_id": "b", "scheduled_start_at": "2026-09-30T00:00:00Z"},
        {"id": 23, "sport": "nfl", "event_id": "n", "season_type": "regular",
         "scheduled_start_at": "2026-10-04T17:00:00Z"},
    ]
    failures: list[tuple[list[int], str]] = []
    audits: list[dict] = []
    nfl_calls: list = []
    monkeypatch.setattr(closes, "seed_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "reconcile_checkpoints", lambda *_args: 0)
    monkeypatch.setattr(closes, "due_checkpoints", lambda *_args: jobs)
    monkeypatch.setattr(closes, "quota_allows", lambda *_args: (True, None))
    monkeypatch.setattr(closes, "_audit_usage", lambda *_args, **kwargs: audits.append(kwargs))
    monkeypatch.setattr(closes, "_mark_attempt", lambda *_args: None)
    monkeypatch.setattr(closes, "_mark_failure", lambda _db, jobs, reason: failures.append(([j["id"] for j in jobs], reason)))
    monkeypatch.setattr(closes, "fetch_nfl_odds", lambda *_a, **k: nfl_calls.append(k) or
                        k["request_audit"].update({"request_count": 1}) or 1)
    monkeypatch.setattr(closes, "fetch_nhl_odds", fetch)
    result = closes.capture_due_checkpoints(EmptyDb(), "key", now=datetime(2026, 9, 29, 17, tzinfo=timezone.utc))
    return result, failures, audits, nfl_calls


def test_nhl_due_games_share_one_bulk_capture(monkeypatch) -> None:
    observed = {}

    def fetch(_db, _key, *, event_ids, request_audit):
        observed["event_ids"] = event_ids
        request_audit.update({"endpoint": "odds", "status": 200, "request_count": 1, "requests_last": "3"})
        return 5

    result, failures, audits, nfl_calls = _nhl_capture_setup(monkeypatch, fetch)
    assert observed["event_ids"] == {"a", "b"}
    assert result["paid_requests"] == 2 and result["groups"] == 2  # one NFL, one NHL
    assert failures == []
    assert audits[-1]["sport"] == "nhl" and audits[-1]["metadata"]["cadence_version"] == "nhl-dense-v1"
    assert len(nfl_calls) == 1


def test_nhl_provider_failure_is_isolated_and_audited(monkeypatch) -> None:
    import requests

    def fetch(_db, _key, *, event_ids, request_audit):
        request_audit.update({"endpoint": "odds", "status": 500, "request_count": 1})
        raise requests.HTTPError("500 Server Error")

    result, failures, audits, nfl_calls = _nhl_capture_setup(monkeypatch, fetch)
    assert len(nfl_calls) == 1  # NFL, earlier in the run, is unaffected
    assert failures == [([21, 22], "provider request failed: 500 Server Error")]
    assert audits[-1]["sport"] == "nhl" and "error" in audits[-1]["metadata"]
    assert result["paid_requests"] == 2


def test_nhl_skipped_fetch_is_not_billed_or_misreported(monkeypatch) -> None:
    result, failures, audits, _ = _nhl_capture_setup(monkeypatch, lambda *_a, **_k: 0)
    assert result["paid_requests"] == 1  # the NFL call only
    assert [a["sport"] for a in audits] == ["nfl"]
    assert failures == [([21, 22], "no due game still mapped and upcoming; no request made")]


def test_reconcile_supersedes_rescheduled_or_postponed_nhl_jobs() -> None:
    db = EmptyDb()
    closes.reconcile_checkpoints(db, now=datetime(2026, 9, 29, 12, tzinfo=timezone.utc))
    nhl_sql = next(sql for sql, _ in db.calls if "c.sport='nhl'" in sql)
    assert "c.scheduled_start_at IS DISTINCT FROM m.commence_time" in nhl_sql
    assert "schedule_state" in nhl_sql


def test_nhl_close_uses_scheduled_boundary(monkeypatch) -> None:
    rows = [{"sport": "nhl", "matchup_id": 9, "official_game_id": None, "event_id": "evt",
             "scheduled_start_at": datetime(2026, 9, 29, 21, tzinfo=timezone.utc)}]

    class Db:
        def execute(self, sql, params=None):
            assert "FROM nhl_matchups m" in sql
            return rows
    frozen = {}
    monkeypatch.setattr(closes, "_freeze_one", lambda _db, **kwargs: frozen.update(kwargs) or True)
    result = closes.freeze_due_closes(Db(), now=datetime(2026, 9, 29, 21, 5, tzinfo=timezone.utc))
    assert result["frozen"] == 1
    assert frozen["boundary_source"] == "scheduled_nhl"
    assert frozen["boundary"] == datetime(2026, 9, 29, 21, tzinfo=timezone.utc)


def test_nhl_seed_failure_does_not_stop_other_sports(monkeypatch) -> None:
    seeded: list[str] = []

    class Db:
        def execute(self, sql, params=None):
            sport = params[0] if params else None
            if sport == "nhl":
                raise RuntimeError("new row violates check constraint")
            seeded.append(sport)
            return [{"id": 1}]
    monkeypatch.setattr(closes, "_seed_nfl_checkpoints", lambda *_args: 7)
    assert closes.seed_checkpoints(Db(), datetime(2026, 9, 29, tzinfo=timezone.utc)) == 3 + 7
    assert seeded == ["mlb", "tennis", "cfb"]


def test_nhl_freeze_failure_does_not_stop_other_sports(monkeypatch) -> None:
    rows = [
        {"sport": "nhl", "matchup_id": 9, "official_game_id": None, "event_id": "e",
         "scheduled_start_at": datetime(2026, 9, 29, 21, tzinfo=timezone.utc)},
        {"sport": "cfb", "matchup_id": 4, "official_game_id": None, "event_id": "f",
         "scheduled_start_at": datetime(2026, 9, 29, 21, tzinfo=timezone.utc)},
    ]

    class Db:
        def execute(self, sql, params=None): return rows

    def freeze(_db, **kwargs):
        if kwargs["sport"] == "nhl":
            raise RuntimeError("constraint")
        return True
    monkeypatch.setattr(closes, "_freeze_one", freeze)
    assert closes.freeze_due_closes(Db(), now=datetime(2026, 9, 29, 21, 5, tzinfo=timezone.utc))["frozen"] == 1
