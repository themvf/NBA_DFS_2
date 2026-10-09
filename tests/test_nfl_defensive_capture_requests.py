"""Upload-triggered opponent captures: eligibility, outcome mapping and verification."""
from datetime import datetime, timezone

from ingest.nfl_defensive_capture_requests import (
    MODEL_VERSIONS, PFR, VOLUME, claim, outcome_from_report, preflight_reason, process, verify_capture,
)

RUN = "11111111-1111-1111-1111-111111111111"
OTHER_RUN = "22222222-2222-2222-2222-222222222222"
UPLOAD = "33333333-3333-3333-3333-333333333333"
REQ = {"request_id": "r1", "upload_id": UPLOAD, "projection_run_id": RUN, "profile": PFR}
UP = {"upload_id": UPLOAD, "player_count": 50, "projection_run_id": RUN}
V5 = {"run_id": RUN, "model_version": "nfl-dfs-historical-v5", "season": 2026, "week": 4}


def test_preflight_passes_a_complete_pregame_upload():
    assert preflight_reason(REQ, UP, 50, V5, {"PIT", "CLE"}, {"PIT", "CLE", "NYJ"}) is None


def test_preflight_names_each_terminal_reason():
    assert preflight_reason(REQ, None, 0, V5, set(), set()) == "upload_missing"
    assert preflight_reason(REQ, UP, 49, V5, {"PIT"}, {"PIT"}).startswith("incomplete_upload")
    assert preflight_reason(REQ, {**UP, "projection_run_id": OTHER_RUN}, 50, V5, {"PIT"}, {"PIT"}) == "upload_bound_to_a_different_run"
    assert preflight_reason(REQ, UP, 50, None, {"PIT"}, {"PIT"}) == "projection_run_missing"
    assert preflight_reason(REQ, UP, 50, {**V5, "model_version": "nfl-dfs-historical-v4"}, {"PIT"}, {"PIT"}).startswith("unsupported_baseline")
    assert preflight_reason(REQ, UP, 50, V5, set(), {"PIT"}) == "upload_has_no_teams"


def test_preflight_fails_closed_once_any_game_has_started():
    reason = preflight_reason(REQ, UP, 50, V5, {"PIT", "CLE", "NYJ", "MIA"}, {"NYJ", "MIA"})
    assert reason == "slate_started_or_unscheduled (CLE, PIT)"


def _report(profile, detail=None, failed=None, uploads=True):
    return {"status": "captured", "failed": failed or [],
            "uploads": [{"upload_id": UPLOAD, **({profile: detail} if detail is not None else {})}] if uploads else []}


def test_outcome_requires_a_persisted_run():
    ok = outcome_from_report(PFR, _report(PFR, {"players": 40, "applied": 12, "persisted": {"run_id": "abc", "players": 38}}))
    assert ok == {"state": "verify", "capture_run_id": "abc", "players": 38}
    assert outcome_from_report(PFR, _report(PFR, {"players": 40, "persisted": None}))["state"] == "failed"
    assert outcome_from_report(PFR, _report(PFR))["state"] == "failed"
    assert outcome_from_report(PFR, _report(PFR, uploads=False))["error"].startswith("upload not selectable")


def test_outcome_reports_this_profiles_failure_only():
    report = _report(PFR, {"persisted": {"run_id": "abc", "players": 3}},
                     failed=[{"upload_id": UPLOAD, "profile": VOLUME, "error": "boom"}])
    assert outcome_from_report(PFR, report)["state"] == "verify", "the other profile's failure is not this one's"
    assert outcome_from_report(VOLUME, report) == {"state": "failed", "error": "boom"}


def test_volume_skip_is_ineligible_not_failed():
    out = outcome_from_report(VOLUME, _report(VOLUME, {"status": "skipped", "reason": "slate_partially_started"}))
    assert out == {"state": "ineligible", "error": "slate_partially_started"}
    ok = outcome_from_report(VOLUME, _report(VOLUME, {"status": "captured", "persisted": {"run_id": "v", "players": 9}}))
    assert ok["state"] == "verify"


class FakeDB:
    def __init__(self, row):
        self.row = row
        self.updates = []

    def execute_one(self, sql, params=None):
        return self.row

    def execute(self, sql, params=None):
        self.updates.append((sql, params))
        return []


def test_claim_clears_a_dispatch_error_left_by_an_earlier_attempt():
    db = FakeDB(None)
    claim(db, datetime(2026, 10, 4, 22, 8, tzinfo=timezone.utc), "https://github.com/run/1")
    (sql, _), = db.updates
    assert "dispatch_error=NULL" in " ".join(sql.split())


def test_verify_checks_upload_run_model_and_rows():
    good = {"upload_id": UPLOAD, "baseline_run_id": RUN, "model_version": MODEL_VERSIONS[PFR], "players": 38}
    assert verify_capture(FakeDB(good), REQ, "abc", 38) is None
    assert verify_capture(FakeDB(None), REQ, "abc", 38) == "forecast run not found after capture"
    assert "different upload" in verify_capture(FakeDB({**good, "upload_id": "x"}), REQ, "abc", 38)
    assert "different projection run" in verify_capture(FakeDB({**good, "baseline_run_id": OTHER_RUN}), REQ, "abc", 38)
    assert "model" in verify_capture(FakeDB({**good, "model_version": MODEL_VERSIONS[VOLUME]}), REQ, "abc", 38)
    assert "expected 38" in verify_capture(FakeDB({**good, "players": 37}), REQ, "abc", 38)


def test_process_never_raises_and_marks_the_request_failed(monkeypatch):
    import ingest.nfl_defensive_capture_requests as worker

    def broken(*args, **kwargs):
        raise RuntimeError("database went away")
    monkeypatch.setattr(worker, "_upload_context", broken)
    db = FakeDB(None)
    out = process(db, dict(REQ), fitted={}, output_dir=None)
    assert out["state"] == "failed" and "database went away" in out["error"]
    assert any("SET state=%s" in sql and params[0] == "failed" for sql, params in db.updates)


def test_process_ends_ineligible_without_capturing(monkeypatch):
    import ingest.nfl_defensive_capture_requests as worker
    monkeypatch.setattr(worker, "_upload_context", lambda db, r, now: (UP, 50, V5, {"PIT"}, set()))
    called = []
    import research.nfl_matchup_implementation as impl
    monkeypatch.setattr(impl, "capture_week", lambda *a, **k: called.append(1))
    db = FakeDB(None)
    out = process(db, dict(REQ), fitted={}, output_dir=None)
    assert out["state"] == "ineligible" and called == []


def test_web_schema_mirrors_the_python_ddl():
    """The web app creates the table on first upload; both definitions must agree."""
    import re
    from pathlib import Path
    from db.schema import NFL_DEFENSIVE_CAPTURE_REQUESTS_DDL
    norm = lambda text: re.sub(r"\s+", " ", text).strip()
    web = Path(__file__).resolve().parents[1] / "web" / "src" / "db" / "ensure-schema.ts"
    assert norm(NFL_DEFENSIVE_CAPTURE_REQUESTS_DDL) in norm(web.read_text(encoding="utf-8"))
