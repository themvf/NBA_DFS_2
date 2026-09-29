from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from research import cfb_pilot_operations
from research.cfb_pilot_operations import _expected_slots, _moneyline_settlement


def test_expected_slots_match_committed_workflow_cadence():
    start = datetime(2026, 9, 25, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 9, 25, 1, 0, tzinfo=timezone.utc)
    assert _expected_slots("capture_event_closes.yml", start, end) == 12
    assert _expected_slots("refresh_cfb_terminal.yml", start, end) == 4


def test_moneyline_evidence_counts_zero_ratio_and_legacy_market() -> None:
    day = datetime(2026, 9, 26, tzinfo=timezone.utc).date()
    base = {
        "alert_type": "dk_value", "signal_version": "cfb-lines-v1",
        "game_date": day, "settled_at": datetime(2026, 9, 26, tzinfo=timezone.utc),
        "details_json": {"exec_book": "draftkings"}, "close_history_id": 42,
        "grading_json": {"settlement_rule_status": "UNVERIFIED_LEGACY_QUOTES"},
        "result_state": "settled", "metrics": {"decimal_price_ratio_pct": 0.0},
    }
    rows = [base, {**base, "details_json": {"market": "spread"}},
            {**base, "settled_at": None, "metrics": {}}]
    result = _moneyline_settlement(
        rows, [{"alert_type": "dk_value", "signal_version": "cfb-lines-v1"}],
    )
    assert result["frozen_candidate_signals"] == 2
    assert result["settled_signals"] == 1
    assert result["settled_with_primary_metric"] == 1
    assert result["settled_with_unverified_rule"] == 1


def test_pilot_report_retries_schema_deadlock_with_new_attempt(monkeypatch) -> None:
    attempts = []
    def run_once(*_):
        attempts.append(1)
        if len(attempts) < 3:
            raise psycopg2.errors.DeadlockDetected("schema lock")
        return {"status": "incomplete"}
    monkeypatch.setattr(cfb_pilot_operations, "_build_once", run_once)
    monkeypatch.setattr(cfb_pilot_operations.time, "sleep", lambda *_: None)
    assert cfb_pilot_operations.build("test") == {"status": "incomplete"}
    assert len(attempts) == 3


# --- Run-gap check: "nothing was due" is not "captures are missing" -------
# 2026-09-29: the monitor failed every run between CFB game weeks because the
# closing-line workflow (dispatched only on due work) had not run for 20 min.

from research.cfb_pilot_operations import _cfb_checkpoint_windows, _run_gap_check

UTC = timezone.utc
CUTOFF = datetime(2026, 9, 29, 7, 0, tzinfo=UTC)
LAST = datetime(2026, 9, 29, 6, 1, tzinfo=UTC)
START = datetime(2026, 9, 25, tzinfo=UTC)


def test_long_gap_with_no_game_week_is_idle_not_alert() -> None:
    # Next kickoff 2026-10-02 00:00: its first (48h) window opens 09-30 00:00.
    windows = _cfb_checkpoint_windows([(332, datetime(2026, 10, 2, 0, 0, tzinfo=UTC))])
    check = _run_gap_check(LAST, CUTOFF, 20, start=START, windows=windows, captures={})
    assert check["status"] == "idle"
    assert check["age_minutes"] == 59.0
    assert "no CFB capture checkpoint was due" in check["reason"]


def test_long_gap_while_a_checkpoint_is_owed_alerts() -> None:
    # Kickoff at 09:00: the 15-minute cadence (T-345m onward) is open by 06:15.
    windows = _cfb_checkpoint_windows([(900, datetime(2026, 9, 29, 9, 0, tzinfo=UTC))])
    check = _run_gap_check(LAST, CUTOFF, 20, start=START, windows=windows, captures={})
    assert check["status"] == "alert"
    assert "matchup 900" in check["reason"]


def test_a_window_captured_by_another_path_is_not_owed() -> None:
    kickoff = datetime(2026, 9, 29, 9, 0, tzinfo=UTC)
    windows = [w for w in _cfb_checkpoint_windows([(900, kickoff)])
               if w["target_at"] <= CUTOFF - timedelta(minutes=20) and w["due_until"] >= LAST]
    captures = {900: [w["target_at"] + timedelta(minutes=1) for w in windows]}
    check = _run_gap_check(LAST, CUTOFF, 20, start=START, windows=windows, captures=captures)
    assert check["status"] == "idle"


def test_a_window_that_opened_moments_ago_is_not_yet_an_alert() -> None:
    # Only windows open for at least the threshold count; the dispatcher polls.
    kickoff = CUTOFF + timedelta(hours=6)  # T-6h window opens exactly now
    windows = [w for w in _cfb_checkpoint_windows([(901, kickoff)]) if w["checkpoint"] == "t_minus_6h"]
    check = _run_gap_check(LAST, CUTOFF, 20, start=START, windows=windows, captures={})
    assert check["status"] == "idle"


def test_recent_success_passes_and_fixed_cron_workflows_still_alert() -> None:
    assert _run_gap_check(CUTOFF - timedelta(minutes=5), CUTOFF, 20, start=START, windows=[])["status"] == "pass"
    # refresh_cfb_terminal.yml has no due-work input: an over-threshold gap alerts as before.
    assert _run_gap_check(CUTOFF - timedelta(minutes=130), CUTOFF, 120, start=START)["status"] == "alert"


def test_pilot_report_fails_after_bounded_retries(monkeypatch) -> None:
    def always_deadlocks(*_):
        raise psycopg2.errors.DeadlockDetected("schema lock")
    monkeypatch.setattr(cfb_pilot_operations, "_build_once", always_deadlocks)
    monkeypatch.setattr(cfb_pilot_operations.time, "sleep", lambda *_: None)
    with pytest.raises(psycopg2.errors.DeadlockDetected):
        cfb_pilot_operations.build("test")
