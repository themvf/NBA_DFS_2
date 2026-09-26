from datetime import datetime, timezone

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


def test_pilot_report_fails_after_bounded_retries(monkeypatch) -> None:
    def always_deadlocks(*_):
        raise psycopg2.errors.DeadlockDetected("schema lock")
    monkeypatch.setattr(cfb_pilot_operations, "_build_once", always_deadlocks)
    monkeypatch.setattr(cfb_pilot_operations.time, "sleep", lambda *_: None)
    with pytest.raises(psycopg2.errors.DeadlockDetected):
        cfb_pilot_operations.build("test")
