from datetime import datetime, timezone

from research.cfb_pilot_operations import _expected_slots


def test_expected_slots_match_committed_workflow_cadence():
    start = datetime(2026, 9, 25, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 9, 25, 1, 0, tzinfo=timezone.utc)
    assert _expected_slots("capture_event_closes.yml", start, end) == 12
    assert _expected_slots("refresh_cfb_terminal.yml", start, end) == 4
