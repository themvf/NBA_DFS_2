from datetime import datetime, timedelta, timezone

import pytest

from ingest.cfb_context_bootstrap import PILOT_START, capture_origin


def test_auto_origin_respects_frozen_pilot_boundary():
    assert capture_origin(PILOT_START - timedelta(microseconds=1), "auto") == "legacy"
    assert capture_origin(PILOT_START, "auto") == "prospective"
    assert capture_origin(PILOT_START + timedelta(minutes=1), "auto") == "prospective"


def test_auto_origin_refuses_time_without_timezone():
    with pytest.raises(ValueError):
        capture_origin(datetime(2026, 9, 25), "auto")
    assert capture_origin(datetime(2026, 9, 25, tzinfo=timezone.utc), "legacy") == "legacy"
