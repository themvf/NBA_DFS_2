from datetime import datetime, timezone

from research.cfb_moneyline_audit import _decimal, _entry_band, _study_registration


def test_american_decimal_conversion_and_entry_bands():
    assert _decimal(-200) == 1.5
    assert _decimal(400) == 5.0
    assert _entry_band(1.5) == "favorite_60_plus"
    assert _entry_band(5.0) == "longshot_20_35"
    assert _entry_band(None) == "unknown"


def test_registration_freezes_future_windows_and_decision_denial():
    frozen = datetime(2026, 9, 23, 15, tzinfo=timezone.utc)
    registration = _study_registration(frozen)
    assert registration["consumer_permission"] == "decision-denied"
    assert all(window["start_at"] > frozen.isoformat() for window in registration["windows"])
    assert len(registration["configuration_digest"]) == 64
