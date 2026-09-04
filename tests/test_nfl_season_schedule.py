from datetime import timedelta

import pandas as pd

from ingest.nfl_season_schedule import _kickoff


def test_kickoff_uses_daylight_time_in_september() -> None:
    kickoff = _kickoff("2026-09-10", "20:20")

    assert kickoff is not None
    assert kickoff.utcoffset() == -timedelta(hours=4)
    assert kickoff.isoformat() == "2026-09-10T20:20:00-04:00"


def test_kickoff_uses_standard_time_in_december() -> None:
    kickoff = _kickoff("2026-12-10", "20:15:00")

    assert kickoff is not None
    assert kickoff.utcoffset() == -timedelta(hours=5)
    assert kickoff.isoformat() == "2026-12-10T20:15:00-05:00"


def test_kickoff_rejects_missing_schedule_values() -> None:
    assert _kickoff(pd.NA, "13:00") is None
    assert _kickoff("2026-09-13", float("nan")) is None
