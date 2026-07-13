from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest.mlb_schedule import _starter_workload_from_game_logs


def _start(day: str, pitches: int, innings: str) -> dict:
    return {
        "date": day,
        "stat": {"gamesStarted": 1, "numberOfPitches": pitches, "inningsPitched": innings},
    }


def test_workload_uses_three_latest_prior_starts_and_outs_aware_innings() -> None:
    rows = [
        _start("2026-06-20", 80, "4.0"),
        _start("2026-07-01", 90, "5.2"),
        _start("2026-07-06", 96, "6.1"),
        _start("2026-07-11", 102, "7.0"),
        _start("2026-07-17", 110, "8.0"),
    ]
    result = _starter_workload_from_game_logs(
        rows,
        event_start=datetime(2026, 7, 17, 23, tzinfo=timezone.utc),
        season_ip_per_start=5.8,
    )
    assert result is not None
    assert result["last_start_date"] == "2026-07-11"
    assert result["days_rest"] == 5
    assert result["starts_sample"] == 3
    assert result["avg_pitches_last_3"] == 96
    assert result["avg_innings_last_3"] == pytest.approx((7 + 6 + 1 / 3 + 5 + 2 / 3) / 3)
    assert result["expected_innings"] == pytest.approx(
        0.6 * result["avg_innings_last_3"] + 0.4 * 5.8
    )


def test_workload_does_not_treat_relief_appearances_as_starts() -> None:
    rows = [{"date": "2026-07-10", "stat": {"gamesStarted": 0, "numberOfPitches": 40, "inningsPitched": "2.0"}}]
    result = _starter_workload_from_game_logs(
        rows,
        event_start=datetime(2026, 7, 17, 23, tzinfo=timezone.utc),
        season_ip_per_start=5.5,
    )
    assert result is not None
    assert result["starts_sample"] == 0
    assert result["expected_innings"] == 5.5
