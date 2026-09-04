from datetime import datetime, timezone

import pytest

from model.nfl_historical_odds_plan import build_plan, planned_snapshots


def test_shared_kickoff_collapses_to_one_bulk_call_per_checkpoint() -> None:
    kickoff = datetime(2025, 9, 7, 17, tzinfo=timezone.utc)
    plan = build_plan([(2025, 1, kickoff), (2025, 2, kickoff)], seasons=[2025])

    assert plan["games"] == 2
    assert plan["unique_snapshot_calls"] == 6
    assert plan["credits_per_call"] == 30
    assert plan["estimated_credits"] == 180
    assert plan["paid_api_calls_made"] == 0


def test_distinct_kickoffs_remain_distinct_requests() -> None:
    games = [
        (2025, 1, datetime(2025, 9, 7, 17, tzinfo=timezone.utc)),
        (2025, 2, datetime(2025, 9, 7, 20, tzinfo=timezone.utc)),
    ]
    plan = planned_snapshots(games, {"t_minus_6h": 360})

    assert plan["unique_snapshot_calls"] == 2
    assert plan["calls_by_checkpoint"] == {"t_minus_6h": 2}


def test_build_plan_can_cost_a_checkpoint_subset() -> None:
    kickoff = datetime(2025, 9, 7, 17, tzinfo=timezone.utc)
    plan = build_plan(
        [(2025, 1, kickoff)],
        seasons=[2025],
        checkpoints={"t_minus_6h": 360, "t_minus_15m": 15},
    )

    assert plan["unique_snapshot_calls"] == 2
    assert plan["estimated_credits"] == 60
    assert plan["checkpoints_minutes_before_kickoff"] == {"t_minus_6h": 360, "t_minus_15m": 15}


def test_naive_kickoff_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        planned_snapshots([(2025, 1, datetime(2025, 9, 7, 13))])
