from datetime import datetime, timedelta, timezone

import pytest

from model.nfl_game_availability import resolve_game_availability


NOW = datetime(2026, 9, 25, 16, 0, tzinfo=timezone.utc)
KICKOFF = NOW + timedelta(days=2)


def row(observation_id, source, status, when=NOW, **overrides):
    return {
        "observation_id": observation_id,
        "source_snapshot_id": observation_id + 100,
        "source": source,
        "status": status,
        "available_at": when,
        "model_eligible": True,
        "snapshot_status": "success",
        "game_scope_valid": True,
        "observation_kickoff": KICKOFF.isoformat() if source == "nfl_official" else None,
        **overrides,
    }


def test_available_after_decision_is_display_only_even_when_before_kickoff():
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT", NOW + timedelta(hours=2))],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "UNKNOWN"
    assert decision.display_only_observation_ids == (1,)


@pytest.mark.parametrize("status", ["partial", "failed"])
def test_incomplete_refresh_cannot_create_or_clear_status(status):
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT", snapshot_status=status)],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "UNKNOWN"
    assert decision.projection_status is None


def test_missing_newer_row_does_not_clear_an_established_out():
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT", NOW - timedelta(hours=2))],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "OUT_CONFIRMED"
    assert decision.projection_status == "OUT"


def test_complete_newer_same_source_active_can_clear_out():
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT", NOW - timedelta(hours=2)),
         row(2, "sleeper", "HEALTHY", NOW - timedelta(hours=1))],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "EXPECTED_ACTIVE"
    assert decision.projection_status is None
    assert decision.observation_id == 2


def test_qualified_cross_source_conflict_retains_baseline():
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT"), row(2, "fantasypros", "HEALTHY")],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "CONFLICT"
    assert decision.projection_status is None


def test_ineligible_source_cannot_create_conflict():
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT"),
         row(2, "fantasypros", "HEALTHY", model_eligible=False)],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "OUT_CONFIRMED"
    assert decision.projection_status == "OUT"
    assert decision.display_only_observation_ids == (2,)


def test_questionable_retains_baseline_in_v1():
    decision = resolve_game_availability(
        [row(1, "sleeper", "QUESTIONABLE")], as_of_at=NOW, kickoff=KICKOFF)
    assert decision.state == "QUESTIONABLE"
    assert decision.projection_status is None


def test_stale_observation_cannot_authorize_new_exclusion():
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT", NOW - timedelta(hours=73))],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "STALE"
    assert decision.projection_status is None


def test_official_inactive_settles_a_source_conflict():
    decision = resolve_game_availability(
        [row(1, "sleeper", "HEALTHY"), row(2, "fantasypros", "OUT"),
         row(3, "nfl_official", "INACTIVE")],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "OUT_CONFIRMED"
    assert decision.source == "nfl_official"


def test_official_inactive_for_another_game_is_display_only():
    decision = resolve_game_availability(
        [row(3, "nfl_official", "INACTIVE", observation_kickoff=(KICKOFF + timedelta(days=7)).isoformat())],
        as_of_at=NOW, kickoff=KICKOFF,
    )
    assert decision.state == "UNKNOWN"
    assert decision.display_only_observation_ids == (3,)


def test_no_pregame_decision_at_or_after_kickoff():
    decision = resolve_game_availability(
        [row(1, "sleeper", "OUT")], as_of_at=KICKOFF, kickoff=KICKOFF)
    assert decision.state == "UNKNOWN"
    assert decision.projection_status is None


def test_naive_decision_time_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_game_availability([], as_of_at=NOW.replace(tzinfo=None), kickoff=KICKOFF)
