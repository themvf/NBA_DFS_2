from copy import deepcopy
from datetime import datetime, timedelta, timezone

from model.nfl_availability_context import (
    PLAYER_GAME_AVAILABILITY,
    TEAM_QB_STATE,
    build_availability_contexts,
    expected_role,
)
from model.nfl_context_engine import InMemoryContextRepository


NOW = datetime(2026, 9, 25, 14, tzinfo=timezone.utc)
PUBLISHED = NOW + timedelta(seconds=5)


def decision(state, player_id, *, source="sleeper"):
    return {
        "version": "player-game-availability-v1",
        "state": state,
        "projection_status": "OUT" if state == "OUT_CONFIRMED" else None,
        "source": source,
        "observation_id": player_id * 10,
        "source_snapshot_id": 99,
        "available_at": (NOW - timedelta(hours=1)).isoformat(),
        "as_of_at": NOW.isoformat(),
        "kickoff": (NOW + timedelta(days=1)).isoformat(),
        "reason": "test",
        "qualifying_observation_ids": [player_id * 10],
        "display_only_observation_ids": [],
        "qualifying_source_snapshot_ids": [99],
        "display_only_source_snapshot_ids": [],
    }


def players():
    return [
        {"player_id": 1, "team": "WAS", "position": "QB", "depth_order": 1,
         "game_id": "2026_04_SEA_WAS", "event_id": "odds-1"},
        {"player_id": 2, "team": "WAS", "position": "QB", "depth_order": 2,
         "game_id": "2026_04_SEA_WAS", "event_id": "odds-1"},
        {"player_id": 3, "team": "WAS", "position": "WR", "depth_order": 2,
         "game_id": "2026_04_SEA_WAS", "event_id": "odds-1"},
    ]


def build(rows=None, decisions=None):
    return build_availability_contexts(
        rows or players(),
        decisions or {"1": decision("OUT_CONFIRMED", 1),
                      "2": decision("EXPECTED_ACTIVE", 2),
                      "3": decision("QUESTIONABLE", 3)},
        season=2026,
        week=4,
        as_of_at=NOW,
        available_at=PUBLISHED,
        fact_release_id="availability-release-1",
    )


def test_structured_player_and_qb_contracts_share_evidence():
    contexts, report = build()
    player = next(value for value in contexts if value.definition_id == PLAYER_GAME_AVAILABILITY.definition_id and value.subject_id == "1")
    qb = next(value for value in contexts if value.definition_id == TEAM_QB_STATE.definition_id)
    assert player.payload["replacement_player_id"] == 2
    assert player.payload["participation_probability"] is None
    assert player.payload["expected_role"] == "QB1"
    assert qb.payload["baseline_starter_player_id"] == 1
    assert qb.payload["expected_starter_player_id"] == 2
    assert qb.payload["starter_change_state"] == "CONFIRMED_REPLACEMENT"
    assert player.snapshot_id in qb.payload["availability_evidence"]["playerContextIds"]
    assert report["playerContexts"] == 3
    assert report["byState"]["OUT_CONFIRMED"] == 1


def test_context_identity_and_evidence_digest_are_stable():
    first, first_report = build()
    second, second_report = build(deepcopy(players()))
    assert [value.snapshot_id for value in first] == [value.snapshot_id for value in second]
    assert [value.payload.get("evidence_digest") for value in first] == [
        value.payload.get("evidence_digest") for value in second
    ]
    assert first_report["reportDigest"] == second_report["reportDigest"]


def test_correction_does_not_mutate_pinned_snapshot():
    first, _ = build()
    corrected_decisions = {"1": decision("EXPECTED_ACTIVE", 1),
                           "2": decision("EXPECTED_ACTIVE", 2),
                           "3": decision("QUESTIONABLE", 3)}
    corrected, _ = build(decisions=corrected_decisions)
    repository = InMemoryContextRepository(first)
    old = next(value for value in first if value.subject_id == "1")
    new = next(value for value in corrected if value.subject_id == "1")
    repository.add(new)
    assert old.snapshot_id != new.snapshot_id
    assert repository.pinned(old.snapshot_id).payload["resolved_availability_state"] == "OUT_CONFIRMED"
    assert repository.current(
        definition_id=PLAYER_GAME_AVAILABILITY.definition_id,
        subject_id="1",
        target_id="2026_04_SEA_WAS",
        as_of_at=PUBLISHED,
    ).payload["resolved_availability_state"] == "EXPECTED_ACTIVE"


def test_missing_depth_stays_unresolved_instead_of_backfilling_current_roster():
    rows = players()
    rows[0]["depth_order"] = None
    contexts, _ = build(rows=rows)
    player = next(value for value in contexts if value.definition_id == PLAYER_GAME_AVAILABILITY.definition_id and value.subject_id == "1")
    assert player.payload["expected_role"] == "UNRESOLVED"
    assert player.payload["replacement_player_id"] == 2
    assert player.coverage["hasFreshDepth"] is False


def test_role_mapping_is_position_specific():
    assert expected_role("QB", 3) == "QB3_PLUS"
    assert expected_role("RB", 2) == "RB_COMMITTEE"
    assert expected_role("WR", 4) == "WR_ROTATION"
    assert expected_role("TE", 1) == "TE1"
    assert expected_role("K", 1) == "UNRESOLVED"
