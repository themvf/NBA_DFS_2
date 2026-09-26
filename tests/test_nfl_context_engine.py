from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from model.nfl_context_engine import (
    ContextMeasurement,
    ContextPolicyRegistry,
    ContextQualification,
    ContextReader,
    ContextState,
    ContextUsage,
    InMemoryContextRepository,
    UnqualifiedContext,
)
from model.nfl_context_measures import (
    NEUTRAL_SNAP_INTERVAL,
    build_neutral_snap_interval_context,
    neutral_snap_intervals,
)


NOW = datetime(2026, 9, 20, 16, tzinfo=timezone.utc)
DEFINITION = NEUTRAL_SNAP_INTERVAL.definition_id


def measurement(*, value: float, as_of: datetime, release: str) -> ContextMeasurement:
    return ContextMeasurement(
        subject_type="team",
        subject_id="NYG",
        target_id="2026_02_NYG_LA",
        definition_id=DEFINITION,
        as_of_at=as_of,
        window={"games": ["g1", "g2"]},
        numerator=value * 20,
        denominator=20,
        value=value,
        state=ContextState.OBSERVED,
        coverage={"minimumCoverageMet": True},
        source_snapshot_ids=(f"source-{release}",),
        fact_release_id=release,
    )


def registry(*, fallback: str | None = None) -> ContextPolicyRegistry:
    rows = [
        ContextQualification(
            consumer_id="nfl_pbp_explorer",
            definition_id=DEFINITION,
            use_case="game_explanation",
            cohort="all_teams",
            usage=ContextUsage.DESCRIPTIVE,
            policy_version="policy-v1",
            approved=True,
            fallback_definition_id=fallback,
        ),
        ContextQualification(
            consumer_id="nfl_dfs_shadow",
            definition_id=DEFINITION,
            use_case="team_opportunity",
            cohort="returning_offenses",
            usage=ContextUsage.PREDICTIVE,
            policy_version="policy-v1",
            approved=True,
        ),
    ]
    if fallback:
        rows.append(
            ContextQualification(
                consumer_id="nfl_pbp_explorer",
                definition_id=fallback,
                use_case="game_explanation",
                cohort="all_teams",
                usage=ContextUsage.DESCRIPTIVE,
                policy_version="policy-v1",
                approved=True,
            )
        )
    return ContextPolicyRegistry(rows)


def test_pinned_read_never_substitutes_a_newer_correction() -> None:
    old = measurement(value=28.0, as_of=NOW - timedelta(days=2), release="facts-v1")
    corrected = measurement(value=26.0, as_of=NOW - timedelta(days=1), release="facts-v2")
    repository = InMemoryContextRepository([old, corrected])
    reader = ContextReader(repository, registry())

    pinned = reader.read_pinned(
        snapshot_id=old.snapshot_id,
        consumer_id="nfl_pbp_explorer",
        use_case="game_explanation",
        cohort="all_teams",
        usage=ContextUsage.DESCRIPTIVE,
        requested_as_of=NOW,
    )
    current = reader.read_current(
        definition_id=DEFINITION,
        subject_id="NYG",
        target_id="2026_02_NYG_LA",
        consumer_id="nfl_pbp_explorer",
        use_case="game_explanation",
        cohort="all_teams",
        usage=ContextUsage.DESCRIPTIVE,
        requested_as_of=NOW,
    )

    assert pinned.measurement.value == 28.0
    assert pinned.manifest.context_snapshot_id == old.snapshot_id
    assert current.measurement.value == 26.0
    assert current.manifest.context_snapshot_id == corrected.snapshot_id


def test_caller_cannot_grant_itself_decision_permission() -> None:
    value = measurement(value=28.0, as_of=NOW, release="facts-v1")
    reader = ContextReader(InMemoryContextRepository([value]), registry())
    with pytest.raises(UnqualifiedContext):
        reader.read_pinned(
            snapshot_id=value.snapshot_id,
            consumer_id="nfl_dfs_optimizer",
            use_case="lineup_selection",
            cohort="classic",
            usage=ContextUsage.DECISION,
            requested_as_of=NOW,
        )


def test_qualification_is_specific_to_consumer_use_case_cohort_and_usage() -> None:
    value = measurement(value=28.0, as_of=NOW, release="facts-v1")
    reader = ContextReader(InMemoryContextRepository([value]), registry())
    with pytest.raises(UnqualifiedContext):
        reader.read_pinned(
            snapshot_id=value.snapshot_id,
            consumer_id="nfl_dfs_shadow",
            use_case="team_opportunity",
            cohort="rookie_qb",
            usage=ContextUsage.PREDICTIVE,
            requested_as_of=NOW,
        )


def test_stale_dependency_uses_only_a_separately_qualified_fallback() -> None:
    fallback_id = "eligible_plays_per_possession@v1"
    primary = measurement(value=28.0, as_of=NOW - timedelta(days=2), release="facts-v1")
    fallback = ContextMeasurement(
        subject_type="team",
        subject_id="NYG",
        target_id="2026_02_NYG_LA",
        definition_id=fallback_id,
        as_of_at=NOW - timedelta(hours=1),
        window={"games": ["g1", "g2"]},
        numerator=120,
        denominator=20,
        value=6,
        state=ContextState.OBSERVED,
        coverage={"minimumCoverageMet": True},
        source_snapshot_ids=("source-f1",),
        fact_release_id="facts-v1",
    )
    policies = ContextPolicyRegistry(
        [
            ContextQualification(
                consumer_id="nfl_pbp_explorer",
                definition_id=DEFINITION,
                use_case="game_explanation",
                cohort="all_teams",
                usage=ContextUsage.DESCRIPTIVE,
                policy_version="policy-v2",
                approved=True,
                max_age_seconds=86400,
                fallback_definition_id=fallback_id,
            ),
            ContextQualification(
                consumer_id="nfl_pbp_explorer",
                definition_id=fallback_id,
                use_case="game_explanation",
                cohort="all_teams",
                usage=ContextUsage.DESCRIPTIVE,
                policy_version="policy-v2",
                approved=True,
                max_age_seconds=86400,
            ),
        ]
    )
    resolved = ContextReader(
        InMemoryContextRepository([primary, fallback]), policies
    ).read_current(
        definition_id=DEFINITION,
        subject_id="NYG",
        target_id="2026_02_NYG_LA",
        consumer_id="nfl_pbp_explorer",
        use_case="game_explanation",
        cohort="all_teams",
        usage=ContextUsage.DESCRIPTIVE,
        requested_as_of=NOW,
    )
    assert resolved.measurement.definition_id == fallback_id
    assert resolved.manifest.eligibility.fallback_used == fallback_id


def test_context_distinguishes_unknown_from_zero() -> None:
    unknown = ContextMeasurement(
        subject_type="team",
        subject_id="NYG",
        target_id="game",
        definition_id=DEFINITION,
        as_of_at=NOW,
        window={},
        numerator=0,
        denominator=0,
        value=None,
        state=ContextState.OBSERVED,
        coverage={"minimumCoverageMet": False},
        source_snapshot_ids=("s1",),
        fact_release_id="f1",
    )
    assert unknown.value is None
    assert unknown.denominator == 0


def test_estimate_requires_method_metadata() -> None:
    with pytest.raises(ValueError):
        ContextMeasurement(
            subject_type="team",
            subject_id="NYG",
            target_id="game",
            definition_id=DEFINITION,
            as_of_at=NOW,
            window={},
            numerator=1,
            denominator=2,
            value=0.5,
            state=ContextState.ESTIMATED,
            coverage={},
            source_snapshot_ids=("s1",),
            fact_release_id="f1",
        )


def test_intervals_are_formed_before_filtering_and_never_bridge_exclusions() -> None:
    base = {
        "game_id": "g1",
        "posteam": "NYG",
        "drive": 1,
        "qtr": 1,
        "play_type": "run",
        "qb_kneel": 0,
        "qb_spike": 0,
        "two_point_attempt": 0,
        "score_differential": 0,
    }
    rows = pd.DataFrame(
        [
            {**base, "play_id": 1, "game_seconds_remaining": 3600},
            {**base, "play_id": 2, "game_seconds_remaining": 3572},
            {**base, "play_id": 3, "game_seconds_remaining": 3550, "play_type": "no_play"},
            {**base, "play_id": 4, "game_seconds_remaining": 3520},
            {**base, "play_id": 5, "game_seconds_remaining": 3490, "score_differential": 10},
            {**base, "play_id": 6, "game_seconds_remaining": 3460},
            {**base, "play_id": 7, "game_seconds_remaining": 3435, "drive": 2},
            {**base, "play_id": 8, "game_seconds_remaining": 3400, "drive": 2},
        ]
    )
    intervals = neutral_snap_intervals(rows, team="NYG")
    assert intervals[["from_play_id", "to_play_id", "seconds"]].to_dict("records") == [
        {"from_play_id": 1.0, "to_play_id": 2, "seconds": 28.0},
        {"from_play_id": 7.0, "to_play_id": 8, "seconds": 35.0},
    ]


def test_context_object_carries_denominator_coverage_and_evidence() -> None:
    base = {
        "game_id": "g1",
        "posteam": "NYG",
        "drive": 1,
        "qtr": 1,
        "play_type": "run",
        "qb_kneel": 0,
        "qb_spike": 0,
        "two_point_attempt": 0,
        "score_differential": 0,
    }
    rows = pd.DataFrame(
        [
            {**base, "play_id": 1, "game_seconds_remaining": 3600},
            {**base, "play_id": 2, "game_seconds_remaining": 3570},
        ]
    )
    result = build_neutral_snap_interval_context(
        rows,
        team="NYG",
        target_id="2026_02_NYG_LA",
        as_of_at=NOW,
        source_snapshot_ids=["pbp-2025-v1"],
        fact_release_id="facts-2025-v1",
    )
    assert result.value == 30.0
    assert result.numerator == 30.0
    assert result.denominator == 1.0
    assert result.coverage["minimumCoverageMet"] is False
    assert result.source_snapshot_ids == ("pbp-2025-v1",)
    assert result.as_dict()["contextSnapshotId"] == result.snapshot_id
