from datetime import datetime, timezone

import pytest

from ingest.nfl_context_postgame import compare_contexts
from model.nfl_context_engine import (
    ContextMeasurement,
    ContextState,
    ContextUsage,
    EligibilityDecision,
    ResolvedContext,
    ResolvedManifest,
)


NOW = datetime(2026, 9, 24, tzinfo=timezone.utc)


def resolved(team: str, seconds: float) -> ResolvedContext:
    measurement = ContextMeasurement(
        snapshot_id=f"snap-{team}",
        subject_type="team",
        subject_id=team,
        target_id="game",
        definition_id="neutral_offensive_snap_interval_seconds@v1",
        as_of_at=NOW,
        window={},
        numerator=seconds * 100,
        denominator=100,
        value=seconds,
        state=ContextState.OBSERVED,
        coverage={},
        source_snapshot_ids=("source",),
        fact_release_id="facts",
    )
    eligibility = EligibilityDecision(
        approved=True,
        consumer_id="nfl_postgame_evaluator",
        definition_id=measurement.definition_id,
        use_case="context_replay",
        cohort="all_teams",
        usage=ContextUsage.DESCRIPTIVE,
        policy_version="policy-v1",
        reason="qualified",
    )
    return ResolvedContext(
        measurement=measurement,
        manifest=ResolvedManifest(
            context_snapshot_id=measurement.snapshot_id,
            definition_id=measurement.definition_id,
            fact_release_id="facts",
            source_snapshot_ids=("source",),
            policy_version="policy-v1",
            eligibility=eligibility,
        ),
    )


def test_postgame_comparison_is_descriptive_and_uses_saved_snapshots() -> None:
    report = compare_contexts([resolved("NYG", 31.9), resolved("LA", 31.5)])
    assert report["fasterHistoricalTeam"] == "LA"
    assert report["differenceSeconds"] == pytest.approx(0.4)
    assert [row["snapshotId"] for row in report["teams"]] == ["snap-LA", "snap-NYG"]
    assert "no causal" in report["interpretation"]


def test_postgame_requires_two_known_team_values() -> None:
    with pytest.raises(ValueError):
        compare_contexts([resolved("NYG", 31.9)])
