from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from model.ff_v2_audits import (
    FROZEN_PROTOCOL_DIGEST,
    assert_frozen_comparison_protocol,
    audit_evaluation_rows,
    build_audit_report,
    derive_seed,
    representative_rows,
    verify_audit_report,
)
from model.ff_v2_backtest import DEFAULT_SEED, verify_manifest_integrity


ROOT = Path(__file__).resolve().parents[1]
BACKTEST_PATH = ROOT / "artifacts" / "ff_v2_backtest_harness_2020_2025.json"
CHAMPION_PATH = ROOT / "artifacts" / "ff_champion_baseline_v1.14.json"
CUTOFFS = {2025: "2025-09-03T23:59:59-04:00"}


def _feature(
    value: object = 64.0,
    *,
    name: str = "priorPlays",
    available_at: str = "2025-09-03T20:00:00-04:00",
    dataset: str = "historical_team_week_facts",
    feature_group: str = "football_performance",
    source_season: int = 2024,
) -> tuple[str, dict[str, object]]:
    return name, {
        "value": value,
        "availableAt": available_at,
        "sourceDataset": dataset,
        "sourceSeason": source_season,
        "featureGroup": feature_group,
        "eligible": value is not None,
        "missingReason": None if value is not None else "unavailable_as_of_cutoff",
    }


def _row(identity: str = "TB:2025:1") -> dict[str, object]:
    token = f"prediction:{identity}"
    name, feature = _feature()
    return {
        "identity": identity,
        "evaluationSeason": 2025,
        "seedToken": token,
        "seed": derive_seed(DEFAULT_SEED, token),
        "features": {name: feature},
    }


def test_every_evaluated_row_exposes_feature_eligibility_and_missingness() -> None:
    row = _row()
    missing_name, missing = _feature(None, name="playCaller", source_season=2025)
    row["features"][missing_name] = missing  # type: ignore[index]

    audited = audit_evaluation_rows([row], preseason_cutoffs=CUTOFFS)

    assert audited[0]["featureCount"] == 2
    assert audited[0]["eligibleFeatureCount"] == 1
    assert audited[0]["missingFeatureCount"] == 1
    assert {feature["feature"] for feature in audited[0]["features"]} == {
        "playCaller", "priorPlays"
    }
    assert all("valueDigest" in feature for feature in audited[0]["features"])


def test_post_cutoff_feature_fails_closed() -> None:
    row = _row()
    row["features"]["priorPlays"]["availableAt"] = "2025-09-04T00:00:00-04:00"  # type: ignore[index]
    with pytest.raises(ValueError, match="Post-cutoff"):
        audit_evaluation_rows([row], preseason_cutoffs=CUTOFFS)


@pytest.mark.parametrize(
    "dataset,temporal_scope",
    [
        ("sleeper_current_roster", None),
        ("current_depth_chart", None),
        ("weekly_rosters", "current"),
    ],
)
def test_current_roster_leakage_fails_closed(dataset: str, temporal_scope: str | None) -> None:
    row = _row()
    feature = row["features"]["priorPlays"]  # type: ignore[index]
    feature["sourceDataset"] = dataset
    if temporal_scope:
        feature["temporalScope"] = temporal_scope
    with pytest.raises(ValueError, match="Current-roster leakage"):
        audit_evaluation_rows([row], preseason_cutoffs=CUTOFFS)


def test_future_roster_season_fails_closed() -> None:
    row = _row()
    row["features"]["priorPlays"]["sourceSeason"] = 2026  # type: ignore[index]
    with pytest.raises(ValueError, match="Future/current-season source leakage"):
        audit_evaluation_rows([row], preseason_cutoffs=CUTOFFS)


@pytest.mark.parametrize("feature_name", ["adp", "fantasypros_ecr", "market_projection"])
def test_adp_ecr_and_market_performance_features_fail_closed(feature_name: str) -> None:
    row = _row()
    _, feature = _feature(name=feature_name)
    row["features"] = {feature_name: feature}
    with pytest.raises(ValueError, match="ADP/ECR/market"):
        audit_evaluation_rows([row], preseason_cutoffs=CUTOFFS)


def test_market_baseline_remains_separate_from_football_performance_features() -> None:
    row = _row()
    _, feature = _feature(name="adp", feature_group="comparison_baseline")
    row["features"] = {"adp": feature}
    audited = audit_evaluation_rows([row], preseason_cutoffs=CUTOFFS)
    assert audited[0]["eligibleFeatureCount"] == 1


def test_duplicate_identity_and_unstable_seeds_fail_closed() -> None:
    row = _row()
    with pytest.raises(ValueError, match="Duplicate evaluated row identity"):
        audit_evaluation_rows([row, copy.deepcopy(row)], preseason_cutoffs=CUTOFFS)

    unstable = _row()
    unstable["seed"] = 123
    with pytest.raises(ValueError, match="Unstable or non-derived seed"):
        audit_evaluation_rows([unstable], preseason_cutoffs=CUTOFFS)
    with pytest.raises(ValueError, match="Root seed is unstable"):
        audit_evaluation_rows([_row()], preseason_cutoffs=CUTOFFS, root_seed=123)


def test_missing_feature_without_explicit_missingness_fails_closed() -> None:
    row = _row()
    row["features"]["priorPlays"]["value"] = None  # type: ignore[index]
    with pytest.raises(ValueError, match="Missing feature is not explicitly ineligible"):
        audit_evaluation_rows([row], preseason_cutoffs=CUTOFFS)


def test_champion_challenger_protocol_is_frozen_before_fit() -> None:
    backtest = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
    champion = json.loads(CHAMPION_PATH.read_text(encoding="utf-8"))
    assert len(FROZEN_PROTOCOL_DIGEST) == 64
    assert_frozen_comparison_protocol(champion, backtest)

    fitted = copy.deepcopy(backtest)
    fitted["modelVersion"] = "team-opportunity-v1"
    with pytest.raises(ValueError, match="before Team Opportunity fit"):
        assert_frozen_comparison_protocol(champion, fitted)


def test_audit_artifact_replays_and_tampering_fails() -> None:
    backtest = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
    champion = json.loads(CHAMPION_PATH.read_text(encoding="utf-8"))
    report = build_audit_report(backtest, champion, representative_rows(backtest))
    verify_audit_report(report, backtest, champion)
    assert report["summary"] == {
        "evaluatedRowCount": 5,
        "featureCount": 15,
        "eligibleFeatureCount": 10,
        "missingFeatureCount": 5,
        "teamOpportunityFitted": False,
        "liveProjectionChanged": False,
    }

    tampered = copy.deepcopy(report)
    tampered["rowAudits"][0]["features"][0]["eligible"] = True
    with pytest.raises(RuntimeError, match="digest differs"):
        verify_audit_report(tampered, backtest, champion)


def _persisted_records(stored: dict, artifact_path: Path) -> tuple[dict, list[dict]]:
    run = {
        "run_id": stored["runId"],
        "harness_version": stored["harnessVersion"],
        "status": "complete",
        "context_run_id": stored["contextRunId"],
        "model_version": stored["modelVersion"],
        "calibration_version": stored["calibrationVersion"],
        "seed": stored["seed"],
        "evaluation_seasons": stored["evaluationSeasons"],
        "preseason_cutoffs": stored["preseasonCutoffs"],
        "source_snapshot_ids": stored["sourceSnapshotIds"],
        "cohort_counts": stored["cohortCounts"],
        "config": stored["config"],
        "output_digest": stored["outputDigest"],
        "artifact_path": str(artifact_path),
    }
    splits = [
        {
            "run_id": stored["runId"],
            "evaluation_season": split["evaluationSeason"],
            "preseason_cutoff": split["preseasonCutoff"],
            "training_seasons": split["trainingSeasons"],
            "training_row_counts": split["trainingRowCounts"],
            "evaluation_row_counts": split["evaluationRowCounts"],
            "training_digest": split["trainingDigest"],
            "evaluation_digest": split["evaluationDigest"],
            "split_digest": split["splitDigest"],
            "scorable": split["scorable"],
            "exclusion_reason": split["exclusionReason"],
        }
        for split in stored["splits"]
    ]
    return run, splits


@pytest.mark.parametrize(
    "field",
    [
        "preseason_cutoff", "training_seasons", "training_row_counts",
        "evaluation_row_counts", "training_digest", "evaluation_digest",
        "split_digest", "scorable", "exclusion_reason",
    ],
)
def test_backtest_verifier_compares_every_persisted_split_field(field: str) -> None:
    stored = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
    rebuilt = copy.deepcopy(stored)
    rebuilt["createdAt"] = "different-but-informational"
    run, splits = _persisted_records(stored, BACKTEST_PATH.relative_to(ROOT))
    target = splits[1]
    if field in {"training_seasons", "training_row_counts", "evaluation_row_counts"}:
        target[field] = {"tampered": True}
    elif field == "scorable":
        target[field] = not target[field]
    elif field == "exclusion_reason":
        target[field] = "tampered"
    else:
        target[field] = "tampered"
    with pytest.raises(RuntimeError, match="fields differ"):
        verify_manifest_integrity(
            stored,
            rebuilt,
            run,
            splits,
            artifact_path=BACKTEST_PATH.relative_to(ROOT),
        )


def test_backtest_verifier_rejects_unreproducible_artifact_and_run_fields() -> None:
    stored = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
    rebuilt = copy.deepcopy(stored)
    run, splits = _persisted_records(stored, BACKTEST_PATH.relative_to(ROOT))
    rebuilt["cohortCounts"]["2025"]["team_week"] += 1
    with pytest.raises(RuntimeError, match="did not reproduce fields"):
        verify_manifest_integrity(
            stored, rebuilt, run, splits, artifact_path=BACKTEST_PATH.relative_to(ROOT)
        )

    rebuilt = copy.deepcopy(stored)
    run["seed"] += 1
    with pytest.raises(RuntimeError, match="run fields differ"):
        verify_manifest_integrity(
            stored, rebuilt, run, splits, artifact_path=BACKTEST_PATH.relative_to(ROOT)
        )
