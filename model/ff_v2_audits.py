"""Fail-closed validation audits for the roster-aware V2 shadow challenger.

This module freezes the champion-versus-challenger evaluation contract before
Team Opportunity is fitted and audits every evaluation feature for provenance,
cutoff eligibility, missingness, identity uniqueness, and deterministic seeds.
It does not fit a model or alter any live projection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from model.ff_v2_backtest import DEFAULT_SEED, HARNESS_VERSION
from model.ff_v2_metrics import (
    DEFAULT_BOOTSTRAP_DRAWS,
    DEFAULT_BOOTSTRAP_SEED,
    FROZEN_METRIC_POLICY,
    METRIC_SUITE_VERSION,
    REQUIRED_MODEL_LABELS,
)


AUDIT_VERSION = "ff-v2-validation-audit-v1"
DEFAULT_BACKTEST_ARTIFACT = Path("artifacts/ff_v2_backtest_harness_2020_2025.json")
DEFAULT_CHAMPION_ARTIFACT = Path("artifacts/ff_champion_baseline_v1.14.json")
DEFAULT_AUDIT_ARTIFACT = Path("artifacts/ff_v2_validation_audit_2020_2025.json")

FROZEN_CHAMPION = {
    "artifactType": "fantasy-football-champion-baseline",
    "championModelVersion": "ff-independent-v1.14",
    "combinedDigest": "d9dbdb129aeec4e79e6421ca32bc71c06f128b4b822ac2917bca7f202989d6ac",
}
FROZEN_METRIC_POLICY_DIGEST = "c892f475015886ac647a051f6ddd6429b746a4ea974cd7e25795d61e3554522a"
FROZEN_PROTOCOL_DIGEST = "56462cd23e414065a3fbc5157b2e67ad9e1ffda777d6d7fa474a95fb1963907b"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


FROZEN_COMPARISON_PROTOCOL = {
    "protocolVersion": "ff-v2-champion-challenger-v1",
    "frozenAt": "2026-08-29T00:00:00-04:00",
    "frozenBeforeTeamOpportunityFit": True,
    "champion": FROZEN_CHAMPION,
    "challengerLabel": "challenger",
    "requiredModelLabels": list(REQUIRED_MODEL_LABELS),
    "backtestHarnessVersion": HARNESS_VERSION,
    "backtestSeed": DEFAULT_SEED,
    "metricSuiteVersion": METRIC_SUITE_VERSION,
    "metricPolicy": FROZEN_METRIC_POLICY,
    "metricPolicyDigest": FROZEN_METRIC_POLICY_DIGEST,
    "bootstrapDraws": DEFAULT_BOOTSTRAP_DRAWS,
    "bootstrapSeed": DEFAULT_BOOTSTRAP_SEED,
    "comparisonIdentity": "exact_shared_artifact_identity",
    "promotionAuthority": "V2-021_verdict_plus_explicit_post_evidence_user_authorization",
}

_MARKET_TOKENS = {
    "adp", "average_draft_position", "ecr", "expert_consensus_rankings",
    "market", "market_rank", "market_projection", "consensus_rank",
}
_CURRENT_ROSTER_DATASET_MARKERS = {
    "current_roster", "live_roster", "current_depth", "live_depth",
    "sleeper_current", "current_transaction", "live_transaction",
}


def derive_seed(root_seed: int, token: str) -> int:
    """Derive a stable child seed without process-randomized ``hash()``."""

    payload = f"{int(root_seed)}|{token}".encode("utf-8")
    return int(hashlib.sha256(payload).hexdigest()[:16], 16)


def _parse_timestamp(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include an offset: {value!r}")
    return parsed


def _tokens(value: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    pieces = set(normalized.split("_"))
    pieces.add(normalized)
    return pieces


def _is_market_feature(name: str, metadata: Mapping[str, Any]) -> bool:
    candidates = {name, str(metadata.get("sourceDataset", "")), str(metadata.get("field", ""))}
    tokens: set[str] = set()
    for candidate in candidates:
        tokens.update(_tokens(candidate))
    return bool(tokens & _MARKET_TOKENS) or any(
        marker in "_".join(sorted(tokens)) for marker in _MARKET_TOKENS
    )


def _is_current_roster_source(metadata: Mapping[str, Any]) -> bool:
    dataset = re.sub(r"[^a-z0-9]+", "_", str(metadata.get("sourceDataset", "")).lower())
    temporal_scope = str(metadata.get("temporalScope", "")).lower()
    return temporal_scope in {"current", "live", "latest"} or any(
        marker in dataset for marker in _CURRENT_ROSTER_DATASET_MARKERS
    )


def assert_frozen_comparison_protocol(
    champion_artifact: Mapping[str, Any],
    backtest_artifact: Mapping[str, Any],
) -> None:
    """Verify the exact preregistered champion, harness, seeds, and V2-005 policy."""

    for field, expected in FROZEN_CHAMPION.items():
        if champion_artifact.get(field) != expected:
            raise ValueError(f"Frozen champion field differs: {field}")
    for board in champion_artifact.get("boards", []):
        if board.get("sourceRequestParams", {}).get("adp_used_for_projection") is not False:
            raise ValueError("Frozen champion permits ADP as a football-performance feature")
    if backtest_artifact.get("harnessVersion") != HARNESS_VERSION:
        raise ValueError("Backtest harness version differs from frozen protocol")
    if int(backtest_artifact.get("seed", -1)) != DEFAULT_SEED:
        raise ValueError("Backtest seed differs from frozen protocol")
    if backtest_artifact.get("modelVersion") != "unfitted-harness-contract":
        raise ValueError("Comparison protocol must be frozen before Team Opportunity fit")
    if backtest_artifact.get("calibrationVersion") != "unfitted":
        raise ValueError("Comparison protocol must be frozen before calibration")
    if _digest(FROZEN_COMPARISON_PROTOCOL["metricPolicy"]) != FROZEN_COMPARISON_PROTOCOL["metricPolicyDigest"]:
        raise RuntimeError("Frozen V2-005 metric policy digest differs")
    if _digest(FROZEN_COMPARISON_PROTOCOL) != FROZEN_PROTOCOL_DIGEST:
        raise RuntimeError("Frozen champion-versus-challenger protocol differs")


def audit_evaluation_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    preseason_cutoffs: Mapping[int, str],
    root_seed: int = DEFAULT_SEED,
) -> list[dict[str, Any]]:
    """Audit and return feature eligibility/missingness for every input row.

    Each row must carry an ``identity``, ``evaluationSeason``, deterministic
    ``seedToken``/``seed`` pair, and a mapping of feature name to metadata.
    Every feature explicitly declares ``eligible`` and ``missingReason``.
    """

    if isinstance(root_seed, bool) or not isinstance(root_seed, int) or root_seed != DEFAULT_SEED:
        raise ValueError("Root seed is unstable or differs from the frozen backtest seed")
    seen: set[str] = set()
    audits: list[dict[str, Any]] = []
    for row in rows:
        identity = str(row.get("identity", "")).strip()
        if not identity:
            raise ValueError("Evaluated row identity is required")
        if identity in seen:
            raise ValueError(f"Duplicate evaluated row identity: {identity}")
        seen.add(identity)
        season = int(row["evaluationSeason"])
        if season not in preseason_cutoffs:
            raise ValueError(f"No frozen cutoff for evaluation season {season}")
        cutoff = _parse_timestamp(preseason_cutoffs[season])
        seed_token = str(row.get("seedToken", "")).strip()
        if not seed_token or int(row.get("seed", -1)) != derive_seed(root_seed, seed_token):
            raise ValueError(f"Unstable or non-derived seed for evaluated row {identity}")
        features = row.get("features")
        if not isinstance(features, Mapping) or not features:
            raise ValueError(f"Evaluated row {identity} has no auditable features")

        feature_audits: list[dict[str, Any]] = []
        for feature_name in sorted(features):
            metadata = features[feature_name]
            if not isinstance(metadata, Mapping):
                raise ValueError(f"Feature {feature_name} metadata must be a mapping")
            required = {"value", "availableAt", "sourceDataset", "featureGroup", "eligible", "missingReason"}
            missing_contract = sorted(required - metadata.keys())
            if missing_contract:
                raise ValueError(
                    f"Feature {feature_name} lacks audit fields: {', '.join(missing_contract)}"
                )
            available_at = _parse_timestamp(metadata["availableAt"])
            if available_at > cutoff:
                raise ValueError(f"Post-cutoff feature: {identity}/{feature_name}")
            source_season = metadata.get("sourceSeason")
            if source_season is not None and int(source_season) > season:
                raise ValueError(f"Future/current-season source leakage: {identity}/{feature_name}")
            if _is_current_roster_source(metadata):
                raise ValueError(f"Current-roster leakage: {identity}/{feature_name}")
            if str(metadata["featureGroup"]) == "football_performance" and _is_market_feature(
                feature_name, metadata
            ):
                raise ValueError(f"ADP/ECR/market performance feature: {identity}/{feature_name}")

            eligible = metadata["eligible"]
            if not isinstance(eligible, bool):
                raise ValueError(f"Feature eligibility must be boolean: {identity}/{feature_name}")
            missing = metadata["value"] is None
            missing_reason = metadata["missingReason"]
            if missing and (eligible or not isinstance(missing_reason, str) or not missing_reason.strip()):
                raise ValueError(f"Missing feature is not explicitly ineligible: {identity}/{feature_name}")
            if not missing and missing_reason is not None:
                raise ValueError(f"Present feature has a missingness reason: {identity}/{feature_name}")
            if not missing and not eligible:
                raise ValueError(f"Present feature is silently ineligible: {identity}/{feature_name}")
            feature_audits.append(
                {
                    "feature": feature_name,
                    "sourceDataset": str(metadata["sourceDataset"]),
                    "sourceSeason": int(source_season) if source_season is not None else None,
                    "availableAt": str(metadata["availableAt"]),
                    "featureGroup": str(metadata["featureGroup"]),
                    "eligible": eligible,
                    "missing": missing,
                    "missingReason": missing_reason,
                    "valueDigest": _digest(metadata["value"]),
                }
            )
        audits.append(
            {
                "identity": identity,
                "evaluationSeason": season,
                "seedToken": seed_token,
                "seed": int(row["seed"]),
                "featureCount": len(feature_audits),
                "eligibleFeatureCount": sum(item["eligible"] for item in feature_audits),
                "missingFeatureCount": sum(item["missing"] for item in feature_audits),
                "features": feature_audits,
            }
        )
    return audits


def representative_rows(backtest_artifact: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build one real historical contract row for every scorable fold."""

    rows: list[dict[str, Any]] = []
    for split in backtest_artifact["splits"]:
        if not split["scorable"]:
            continue
        season = int(split["evaluationSeason"])
        cutoff = split["preseasonCutoff"]
        token = f"team-opportunity-fold:{season}"
        rows.append(
            {
                "identity": f"team-opportunity-fold:{season}",
                "evaluationSeason": season,
                "seedToken": token,
                "seed": derive_seed(DEFAULT_SEED, token),
                "features": {
                    "priorTeamWeekFacts": {
                        "value": split["trainingDigest"],
                        "availableAt": cutoff,
                        "sourceDataset": "ff_v2_team_week_facts_prior_seasons",
                        "sourceSeason": max(split["trainingSeasons"]),
                        "featureGroup": "football_performance",
                        "eligible": True,
                        "missingReason": None,
                    },
                    "scheduleContext": {
                        "value": {"evaluationSeason": season, "cutoff": cutoff},
                        "availableAt": cutoff,
                        "sourceDataset": "nflverse_historical_schedule_as_of",
                        "sourceSeason": season,
                        "featureGroup": "schedule_context",
                        "eligible": True,
                        "missingReason": None,
                    },
                    "playCaller": {
                        "value": None,
                        "availableAt": cutoff,
                        "sourceDataset": "attributable_play_caller_history",
                        "sourceSeason": season,
                        "featureGroup": "football_performance",
                        "eligible": False,
                        "missingReason": "not_captured_historical_as_of",
                    },
                },
            }
        )
    return rows


def build_audit_report(
    backtest_artifact: Mapping[str, Any],
    champion_artifact: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    assert_frozen_comparison_protocol(champion_artifact, backtest_artifact)
    cutoffs = {int(key): value for key, value in backtest_artifact["preseasonCutoffs"].items()}
    row_audits = audit_evaluation_rows(
        rows, preseason_cutoffs=cutoffs, root_seed=int(backtest_artifact["seed"])
    )
    deterministic = {
        "artifactType": "fantasy-football-v2-validation-audit",
        "schemaVersion": 1,
        "auditVersion": AUDIT_VERSION,
        "comparisonProtocol": FROZEN_COMPARISON_PROTOCOL,
        "comparisonProtocolDigest": FROZEN_PROTOCOL_DIGEST,
        "backtestRunId": backtest_artifact["runId"],
        "backtestOutputDigest": backtest_artifact["outputDigest"],
        "championCombinedDigest": champion_artifact["combinedDigest"],
        "rootSeed": int(backtest_artifact["seed"]),
        "preseasonCutoffs": backtest_artifact["preseasonCutoffs"],
        "evaluatedRows": list(rows),
        "rowAudits": row_audits,
        "summary": {
            "evaluatedRowCount": len(row_audits),
            "featureCount": sum(row["featureCount"] for row in row_audits),
            "eligibleFeatureCount": sum(row["eligibleFeatureCount"] for row in row_audits),
            "missingFeatureCount": sum(row["missingFeatureCount"] for row in row_audits),
            "teamOpportunityFitted": False,
            "liveProjectionChanged": False,
        },
    }
    return {**deterministic, "artifactDigest": _digest(deterministic)}


def verify_audit_report(
    report: Mapping[str, Any],
    backtest_artifact: Mapping[str, Any],
    champion_artifact: Mapping[str, Any],
) -> None:
    deterministic = {key: value for key, value in report.items() if key != "artifactDigest"}
    if report.get("artifactDigest") != _digest(deterministic):
        raise RuntimeError("Validation audit artifact digest differs")
    rebuilt = build_audit_report(backtest_artifact, champion_artifact, report["evaluatedRows"])
    if rebuilt != report:
        mismatches = sorted(key for key in set(rebuilt) | set(report) if rebuilt.get(key) != report.get(key))
        raise RuntimeError("Validation audit artifact did not reproduce: " + ", ".join(mismatches))


def run(backtest_path: Path, champion_path: Path, artifact_path: Path) -> dict[str, Any]:
    backtest = json.loads(backtest_path.read_text(encoding="utf-8"))
    champion = json.loads(champion_path.read_text(encoding="utf-8"))
    report = build_audit_report(backtest, champion, representative_rows(backtest))
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def verify(backtest_path: Path, champion_path: Path, artifact_path: Path) -> dict[str, Any]:
    backtest = json.loads(backtest_path.read_text(encoding="utf-8"))
    champion = json.loads(champion_path.read_text(encoding="utf-8"))
    report = json.loads(artifact_path.read_text(encoding="utf-8"))
    verify_audit_report(report, backtest, champion)
    return {
        "status": "verified",
        "artifactDigest": report["artifactDigest"],
        **report["summary"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backtest-artifact", type=Path, default=DEFAULT_BACKTEST_ARTIFACT)
    parser.add_argument("--champion-artifact", type=Path, default=DEFAULT_CHAMPION_ARTIFACT)
    parser.add_argument("--artifact", type=Path, default=DEFAULT_AUDIT_ARTIFACT)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    result = (
        verify(args.backtest_artifact, args.champion_artifact, args.artifact)
        if args.verify
        else run(args.backtest_artifact, args.champion_artifact, args.artifact)
    )
    print(json.dumps(result, indent=2, sort_keys=True))
