"""Deterministic rolling-origin harness for Fantasy Football roster-aware V2.

This module owns chronological splits, prediction/outcome separation, scoring
artifact contracts, and reproducible run metadata. It deliberately does not
fit Team Opportunity. The preregistered V2-005 suite is exposed through
``score_definitive_metrics``; Team Opportunity remains V2-W2.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from model.ff_v2_metrics import evaluate_metric_suite


HARNESS_VERSION = "ff-v2-backtest-v1"
RUN_NAMESPACE = uuid.UUID("cb76ab89-7cd0-4413-91a5-5d0fa74ab92f")
DEFAULT_CONTEXT_ARTIFACT = Path("artifacts/ff_v2_historical_context_2020_2025.json")
DEFAULT_ARTIFACT = Path("artifacts/ff_v2_backtest_harness_2020_2025.json")
DEFAULT_SEED = 20260828
ARTIFACT_KINDS = (
    "team_week",
    "player_week",
    "season_total",
    "roster_simulation",
)


def score_definitive_metrics(
    rows: Sequence[Mapping[str, Any]], **kwargs: Any
) -> dict[str, Any]:
    """Score frozen artifacts with the preregistered V2-005 metric policy."""

    return evaluate_metric_suite(rows, **kwargs)

# Frozen before any V2 fitting. Each instant is the end of the day immediately
# before the season opener in US Eastern time. Schedule is permitted context;
# no result or post-cutoff football observation is exposed to prediction code.
DEFAULT_PRESEASON_CUTOFFS = {
    2020: "2020-09-09T23:59:59-04:00",
    2021: "2021-09-08T23:59:59-04:00",
    2022: "2022-09-07T23:59:59-04:00",
    2023: "2023-09-06T23:59:59-04:00",
    2024: "2024-09-04T23:59:59-04:00",
    2025: "2025-09-03T23:59:59-04:00",
}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _parse_timestamp(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include an offset: {value!r}")
    return parsed


def _finite_number(value: Any, *, label: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label} must be finite")
    return number


def prediction_view(
    training_rows: Sequence[Mapping[str, Any]],
    evaluation_features: Sequence[Mapping[str, Any]],
    *,
    cutoff: str,
    evaluation_season: int,
) -> dict[str, list[dict[str, Any]]]:
    """Return the only rows prediction code may see for one fold.

    Outcome-like keys are removed from evaluation features. Training rows must
    be from earlier seasons and observed no later than the declared cutoff.
    """

    cutoff_at = _parse_timestamp(cutoff)
    cleaned_training: list[dict[str, Any]] = []
    for row in training_rows:
        season = int(row["season"])
        observed_at = _parse_timestamp(row["observedAt"])
        if season >= evaluation_season:
            raise ValueError("Training rows must precede the evaluation season")
        if observed_at > cutoff_at:
            raise ValueError("Training row is newer than the preseason cutoff")
        cleaned_training.append(dict(row))

    blocked_keys = {"outcome", "actual", "actualValue", "observedValue", "value"}
    cleaned_features: list[dict[str, Any]] = []
    for row in evaluation_features:
        if int(row["season"]) != evaluation_season:
            raise ValueError("Evaluation feature belongs to a different season")
        available_at = _parse_timestamp(row["availableAt"])
        if available_at > cutoff_at:
            raise ValueError("Evaluation feature is newer than the preseason cutoff")
        cleaned_features.append({key: value for key, value in row.items() if key not in blocked_keys})
    return {"trainingRows": cleaned_training, "evaluationFeatures": cleaned_features}


def score_artifacts(
    artifact_kind: str,
    predictions: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    *,
    cutoff: str,
    evaluation_season: int,
) -> dict[str, Any]:
    """Pair and score one supported artifact scope after prediction is frozen.

    This small point-error scorer proves that all four product scopes share one
    chronological artifact protocol. V2-005 adds the preregistered definitive
    metrics; callers may not treat this smoke score as promotion evidence.
    """

    if artifact_kind not in ARTIFACT_KINDS:
        raise ValueError(f"Unsupported artifact kind: {artifact_kind}")
    cutoff_at = _parse_timestamp(cutoff)

    prediction_map: dict[str, float] = {}
    for row in predictions:
        identity = str(row["entityId"])
        if identity in prediction_map:
            raise ValueError(f"Duplicate prediction identity: {identity}")
        if int(row["season"]) != evaluation_season:
            raise ValueError("Prediction belongs to a different evaluation season")
        if _parse_timestamp(row["availableAt"]) > cutoff_at:
            raise ValueError("Prediction used a post-cutoff artifact")
        prediction_map[identity] = _finite_number(row["value"], label="prediction")

    outcome_map: dict[str, float] = {}
    for row in outcomes:
        identity = str(row["entityId"])
        if identity in outcome_map:
            raise ValueError(f"Duplicate outcome identity: {identity}")
        if int(row["season"]) != evaluation_season:
            raise ValueError("Outcome belongs to a different evaluation season")
        if _parse_timestamp(row["observedAt"]) <= cutoff_at:
            raise ValueError("Held-out outcome must occur after the preseason cutoff")
        outcome_map[identity] = _finite_number(row["value"], label="outcome")

    if prediction_map.keys() != outcome_map.keys():
        missing_predictions = sorted(outcome_map.keys() - prediction_map.keys())
        missing_outcomes = sorted(prediction_map.keys() - outcome_map.keys())
        raise ValueError(
            f"Prediction/outcome identities differ: "
            f"missing_predictions={missing_predictions}, missing_outcomes={missing_outcomes}"
        )

    errors = [prediction_map[key] - outcome_map[key] for key in sorted(prediction_map)]
    result = {
        "artifactKind": artifact_kind,
        "evaluationSeason": evaluation_season,
        "preseasonCutoff": cutoff,
        "n": len(errors),
        "absoluteErrorTotal": round(sum(abs(error) for error in errors), 10),
        "signedErrorTotal": round(sum(errors), 10),
        "protocol": "point-error-smoke-v1",
    }
    return {**result, "scoreDigest": _digest(result)}


def _context_source_ids(context_manifest: Mapping[str, Any]) -> list[int]:
    return sorted(
        {
            int(source["sourceSnapshotId"])
            for source in context_manifest["sources"].values()
        }
    )


def _rows_by_season(rows: Iterable[Mapping[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(int(row["season"]), []).append(dict(row))
    return result


def build_run_manifest(
    db: DatabaseManager,
    context_manifest: Mapping[str, Any],
    *,
    model_version: str = "unfitted-harness-contract",
    calibration_version: str = "unfitted",
    seed: int = DEFAULT_SEED,
    cutoffs: Mapping[int, str] = DEFAULT_PRESEASON_CUTOFFS,
) -> dict[str, Any]:
    seasons = sorted(int(season) for season in context_manifest["seasons"])
    if seasons != sorted(cutoffs):
        raise ValueError(f"Cutoffs must be declared for exactly {seasons}")

    context_run_id = str(context_manifest["runId"])
    fact_rows = db.execute(
        """SELECT season, week, game_id, team, fact_digest, observed_at
             FROM ff_v2_team_week_facts
            WHERE run_id=%s
            ORDER BY season, week, game_id, team""",
        (context_run_id,),
    )
    roster_rows = db.execute(
        """SELECT season, COUNT(*) AS row_count,
                  COUNT(DISTINCT player_gsis_id) AS unique_players
             FROM ff_v2_roster_weeks
            WHERE run_id=%s
            GROUP BY season
            ORDER BY season""",
        (context_run_id,),
    )
    if not fact_rows:
        raise RuntimeError(f"No team-week facts exist for context run {context_run_id}")

    facts_by_season = _rows_by_season(fact_rows)
    roster_counts = {
        int(row["season"]): {
            "player_week": int(row["row_count"]),
            "season_total": int(row["unique_players"]),
        }
        for row in roster_rows
    }

    splits: list[dict[str, Any]] = []
    cohort_counts: dict[str, Any] = {}
    for evaluation_season in seasons:
        cutoff = cutoffs[evaluation_season]
        cutoff_at = _parse_timestamp(cutoff)
        training_seasons = [season for season in seasons if season < evaluation_season]
        training_rows = [row for season in training_seasons for row in facts_by_season.get(season, [])]
        evaluation_rows = facts_by_season.get(evaluation_season, [])
        late_training = [
            row for row in training_rows if _parse_timestamp(row["observed_at"]) > cutoff_at
        ]
        if late_training:
            raise RuntimeError(
                f"Split {evaluation_season} contains {len(late_training)} post-cutoff training rows"
            )

        training_counts = {
            "team_week": len(training_rows),
            "player_week": sum(roster_counts.get(season, {}).get("player_week", 0) for season in training_seasons),
            "season_total": sum(roster_counts.get(season, {}).get("season_total", 0) for season in training_seasons),
            "roster_simulation": 0,
        }
        evaluation_counts = {
            "team_week": len(evaluation_rows),
            "player_week": roster_counts.get(evaluation_season, {}).get("player_week", 0),
            "season_total": roster_counts.get(evaluation_season, {}).get("season_total", 0),
            "roster_simulation": 0,
        }
        split_core = {
            "evaluationSeason": evaluation_season,
            "preseasonCutoff": cutoff,
            "trainingSeasons": training_seasons,
            "trainingRowCounts": training_counts,
            "evaluationRowCounts": evaluation_counts,
            "trainingDigest": _digest([row["fact_digest"] for row in training_rows]),
            "evaluationDigest": _digest([row["fact_digest"] for row in evaluation_rows]),
            "scorable": bool(training_seasons),
            "exclusionReason": None if training_seasons else "no_prior_season_training_history",
        }
        split = {**split_core, "splitDigest": _digest(split_core)}
        splits.append(split)
        cohort_counts[str(evaluation_season)] = evaluation_counts

    config = {
        "splitStrategy": "rolling_origin_expanding_window",
        "outcomeAccess": "after_predictions_frozen",
        "contextMode": "hash_pinned_retrospective_reconstruction",
        "trainingEligibility": "season < evaluation_season and observed_at <= preseason_cutoff",
        "scoringProtocol": "artifact-kind-neutral; definitive metrics deferred to V2-005",
        "zeroRosterSimulationRows": "historical draft-decision artifacts not populated yet",
    }
    deterministic = {
        "artifactType": "fantasy-football-v2-backtest-harness",
        "schemaVersion": 1,
        "harnessVersion": HARNESS_VERSION,
        "contextRunId": context_run_id,
        "contextArtifactDigest": context_manifest["artifactDigest"],
        "modelVersion": model_version,
        "calibrationVersion": calibration_version,
        "seed": int(seed),
        "evaluationSeasons": seasons,
        "preseasonCutoffs": {str(season): cutoffs[season] for season in seasons},
        "sourceSnapshotIds": _context_source_ids(context_manifest),
        "scoringScopes": list(ARTIFACT_KINDS),
        "cohortCounts": cohort_counts,
        "config": config,
        "splits": splits,
    }
    output_digest = _digest(deterministic)
    return {
        **deterministic,
        "runId": str(uuid.uuid5(RUN_NAMESPACE, output_digest)),
        "outputDigest": output_digest,
        "createdAt": datetime.now().astimezone().isoformat(),
    }


def persist_run(db: DatabaseManager, manifest: Mapping[str, Any], artifact_path: Path) -> None:
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO ff_v2_backtest_runs
               (run_id, harness_version, status, context_run_id, model_version,
                calibration_version, seed, evaluation_seasons, preseason_cutoffs,
                source_snapshot_ids, cohort_counts, config, output_digest, artifact_path)
               VALUES (%s,%s,'complete',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (run_id) DO NOTHING""",
            (
                manifest["runId"],
                manifest["harnessVersion"],
                manifest["contextRunId"],
                manifest["modelVersion"],
                manifest["calibrationVersion"],
                manifest["seed"],
                Json(manifest["evaluationSeasons"]),
                Json(manifest["preseasonCutoffs"]),
                Json(manifest["sourceSnapshotIds"]),
                Json(manifest["cohortCounts"]),
                Json(manifest["config"]),
                manifest["outputDigest"],
                str(artifact_path),
            ),
        )
        for split in manifest["splits"]:
            cur.execute(
                """INSERT INTO ff_v2_backtest_splits
                   (run_id, evaluation_season, preseason_cutoff, training_seasons,
                    training_row_counts, evaluation_row_counts, training_digest,
                    evaluation_digest, split_digest, scorable, exclusion_reason)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (run_id, evaluation_season) DO NOTHING""",
                (
                    manifest["runId"],
                    split["evaluationSeason"],
                    split["preseasonCutoff"],
                    Json(split["trainingSeasons"]),
                    Json(split["trainingRowCounts"]),
                    Json(split["evaluationRowCounts"]),
                    split["trainingDigest"],
                    split["evaluationDigest"],
                    split["splitDigest"],
                    split["scorable"],
                    split["exclusionReason"],
                ),
            )


def run(context_artifact: Path, artifact_path: Path) -> dict[str, Any]:
    context_manifest = json.loads(context_artifact.read_text(encoding="utf-8"))
    db = DatabaseManager(load_config().database_url)
    manifest = build_run_manifest(db, context_manifest)
    persist_run(db, manifest, artifact_path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def _json_value(value: Any) -> Any:
    """Normalize JSON/driver values before exact persisted-state comparison."""

    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def verify_manifest_integrity(
    stored: Mapping[str, Any],
    rebuilt: Mapping[str, Any],
    persisted_run: Mapping[str, Any] | None,
    persisted_splits: Sequence[Mapping[str, Any]],
    *,
    artifact_path: Path,
) -> None:
    """Fail closed unless artifact replay and every persisted field agree.

    ``createdAt`` is deliberately informational. Every deterministic artifact
    field, every persisted run field, and every persisted split field is
    compared. This prevents a valid top-level digest from masking a mutated
    cutoff, cohort count, split eligibility flag, or input digest in storage.
    """

    deterministic_fields = sorted(set(stored) | set(rebuilt) - {"createdAt"})
    mismatched_artifact_fields = [
        field for field in deterministic_fields
        if field != "createdAt" and stored.get(field) != rebuilt.get(field)
    ]
    if mismatched_artifact_fields:
        raise RuntimeError(
            "Backtest harness artifact did not reproduce fields: "
            + ", ".join(mismatched_artifact_fields)
        )

    digest_payload = {
        key: stored[key]
        for key in stored
        if key not in {"runId", "outputDigest", "createdAt"}
    }
    expected_digest = _digest(digest_payload)
    expected_run_id = str(uuid.uuid5(RUN_NAMESPACE, expected_digest))
    if stored.get("outputDigest") != expected_digest or stored.get("runId") != expected_run_id:
        raise RuntimeError("Backtest artifact self-digest or deterministic run ID differs")

    if not persisted_run:
        raise RuntimeError("Persisted backtest harness run is missing")
    expected_run = {
        "run_id": stored["runId"],
        "harness_version": stored["harnessVersion"],
        "status": "complete",
        "context_run_id": stored["contextRunId"],
        "model_version": stored["modelVersion"],
        "calibration_version": stored["calibrationVersion"],
        "seed": int(stored["seed"]),
        "evaluation_seasons": stored["evaluationSeasons"],
        "preseason_cutoffs": stored["preseasonCutoffs"],
        "source_snapshot_ids": stored["sourceSnapshotIds"],
        "cohort_counts": stored["cohortCounts"],
        "config": stored["config"],
        "output_digest": stored["outputDigest"],
        "artifact_path": str(artifact_path),
    }
    run_mismatches: list[str] = []
    for field, expected in expected_run.items():
        actual = persisted_run.get(field)
        if field in {
            "evaluation_seasons", "preseason_cutoffs", "source_snapshot_ids",
            "cohort_counts", "config",
        }:
            actual = _json_value(actual)
        elif field == "seed":
            actual = int(actual)
        else:
            actual = str(actual) if actual is not None else actual
        if actual != expected:
            run_mismatches.append(field)
    if run_mismatches:
        raise RuntimeError("Persisted backtest run fields differ: " + ", ".join(run_mismatches))

    expected_by_season = {int(row["evaluationSeason"]): row for row in stored["splits"]}
    actual_by_season: dict[int, Mapping[str, Any]] = {}
    for row in persisted_splits:
        season = int(row["evaluation_season"])
        if season in actual_by_season:
            raise RuntimeError(f"Duplicate persisted backtest split identity: {season}")
        actual_by_season[season] = row
    if actual_by_season.keys() != expected_by_season.keys():
        raise RuntimeError("Persisted backtest split identities differ")

    split_field_map = {
        "preseason_cutoff": "preseasonCutoff",
        "training_seasons": "trainingSeasons",
        "training_row_counts": "trainingRowCounts",
        "evaluation_row_counts": "evaluationRowCounts",
        "training_digest": "trainingDigest",
        "evaluation_digest": "evaluationDigest",
        "split_digest": "splitDigest",
        "scorable": "scorable",
        "exclusion_reason": "exclusionReason",
    }
    for season, expected in expected_by_season.items():
        actual = actual_by_season[season]
        mismatches: list[str] = []
        if str(actual.get("run_id")) != stored["runId"]:
            mismatches.append("run_id")
        for db_field, artifact_field in split_field_map.items():
            value = actual.get(db_field)
            if db_field == "preseason_cutoff":
                try:
                    value = _parse_timestamp(value)
                except (TypeError, ValueError):
                    mismatches.append(db_field)
                    continue
            elif db_field in {"training_seasons", "training_row_counts", "evaluation_row_counts"}:
                value = _json_value(value)
            elif db_field == "scorable":
                value = bool(value)
            expected_value = expected[artifact_field]
            if db_field == "preseason_cutoff":
                expected_value = _parse_timestamp(expected_value)
            if value != expected_value:
                mismatches.append(db_field)
        if mismatches:
            raise RuntimeError(
                f"Persisted backtest split {season} fields differ: " + ", ".join(mismatches)
            )


def verify(artifact_path: Path, context_artifact: Path) -> dict[str, Any]:
    stored = json.loads(artifact_path.read_text(encoding="utf-8"))
    context_manifest = json.loads(context_artifact.read_text(encoding="utf-8"))
    db = DatabaseManager(load_config().database_url)
    cutoffs = {int(season): value for season, value in stored["preseasonCutoffs"].items()}
    rebuilt = build_run_manifest(
        db,
        context_manifest,
        model_version=stored["modelVersion"],
        calibration_version=stored["calibrationVersion"],
        seed=int(stored["seed"]),
        cutoffs=cutoffs,
    )
    persisted = db.execute_one(
        """SELECT run_id, harness_version, status, context_run_id, model_version,
                  calibration_version, seed, evaluation_seasons, preseason_cutoffs,
                  source_snapshot_ids, cohort_counts, config, output_digest, artifact_path
             FROM ff_v2_backtest_runs WHERE run_id=%s""",
        (stored["runId"],),
    )
    persisted_splits = db.execute(
        """SELECT run_id, evaluation_season, preseason_cutoff, training_seasons,
                  training_row_counts, evaluation_row_counts, training_digest,
                  evaluation_digest, split_digest, scorable, exclusion_reason
             FROM ff_v2_backtest_splits WHERE run_id=%s
             ORDER BY evaluation_season""",
        (stored["runId"],),
    )
    verify_manifest_integrity(
        stored,
        rebuilt,
        persisted,
        persisted_splits,
        artifact_path=artifact_path,
    )
    return {
        "status": "verified",
        "runId": stored["runId"],
        "outputDigest": stored["outputDigest"],
        "splitCount": len(stored["splits"]),
        "scorableSeasons": [
            split["evaluationSeason"] for split in stored["splits"] if split["scorable"]
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context-artifact", type=Path, default=DEFAULT_CONTEXT_ARTIFACT)
    parser.add_argument("--artifact", type=Path, default=DEFAULT_ARTIFACT)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    result = (
        verify(args.artifact, args.context_artifact)
        if args.verify
        else run(args.context_artifact, args.artifact)
    )
    print(json.dumps(result, indent=2, sort_keys=True))
