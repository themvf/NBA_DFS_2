"""PostgreSQL adapters for the shared NFL context contracts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Iterable

from db.database import DatabaseManager
from model.nfl_context_engine import (
    ContextMeasurement,
    ContextQualification,
    ContextState,
    ContextUsage,
    MissingContext,
    ResolvedContext,
    UnqualifiedContext,
    stable_digest,
)


def _measurement(row: dict[str, Any]) -> ContextMeasurement:
    return ContextMeasurement(
        snapshot_id=str(row["snapshot_id"]),
        subject_type=str(row["subject_type"]),
        subject_id=str(row["subject_id"]),
        target_id=str(row["target_id"]),
        definition_id=str(row["definition_id"]),
        as_of_at=row["as_of_at"],
        window=dict(row["measurement_window"]),
        numerator=None if row["numerator"] is None else float(row["numerator"]),
        denominator=None if row["denominator"] is None else float(row["denominator"]),
        value=None if row["value"] is None else float(row["value"]),
        state=ContextState(str(row["value_state"])),
        coverage=dict(row["coverage"]),
        uncertainty=dict(row["uncertainty"]) if row.get("uncertainty") else None,
        estimation=dict(row["estimation"]) if row.get("estimation") else None,
        source_snapshot_ids=tuple(str(value) for value in row["source_snapshot_ids"]),
        fact_release_id=str(row["fact_release_id"]),
        payload=dict(row.get("payload") or {}),
        available_at=row.get("available_at") or row["as_of_at"],
    )


class PostgresContextRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def pinned(self, snapshot_id: str) -> ContextMeasurement:
        row = self.db.execute_one(
            "SELECT * FROM nfl_context_snapshots WHERE snapshot_id=%s",
            (snapshot_id,),
        )
        if not row:
            raise MissingContext(f"unknown context snapshot {snapshot_id}")
        return _measurement(dict(row))

    def current(
        self,
        *,
        definition_id: str,
        subject_id: str,
        target_id: str,
        as_of_at: datetime,
    ) -> ContextMeasurement:
        row = self.db.execute_one(
            """
            SELECT * FROM nfl_context_snapshots
            WHERE definition_id=%s AND subject_id=%s AND target_id=%s
              AND publication_status='current' AND as_of_at<=%s
              AND available_at<=%s
            ORDER BY as_of_at DESC, snapshot_id DESC LIMIT 1
            """,
            (definition_id, subject_id, target_id, as_of_at, as_of_at),
        )
        if not row:
            raise MissingContext(
                f"no eligible {definition_id} context for {subject_id}/{target_id}"
            )
        return _measurement(dict(row))


class PostgresPolicyRegistry:
    """Resolve only the centrally activated policy for a consumer."""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def require(
        self,
        *,
        consumer_id: str,
        definition_id: str,
        use_case: str,
        cohort: str,
        usage: ContextUsage,
    ) -> ContextQualification:
        row = self.db.execute_one(
            """
            SELECT q.* FROM nfl_context_qualifications q
            JOIN nfl_consumer_policy_pointers p
              ON p.consumer_id=q.consumer_id AND p.policy_version=q.policy_version
            WHERE q.consumer_id=%s AND q.definition_id=%s AND q.use_case=%s
              AND q.cohort=%s AND q.usage=%s AND q.approved=TRUE
            LIMIT 1
            """,
            (consumer_id, definition_id, use_case, cohort, usage.value),
        )
        if not row:
            raise UnqualifiedContext(
                f"{consumer_id} is not qualified for {definition_id} "
                f"as {usage.value} in {use_case}/{cohort}"
            )
        return ContextQualification(
            consumer_id=str(row["consumer_id"]),
            definition_id=str(row["definition_id"]),
            use_case=str(row["use_case"]),
            cohort=str(row["cohort"]),
            usage=ContextUsage(str(row["usage"])),
            policy_version=str(row["policy_version"]),
            approved=bool(row["approved"]),
            max_age_seconds=(
                None if row["max_age_seconds"] is None else int(row["max_age_seconds"])
            ),
            fallback_definition_id=(
                None
                if row["fallback_definition_id"] is None
                else str(row["fallback_definition_id"])
            ),
        )


class PostgresManifestStore:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def persist(
        self,
        *,
        consumer_id: str,
        run_id: str,
        contexts: Iterable[ResolvedContext],
        model_artifact_id: str | None = None,
        model_config_id: str | None = None,
        scenario_id: str | None = None,
        resolved_at: datetime | None = None,
    ) -> str:
        values = list(contexts)
        if not values:
            raise ValueError("a consumer manifest requires at least one resolved context")
        policy_versions = {value.manifest.policy_version for value in values}
        if len(policy_versions) != 1:
            raise ValueError("one consumer run cannot mix active policy versions")
        for value in values:
            if value.manifest.eligibility.consumer_id != consumer_id:
                raise ValueError("resolved context belongs to a different consumer")
        policy_version = next(iter(policy_versions))
        context_ids = sorted({value.manifest.context_snapshot_id for value in values})
        fact_ids = sorted({value.manifest.fact_release_id for value in values})
        source_ids = sorted(
            {
                source_id
                for value in values
                for source_id in value.manifest.source_snapshot_ids
            }
        )
        eligibility = [
            {
                "approved": value.manifest.eligibility.approved,
                "consumerId": value.manifest.eligibility.consumer_id,
                "definitionId": value.manifest.eligibility.definition_id,
                "useCase": value.manifest.eligibility.use_case,
                "cohort": value.manifest.eligibility.cohort,
                "usage": value.manifest.eligibility.usage.value,
                "policyVersion": value.manifest.eligibility.policy_version,
                "reason": value.manifest.eligibility.reason,
                "fallbackUsed": value.manifest.eligibility.fallback_used,
            }
            for value in values
        ]
        fallback = [row for row in eligibility if row["fallbackUsed"] is not None]
        body = {
            "consumerId": consumer_id,
            "runId": run_id,
            "policyVersion": policy_version,
            "contextSnapshotIds": context_ids,
            "factReleaseIds": fact_ids,
            "sourceSnapshotIds": source_ids,
            "eligibility": eligibility,
            "fallback": fallback,
            "modelArtifactId": model_artifact_id,
            "modelConfigId": model_config_id,
            "scenarioId": scenario_id,
        }
        manifest_id = stable_digest(body)
        timestamp = resolved_at or datetime.now(timezone.utc)
        existing = self.db.execute_one(
            """SELECT manifest_id FROM nfl_consumer_snapshot_manifests
               WHERE consumer_id=%s AND run_id=%s""",
            (consumer_id, run_id),
        )
        if existing:
            if str(existing["manifest_id"]) != manifest_id:
                raise ValueError("consumer run already has a different frozen manifest")
            return manifest_id
        self.db.execute(
            """
            INSERT INTO nfl_consumer_snapshot_manifests
                (manifest_id, consumer_id, run_id, policy_version, resolved_at,
                 context_snapshot_ids, fact_release_ids, source_snapshot_ids,
                 eligibility_decisions, fallback_decisions, model_artifact_id,
                 model_config_id, scenario_id)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,
                    %s::jsonb,%s,%s,%s)
            """,
            (
                manifest_id,
                consumer_id,
                run_id,
                policy_version,
                timestamp,
                json.dumps(context_ids),
                json.dumps(fact_ids),
                json.dumps(source_ids),
                json.dumps(eligibility),
                json.dumps(fallback),
                model_artifact_id,
                model_config_id,
                scenario_id,
            ),
        )
        return manifest_id
