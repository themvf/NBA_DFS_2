"""Shared contracts for versioned NFL context.

This module deliberately contains no football calculations.  It is the policy
and replay boundary between evidence-derived context and its consumers.  A
consumer cannot grant itself access: qualifications are registered centrally,
and every successful read returns the exact manifest needed to replay it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Any, Iterable, Protocol


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def stable_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ContextUsage(str, Enum):
    DESCRIPTIVE = "descriptive"
    PREDICTIVE = "predictive"
    SCENARIO = "scenario"
    DECISION = "decision"


class ContextState(str, Enum):
    OBSERVED = "observed"
    ESTIMATED = "estimated"
    SCENARIO = "scenario"


class ContextReadError(RuntimeError):
    """Base class for a context dependency that cannot be resolved safely."""


class UnqualifiedContext(ContextReadError):
    pass


class MissingContext(ContextReadError):
    pass


class StaleContext(ContextReadError):
    pass


@dataclass(frozen=True)
class ContextDefinition:
    key: str
    version: str
    unit: str
    description: str
    definition: dict[str, Any]
    freshness_seconds: int | None = None

    @property
    def definition_id(self) -> str:
        return f"{self.key}@{self.version}"


@dataclass(frozen=True)
class ContextMeasurement:
    subject_type: str
    subject_id: str
    target_id: str
    definition_id: str
    as_of_at: datetime
    window: dict[str, Any]
    numerator: float | None
    denominator: float | None
    value: float | None
    state: ContextState
    coverage: dict[str, Any]
    source_snapshot_ids: tuple[str, ...]
    fact_release_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    available_at: datetime | None = None
    uncertainty: dict[str, Any] | None = None
    estimation: dict[str, Any] | None = None
    snapshot_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_at", _utc(self.as_of_at))
        object.__setattr__(
            self,
            "available_at",
            self.as_of_at if self.available_at is None else _utc(self.available_at),
        )
        if self.state == ContextState.ESTIMATED and not self.estimation:
            raise ValueError("estimated context requires estimation metadata")
        if self.state != ContextState.ESTIMATED and self.estimation:
            raise ValueError("only estimated context may carry estimation metadata")
        if self.denominator is not None and self.denominator < 0:
            raise ValueError("denominator cannot be negative")
        if not self.snapshot_id:
            body = self.as_dict(include_snapshot=False)
            object.__setattr__(self, "snapshot_id", stable_digest(body))

    def as_dict(self, *, include_snapshot: bool = True) -> dict[str, Any]:
        result = {
            "subject": {"type": self.subject_type, "id": self.subject_id},
            "targetId": self.target_id,
            "definitionId": self.definition_id,
            "asOfAt": self.as_of_at.isoformat(),
            "availableAt": self.available_at.isoformat(),
            "window": self.window,
            "measurement": {
                "numerator": self.numerator,
                "denominator": self.denominator,
                "value": self.value,
                "state": self.state.value,
            },
            "coverage": self.coverage,
            "sourceSnapshotIds": list(self.source_snapshot_ids),
            "factReleaseId": self.fact_release_id,
            "payload": self.payload,
            "uncertainty": self.uncertainty,
            "estimation": self.estimation,
        }
        if include_snapshot:
            result["contextSnapshotId"] = self.snapshot_id
        return result


@dataclass(frozen=True)
class ContextQualification:
    """Trusted registry row: definition x consumer x use case x cohort."""

    consumer_id: str
    definition_id: str
    use_case: str
    cohort: str
    usage: ContextUsage
    policy_version: str
    approved: bool
    max_age_seconds: int | None = None
    fallback_definition_id: str | None = None


@dataclass(frozen=True)
class EligibilityDecision:
    approved: bool
    consumer_id: str
    definition_id: str
    use_case: str
    cohort: str
    usage: ContextUsage
    policy_version: str
    reason: str
    fallback_used: str | None = None


@dataclass(frozen=True)
class ResolvedManifest:
    context_snapshot_id: str
    definition_id: str
    fact_release_id: str
    source_snapshot_ids: tuple[str, ...]
    policy_version: str
    eligibility: EligibilityDecision
    resolved_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class ResolvedContext:
    measurement: ContextMeasurement
    manifest: ResolvedManifest


class ContextPolicyRegistry:
    def __init__(self, qualifications: Iterable[ContextQualification]) -> None:
        self._rows: dict[tuple[str, str, str, str, ContextUsage], ContextQualification] = {}
        for row in qualifications:
            key = (row.consumer_id, row.definition_id, row.use_case, row.cohort, row.usage)
            if key in self._rows:
                raise ValueError(f"duplicate context qualification: {key}")
            self._rows[key] = row

    def require(
        self,
        *,
        consumer_id: str,
        definition_id: str,
        use_case: str,
        cohort: str,
        usage: ContextUsage,
    ) -> ContextQualification:
        key = (consumer_id, definition_id, use_case, cohort, usage)
        row = self._rows.get(key)
        if row is None or not row.approved:
            raise UnqualifiedContext(
                f"{consumer_id} is not qualified for {definition_id} "
                f"as {usage.value} in {use_case}/{cohort}"
            )
        return row


class InMemoryContextRepository:
    """Reference repository used by tests and offline research.

    Production adapters may use Postgres, but must retain these two distinct
    operations.  A pinned read never substitutes; a current read returns the
    exact object selected so the caller can freeze it.
    """

    def __init__(self, measurements: Iterable[ContextMeasurement] = ()) -> None:
        self._by_id: dict[str, ContextMeasurement] = {}
        for measurement in measurements:
            self.add(measurement)

    def add(self, measurement: ContextMeasurement) -> None:
        existing = self._by_id.get(measurement.snapshot_id)
        if existing is not None and existing != measurement:
            raise ValueError("snapshot id collision")
        self._by_id[measurement.snapshot_id] = measurement

    def pinned(self, snapshot_id: str) -> ContextMeasurement:
        try:
            return self._by_id[snapshot_id]
        except KeyError as exc:
            raise MissingContext(f"unknown context snapshot {snapshot_id}") from exc

    def current(
        self,
        *,
        definition_id: str,
        subject_id: str,
        target_id: str,
        as_of_at: datetime,
    ) -> ContextMeasurement:
        cutoff = _utc(as_of_at)
        candidates = [
            value
            for value in self._by_id.values()
            if value.definition_id == definition_id
            and value.subject_id == subject_id
            and value.target_id == target_id
            and value.as_of_at <= cutoff
            and value.available_at <= cutoff
        ]
        if not candidates:
            raise MissingContext(
                f"no eligible {definition_id} context for {subject_id}/{target_id}"
            )
        return max(candidates, key=lambda value: (value.as_of_at, value.snapshot_id))


class ContextRepository(Protocol):
    def pinned(self, snapshot_id: str) -> ContextMeasurement: ...

    def current(
        self,
        *,
        definition_id: str,
        subject_id: str,
        target_id: str,
        as_of_at: datetime,
    ) -> ContextMeasurement: ...


class ContextPolicyProvider(Protocol):
    def require(
        self,
        *,
        consumer_id: str,
        definition_id: str,
        use_case: str,
        cohort: str,
        usage: ContextUsage,
    ) -> ContextQualification: ...


class ContextReader:
    def __init__(self, repository: ContextRepository, registry: ContextPolicyProvider) -> None:
        self.repository = repository
        self.registry = registry

    def _resolve(
        self,
        measurement: ContextMeasurement,
        qualification: ContextQualification,
        *,
        requested_as_of: datetime,
        fallback_used: str | None = None,
    ) -> ResolvedContext:
        requested = _utc(requested_as_of)
        if measurement.as_of_at > requested:
            raise ContextReadError("context decision time is after the requested as-of time")
        if measurement.available_at > requested:
            raise ContextReadError("context was not published by the requested as-of time")
        if qualification.max_age_seconds is not None:
            age = (requested - measurement.as_of_at).total_seconds()
            if age > qualification.max_age_seconds:
                raise StaleContext(
                    f"{measurement.definition_id} is {int(age)}s old; "
                    f"maximum is {qualification.max_age_seconds}s"
                )
        decision = EligibilityDecision(
            approved=True,
            consumer_id=qualification.consumer_id,
            definition_id=measurement.definition_id,
            use_case=qualification.use_case,
            cohort=qualification.cohort,
            usage=qualification.usage,
            policy_version=qualification.policy_version,
            reason="qualified",
            fallback_used=fallback_used,
        )
        manifest = ResolvedManifest(
            context_snapshot_id=measurement.snapshot_id,
            definition_id=measurement.definition_id,
            fact_release_id=measurement.fact_release_id,
            source_snapshot_ids=measurement.source_snapshot_ids,
            policy_version=qualification.policy_version,
            eligibility=decision,
        )
        return ResolvedContext(measurement=measurement, manifest=manifest)

    def read_pinned(
        self,
        *,
        snapshot_id: str,
        consumer_id: str,
        use_case: str,
        cohort: str,
        usage: ContextUsage,
        requested_as_of: datetime,
    ) -> ResolvedContext:
        measurement = self.repository.pinned(snapshot_id)
        qualification = self.registry.require(
            consumer_id=consumer_id,
            definition_id=measurement.definition_id,
            use_case=use_case,
            cohort=cohort,
            usage=usage,
        )
        return self._resolve(measurement, qualification, requested_as_of=requested_as_of)

    def read_current(
        self,
        *,
        definition_id: str,
        subject_id: str,
        target_id: str,
        consumer_id: str,
        use_case: str,
        cohort: str,
        usage: ContextUsage,
        requested_as_of: datetime,
    ) -> ResolvedContext:
        qualification = self.registry.require(
            consumer_id=consumer_id,
            definition_id=definition_id,
            use_case=use_case,
            cohort=cohort,
            usage=usage,
        )
        try:
            measurement = self.repository.current(
                definition_id=definition_id,
                subject_id=subject_id,
                target_id=target_id,
                as_of_at=requested_as_of,
            )
            return self._resolve(measurement, qualification, requested_as_of=requested_as_of)
        except (MissingContext, StaleContext):
            fallback = qualification.fallback_definition_id
            if not fallback:
                raise
            fallback_qualification = self.registry.require(
                consumer_id=consumer_id,
                definition_id=fallback,
                use_case=use_case,
                cohort=cohort,
                usage=usage,
            )
            measurement = self.repository.current(
                definition_id=fallback,
                subject_id=subject_id,
                target_id=target_id,
                as_of_at=requested_as_of,
            )
            return self._resolve(
                measurement,
                fallback_qualification,
                requested_as_of=requested_as_of,
                fallback_used=fallback,
            )
