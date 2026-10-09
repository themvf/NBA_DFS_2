"""Pure policy resolver used by the CFB context service boundary.

Authentication stays outside this module: callers must supply the consumer ID
derived from service identity, never a request-body override.  The resolver
uses exact registered versions and produces auditable denial reasons.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import product
from typing import Callable, Iterable, Mapping


@dataclass(frozen=True)
class Candidate:
    snapshot_id: str
    manifest_id: str
    definition_id: str
    definition_version: int
    subject_key: str
    target_event_key: str | None
    scenario_key: str
    as_of_at: datetime
    created_at: datetime
    source_age_seconds: float | None
    origin: str
    coverage_state: str
    invalidated: bool = False


@dataclass(frozen=True)
class Slot:
    name: str
    required: bool
    versions: tuple[tuple[str, int], ...]
    max_context_age_seconds: int
    max_source_age_seconds: int


@dataclass(frozen=True)
class Step:
    index: int
    action: str
    slots: tuple[Slot, ...] = ()
    baseline_manifest_id: str | None = None


@dataclass(frozen=True)
class Resolution:
    result: str
    step_index: int | None
    manifest_ids: tuple[str, ...]
    snapshot_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]


def _eligible(
    candidate: Candidate, slot: Slot, *, requested_as_of: datetime,
    freshness_at: datetime, allowed_origins: frozenset[str], subject_key: str,
    target_event_key: str | None, scenario_key: str,
) -> bool:
    if candidate.invalidated or candidate.coverage_state not in {"complete", "partial"}:
        return False
    if candidate.origin not in allowed_origins or candidate.subject_key != subject_key:
        return False
    if candidate.target_event_key != target_event_key or candidate.scenario_key != scenario_key:
        return False
    if candidate.as_of_at > requested_as_of:
        return False
    context_age = (freshness_at - candidate.as_of_at).total_seconds()
    if context_age < 0 or context_age > slot.max_context_age_seconds:
        return False
    if candidate.source_age_seconds is None or candidate.source_age_seconds < 0:
        return False
    if candidate.source_age_seconds > slot.max_source_age_seconds:
        return False
    return (candidate.definition_id, candidate.definition_version) in slot.versions


def resolve_current(
    *, steps: Iterable[Step], candidates: Iterable[Candidate], requested_as_of: datetime,
    evaluation_at: datetime, live_request: bool, allowed_origins: Iterable[str],
    subject_key: str, target_event_key: str | None, scenario_key: str,
    compatible: Callable[[Mapping[str, Candidate]], bool] | None = None,
) -> Resolution:
    """Resolve exact dependency versions in deterministic policy order."""
    allowed = frozenset(allowed_origins)
    pool = tuple(candidates)
    freshness_at = evaluation_at if live_request else requested_as_of
    rejected_reasons: set[str] = set()
    for step in sorted(steps, key=lambda item: item.index):
        if step.action == "deny":
            return Resolution("deny", step.index, (), (), tuple(sorted(rejected_reasons | {"policy_deny_step"})))
        if step.action == "pinned_baseline":
            if step.baseline_manifest_id is None:
                return Resolution("deny", step.index, (), (), ("invalid_baseline_step",))
            return Resolution("fallback", step.index, (step.baseline_manifest_id,), (), tuple(sorted(rejected_reasons)))
        if step.action != "resolve":
            return Resolution("deny", step.index, (), (), ("unsupported_policy_action",))
        ranked_by_slot: list[tuple[Slot, list[Candidate | None]]] = []
        for slot in step.slots:
            preference = {version: index for index, version in enumerate(slot.versions)}
            eligible = [candidate for candidate in pool if _eligible(
                candidate, slot, requested_as_of=requested_as_of, freshness_at=freshness_at,
                allowed_origins=allowed, subject_key=subject_key,
                target_event_key=target_event_key, scenario_key=scenario_key,
            )]
            eligible.sort(key=lambda candidate: (
                preference[(candidate.definition_id, candidate.definition_version)],
                -candidate.as_of_at.timestamp(), -candidate.created_at.timestamp(), candidate.snapshot_id,
            ))
            if not eligible and slot.required:
                rejected_reasons.add(f"required_slot_unavailable:{slot.name}")
                ranked_by_slot = []
                break
            ranked_by_slot.append((slot, eligible if eligible else [None]))
        if not ranked_by_slot and step.slots:
            continue
        for combination in product(*(items for _, items in ranked_by_slot)):
            selected = {slot.name: candidate for (slot, _), candidate in zip(ranked_by_slot, combination) if candidate is not None}
            if compatible is not None and not compatible(selected):
                rejected_reasons.add("incompatible_combination")
                continue
            values = tuple(selected[name] for name in sorted(selected))
            return Resolution("allow" if step.index == 0 else "fallback", step.index,
                              tuple(value.manifest_id for value in values),
                              tuple(value.snapshot_id for value in values), tuple(sorted(rejected_reasons)))
    return Resolution("deny", None, (), (), tuple(sorted(rejected_reasons | {"no_eligible_resolution"})))


def resolve_pinned(manifest_id: str, *, replay_available: bool, invalidated: bool) -> Resolution:
    """Pinned research reads never substitute or search for a replacement."""
    if not replay_available:
        return Resolution("replay_unavailable", None, (manifest_id,), (), ("retained_evidence_unavailable",))
    reasons = ("manifest_invalidated",) if invalidated else ()
    return Resolution("pinned", None, (manifest_id,), (), reasons)
