"""Bounded detector-funnel accounting for CFB contract revision 3."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Iterable, Mapping


REJECTION_PRECEDENCE = (
    "identity_schedule", "unsupported_market_selection", "missing_endpoint",
    "missing_required_fields", "invalid_quote", "stale_quote",
    "insufficient_book_intersection", "temporal_origin_violation",
)


@dataclass(frozen=True)
class Funnel:
    candidate_count: int
    eligible_count: int
    below_threshold_count: int
    matched_count: int
    deduped_count: int
    persisted_count: int
    failed_persistence_count: int
    rejection_counts: dict[str, int]
    rejection_samples: dict[str, tuple[str, ...]]


def summarize_opportunities(opportunities: Iterable[Mapping[str, object]]) -> Funnel:
    """Partition opportunities once, with at most three stable samples/reason."""
    rows = list(opportunities)
    rejection_counts: dict[str, int] = {}
    sample_keys: dict[str, list[str]] = {}
    eligible = below = matched = deduped = persisted = failed = 0
    for row in rows:
        flags = set(row.get("rejection_reasons") or ())
        primary = next((reason for reason in REJECTION_PRECEDENCE if reason in flags), None)
        if primary is None and flags:
            primary = sorted(flags)[0]
        if primary:
            rejection_counts[primary] = rejection_counts.get(primary, 0) + 1
            key = str(row.get("opportunity_key") or sha256(repr(sorted(row.items())).encode()).hexdigest())
            sample_keys.setdefault(primary, []).append(key)
            continue
        eligible += 1
        if not row.get("threshold_match"):
            below += 1
            continue
        matched += 1
        state = row.get("persistence_state")
        if state == "deduped":
            deduped += 1
        elif state == "persisted":
            persisted += 1
        elif state == "failed":
            failed += 1
        else:
            raise ValueError("matched opportunity requires a persistence_state")
    if len(rows) != sum(rejection_counts.values()) + eligible or eligible != below + matched or matched != deduped + persisted + failed:
        raise AssertionError("detector funnel partition invariant failed")
    samples = {reason: tuple(sorted(keys)[:3]) for reason, keys in sample_keys.items()}
    return Funnel(len(rows), eligible, below, matched, deduped, persisted, failed, rejection_counts, samples)
