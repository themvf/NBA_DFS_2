from datetime import datetime, timezone

import pytest

from ingest.ff_source_contracts import (
    AsOfCutoffError,
    SOURCE_CONTRACTS,
    SnapshotProvenance,
    assert_as_of_eligible,
    persist_source_snapshot,
    select_fallback,
    validate_source_contract_registry,
)


UTC = timezone.utc


def _snapshot(**overrides) -> SnapshotProvenance:
    values = {
        "source": "nflverse",
        "dataset": "weekly-player-stats",
        "contract_key": "weekly-stats",
        "season": 2025,
        "week": 1,
        "response_hash": "a" * 64,
        "row_count": 500,
        "fetched_at": datetime(2025, 8, 20, tzinfo=UTC),
        "source_published_at": datetime(2025, 8, 19, tzinfo=UTC),
        "as_of_at": datetime(2025, 8, 25, tzinfo=UTC),
    }
    values.update(overrides)
    return SnapshotProvenance(**values)


def test_required_contract_registry_is_complete() -> None:
    validate_source_contract_registry()
    assert {
        "weekly-rosters", "weekly-stats", "play-by-play",
        "participation", "schedule", "transactions",
    }.issubset(SOURCE_CONTRACTS)
    assert all(contract.license for contract in SOURCE_CONTRACTS.values())
    assert all(contract.required_fields for contract in SOURCE_CONTRACTS.values())


def test_historical_cutoff_accepts_only_pre_cutoff_availability() -> None:
    cutoff = datetime(2025, 8, 25, tzinfo=UTC)
    eligible = assert_as_of_eligible(
        as_of_at=cutoff,
        source_published_at=datetime(2025, 8, 20, tzinfo=UTC),
        fetched_at=datetime(2026, 8, 28, tzinfo=UTC),
    )
    assert eligible == datetime(2025, 8, 20, tzinfo=UTC)

    with pytest.raises(AsOfCutoffError):
        assert_as_of_eligible(
            as_of_at=cutoff,
            source_published_at=datetime(2025, 9, 2, tzinfo=UTC),
            fetched_at=datetime(2025, 9, 2, tzinfo=UTC),
        )


def test_missing_publish_time_uses_conservative_fetch_time() -> None:
    with pytest.raises(AsOfCutoffError):
        assert_as_of_eligible(
            as_of_at=datetime(2025, 8, 25, tzinfo=UTC),
            source_published_at=None,
            fetched_at=datetime(2026, 8, 28, tzinfo=UTC),
        )


def test_missing_sources_choose_explicit_nonzero_fallback_contract() -> None:
    required = SOURCE_CONTRACTS.keys()
    assert select_fallback(required, required).tier == "A"

    tier_b = select_fallback(required, set(required) - {"participation"})
    assert tier_b.tier == "B"
    assert tier_b.confidence_multiplier == 0.8
    assert tier_b.missing_sources == ("participation",)

    tier_c = select_fallback(required, set(required) - {"play-by-play"})
    assert tier_c.tier == "C"
    assert tier_c.confidence_multiplier == 0.6
    assert tier_c.missing_sources == ("play-by-play",)


def test_missingness_cannot_claim_full_tier_a_confidence() -> None:
    with pytest.raises(ValueError, match="missing inputs"):
        _snapshot(missingness={"participation": "missing"})

    degraded = _snapshot(
        missingness={"participation": "missing"},
        fallback_tier="B",
        confidence_multiplier=0.8,
    )
    assert degraded.available_at == datetime(2025, 8, 19, tzinfo=UTC)


def test_failed_snapshot_cannot_be_model_eligible() -> None:
    with pytest.raises(ValueError, match="failed snapshots"):
        _snapshot(status="failed", model_eligible=True)


class FakeDatabase:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def execute_one(self, statement: str, params: tuple):
        self.calls.append((statement, params))
        return {"id": 42}


def test_snapshot_persistence_is_hash_idempotent_without_mutation() -> None:
    db = FakeDatabase()
    assert persist_source_snapshot(db, _snapshot()) == 42
    statement, params = db.calls[0]
    assert "ON CONFLICT(source,dataset,response_hash) DO NOTHING" in statement
    assert "DO UPDATE" not in statement
    assert params[0:3] == ("nflverse", "weekly-player-stats", 2025)
    assert params[13] == "a" * 64
