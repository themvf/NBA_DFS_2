from datetime import datetime, timezone

import pytest

from model.nfl_context_engine import (
    ContextReader,
    ContextUsage,
)
from model.nfl_context_store import (
    PostgresContextRepository,
    PostgresManifestStore,
    PostgresPolicyRegistry,
)


NOW = datetime(2026, 9, 24, tzinfo=timezone.utc)


class FakeDb:
    def __init__(self, rows):
        self.rows = rows
        self.inserts = []

    def execute_one(self, sql, params=None):
        if "nfl_context_qualifications" in sql:
            return self.rows.get("policy")
        if "nfl_context_snapshots" in sql:
            return self.rows.get("snapshot")
        if "nfl_consumer_snapshot_manifests" in sql:
            return self.rows.get("manifest")
        raise AssertionError(sql)

    def execute(self, sql, params=None):
        self.inserts.append((sql, params))
        return []


def snapshot_row():
    return {
        "snapshot_id": "snap-1",
        "definition_id": "pace@v1",
        "subject_type": "team",
        "subject_id": "NYG",
        "target_id": "game",
        "as_of_at": NOW,
        "measurement_window": {"games": ["g1"]},
        "numerator": 60,
        "denominator": 2,
        "value": 30,
        "value_state": "observed",
        "coverage": {"minimumCoverageMet": True},
        "uncertainty": None,
        "estimation": None,
        "source_snapshot_ids": ["source-1"],
        "fact_release_id": "facts-1",
    }


def policy_row():
    return {
        "consumer_id": "consumer",
        "definition_id": "pace@v1",
        "use_case": "explain",
        "cohort": "all",
        "usage": "descriptive",
        "policy_version": "policy-v1",
        "approved": True,
        "max_age_seconds": None,
        "fallback_definition_id": None,
    }


def resolved_context():
    db = FakeDb({"snapshot": snapshot_row(), "policy": policy_row()})
    return ContextReader(PostgresContextRepository(db), PostgresPolicyRegistry(db)).read_pinned(
        snapshot_id="snap-1",
        consumer_id="consumer",
        use_case="explain",
        cohort="all",
        usage=ContextUsage.DESCRIPTIVE,
        requested_as_of=NOW,
    )


def test_postgres_adapters_resolve_the_shared_contract() -> None:
    db = FakeDb({"snapshot": snapshot_row(), "policy": policy_row()})
    reader = ContextReader(PostgresContextRepository(db), PostgresPolicyRegistry(db))
    result = reader.read_current(
        definition_id="pace@v1",
        subject_id="NYG",
        target_id="game",
        consumer_id="consumer",
        use_case="explain",
        cohort="all",
        usage=ContextUsage.DESCRIPTIVE,
        requested_as_of=NOW,
    )
    assert result.measurement.value == 30
    assert result.manifest.context_snapshot_id == "snap-1"


def test_manifest_is_frozen_and_idempotent() -> None:
    db = FakeDb({"manifest": None})
    resolved = resolved_context()
    store = PostgresManifestStore(db)
    manifest_id = store.persist(
        consumer_id="consumer",
        run_id="run-1",
        contexts=[resolved],
        resolved_at=NOW,
    )
    assert len(db.inserts) == 1
    db.rows["manifest"] = {"manifest_id": manifest_id}
    assert store.persist(
        consumer_id="consumer",
        run_id="run-1",
        contexts=[resolved],
        resolved_at=NOW,
    ) == manifest_id
    assert len(db.inserts) == 1


def test_manifest_refuses_to_rewrite_an_existing_run() -> None:
    db = FakeDb({"manifest": {"manifest_id": "different"}})
    with pytest.raises(ValueError, match="different frozen manifest"):
        PostgresManifestStore(db).persist(
            consumer_id="consumer",
            run_id="run-1",
            contexts=[resolved_context()],
        )
