from __future__ import annotations

import inspect
from datetime import datetime, timezone

import pytest

from db.schema import MIGRATIONS
from model import mlb_game_total_model, mlb_moneyline_model
from model.mlb_bet_rating import record_bet
from model.mlb_prediction_provenance import (
    PROSPECTIVE,
    create_prediction_run,
    latest_prediction_snapshot_id,
    record_prediction_snapshot,
)


class FakeDb:
    def __init__(self, *, snapshot_row=None, latest_row=None) -> None:
        self.insert_sql = ""
        self.insert_params = ()
        self.snapshot_sql = ""
        self.snapshot_params = ()
        self.snapshot_row = snapshot_row
        self.latest_row = latest_row

    def execute_insert(self, sql, params):
        self.insert_sql = sql
        self.insert_params = params
        return 41

    def execute_one(self, sql, params):
        if "INSERT INTO mlb_game_prediction_snapshots" in sql:
            self.snapshot_sql = sql
            self.snapshot_params = params
            return self.snapshot_row
        return self.latest_row


def test_prediction_run_records_origin_model_and_training_cutoff() -> None:
    db = FakeDb()
    run_id = create_prediction_run(
        db,  # type: ignore[arg-type]
        model_version="mlb-total-v1",
        trained_through="2026-07-10",
        origin=PROSPECTIVE,
        source="predict_and_write",
        config={"training_games": 100},
    )
    assert run_id == 41
    assert "INSERT INTO mlb_prediction_runs" in db.insert_sql
    assert "prospective" in db.insert_params


def test_snapshot_is_insert_only_and_requires_future_commence() -> None:
    db = FakeDb(snapshot_row={"id": 73})
    snapshot_id = record_prediction_snapshot(
        db,  # type: ignore[arg-type]
        run_id=41,
        matchup_id=9,
        market="total",
        feature_values={"vegas_total": 8.5},
        raw_prediction=9.1,
        market_line=8.5,
        feature_available_at=datetime(2026, 7, 11, 17, tzinfo=timezone.utc),
    )
    assert snapshot_id == 73
    assert "INSERT INTO mlb_game_prediction_snapshots" in db.snapshot_sql
    assert "UPDATE" not in db.snapshot_sql.upper()
    assert "DELETE" not in db.snapshot_sql.upper()
    assert "commence_time IS NOT NULL" in db.snapshot_sql
    assert "%s < m.commence_time" in db.snapshot_sql


def test_snapshot_rejects_missing_or_started_matchup() -> None:
    db = FakeDb(snapshot_row=None)
    with pytest.raises(ValueError, match="snapshot rejected"):
        record_prediction_snapshot(
            db,  # type: ignore[arg-type]
            run_id=41,
            matchup_id=9,
            market="moneyline",
            feature_values={"vegas_prob_home": 0.52},
            raw_prediction=0.55,
        )


def test_latest_snapshot_lookup_is_origin_scoped() -> None:
    db = FakeDb(latest_row={"id": 91})
    assert latest_prediction_snapshot_id(
        db,  # type: ignore[arg-type]
        matchup_id=9,
        market="moneyline",
        origin=PROSPECTIVE,
    ) == 91


def test_both_live_models_write_provenance_before_cache() -> None:
    for writer in (
        mlb_game_total_model.predict_and_write,
        mlb_moneyline_model.predict_and_write,
    ):
        source = inspect.getsource(writer)
        assert "create_prediction_run" in source
        assert "record_prediction_snapshot" in source
        assert source.index("record_prediction_snapshot") < source.index("UPDATE mlb_matchups")


def test_prospective_bet_without_prediction_snapshot_fails_closed() -> None:
    assert record_bet(
        object(),  # type: ignore[arg-type]
        model_version="mlb-gameline-v3",
        bet_type="moneyline",
        scope="9",
        selection_label="NYM",
        our_prob=0.55,
        capture_key="test",
        origin=PROSPECTIVE,
        prediction_snapshot_id=None,
    ) is None


def test_database_schema_enforces_append_only_provenance() -> None:
    migration_text = "\n".join(MIGRATIONS)
    assert "mlb_prediction_runs_immutable" in migration_text
    assert "mlb_game_prediction_snapshots_immutable" in migration_text
    assert "BEFORE UPDATE OR DELETE" in migration_text
