from __future__ import annotations

from datetime import datetime, timezone

from db.queries import (
    insert_mlb_schedule_revision,
    insert_mlb_pitcher_stats_snapshot,
    insert_mlb_team_stats_snapshot,
)
from db.schema import MIGRATIONS


class CaptureDb:
    def __init__(self) -> None:
        self.sql = ""
        self.params = ()

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params or ()
        return []

    def execute_one(self, sql, params=None):
        self.sql = sql
        self.params = params or ()
        return {"id": 77}


def test_team_history_capture_is_append_only_and_source_aware() -> None:
    db = CaptureDb()
    available_at = datetime(2026, 7, 12, 16, tzinfo=timezone.utc)
    insert_mlb_team_stats_snapshot(
        db,  # type: ignore[arg-type]
        team_id=1,
        season="2026",
        snapshot_date="2026-07-12",
        team_wrc_plus=108.0,
        source="pybaseball_fangraphs_team_season",
        available_at=available_at,
        stats_through_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
        sample_size=92,
        window_label="season_to_date",
        raw_checksum="abc123",
    )
    assert "INSERT INTO mlb_team_stats_history" in db.sql
    assert "ON CONFLICT" not in db.sql.upper()
    assert "available_at" in db.sql
    assert "stats_through_at" in db.sql
    assert "raw_checksum" in db.sql
    assert "pybaseball_fangraphs_team_season" in db.params
    assert available_at in db.params


def test_pitcher_history_capture_is_append_only_and_source_aware() -> None:
    db = CaptureDb()
    insert_mlb_pitcher_stats_snapshot(
        db,  # type: ignore[arg-type]
        player_id=123,
        season="2026",
        snapshot_date="2026-07-12",
        team_id=1,
        name="Test Pitcher",
        k_per_9=9.4,
        source="mlb_stats_api_season",
        sample_size=18,
        window_label="season_to_date",
        raw_checksum="def456",
    )
    assert "INSERT INTO mlb_pitcher_stats_history" in db.sql
    assert "ON CONFLICT" not in db.sql.upper()
    assert "transformation_version" in db.sql
    assert "mlb_stats_api_season" in db.params


def test_schema_rejects_history_updates_and_deletes() -> None:
    migration_text = "\n".join(MIGRATIONS)
    assert "reject_mlb_stats_history_mutation" in migration_text
    assert "mlb_team_stats_history_immutable" in migration_text
    assert "mlb_pitcher_stats_history_immutable" in migration_text
    assert "BEFORE UPDATE OR DELETE ON mlb_team_stats_history" in migration_text
    assert "BEFORE UPDATE OR DELETE ON mlb_pitcher_stats_history" in migration_text


def test_schedule_revision_is_immutable_and_source_aware() -> None:
    db = CaptureDb()
    captured_at = datetime(2026, 7, 12, 16, tzinfo=timezone.utc)
    revision_id = insert_mlb_schedule_revision(
        db,  # type: ignore[arg-type]
        matchup_id=9,
        game_id="777001",
        revision_hash="schedulehash",
        game_date="2026-07-17",
        commence_time="2026-07-17T23:10:00Z",
        home_team_id=1,
        away_team_id=2,
        venue_id=10,
        venue_name="Test Park",
        home_sp_id=101,
        home_sp_name="Home Starter",
        home_sp_status="probable",
        away_sp_id=None,
        away_sp_name=None,
        away_sp_status="unavailable",
        game_status="Scheduled",
        source_available_at=captured_at,
        raw_json={"gamePk": 777001},
    )
    assert revision_id == 77
    assert "INSERT INTO mlb_schedule_revisions" in db.sql
    assert "mlb_stats_api_schedule" in db.sql
    assert "ON CONFLICT (game_id, revision_hash) DO NOTHING" in db.sql
    assert captured_at in db.params

    schema_text = "\n".join(MIGRATIONS)
    assert "mlb_schedule_revisions_immutable" in schema_text
    assert "BEFORE UPDATE OR DELETE ON mlb_schedule_revisions" in schema_text
