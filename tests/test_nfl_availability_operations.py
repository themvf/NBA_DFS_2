from datetime import datetime, timedelta, timezone

import pytest

from ingest.nfl_availability_operations import freeze_prelock, should_capture
from ingest.nfl_official_inactives import prepare_import


NOW = datetime(2026, 9, 27, 15, tzinfo=timezone.utc)


class ReviewDb:
    def execute_one(self, sql, params=None):
        if "nfl_season_games" in sql:
            return {
                "game_id": "2026_03_ARI_SF",
                "kickoff": NOW + timedelta(hours=2),
                "home_team": "SF",
                "away_team": "ARI",
            }
        if "ff_players" in sql:
            return {"id": 10, "team_abbrev": "ARI"}
        raise AssertionError(sql)


def payload(status="INACTIVE"):
    return {
        "season": 2026,
        "week": 3,
        "source_label": "reviewed league inactive report",
        "source_published_at": (NOW - timedelta(minutes=5)).isoformat(),
        "records": [
            {"game_id": "2026_03_ARI_SF", "player_id": 10, "status": status},
        ],
    }


def test_cadence_is_hourly_near_kickoff_and_two_hourly_otherwise():
    odd_hour = NOW.replace(hour=15)
    even_hour = NOW.replace(hour=14)
    assert should_capture(odd_hour, [odd_hour + timedelta(hours=5)]) is True
    assert should_capture(odd_hour, [odd_hour + timedelta(hours=8)]) is False
    assert should_capture(even_hour, [even_hour + timedelta(hours=8)]) is True


def test_reviewed_inactive_contract_resolves_exact_game_and_player():
    prepared = prepare_import(ReviewDb(), payload(), reviewed_by="operator@example.test", reviewed_at=NOW)
    assert prepared["records"][0]["playerId"] == 10
    assert prepared["records"][0]["report_type"] == "inactive_list"
    assert prepared["records"][0]["kickoff"].startswith("2026-09-27")
    assert len(prepared["importId"]) == 64


def test_reviewed_import_never_infers_active_from_list_omission():
    with pytest.raises(ValueError, match="INACTIVE"):
        prepare_import(ReviewDb(), payload("ACTIVE"), reviewed_by="operator", reviewed_at=NOW)


def test_review_after_kickoff_is_rejected():
    with pytest.raises(ValueError, match="not pre-kickoff"):
        prepare_import(
            ReviewDb(), payload(), reviewed_by="operator",
            reviewed_at=NOW + timedelta(hours=3),
        )


def test_reviewer_identity_is_mandatory():
    with pytest.raises(ValueError, match="reviewed_by"):
        prepare_import(ReviewDb(), payload(), reviewed_by=" ", reviewed_at=NOW)


class FreezeDb:
    def __init__(self):
        self.inserts = []

    def execute(self, sql, params=None):
        if "FROM nfl_season_games" in sql:
            return [{"game_id": "2026_03_ARI_SF", "kickoff": NOW + timedelta(minutes=60)}]
        if "FROM nfl_context_snapshots" in sql:
            return [
                {"snapshot_id": "player-snap", "source_snapshot_ids": [101]},
                {"snapshot_id": "qb-snap", "source_snapshot_ids": [101, 102]},
            ]
        if "INSERT INTO nfl_availability_prelock_manifests" in sql:
            self.inserts.append(params)
            return []
        raise AssertionError(sql)

    def execute_one(self, sql, params=None):
        if "FROM nfl_dfs_projection_runs" in sql:
            return {"run_id": "00000000-0000-0000-0000-000000000001"}
        raise AssertionError(sql)


def test_prelock_manifest_freezes_saved_context_ids_without_reresolving():
    db = FreezeDb()
    manifests = freeze_prelock(db, season=2026, week=3, now=NOW)
    assert len(manifests) == 1
    assert manifests[0]["contextSnapshotIds"] == ["player-snap", "qb-snap"]
    assert manifests[0]["sourceSnapshotIds"] == ["101", "102"]
    assert manifests[0]["projectionRunId"].endswith("0001")
    assert len(db.inserts) == 1
