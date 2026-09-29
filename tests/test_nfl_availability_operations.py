from datetime import datetime, timedelta, timezone

import pytest

from ingest.nfl_availability_operations import (
    LIVE_DATASET_PREFIX,
    availability_health,
    freeze_prelock,
    should_capture,
)
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


class HealthDb:
    """Answers availability_health's queries from in-memory rows.

    Snapshot selection honours the SQL's own filters (source, season and, when
    present, the ``dataset LIKE`` prefix), so the test exercises the query the
    monitor actually sends rather than a hand-picked answer.
    """

    def __init__(self, snapshots, *, kickoff, unresolved=None):
        self.snapshots = snapshots
        self.kickoff = kickoff
        self.unresolved = unresolved or []
        self.inserts = []

    def _snapshots(self, sql, params):
        rows = [row for row in self.snapshots if row["season"] == params[0]]
        rest = list(params[1:])
        if "dataset LIKE" in sql:
            prefix = rest.pop(0).rstrip("%")
            rows = [row for row in rows if row["dataset"].startswith(prefix)]
        if "fetched_at<=" in sql:
            bound = rest.pop(0)
            rows = [row for row in rows if row["fetched_at"] <= bound]
        if "id<>COALESCE" in sql:
            excluded = rest.pop(0)
            rows = [row for row in rows if row["id"] != excluded]
        return sorted(rows, key=lambda row: (row["fetched_at"], row["id"]), reverse=True)

    def execute(self, sql, params=None):
        if "FROM nfl_season_games" in sql:
            return [{"game_id": f"2026_04_G{i}", "kickoff": self.kickoff} for i in range(16)]
        if "FROM nfl_context_snapshots" in sql:
            rows = []
            for game in params[0]:
                for team in ("AAA", "BBB"):
                    rows.append({"definition_id": "player_game_availability@v1", "target_id": game,
                                 "payload": {"team": team, "resolved_availability_state": "EXPECTED_ACTIVE"}})
                    rows.append({"definition_id": "team_qb_state@v1", "target_id": game,
                                 "payload": {"team": team}})
            return rows
        if "INSERT INTO nfl_availability_operation_runs" in sql:
            self.inserts.append(params)
            return []
        raise AssertionError(sql)

    def execute_one(self, sql, params=None):
        if "FROM ff_source_snapshots" in sql:
            rows = self._snapshots(sql, params)
            return rows[0] if rows else None
        if "ff_player_injury_observations" in sql:
            return {"observations": 0, "games": 0, "prelock_games": 0}
        if "FROM nfl_dfs_projection_runs" in sql:
            return {"run_id": "00000000-0000-0000-0000-000000000009", "unresolved": self.unresolved}
        raise AssertionError(sql)


def snap(id_, dataset, fetched_at, matched):
    return {"id": id_, "season": 2026, "dataset": dataset, "fetched_at": fetched_at,
            "row_count": 9422, "matched_count": matched, "unmatched_count": 9422 - matched,
            "status": "success"}


def test_monitor_ignores_the_fantasy_football_roster_snapshot():
    """The 2026-09-29 07:11 UTC case: ff_independent wrote dataset='players'
    (matched_count=0) at 06:57, and the monitor took it for the live feed."""
    evaluated = datetime(2026, 9, 29, 7, 11, tzinfo=timezone.utc)
    db = HealthDb([
        snap(5086, "players-live-2026-2026092904", datetime(2026, 9, 29, 4, 7, tzinfo=timezone.utc), 1060),
        snap(5091, "players-live-2026-2026092906", datetime(2026, 9, 29, 6, 7, tzinfo=timezone.utc), 1060),
        snap(5093, "players", datetime(2026, 9, 29, 6, 57, tzinfo=timezone.utc), 0),
        # Captured after the evaluation time: invisible to a replay at 07:11.
        snap(5113, "players-live-2026-2026092912", datetime(2026, 9, 29, 12, 7, tzinfo=timezone.utc), 1060),
    ], kickoff=datetime(2026, 10, 2, 0, 15, tzinfo=timezone.utc))
    report = availability_health(db, season=2026, week=4, now=evaluated)
    assert report["latestSleeperSnapshotId"] == 5091
    assert report["latestSleeperDataset"].startswith(LIVE_DATASET_PREFIX)
    assert report["status"] == "healthy", report["alerts"]
    assert len(db.inserts) == 1


def test_a_stale_live_feed_is_not_hidden_by_a_fresh_roster_snapshot():
    evaluated = datetime(2026, 9, 29, 7, 11, tzinfo=timezone.utc)
    db = HealthDb([
        snap(5000, "players-live-2026-2026092812", datetime(2026, 9, 28, 12, 7, tzinfo=timezone.utc), 1060),
        snap(5093, "players", datetime(2026, 9, 29, 6, 57, tzinfo=timezone.utc), 0),
    ], kickoff=datetime(2026, 10, 2, 0, 15, tzinfo=timezone.utc))
    report = availability_health(db, season=2026, week=4, now=evaluated, persist=False)
    codes = {alert["code"] for alert in report["alerts"]}
    assert "sleeper_stale" in codes
    assert "implausible_rows" not in codes
    assert report["status"] == "critical"
    assert db.inserts == [], "persist=False must not record an operation run"


def test_a_starter_out_without_a_promoted_backup_is_reported():
    evaluated = datetime(2026, 9, 29, 8, 16, tzinfo=timezone.utc)
    db = HealthDb(
        [snap(5107, "players-live-2026-2026092908", datetime(2026, 9, 29, 8, 7, tzinfo=timezone.utc), 1060)],
        kickoff=datetime(2026, 10, 2, 0, 15, tzinfo=timezone.utc),
        unresolved=[
            {"player_id": 7, "player": "Starter QB", "team": "CHI", "position": "QB",
             "starter_evidence": "depth_chart", "reason": "no verified starter-to-backup promotion"},
            # A ruled-out backup has nothing to promote: not an alert.
            {"player_id": 8, "player": "Third QB", "team": "CHI", "position": "QB",
             "starter_evidence": None, "reason": "no verified starter-to-backup promotion"},
        ])
    report = availability_health(db, season=2026, week=4, now=evaluated, persist=False)
    alerts = {alert["code"]: alert for alert in report["alerts"]}
    assert alerts["starter_promotion_unresolved"]["severity"] == "warning"
    assert "Starter QB (CHI)" in alerts["starter_promotion_unresolved"]["message"]
    assert [row["player_id"] for row in report["unresolvedStarters"]] == [7]
    assert report["status"] == "warning"


def test_prelock_manifest_freezes_saved_context_ids_without_reresolving():
    db = FreezeDb()
    manifests = freeze_prelock(db, season=2026, week=3, now=NOW)
    assert len(manifests) == 1
    assert manifests[0]["contextSnapshotIds"] == ["player-snap", "qb-snap"]
    assert manifests[0]["sourceSnapshotIds"] == ["101", "102"]
    assert manifests[0]["projectionRunId"].endswith("0001")
    assert len(db.inserts) == 1
