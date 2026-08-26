from datetime import datetime, timezone

from ingest.ff_injuries import (
    normalize_injury_observation,
    normalize_injury_status,
    persist_injury_observation,
)


class InjuryDatabase:
    def __init__(self) -> None:
        self.observations: dict[tuple[int, int, str], dict] = {}
        self.active_injury: dict | None = None
        self.events: list[tuple] = []
        self.player_status: str | None = None
        self.now = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)

    def execute_one(self, sql: str, params: tuple):
        compact = " ".join(sql.split())
        if compact.startswith("INSERT INTO ff_player_injury_observations"):
            key = (params[3], params[0], params[2])
            if key in self.observations:
                return None
            row = {"id": len(self.observations) + 1, "observed_at": self.now}
            self.observations[key] = row
            return row
        if compact.startswith("SELECT id,observed_at FROM ff_player_injury_observations"):
            return self.observations.get((params[0], params[1], params[2]))
        if compact.startswith("SELECT * FROM ff_player_injuries"):
            return self.active_injury if self.active_injury and self.active_injury["active"] else None
        if compact.startswith("INSERT INTO ff_player_injuries"):
            self.active_injury = {
                "id": 10,
                "status": params[2],
                "body_part": params[3],
                "injury_type": params[4],
                "expected_return_min": params[7],
                "expected_return_max": params[8],
                "weeks_out_min": params[9],
                "weeks_out_max": params[10],
                "estimate_basis": params[11],
                "primary_source": params[13],
                "source_conflict": False,
                "active": True,
            }
            return {"id": 10}
        raise AssertionError(f"Unexpected execute_one SQL: {compact}")

    def execute(self, sql: str, params: tuple):
        compact = " ".join(sql.split())
        if compact.startswith("INSERT INTO ff_injury_events"):
            if not any(event[-1] == params[-1] for event in self.events):
                self.events.append(params)
            return []
        if compact.startswith("UPDATE ff_players SET injury_status=NULL"):
            self.player_status = None
            return []
        if compact.startswith("UPDATE ff_players SET injury_status=%s"):
            self.player_status = params[0]
            return []
        if compact.startswith("UPDATE ff_player_injuries SET active=FALSE"):
            assert self.active_injury is not None
            self.active_injury["active"] = False
            return []
        if compact.startswith("UPDATE ff_player_injuries SET status=%s"):
            assert self.active_injury is not None
            self.active_injury["status"] = params[0]
            return []
        raise AssertionError(f"Unexpected execute SQL: {compact}")


def test_status_normalization_covers_healthy_and_reserve_designations() -> None:
    assert normalize_injury_status(None) == "HEALTHY"
    assert normalize_injury_status("Active") == "HEALTHY"
    assert normalize_injury_status("Q") == "QUESTIONABLE"
    assert normalize_injury_status("Injured Reserve") == "IR"
    assert normalize_injury_status("Reserve/PUP") == "PUP"


def test_rich_fantasypros_fields_are_normalized_without_modeling_them() -> None:
    result = normalize_injury_observation("fantasypros", {
        "status": "Out",
        "body_part": "Knee",
        "comment": "Expected to miss multiple games",
        "practice_status": "DNP",
        "ir_weeks": 4,
        "playing_probability": 15,
        "updated_at": "2026-08-26T10:15:00Z",
    })
    assert result.normalized_status == "OUT"
    assert result.body_part == "Knee"
    assert result.description == "Expected to miss multiple games"
    assert result.practice_status == "DNP"
    assert result.weeks_out_min == 4
    assert result.weeks_out_max == 4
    assert result.availability_probability == 0.15
    assert result.estimate_basis == "provider"


def test_new_injury_is_idempotent_and_clearance_closes_episode() -> None:
    db = InjuryDatabase()
    first = persist_injury_observation(
        db,
        player_id=42,
        season=2026,
        source="sleeper",
        source_snapshot_id=100,
        row={"injury_status": "Out", "injury_body_part": "Hamstring"},
    )
    duplicate = persist_injury_observation(
        db,
        player_id=42,
        season=2026,
        source="sleeper",
        source_snapshot_id=100,
        row={"injury_status": "Out", "injury_body_part": "Hamstring"},
    )
    cleared = persist_injury_observation(
        db,
        player_id=42,
        season=2026,
        source="sleeper",
        source_snapshot_id=101,
        row={"injury_status": None},
    )

    assert first["event"] == "NEW_INJURY"
    assert duplicate == {"observation_id": 1, "event": None, "duplicate": True}
    assert cleared["event"] == "CLEARED"
    assert [event[3] for event in db.events] == ["NEW_INJURY", "CLEARED"]
    assert db.active_injury is not None and db.active_injury["active"] is False
    assert db.player_status is None


def test_shadow_observation_does_not_change_canonical_status() -> None:
    db = InjuryDatabase()
    result = persist_injury_observation(
        db,
        player_id=42,
        season=2026,
        source="fantasypros",
        source_snapshot_id=200,
        row={"status": "Out", "ir_weeks": 3},
        reconcile_current=False,
    )
    assert result["event"] is None
    assert db.active_injury is None
    assert db.player_status is None
