"""A run built after a kickoff must not republish (or supersede) that game's contexts."""
from datetime import datetime, timedelta, timezone

from ingest.nfl_availability_context_publish import persist_availability_contexts

UTC = timezone.utc
AS_OF = datetime(2026, 9, 27, 18, 0, tzinfo=UTC)
EARLY_KICKOFF = datetime(2026, 9, 27, 17, 0, tzinfo=UTC)   # started an hour ago
LATE_KICKOFF = datetime(2026, 9, 27, 20, 25, tzinfo=UTC)   # still pregame


class RecordingCursor:
    def __init__(self):
        self.calls = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))


def player(pid, game_id, team, kickoff, position="QB", depth=1):
    return {"player_id": pid, "game_id": game_id, "event_id": game_id, "team": team,
            "position": position, "depth_order": depth, "commence_time": kickoff}


def decision(state, kickoff):
    return {"state": state, "projection_status": "OUT" if state == "OUT_CONFIRMED" else None,
            "kickoff": kickoff.isoformat(), "qualifying_observation_ids": [7],
            "display_only_observation_ids": [], "qualifying_source_snapshot_ids": [70],
            "display_only_source_snapshot_ids": [], "reason": "test"}


def manifest(projections, as_of=AS_OF):
    return {
        "season": 2026, "week": 3, "model_version": "test", "as_of_at": as_of.isoformat(),
        "availability_decisions": {
            str(row["player_id"]): decision("OUT_CONFIRMED" if row["player_id"] == 1 else "EXPECTED_ACTIVE",
                                            row["commence_time"])
            for row in projections
        },
    }


def game_ids_written(cursor):
    """Every game id that reached an INSERT/UPDATE of nfl_context_snapshots."""
    touched = set()
    for sql, params in cursor.calls:
        if "INSERT INTO nfl_context_snapshots" in sql:
            touched.add(params[4])            # target_id
        elif "UPDATE nfl_context_snapshots" in sql and "'withdrawn'" in sql:
            touched.update(params[1])         # target_id=ANY(...)
        elif "UPDATE nfl_context_snapshots" in sql and "'superseded'" in sql:
            touched.add(params[3])            # target_id
    return touched


def test_started_games_keep_their_pregame_contexts():
    projections = [
        player(1, "2026_03_EARLY", "AAA", EARLY_KICKOFF),
        player(2, "2026_03_EARLY", "BBB", EARLY_KICKOFF),
        player(3, "2026_03_LATE", "CCC", LATE_KICKOFF),
        player(4, "2026_03_LATE", "DDD", LATE_KICKOFF),
    ]
    cursor = RecordingCursor()
    result = persist_availability_contexts(
        cursor, run_id="run", projections=projections, manifest=manifest(projections),
        available_at=AS_OF + timedelta(minutes=1))
    assert game_ids_written(cursor) == {"2026_03_LATE"}
    assert result["startedGamesSkipped"] == ["2026_03_EARLY"]
    assert result["snapshotCount"] > 0


def test_a_run_after_every_kickoff_writes_nothing():
    projections = [player(1, "2026_03_EARLY", "AAA", EARLY_KICKOFF)]
    cursor = RecordingCursor()
    result = persist_availability_contexts(
        cursor, run_id="run", projections=projections, manifest=manifest(projections),
        available_at=AS_OF)
    assert cursor.calls == []
    assert result["contextsPublished"] == 0 and result["releaseId"] is None
    assert result["startedGamesSkipped"] == ["2026_03_EARLY"]


def test_a_pregame_run_publishes_every_game_as_before():
    projections = [
        player(1, "2026_03_EARLY", "AAA", EARLY_KICKOFF),
        player(3, "2026_03_LATE", "CCC", LATE_KICKOFF),
    ]
    cursor = RecordingCursor()
    before_any_kickoff = EARLY_KICKOFF - timedelta(hours=2)
    result = persist_availability_contexts(
        cursor, run_id="run", projections=projections,
        manifest=manifest(projections, as_of=before_any_kickoff), available_at=before_any_kickoff)
    assert game_ids_written(cursor) == {"2026_03_EARLY", "2026_03_LATE"}
    assert result["startedGamesSkipped"] == []


def test_kickoff_as_iso_string_is_honoured():
    projections = [player(1, "2026_03_EARLY", "AAA", EARLY_KICKOFF.isoformat()),
                   player(3, "2026_03_LATE", "CCC", LATE_KICKOFF.isoformat())]
    m = manifest([dict(row, commence_time=EARLY_KICKOFF if row["player_id"] == 1 else LATE_KICKOFF)
                  for row in projections])
    cursor = RecordingCursor()
    result = persist_availability_contexts(cursor, run_id="run", projections=projections,
                                           manifest=m, available_at=AS_OF)
    assert result["startedGamesSkipped"] == ["2026_03_EARLY"]
    assert game_ids_written(cursor) == {"2026_03_LATE"}
