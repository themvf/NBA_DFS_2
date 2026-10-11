"""A run built after a kickoff must not republish (or supersede) that game's contexts,
and a context whose meaning has not changed stays current instead of being re-inserted."""
from datetime import datetime, timedelta, timezone
import json

from ingest.nfl_availability_context_publish import REASSERT_AFTER, persist_availability_contexts

UTC = timezone.utc
AS_OF = datetime(2026, 9, 27, 18, 0, tzinfo=UTC)
EARLY_KICKOFF = datetime(2026, 9, 27, 17, 0, tzinfo=UTC)   # started an hour ago
LATE_KICKOFF = datetime(2026, 9, 27, 20, 25, tzinfo=UTC)   # still pregame


class RecordingCursor:
    """Records every statement; the current-row lookup returns the rows it was given."""

    def __init__(self, current=()):
        self.calls = []
        self.rowcount = 1
        self.current = list(current)
        self.result = []

    def execute(self, sql, params=None):
        text = " ".join(sql.split())
        self.calls.append((text, params))
        self.result = self.current if text.startswith("SELECT snapshot_id,definition_id") else []

    def fetchall(self):
        return list(self.result)


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


# --- An unchanged context stays current -------------------------------------------

PREGAME = LATE_KICKOFF - timedelta(hours=8)
GAME = "2026_03_LATE"


def team_projections():
    return [player(1, GAME, "CCC", LATE_KICKOFF, "QB", 1), player(2, GAME, "CCC", LATE_KICKOFF, "QB", 2),
            player(3, GAME, "CCC", LATE_KICKOFF, "WR", 1)]


def run(as_of, current=(), observation_id=7, out_player=None):
    projections = team_projections()
    m = manifest(projections, as_of=as_of)
    for pid, value in m["availability_decisions"].items():
        value["state"] = "OUT_CONFIRMED" if int(pid) == out_player else "EXPECTED_ACTIVE"
        value["qualifying_observation_ids"] = [observation_id]
        value["qualifying_source_snapshot_ids"] = [observation_id * 10]
    cursor = RecordingCursor(current)
    result = persist_availability_contexts(cursor, run_id=f"run-{as_of.isoformat()}", projections=projections,
                                           manifest=m, available_at=as_of)
    return cursor, result


def inserted(cursor):
    return [{"snapshot_id": p[0], "definition_id": p[1], "subject_id": p[3], "target_id": p[4],
             "as_of_at": p[5], "material_digest": p[-1], "payload": p[-2]}
            for sql, p in cursor.calls if sql.startswith("INSERT INTO nfl_context_snapshots")]


def superseded(cursor):
    return [p for sql, p in cursor.calls if sql.startswith("UPDATE nfl_context_snapshots") and "'superseded'" in sql]


def withdrawal_keeps(cursor):
    return {snapshot_id for sql, p in cursor.calls
            if sql.startswith("UPDATE nfl_context_snapshots") and "'withdrawn'" in sql for snapshot_id in p[2]}


def test_a_run_whose_contexts_did_not_change_writes_none_and_withdraws_none():
    first, _ = run(PREGAME)
    current = inserted(first)
    assert len(current) == 4, "three players and one team QB context"
    second, result = run(PREGAME + timedelta(hours=1), current, observation_id=8)
    assert inserted(second) == [] and superseded(second) == []
    assert withdrawal_keeps(second) == {row["snapshot_id"] for row in current}
    assert result["contextsPublished"] == 0 and result["contextsUnchanged"] == 4


def test_a_changed_player_is_republished_and_the_qb_context_follows_it():
    first, _ = run(PREGAME)
    current = inserted(first)
    second, result = run(PREGAME + timedelta(hours=1), current, observation_id=8, out_player=1)
    written = {row["subject_id"] for row in inserted(second)}
    assert written == {"1", "CCC"}, "the starter who is now out, and his team's QB context"
    assert {p[2] for p in superseded(second)} == {"1", "CCC"}
    kept = {row["snapshot_id"] for row in current if row["subject_id"] in {"2", "3"}}
    assert kept <= withdrawal_keeps(second), "unchanged players stay current"
    assert result["contextsPublished"] == 2 and result["contextsUnchanged"] == 2


def test_the_qb_context_names_the_player_contexts_that_stay_current():
    first, _ = run(PREGAME)
    current = inserted(first)
    current_by_player = {row["subject_id"]: row["snapshot_id"] for row in current if row["definition_id"].startswith("player")}
    second, _ = run(PREGAME + timedelta(hours=1), current, observation_id=8, out_player=3)
    [qb] = [row for row in current if row["subject_id"] == "CCC"]
    named = json.loads(qb["payload"])["availability_evidence"]["playerContextIds"]
    assert named == [current_by_player["1"], current_by_player["2"]]
    assert "CCC" not in {row["subject_id"] for row in inserted(second)}, "a WR change leaves the QB context current"


def test_an_unchanged_context_is_reasserted_once_it_is_six_hours_old():
    first, _ = run(PREGAME)
    current = inserted(first)
    later, result = run(PREGAME + REASSERT_AFTER, current, observation_id=8)
    assert len(inserted(later)) == 4 and result["contextsUnchanged"] == 0
    assert len(superseded(later)) == 4


def test_a_current_row_written_before_the_digest_existed_is_republished():
    first, _ = run(PREGAME)
    legacy = [dict(row, material_digest=None) for row in inserted(first)]
    second, result = run(PREGAME + timedelta(hours=1), legacy, observation_id=8)
    assert len(inserted(second)) == 4 and result["contextsUnchanged"] == 0


def test_a_current_row_newer_than_the_run_is_never_reused():
    first, _ = run(PREGAME + timedelta(hours=2))
    _, result = run(PREGAME, inserted(first))
    assert result["contextsUnchanged"] == 0
