import re
from datetime import datetime, timezone
from pathlib import Path

from ingest.ff_injuries import (
    COMPACT_MARKER,
    SLEEPER_COMPACT_KEYS,
    _payload_hash,
    compact_payload,
    normalize_injury_observation,
    normalize_injury_status,
    persist_injury_observation,
    persist_sleeper_injury_observations,
)


class InjuryDatabase:
    def __init__(self) -> None:
        self.observations: dict[tuple[int, int, str], dict] = {}
        self.active_injury: dict | None = None
        self.events: list[tuple] = []
        self.player_status: str | None = None
        self.payloads: dict[str, tuple] = {}
        self.stored_raw: dict[tuple[int, int, str], dict] = {}
        self.now = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)

    def execute_one(self, sql: str, params: tuple):
        compact = " ".join(sql.split())
        if compact.startswith("INSERT INTO ff_player_injury_observations"):
            key = (params[3], params[0], params[2])
            if key in self.observations:
                return None
            row = {"id": len(self.observations) + 1, "observed_at": self.now}
            self.observations[key] = row
            self.stored_raw[key] = {"raw_payload": params[17].adapted, "response_hash": params[18]}
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
        if compact.startswith("INSERT INTO ff_injury_payloads"):
            self.payloads.setdefault(params[0], (params[1], params[2].adapted))
            return []
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


SLEEPER_ROW = {
    "team": "PHI", "position": "WR", "depth_chart_order": 1, "injury_status": None, "status": "Active",
    "full_name": "A Receiver", "news_updated": 1761900000000, "metadata": {"rookie_year": "2024"},
}


def test_sleeper_payload_is_compacted_and_the_full_payload_stored_once() -> None:
    db = InjuryDatabase()
    for snapshot in (300, 301):
        persist_injury_observation(db, player_id=7, season=2026, source="sleeper",
                                   source_snapshot_id=snapshot, row=dict(SLEEPER_ROW))
    full_hash = _payload_hash("sleeper", SLEEPER_ROW)
    assert db.payloads == {full_hash: ("sleeper", SLEEPER_ROW)}
    for snapshot in (300, 301):
        stored = db.stored_raw[(snapshot, 7, "sleeper")]
        # The hash is still the hash of the FULL payload, so it addresses ff_injury_payloads.
        assert stored["response_hash"] == full_hash
        assert stored["raw_payload"] == {
            "team": "PHI", "position": "WR", "depth_chart_order": 1, "injury_status": None,
            "status": "Active", **COMPACT_MARKER}


def test_compaction_keeps_key_presence_exactly() -> None:
    # A key absent from the provider row stays absent: `raw_payload ? 'depth_chart_order'`
    # (the partial depth index and getNflRosterEvidence) must answer the same.
    partial = {"injury_status": "Out"}
    assert compact_payload("sleeper", partial) == {"injury_status": "Out", **COMPACT_MARKER}
    assert "url" not in compact_payload("sleeper", SLEEPER_ROW)


def test_only_sleeper_is_compacted() -> None:
    # research/nfl_game_leaders.py (hash-pinned) reads the FantasyPros payload whole.
    row = {"status": "Out", "url": "https://example.test", "injury_update_date": "2026-10-30"}
    assert compact_payload("fantasypros", row) is row
    assert compact_payload("nfl_official", row) is row
    db = InjuryDatabase()
    persist_injury_observation(db, player_id=8, season=2026, source="fantasypros",
                               source_snapshot_id=400, row=row, reconcile_current=False)
    assert db.payloads == {}
    assert db.stored_raw[(400, 8, "fantasypros")]["raw_payload"] == row


def test_every_web_sleeper_evidence_field_survives_compaction() -> None:
    source = (Path(__file__).resolve().parents[1] / "web/src/lib/nfl-dfs/availability.ts").read_text(encoding="utf-8")
    match = re.search(r"SLEEPER_EVIDENCE_FIELDS = \[([^\]]*)\]", source)
    assert match, "SLEEPER_EVIDENCE_FIELDS not found"
    fields = set(re.findall(r'"([^"]+)"', match.group(1)))
    assert fields and fields <= set(SLEEPER_COMPACT_KEYS), (
        "a web reader takes a Sleeper key the compacted observation drops; add it to "
        "ingest.ff_injuries.SLEEPER_COMPACT_KEYS or read ff_injury_payloads")


class BatchCursorDb(InjuryDatabase):
    """persist_sleeper_injury_observations stores payloads through execute_values."""

    def __init__(self) -> None:
        super().__init__()
        self.conn = self

    def cursor(self):
        return self


def test_sleeper_universe_writer_stores_payloads_in_one_statement(monkeypatch) -> None:
    import ingest.ff_injuries as module
    db = BatchCursorDb()
    calls = []

    def fake_execute_values(cursor, sql, rows, page_size=None, **_):
        calls.append(" ".join(sql.split()))
        for response_hash, source, payload in rows:
            db.payloads.setdefault(response_hash, (source, payload.adapted))

    monkeypatch.setattr("psycopg2.extras.execute_values", fake_execute_values)
    players = [{"player_id": 1, "position": "WR", "metadata": {"sleeper": dict(SLEEPER_ROW)}},
               {"player_id": 2, "position": "QB", "metadata": {}, "injury_status": None},
               {"player_id": 3, "position": "DST", "metadata": {"sleeper": {"x": 1}}}]
    counts = persist_sleeper_injury_observations(db, season=2026, source_snapshot_id=500, players=players)
    assert counts["observations"] == 2
    assert len(calls) == 1 and calls[0].startswith("INSERT INTO ff_injury_payloads")
    assert set(db.payloads) == {_payload_hash("sleeper", SLEEPER_ROW), _payload_hash("sleeper", {"injury_status": None})}
    assert db.stored_raw[(500, 1, "sleeper")]["raw_payload"]["_compact"] == COMPACT_MARKER["_compact"]
    assert module.COMPACT_SOURCES == frozenset({"sleeper"})


def test_batch_capture_compacts_sleeper_rows_and_stores_full_payloads_first(monkeypatch) -> None:
    from ingest.ff_injuries import persist_injury_observations_batch
    db = BatchCursorDb()
    order: list[str] = []
    observation_rows: list[tuple] = []

    def fake_execute_values(cursor, sql, rows, page_size=None, fetch=False, **_):
        statement = " ".join(sql.split())
        order.append(statement)
        if statement.startswith("INSERT INTO ff_injury_payloads"):
            for response_hash, source, payload in rows:
                db.payloads.setdefault(response_hash, (source, payload.adapted))
            return None
        if statement.startswith("INSERT INTO ff_player_injury_observations"):
            observation_rows.extend(rows)
            return [{"player_id": r[0], "id": i + 1, "observed_at": db.now} for i, r in enumerate(rows)]
        return None

    monkeypatch.setattr("psycopg2.extras.execute_values", fake_execute_values)
    db.execute = lambda sql, params=None: []  # no active episodes
    second = {**SLEEPER_ROW, "depth_chart_order": 2}
    persist_injury_observations_batch(db, season=2026, source="sleeper", source_snapshot_id=600,
                                      rows=[(1, dict(SLEEPER_ROW)), (2, second)])
    assert order[0].startswith("INSERT INTO ff_injury_payloads")
    assert order[1].startswith("INSERT INTO ff_player_injury_observations")
    assert set(db.payloads) == {_payload_hash("sleeper", SLEEPER_ROW), _payload_hash("sleeper", second)}
    by_player = {r[0]: r for r in observation_rows}
    assert by_player[2][17].adapted == {**{k: second[k] for k in SLEEPER_COMPACT_KEYS if k in second}, **COMPACT_MARKER}
    assert by_player[2][18] == _payload_hash("sleeper", second)
