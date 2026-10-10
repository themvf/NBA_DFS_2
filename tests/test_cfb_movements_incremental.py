"""record_movements reconciles only games with unprocessed snapshots unless asked for the whole ledger.

2026-10-04: the per-run full reconciliation re-derived every CFB transition and
re-sent them as no-op inserts, 7.7 GB of pages touched in 23 minutes for zero
new rows. These tests pin the incremental contract: which games are loaded,
which saved rows are verified, and that every processed snapshot is recorded
as covered, with or without a transition.
"""
from datetime import datetime, timedelta, timezone

import psycopg2.extras

import ingest.cfb_movements as mod

T0 = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def snapshot(history_id, matchup_id, minutes, books):
    return {"history_id": history_id, "matchup_id": matchup_id, "books": books,
            "captured_at": T0 + timedelta(minutes=minutes)}


class FakeCursor:
    def __init__(self, history_rows):
        self.history_rows = history_rows
        self.sql = []
        self.movement_rows = []
        self.coverage_rows = []
        self.last = ""

    def execute(self, sql, params=None):
        self.last = " ".join(sql.split())
        self.sql.append((self.last, params))

    def fetchall(self):
        if self.last.startswith("SELECT DISTINCT h.matchup_id"):
            return [{"matchup_id": m} for m in dict.fromkeys(r["matchup_id"] for r in self.history_rows)]
        if "FROM game_odds_history h JOIN cfb_matchups m" in self.last:
            return [dict(r) for r in self.history_rows]
        if self.last.startswith("SELECT * FROM cfb_quote_movements"):
            return [dict(r) for r in self.movement_rows]
        return []


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        return False
    def cursor(self):
        return self._cursor


class FakeDB:
    def __init__(self, cursor):
        self.conn = FakeConnection(cursor)
    def connect(self):
        return self.conn


COLUMNS = ("matchup_id", "previous_history_id", "history_id", "book", "market", "field", "kind", "before_value", "after_value")


def install_execute_values(monkeypatch, cursor):
    """Stand in for psycopg2's execute_values: store movement inserts so the verification read sees them."""
    calls = []
    def execute_values(cur, sql, rows, **kwargs):
        text = " ".join(sql.split())
        calls.append((text, list(rows)))
        if text.startswith("INSERT INTO cfb_quote_movements ("):
            present = {(r["history_id"], r["book"], r["field"]) for r in cursor.movement_rows}
            for row in rows:
                saved = dict(zip(COLUMNS, row))
                if (saved["history_id"], saved["book"], saved["field"]) in present:
                    continue  # ON CONFLICT DO NOTHING: the saved row wins
                saved["before_value"] = saved["before_value"].adapted
                saved["after_value"] = saved["after_value"].adapted
                cursor.movement_rows.append(saved)
        if text.startswith("INSERT INTO cfb_quote_movement_coverage"):
            cursor.coverage_rows.extend(rows)
    monkeypatch.setattr(psycopg2.extras, "execute_values", execute_values)
    return calls


def history_reads(cursor):
    return [(s, p) for s, p in cursor.sql
            if "FROM game_odds_history h JOIN cfb_matchups m" in s and not s.startswith("SELECT DISTINCT")]


def test_incremental_run_loads_only_uncovered_games_and_verifies_them_alone(monkeypatch):
    cursor = FakeCursor([snapshot(1, 7, 0, {"dk": {"total_line": 50}}),
                         snapshot(2, 7, 60, {"dk": {"total_line": 52}})])
    calls = install_execute_values(monkeypatch, cursor)
    result = mod.record_movements(FakeDB(cursor))
    pending_sql = cursor.sql[1][0]
    assert pending_sql.startswith("SELECT DISTINCT h.matchup_id")
    assert "NOT EXISTS (SELECT 1 FROM cfb_quote_movement_coverage c WHERE c.history_id=h.id)" in pending_sql
    [(history_sql, history_params)] = history_reads(cursor)
    assert history_sql.endswith("AND h.matchup_id=ANY(%s) ORDER BY h.matchup_id,h.captured_at,h.id")
    assert "cfb_quote_movement_coverage" not in history_sql, "the pending games are resolved before the history read"
    assert history_params == ([7],)
    verify = [s for s, p in cursor.sql if s.startswith("SELECT * FROM cfb_quote_movements")]
    assert verify == ["SELECT * FROM cfb_quote_movements WHERE matchup_id=ANY(%s)"], "only the affected games are verified"
    assert [p for s, p in cursor.sql if s.startswith("SELECT * FROM cfb_quote_movements")] == [([7],)]
    assert result == {"integrity_status": "pass", "mode": "incremental", "snapshots": 2, "field_transitions": 1, "games": 1}
    assert sorted(cursor.coverage_rows) == [(1, 7), (2, 7)], "the baseline snapshot is covered too, not only the one with a transition"
    inserted = [rows for text, rows in calls if text.startswith("INSERT INTO cfb_quote_movements (")]
    assert len(inserted) == 1 and len(inserted[0]) == 1


def test_full_run_reads_everything_and_verifies_the_whole_ledger(monkeypatch):
    cursor = FakeCursor([snapshot(1, 7, 0, {"dk": {"ml_home": -110}}), snapshot(2, 7, 60, {"dk": {"ml_home": -115}})])
    install_execute_values(monkeypatch, cursor)
    result = mod.record_movements(FakeDB(cursor), full=True)
    [(history_sql, history_params)] = history_reads(cursor)
    assert "cfb_quote_movement_coverage" not in history_sql and "ANY" not in history_sql
    assert history_params is None
    assert not any(s.startswith("SELECT DISTINCT h.matchup_id") for s, p in cursor.sql)
    assert [s for s, p in cursor.sql if s.startswith("SELECT * FROM cfb_quote_movements")] == ["SELECT * FROM cfb_quote_movements"]
    assert result["mode"] == "full" and result["field_transitions"] == 1
    assert sorted(cursor.coverage_rows) == [(1, 7), (2, 7)], "a full pass covers every snapshot so the next incremental run has nothing to redo"


def test_nothing_new_means_no_inserts_and_no_ledger_read(monkeypatch):
    cursor = FakeCursor([])
    calls = install_execute_values(monkeypatch, cursor)
    result = mod.record_movements(FakeDB(cursor))
    assert result == {"integrity_status": "pass", "mode": "incremental", "snapshots": 0, "field_transitions": 0, "games": 0}
    assert calls == []
    assert history_reads(cursor) == [], "no pending game means no history read at all"
    assert not any(s.startswith("SELECT * FROM cfb_quote_movements") for s, p in cursor.sql)


def test_a_saved_row_that_disagrees_with_history_still_fails(monkeypatch):
    cursor = FakeCursor([snapshot(1, 7, 0, {"dk": {"total_line": 50}}), snapshot(2, 7, 60, {"dk": {"total_line": 52}})])
    install_execute_values(monkeypatch, cursor)
    cursor.movement_rows.append({"matchup_id": 7, "previous_history_id": 1, "history_id": 2, "book": "dk", "market": "total",
                                 "field": "total_line", "kind": "changed", "before_value": 49, "after_value": 52})
    try:
        mod.record_movements(FakeDB(cursor))
    except ValueError as exc:
        assert "mismatch" in str(exc)
    else:
        raise AssertionError("a disagreeing saved transition must still fail the incremental run")
