"""A failed read is an error, never an empty result.

Until 2026-10-07 DatabaseManager.execute caught every exception from fetchall
and returned []. The pick'em grader's 7 GB query hit MemoryError, got [], and
reported "no eligible outcomes" from a green run. A statement with no result
set still returns []; anything else that goes wrong while fetching raises.
"""
from __future__ import annotations

import pytest

from db.database import DatabaseManager


class _Cursor:
    def __init__(self, description, fetch):
        self.description = description
        self._fetch = fetch

    def execute(self, *_args):
        pass

    def fetchall(self):
        return self._fetch()


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _db(monkeypatch, cursor) -> DatabaseManager:
    monkeypatch.setattr("psycopg2.connect", lambda *_a, **_k: _Connection(cursor))
    db = object.__new__(DatabaseManager)
    db.database_url = "test"
    return db


def test_statement_without_result_set_returns_empty(monkeypatch):
    def unreachable():
        raise AssertionError("fetchall must not run without a result set")
    assert _db(monkeypatch, _Cursor(None, unreachable)).execute("UPDATE t SET x=1") == []


def test_rows_are_returned(monkeypatch):
    assert _db(monkeypatch, _Cursor([("x",)], lambda: [{"x": 1}])).execute("SELECT 1 x") == [{"x": 1}]


def test_fetch_failure_raises_instead_of_reading_as_empty(monkeypatch):
    def out_of_memory():
        raise MemoryError()
    with pytest.raises(MemoryError):
        _db(monkeypatch, _Cursor([("payload",)], out_of_memory)).execute("SELECT payload FROM big")


def test_duplicate_helpers_follow_the_same_rule():
    from ingest.cfb_history import _TransactionDb
    from ingest.ff_fantasypros import RefreshDatabase

    def out_of_memory():
        raise MemoryError()
    for cls in (_TransactionDb, RefreshDatabase):
        db = object.__new__(cls)
        conn = _Connection(_Cursor([("payload",)], out_of_memory))
        db.connection = db.conn = conn
        with pytest.raises(MemoryError):
            db.execute("SELECT payload FROM big")
        conn._cursor = _Cursor(None, out_of_memory)
        assert db.execute("CREATE TABLE t (x int)") == []
