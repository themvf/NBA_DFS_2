from __future__ import annotations

import pytest

from db.database import DatabaseManager, fetch_rows
from ingest.cfb_history import _TransactionDb
from ingest.ff_fantasypros import RefreshDatabase


def out_of_memory():
    raise MemoryError()


class Cursor:
    def __init__(self, description, fetch):
        self.description = description
        self._fetch = fetch

    def execute(self, *_args):
        pass

    def fetchall(self):
        return self._fetch()


class Connection:
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


def test_a_statement_without_a_result_set_returns_no_rows():
    assert fetch_rows(Cursor(None, out_of_memory)) == []


def test_rows_are_returned():
    assert fetch_rows(Cursor([("x",)], lambda: [{"x": 1}])) == [{"x": 1}]


def test_a_failed_fetch_raises_instead_of_reading_as_empty():
    with pytest.raises(MemoryError):
        fetch_rows(Cursor([("payload",)], out_of_memory))


def database_manager(monkeypatch, connection):
    monkeypatch.setattr("psycopg2.connect", lambda *_a, **_k: connection)
    db = object.__new__(DatabaseManager)
    db.database_url = "test"
    return db


def wrapper(cls, connection):
    db = object.__new__(cls)
    db.connection = db.conn = connection
    return db


@pytest.mark.parametrize("build", [
    lambda monkeypatch, connection: database_manager(monkeypatch, connection),
    lambda _monkeypatch, connection: wrapper(_TransactionDb, connection),
    lambda _monkeypatch, connection: wrapper(RefreshDatabase, connection),
], ids=["DatabaseManager", "cfb_history", "ff_fantasypros"])
def test_every_execute_wrapper_surfaces_a_failed_fetch(monkeypatch, build):
    db = build(monkeypatch, Connection(Cursor([("payload",)], out_of_memory)))
    with pytest.raises(MemoryError):
        db.execute("SELECT payload FROM big")
