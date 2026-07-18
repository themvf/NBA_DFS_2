from __future__ import annotations

from contextlib import contextmanager

import psycopg2

from db import database
from db.database import DatabaseManager


class _FakeConnection:
    def __init__(self, statements: list[tuple[str, object]], *, deadlock: bool) -> None:
        self.statements = statements
        self.deadlock = deadlock

    def cursor(self) -> "_FakeConnection":
        return self

    def execute(self, sql: str, params=None) -> None:
        self.statements.append((sql, params))
        if self.deadlock and "pg_advisory_xact_lock" in sql:
            raise psycopg2.errors.DeadlockDetected()


def test_schema_setup_serializes_and_retries_deadlocks(monkeypatch) -> None:
    attempts = 0
    statements: list[tuple[str, object]] = []
    sleeps: list[int] = []

    @contextmanager
    def connect():
        nonlocal attempts
        attempts += 1
        yield _FakeConnection(statements, deadlock=attempts == 1)

    manager = object.__new__(DatabaseManager)
    manager.connect = connect  # type: ignore[method-assign]

    monkeypatch.setattr(database, "TABLES", ["CREATE TABLE test_table (id int)"])
    monkeypatch.setattr(database, "MIGRATIONS", [])
    monkeypatch.setattr(database, "INDEXES", [])
    monkeypatch.setattr(database.time, "sleep", sleeps.append)

    manager._ensure_schema()

    assert attempts == 2
    assert sleeps == [1]
    assert sum("pg_advisory_xact_lock" in sql for sql, _ in statements) == 2
    assert statements[-1][0] == "CREATE TABLE test_table (id int)"
