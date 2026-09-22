from __future__ import annotations

from contextlib import contextmanager

import psycopg2

from db import database
from db.database import DatabaseManager


class _FakeConnection:
    """Records every statement; answers the fingerprint probe from `applied`."""

    def __init__(self, statements: list[tuple[str, object]], *, applied: set[str],
                 deadlock_on_lock: bool = False) -> None:
        self.statements = statements
        self.applied = applied
        self.deadlock_on_lock = deadlock_on_lock
        self._row: dict | None = None

    def cursor(self) -> "_FakeConnection":
        return self

    def execute(self, sql: str, params=None) -> None:
        self.statements.append((sql, params))
        if self.deadlock_on_lock and "pg_advisory_xact_lock" in sql:
            raise psycopg2.errors.DeadlockDetected()
        if "FROM schema_state" in sql:
            self._row = {"ok": 1} if params and params[0] in self.applied else None
        elif "INSERT INTO schema_state" in sql:
            self.applied.add(params[0])
            self._row = None
        else:
            self._row = None

    def fetchone(self):
        return self._row

    def fetchall(self):
        return [self._row] if self._row else []


def _manager(statements, applied, *, deadlock_first: bool = False):
    calls = {"n": 0}

    @contextmanager
    def connect():
        calls["n"] += 1
        yield _FakeConnection(
            statements, applied=applied,
            deadlock_on_lock=deadlock_first and calls["n"] == 2,
        )

    manager = object.__new__(DatabaseManager)
    manager.connect = connect  # type: ignore[method-assign]
    return manager, calls


def _freeze_schema(monkeypatch, sleeps):
    monkeypatch.setattr(database, "TABLES", ["CREATE TABLE test_table (id int)"])
    monkeypatch.setattr(database, "MIGRATIONS", [])
    monkeypatch.setattr(database, "INDEXES", [])
    monkeypatch.setattr(database.time, "sleep", sleeps.append)
    monkeypatch.delenv("NBA_DFS_FORCE_SCHEMA", raising=False)


def test_first_run_applies_ddl_and_records_the_fingerprint(monkeypatch) -> None:
    statements: list[tuple[str, object]] = []
    sleeps: list[int] = []
    applied: set[str] = set()
    _freeze_schema(monkeypatch, sleeps)
    manager, _ = _manager(statements, applied)

    manager._ensure_schema()

    sql = [s for s, _ in statements]
    assert "CREATE TABLE test_table (id int)" in sql
    assert sum("pg_advisory_xact_lock" in s for s in sql) == 1
    assert applied == {manager._schema_fingerprint()}


def test_an_unchanged_schema_takes_no_locks_and_runs_no_ddl(monkeypatch) -> None:
    """The fix. A no-op pass must not touch a table a capture is writing."""
    statements: list[tuple[str, object]] = []
    sleeps: list[int] = []
    _freeze_schema(monkeypatch, sleeps)
    manager, calls = _manager(statements, applied=set())
    applied = {manager._schema_fingerprint()}
    manager, calls = _manager(statements, applied)

    manager._ensure_schema()

    sql = [s for s, _ in statements]
    assert calls["n"] == 1, "one cheap SELECT, not a DDL transaction"
    assert not any("pg_advisory_xact_lock" in s for s in sql)
    assert not any("CREATE TABLE test_table" in s for s in sql)
    assert not any("ALTER" in s for s in sql)


def test_changed_ddl_reapplies(monkeypatch) -> None:
    statements: list[tuple[str, object]] = []
    sleeps: list[int] = []
    _freeze_schema(monkeypatch, sleeps)
    manager, _ = _manager(statements, applied={"a-fingerprint-from-an-older-build"})

    manager._ensure_schema()

    assert any("CREATE TABLE test_table" in s for s, _ in statements)


def test_force_env_reapplies_even_when_current(monkeypatch) -> None:
    statements: list[tuple[str, object]] = []
    sleeps: list[int] = []
    _freeze_schema(monkeypatch, sleeps)
    manager, _ = _manager(statements, applied=set())
    manager, _ = _manager(statements, applied={manager._schema_fingerprint()})
    monkeypatch.setenv("NBA_DFS_FORCE_SCHEMA", "1")

    manager._ensure_schema()

    assert any("CREATE TABLE test_table" in s for s, _ in statements)


def test_schema_setup_serializes_and_retries_deadlocks(monkeypatch) -> None:
    statements: list[tuple[str, object]] = []
    sleeps: list[int] = []
    _freeze_schema(monkeypatch, sleeps)
    # call 1 = the fingerprint probe, call 2 = first DDL attempt (deadlocks)
    manager, calls = _manager(statements, applied=set(), deadlock_first=True)

    manager._ensure_schema()

    assert sleeps == [1]
    assert sum("pg_advisory_xact_lock" in s for s, _ in statements) == 2
    assert any("CREATE TABLE test_table" in s for s, _ in statements)
