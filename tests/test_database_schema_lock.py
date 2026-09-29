from __future__ import annotations

from contextlib import contextmanager

import psycopg2

from db import database
from db.database import DatabaseManager


class _FakeConnection:
    """Records statements; answers the schema-state reads from `state`.

    `state` is the digest stored in db_schema_state (None = table absent), or a
    callable returning it, so a test can change it between reads.
    """

    def __init__(self, statements: list[tuple[str, object]], *, deadlock: bool, state=None) -> None:
        self.statements = statements
        self.deadlock = deadlock
        self.state = state
        self._next = None

    def cursor(self) -> "_FakeConnection":
        return self

    def _stored(self):
        return self.state() if callable(self.state) else self.state

    def execute(self, sql: str, params=None) -> None:
        self.statements.append((sql, params))
        if self.deadlock and "pg_advisory_xact_lock" in sql:
            raise psycopg2.errors.DeadlockDetected()
        if "to_regclass" in sql:
            self._next = {"present": self._stored() is not None}
        elif sql.startswith("SELECT digest FROM db_schema_state"):
            self._next = {"digest": self._stored()}
        else:
            self._next = None

    def fetchone(self):
        return self._next


def _manager(connect) -> DatabaseManager:
    manager = object.__new__(DatabaseManager)
    manager.connect = connect  # type: ignore[method-assign]
    return manager


def _schema(monkeypatch) -> None:
    monkeypatch.setattr(database, "TABLES", ["CREATE TABLE test_table (id int)"])
    monkeypatch.setattr(database, "MIGRATIONS", [])
    monkeypatch.setattr(database, "INDEXES", [])


def test_schema_setup_serializes_and_retries_deadlocks(monkeypatch) -> None:
    attempts = 0
    statements: list[tuple[str, object]] = []
    sleeps: list[int] = []

    @contextmanager
    def connect():
        nonlocal attempts
        attempts += 1
        # Call 1 is the lock-free digest read; call 2 deadlocks; call 3 applies.
        yield _FakeConnection(statements, deadlock=attempts == 2)

    _schema(monkeypatch)
    monkeypatch.setattr(database.time, "sleep", sleeps.append)

    _manager(connect)._ensure_schema()

    assert attempts == 3
    assert sleeps == [1]
    assert sum("pg_advisory_xact_lock" in sql for sql, _ in statements) == 2
    assert ("CREATE TABLE test_table (id int)", None) in statements
    # The applied digest is recorded last, in the same transaction as the DDL.
    assert statements[-1][0].startswith("INSERT INTO db_schema_state")
    assert statements[-1][1] == (database.schema_digest(),)


def test_unchanged_schema_runs_no_ddl_and_takes_no_lock(monkeypatch) -> None:
    _schema(monkeypatch)
    statements: list[tuple[str, object]] = []
    connects = 0

    @contextmanager
    def connect():
        nonlocal connects
        connects += 1
        yield _FakeConnection(statements, deadlock=False, state=database.schema_digest())

    _manager(connect)._ensure_schema()

    assert connects == 1
    assert not any("pg_advisory_xact_lock" in sql or sql.startswith("CREATE") or "lock_timeout" in sql
                   for sql, _ in statements)


def test_changed_schema_text_reapplies(monkeypatch) -> None:
    _schema(monkeypatch)
    statements: list[tuple[str, object]] = []

    @contextmanager
    def connect():
        yield _FakeConnection(statements, deadlock=False, state="digest-of-an-older-schema")

    _manager(connect)._ensure_schema()

    assert ("CREATE TABLE test_table (id int)", None) in statements


def test_digest_applied_by_another_job_while_waiting_skips_ddl(monkeypatch) -> None:
    _schema(monkeypatch)
    statements: list[tuple[str, object]] = []
    locked = {"yes": False}

    def stored():
        return database.schema_digest() if locked["yes"] else None

    class _Racing(_FakeConnection):
        def execute(self, sql: str, params=None) -> None:
            super().execute(sql, params)
            if "pg_advisory_xact_lock" in sql:
                locked["yes"] = True  # the other job finished while we waited

    @contextmanager
    def connect():
        yield _Racing(statements, deadlock=False, state=stored)

    _manager(connect)._ensure_schema()

    assert sum("pg_advisory_xact_lock" in sql for sql, _ in statements) == 1
    assert ("CREATE TABLE test_table (id int)", None) not in statements


def test_digest_tracks_every_schema_part(monkeypatch) -> None:
    _schema(monkeypatch)
    base = database.schema_digest()
    monkeypatch.setattr(database, "INDEXES", ["CREATE INDEX i ON test_table(id)"])
    with_index = database.schema_digest()
    monkeypatch.setattr(database, "INDEXES", [])
    monkeypatch.setattr(database, "MIGRATIONS", ["CREATE INDEX i ON test_table(id)"])
    as_migration = database.schema_digest()
    assert len({base, with_index, as_migration}) == 3
