from db.database import DatabaseManager
import pytest


def test_worker_can_skip_global_schema_without_changing_default(monkeypatch):
    calls = []
    monkeypatch.setattr(DatabaseManager, "_ensure_schema", lambda self: calls.append(self.database_url))
    DatabaseManager("default")
    DatabaseManager("worker", initialize_schema=False)
    assert calls == ["default"]
    with pytest.raises(ValueError):
        DatabaseManager("", initialize_schema=False)


def test_reuse_keeps_transactions_independent_and_closes_once(monkeypatch):
    class Connection:
        commits = rollbacks = closes = 0
        def commit(self): self.commits += 1
        def rollback(self): self.rollbacks += 1
        def close(self): self.closes += 1
    conn = Connection()
    calls = []
    def connect(*args, **kwargs):
        calls.append(1)
        return conn
    monkeypatch.setattr("psycopg2.connect", connect)
    db = object.__new__(DatabaseManager)
    db.database_url = "test"
    with db.reuse_connection():
        with db.connect() as first:
            assert first is conn
        with pytest.raises(ValueError):
            with db.connect():
                raise ValueError("failed later operation")
        with db.reuse_connection():
            with db.connect() as third:
                assert third is conn
        assert conn.closes == 0
    assert len(calls) == 1
    assert (conn.commits, conn.rollbacks, conn.closes) == (2, 1, 1)
    assert db._shared_connection is None
