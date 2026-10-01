from __future__ import annotations

import sys
from types import SimpleNamespace

from ingest.cfb_early_capture_schema import migrate


def test_early_checkpoint_migration_applies_once(monkeypatch) -> None:
    statements: list[str] = []
    definition = ["CHECK (checkpoint = 't_minus_48h')"]

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, _params=None):
            statements.append(sql)

        def fetchone(self):
            return (definition[0],)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=lambda _url: Connection()))
    assert migrate("test-url") is True
    assert sum("DROP CONSTRAINT" in sql for sql in statements) == 1
    assert sum("ADD CONSTRAINT" in sql for sql in statements) == 1

    statements.clear()
    definition[0] = "CHECK (checkpoint IN ('cfb_t_minus_7d', 'cfb_t_minus_4d'))"
    assert migrate("test-url") is False
    assert not any("DROP CONSTRAINT" in sql for sql in statements)
