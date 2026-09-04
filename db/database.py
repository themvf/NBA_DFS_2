"""Database manager for Neon PostgreSQL.

Uses psycopg2 with a connection wrapper for consistent API.
All queries use %s placeholders (native PostgreSQL).
"""

from __future__ import annotations

import time
from contextlib import contextmanager

from db.schema import TABLES, INDEXES, MIGRATIONS


class DatabaseManager:
    def __init__(self, database_url: str, *, initialize_schema: bool = True) -> None:
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self.database_url = database_url
        if initialize_schema:
            self._ensure_schema()

    @contextmanager
    def connect(self):
        """Yield a psycopg2 connection with RealDictCursor.

        Auto-commits on clean exit, rolls back on exception.
        """
        import psycopg2
        from psycopg2.extras import RealDictCursor

        shared = getattr(self, "_shared_connection", None)
        conn = shared if shared is not None else psycopg2.connect(self.database_url, cursor_factory=RealDictCursor)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if shared is None:
                conn.close()

    @contextmanager
    def reuse_connection(self):
        """Opt-in, single-threaded worker session; preserve per-operation commits.

        A failed later detector must not roll back already accepted captures.
        Reuse only the transport, not one transaction spanning the whole worker.
        """
        import psycopg2
        from psycopg2.extras import RealDictCursor
        if getattr(self, "_shared_connection", None) is not None:
            yield self
            return
        conn = psycopg2.connect(self.database_url, cursor_factory=RealDictCursor)
        self._shared_connection = conn
        try:
            yield self
        finally:
            self._shared_connection = None
            conn.close()

    def execute(self, sql: str, params=None):
        """Execute a single SQL statement and return all rows."""
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params or ())
            try:
                return cur.fetchall()
            except Exception:
                return []

    def execute_one(self, sql: str, params=None):
        """Execute and return the first row, or None."""
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params or ())
            return cur.fetchone()

    def execute_insert(self, sql: str, params=None) -> int:
        """Execute an INSERT with RETURNING id and return the new id."""
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return row["id"] if row else 0

    def execute_many(self, sql: str, params_list: list):
        """Execute a statement for each set of params in a single transaction."""
        with self.connect() as conn:
            cur = conn.cursor()
            for params in params_list:
                cur.execute(sql, params)

    def _ensure_schema(self) -> None:
        """Create all tables, run migrations, then create indexes.

        Order matters: TABLES first (base structure), MIGRATIONS second
        (column additions/changes), INDEXES last (may reference migrated columns).
        Scheduled jobs all construct this manager, so schema work is serialized
        to prevent incompatible DDL locks across concurrent workflows.
        """
        import psycopg2

        retryable = (psycopg2.errors.DeadlockDetected, psycopg2.errors.LockNotAvailable)
        attempts = 4
        for attempt in range(attempts):
            try:
                with self.connect() as conn:
                    cur = conn.cursor()
                    cur.execute("SET LOCAL lock_timeout = '30s'")
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))",
                        ("nba_dfs_v2_schema_initialization",),
                    )
                    for table_sql in TABLES:
                        cur.execute(table_sql)
                    for migration_sql in MIGRATIONS:
                        cur.execute(migration_sql)
                    for index_sql in INDEXES:
                        cur.execute(index_sql)
                return
            except retryable:
                if attempt == attempts - 1:
                    raise
                time.sleep(2 ** attempt)
