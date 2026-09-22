"""Database manager for Neon PostgreSQL.

Uses psycopg2 with a connection wrapper for consistent API.
All queries use %s placeholders (native PostgreSQL).
"""

from __future__ import annotations

import hashlib
import os
import time
from contextlib import contextmanager

from db.schema import TABLES, INDEXES, MIGRATIONS

# Created before the advisory lock is taken, so a first-ever run can record
# its fingerprint. Nothing else writes this table.
SCHEMA_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS schema_state (
    id INTEGER PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
)
"""


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

    def _schema_fingerprint(self) -> str:
        """Identify the exact DDL this build would apply."""
        payload = "\n".join((*TABLES, *MIGRATIONS, *INDEXES)).encode()
        return hashlib.sha256(payload).hexdigest()

    def _schema_is_current(self, fingerprint: str) -> bool:
        """True when this fingerprint has already been applied.

        Reads one row from a table no other workflow writes, so it cannot
        queue behind a capture's locks the way the DDL itself does.
        """
        try:
            row = self.execute_one(
                "SELECT 1 AS ok FROM schema_state WHERE fingerprint = %s",
                (fingerprint,),
            )
        except Exception:
            return False
        return bool(row)

    def _ensure_schema(self) -> None:
        """Apply the DDL only when this build's schema is not already live.

        Order matters when it does run: TABLES first (base structure),
        MIGRATIONS second (column additions/changes), INDEXES last (may
        reference migrated columns).

        It usually must not run at all. Every scheduled job constructs this
        manager, and unconditional DDL meant each one took ACCESS EXCLUSIVE on
        every migrated table -- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
        takes that lock even when the column already exists and the statement
        does nothing. Against a 15-minute capture writing the same tables, that
        was a recurring `LockNotAvailable` that failed the whole run. A no-op
        pass now costs one indexed SELECT and takes no table locks.

        Set NBA_DFS_FORCE_SCHEMA=1 to reapply regardless -- the escape hatch
        for a table dropped out of band, which the fingerprint cannot see.
        """
        import psycopg2

        fingerprint = self._schema_fingerprint()
        forced = os.getenv("NBA_DFS_FORCE_SCHEMA") == "1"
        if not forced and self._schema_is_current(fingerprint):
            return

        retryable = (psycopg2.errors.DeadlockDetected, psycopg2.errors.LockNotAvailable)
        attempts = 4
        for attempt in range(attempts):
            try:
                with self.connect() as conn:
                    cur = conn.cursor()
                    cur.execute("SET LOCAL lock_timeout = '30s'")
                    cur.execute(SCHEMA_STATE_TABLE)
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))",
                        ("nba_dfs_v2_schema_initialization",),
                    )
                    # Re-check under the lock: when several workers queue on the
                    # same startup, only the first should pay for the DDL.
                    cur.execute(
                        "SELECT 1 AS ok FROM schema_state WHERE fingerprint = %s",
                        (fingerprint,),
                    )
                    if not forced and cur.fetchone():
                        return
                    for table_sql in TABLES:
                        cur.execute(table_sql)
                    for migration_sql in MIGRATIONS:
                        cur.execute(migration_sql)
                    for index_sql in INDEXES:
                        cur.execute(index_sql)
                    cur.execute(
                        """
                        INSERT INTO schema_state (id, fingerprint, applied_at)
                        VALUES (1, %s, NOW())
                        ON CONFLICT (id) DO UPDATE
                        SET fingerprint = EXCLUDED.fingerprint,
                            applied_at = EXCLUDED.applied_at
                        """,
                        (fingerprint,),
                    )
                return
            except retryable:
                if attempt == attempts - 1:
                    raise
                time.sleep(2 ** attempt)
