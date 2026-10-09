"""Database manager for Neon PostgreSQL.

Uses psycopg2 with a connection wrapper for consistent API.
All queries use %s placeholders (native PostgreSQL).
"""

from __future__ import annotations

import hashlib
import time
from contextlib import contextmanager

from db.schema import TABLES, INDEXES, MIGRATIONS

# The schema digest last applied in full. Every scheduled job constructs a
# DatabaseManager, and the full pass drops and recreates 17 immutability
# triggers (ACCESS EXCLUSIVE on their tables) and re-issues every index: run on
# each start it contended with live writers and killed jobs on
# `LockNotAvailable` (the 2026-09-28 23:07 UTC NFL availability refresh, for
# one). Every statement is idempotent, so once a digest is applied the pass
# changes nothing until the schema text changes.
SCHEMA_STATE_DDL = (
    "CREATE TABLE IF NOT EXISTS db_schema_state ("
    "id SMALLINT PRIMARY KEY CHECK (id = 1), "
    "digest TEXT NOT NULL, "
    "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
)


def schema_digest() -> str:
    """Digest of the schema text this code would apply (read at call time)."""
    digest = hashlib.sha256()
    for part in (TABLES, MIGRATIONS, INDEXES):
        for sql in part:
            digest.update(sql.encode("utf-8"))
            digest.update(b"\x00")
        digest.update(b"\x01")
    return digest.hexdigest()


def _first(row, key: str):
    if row is None:
        return None
    return row[key] if isinstance(row, dict) else row[0]


def schema_is_current(cur, digest: str) -> bool:
    """True when this exact schema text was already applied in full."""
    cur.execute("SELECT to_regclass('public.db_schema_state') IS NOT NULL AS present")
    if not _first(cur.fetchone(), "present"):
        return False
    cur.execute("SELECT digest FROM db_schema_state WHERE id = 1")
    return _first(cur.fetchone(), "digest") == digest


def fetch_rows(cursor) -> list:
    """Rows of the last statement; [] only when it produced no result set. A failed fetch raises."""
    return cursor.fetchall() if cursor.description is not None else []


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
            return fetch_rows(cur)

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

    def require_tables(self, names) -> None:
        """Fail loudly if any named table is absent.

        The counterpart of `initialize_schema=False`: a scheduled job that
        skips the per-invocation DDL (because `_ensure_schema` contends with
        other writers and has been killing jobs on `LockNotAvailable`) must
        still refuse to run against a database that was never migrated,
        rather than failing later with an opaque "relation does not exist".
        A missing table is an operator problem (run any schema-initializing
        entrypoint once), not something a read-mostly job should DDL its way
        out of.
        """
        wanted = sorted(set(names))
        rows = self.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = ANY(%s)",
            (wanted,),
        )
        present = {r["table_name"] for r in rows}
        missing = [n for n in wanted if n not in present]
        if missing:
            raise RuntimeError(
                "database schema is not initialized for this job; missing tables: "
                + ", ".join(missing)
                + ". Run a schema-initializing entrypoint once, then retry."
            )

    def _ensure_schema(self) -> None:
        """Create all tables, run migrations, then create indexes.

        Order matters: TABLES first (base structure), MIGRATIONS second
        (column additions/changes), INDEXES last (may reference migrated columns).
        Scheduled jobs all construct this manager, so schema work is serialized
        to prevent incompatible DDL locks across concurrent workflows.

        When the schema text is unchanged since the last full pass (its digest
        is recorded in `db_schema_state`), nothing is executed: two catalog
        reads, no DDL, no table locks.
        """
        import psycopg2

        digest = schema_digest()
        try:
            with self.connect() as conn:
                if schema_is_current(conn.cursor(), digest):
                    return
        except psycopg2.Error:
            pass  # fall through to the full pass, which reports real failures

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
                    # Another job may have applied it while this one waited.
                    if schema_is_current(cur, digest):
                        return
                    for table_sql in TABLES:
                        cur.execute(table_sql)
                    for migration_sql in MIGRATIONS:
                        cur.execute(migration_sql)
                    for index_sql in INDEXES:
                        cur.execute(index_sql)
                    cur.execute(SCHEMA_STATE_DDL)
                    cur.execute(
                        "INSERT INTO db_schema_state (id, digest, applied_at) VALUES (1, %s, NOW()) "
                        "ON CONFLICT (id) DO UPDATE SET digest = EXCLUDED.digest, applied_at = NOW()",
                        (digest,),
                    )
                return
            except retryable:
                if attempt == attempts - 1:
                    raise
                time.sleep(2 ** attempt)
