"""Database manager for Neon PostgreSQL.

Uses psycopg2 with a connection wrapper for consistent API.
All queries use %s placeholders (native PostgreSQL).
"""

from __future__ import annotations

from contextlib import contextmanager

from db.schema import TABLES, INDEXES, MIGRATIONS


class DatabaseManager:
    def __init__(self, database_url: str) -> None:
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self.database_url = database_url
        self._ensure_schema()

    @contextmanager
    def connect(self):
        """Yield a psycopg2 connection with RealDictCursor.

        Auto-commits on clean exit, rolls back on exception.
        """
        import psycopg2
        from psycopg2.extras import RealDictCursor

        conn = psycopg2.connect(self.database_url, cursor_factory=RealDictCursor)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
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
        """Create all tables, indexes, and run idempotent column migrations."""
        with self.connect() as conn:
            cur = conn.cursor()
            for table_sql in TABLES:
                cur.execute(table_sql)
            for index_sql in INDEXES:
                cur.execute(index_sql)
            for migration_sql in MIGRATIONS:
                cur.execute(migration_sql)
