"""Validate or explicitly apply the additive CFB context schema."""

from __future__ import annotations

import argparse

from config import load_config
from db.cfb_context_schema import DDL, SCHEMA_VERSION


def migrate(database_url: str, *, apply: bool) -> dict:
    import psycopg2

    with psycopg2.connect(database_url) as connection:
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute("SET LOCAL lock_timeout='30s'")
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (SCHEMA_VERSION,))
        cursor.execute(DDL)
        table_count = cursor.execute(
            "SELECT COUNT(*) FROM pg_tables WHERE schemaname=current_schema() AND tablename LIKE 'cfb_%'"
        ) or cursor.fetchone()[0]
        if apply:
            connection.commit()
        else:
            connection.rollback()
    return {"schema_version": SCHEMA_VERSION, "mode": "applied" if apply else "validated_rollback", "cfb_table_count_seen": table_count}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit the migration; default validates then rolls back")
    args = parser.parse_args()
    print(migrate(load_config().database_url or "", apply=args.apply))


if __name__ == "__main__":
    main()
