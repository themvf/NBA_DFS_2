"""Apply the two CFB early-pilot checkpoint names before the hot capture worker."""

from __future__ import annotations

from config import load_config
from db.schema import CLOSE_CAPTURE_CONSTRAINT_DDLS


def migrate(database_url: str) -> bool:
    import psycopg2

    with psycopg2.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '20s'")
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("cfb-early-capture-schema-v1",))
            cursor.execute("""
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conname='odds_capture_checkpoints_checkpoint_check'
                  AND conrelid='odds_capture_checkpoints'::regclass
            """)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("odds capture checkpoint constraint is missing")
            definition = row[0]
            if "cfb_t_minus_7d" in definition and "cfb_t_minus_4d" in definition:
                return False
            for ddl in CLOSE_CAPTURE_CONSTRAINT_DDLS:
                if "odds_capture_checkpoints_checkpoint_check" in ddl:
                    cursor.execute(ddl)
    return True


if __name__ == "__main__":
    print({"cfb_early_checkpoint_constraint_updated": migrate(load_config().database_url or "")})
