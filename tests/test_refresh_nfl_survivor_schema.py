"""The survivor refresh must not run per-invocation schema DDL, and must still
refuse to run against an unmigrated database.

`DatabaseManager._ensure_schema` executes every table/migration/index statement
under an advisory lock on every process start. The survivor job died on
`LockNotAvailable` inside that constructor on 4 of 5 scheduled runs
(2026-09-03 .. 09-17), before any survivor step ran, and the season's market
columns sat frozen at 2026-09-08 for two weeks. Skipping the DDL fixes that;
`require_tables` keeps the failure mode loud when the schema truly is missing.
"""

from __future__ import annotations

import pytest

from db.database import DatabaseManager
import ingest.refresh_nfl_survivor as R


class _Db(DatabaseManager):
    def __init__(self, present: set[str]):
        # Bypass the real constructor: no URL, no schema work.
        self.database_url = "postgres://test"
        self.present = present
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        assert "information_schema.tables" in sql
        wanted = params[0]
        return [{"table_name": n} for n in wanted if n in self.present]


def test_require_tables_passes_when_everything_exists() -> None:
    db = _Db(set(R.SURVIVOR_TABLES))
    db.require_tables(R.SURVIVOR_TABLES)
    assert len(db.queries) == 1


def test_require_tables_names_every_missing_table() -> None:
    db = _Db(set(R.SURVIVOR_TABLES) - {"survivor_pools", "nfl_game_win_probs"})
    with pytest.raises(RuntimeError) as exc:
        db.require_tables(R.SURVIVOR_TABLES)
    msg = str(exc.value)
    assert "nfl_game_win_probs" in msg and "survivor_pools" in msg
    assert "schema-initializing entrypoint" in msg


def test_survivor_refresh_constructs_without_schema_ddl() -> None:
    """Source-level guard: the constructor call must pass initialize_schema=False
    and check its tables. Re-adding the default constructor re-introduces the
    lock-timeout failure."""
    src = open(R.__file__, encoding="utf-8").read()
    assert "DatabaseManager(load_config().database_url, initialize_schema=False)" in src
    assert "db.require_tables(SURVIVOR_TABLES)" in src
    assert "DatabaseManager(load_config().database_url)\n" not in src


def test_survivor_table_list_covers_every_table_the_job_touches() -> None:
    """Grep the modules the refresh imports for table names and make sure each
    one is in SURVIVOR_TABLES, so a new table cannot slip past the check."""
    import re
    import ingest.nfl_season_schedule as a
    import ingest.nfl_survivor_odds as b
    import model.nfl_survivor_model as c
    import ingest.survivor_pick_popularity as d
    import model.survivor_settlement as e

    pattern = re.compile(r"\b(?:FROM|INTO|UPDATE|JOIN)\s+((?:nfl|survivor)_[a-z_]+)\b")
    seen: set[str] = set()
    for mod in (R, a, b, c, d, e):
        seen |= set(pattern.findall(open(mod.__file__, encoding="utf-8").read()))
    missing = sorted(seen - set(R.SURVIVOR_TABLES))
    assert not missing, f"tables used but not verified up front: {missing}"
