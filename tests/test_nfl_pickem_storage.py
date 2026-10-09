from __future__ import annotations

import pytest

from model.nfl_matchup_study import digest
from research import nfl_pickem_storage as storage


class Cursor:
    def __init__(self, db):
        self.db = db
        self.rowcount = 0
        self._rows: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params):
        self.db.statements.append(sql)
        if sql.lstrip().startswith("UPDATE nfl_pickem_matchup_forecasts SET input_digest"):
            self.db.digests.setdefault(params[1], params[0])
        elif sql.lstrip().startswith("SELECT forecast_id FROM"):
            pending = [k for k in sorted(self.db.inputs) if k not in self.db.compacted]
            self._rows = [{"forecast_id": k} for k in pending[:params[0]]]
        elif "SET payload = nfl_pickem_swap_manifest" in sql:
            ids = params[0]
            verified = [k for k in ids if k not in self.db.unverifiable]
            self.db.compacted.update(verified)
            self.rowcount = len(verified)

    def fetchall(self):
        return self._rows


class Connection:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return Cursor(self.db)


class Database:
    def __init__(self, inputs, *, unverifiable=()):
        self.inputs = inputs
        self.unverifiable = set(unverifiable)
        self.digests: dict[str, str] = {}
        self.compacted: set[str] = set()
        self.statements: list[str] = []

    def connect(self):
        return Connection(self)

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if "information_schema.columns" in sql:
            return []
        if sql.lstrip().startswith("SELECT forecast_id, payload->'input'"):
            pending = [k for k in sorted(self.inputs) if k not in self.digests]
            return [{"forecast_id": k, "input": self.inputs[k]} for k in pending[:params[0]]]
        return []


INPUTS = {f"f{i}": {"gameId": f"g{i}", "baseline": {"homeConditional": .5 + i / 100}} for i in range(5)}


def test_input_digest_matches_the_inline_digest_and_is_never_recomputed():
    db = Database(INPUTS)
    assert storage.fill_input_digests(db, batch=2) == 5
    assert db.digests == {k: digest(v) for k, v in INPUTS.items()}
    assert storage.fill_input_digests(db, batch=2) == 0


def test_only_uncompacted_rows_are_digested():
    db = Database(INPUTS)
    storage.fill_input_digests(db)
    digest_reads = [s for s in db.statements if s.lstrip().startswith("SELECT forecast_id, payload->'input'")]
    assert all("manifest_ref IS NULL" in s for s in digest_reads), "a compacted input holds references, not the manifest"


def test_compaction_runs_in_batches_until_nothing_is_left():
    db = Database(INPUTS)
    assert storage.compact(db, batch=2) == 5
    assert db.compacted == set(INPUTS)


def test_a_batch_that_does_not_rehydrate_exactly_is_refused():
    db = Database(INPUTS, unverifiable={"f1"})
    with pytest.raises(RuntimeError, match="1 of 2 forecasts do not rehydrate"):
        storage.compact(db, batch=2)


def test_columns_are_added_only_when_missing():
    db = Database({})
    storage.ensure(db)
    alters = [s for s in db.statements if s.startswith("ALTER TABLE")]
    assert len(alters) == len(storage.FORECAST_COLUMNS)

