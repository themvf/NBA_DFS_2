import json

import psycopg2.extras
import pytest

from model import nfl_dfs_row_chunks as rc
from model.nfl_dfs_feature_audit import digest


def rows(n, start=0, tag="a"):
    return [{"record_id": str(i), "season": 2025, "week": 1 + i % 18, "values": {"targets": i % 7, "yards": i * 1.5},
             "source_hash": f"{tag}{i}", "captured_at": "2026-10-01T00:00:00+00:00"} for i in range(start, start + n)]


class FakeCursor:
    """Stores chunk rows as JSON text, as jsonb would, and answers the read-back query."""

    def __init__(self):
        self.table = {}
        self._result = []

    def execute(self, sql, params=()):
        if "SELECT chunk_digest, rows FROM nfl_dfs_row_chunks" in sql:
            self._result = [{"chunk_digest": k, "rows": json.loads(self.table[k])} for k in params[0] if k in self.table]
        else:
            raise AssertionError(sql)

    def fetchall(self):
        return self._result


@pytest.fixture
def cursor(monkeypatch):
    fake = FakeCursor()

    def execute_values(cur, sql, values, page_size=100):
        assert "ON CONFLICT (chunk_digest) DO NOTHING" in sql
        for key, count, body in values:
            text = body.dumps(body.adapted)
            assert len(json.loads(text)) == count
            cur.table.setdefault(key, text)

    monkeypatch.setattr(psycopg2.extras, "execute_values", execute_values)
    return fake


def test_round_trip_is_exact_and_in_order():
    data = rows(5000)
    ref, bodies = rc.compact(data)
    assert rc.is_ref(ref) and ref[rc.MARKER]["count"] == 5000
    assert rc.rehydrate(ref, bodies) == data
    assert 5 <= len(ref[rc.MARKER]["chunks"]) <= 60


def test_digests_of_the_full_value_are_unchanged():
    data = rows(3000)
    record = {"version": "v", "players": data, "teams": rows(40, tag="t")}
    compacted, bodies = rc.compact_fields(record, ("players", "teams"))
    restored = {k: rc.rehydrate(v, bodies) if rc.is_ref(v) else v for k, v in compacted.items()}
    assert digest(restored) == digest(record)
    assert digest(sorted(restored["players"], key=lambda r: r["record_id"])) == digest(sorted(data, key=lambda r: r["record_id"]))


def test_one_changed_row_rewrites_one_chunk():
    before = rows(8000)
    after = [dict(r) for r in before]
    after[4321] = {**after[4321], "source_hash": "changed"}
    a = rc.compact(before)[0][rc.MARKER]["chunks"]
    b = rc.compact(after)[0][rc.MARKER]["chunks"]
    assert len(set(b) - set(a)) == 1


def test_appended_rows_keep_earlier_chunks():
    before = rows(6000)
    after = before + rows(300, start=6000)
    a = rc.compact(before)[0][rc.MARKER]["chunks"]
    b = rc.compact(after)[0][rc.MARKER]["chunks"]
    assert a[:-1] == b[:len(a) - 1]


def test_missing_chunk_or_wrong_count_raises():
    ref, bodies = rc.compact(rows(2000))
    first = ref[rc.MARKER]["chunks"][0]
    with pytest.raises(ValueError, match="missing"):
        rc.rehydrate(ref, {k: v for k, v in bodies.items() if k != first})
    bad = {rc.MARKER: {**ref[rc.MARKER], "count": 1}}
    with pytest.raises(ValueError, match="expects"):
        rc.rehydrate(bad, bodies)


def test_store_compact_proves_round_trip_and_dedupes(cursor):
    record = {"frozen_history": rows(4000, tag="f"), "working_source": rows(2500, tag="w")}
    stored = rc.store_compact(cursor, record, ("frozen_history", "working_source"))
    assert all(rc.is_ref(v) for v in stored.values())
    assert rc.rehydrate_fields(stored, cursor) == record
    n = len(cursor.table)
    again = {**record, "working_source": record["working_source"] + rows(10, start=2500, tag="w")}
    rc.store_compact(cursor, again, ("frozen_history", "working_source"))
    assert len(cursor.table) - n <= 2  # only the tail chunk is new


def test_store_compact_refuses_a_lossy_round_trip(cursor, monkeypatch):
    record = {"players": rows(600)}
    real = rc.rehydrate_fields
    monkeypatch.setattr(rc, "rehydrate_fields", lambda rec, cur: {**real(rec, cur), "players": real(rec, cur)["players"][:-1]})
    with pytest.raises(RuntimeError, match="round trip"):
        rc.store_compact(cursor, record, ("players",))


def test_non_list_fields_are_left_alone():
    record = {"version": "x", "players": "not a list", "source_rows": 3}
    compacted, bodies = rc.compact_fields(record, ("players",))
    assert compacted == record and bodies == {}
