"""Content-addressed storage for large row lists in NFL DFS research records.

Why (measured 2026-10-31): `nfl_dfs_feature_audits.input_evidence` (958 MB,
143 rows) and `nfl_dfs_component_datasets.payload` (418 MB, 177 rows) each
store a full copy of 18k-57k history rows on every write, while consecutive
copies differ in a handful of rows (raw components: 4 of 18,312; feature
audit working rows: 0 of 20,160 between the last two audits). Nothing reads
these lists back; they are kept so a record can be replayed exactly.

A row list is cut into content-defined chunks (a chunk ends after a row whose
digest falls in 1/CHUNK_TARGET of hash space), each chunk is stored once in
`nfl_dfs_row_chunks` keyed by the sha256 of its canonical JSON, and the record
keeps `{"$rowChunks": {...}}` naming the chunks in order. A changed row
rewrites only its own chunk, and an inserted row shifts no boundaries but its
neighbours'. Rehydrating returns the original list exactly, in order; every
writer checks that before it commits.

The digests the records are keyed and graded by (`audit_digest`,
`dataset_digest`, each dataset's `input_digest`) are computed from the full,
uncompacted values, so they do not change.
"""
from __future__ import annotations

import json
from hashlib import sha256
from typing import Iterable

VERSION = "nfl-dfs-row-chunks-v1"
MARKER = "$rowChunks"
CHUNK_TARGET = 256     # expected rows per chunk
CHUNK_MAX = 2048       # hard cap so one pathological run cannot form a huge chunk
# The table is declared in db/schema.py (nfl_dfs_row_chunks).


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def _digest(value) -> str:
    return sha256(canonical(value).encode()).hexdigest()


def chunk(rows: list) -> list[list]:
    """Split `rows` into content-defined chunks; concatenating them returns `rows`."""
    chunks, current = [], []
    for row in rows:
        current.append(row)
        if int(_digest(row)[:8], 16) % CHUNK_TARGET == 0 or len(current) >= CHUNK_MAX:
            chunks.append(current)
            current = []
    if current:
        chunks.append(current)
    return chunks


def is_ref(value) -> bool:
    return isinstance(value, dict) and set(value) == {MARKER}


def compact(rows: list) -> tuple[dict, dict[str, list]]:
    """(reference, {chunk_digest: rows}) for one row list."""
    bodies: dict[str, list] = {}
    order = []
    for part in chunk(rows):
        key = _digest(part)
        bodies[key] = part
        order.append(key)
    return {MARKER: {"version": VERSION, "count": len(rows), "chunks": order}}, bodies


def rehydrate(ref: dict, bodies: dict[str, list]) -> list:
    meta = ref[MARKER]
    if meta.get("version") != VERSION:
        raise ValueError(f"Unsupported row-chunk reference {meta.get('version')!r}")
    rows: list = []
    for key in meta["chunks"]:
        if key not in bodies:
            raise ValueError(f"Row chunk {key} is missing")
        rows.extend(bodies[key])
    if len(rows) != meta["count"]:
        raise ValueError(f"Row chunk reference expects {meta['count']} rows, got {len(rows)}")
    return rows


def compact_fields(record: dict, fields: Iterable[str]) -> tuple[dict, dict[str, list]]:
    """Replace each named top-level list in `record` with a chunk reference."""
    out, bodies = dict(record), {}
    for name in fields:
        value = record.get(name)
        if isinstance(value, list):
            out[name], parts = compact(value)
            bodies.update(parts)
    return out, bodies


def chunk_keys(record: dict) -> set[str]:
    return {key for value in record.values() if is_ref(value) for key in value[MARKER]["chunks"]}


def fetch_chunks(cursor, keys: Iterable[str]) -> dict[str, list]:
    keys = sorted(set(keys))
    if not keys:
        return {}
    cursor.execute("SELECT chunk_digest, rows FROM nfl_dfs_row_chunks WHERE chunk_digest = ANY(%s)", (keys,))
    return {(r["chunk_digest"] if isinstance(r, dict) else r[0]): (r["rows"] if isinstance(r, dict) else r[1])
            for r in cursor.fetchall()}


def rehydrate_fields(record: dict, cursor) -> dict:
    """The original record: every chunk reference replaced by its rows."""
    bodies = fetch_chunks(cursor, chunk_keys(record))
    return {k: rehydrate(v, bodies) if is_ref(v) else v for k, v in record.items()}


def store_compact(cursor, record: dict, fields: Iterable[str]) -> dict:
    """Insert the record's chunks, read them back, prove the round trip, return the compact record.

    Runs inside the caller's transaction, so a failed proof leaves nothing behind.
    """
    from psycopg2.extras import Json, execute_values

    compacted, bodies = compact_fields(record, fields)
    if bodies:
        execute_values(cursor, """INSERT INTO nfl_dfs_row_chunks (chunk_digest, row_count, rows) VALUES %s
            ON CONFLICT (chunk_digest) DO NOTHING""",
            [(key, len(rows), Json(rows, dumps=canonical)) for key, rows in bodies.items()], page_size=100)
    restored = rehydrate_fields(compacted, cursor)
    original = json.loads(canonical(record))
    if canonical(restored) != canonical(original):
        raise RuntimeError("Row-chunk round trip does not reproduce the record; refusing to store it compacted")
    return compacted
