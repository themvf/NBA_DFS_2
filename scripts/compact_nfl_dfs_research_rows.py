"""Owner-run: compact stored NFL DFS research row lists, then reclaim the space.

Two tables keep a full copy of 18k-57k history rows on every write while
consecutive copies are almost identical (measured 2026-10-31):

  nfl_dfs_feature_audits.input_evidence   958 MB, 143 rows
  nfl_dfs_component_datasets.payload      418 MB, 177 rows

Nothing reads these lists (the web reads feature-audit `payload` only; no code
reads a component dataset's payload). The writers now store them as
content-addressed chunks (model/nfl_dfs_row_chunks.py). This script converts
the rows written before that, one row per transaction:

  1. read the row and compact its lists in memory;
  2. insert the chunks, read them back and rehydrate (store_compact refuses a
     round trip that is not exact);
  3. check the record's own digests on the rehydrated value: each feature
     audit dataset's `input_digest`, and a component dataset's primary key;
  4. UPDATE the row only if it is byte-identical to what was read (md5 guard);
  5. read the updated row back, rehydrate it again and compare with the
     original before COMMIT. Any failure rolls that row back and stops.

`audit_digest`, `dataset_digest`, `payload` (feature audits) and every run that
references a dataset are untouched. A dry run (the default) does steps 1 and 3
in memory and reports what would be written; it changes nothing.

`--vacuum-full` then rewrites nfl_dfs_feature_audits,
nfl_dfs_component_datasets and nfl_dfs_weekly_report_cards (the last holds
~360 MB of space freed by the report-card retention pass that was never
returned). VACUUM FULL takes an ACCESS EXCLUSIVE lock on each table for its
duration (seconds to a few minutes here): the research workflow's feature
audit / workload / efficiency / report-card steps and the /dfs/nfl/model and
report-card pages block until it finishes. Run it when no research workflow is
running. `--vacuum-pool-captures` also rewrites nfl_dfs_pool_captures, after
web/scripts/compact-nfl-pool-captures.ts has compacted it (that lock blocks
the per-minute pool-capture cron and /dfs/nfl/pool-review; do it on a day
with no NFL kickoff in the next 24 hours). `--only-vacuum` skips compaction.

Reading every row transfers ~5-6 GB from Neon once (rows are sent
uncompressed). Usage:

  python -u scripts/compact_nfl_dfs_research_rows.py                 # dry run
  python -u scripts/compact_nfl_dfs_research_rows.py --apply
  python -u scripts/compact_nfl_dfs_research_rows.py --apply --vacuum-full
  python -u scripts/compact_nfl_dfs_research_rows.py --apply --only-vacuum --vacuum-pool-captures
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, RealDictCursor

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import load_config  # noqa: E402
from db.schema import TABLES  # noqa: E402
from model.nfl_dfs_feature_audit import digest  # noqa: E402
from model import nfl_dfs_row_chunks as rc  # noqa: E402

TARGETS = {
    "feature_audits": {"table": "nfl_dfs_feature_audits", "key": "audit_digest", "column": "input_evidence",
                       "fields": ("frozen_history", "working_source")},
    "component_datasets": {"table": "nfl_dfs_component_datasets", "key": "dataset_digest", "column": "payload",
                           "fields": ("players", "teams")},
}
VACUUM_TABLES = ("nfl_dfs_feature_audits", "nfl_dfs_component_datasets", "nfl_dfs_weekly_report_cards",
                 "nfl_dfs_row_chunks")
POOL_VACUUM_TABLES = ("nfl_dfs_pool_captures", "nfl_dfs_pool_capture_blobs")
DRY_CHUNKS: dict[str, int] = {}  # distinct chunk text sizes a dry run would store


def direct_url() -> str:
    # Direct (non-pooler) endpoint: long transactions and VACUUM, and no session
    # setting can leak into PgBouncer's shared connections.
    return load_config().database_url.replace("-pooler.", ".")


def check_digests(name: str, key: str, restored: dict, extra: dict) -> None:
    if name == "feature_audits":
        for ds in (extra.get("payload") or {}).get("datasets", []):
            rows = restored.get(ds["dataset"])
            if rows is None:
                continue
            got = digest(sorted(rows, key=lambda r: r["record_id"]))
            if got != ds["input_digest"]:
                raise RuntimeError(f"{key}: {ds['dataset']} input_digest {got} != stored {ds['input_digest']}")
    elif digest(restored) != key:
        raise RuntimeError(f"{key}: rehydrated dataset digest does not equal its primary key")


def pending(cur, spec) -> list[str]:
    checks = " OR ".join(f"jsonb_typeof({spec['column']}->'{f}') = 'array'" for f in spec["fields"])
    cur.execute(f"SELECT {spec['key']} k FROM {spec['table']} WHERE {checks} ORDER BY created_at")
    return [r["k"] for r in cur.fetchall()]


def compact_one(conn, name, spec, key, apply) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '600s'")
        extra_cols = ", payload" if name == "feature_audits" else ""
        cur.execute(f"""SELECT {spec['column']} v, md5({spec['column']}::text) m{extra_cols}
            FROM {spec['table']} WHERE {spec['key']}=%s{' FOR UPDATE' if apply else ''}""", (key,))
        row = cur.fetchone()
        original = row["v"]
        before = len(rc.canonical(original))
        if not apply:
            compacted, bodies = rc.compact_fields(original, spec["fields"])
            bodies = {k: json.loads(rc.canonical(v)) for k, v in bodies.items()}
            restored = {f: rc.rehydrate(v, bodies) if rc.is_ref(v) else v for f, v in compacted.items()}
            if restored != original:
                raise RuntimeError(f"{key}: in-memory round trip differs")
            check_digests(name, key, restored, row)
            DRY_CHUNKS.update({k: len(rc.canonical(v)) for k, v in bodies.items()})
            conn.rollback()
            return before, len(rc.canonical(compacted))
        compacted = rc.store_compact(cur, original, spec["fields"])
        check_digests(name, key, rc.rehydrate_fields(compacted, cur), row)
        cur.execute(f"""UPDATE {spec['table']} SET {spec['column']}=%s
            WHERE {spec['key']}=%s AND md5({spec['column']}::text)=%s""",
            (Json(compacted, dumps=rc.canonical), key, row["m"]))
        if cur.rowcount != 1:
            raise RuntimeError(f"{key}: row changed while compacting (updated {cur.rowcount})")
        cur.execute(f"SELECT {spec['column']} v FROM {spec['table']} WHERE {spec['key']}=%s", (key,))
        if rc.rehydrate_fields(cur.fetchone()["v"], cur) != original:
            raise RuntimeError(f"{key}: stored row does not rehydrate to the original; rolled back")
    conn.commit()
    return before, len(rc.canonical(compacted))


def sizes(cur) -> dict:
    cur.execute("""SELECT relname t, pg_total_relation_size(oid)/1048576 mb FROM pg_class
        WHERE relname = ANY(%s) AND relkind='r'""", (list(VACUUM_TABLES + POOL_VACUUM_TABLES),))
    return {r["t"]: r["mb"] for r in cur.fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write; without it nothing changes")
    parser.add_argument("--only", choices=sorted(TARGETS), action="append", help="limit to one table (repeatable)")
    parser.add_argument("--limit", type=int, help="process at most N rows per table")
    parser.add_argument("--vacuum-full", action="store_true", help="after --apply, VACUUM (FULL, ANALYZE) the tables")
    parser.add_argument("--vacuum-pool-captures", action="store_true",
                        help="also VACUUM FULL nfl_dfs_pool_captures (after web/scripts/compact-nfl-pool-captures.ts)")
    parser.add_argument("--only-vacuum", action="store_true", help="skip compaction; run only the requested vacuums (needs --apply)")
    args = parser.parse_args()
    conn = psycopg2.connect(direct_url(), cursor_factory=RealDictCursor)
    if not args.apply:
        # Each transaction BEGINs READ ONLY. This connection never turns on
        # autocommit (that would make the setting session-wide); VACUUM below
        # uses its own connection.
        conn.readonly = True
    with conn.cursor() as cur:
        print("sizes before (MB):", sizes(cur))
        if args.apply:
            ddl = next(t for t in TABLES if "CREATE TABLE IF NOT EXISTS nfl_dfs_row_chunks" in t)
            cur.execute(ddl)
    conn.commit()
    for name in [] if args.only_vacuum else (args.only or sorted(TARGETS)):
        spec = TARGETS[name]
        with conn.cursor() as cur:
            keys = pending(cur, spec)
        conn.commit()
        keys = keys[:args.limit] if args.limit else keys
        total_before = total_after = 0
        for i, key in enumerate(keys, 1):
            try:
                b, a = compact_one(conn, name, spec, key, args.apply)
            except Exception:
                conn.rollback()
                raise
            total_before += b
            total_after += a
            print(f"{name} {i}/{len(keys)} {key[:16]} {b/1e6:.1f} MB -> {a/1e6:.2f} MB"
                  f" {'compacted' if args.apply else '(dry run)'}", flush=True)
        print(f"{name}: {len(keys)} rows, row text {total_before/1e6:.0f} MB -> {total_after/1e6:.1f} MB"
              f" plus shared chunks")
    tables = (VACUUM_TABLES if args.vacuum_full else ()) + (POOL_VACUUM_TABLES if args.vacuum_pool_captures else ())
    if tables and not args.apply:
        print("vacuum flags are ignored without --apply")
    elif tables:
        vacuum_conn = psycopg2.connect(direct_url(), cursor_factory=RealDictCursor)
        vacuum_conn.autocommit = True  # VACUUM cannot run inside a transaction
        with vacuum_conn.cursor() as cur:
            for table in tables:
                cur.execute("SELECT to_regclass(%s) t", (table,))
                if cur.fetchone()["t"] is None:
                    continue
                print(f"VACUUM (FULL, ANALYZE) {table} ...", flush=True)
                cur.execute(f"VACUUM (FULL, ANALYZE) {table}")
        vacuum_conn.close()
    if not args.apply:
        print(f"dry run: {len(DRY_CHUNKS)} distinct chunks, {sum(DRY_CHUNKS.values())/1e6:.0f} MB of text"
              " (jsonb stores it compressed, roughly a quarter of that)")
    with conn.cursor() as cur:
        print("sizes after (MB):", sizes(cur))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
