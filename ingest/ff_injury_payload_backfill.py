"""Owner-run: compact Sleeper payloads already written to ff_player_injury_observations.

The writer compacts new Sleeper rows (ingest.ff_injuries.compact_payload) since
2026-10-31; this moves the ~950k rows written before that. For each existing
Sleeper row whose raw_payload is not yet compact, in id batches:

  1. Re-hash the stored payload the way the writer did (_payload_hash). A row
     whose payload does not re-hash to its own response_hash is LEFT WHOLE and
     counted: compacting it would lose bytes the hash cannot recover.
  2. Insert the full payload into ff_injury_payloads (ON CONFLICT DO NOTHING),
     then check the stored copy for that hash equals this payload.
  3. Replace raw_payload with compact_payload(...) for exactly those ids.

Every reader was replayed with every Sleeper row compacted (45/45 identical,
weeks 5-8, 2026-10-31). The exception is artifacts/capture_nfl_value_history_20261021.py,
a one-off that dumps `i.*`; its output is already frozen under SHA-256.

Dry run by default, inside a READ ONLY transaction. `--apply` writes, one
transaction per batch, through the DIRECT (non-pooler) endpoint. Afterwards run
`VACUUM (ANALYZE) ff_player_injury_observations` so the freed space is reused
and the index-only Sleeper window scan stays off the heap; the file shrinks
only after `VACUUM FULL` (exclusive lock: a quiet hour, by hand).

    python -u -m ingest.ff_injury_payload_backfill            # report only
    python -u -m ingest.ff_injury_payload_backfill --apply
"""
from __future__ import annotations

import argparse
import json

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json, execute_values

from config import load_config
from ingest.ff_injuries import COMPACT_MARKER, _payload_hash, compact_payload

BATCH = 5000
MARKER_KEY = next(iter(COMPACT_MARKER))


def _dumps(value):
    return json.dumps(value, default=str)


def run(apply: bool, batch: int = BATCH, limit: int | None = None) -> dict:
    url = load_config().database_url.replace("-pooler.", ".")
    conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    report = {"applied": apply, "rows_seen": 0, "compacted": 0, "left_whole_hash_mismatch": 0,
              "payloads_inserted": 0, "bytes_before": 0, "bytes_after": 0}
    last_id = 0
    try:
        while True:
            cur = conn.cursor()
            cur.execute("BEGIN" if apply else "BEGIN READ ONLY")
            cur.execute("SET LOCAL statement_timeout = 600000")
            cur.execute(
                """SELECT id, response_hash, raw_payload, pg_column_size(raw_payload) b
                   FROM ff_player_injury_observations
                   WHERE source='sleeper' AND id > %s AND NOT (raw_payload ? %s)
                   ORDER BY id LIMIT %s""",
                (last_id, MARKER_KEY, batch),
            )
            rows = cur.fetchall()
            if not rows:
                cur.execute("ROLLBACK")
                break
            last_id = int(rows[-1]["id"])
            good = []
            for row in rows:
                report["rows_seen"] += 1
                payload = row["raw_payload"]
                if _payload_hash("sleeper", payload) != row["response_hash"]:
                    report["left_whole_hash_mismatch"] += 1
                    continue
                good.append(row)
                report["bytes_before"] += int(row["b"])
                report["bytes_after"] += len(json.dumps(compact_payload("sleeper", payload)))
            if apply and good:
                unique = {r["response_hash"]: r["raw_payload"] for r in good}
                inserted = execute_values(
                    cur,
                    """INSERT INTO ff_injury_payloads (response_hash, source, raw_payload)
                       VALUES %s ON CONFLICT (response_hash) DO NOTHING RETURNING response_hash""",
                    [(h, "sleeper", Json(p, dumps=_dumps)) for h, p in unique.items()],
                    fetch=True,
                )
                report["payloads_inserted"] += len(inserted)
                cur.execute("SELECT response_hash, raw_payload FROM ff_injury_payloads WHERE response_hash = ANY(%s)",
                            (list(unique),))
                stored = {r["response_hash"]: r["raw_payload"] for r in cur.fetchall()}
                bad = [h for h, p in unique.items() if stored.get(h) != p]
                if bad:
                    cur.execute("ROLLBACK")
                    raise RuntimeError(f"{len(bad)} stored payloads differ from the rows they replace; nothing written")
                execute_values(
                    cur,
                    """UPDATE ff_player_injury_observations AS o SET raw_payload = v.compact
                       FROM (VALUES %s) AS v(id, compact) WHERE o.id = v.id""",
                    [(int(r["id"]), Json(compact_payload("sleeper", r["raw_payload"]), dumps=_dumps)) for r in good],
                    template="(%s::bigint, %s::jsonb)",
                    page_size=1000,
                )
                cur.execute("""SELECT count(*) n FROM ff_player_injury_observations
                               WHERE id = ANY(%s) AND raw_payload ? %s""", ([int(r["id"]) for r in good], MARKER_KEY))
                if cur.fetchone()["n"] != len(good):
                    cur.execute("ROLLBACK")
                    raise RuntimeError("compacted row count does not match the batch; batch rolled back")
                cur.execute("COMMIT")
            else:
                cur.execute("ROLLBACK")
            report["compacted"] += len(good)
            print(json.dumps({"through_id": last_id, **report}), flush=True)
            if limit is not None and report["rows_seen"] >= limit:
                break
    finally:
        conn.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--batch", type=int, default=BATCH)
    parser.add_argument("--limit", type=int, help="stop after this many rows (dry-run sampling)")
    args = parser.parse_args()
    print(json.dumps(run(args.apply, args.batch, args.limit), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
