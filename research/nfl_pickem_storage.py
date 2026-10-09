"""Content-addressed storage for frozen pick'em matchup forecasts.

A frozen forecast carries the same matchup source manifest five times: once as
`featureManifest` and once in every feature's `sourceManifest`. Compaction stores
each distinct manifest once in `nfl_matchup_manifests` and replaces every copy
with `{"manifestRef": <hash>}`. Rehydrating with the stored body reproduces the
original payload exactly; the update is refused unless it does.

The freezer and its refresh are hash-pinned by registered studies, so they keep
writing full payloads and this module compacts them afterwards.
"""

from __future__ import annotations

from model.nfl_matchup_study import digest

DIGEST_BATCH = 50
COMPACT_BATCH = 200

FUNCTIONS = """
CREATE OR REPLACE FUNCTION nfl_pickem_swap_manifest(payload jsonb, replacement jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
  SELECT jsonb_set(
    CASE WHEN jsonb_typeof(payload->'input'->'features') = 'array' THEN
      jsonb_set(payload, '{input,features}', (
        SELECT coalesce(jsonb_agg(CASE WHEN feature ? 'sourceManifest'
                                       THEN jsonb_set(feature, '{sourceManifest}', replacement)
                                       ELSE feature END ORDER BY position), '[]'::jsonb)
        FROM jsonb_array_elements(payload->'input'->'features') WITH ORDINALITY AS f(feature, position)))
    ELSE payload END,
    '{featureManifest}', replacement)
$$;
CREATE OR REPLACE FUNCTION nfl_pickem_manifest_hash(body jsonb) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT encode(sha256(convert_to(body::text, 'UTF8')), 'hex') $$;
CREATE TABLE IF NOT EXISTS nfl_matchup_manifests (
  manifest_hash TEXT PRIMARY KEY, body JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
"""

FORECAST_COLUMNS = {"input_digest": "TEXT", "manifest_ref": "TEXT"}

MANIFEST_JOIN_SQL = "LEFT JOIN nfl_matchup_manifests m ON m.manifest_hash = f.manifest_ref"
FEATURE_MANIFEST_SQL = "COALESCE(m.body, f.payload->'featureManifest')"



def ensure(db) -> None:
    """Columns are added only when missing: ALTER TABLE locks a table the close worker writes every 5 minutes."""
    db.execute(FUNCTIONS)
    present = {row["column_name"] for row in db.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_name = 'nfl_pickem_matchup_forecasts' AND column_name = ANY(%s)""",
        (list(FORECAST_COLUMNS),))}
    for column, kind in FORECAST_COLUMNS.items():
        if column not in present:
            db.execute(f"ALTER TABLE nfl_pickem_matchup_forecasts ADD COLUMN IF NOT EXISTS {column} {kind}")


def fill_input_digests(db, batch: int = DIGEST_BATCH) -> int:
    """digest(payload.input) of the full, uncompacted payload; grading compares it with the registration."""
    filled = 0
    while rows := db.execute(
            """SELECT forecast_id, payload->'input' input FROM nfl_pickem_matchup_forecasts
               WHERE input_digest IS NULL AND manifest_ref IS NULL ORDER BY forecast_id LIMIT %s""", (batch,)):
        with db.connect() as connection, connection.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    """UPDATE nfl_pickem_matchup_forecasts SET input_digest = %s
                       WHERE forecast_id = %s AND input_digest IS NULL AND manifest_ref IS NULL""",
                    (digest(row["input"]), row["forecast_id"]))
        filled += len(rows)
    return filled


def compact(db, batch: int = COMPACT_BATCH) -> int:
    """Every candidate must compact: one that cannot rolls back its batch and fails the run."""
    compacted = 0
    while True:
        with db.connect() as connection, connection.cursor() as cursor:
            cursor.execute("""SELECT forecast_id FROM nfl_pickem_matchup_forecasts
                              WHERE manifest_ref IS NULL AND input_digest IS NOT NULL
                              ORDER BY forecast_id LIMIT %s FOR UPDATE SKIP LOCKED""", (batch,))
            ids = [row["forecast_id"] for row in cursor.fetchall()]
            if not ids:
                return compacted
            cursor.execute(
                """INSERT INTO nfl_matchup_manifests (manifest_hash, body)
                   SELECT DISTINCT ON (hash) hash, body FROM (
                     SELECT nfl_pickem_manifest_hash(payload->'featureManifest') hash, payload->'featureManifest' body
                     FROM nfl_pickem_matchup_forecasts
                     WHERE forecast_id = ANY(%s) AND jsonb_typeof(payload->'featureManifest') = 'object') manifests
                   ON CONFLICT (manifest_hash) DO NOTHING""", (ids,))
            cursor.execute(
                """UPDATE nfl_pickem_matchup_forecasts f
                   SET payload = nfl_pickem_swap_manifest(f.payload, jsonb_build_object('manifestRef', m.manifest_hash)),
                       manifest_ref = m.manifest_hash
                   FROM nfl_matchup_manifests m
                   WHERE f.forecast_id = ANY(%s)
                     AND m.manifest_hash = nfl_pickem_manifest_hash(f.payload->'featureManifest')
                     AND m.body = f.payload->'featureManifest'
                     AND nfl_pickem_swap_manifest(
                           nfl_pickem_swap_manifest(f.payload, jsonb_build_object('manifestRef', m.manifest_hash)),
                           m.body) = f.payload""", (ids,))
            if cursor.rowcount != len(ids):
                raise RuntimeError(f"pick'em compaction: {len(ids) - cursor.rowcount} of {len(ids)} forecasts do not "
                                   "rehydrate to their original payload (the freezer's payload shape changed); "
                                   "batch rolled back")
        compacted += len(ids)


def maintain(db) -> dict:
    ensure(db)
    return {"digests_filled": fill_input_digests(db), "compacted": compact(db)}


def main() -> int:
    import json
    from config import load_config
    from ingest.nfl_dfs_weekly import PipelineDatabase
    print(json.dumps(maintain(PipelineDatabase(load_config().database_url, initialize_schema=False))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
