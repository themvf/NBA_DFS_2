/**
 * Owner-run: move each pool capture's slate-wide `context.situationTeams` into
 * nfl_dfs_pool_capture_blobs, leaving `{"$poolBlob": <sha256>}` in its place.
 *
 *   cd web && npx tsx --conditions=react-server scripts/compact-nfl-pool-captures.ts            # dry run
 *   cd web && npx tsx --conditions=react-server scripts/compact-nfl-pool-captures.ts --apply
 *
 * Measured 2026-10-31: 3,261 captures carried 293 distinct situationTeams
 * values, ~1.2 GB of the 1.66 GB table. New captures are written compacted by
 * savePoolCapture; this converts the ones written before.
 *
 * ONLY RUN AFTER the reader change (readPoolCaptures / the replacement-upside
 * grader rehydrating before they verify a digest) is deployed to production
 * and merged to main. An older reader would reject a compacted row as a digest
 * mismatch.
 *
 * Why this is safe:
 *  - A capture's digest is the sha256 of the FULL payload and is not changed.
 *    Readers rehydrate before verifying, so the same digest verifies.
 *  - Each blob is copied inside the database from a capture that holds it, and
 *    its key is recomputed here from what the database returns.
 *  - A capture is rewritten only WHERE its situationTeams equals the blob's
 *    content (jsonb equality), so replacing the reference with the blob gives
 *    back exactly the jsonb that was stored. Each UPDATE is one atomic statement.
 *  - Afterwards one capture per group is read back, rehydrated and checked
 *    against its digest; any failure stops the run.
 *
 * The table is append-only by trigger (nfl_pool_capture_immutable). --apply
 * disables that trigger for the duration and re-enables it in `finally`;
 * the final check asserts it is enabled. Run it when no capture is due (no NFL
 * kickoff in the next 24 hours, e.g. Tuesday/Wednesday): the cron writes only
 * then. Afterwards reclaim the space with
 *   python -u scripts/compact_nfl_dfs_research_rows.py --apply --only-vacuum --vacuum-pool-captures   (from the repo root)
 * (VACUUM FULL takes an ACCESS EXCLUSIVE lock on the table; the capture cron
 * and /dfs/nfl/pool-review wait until it finishes, a few minutes).
 */
import { config } from 'dotenv';
import { createHash } from 'node:crypto';

config({ path: '.env.local', quiet: true });

(async () => {
  const apply = process.argv.includes('--apply');
  const { db } = await import('../src/db');
  const { sql } = await import('drizzle-orm');
  const { canonicalAuditJson } = await import('../src/lib/nfl-dfs/audit-json');
  const { POOL_BLOB_DDL, POOL_BLOB_MARKER, poolBlobDigest, rehydratePoolPayload } = await import('../src/lib/nfl-dfs/pool-capture-blobs');
  const rows = (r: unknown) => (r as { rows: Record<string, unknown>[] }).rows;
  const sha = (v: unknown) => createHash('sha256').update(canonicalAuditJson(v)).digest('hex');
  const inList = (ids: string[]) => sql.join(ids.map((d) => sql`${d}`), sql`, `);

  const found = rows(await db.execute(sql`SELECT digest, md5((payload->'context'->'situationTeams')::text) m,
      pg_column_size(payload->'context'->'situationTeams') bytes
    FROM nfl_dfs_pool_captures
    WHERE jsonb_typeof(payload->'context'->'situationTeams') = 'array'
      AND jsonb_array_length(payload->'context'->'situationTeams') > 0`));
  const groups = new Map<string, string[]>();
  const groupBytes = new Map<string, number>();
  for (const r of found) {
    const m = String(r.m);
    groups.set(m, [...(groups.get(m) ?? []), String(r.digest)]);
    groupBytes.set(m, Number(r.bytes));
  }
  const inlineMB = found.reduce((s, r) => s + Number(r.bytes), 0) / 1048576;
  const distinctMB = [...groupBytes.values()].reduce((s, b) => s + b, 0) / 1048576;
  console.log(JSON.stringify({ apply, inlineCaptures: found.length, distinctValues: groups.size,
    inlineMBUncompressed: +inlineMB.toFixed(0), afterMBUncompressed: +distinctMB.toFixed(0) }));
  if (!apply) return;

  await db.execute(sql.raw(POOL_BLOB_DDL));
  let updated = 0;
  await db.execute(sql`ALTER TABLE nfl_dfs_pool_captures DISABLE TRIGGER nfl_pool_capture_immutable`);
  try {
    let i = 0;
    for (const [m, digests] of groups) {
      i++;
      const rep = digests[0];
      const value = rows(await db.execute(sql`SELECT payload->'context'->'situationTeams' v FROM nfl_dfs_pool_captures WHERE digest=${rep}`))[0].v;
      const key = poolBlobDigest(value);
      await db.execute(sql`INSERT INTO nfl_dfs_pool_capture_blobs (blob_digest, content)
        SELECT ${key}, payload->'context'->'situationTeams' FROM nfl_dfs_pool_captures WHERE digest=${rep}
        ON CONFLICT (blob_digest) DO NOTHING`);
      const blob = rows(await db.execute(sql`SELECT content FROM nfl_dfs_pool_capture_blobs WHERE blob_digest=${key}`))[0];
      if (!blob || poolBlobDigest(blob.content) !== key) throw new Error(`blob ${key} did not store its value (group ${m})`);
      for (let s = 0; s < digests.length; s += 50) {
        const part = digests.slice(s, s + 50);
        const res = rows(await db.execute(sql`UPDATE nfl_dfs_pool_captures c SET payload =
            jsonb_set(c.payload, '{context,situationTeams}', jsonb_build_object(${POOL_BLOB_MARKER}::text, b.blob_digest))
          FROM nfl_dfs_pool_capture_blobs b
          WHERE b.blob_digest=${key} AND c.digest IN (${inList(part)})
            AND c.payload->'context'->'situationTeams' = b.content
          RETURNING c.digest`));
        updated += res.length;
        if (res.length !== part.length) console.warn(`group ${m}: ${part.length - res.length} capture(s) left inline (value differs)`);
      }
      // One full read-back per group: rehydrate and check the capture's own digest.
      const back = rows(await db.execute(sql`SELECT digest, payload FROM nfl_dfs_pool_captures WHERE digest=${rep}`))[0];
      const restored = rehydratePoolPayload(back.payload, new Map([[key, blob.content]]));
      if (sha(restored) !== String(back.digest).split(':')[0]) throw new Error(`capture ${rep} no longer verifies; stopping`);
      console.log(`${i}/${groups.size} ${key.slice(0, 12)} ${digests.length} captures`);
    }
  } finally {
    await db.execute(sql`ALTER TABLE nfl_dfs_pool_captures ENABLE TRIGGER nfl_pool_capture_immutable`);
  }
  const trigger = rows(await db.execute(sql`SELECT tgenabled FROM pg_trigger WHERE tgname='nfl_pool_capture_immutable'`))[0];
  if (trigger?.tgenabled !== 'O') throw new Error('append-only trigger is not enabled; re-enable it now');
  const left = rows(await db.execute(sql`SELECT count(*)::int n FROM nfl_dfs_pool_captures
    WHERE jsonb_typeof(payload->'context'->'situationTeams') = 'array' AND jsonb_array_length(payload->'context'->'situationTeams') > 0`))[0];
  console.log(JSON.stringify({ updated, stillInline: left.n, appendOnlyTrigger: 'enabled' }));
})().catch((e) => { console.error(e); process.exitCode = 1; });
