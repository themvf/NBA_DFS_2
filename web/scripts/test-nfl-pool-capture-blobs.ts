import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { canonicalAuditJson } from '../src/lib/nfl-dfs/audit-json';
import { compactPoolPayload, POOL_BLOB_MARKER, poolBlobDigest, poolBlobRefs, rehydratePoolPayload } from '../src/lib/nfl-dfs/pool-capture-blobs';

const sha = (v: unknown) => createHash('sha256').update(canonicalAuditJson(v)).digest('hex');
const teams = [{ team: 'MIN', workload: { carries: 25.5, targets: 31 }, notes: ['a', 'b'], missing: null },
  { team: 'ATL', workload: { carries: 22, targets: 35.25 }, evidence: { é: 'ünïcode', n: 1e-7 } }];
const payload = { version: 'v1', uploadId: 'u', origin: 'live_pool', game: { id: 1 },
  players: [{ dkPlayerId: 1, evidence: { replacementUpside: null } }],
  context: { modelVersion: 'm', situationTeams: teams, replacementUpside: { version: 'v2' }, warnings: [] } };

// Compaction keeps the digest of the full payload, through a JSON (jsonb) round trip.
const { stored, blobs } = compactPoolPayload(payload);
assert.equal(blobs.length, 1);
assert.equal(blobs[0].digest, poolBlobDigest(teams));
assert.deepEqual((stored.context as Record<string, unknown>).situationTeams, { [POOL_BLOB_MARKER]: blobs[0].digest });
assert.deepEqual(poolBlobRefs(stored), [blobs[0].digest]);
assert.deepEqual(payload.context.situationTeams, teams, 'input is not mutated');
const viaJsonb = JSON.parse(canonicalAuditJson(stored));
const blobViaJsonb = new Map([[blobs[0].digest, JSON.parse(canonicalAuditJson(blobs[0].content))]]);
const restored = rehydratePoolPayload(viaJsonb, blobViaJsonb);
assert.equal(sha(restored), sha(payload));
assert.deepEqual(restored, JSON.parse(canonicalAuditJson(payload)));
assert.ok(canonicalAuditJson(stored).length < canonicalAuditJson(payload).length);

// Identical slate-wide values share one blob.
assert.equal(compactPoolPayload({ ...payload, game: { id: 2 } }).blobs[0].digest, blobs[0].digest);

// Missing blob is loud; uncompacted payloads pass through untouched.
assert.throws(() => rehydratePoolPayload(viaJsonb, new Map()), /missing/);
for (const plain of [{ ...payload, context: { ...payload.context, situationTeams: [] } },
  { ...payload, context: { modelVersion: 'm' } }, { ...payload, context: null }, { version: 'v1' }]) {
  const out = compactPoolPayload(plain);
  assert.equal(out.stored, plain);
  assert.deepEqual(out.blobs, []);
  assert.deepEqual(poolBlobRefs(plain), []);
  assert.equal(rehydratePoolPayload(plain, new Map()), plain);
}
// An object that merely resembles a ref but has extra keys is not one.
const lookalike = { ...payload, context: { situationTeams: { [POOL_BLOB_MARKER]: 'x', other: 1 } } };
assert.deepEqual(poolBlobRefs(lookalike), []);
assert.equal(rehydratePoolPayload(lookalike, new Map()), lookalike);
console.log('pool capture blob tests passed');
