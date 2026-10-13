import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import { buildPair, digest, restorePlayers, type Raw } from './nfl-classic-pool-study-core';
import { optimizeNflLineups, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';

// Discovery fixture is ONLY an offline regression test. No database enrollment.
const archived = JSON.parse(gunzipSync(readFileSync('../artifacts/nfl-classic-attribution-2026-10-13/inputs/2026-w5-classic.json.gz')).toString());
function fixture() {
  const players = archived.players.map((p: Raw) => {
    const q = { ...p }; delete q.actual; delete q.actualSource; return q;
  });
  const settings = { ...archived.run.settings, nLineups: 20 };
  const source: Raw = { run_id: 'offline-regression', settings, input_snapshot: players,
    optimizer_version: archived.run.optimizerVersion, build_info: null };
  sign(source);
  return { source, players, pairId: 'test-pair', season: 2026, week: 5,
    capturedAt: '2026-10-11T12:00:00Z', firstKickoff: '2026-10-11T17:00:00Z',
    implementation: { commit: 'offline-test', digest: 'fixture' }, priorOpportunityCapture: {} };
}
function sign(source: Raw) {
  source.input_digest = digest({ settings: source.settings, inputSnapshot: source.input_snapshot,
    optimizerVersion: source.optimizer_version });
}
const input = fixture(), before = digest(input), pair = buildPair(input);
assert.equal(digest(input), before, 'source and settings immutable');
assert.equal(pair.arms.length, 2);
const [a, b] = pair.arms;
assert.equal(a.status, 'research_complete');
assert.equal(b.status, 'research_complete');
assert.equal(a.lineups.length, 20); assert.equal(b.lineups.length, 20);
assert.deepEqual(a.inputSnapshot, b.inputSnapshot, 'identical frozen projections, ownership, roles, prior opportunity');
const strip = (s: Raw) => { const q = { ...s }; delete q.excludedPlayerIds; delete q.researchStudy; delete q.runEvidence; return q; };
assert.deepEqual(strip(a.settings), strip(b.settings), 'all non-exclusion settings identical');
const positions = new Map(input.players.map((p: Raw) => [p.dkPlayerId, p.position]));
const admitted = a.settings.excludedPlayerIds.filter((id: number) => !b.settings.excludedPlayerIds.includes(id));
assert.ok(admitted.length > 0);
assert.ok(admitted.every((id: number) => ['TE', 'WR'].includes(String(positions.get(id)))));
assert.deepEqual(admitted, a.settings.researchStudy.removedRulePlayerIds);
const native = optimizeNflLineups(restorePlayers(input.players), { ...input.source.settings, buildShape: 'winners' } as NflOptimizerSettings);
assert.deepEqual(a.lineups.map(l => [l.playerIds, l.projectedFpts, l.totalSalary]),
  native.lineups.map(l => [l.playerIds, l.projectedFpts, l.totalSalary]), 'arm A exactly matches native Winners generation');
assert.deepEqual(buildPair(input), pair, 'fixed-seed paired build is reproducible');
for (const arm of pair.arms) {
  assert.equal(arm.inputDigest, digest({ settings: arm.settings, inputSnapshot: arm.inputSnapshot,
    optimizerVersion: arm.optimizerVersion, buildInfo: arm.buildInfo }));
}
const conflict = fixture();
conflict.source.settings.minExposureByPlayer = { [admitted[0]]: 0.1 };
sign(conflict.source);
const failure = buildPair(conflict);
assert.equal(failure.arms[0].status, 'research_failed', 'impossible minimum remains a recorded failure');
assert.equal(failure.arms[0].lineups.length, 0);
assert.match(failure.arms[0].failureReason!, /minimum exposure/);
assert.equal(failure.arms[1].settings.minExposureByPlayer[admitted[0]], 0.1, 'no retry/relaxation');
const manual = fixture();
manual.source.settings.excludedPlayerIds = [...manual.source.settings.excludedPlayerIds, admitted[0]];
sign(manual.source);
const manualPair = buildPair(manual);
assert.ok(manualPair.arms.every(arm => arm.settings.excludedPlayerIds.includes(admitted[0])), 'manual exclusions survive both arms');
const corrupt = fixture(); corrupt.source.settings.randomness += 0.01;
assert.throws(() => buildPair(corrupt), /digest mismatch/);
const cash = fixture(); cash.source.settings.mode = 'cash'; sign(cash.source);
assert.throws(() => buildPair(cash), /Classic GPP/);
const drift = fixture(); drift.source.settings.buildShape = 'winners';
drift.source.settings.runEvidence = { eligibility: [] }; sign(drift.source);
assert.throws(() => buildPair(drift), /Captured Winners exclusions differ/, 'reconstruction drift cannot silently enroll');
console.log('Classic pool study: native parity, isolation, determinism, failures, manual exclusions and integrity passed.');
