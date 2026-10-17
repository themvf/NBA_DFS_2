import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import { buildPair, digest, restorePlayers, STUDIES, type Raw } from './nfl-pool-rule-study-core';
import { optimizeNflLineups, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { FIELD_STACKING } from '../src/lib/nfl-dfs/portfolio-objective';

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
assert.deepEqual(a.lineups.map((l: Raw) => [l.playerIds, l.projectedFpts, l.totalSalary]),
  native.lineups.map((l: Raw) => [l.playerIds, l.projectedFpts, l.totalSalary]), 'arm A exactly matches native Winners generation');
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
// ---- Showdown kicker study (nfl-showdown-kicker-rule-v1), offline on the week-5 discovery fixture ----
const sdArchived = JSON.parse(gunzipSync(readFileSync('../artifacts/nfl-showdown-attribution-2026-10-13/inputs/2026-w5-BUF_at_LAR.json.gz')).toString());
function sdFixture(): Raw {
  const players = sdArchived.players.map((p: Raw) => { const q = { ...p }; delete q.actual; delete q.actualSource; delete q.eligibilityClass; return q; });
  const settings = { ...sdArchived.run.settings, nLineups: 20 };
  const source: Raw = { run_id: 'offline-showdown', settings, input_snapshot: players,
    optimizer_version: sdArchived.run.optimizerVersion, build_info: null };
  sign(source);
  return { version: STUDIES.showdown, source, players, pairId: 'sd-pair', season: 2026, week: 5, gameIds: [77],
    capturedAt: '2026-10-11T12:00:00Z', firstKickoff: '2026-10-12T00:20:00Z',
    implementation: { commit: 'offline-test', digest: 'fixture' }, priorOpportunityCapture: null };
}
const sdInput = sdFixture(), sdBefore = digest(sdInput), sdPair = buildPair(sdInput);
assert.equal(digest(sdInput), sdBefore, 'showdown inputs immutable');
assert.equal(sdPair.version, STUDIES.showdown);
const [sa, sb] = sdPair.arms;
// Winners shape v6/v7 Showdown rules (a QB in every lineup, the captain stack,
// the winners' QB and captain distributions) can make a planned archetype
// lineup unbuildable; it folds into Standard ceiling and the run reports
// partial. Both arms must still build all 20, and the only issues allowed are
// those plan-count lines (Double fade, for one, fades both quarterbacks here).
const foldOnly = (arm: Raw, label: string) => {
  assert.ok(['research_complete', 'research_partial'].includes(arm.status), `${label}: ${arm.status} (${arm.failureReason})`);
  for (const issue of arm.settings.researchStudy.issues) assert.match(issue, /^[A-Za-z /-]+: \d+ of \d+ requested lineups\.$/, `${label}: only plan folds, got "${issue}"`);
};
foldOnly(sa, 'showdown arm A'); foldOnly(sb, 'showdown arm B');
assert.equal(sa.lineups.length, 20); assert.equal(sb.lineups.length, 20);
assert.equal(sa.settings.excludeKickers, true); assert.equal(sb.settings.excludeKickers, false);
const sdStrip = (s: Raw) => { const q = strip(s); delete q.excludeKickers; return q; };
assert.deepEqual(sdStrip(sa.settings), sdStrip(sb.settings), 'showdown arms differ only in the kicker ban');
assert.deepEqual(sa.settings.excludedPlayerIds, sb.settings.excludedPlayerIds, 'same manual exclusions in both arms');
const sdPositions = new Map(sdInput.players.map((p: Raw) => [p.dkPlayerId, p.position]));
const kickers = sdInput.players.filter((p: Raw) => p.position === 'K').map((p: Raw) => p.dkPlayerId);
assert.ok(kickers.length > 0, 'fixture has kickers');
assert.deepEqual([...sa.settings.researchStudy.removedRulePlayerIds].sort(), [...kickers].sort());
assert.ok(sa.lineups.every((l: Raw) => l.playerIds.every((id: number) => sdPositions.get(id) !== 'K')), 'arm A never plays a kicker');
assert.deepEqual(sb.settings.researchStudy.ruleExclusions, [], 'arm B excludes nothing by rule');
assert.deepEqual(sa.settings.researchStudy.gameIds, [77]);
const sdNative = optimizeNflLineups(restorePlayers(sdInput.players), { ...sdInput.source.settings, buildShape: 'winners' } as NflOptimizerSettings);
assert.deepEqual(sa.lineups.map((l: Raw) => [l.playerIds, l.projectedFpts, l.totalSalary]),
  sdNative.lineups.map((l: Raw) => [l.playerIds, l.projectedFpts, l.totalSalary]), 'showdown arm A exactly matches native Winners generation');
assert.deepEqual(buildPair(sdInput), sdPair, 'showdown paired build is reproducible');
const sdWrong = sdFixture(); sdWrong.version = STUDIES.classic;
assert.throws(() => buildPair(sdWrong), /Classic GPP/, 'a showdown source cannot enroll in the Classic study');
const clWrong = fixture() as Raw; clWrong.version = STUDIES.showdown;
assert.throws(() => buildPair(clWrong), /Showdown GPP/, 'a classic source cannot enroll in the Showdown study');
const unknown = sdFixture(); unknown.version = 'nfl-something-else';
assert.throws(() => buildPair(unknown), /Unknown study/);
console.log(`Showdown kicker study: arm B played a kicker in ${sb.lineups.filter((l: Raw) => l.playerIds.some((id: number) => sdPositions.get(id) === 'K')).length} of 20 lineups.`);
// ---- Portfolio objective study (nfl-portfolio-objective-v2): both formats, offline on the week-5 discovery fixtures ----
function portfolioFixture(base: Raw, format: 'classic' | 'showdown'): Raw {
  const f = structuredClone(base);
  f.version = STUDIES.portfolio;
  f.format = format;
  f.sourceLineups = [];
  return f;
}
for (const [label, base] of [['showdown', sdFixture()], ['classic', fixture()]] as const) {
  const pin = portfolioFixture(base as Raw, label);
  const pBefore = digest(pin), pp = buildPair(pin);
  assert.equal(digest(pin), pBefore, `${label}: portfolio inputs immutable`);
  assert.equal(pp.version, STUDIES.portfolio);
  const [pa, pb] = pp.arms;
  // Showdown arm A is the production build; plan lineups may fold under the
  // v6/v7 Showdown rules (see the kicker study above). Classic is unchanged.
  if (label === 'showdown') foldOnly(pa, `${label} arm A`);
  else assert.equal(pa.status, 'research_complete', `${label}: arm A built`);
  // Arm B (the portfolio challenger, unenrolled: its freeze gate failed) may
  // select fewer than 20 in Showdown: the v6/v7 rules leave fewer distinct
  // candidates that also meet the exposure cap. The shortfall must be reported.
  const expectB = pb.lineups.length;
  if (label === 'showdown') assert.ok(expectB >= 15 && expectB <= 20, `${label}: arm B selected ${expectB}`);
  else assert.equal(expectB, 20);
  assert.equal(pb.status, expectB === 20 ? 'research_complete' : 'research_partial', `${label}: arm B built (${pb.failureReason})`);
  assert.equal(pa.lineups.length, 20);
  const nativeP = optimizeNflLineups(restorePlayers(pin.players), { ...pin.source.settings, buildShape: 'winners' } as NflOptimizerSettings);
  assert.deepEqual(pa.lineups.map((l: Raw) => [l.playerIds, l.projectedFpts, l.totalSalary]),
    nativeP.lineups.map((l: Raw) => [l.playerIds, l.projectedFpts, l.totalSalary]), `${label}: arm A is the production build`);
  const sA = pa.settings.researchStudy, sB = pb.settings.researchStudy;
  assert.deepEqual(strip(pa.settings), strip(pb.settings), `${label}: arms share every setting`);
  assert.deepEqual(sA.commonSettings, sB.commonSettings);
  assert.ok(sB.candidatePool.distinct >= 20 && sB.candidatePool.distinct <= 200 + 20, `${label}: candidate pool ${sB.candidatePool.distinct}`);
  assert.ok(sB.candidatePool.fromArmA === 20);
  assert.equal(sB.selection.shortfall, 20 - expectB);
  assert.ok(sB.selection.selectionSuccess > 0 && sB.selection.selectionSuccess <= 1);
  for (const arm of [sA, sB]) {
    assert.ok(arm.forecast.successTop1Pct >= 0 && arm.forecast.successTop1Pct <= 1, `${label}: forecast in [0,1]`);
    // The freeze gate (research/nfl_portfolio_freeze.py) judges field fidelity; here only that it is measured and bounded.
    assert.ok(Number.isFinite(arm.fieldCalibration.maxAbsDeviationPct) && arm.fieldCalibration.maxAbsDeviationPct < 60, `${label}: field calibration recorded (max dev ${arm.fieldCalibration.maxAbsDeviationPct}pp)`);
    assert.equal(arm.fieldLineups, 2000); assert.equal(arm.draws, 4000);
    // v2 field: the sampled lineups carry the measured stack shapes (realized, read off the lineups), within 5pp of the stated rates.
    const shapes = arm.fieldShapes[label];
    if (label === 'classic') assert.ok(Math.abs(shapes.qbStack - FIELD_STACKING.classic.qbStack) < 0.05, `${label}: field QB-stack rate ${shapes.qbStack}`);
    else for (const [k, v] of Object.entries(FIELD_STACKING.showdown.split)) assert.ok(Math.abs(shapes.split[`${k}-${6 - Number(k)}`] - v) < 0.05, `${label}: field split ${k} = ${shapes.split[`${k}-${6 - Number(k)}`]}`);
    assert.ok(arm.impliedField.top1Pct.p50 > arm.impliedField.median.p50);
  }
  // Selection honours the source's exposure cap and uniqueness on the chosen set.
  // The cap is a share of the requested lineups (20), which is what selection enforces, even when it selects fewer.
  const n = pb.lineups.length, cap = Math.floor(pin.source.settings.maxExposure * 20 + 1e-9);
  const counts = new Map<number, number>();
  for (const l of pb.lineups) for (const id of l.playerIds) counts.set(id, (counts.get(id) ?? 0) + 1);
  assert.ok([...counts.values()].every(c => c <= cap), `${label}: exposure cap ${cap} respected`);
  for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
    const shared = pb.lineups[i].playerIds.filter((id: number) => pb.lineups[j].playerIds.includes(id)).length;
    assert.ok(pb.lineups[i].playerIds.length - shared >= pin.source.settings.minUnique, `${label}: minUnique respected`);
  }
  // Every arm-B lineup came from the candidate pool, which the production optimizer built: legal by construction.
  const keysB = new Set(pb.lineups.map((l: Raw) => [...l.playerIds].sort().join(',')));
  assert.equal(keysB.size, n, `${label}: arm B lineups distinct`);
  assert.deepEqual(buildPair(pin), pp, `${label}: portfolio pair is reproducible`);
  console.log(`Portfolio study ${label}: A forecast ${sA.forecast.successTop1Pct.toFixed(3)}, B forecast ${sB.forecast.successTop1Pct.toFixed(3)}, B kept ${pb.lineups.filter((l: Raw) => pa.lineups.some((a: Raw) => a.playerIds.join() === l.playerIds.join())).length} of A's 20, pool ${sB.candidatePool.distinct}, field max dev ${sA.fieldCalibration.maxAbsDeviationPct}pp.`);
}
const cashP = portfolioFixture(sdFixture() as Raw, 'showdown'); cashP.source.settings.mode = 'cash'; sign(cashP.source);
assert.throws(() => buildPair(cashP), /GPP source/);
console.log('Pool-rule studies (Classic TE/WR, Showdown kicker, portfolio objective): native parity, isolation, determinism, failures, manual exclusions and integrity passed.');
