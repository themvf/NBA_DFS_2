/**
 * nfl-portfolio-objective-v3: the faster field thresholds and greedy selection
 * must give v2's results exactly. The reference functions below are v2's code,
 * copied verbatim (commit a9681524) so this test keeps proving equality after
 * the module changes. Runs offline on the week-5 discovery fixtures.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import { drawJointBank, sampleField, fieldThresholds, fieldThresholdsMany, scoreLineup, selectPortfolio, slateFromSnapshot,
  PORTFOLIO_OBJECTIVE, PORTFOLIO_OBJECTIVE_VERSION, type FieldSample, type JointBank, type SelectionConstraints } from '../src/lib/nfl-dfs/portfolio-objective';
import type { NflLineup } from '../src/lib/nfl-dfs/lineups';
import { normalQuantile, allowedOutside, pit, verdict, GATE } from './research-nfl-portfolio-freeze';

// ---- v2 reference implementation (verbatim) ----
function v2FieldThresholds(field: FieldSample, bank: JointBank, fraction: number): Float64Array {
  const scored = field.lineups.map(l => scoreLineup(l, bank));
  const k = Math.max(1, Math.ceil(fraction * field.lineups.length));
  const out = new Float64Array(bank.draws);
  const column = new Float64Array(scored.length);
  for (let d = 0; d < bank.draws; d++) {
    for (let i = 0; i < scored.length; i++) column[i] = scored[i][d];
    column.sort().reverse();
    out[d] = column[k - 1];
  }
  return out;
}
const v2Covered = (rows: Float64Array[], threshold: Float64Array) => {
  let hits = 0;
  for (let d = 0; d < threshold.length; d++) if (rows.some(r => r[d] >= threshold[d])) hits++;
  return hits / threshold.length;
};
function v2SelectPortfolio(candidates: Array<{ key: string; lineup: NflLineup; draws: Float64Array }>, threshold: Float64Array,
  constraints: SelectionConstraints) {
  const chosen: number[] = [];
  const exposure = new Map<number, number>();
  const slotsPer = candidates[0]?.lineup.length ?? 0;
  const capFor = (id: number) => Math.min(constraints.maxExposure, constraints.maxExposureByPlayer[String(id)] ?? 1);
  const allowed = (i: number) => {
    const ids = candidates[i].lineup.map(e => e.playerId);
    for (const id of ids) if ((exposure.get(id) ?? 0) + 1 > Math.floor(capFor(id) * constraints.count + 1e-9)) return false;
    for (const j of chosen) {
      const other = new Set(candidates[j].lineup.map(e => e.playerId));
      const shared = ids.filter(id => other.has(id)).length;
      if (slotsPer - shared < constraints.minUnique) return false;
    }
    return true;
  };
  const selected: Array<{ key: string; index: number; marginalGain: number }> = [];
  let current = 0;
  while (chosen.length < constraints.count) {
    let best: { i: number; gain: number; success: number } | null = null;
    for (let i = 0; i < candidates.length; i++) {
      if (chosen.includes(i) || !allowed(i)) continue;
      const success = v2Covered([...chosen.map(j => candidates[j].draws), candidates[i].draws], threshold);
      const gain = success - current;
      if (!best || gain > best.gain) best = { i, gain, success };
    }
    if (!best) break;
    chosen.push(best.i);
    for (const e of candidates[best.i].lineup) exposure.set(e.playerId, (exposure.get(e.playerId) ?? 0) + 1);
    selected.push({ key: candidates[best.i].key, index: best.i, marginalGain: best.gain });
    current = best.success;
  }
  return { selected, selectionSuccess: current, shortfall: constraints.count - chosen.length };
}

assert.equal(PORTFOLIO_OBJECTIVE_VERSION, 'nfl-portfolio-objective-v3');
assert.equal(PORTFOLIO_OBJECTIVE.draws, 20000);
assert.equal(PORTFOLIO_OBJECTIVE.stabilitySeed, 20261019);

const fixtures = [
  ['showdown', '../artifacts/nfl-showdown-attribution-2026-10-13/inputs/2026-w5-BUF_at_LAR.json.gz'],
  ['classic', '../artifacts/nfl-classic-attribution-2026-10-13/inputs/2026-w5-classic.json.gz'],
] as const;
for (const [format, file] of fixtures) {
  const players = JSON.parse(gunzipSync(readFileSync(file)).toString()).players;
  const slate = slateFromSnapshot(format, players);
  // Few draws keep the test quick; the arithmetic is the same at 20,000.
  const bank = drawJointBank(players, PORTFOLIO_OBJECTIVE.selectionSeed, 1500);
  const field = sampleField(slate, players, PORTFOLIO_OBJECTIVE.fieldSeed);
  const fractions = [PORTFOLIO_OBJECTIVE.successFraction, PORTFOLIO_OBJECTIVE.secondaryFraction, 0, 0.5, 0.25];
  const many = fieldThresholdsMany(field, bank, fractions);
  fractions.forEach((f, j) => {
    const ref = v2FieldThresholds(field, bank, f);
    assert.deepEqual([...many[j]], [...ref], `${format}: thresholds at ${f} equal v2's`);
    assert.deepEqual([...fieldThresholds(field, bank, f)], [...ref], `${format}: single threshold at ${f} equals v2's`);
  });
  // Candidates: 300 sampled field lineups (legal by construction), scored on the bank.
  const candidates = field.lineups.slice(0, 300).map((lineup, i) => ({ key: `c${i}`, lineup, draws: scoreLineup(lineup, bank) }));
  const threshold = many[0];
  for (const constraints of [
    { count: 20, maxExposure: 0.6, maxExposureByPlayer: {}, minUnique: 2 },
    { count: 40, maxExposure: 0.35, maxExposureByPlayer: { [String(candidates[0].lineup[0].playerId)]: 0.1 }, minUnique: 3 },
    { count: 300, maxExposure: 0.2, maxExposureByPlayer: {}, minUnique: 4 },
  ] as SelectionConstraints[]) {
    const fast = selectPortfolio(candidates, threshold, constraints);
    const ref = v2SelectPortfolio(candidates, threshold, constraints);
    assert.deepEqual(fast.selected, ref.selected, `${format}: picks and gains equal v2's (count ${constraints.count})`);
    assert.equal(fast.selectionSuccess, ref.selectionSuccess, `${format}: success equals v2's`);
    assert.equal(fast.shortfall, ref.shortfall, `${format}: shortfall equals v2's`);
    console.log(`${format}: count ${constraints.count} -> ${fast.selected.length} picks, success ${fast.selectionSuccess.toFixed(4)}, shortfall ${fast.shortfall}`);
  }
}

// ---- v3 gate arithmetic (registered rule) ----
assert.ok(Math.abs(normalQuantile(0.975) - 1.959964) < 1e-5 && Math.abs(normalQuantile(0.5)) < 1e-12 && Math.abs(normalQuantile(0.01) + 2.326348) < 1e-5);
assert.equal(allowedOutside(3), 1, 'n = 3 allows one slate outside the 90% band');
assert.equal(allowedOutside(4), 2);
assert.equal(allowedOutside(1), 1);
assert.equal(pit(Float64Array.from([1, 2, 3, 4]), 3), 0.625, 'ties count half');
assert.equal(pit(Float64Array.from([1, 2, 3, 4]), 9), 1);
assert.equal(verdict([0.5, 0.5], [1, 1]).status, 'unchecked');
assert.equal(verdict([0.5, 0.4, 0.6], [0.9, 0.85, 0.8]).status, 'pass');
assert.equal(verdict([0.5, 0.4, 0.6], [0.9, 0.79, 0.8]).status, 'fail', 'one unstable slate fails the format');
assert.equal(verdict([0.97, 0.98, 0.5], [1, 1, 1]).status, 'fail', 'two of three outside the band fails coverage');
const biased = verdict([0.9, 0.9, 0.9], [1, 1, 1]);
assert.ok(biased.status === 'fail' && biased.coverage!.pass && !biased.bias!.pass, 'a consistent one-sided miss at u = 0.9 fails the bias check');
// The registered low power: three slates at u = 0.85 (mean z 1.04) still pass the 1.13 limit.
assert.equal(verdict([0.85, 0.85, 0.85], [1, 1, 1]).status, 'pass');
// Under calibration (u uniform) the two calibration checks together fail about 8% of the time at n = 3.
let s0 = 12345;
const rand = () => { s0 = (s0 * 1103515245 + 12345) % 2147483648; return s0 / 2147483648; };
let fails = 0;
const trials = 100000;
for (let t = 0; t < trials; t++) if (verdict([rand(), rand(), rand()], [1, 1, 1]).status === 'fail') fails++;
assert.ok(fails / trials > 0.06 && fails / trials < 0.10, `calibrated false-fail rate ${fails / trials}`);
assert.equal(GATE.minJaccard, 0.8);
console.log(`Gate rule: calibrated false-fail rate ${(100 * fails / trials).toFixed(1)}% at n = 3.`);
console.log('Portfolio objective v3: thresholds and greedy selection equal v2 exactly on both formats.');
