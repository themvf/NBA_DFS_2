import assert from 'node:assert/strict';
import fs from 'node:fs';
import {
  BOOM_THRESHOLDS, ROLE_PI, STARTER_THRESHOLDS, computeReplacementUpside, mixDistributions, volumeBaseline,
  type TeamUsageWindow, type UpsideDistribution, type UpsidePlayer,
} from '../src/lib/nfl-dfs/replacement-upside';

const close = (a: number, b: number, tol = 0.05, msg?: string) => assert.ok(Math.abs(a - b) <= tol, msg ?? `${a} != ${b}`);
const weeks = (n: number) => Array.from({ length: n }, (_, i) => ({ season: 2026, week: 10 - i }));
const row = (targets: number, carries: number) => ({ targets, carries });

// Volume baseline: recency-weighted over active games, >= 2 active games.
{
  const w = volumeBaseline([row(10, 0), null, row(4, 0)], 'targets')!;
  const w0 = 1, w2 = 0.5 ** (2 / 4);
  close(w.base, (10 * w0 + 4 * w2) / (w0 + w2), 1e-9);
  assert.equal(w.games, 2);
  assert.equal(volumeBaseline([row(10, 0), null, null], 'targets'), null, 'one active game is not a baseline');
  assert.equal(volumeBaseline(Array(9).fill(null).concat([row(9, 9), row(9, 9)]), 'targets'), null, 'window is 8 games');
}

// Mixture math.
const backup: UpsideDistribution = { mean: 7, p10: 1.5, median: 6, p90: 13, boom: 0.02 };
const starter: UpsideDistribution = { mean: 16, p10: 6, median: 15, p90: 28, boom: 0.2 };
{
  const same = mixDistributions(backup, starter, 0, 25);
  close(same.p10, backup.p10); close(same.median, backup.median); close(same.p90, backup.p90);
  close(same.mean, backup.mean, 1e-9); close(same.boom!, backup.boom!, 1e-9);
  const all = mixDistributions(backup, starter, 1, 25);
  close(all.p10, starter.p10); close(all.median, starter.median); close(all.p90, starter.p90);
  const half = mixDistributions(backup, starter, 0.5, 25);
  close(half.mean, 11.5, 1e-9);
  close(half.boom!, 0.11, 1e-9);
  assert.ok(half.p90 > backup.p90 + 8, `ceiling should move a lot, got ${half.p90}`);
  assert.ok(half.mean - backup.mean < half.p90 - backup.p90, 'the tail moves more than the mean');
  assert.ok(half.p10 < half.median && half.median < half.p90);
  assert.throws(() => mixDistributions(backup, starter, 1.2, 25));
  const noStoredBoom = mixDistributions(backup, { ...starter, boom: null }, 0.5, 25);
  assert.ok(noStoredBoom.boom! > backup.boom! && noStoredBoom.boom! < 0.5, 'boom read off the curve when not stored');
  assert.equal(mixDistributions(backup, starter, 0.5, null).boom, 0.11, 'stored rates used when both present');
}

// A slate: RB1 out after playing last week, WR1 out after playing, TE1 out but already missed last week.
const player = (key: number, name: string, position: string, out: boolean, dist: UpsideDistribution | null): UpsidePlayer =>
  ({ key, name, position, team: 'LAR', out, dist });
const players: UpsidePlayer[] = [
  player(1, 'Starter RB', 'RB', true, starter),
  player(2, 'Backup RB', 'RB', false, backup),
  player(3, 'Third RB', 'RB', false, { mean: 3, p10: 0, median: 2, p90: 8, boom: 0 }),
  player(10, 'Star WR', 'WR', true, { mean: 19, p10: 7, median: 17, p90: 32, boom: 0.25 }),
  player(11, 'WR2', 'WR', false, { mean: 14, p10: 5, median: 13, p90: 24, boom: 0.1 }),
  player(12, 'WR3', 'WR', false, { mean: 8, p10: 2, median: 7, p90: 15, boom: 0.02 }),
  player(20, 'TE1', 'TE', true, { mean: 10, p10: 3, median: 9, p90: 18, boom: 0.06 }),
  player(21, 'TE2', 'TE', false, { mean: 4, p10: 0.5, median: 3, p90: 9, boom: 0.01 }),
];
const window: TeamUsageWindow = {
  team: 'LAR', games: weeks(4),
  usage: {
    1: [row(3, 18), row(2, 16), row(4, 17), row(3, 15)],
    2: [row(1, 3), row(2, 4), row(1, 2), row(0, 3)],
    3: [null, row(0, 1), row(1, 1), null],
    10: [row(10, 0), row(9, 0), row(11, 0), row(8, 0)],
    11: [row(7, 0), row(6, 0), row(7, 0), row(8, 0)],
    12: [row(3, 0), row(4, 0), row(2, 0), row(3, 0)],
    20: [null, row(6, 0), row(5, 0), row(7, 0)],
    21: [row(2, 0), row(1, 0), row(1, 0), row(2, 0)],
  },
};
{
  const before = JSON.stringify({ players, window });
  const report = computeReplacementUpside(players, [window]);
  assert.equal(JSON.stringify({ players, window }), before, 'no input mutation');
  assert.deepEqual(computeReplacementUpside(players, [window]), report, 'deterministic');
  const byKey = new Map(report.upside.map((u) => [u.key, u]));
  assert.equal(byKey.get(2)?.pi, ROLE_PI.RB.lead, 'lead back gets the lead chance');
  assert.equal(byKey.get(2)?.role, 'lead');
  assert.equal(byKey.get(3)?.pi, ROLE_PI.RB.other);
  assert.equal(byKey.get(2)?.from.name, 'Starter RB');
  assert.ok(byKey.get(2)!.ifStarterRole.p90 > byKey.get(2)!.baseline.p90 + 5);
  assert.equal(byKey.get(2)!.baseline.p90, backup.p90, 'baseline shown untouched');
  assert.ok(!byKey.has(11), 'top remaining WR: his own games already carry the ceiling (pi = 0)');
  assert.deepEqual(report.unchanged.map((u) => [u.key, u.from]), [[11, 'Star WR']], 'the unadjusted lead WR is explained, not silent');
  assert.equal(byKey.get(12)?.pi, ROLE_PI.WR.other);
  assert.ok(!byKey.has(21), 'TE1 already missed last week: not a first-game absence');
  assert.ok(!byKey.has(1) && !byKey.has(10), 'ruled-out players never receive upside');
  assert.ok(byKey.get(2)!.note.includes('50%'));
}

// Thresholds and gates.
{
  const light = { ...window, usage: { ...window.usage, 1: [row(0, 8), row(0, 9), row(0, 8), row(0, 9)] } };
  assert.ok(!computeReplacementUpside(players, [light]).upside.some((u) => u.room === 'RB'), `under ${STARTER_THRESHOLDS.RB.min} carries is not a starter`);
  const noDist = players.map((p) => (p.key === 1 ? { ...p, dist: null } : p));
  const report = computeReplacementUpside(noDist, [window]);
  assert.ok(report.skipped.some((s) => s.name === 'Starter RB' && /No stored projection/.test(s.reason)), 'never silent');
  const zeroed = players.map((p) => (p.key === 1 ? { ...p, dist: { mean: 0, p10: 0, median: 0, p90: 0, boom: 0 } } : p));
  const z = computeReplacementUpside(zeroed, [window]);
  assert.ok(!z.upside.some((u) => u.room === 'RB') && z.skipped.some((s) => s.name === 'Starter RB'), 'a zeroed starter is skipped, not mixed in');
  assert.equal(computeReplacementUpside(players, []).upside.length, 0, 'no usage window, no flags');
  const otherTeam = computeReplacementUpside(players.map((p) => ({ ...p, team: p.key === 2 ? 'SEA' : p.team })), [window]);
  assert.ok(!otherTeam.upside.some((u) => u.key === 2), 'upside stays within the team');
}

// A fullback behind a halfback: no FB boom line, so no boom rate is invented.
{
  const fb = players.map((p) => (p.key === 2 ? { ...p, position: 'FB' } : p));
  const u = computeReplacementUpside(fb, [window]).upside.find((x) => x.key === 2)!;
  assert.equal(u.ifStarterRole.boom, null);
  assert.equal(BOOM_THRESHOLDS.FB, undefined);
}

// The app's chances must be exactly what model/nfl_replacement_upside_fit.py shipped.
{
  const fit = JSON.parse(fs.readFileSync('../artifacts/nfl_replacement_upside_v1_fit.json', 'utf8'));
  for (const room of ['RB', 'TE', 'WR'] as const) {
    for (const role of ['lead', 'other'] as const) {
      assert.equal(ROLE_PI[room][role], fit.families[room][role].shipped_pi, `${room} ${role} chance drifted from the fit artifact`);
    }
  }
  assert.equal(fit.families.WR.TE.shipped_pi, 0, 'a TE is not a WR-room recipient');
  assert.equal(fit.version, 'nfl-replacement-upside-v1');
}

console.log('Verified replacement upside: baselines, first-game trigger, role chances, mixture quantiles, boom, gates and determinism passed');
