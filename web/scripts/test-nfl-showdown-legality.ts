import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { parseNflDkSalaryCsv } from '../src/lib/nfl-dfs/dk-salary-csv';
import { assertShowdownLineup } from '../src/lib/nfl-dfs/showdown-legality';
import { exportNflDkEntries } from '../src/lib/nfl-dfs/entry-export';
import { runNflPreExportQa } from '../src/lib/nfl-dfs/pre-export-qa';

const settings: NflOptimizerSettings = {
  format: 'showdown', mode: 'cash', projectionSource: 'dk_avg', allowDkFallback: false,
  nLineups: 1, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 0,
  bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [],
  minExposureByPlayer: {}, maxExposureByPlayer: {},
};
const header = 'Position,Name + ID,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame';
const csv = [header, ...[10000, 9000, 8000, 7500, 7000, 6500, 5000, 3000].flatMap((salary, i) =>
  ['CPT', 'FLEX'].map(slot => `WR,Player ${i},Player ${i},${(slot === 'CPT' ? 100 : 200) + i},${slot},${salary * (slot === 'CPT' ? 1.5 : 1)},AAA@BBB 09/24/2026 08:15PM ET,${i % 2 ? 'AAA' : 'BBB'},${30 - i * 2}`))].join('\n');
function pool(content: string): NflOptimizerPlayer[] {
  return parseNflDkSalaryCsv(content).players.map(p => ({
    id: p.dkPlayerId, dkPlayerId: p.dkPlayerId, captainDkPlayerId: p.captain?.dkPlayerId ?? null,
    captainSalary: p.captain?.salary ?? null, salary: p.salary, name: p.name,
    position: p.position, team: p.teamAbbrev, opponent: p.opponent, gameKey: p.gameKey,
    rosterPositions: p.rosterPositions, isOut: false, projectionStatus: 'historical',
    avgFptsDk: p.avgFptsDk, ourProj: null, floorFpts: null, ceilingFpts: null,
    boomRate: null, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
  }));
}
const players = pool(csv);
const lineup = optimizeNflLineups(players, settings).lineups[0];
assert.ok(lineup);
assertShowdownLineup(lineup);
assert.equal(lineup.slots[0].salary, lineup.slots[0].player.salary * 1.5);
// Exhaustive independent oracle: pay the captain premium exactly once.
let best = -Infinity;
for (let captain = 0; captain < players.length; captain++) {
  for (let mask = 0; mask < 1 << players.length; mask++) {
    const flex = players.filter((_, i) => (mask & (1 << i)) !== 0);
    if (flex.length !== 5 || (mask & (1 << captain))) continue;
    const p = players[captain];
    if (p.salary * 1.5 + flex.reduce((s, f) => s + f.salary, 0) > 50000) continue;
    if (new Set([p, ...flex].map(f => f.team)).size !== 2) continue;
    best = Math.max(best, p.avgFptsDk! * 1.5 + flex.reduce((s, f) => s + f.avgFptsDk!, 0));
  }
}
assert.equal(lineup.projectedFpts, best);
const entries = 'Entry ID,Contest Name,CPT,FLEX,FLEX,FLEX,FLEX,FLEX\n1,Test,,,,,,';
assert.ok(exportNflDkEntries(entries, [lineup]).includes(`(${lineup.slots[0].player.captainDkPlayerId})`));
for (const corrupt of [
  (l: typeof lineup) => { l.slots[0].salary = l.slots[0].player.salary; l.totalSalary = l.slots.reduce((s, e) => s + e.salary, 0); },
  (l: typeof lineup) => { l.slots[0].player.captainSalary = l.slots[0].player.salary; },
  (l: typeof lineup) => { l.slots[0].player.captainDkPlayerId = null; },
  (l: typeof lineup) => { l.slots[0].slot = 'FLEX'; },
  (l: typeof lineup) => { l.slots[1].player = l.slots[0].player; },
  (l: typeof lineup) => { l.slots.forEach(e => { e.player.team = 'AAA'; }); },
  (l: typeof lineup) => { l.slots[1].player.gameKey = 'CCC@DDD'; },
  (l: typeof lineup) => { l.slots[1].player.rosterPositions = ['CPT']; },
  (l: typeof lineup) => { l.totalSalary = NaN; },
  (l: typeof lineup) => { l.slots.forEach(e => { e.player.salary = 10000; e.player.captainSalary = 15000; e.salary = e.slot === 'CPT' ? 15000 : 10000; }); l.totalSalary = 49000; },
]) {
  const bad = structuredClone(lineup);
  corrupt(bad);
  assert.throws(() => exportNflDkEntries(entries, [bad]));
  const qa = runNflPreExportQa({ format: 'showdown', requestedLineups: 1,
    lineups: [{ ...bad, slots: bad.slots.map(s => ({ ...s, playerId: s.player.dkPlayerId })) }] });
  assert.ok(qa.openBlockers.includes('legal_roster'));
}
assert.throws(() => optimizeNflLineups(players.map(p => ({ ...p, captainSalary: p.salary })), settings), /Captain price/);
const captainOnly = { ...players[0], dkPlayerId: players[0].captainDkPlayerId!, rosterPositions: ['CPT'], avgFptsDk: 100 };
const withCaptainOnly = optimizeNflLineups([captainOnly, ...players.slice(1)], { ...settings, lockedPlayerIds: [captainOnly.dkPlayerId] }).lineups[0];
assert.equal(withCaptainOnly.slots[0].player.dkPlayerId, captainOnly.dkPlayerId);
if (process.argv[2]) {
  const real = optimizeNflLineups(pool(readFileSync(process.argv[2], 'utf8')), { ...settings, nLineups: 5 });
  assert.equal(real.lineups.length, 5);
  real.lineups.forEach(assertShowdownLineup);
  console.log('Supplied CSV: five legal lineups, Captain pricing and totals verified.');
}
console.log('Showdown generation, independent optimum, export, and QA regression checks passed.');
