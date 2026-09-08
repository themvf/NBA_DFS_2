import assert from 'node:assert/strict';
import { restoreSavedLineups, savedSlateLabel, type SavedLineupRow } from '../src/lib/nfl-dfs/saved-workspace';
import { exportNflDkEntries } from '../src/lib/nfl-dfs/entry-export';

assert.equal(savedSlateLabel('showdown', 'NE@SEA 09/09/2026 08:20PM ET', ['NE@SEA']), 'Wednesday, Sep 9, 2026 · Showdown · NE@SEA');
assert.match(savedSlateLabel('classic', 'NO@DET 09/13/2026 01:00PM ET', Array(12).fill('GAME')), /Sunday, Sep 13, 2026 · Classic · 12 games/);
const snapshot = Array.from({ length: 6 }, (_, i) => ({ dkPlayerId: i + 1, captainDkPlayerId: 100 + i,
  name: `Player ${i}`, position: 'WR', team: i < 3 ? 'NE' : 'SEA', salary: 5000, captainSalary: 7500,
  ourProj: 10, floor: -1, ceiling: 20, dkAvg: 8, ownership: 0 }));
const rows: SavedLineupRow[] = [{ lineupNumber: 1, slots: snapshot.map((p, i) => ({ slot: i ? `FLEX${i}` : 'CPT',
  dkPlayerId: p.dkPlayerId, captainDkPlayerId: p.captainDkPlayerId, name: p.name, team: p.team,
  salary: i ? 5000 : 7500, projection: i ? 10 : 15, source: 'our' })), playerIds: [1, 2, 3, 4, 5, 6],
  totalSalary: 32500, projectedFpts: 65, floorFpts: -6.5, ceilingFpts: 130, projectedOwnership: 0,
  stackSummary: { quarterback: null, passCatchers: [], bringBack: null } }];
const restored = restoreSavedLineups(snapshot, rows);
assert.equal(restored[0].slots[0].player.captainDkPlayerId, 100);
assert.equal(restored[0].slots[0].multiplier, 1.5);
assert.equal(restored[0].floorFpts, -6.5);
assert.equal(restored[0].projectedFpts, 65);
assert.equal(restored[0].slots[1].player.ourProj, 10);
const csv = exportNflDkEntries('Entry ID,Contest Name,CPT,FLEX,FLEX,FLEX,FLEX,FLEX\n123,Test,,,,,,', restored);
assert.match(csv, /Player 0 \(100\)/);
assert.match(csv, /Player 1 \(2\)/);
assert.throws(() => restoreSavedLineups([], rows), /missing from its snapshot/);
assert.throws(() => restoreSavedLineups(snapshot, [{ ...rows[0], floorFpts: null }]), /legacy run/);
console.log('Saved workspace: date labels, frozen scores, Captain/Flex export, missing snapshot checks passed.');

async function verifyDatabase() {
  const { listSavedNflSlates, loadSavedNflWorkspace, loadSavedNflLineups } = await import('../src/app/dfs/nfl/actions');
  const library = await listSavedNflSlates();
  const wed = library.find(s => s.label.includes('Wednesday, Sep 9, 2026'));
  const sun = library.find(s => s.label.includes('Sunday, Sep 13, 2026'));
  assert.ok(wed); assert.ok(sun);
  const w = await loadSavedNflWorkspace(wed.uploadId);
  const s = await loadSavedNflWorkspace(sun.uploadId);
  assert.equal(w.slate.format, 'showdown'); assert.equal(w.slate.players.length, 68);
  assert.equal(s.slate.format, 'classic'); assert.equal(s.slate.players.length, 719);
  if (s.runs[0]) {
    const restored = await loadSavedNflLineups(sun.uploadId, s.runs[0].runId);
    assert.equal(restored.lineups.length, s.runs[0].count);
    await assert.rejects(loadSavedNflLineups(wed.uploadId, s.runs[0].runId), /different slate/);
  }
  console.log(JSON.stringify({ library, sundayRuns: s.runs.length, wednesdayRuns: w.runs.length }));
}
if (process.argv.includes('--database')) verifyDatabase().catch(error => { console.error(error); process.exitCode = 1; });
