/** Read-only replay of an already frozen run; never writes to the database. */
import fs from 'node:fs';
import assert from 'node:assert/strict';
import { optimizeNflLineups, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { restorePlayers } from '../src/lib/nfl-dfs/frozen-players';
import { applyDiversified, diversifiedPolicy, groupLimit, repeatedGroupCounts } from '../src/lib/nfl-dfs/diversified';

const input = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const players = restorePlayers(input.run.input_snapshot);
const settings: NflOptimizerSettings = applyDiversified({ ...input.run.settings, buildShape: 'diversified',
  diversified: { candidateTarget: Number(process.argv[3] ?? 500) } });
const start = Date.now();
const result = optimizeNflLineups(players, settings);
assert.equal(result.lineups.length, settings.nLineups);
assert.ok(result.exposureReport!.every(r => r.overall <= r.overallMax));
const policy = diversifiedPolicy(settings);
for (const {group,count} of repeatedGroupCounts(result.lineups).values()) assert.ok(count <= groupLimit(group,policy,settings.nLineups));
for (const a of result.lineups) {
  assert.equal(a.playerIds.length,9); assert.equal(new Set(a.playerIds).size,9);
  assert.equal(a.totalSalary,a.slots.reduce((s,p)=>s+p.salary,0)); assert.ok(a.totalSalary<=50_000);
  assert.ok(a.slots.every(s=>!s.player.isOut));
  for (const b of result.lineups) if(a!==b) assert.ok(a.playerIds.filter(id=>b.playerIds.includes(id)).length<=5);
}
console.log(JSON.stringify({elapsedSeconds:(Date.now()-start)/1000,report:result.diversifiedReport,warnings:result.warnings},null,2));
