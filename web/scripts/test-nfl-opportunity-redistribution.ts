import assert from 'node:assert/strict';
import fixture from './fixtures/nfl-jones-opportunity.json';
import { MAX_MULTIPLIER, MIN_OBSERVED_GAMES, POOLS, VERSION, hasObservedOpportunity,
  inheritanceNote, redistributeOutOpportunity, type RedistributionRow } from '../src/lib/nfl-dfs/opportunity-redistribution';
import { resolveOpportunityProjection } from '../src/lib/nfl-dfs/resolved-projection';
import { scoreNflOffense, scoreNflOffenseLinear } from '../src/lib/nfl-dfs/scoring';

const player = (over: Partial<RedistributionRow> = {}): RedistributionRow => ({
  key: 1, name: 'QB1', position: 'QB', team: 'MIN', isOut: true, historyGames: 17, depthOrder: 1,
  statMeans: { attempts: 34, passing_yards: 250, carries: 5, rushing_yards: 30 },
  ourProj: 15, floorFpts: 5, ceilingFpts: 30, ...over,
});
const backup = player({ key: 2, name: 'QB2', isOut: false, depthOrder: 2,
  statMeans: {attempts:12,passing_yards:96,passing_interceptions:.5,carries:2,rushing_yards:12}, ourProj:5.74 });
const base = {projectionStatus:'historical',ourProj:backup.ourProj,floorFpts:5,medianFpts:12,
  ceilingFpts:30,boomRate:.01,statMeans:backup.statMeans};

// Captured saved slate: the old v1 donor pool exactly reproduces the report.
const rows: RedistributionRow[] = fixture.rows.map(row => ({...row,
  statMeans: Object.fromEntries(Object.entries(row.statMeans).flatMap(([k,v]) => typeof v === 'number' ? [[k,v]] : [])),
}));
const original = structuredClone(rows);
const jones = rows.find(r => r.name === 'Aaron Jones Sr.')!;
const oldDonors = rows.filter(r => r.isOut && (r.historyGames ?? 0) >= 2 && r.statMeans.carries > 0);
const eligible = rows.filter(r => !r.isOut && r.position === 'RB' && (r.historyGames ?? 0) >= 2);
const historicalPool = oldDonors.reduce((s,r) => s+r.statMeans.carries,0);
const ownPool = eligible.reduce((s,r) => s+r.statMeans.carries,0);
assert(Math.abs(historicalPool*jones.statMeans.carries/ownPool - 13.7581)<.00005);
assert.deepEqual(oldDonors.map(r=>r.name), ['Jordan Mason','Kyler Murray','Jauan Jennings','Ben Yurosek']);
assert(!POOLS.rush.donors.has('QB'));
const report = redistributeOutOpportunity(rows);
assert.equal(report.version, VERSION);
assert.equal(report.applied.length,0,'No unsupported historical workload can inflate the saved slate');
assert.equal(report.pools.length,0,'Unknown team budget must not masquerade as offered work');
const withheld = report.withheld!.find(p=>p.pool==='rush')!;
assert.deepEqual(withheld.donors.map(d=>[d.name,d.historicalUnits]), [
  ['Jordan Mason',9.701],['Jauan Jennings',.0435],['Ben Yurosek',.002],
]);
assert.equal(report.withheld!.find(p=>p.pool==='target')!.unit,'receptions');
assert(!report.withheld!.some(p=>p.donors.some(d=>d.name==='Kyler Murray')));
assert.deepEqual(rows,original,'Read-time resolution never changes immutable inputs');
for(const input of [rows,[...rows].reverse(),rows.filter(r=>r.isOut||r.key===jones.key)]) {
  assert.equal(redistributeOutOpportunity(input).applied.length,0,'Filtering or row order cannot create a workload budget');
}
// Even huge, overlapping workloads remain unresolved; clearing/rejecting a donor never repays it.
for(const donor of [player({position:'RB',statMeans:{carries:90,receptions:50}}),
  player({position:'RB',projectionStatus:'out',statMeans:{carries:90,receptions:50}})]) {
  assert.equal(redistributeOutOpportunity([donor,{...jones,key:2}]).applied.length,0);
}
for(const change of [{statMeans:{}},{canDonate:false},{historyGames:1},{depthOrder:null},{depthOrder:2}]) {
  assert.equal(redistributeOutOpportunity([player(change),backup]).applied.length,0);
}
assert.equal(redistributeOutOpportunity([player(),{...backup,historyGames:0}]).applied.length,0);
assert.equal(redistributeOutOpportunity([player(),{...backup,team:'CHI'}]).applied.length,0);
assert.equal(redistributeOutOpportunity([player(),player({key:3}),backup]).applied.length,0,'Conflicting QB1s cannot add workloads');
const healthy = {...backup,depthOrder:1};
assert.equal(redistributeOutOpportunity([player({depthOrder:3}),healthy]).applied.length,0,'Absent backup cannot reduce healthy QB1');
assert.equal(redistributeOutOpportunity([player({statMeans:{attempts:5}}),backup]).applied.length,0,'Promotion never reduces existing workload');

// Supported QB promotion preserves its own rushing and linear expected-points contract.
const promoted = redistributeOutOpportunity([player(),backup]);
const adjusted = promoted.applied[0];
assert.equal(adjusted.statMeans.attempts,34);
assert(Math.abs(adjusted.statMeans.carries-2*34/12)<.0001);
assert.equal(adjusted.inherited[0].pool,'pass');
assert.equal(adjusted.floorFpts,null);
assert.equal(adjusted.ceilingFpts,null);
assert.match(inheritanceNote(adjusted.inherited),/adjusted estimate/);
assert.deepEqual(promoted.pools,[{team:'MIN',pool:'pass',offered:22,assigned:22,unassigned:0}]);
const capped = redistributeOutOpportunity([player({statMeans:{attempts:40}}), {...backup,statMeans:{attempts:2}}]);
assert.equal(capped.applied[0].inherited[0].multiplier,MAX_MULTIPLIER);
assert.deepEqual(capped.pools,[{team:'MIN',pool:'pass',offered:38,assigned:6,unassigned:32}]);
assert.equal(capped.unresolved[0].pooled,32);
for(const p of [...promoted.pools,...capped.pools]) assert.equal(p.offered,p.assigned+p.unassigned);
const reversed = redistributeOutOpportunity([backup,player()]);
assert.deepEqual(reversed,promoted);

// Shared payload suppresses stale distributions for web AND upstream estimates.
const resolved = resolveOpportunityProjection(base,adjusted,null,false);
assert.equal(resolved.ourProj,adjusted.ourProj);
assert.deepEqual(resolved.statMeans,adjusted.statMeans);
assert.equal(resolved.projectionScenario,'availability_estimate');
for(const p of [resolved,resolveOpportunityProjection(base,undefined,{rule:'inherits',applied:true},false)]) {
  for(const key of ['floorFpts','medianFpts','ceilingFpts','boomRate'] as const) assert.equal(p[key],null);
}
const unchanged = resolveOpportunityProjection(base,undefined,null,false);
assert.equal(unchanged.ourProj,base.ourProj);
assert.equal(unchanged.boomRate,.01);
assert.equal(unchanged.projectionScenario,'baseline_simulation');
const zeroed=resolveOpportunityProjection(base,adjusted,null,true);
assert.equal(zeroed.ourProj,0); assert.deepEqual(zeroed.statMeans,{});
assert.equal(zeroed.projectionScenario,'unavailable');

// Expected bonuses are scored per draw; no staged estimate invents a threshold bonus.
const draws=[{rushYds:96},{rushYds:104}];
assert.equal(draws.reduce((s,d)=>s+scoreNflOffense(d),0)/2,11.5);
assert.equal(scoreNflOffense({rushYds:100}),13);
assert.equal(scoreNflOffenseLinear({rushYds:100}),10);
const crossing = redistributeOutOpportunity([player({statMeans:{attempts:40}}),
  {...backup,statMeans:{attempts:20,passing_yards:160},ourProj:7.4}]).applied[0];
assert.equal(crossing.statMeans.passing_yards,320);
assert.equal(crossing.ourProj,13.8,'Only linear delta 6.4; baseline expected bonus is preserved');
assert.equal(MIN_OBSERVED_GAMES,2);
assert(hasObservedOpportunity({historyGames:2}));
assert(!hasObservedOpportunity({historyGames:null}));
console.log('NFL opportunity: captured Jones, budget withholding, QB promotion, accounting, shared scenario and scoring regressions passed');
