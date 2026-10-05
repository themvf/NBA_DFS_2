import assert from 'node:assert/strict';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { kickerRoleBlockedReason, presentPinnedGameAvailability, resolveAvailability, type PinnedGameAvailabilityDecision } from '../src/lib/nfl-dfs/availability';
import { buildCompletion } from '../src/lib/nfl-dfs/build-completion';
import { currentPoolForQa, runNflPreExportQa } from '../src/lib/nfl-dfs/pre-export-qa';
import { defensiveSettingsFor } from '../src/lib/nfl-dfs/defensive-display';
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { exportNflDkEntries } from '../src/lib/nfl-dfs/entry-export';
import RunRiskSummary from '../src/app/dfs/nfl/run-risk-summary';

const now = Date.parse('2026-10-05T21:42:55Z');
const roster = (depth: number | null, fetchedAt = '2026-10-05T21:13:02Z') => ({team:'NO', position:'K', fetchedAt,
  sleeper:{team:'NO',position:'K',status:'Active',depth_chart_order:depth}});
assert.equal(resolveAvailability(roster(1), 'NO', 'K', now).blockedReason, null);
for (const evidence of [undefined, roster(null), roster(2), roster(1, '2026-09-01T00:00:00Z')])
  assert.match(resolveAvailability(evidence, 'NO', 'K', now).blockedReason!, /[Kk]ick|K1/);
const unresolved = resolveAvailability(roster(null), 'NO', 'K', now);
const decision = {version:'test',state:'EXPECTED_ACTIVE',projection_status:null,source:'sleeper',observation_id:1,source_snapshot_id:1,
  available_at:'2026-10-05T21:13:02Z',as_of_at:'2026-10-05T21:42:55Z',kickoff:'2026-10-06T00:15:00Z',reason:'Healthy',qualifying_observation_ids:[1],display_only_observation_ids:[]} satisfies PinnedGameAvailabilityDecision;
assert.ok(presentPinnedGameAvailability(decision, unresolved.role, unresolved.roleBlockedReason).blockedReason,
  'Pinned healthy evidence must not clear a missing kicking role');
assert.equal(kickerRoleBlockedReason({position:'WR'}),null);
assert.match(kickerRoleBlockedReason({position:'K',availability:{role:'Listed K1',fresh:false}})!,/stale/);

const player = (id:number, position:NflOptimizerPlayer['position'], team:string, mean=10):NflOptimizerPlayer => ({
  id,dkPlayerId:id,captainDkPlayerId:id+100,name:`${team} ${position} ${id}`,position,team,opponent:team==='NO'?'ATL':'NO',gameKey:'ATL@NO',
  salary:5000,captainSalary:7500,isOut:false,projectionStatus:'historical',ourProj:mean,floorFpts:3,ceilingFpts:mean*2,boomRate:.2,
  avgFptsDk:mean,fantasyprosProj:null,linestarProj:null,linestarOwnPct:null,customProj:null,
  availability:{role:position==='K'?'Listed K1':`Listed ${position}1`,status:'EXPECTED_ACTIVE',blockedReason:null},
});
const pool = [player(1,'QB','NO'),player(2,'WR','NO'),player(3,'TE','NO'),player(4,'RB','ATL'),player(5,'WR','ATL'),player(6,'TE','ATL'),player(7,'K','NO')];
const smyth = {...player(8,'K','NO',50),name:'Charlie Smyth',availability:{role:'Role unresolved',status:'EXPECTED_ACTIVE',blockedReason:null}};
const settings:NflOptimizerSettings = {format:'showdown',mode:'gpp',projectionSource:'our',nLineups:1,minSalary:0,maxExposure:1,minUnique:1,
  stackPassCatchers:0,bringBack:false,randomness:0,lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{},allowDkFallback:true};
const result=optimizeNflLineups([...pool,smyth],settings);
assert.equal(result.lineups.length,1);
assert.ok(result.lineups.every(l=>!l.playerIds.includes(8)), 'Huge historical ceiling and DK fallback cannot admit an unresolved kicker');
assert.equal(result.eligibility?.find(p=>p.dkPlayerId===8)?.reasonCode,'KICKER_ROLE');
assert.throws(()=>optimizeNflLineups([...pool,smyth],{...settings,lockedPlayerIds:[8]}),/Charlie Smyth.*Kicking role/);
const qa = runNflPreExportQa({format:'showdown',requestedLineups:1,lineups:[{lineupNumber:1,playerIds:[8],totalSalary:5000,slots:[{slot:'FLEX',playerId:8}]}],
  currentPool:currentPoolForQa([smyth])});
assert.ok(qa.openBlockers.includes('current_pool'));
const invalid={...result.lineups[0],slots:result.lineups[0].slots.map((s,i)=>i===1?{...s,player:smyth}:s)};
assert.throws(()=>exportNflDkEntries('Entry ID,Contest Name,Contest ID,Entry Fee,CPT,FLEX,FLEX,FLEX,FLEX,FLEX\n1,Test,1,1,,,,,,',[invalid]),/Charlie Smyth.*Kicking role/);

const missed=[{name:'Chris Olave',binding:'captain min missed (6/10)'}];
assert.equal(buildCompletion({requestedLineups:40,generatedLineups:40,exposureReport:missed}).status,'partial');
assert.equal(buildCompletion({requestedLineups:40,generatedLineups:40,exposureReport:[]}).status,'complete');
assert.equal(buildCompletion({requestedLineups:40,generatedLineups:0}).status,'failed');
assert.equal(buildCompletion({requestedLineups:40,generatedLineups:40,salaryBandReport:[{withinPlan:false}]}).status,'partial');
assert.equal(buildCompletion({requestedLineups:40,generatedLineups:40,archetypePlan:[{label:'Onslaught',requested:6,realized:3}]}).status,'partial');
assert.equal(defensiveSettingsFor('our').profile,'gpp-integrated');
assert.notEqual(defensiveSettingsFor('our').mode,'off');
assert.equal(defensiveSettingsFor('fantasypros').mode,'off');
const html=renderToStaticMarkup(<RunRiskSummary lineups={result.lineups} uncalibratedLeverage={false} exposureReport={missed}
  currentPlayers={[{dkPlayerId:result.lineups[0].playerIds[0],name:'Noah Fant',availability:{status:'QUESTIONABLE'}}]}
  withheld={[{team:'NO',pool:'rush',donors:[{name:'Travis Etienne'}]}]} />);
assert.match(html,/Completed with unmet targets/);
assert.match(html,/Noah Fant.*questionable.*1 of 1/);
assert.match(html,/Travis Etienne.*baseline projections/);
const oldRun = { ...invalid, playerIds: invalid.slots.map(s => s.player.dkPlayerId) };
const oldHtml = renderToStaticMarkup(<RunRiskSummary lineups={[oldRun]} uncalibratedLeverage={false} currentPlayers={[smyth]} />);
assert.match(oldHtml,/Rebuild required.*Charlie Smyth.*cannot be exported/);
console.log('NFL lineup integrity: kicker role, pinned health, solver, lock, saved export, completion and review notices passed.');
