import assert from 'node:assert/strict';
import { readSpecialTeamsProjection, SPECIAL_TEAMS_VERSION } from '../src/lib/nfl-dfs/special-teams-projection';
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { summarizeRunRisks } from '../src/lib/nfl-dfs/run-risk-summary';
import { assertDstGameScript, assertShowdownLineup } from '../src/lib/nfl-dfs/showdown-legality';
import { exportNflDkEntries } from '../src/lib/nfl-dfs/entry-export';
import { runNflPreExportQa } from '../src/lib/nfl-dfs/pre-export-qa';

const snapshot = (position: 'DST' | 'K', mean: number) => ({special_teams_candidate: {
  version: SPECIAL_TEAMS_VERSION, status: 'candidate', position, mean,
  p10: mean - 3, p50: mean, p90: mean + 6, boom: .2,
  feature_snapshot: {authority: 'candidate_only', opponent_team: 'MIA'},
}});
assert.equal(readSpecialTeamsProjection(snapshot('DST', 9), 'DST').projection?.mean, 9);
assert.equal(readSpecialTeamsProjection(snapshot('DST', -2), 'DST').projection?.mean, -2);
assert.match(readSpecialTeamsProjection({}, 'DST').reason ?? '', /No matchup forecast was saved/);
assert.match(readSpecialTeamsProjection(snapshot('K', 9), 'DST').reason ?? '', /failed/);
assert.match(readSpecialTeamsProjection({special_teams_candidate: {...snapshot('K', 9).special_teams_candidate, p90: 1}}, 'K').reason ?? '', /failed/);

let id = 0;
const players: NflOptimizerPlayer[] = [];
for (const [team, opponent] of [['BUF', 'MIA'], ['MIA', 'BUF']] as const) {
  for (const position of ['QB', 'RB', 'WR', 'TE', 'K', 'DST'] as const) {
    const ownId = ++id;
    const candidate = position === 'DST' || position === 'K'
      ? readSpecialTeamsProjection(snapshot(position, position === 'DST' ? 12 : 10), position).projection : null;
    players.push({id: ownId, dkPlayerId: ownId, captainDkPlayerId: ownId + 100,
      name: `${team} ${position}`, position, team, opponent, gameKey: 'BUF@MIA',
      salary: 5000, captainSalary: 7500, isOut: false, projectionStatus: 'historical',
      ourProj: 6, floorFpts: 3, ceilingFpts: 9, boomRate: .1,
      avgFptsDk: 5, fantasyprosProj: null, linestarProj: null,
      linestarOwnPct: null, customProj: null, specialTeams: candidate});
  }
}
const dst = players.find(p => p.position === 'DST')!;
const kicker = players.find(p => p.position === 'K')!;
const settings: NflOptimizerSettings = {format:'showdown', mode:'gpp', projectionSource:'our',
  allowDkFallback:false, nLineups:1, minSalary:0,
  maxExposure:1, minUnique:1, stackPassCatchers:0, bringBack:false, randomness:0,
  lockedPlayerIds:[dst.dkPlayerId,kicker.dkPlayerId], excludedPlayerIds:[],
  minExposureByPlayer:{}, maxExposureByPlayer:{}};
const baseline = optimizeNflLineups(players.map(p => ({...p,specialTeams:null})), settings).lineups[0];
const adjusted = optimizeNflLineups(players, settings).lineups[0];
assert.ok(baseline && adjusted);
for (const [player, expected] of [[dst, 12], [kicker, 10]] as const) {
  const slot = adjusted.slots.find(s => s.player.dkPlayerId === player.dkPlayerId)!;
  assert.equal(slot.projectionSource, 'special_teams');
  assert.equal(slot.projection, expected * slot.multiplier);
}
assert.ok(adjusted.slots.some(s => s.projectionSource === 'special_teams'));
assert.deepEqual(summarizeRunRisks([adjusted]).sourceFamilies, ['historical']);
const classicPlayers: NflOptimizerPlayer[] = [];
for (const [team, opponent] of [['BUF','MIA'],['MIA','BUF'],['KC','DEN'],['DEN','KC']] as const)
  for (const position of ['QB','RB','RB','WR','WR','WR','TE','DST'] as const) {
    const ownId=++id;
    classicPlayers.push({id:ownId,dkPlayerId:ownId,captainDkPlayerId:null,
      name:`${team} ${position} ${ownId}`,position,team,opponent,gameKey:[team,opponent].sort().join('@'),
      salary:5000,captainSalary:null,isOut:false,projectionStatus:'historical',
      ourProj:6,floorFpts:3,ceilingFpts:9,boomRate:.1,
      avgFptsDk:5,fantasyprosProj:null,linestarProj:null,linestarOwnPct:null,customProj:null,
      specialTeams:position==='DST'?readSpecialTeamsProjection(snapshot('DST',12),'DST').projection:null});
  }
const classicDst=classicPlayers.find(p=>p.position==='DST')!;
const classic=optimizeNflLineups(classicPlayers,{...settings,format:'classic',lockedPlayerIds:[classicDst.dkPlayerId]}).lineups[0];
assert.ok(classic);
assert.equal(classic.slots.find(s=>s.player.dkPlayerId===classicDst.dkPlayerId)?.projectionSource,'special_teams');
const classicScript = optimizeNflLineups(classicPlayers.map(p=>p.team==='MIA'&&['QB','RB','WR','TE'].includes(p.position)
  ? {...p,ourProj:40,ceilingFpts:100} : p),
  {...settings,format:'classic',lockedPlayerIds:[classicDst.dkPlayerId]}).lineups[0];
assert.ok(classicScript);
assert.ok(classicScript.playerIds.includes(classicDst.dkPlayerId));
assert.equal(classicScript.slots.filter(s=>s.player.team==='MIA'&&['QB','RB','WR','TE'].includes(s.player.position)).length,3);
assert.throws(()=>assertDstGameScript('classic',[
  {slot:'DST',player:classicDst},
  ...classicPlayers.filter(p=>p.team==='MIA'&&['QB','RB','WR','TE'].includes(p.position)).slice(0,4)
    .map(p=>({slot:'FLEX',player:p})),
]),/conflicts with the opposing offensive game script/);
assert.doesNotThrow(()=>assertDstGameScript('classic',[
  {slot:'DST',player:classicDst},
  ...classicPlayers.filter(p=>p.team==='KC'&&['QB','RB','WR','TE'].includes(p.position)).slice(0,4)
    .map(p=>({slot:'FLEX',player:p})),
]));
assert.throws(()=>optimizeNflLineups(classicPlayers,{...settings,format:'classic',
  lockedPlayerIds:[classicDst.dkPlayerId,...classicPlayers.filter(p=>p.team==='MIA'
    &&['QB','RB','WR','TE'].includes(p.position)).slice(0,4).map(p=>p.dkPlayerId)]}),
  /conflicts with the opposing offensive game script/);
const badClassic = structuredClone(classicScript);
const replacement = classicPlayers.find(p=>p.team==='MIA'&&['QB','RB','WR','TE'].includes(p.position)
  && !badClassic.playerIds.includes(p.dkPlayerId)
  && badClassic.slots.some(s=>s.player.position===p.position&&s.player.team!=='MIA'&&s.slot!=='DST'))!;
assert.ok(replacement);
const oldSlot = badClassic.slots.find(s=>s.player.position===replacement.position&&s.player.team!=='MIA'&&s.slot!=='DST')!;
badClassic.playerIds = badClassic.playerIds.map(id=>id===oldSlot.player.dkPlayerId?replacement.dkPlayerId:id);
oldSlot.player = replacement;
assert.throws(()=>exportNflDkEntries('Entry ID,QB,RB,RB,WR,WR,WR,TE,FLEX,DST\n1,,,,,,,,,',[badClassic]),
  /conflicts with the opposing offensive game script/);
const missing = optimizeNflLineups(players.map(p => p.dkPlayerId === kicker.dkPlayerId
  ? {...p, specialTeams:null, specialTeamsReason:'Team implied total is missing'} : p), settings);
assert.equal(missing.lineups[0].slots.find(s=>s.player.dkPlayerId===kicker.dkPlayerId)?.projectionSource,'our');
assert.ok(missing.warnings.some(w=>w.includes('Team implied total is missing')));
const external = optimizeNflLineups(players, {...settings, projectionSource:'dk_avg'});
assert.ok(external.lineups[0].slots.every(s=>s.projectionSource==='dk_avg'));
const miaDst = players.find(p => p.team === 'MIA' && p.position === 'DST')!;
const offense = (position: string) => ['QB','RB','WR','TE'].includes(position);
const scripted = players.map(p => ({...p, specialTeams:null,
  captainDkPlayerId:p.name === 'BUF QB' ? p.captainDkPlayerId : null,
  captainSalary:p.name === 'BUF QB' ? p.captainSalary : null,
  avgFptsDk:p.dkPlayerId === miaDst.dkPlayerId ? 100 : p.team === 'BUF' && offense(p.position) ? 80 : 1}));
const guarded = optimizeNflLineups(scripted,{...settings,projectionSource:'dk_avg',lockedPlayerIds:[miaDst.dkPlayerId]}).lineups[0];
assert.ok(guarded);
assert.equal(guarded.slots[0].player.name,'BUF QB');
assert.ok(guarded.playerIds.includes(miaDst.dkPlayerId));
assert.ok(guarded.slots.filter(s=>s.player.team==='BUF'&&offense(s.player.position)).length<=2);
const extra = scripted.find(p=>p.team==='BUF'&&offense(p.position)&&!guarded.playerIds.includes(p.dkPlayerId))!;
const conflicting = structuredClone(guarded);
const replace = conflicting.slots.findIndex(s=>s.slot!=='CPT'&&s.player.team==='MIA'&&s.player.position!=='DST');
assert.ok(replace>0);
conflicting.playerIds = conflicting.playerIds.map(id=>id===conflicting.slots[replace].player.dkPlayerId?extra.dkPlayerId:id);
conflicting.slots[replace].player = extra;
assert.throws(()=>assertShowdownLineup(conflicting),/conflicts with the opposing offensive game script/);
assert.throws(()=>exportNflDkEntries('Entry ID,CPT,FLEX,FLEX,FLEX,FLEX,FLEX\n1,,,,,,',[conflicting]),/conflicts with the opposing offensive game script/);
assert.ok(runNflPreExportQa({format:'showdown',requestedLineups:1,
  lineups:[{...conflicting,slots:conflicting.slots.map(s=>({...s,playerId:s.player.dkPlayerId}))}]})
  .openBlockers.includes('legal_roster'));
const allowed = optimizeNflLineups(scripted.map(p=>({...p,
  captainDkPlayerId:p.name==='MIA QB'?p.dkPlayerId+100:null,
  captainSalary:p.name==='MIA QB'?7500:null})),
  {...settings,projectionSource:'dk_avg',lockedPlayerIds:[miaDst.dkPlayerId]}).lineups[0];
assert.ok(allowed);
assert.equal(allowed.slots[0].player.name,'MIA QB');
assert.ok(allowed.slots.filter(s=>s.player.team==='BUF'&&offense(s.player.position)).length<=3);
console.log('Special teams forecasts, Showdown DST game-script guard, and export rejection passed.');
