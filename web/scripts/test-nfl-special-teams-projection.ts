import assert from 'node:assert/strict';
import { readSpecialTeamsProjection, SPECIAL_TEAMS_VERSION } from '../src/lib/nfl-dfs/special-teams-projection';
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { summarizeRunRisks } from '../src/lib/nfl-dfs/run-risk-summary';

const snapshot = (position: 'DST' | 'K', mean: number) => ({special_teams_candidate: {
  version: SPECIAL_TEAMS_VERSION, status: 'candidate', position, mean,
  p10: mean - 3, p50: mean, p90: mean + 6, boom: .2,
  feature_snapshot: {authority: 'candidate_only', opponent_team: 'MIA'},
}});
assert.equal(readSpecialTeamsProjection(snapshot('DST', 9), 'DST').projection?.mean, 9);
assert.equal(readSpecialTeamsProjection(snapshot('DST', -2), 'DST').projection?.mean, -2);
assert.match(readSpecialTeamsProjection({}, 'DST').reason ?? '', /Refresh projections/);
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
  specialTeamsMode:'experimental', allowDkFallback:false, nLineups:1, minSalary:0,
  maxExposure:1, minUnique:1, stackPassCatchers:0, bringBack:false, randomness:0,
  lockedPlayerIds:[dst.dkPlayerId,kicker.dkPlayerId], excludedPlayerIds:[],
  minExposureByPlayer:{}, maxExposureByPlayer:{}};
const baseline = optimizeNflLineups(players, {...settings, specialTeamsMode:'off'}).lineups[0];
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
assert.throws(() => optimizeNflLineups(players.map(p => p.dkPlayerId === kicker.dkPlayerId
  ? {...p, specialTeams:null, specialTeamsReason:'Team implied total is missing'} : p), settings),
  /Team implied total is missing/);
assert.throws(() => optimizeNflLineups(players, {...settings, projectionSource:'dk_avg'}),
  /require Our historical model/);
console.log('Special-teams saved candidate reading, lineup use, and missing-input stop passed.');
