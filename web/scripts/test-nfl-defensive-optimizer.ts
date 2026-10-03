import assert from 'node:assert/strict';
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { resolveDefensiveForecast, type DefensiveCapture, type DefensivePlayerInput } from '../src/lib/nfl-dfs/defensive-projection';
import { captureProfileFor, selectedDefensiveForecast, DEFAULT_DFS_DEFENSIVE_SETTINGS } from '../src/lib/nfl-dfs/defensive-display';
import { assertShowdownLineup } from '../src/lib/nfl-dfs/showdown-legality';
import { exportNflDkEntries } from '../src/lib/nfl-dfs/entry-export';

const player = (id:number, position:NflOptimizerPlayer['position'], mean=12):NflOptimizerPlayer => ({
  id,dkPlayerId:id,captainDkPlayerId:null,name:`P${id}`,position,
  team:id%4<2?'ARI':'NYG',opponent:id%4<2?'SF':'DAL',gameKey:id%4<2?'ARI@SF':'NYG@DAL',
  salary:5000,captainSalary:null,isOut:false,projectionStatus:'ok',ourProj:mean,floorFpts:8,ceilingFpts:18,
  boomRate:.2,avgFptsDk:10,fantasyprosProj:null,linestarProj:null,linestarOwnPct:null,customProj:null,
});
const input=(p:NflOptimizerPlayer):DefensivePlayerInput=>({dkPlayerId:p.dkPlayerId,ffPlayerId:p.dkPlayerId,
  gameKey:p.gameKey,isOut:p.isOut,ourProj:p.ourProj,floorFpts:p.floorFpts,medianFpts:12,
  ceilingFpts:p.ceilingFpts,boomRate:p.boomRate,statMeans:{rushing_yards:80,carries:15}});
const capture=(p:NflOptimizerPlayer,p10:number,p90:number,boom:number,mean=12):DefensiveCapture=>({
  runId:'capture-1',baselineRunId:'baseline-1',capturedAt:'2026-09-26T12:00:00Z',artifactDigest:'frozen',
  candidate:{player_id:p.dkPlayerId,dk_player_id:p.dkPlayerId,game_id:'2026_03_ARI_SF',kickoff:'2026-09-27T20:00:00Z',
    shadow:{status:'under_evaluation',reproduction:{passed:true},
      baseline:{mean:12,p10:8,p50:12,p90:18,boom:.2,stat_means:{rushing_yards:80,carries:15}},
      candidate:{mean,p10,p50:12,p90,boom,stat_means:{rushing_yards:80,carries:15}},
      ledger:[{component:'rushing_yards',factor:1.1,points_delta:mean-12}]}}
});
const defensive={mode:'experimental' as const,profile:'pfr-efficiency' as const};
const baseSettings:NflOptimizerSettings={format:'classic',mode:'gpp',projectionSource:'our',allowDkFallback:false,
  nLineups:1,minSalary:0,maxExposure:1,minUnique:1,stackPassCatchers:0,bringBack:false,randomness:0,
  lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{}};
const pool=[player(1,'QB'),player(2,'RB'),player(3,'RB'),player(4,'RB'),player(5,'RB'),
  player(6,'WR'),player(7,'WR'),player(8,'WR'),player(9,'TE'),player(10,'DST')];
const off=optimizeNflLineups(pool,baseSettings).lineups[0];
assert.ok(off);
const adjusted=pool.map(p=>({...p,defensiveForecast:resolveDefensiveForecast(input(p),'baseline-1',defensive,
  p.dkPlayerId===5?capture(p,2,40,.6):null)}));
assert.equal(adjusted[4].defensiveForecast.status,'applied');
assert.equal(adjusted[4].defensiveForecast.selected.mean,12);
assert.equal(adjusted[4].defensiveForecast.selected.p90,40);
assert.equal(selectedDefensiveForecast(adjusted[4].defensiveForecast,defensive,'RB')?.p90,40);
assert.equal(selectedDefensiveForecast(adjusted[4].defensiveForecast,{...defensive,mode:'off'},'RB'),null);
assert.equal(selectedDefensiveForecast(adjusted[4].defensiveForecast,{...defensive,profile:'allowed-rushing-volume'},'RB'),null);
assert.deepEqual(DEFAULT_DFS_DEFENSIVE_SETTINGS,{mode:'experimental',profile:'gpp-integrated'});
const integrated=pool.map(p=>{
  const profile=captureProfileFor('gpp-integrated',p.position);
  const candidate=p.dkPlayerId===1?capture(p,7,24,.3):p.dkPlayerId===5?capture(p,2,40,.6):null;
  return {...p,defensiveForecast:resolveDefensiveForecast(input(p),'baseline-1',
    {mode:'experimental',profile},candidate)};
});
assert.equal(integrated[0].defensiveForecast.profile,'pfr-efficiency');
assert.equal(integrated[4].defensiveForecast.profile,'allowed-rushing-volume');
assert.equal(selectedDefensiveForecast(integrated[4].defensiveForecast,DEFAULT_DFS_DEFENSIVE_SETTINGS,'RB')?.p90,40);
assert.equal(selectedDefensiveForecast(integrated[4].defensiveForecast,DEFAULT_DFS_DEFENSIVE_SETTINGS,'QB'),null);
const integratedRun=optimizeNflLineups(integrated,{...baseSettings,defensiveAdjustments:DEFAULT_DFS_DEFENSIVE_SETTINGS});
assert.equal(integratedRun.lineups.length,1);
assert.ok(integratedRun.lineups[0].playerIds.includes(5));
const on=optimizeNflLineups(adjusted,{...baseSettings,defensiveAdjustments:defensive}).lineups[0];
assert.ok(on);
assert.notDeepEqual(on.playerIds,off.playerIds,'adjusted upper tail must affect the selected roster');
assert.ok(on.playerIds.includes(5));
assert.equal(on.slots.find(s=>s.player.dkPlayerId===5)?.player.defensiveForecast?.digest,adjusted[4].defensiveForecast.digest);
const cash=optimizeNflLineups(adjusted,{...baseSettings,mode:'cash',defensiveAdjustments:defensive}).lineups[0];
assert.ok(cash);
assert.ok(!cash.playerIds.includes(5),'adjusted lower tail must affect cash selection');
const mismatch=resolveDefensiveForecast({...input(pool[4]),ourProj:13},'baseline-1',defensive,capture(pool[4],2,40,.6));
assert.equal(mismatch.status,'baseline');
assert.equal(mismatch.reason,'saved_baseline_distribution_mismatch');
const unavailable=resolveDefensiveForecast({...input(pool[4]),isOut:true},'baseline-1',defensive,capture(pool[4],2,40,.6));
assert.equal(unavailable.status,'baseline');
for(const [away,home] of [['ATL','GB'],['SEA','NE']]) {
  const showdown=Array.from({length:8},(_,i)=>({
    ...player(100+i,i<2?'QB':i<4?'RB':i<6?'WR':'TE'),
    team:i%2?home:away,opponent:i%2?away:home,gameKey:`${away}@${home}`,
    captainDkPlayerId:1100+i,captainSalary:7500,rosterPositions:['CPT','FLEX'],
  }));
  const target=showdown[2];
  const frozen=capture(target,9,27,.4,15);
  frozen.candidate.game_id=`2026_03_${away}_${home}`;
  const withBundles=showdown.map(p=>({...p,defensiveForecast:resolveDefensiveForecast(input(p),'baseline-1',
    {mode:'experimental',profile:'allowed-rushing-volume'},p.dkPlayerId===target.dkPlayerId?frozen:null)}));
  const result=optimizeNflLineups(withBundles,{...baseSettings,format:'showdown',nLineups:3,
    defensiveAdjustments:{mode:'experimental',profile:'allowed-rushing-volume'}});
  assert.equal(result.lineups.length,3);
  for(const lineup of result.lineups) {
    assertShowdownLineup(lineup);
    const captain=lineup.slots[0];
    assert.equal(captain.projection,(captain.player.defensiveForecast?.status==='applied'?15:12)*1.5);
    assert.ok(exportNflDkEntries('Entry ID,CPT,FLEX,FLEX,FLEX,FLEX,FLEX\nE1,,,,,,',[lineup])
      .includes(`(${captain.player.captainDkPlayerId})`));
  }
}
// "Fall back to DK's season average" works in defensive mode too (the default).
// Before 2026-09-29 the defensive branch returned first, and 17 players on the
// week-3 classic silently left the pool.
const noProjection={...player(11,'WR'),ourProj:null,floorFpts:null,ceilingFpts:null,avgFptsDk:9};
const fallbackPool=[...pool,noProjection].map(p=>({...p,defensiveForecast:resolveDefensiveForecast(input(p),'baseline-1',defensive,null)}));
const withFallback=optimizeNflLineups(fallbackPool,{...baseSettings,allowDkFallback:true,defensiveAdjustments:defensive});
assert.equal(withFallback.eligibility!.find(e=>e.dkPlayerId===11)!.eligible,true,'DK average fills the missing projection');
assert.ok(withFallback.warnings.includes('1 players used DK Avg fallback.'),'and the page is told');
assert.equal(optimizeNflLineups(fallbackPool,{...baseSettings,defensiveAdjustments:defensive}).eligibility!.find(e=>e.dkPlayerId===11)!.eligible,false,'off: still excluded');
const ruledOutNoProjection=fallbackPool.map(p=>p.dkPlayerId===11?{...p,isOut:true}:p);
assert.equal(optimizeNflLineups(ruledOutNoProjection,{...baseSettings,allowDkFallback:true,defensiveAdjustments:defensive}).eligibility!.find(e=>e.dkPlayerId===11)!.eligible,false,'never restores a ruled-out player');

console.log('Defensive bundle, GPP upper-tail selection, cash lower-tail selection, baseline fallbacks and the DK-average fallback passed.');
