import assert from 'node:assert/strict';
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { applyDiversified, DEFAULT_DIVERSIFIED, diversifiedCount, diversifiedGroups, diversifiedPolicy, groupLimit, repeatedGroupCounts } from '../src/lib/nfl-dfs/diversified';
import { sameGenerationSettings } from '../src/lib/nfl-dfs/generation-settings';

const players: NflOptimizerPlayer[] = [];
let id=0;
for(let team=0;team<12;team++) {
  const name=`T${team}`, opponent=`T${team^1}`, gameKey=`T${team&~1}@T${team|1}`;
  for(const [pos,count,salary,mean] of [['QB',1,6500,20],['RB',2,6000,15],['WR',4,5500,14],['TE',2,4500,10],['DST',1,3000,7]] as const)
    for(let k=0;k<count;k++) players.push({id:++id,dkPlayerId:id,captainDkPlayerId:null,name:`${name} ${pos}${k}`,position:pos,
      team:name,opponent,gameKey,salary:salary+k*100,captainSalary:null,isOut:false,projectionStatus:'historical',historyGames:8,
      ourProj:mean-team*.1-k*.2,floorFpts:mean*.6,ceilingFpts:mean*1.8,boomRate:.1,avgFptsDk:mean,
      fantasyprosProj:null,linestarProj:null,linestarOwnPct:null,ownPct:10+team,customProj:null});
}
const base:NflOptimizerSettings={format:'classic',mode:'gpp',projectionSource:'our',allowDkFallback:false,nLineups:20,minSalary:45000,maxExposure:1,minUnique:2,
  stackPassCatchers:1,bringBack:true,randomness:.08,lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{},
  ownershipLeverageEnabled:true,useHeuristicOwnershipLeverage:true,buildShape:'diversified',diversified:{candidateTarget:50,minimumQbs:8}};
assert.equal(applyDiversified({...base,buildShape:'standard'}).ownershipLeverageEnabled,true);
assert.throws(()=>applyDiversified({...base,format:'showdown'}),/Classic GPP/);
assert.throws(()=>applyDiversified({...base,diversified:{candidateTarget:NaN}}),/candidate target/);
assert.throws(()=>applyDiversified({...base,diversified:{minimumQbs:21}}),/lineup count/);
assert.equal(diversifiedCount(.25,50),12);
assert.equal(diversifiedCount(.15,1),1);
const resolved=applyDiversified(base);
assert.equal(resolved.ownershipLeverageEnabled,false);
assert.equal(resolved.leverageExponent,0);
assert.equal(resolved.minUnique,4);
assert.equal(applyDiversified({...base,diversifiedCandidateSearch:true,diversifiedMeanSearch:true}).diversifiedCandidateSearch,false);
assert.ok(sameGenerationSettings(base,resolved),'effective profile does not falsely show settings changed');

const roster=(ids:number[])=>({slots:ids.map(i=>({player:players.find(p=>p.dkPlayerId===i)!}))});
const qb=players.find(p=>p.team==='T0'&&p.position==='QB')!;
const wrs=players.filter(p=>p.team==='T0'&&p.position==='WR').slice(0,2);
const rb=players.find(p=>p.team==='T0'&&p.position==='RB')!;
const stack=roster([qb.dkPlayerId,...wrs.map(p=>p.dkPlayerId),rb.dkPlayerId]);
assert.ok(!diversifiedGroups(stack).some(g=>g.ids.length===2&&g.ids.includes(qb.dkPlayerId)&&g.ids.includes(wrs[0].dkPlayerId)));
assert.ok(diversifiedGroups(stack).some(g=>g.ids.includes(qb.dkPlayerId)&&g.ids.includes(rb.dkPlayerId)),'QB/RB is not exempt');
const otherQb=players.find(p=>p.team==='T2'&&p.position==='QB')!;
assert.ok(diversifiedGroups(roster([otherQb.dkPlayerId,...wrs.map(p=>p.dkPlayerId)])).some(g=>g.kind==='pair'&&g.exemptTeam==='T0'));

const first=optimizeNflLineups(players,base);
assert.equal(first.lineups.length,20);
assert.ok(first.diversifiedReport!.distinctQbs>=8);
assert.ok(first.exposureReport!.every(r=>r.overall<=r.overallMax));
const policy=diversifiedPolicy(base);
// Independent pair recount, separate from the implementation's feature builder.
const pairCounts=new Map<string,{count:number;dst:boolean}>();
for(const lineup of first.lineups) {
  const roster=lineup.slots.map(s=>s.player),qb=roster.find(p=>p.position==='QB')!;
  for(let a=0;a<roster.length;a++)for(let b=a+1;b<roster.length;b++) {
    const pair=[roster[a],roster[b]];
    if(pair.every(p=>p.team===qb.team&&['QB','WR','TE'].includes(p.position)))continue;
    const key=pair.map(p=>p.dkPlayerId).sort((a,b)=>a-b).join(',');
    pairCounts.set(key,{count:(pairCounts.get(key)?.count??0)+1,dst:pair.some(p=>p.position==='DST')});
  }
}
for(const {count,dst} of pairCounts.values())assert.ok(count<=Math.floor(20*(dst ? .2 : .25)));

for(const {group,count} of repeatedGroupCounts(first.lineups).values()) assert.ok(count<=groupLimit(group,policy,20));
for(const a of first.lineups) {
  assert.equal(a.slots.length,9);assert.equal(a.totalSalary,a.slots.reduce((s,e)=>s+e.salary,0));
  const defense=a.slots.find(s=>s.player.position==='DST')!.player;
  assert.ok(a.slots.every(s=>s.player.position==='DST'||s.player.team!==defense.opponent));
  for(const b of first.lineups) if(a!==b)assert.ok(a.playerIds.filter(i=>b.playerIds.includes(i)).length<=5);
}
const second=optimizeNflLineups(players,{...base,ownershipLeverageEnabled:false,leverageExponent:1});
assert.deepEqual(first.lineups.map(l=>l.playerIds),second.lineups.map(l=>l.playerIds),'ownership-neutral profile ignores caller ownership influence');
const impossible=players.filter(p=>p.position!=='DST'||p.team==='T0');
assert.throws(()=>optimizeNflLineups(impossible,base),/Diversified.*(complete|only)/,'infeasible portfolio cannot be returned as complete');
const overridden=optimizeNflLineups(players,{...base,maxExposureByPlayer:{[qb.dkPlayerId]:.4},minExposureByPlayer:{[qb.dkPlayerId]:.3},
  diversified:{candidateTarget:50},excludedPlayerIds:[otherQb.dkPlayerId]});
const qbReport=overridden.exposureReport!.find(p=>p.dkPlayerId===qb.dkPlayerId)!;
assert.equal(qbReport.overallMax,8);assert.ok(qbReport.overall>=6&&qbReport.overall<=8);
assert.ok(overridden.lineups.every(l=>!l.playerIds.includes(otherQb.dkPlayerId)));
assert.throws(()=>optimizeNflLineups(players,{...base,lockedPlayerIds:[qb.dkPlayerId,rb.dkPlayerId]}),/Locked combination/);
const lockedResult=optimizeNflLineups(players,{...base,lockedPlayerIds:[qb.dkPlayerId],diversified:{candidateTarget:50}});
assert.ok(lockedResult.lineups.every(l=>l.playerIds.includes(qb.dkPlayerId)));
assert.equal(lockedResult.exposureReport!.find(p=>p.dkPlayerId===qb.dkPlayerId)!.overallMax,20);
assert.equal(DEFAULT_DIVERSIFIED.playerDstMax,.2);
console.log('NFL diversified construction: complete portfolios, overrides, overlap, combinations, deterministic replay and format isolation OK');
