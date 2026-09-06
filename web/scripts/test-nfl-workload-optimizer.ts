import assert from 'node:assert/strict';
import {readWorkloadProjection,workloadPoolEligible,type WorkloadReport,type WorkloadTarget} from '../src/lib/nfl-dfs/workload-projection';
import {optimizeNflLineups,type NflOptimizerPlayer,type NflOptimizerSettings} from '../src/app/dfs/nfl/nfl-optimizer';
const now=Date.parse('2026-09-10T12:00:00Z'),kickoff='2026-09-13T17:00:00Z';
const row={identity:'gsis-1',history_games:8,targets_baseline:5,targets_volume:7,fpts_volume:40,p10:30,p50:38,p90:60};
const report:WorkloadReport={version:'nfl-dfs-volume-share-v1',season:2026,week:1,history_cutoff_exclusive:[2026,1],as_of:new Date(now-1000).toISOString(),snapshot_digest:'a'.repeat(64),recipe_digest:'b'.repeat(64),roster_evidence_digest:'c'.repeat(64),sources:[{season:2025}],forward:[{team:'BUF',kickoff,players:[row]}]};
const target:WorkloadTarget={identity:'gsis-1',position:'WR',team:'BUF',gameInfo:'BUF@MIA 09/13/2026 01:00PM ET',isOut:false,availability:{fresh:true,blockedReason:null,status:'ACTIVE',role:'Listed WR1',source:'test',capturedAt:report.as_of,evaluatedAt:new Date(now).toISOString(),kickoff}};
assert.ok(workloadPoolEligible(target,now));
assert.equal(workloadPoolEligible({...target,position:'QB'},now),false);
assert.ok(workloadPoolEligible({...target,position:'QB',availability:{...target.availability!,role:'Expected starter · QB1'}},now));
assert.equal(workloadPoolEligible({...target,isOut:true},now),false);
assert.equal(workloadPoolEligible(target,Date.parse(kickoff)),false);
const decoded=readWorkloadProjection(report,target,2026,1,now).projection!;
assert.equal(decoded.mean,40);assert.equal(decoded.targets,7);assert.equal(decoded.injuryAdjusted,false);
for(const changed of [{...target,team:'KC'},{...target,identity:null},{...target,position:'RB'},{...target,isOut:true},{...target,gameInfo:target.gameInfo!.replace('01:00','04:00')},{...target,availability:{...target.availability!,fresh:false}},{...target,availability:{...target.availability!,evaluatedAt:new Date(now-61000).toISOString()}}])assert.equal(readWorkloadProjection(report,changed,2026,1,now).projection,null);
for(const changed of [{...report,week:2},{...report,as_of:new Date(now+1000).toISOString()},{...report,as_of:new Date(now-73*3600000).toISOString()},{...report,sources:[{season:2026}]},{...report,forward:[...report.forward,...report.forward]},{...report,forward:[{...report.forward[0],players:[row,row]}]},{...report,forward:[{...report.forward[0],players:[{...row,p90:0}]}]}])assert.equal(readWorkloadProjection(changed,target,2026,1,now).projection,null);
assert.equal(readWorkloadProjection(report,target,2026,1,Date.parse(kickoff)).projection,null);
let id=0;const pool:NflOptimizerPlayer[]=[];
for(const [team,opponent] of [['BUF','MIA'],['MIA','BUF'],['KC','DEN'],['DEN','KC']])for(const position of ['QB','RB','RB','WR','WR','WR','TE','DST'] as const){id++;pool.push({id,dkPlayerId:id,captainDkPlayerId:id+1000,name:`${team} ${position} ${id}`,position,team,opponent,gameKey:[team,opponent].sort().join('@'),salary:5000,captainSalary:7500,isOut:false,projectionStatus:'historical',ourProj:10,floorFpts:5,ceilingFpts:15,boomRate:.1,avgFptsDk:9,fantasyprosProj:null,linestarProj:null,linestarOwnPct:null,customProj:null});}
const upgraded=pool.find(p=>p.position==='WR')!;upgraded.ourProj=1;upgraded.floorFpts=.5;upgraded.ceilingFpts=1.5;upgraded.workload=decoded;
const saved=JSON.stringify(pool);
const settings:NflOptimizerSettings={format:'classic',mode:'gpp',projectionSource:'our',allowDkFallback:false,nLineups:1,minSalary:0,maxExposure:1,minUnique:1,stackPassCatchers:0,bringBack:false,randomness:0,lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{}};
for(const mode of ['cash','gpp'] as const){const baseline=optimizeNflLineups(pool,{...settings,mode}).lineups[0];const result=optimizeNflLineups(pool,{...settings,mode,projectionSource:'workload'});const l=result.lineups[0];assert.ok(!baseline.playerIds.includes(upgraded.dkPlayerId));assert.ok(l.playerIds.includes(upgraded.dkPlayerId),'workload must change actual selection');assert.equal(l.projectedFpts,120);assert.equal(l.floorFpts,70);assert.equal(l.ceilingFpts,180);assert.equal(result.sourceCoverage.direct,1);assert.equal(l.slots.filter(s=>s.projectionSource==='our_fallback').length,8);assert.equal(new Set(l.playerIds).size,9);assert.equal(l.totalSalary,45000);assert.deepEqual(optimizeNflLineups(pool,{...settings,mode,projectionSource:'workload'}),result);}
const sd=optimizeNflLineups(pool.filter(p=>['BUF','MIA'].includes(p.team)),{...settings,format:'showdown',projectionSource:'workload'}).lineups[0];assert.equal(sd.slots.find(s=>s.slot==='CPT')!.player.dkPlayerId,upgraded.dkPlayerId);assert.equal(sd.projectedFpts,110);assert.equal(sd.floorFpts,70);assert.equal(sd.ceilingFpts,165);assert.equal(sd.totalSalary,32500);
assert.throws(()=>optimizeNflLineups(pool.map(p=>({...p,workload:null})),{...settings,projectionSource:'workload'}),/No eligible/);
assert.equal(JSON.stringify(pool),saved);
console.log('Workload source: identity/time/eligibility gates, real cash/GPP choices, historical fallback, reproducibility and CPT scaling passed.');
