import assert from 'node:assert/strict';
import {canonicalAuditJson} from '../src/lib/nfl-dfs/audit-json';
assert.equal(canonicalAuditJson({b:2,a:{z:0,b:[3,1]},omit:undefined}),canonicalAuditJson({a:{b:[3,1],z:0},b:2}));
import {auditProjection,validateSituations,DEFAULT_SITUATIONS,type AuditPlayer,type SituationTeam,type SituationSettings} from '../src/lib/nfl-dfs/projection-audit';
import type {EfficiencyRate} from '../src/lib/nfl-dfs/efficiency';
import type {InjuryRole} from '../src/lib/nfl-dfs/injury-redistribution';
import {optimizeNflLineups,type NflOptimizerPlayer,type NflOptimizerSettings} from '../src/app/dfs/nfl/nfl-optimizer';
const now=Date.parse('2026-09-12T12:00:00Z'),stamp=new Date(now).toISOString(),kickoff='2026-09-13T17:00:00Z';
const positions={QB:true,RB:true,WR:true,TE:true};
function member(id:number,pos:string,share:number,carry=0):InjuryRole{return {id:String(id),identity:String(id),name:`Player ${id}`,position:pos,role:pos==='QB'?'Listed QB1':'Listed starter',evidence_current:true,out:false,new_team:false,rookie:false,captured_at:stamp,prior_target_share:share,prior_carry_share:carry,availability:{fresh:true,status:'ACTIVE',source:'test',role:pos==='QB'?'Expected starter · QB1':'Listed starter',capturedAt:stamp,blockedReason:null,evaluatedAt:stamp,officialConfirmed:true,kickoff}};}
const members=[member(1,'QB',0,.1),member(2,'WR',.3),member(3,'WR',.2),member(4,'TE',.2),member(5,'RB',.1,.7)];
members[2].availability.status='INACTIVE';members[2].availability.blockedReason='Official inactive';members[2].out=true;
const profile={plays_per_game:60,designed_run_rate:.4,scramble_rate:.1,sack_rate:.05,target_rate:1};
const team:SituationTeam={team:'BUF',capturedAt:stamp,recipeDigest:'recipe',rosterDigest:'roster',coachingDigest:'coach',continuity:'partial_continuity',coaching:{source:'test'},profiles:{all:profile,trailing:{...profile,designed_run_rate:.2}},members,currentQb:'1',historicalQb:'1',reason:null};
const rates=Object.fromEntries(Object.entries({catch_rate:.7,receiving_yards_per_reception:10,receiving_td_rate:.05,rushing_yards_per_carry:4,rushing_td_rate:.04,completion_rate:.65,passing_yards_per_completion:11,passing_td_rate:.06,interception_rate:.02}).map(([k,mean])=>[k,{mean,label:k,player_rate:mean,position_prior:mean,player_opportunities:100,prior_equivalent_opportunities:50,games:8,prior_rows:100,numerator:'count',denominator:'opportunity'}])) as Record<string,EfficiencyRate>;
const baseTargets=30.6*.3;
const wr:AuditPlayer={dkPlayerId:2,ffPlayerId:2,position:'WR',team:'BUF',ourProj:12,avgFptsDk:10,isOut:false,workloadEligible:true,floorFpts:4,ceilingFpts:22,workload:{mean:14,p10:5,p50:12,p90:24,targets:baseTargets,baselineTargets:8,historyGames:8,snapshotId:'wr',recipeDigest:'recipe',rosterDigest:'roster',capturedAt:stamp,kickoff,identity:'2',season:2026,week:1,injuryAdjusted:false},situationEvidence:{team,rates,ratesDigest:'efficiency',ratesAsOf:stamp,reason:null}};
const raw=JSON.stringify(wr),result=auditProjection(wr,positions,DEFAULT_SITUATIONS,now),audit=result.projectionAudit!;
const gain=30.6*.2*.5*(.3/(.3+.2+.1)),coefficient=.7*(1+10*.1+.05*6);
assert.ok(Math.abs(audit.final!-(14+gain*coefficient))<1e-9);
assert.ok(Math.abs(audit.baseline!+audit.steps.reduce((s,a)=>s+a.points,0)-audit.final!)<1e-9,'ledger reconciles');
assert.equal(result.workload!.p90-wr.workload!.p90,audit.final!-14);
assert.equal(JSON.stringify(wr),raw,'source forecast immutable');
assert.deepEqual(auditProjection(wr,positions,DEFAULT_SITUATIONS,now),result,'repeated previews never compound');
assert.equal(auditProjection(wr,positions,undefined,now).workload!.mean,14,'legacy settings unchanged');
assert.equal(auditProjection(wr,{...positions,WR:false},DEFAULT_SITUATIONS,now).projectionAudit!.final,12);
const modify=(fn:(p:AuditPlayer)=>void)=>{const p=structuredClone(wr);fn(p);return auditProjection(p,positions,DEFAULT_SITUATIONS,now);};
for(const changed of [modify(p=>p.isOut=true),modify(p=>p.situationEvidence!.rates=null),modify(p=>p.situationEvidence!.ratesAsOf=new Date(now+1).toISOString()),modify(p=>p.situationEvidence!.team!.currentQb='changed'),modify(p=>p.situationEvidence!.team!.members[2].availability.officialConfirmed=false),modify(p=>p.situationEvidence!.team!.members[2].availability.status='QUESTIONABLE'),modify(p=>p.situationEvidence!.team!.capturedAt='bad'),modify(p=>p.situationEvidence!.team!.members[1].availability.evaluatedAt=new Date(now-61000).toISOString())])assert.equal(changed.workload!.mean,14);
const manual:SituationSettings={enabled:true,teams:{BUF:{profile:'trailing',roles:{'2':{targets:.4}},passingEfficiency:1,reason:'Synthetic reviewed target-role assumption.'}}};
const edited=auditProjection(wr,positions,manual,now);
assert.ok(edited.projectionAudit!.steps.some(s=>s.label==='Reviewed target role'&&s.points>0));
assert.ok(!edited.projectionAudit!.steps.some(s=>s.label==='Injury target redistribution'),'manual roles prevent double counting');
assert.throws(()=>auditProjection(wr,positions,{...manual,teams:{BUF:{...manual.teams.BUF,roles:{'2':{targets:.9}}}}},now),/Shares exceed/);
assert.throws(()=>validateSituations({...manual,teams:{BUF:{...manual.teams.BUF,passingEfficiency:NaN}}},['BUF']),/multiplier/);
assert.throws(()=>validateSituations(manual,['KC']),/not in this slate/);
// QB, RB and TE all translate role/environment changes to points and shifted tails.
for(const [id,pos,opps] of [[1,'QB',30.6],[4,'TE',6.12],[5,'RB',22.38]] as const){const player:AuditPlayer={...wr,dkPlayerId:id,ffPlayerId:id,position:pos,workload:null,positionWorkload:{mean:20,p10:8,p50:18,p90:34,boom:.2,priorOpportunity:opps,baselineMean:12,baselineP10:4,baselineP90:22,snapshotId:pos,capturedAt:stamp,kickoff,recipeDigest:'recipe',studyDigest:'study',releaseVersion:'test'}};
  const next=auditProjection(player,positions,{enabled:true,teams:{BUF:{profile:'trailing',roles:{},passingEfficiency:1.1,reason:'Synthetic passing game-script assumption.'}}},now);
  assert.notEqual(next.positionWorkload!.mean,20,pos);assert.equal(next.positionWorkload!.boom,0,'old boom bonus disabled');
  assert.ok(Math.abs(next.projectionAudit!.baseline!+next.projectionAudit!.steps.reduce((s,a)=>s+a.points,0)-next.projectionAudit!.final!)<1e-9);
}
// The adjusted forecast must reach the real solver, including CPT multiplication.
let id=10;const pool:NflOptimizerPlayer[]=[];
for(const [t,o] of [['BUF','MIA'],['MIA','BUF'],['KC','DEN']])for(const pos of ['QB','RB','RB','WR','WR','WR','TE','DST'] as const){id++;pool.push({id,dkPlayerId:id,captainDkPlayerId:id+1000,name:`${t} ${pos} ${id}`,position:pos,team:t,opponent:o,gameKey:[t,o].sort().join('@'),salary:5000,captainSalary:7500,isOut:false,projectionStatus:'historical',ourProj:10,floorFpts:5,ceilingFpts:15,boomRate:.1,avgFptsDk:9,fantasyprosProj:null,linestarProj:null,linestarOwnPct:null,customProj:null});}
pool.push({...pool.find(p=>p.position==='WR')!,...result,id:2,name:'Audited receiver',position:'WR',dkPlayerId:2,captainDkPlayerId:1002});
const settings:NflOptimizerSettings={format:'classic',mode:'gpp',projectionSource:'workload',workloadPositions:positions,allowDkFallback:false,nLineups:1,minSalary:0,maxExposure:1,minUnique:1,stackPassCatchers:0,bringBack:false,randomness:0,lockedPlayerIds:[2],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{}};
for(const mode of ['cash','gpp'] as const){const l=optimizeNflLineups(pool,{...settings,mode}).lineups[0],slot=l.slots.find(s=>s.player.dkPlayerId===2)!;assert.equal(slot.projection,audit.final);assert.deepEqual(slot.player.projectionAudit,audit);}
const sd=optimizeNflLineups(pool.filter(p=>['BUF','MIA'].includes(p.team)),{...settings,format:'showdown'}).lineups[0];const cpt=sd.slots.find(s=>s.slot==='CPT')!;assert.equal(cpt.player.dkPlayerId,2);assert.equal(cpt.projection,audit.final!*1.5);
console.log('Projection audit: exact ledger, verified absence, all-position scoring, team shares, immutable inputs, freshness, QB change, explicit assumptions, solver/CPT integration passed.');
