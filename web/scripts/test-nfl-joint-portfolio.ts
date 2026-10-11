import assert from 'node:assert/strict';
import { compareJointPortfolio, compareJointPortfolioIfAvailable } from '../src/lib/nfl-dfs/joint-portfolio';
import { nflDemoBank, nflDemoSlate } from '../src/lib/nfl-dfs/synthetic';
import { generateNflCandidates, type NflLineup } from '../src/lib/nfl-dfs/lineups';
import { assertDstGameScript } from '../src/lib/nfl-dfs/showdown-legality';
import type { NflGeneratedLineup, NflOptimizerPlayer, NflOptimizerResult, NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';

for(const format of ['classic','showdown'] as const) {
  const slate=nflDemoSlate(format);
  const make=(lineup:NflLineup,i:number):NflGeneratedLineup=>{
    const slots=lineup.map(s=>{
      const p=slate.players.find(p=>p.dkPlayerId===s.playerId)!;
      const player:NflOptimizerPlayer={id:p.dkPlayerId,dkPlayerId:p.dkPlayerId,captainDkPlayerId:p.captain?.dkPlayerId??null,
        name:p.name,team:p.teamAbbrev,opponent:p.opponent,position:p.position,gameKey:p.gameKey,salary:p.salary,
        captainSalary:p.captain?.salary??null,isOut:false,projectionStatus:'historical',ourProj:10,floorFpts:2,ceilingFpts:20,
        boomRate:.2,avgFptsDk:null,fantasyprosProj:null,linestarProj:null,linestarOwnPct:null,customProj:null};
      return {slot:s.slot,player,salary:s.slot==='CPT'?p.captain!.salary:p.salary,multiplier:s.slot==='CPT'?1.5:1,projection:10,projectionSource:'our' as const};
    });
    return {lineupNumber:i+1,slots,playerIds:lineup.map(p=>p.playerId),totalSalary:slots.reduce((s,p)=>s+p.salary,0),
      projectedFpts:60,floorFpts:12,ceilingFpts:120,projectedOwnership:null,stackSummary:{quarterback:null,passCatchers:[],bringBack:null}};
  };
  const candidates=generateNflCandidates(slate,{count:50,seed:123}).lineups.map(make).filter(l=>{
    try {assertDstGameScript(format,l.slots);return true;} catch{return false;}
  });
  assert.ok(candidates.length>=4);
  const baseline:NflOptimizerResult={lineups:candidates.slice(0,3),warnings:[],sourceCoverage:{requested:0,direct:0,fallback:0,excluded:0}};
  const settings:NflOptimizerSettings={format,mode:'gpp',projectionSource:'our',allowDkFallback:false,nLineups:3,minSalary:0,maxExposure:1,
    minUnique:0,stackPassCatchers:0,bringBack:false,randomness:0,lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{}};
  const selection={...nflDemoBank(slate,100,100,'selection'),source:'model' as const};
  const evaluation={...nflDemoBank(slate,200,100,'evaluation'),source:'model' as const};
  const input={slate,baseline,candidates,settings,selection,evaluation};
  const result=compareJointPortfolio(input);
  assert.equal(result.selected.length,3);
  assert.equal(result.exportAuthorized,false);
  assert.equal(result.productionChanged,false);
  assert.ok(result.selectedSelection>=result.baselineSelection);
  assert.deepEqual(compareJointPortfolio(input),result);
  const unseen={...nflDemoBank(slate,999,100,'evaluation'),source:'model' as const};
  assert.deepEqual(compareJointPortfolio({...input,evaluation:unseen}).selected,result.selected,'Evaluation outcomes cannot choose entries');
  assert.throws(()=>compareJointPortfolio({...input,evaluation:selection}),/independent/);
  assert.throws(()=>compareJointPortfolio({...input,selection:{...selection,source:'synthetic'},evaluation:{...evaluation,source:'synthetic'}}),/model banks/);
  assert.throws(()=>compareJointPortfolio({...input,candidates:[{...candidates[0],totalSalary:1}]}),/salary/);
  const forged={...candidates[0],slots:candidates[0].slots.map((s,i)=>i? s:{...s,player:{...s.player,team:'OTHER'}})};
  assert.throws(()=>compareJointPortfolio({...input,candidates:[forged]}),/canonical/);
  assert.throws(()=>compareJointPortfolio({...input,settings:{...settings,maxExposure:.5}}),/exposure counts/);
  const partial=compareJointPortfolioIfAvailable({...input,baseline:{...baseline,lineups:baseline.lineups.slice(0,1)}});
  assert.equal(partial.status,'unavailable');
  assert.match(partial.reason!,/complete baseline/);
  const cash=compareJointPortfolio({...input,settings:{...settings,mode:'cash'}});
  assert.equal(cash.objective,'mean_joint_lineup_p10');
  assert.ok(cash.selectedSelection>=cash.baselineSelection);
  const limited=compareJointPortfolio({...input,maxEvaluations:1});
  assert.equal(limited.selected.length,3,'Search budget never causes an incomplete portfolio');
  assert.equal(limited.searchLimitReached,true);
  assert.throws(()=>compareJointPortfolio({...input,settings:{...settings,topProjectedCoverage:true}}),/identities are missing/);
  const leaders=baseline.lineups[0].playerIds.slice(0,2);
  const covered=compareJointPortfolio({...input,baseline:{...baseline,topProjectedPlayerIds:leaders},settings:{...settings,topProjectedCoverage:true}});
  assert.ok(leaders.every(id=>covered.selected.some(l=>l.playerIds.includes(id))));
  assert.throws(()=>compareJointPortfolio({...input,settings:{...settings,gppAirMatchupMinPct:100}}),/Baseline violates/);
  assert.throws(()=>compareJointPortfolio({...input,settings:{...settings,gppGoalLineMinPct:100}}),/Baseline violates/);
  assert.throws(()=>compareJointPortfolio({...input,settings:{...settings,gppSignalMinPerLineup:1}}),/Baseline violates/);
  const freeze={archetypeId:'standard_ceiling' as const,fadePlayerIds:[],eligibleCaptainIds:null,forbiddenCaptainIds:[],teamCountRange:null,
    beneficiaries:[],minKickerDst:null,summary:'Fixed standard policy'};
  const tagged=candidates.map(l=>({...l,constructionContract:freeze,archetype:{id:'standard_ceiling' as const,label:'Standard ceiling',fadedPlayerIds:[],fadedPlayerNames:[],beneficiariesSatisfied:[]}}));
  const labelResult=compareJointPortfolio({...input,baseline:{...baseline,lineups:tagged.slice(0,3)},candidates:tagged});
  assert.ok(labelResult.selected.every(l=>l.constructionContract?.summary===freeze.summary));
  assert.throws(()=>compareJointPortfolio({...input,candidates:[{...tagged[0],constructionContract:{...freeze,fadePlayerIds:[tagged[0].playerIds[0]]}}]}),/archetype construction/);
  const band={min:0,max:50000-candidates[0].totalSalary,minLineups:0,maxLineups:1};
  const banded=compareJointPortfolio({...input,settings:{...settings,salaryPolicy:{minSalaryUsed:0,maxSalaryUsed:50000,minSalaryLeft:0,maxSalaryLeft:50000,salaryLeftBands:[band]}}});
  assert.equal(banded.selected.filter(l=>50000-l.totalSalary<=band.max).length,baseline.lineups.filter(l=>50000-l.totalSalary<=band.max).length);
  if(format==='showdown') {
    const captain=candidates[0].slots[0].player;
    const exact=baseline.lineups.filter(l=>l.slots[0].player.dkPlayerId===captain.dkPlayerId).length;
    const reports=slate.players.map(p=>({dkPlayerId:p.dkPlayerId,name:p.name,overall:0,captain:0,flex:0,overallMin:0,overallMax:3,
      captainMin:p.dkPlayerId===captain.dkPlayerId?exact:0,captainMax:3,flexMin:0,flexMax:3,binding:null}));
    const frozen=compareJointPortfolio({...input,baseline:{...baseline,exposureReport:reports}});
    assert.ok(frozen.selected.filter(l=>l.slots[0].player.dkPlayerId===captain.dkPlayerId).length>=exact);
  }
}
console.log('NFL joint portfolios: both formats, independent evaluation, fulfilled Captain bounds, salary identity, cash/GPP joint objectives and bounded complete output passed.');
