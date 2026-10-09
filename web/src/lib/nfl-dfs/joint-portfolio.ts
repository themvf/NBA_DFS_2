/** Constraint-preserving joint scoring. Shadow only until the registered gates pass. */
import type { NflDkSlate } from './dk-salary-csv';
import { validateNflLineup, type NflLineup, type NflSlot } from './lineups';
import { assertDstGameScript } from './showdown-legality';
import { prepareNflScenarios, scoreNflLineupDraws, summarizeNflDraws, type NflScenarioBank } from './scenarios';
import type { NflGeneratedLineup, NflOptimizerResult, NflOptimizerSettings } from '@/app/dfs/nfl/nfl-optimizer';
import { buildCompletion } from './build-completion';
import { isNflGppSignalPlayer } from './player-signals';

export const JOINT_PORTFOLIO_VERSION = 'nfl-joint-constrained-shadow-v1';
const roster = (lineup:NflGeneratedLineup):NflLineup => lineup.slots.map(s => ({slot:s.slot.replace(/\d+$/,'') as NflSlot,playerId:s.player.dkPlayerId}));

export function compareJointPortfolio(input:{slate:NflDkSlate;baseline:NflOptimizerResult;candidates:NflGeneratedLineup[];
  settings:NflOptimizerSettings;selection:NflScenarioBank;evaluation:NflScenarioBank;maxEvaluations?:number}) {
  const {slate,settings,baseline}=input;
  if (buildCompletion({...baseline,requestedLineups:settings.nLineups,generatedLineups:baseline.lineups.length}).status !== 'complete')
    throw new Error('A complete baseline with fulfilled exposure/archetype/salary targets is required.');
  if (slate.format!==settings.format) throw new Error('Joint slate and build formats differ.');
  if(settings.topProjectedCoverage && !baseline.topProjectedPlayerIds) throw new Error('Frozen top projected player identities are missing.');
  const selection=prepareNflScenarios(slate,input.selection), evaluation=prepareNflScenarios(slate,input.evaluation);
  for (const key of ['snapshotId','modelVersion','decisionAt','inputsCapturedAt','source'] as const)
    if(selection.metadata[key]!==evaluation.metadata[key]) throw new Error(`Joint selection/evaluation ${key} mismatch.`);
  if(selection.metadata.source!=='model' || selection.metadata.sampling!=='iid' || evaluation.metadata.sampling!=='iid')
    throw new Error('Audited IID model banks are required; independent marginal draws cannot certify joint upside.');
  if(selection.metadata.seed===evaluation.metadata.seed || selection.metadata.runId===evaluation.metadata.runId || selection.metadata.streamId===evaluation.metadata.streamId
    || evaluation.scenarioIds.some(id=>selection.scenarioIds.includes(id))) throw new Error('Joint selection/evaluation streams must be independent.');
  const budget=input.maxEvaluations??2000;
  if(!Number.isSafeInteger(budget)||budget<1||budget>10000) throw new Error('Invalid joint search budget.');
  if(!baseline.exposureReport && (settings.maxExposure<1 || settings.exposurePolicies?.length || Object.values(settings.minExposureByPlayer).some(v=>v>0)
    || Object.keys(settings.maxExposureByPlayer).length)) throw new Error('Frozen exposure counts are missing; joint selection cannot relax them.');
  const candidates=new Map<string,NflGeneratedLineup>();
  // Baseline labels win duplicate identities, so a new search cannot relabel an
  // old roster to make an archetype quota appear fulfilled.
  for(const lineup of [...input.candidates,...baseline.lineups]) {
    const {key,salary}=validateNflLineup(slate,roster(lineup));
    if(lineup.slots.some(s=>{const p=slate.players.find(p=>p.dkPlayerId===s.player.dkPlayerId)!;
      return s.player.position!==p.position || s.player.team!==p.teamAbbrev || s.player.opponent!==p.opponent
        || s.salary!==(s.slot==='CPT'?p.captain?.salary:p.salary) || s.multiplier!==(s.slot==='CPT'?1.5:1);}))throw new Error('Candidate attributes differ from the canonical salary roster.');
    assertDstGameScript(slate.format,lineup.slots);
    if(lineup.totalSalary!==salary || lineup.totalSalary!==lineup.slots.reduce((sum,s)=>sum+s.salary,0)) throw new Error('Candidate salary total differs from its roster.');
    if(lineup.playerIds.length!==lineup.slots.length || lineup.slots.some(s=>!lineup.playerIds.includes(s.player.dkPlayerId))) throw new Error('Candidate player IDs differ from its roster.');
    if(baseline.eligibility && lineup.slots.some(s=>{const decision=baseline.eligibility!.find(p=>p.dkPlayerId===s.player.dkPlayerId);
      return !decision?.eligible || (s.slot==='CPT' && !decision.captainEligible);})) throw new Error('Candidate violates the frozen eligibility/Captain decisions.');
    if(settings.puntPolicy && !baseline.eligibility)throw new Error('Frozen role/punt decisions are missing.');
    if(settings.puntPolicy && lineup.slots.filter(s=>baseline.eligibility!.find(p=>p.dkPlayerId===s.player.dkPlayerId)?.salaryRelief).length>settings.puntPolicy.maxSalaryReliefPlayersPerLineup)
      throw new Error('Candidate exceeds the frozen salary-relief limit.');
    const contract=lineup.constructionContract, captain=lineup.slots.find(s=>s.slot==='CPT')?.player.dkPlayerId;
    if(lineup.archetype && lineup.archetype.id!=='standard_ceiling' && !contract)throw new Error('Frozen archetype construction rules are missing.');
    if(contract && (contract.archetypeId!==lineup.archetype?.id || contract.fadePlayerIds.some(id=>lineup.playerIds.includes(id))
      || (captain!=null && (contract.forbiddenCaptainIds.includes(captain) || (contract.eligibleCaptainIds?.length && !contract.eligibleCaptainIds.includes(captain))))
      || contract.beneficiaries.some(group=>lineup.playerIds.filter(id=>group.playerIds.includes(id)).length<group.minFromGroup)
      || (contract.minKickerDst!=null && lineup.slots.filter(s=>['K','DST'].includes(s.player.position)).length<contract.minKickerDst)
      || (contract.teamCountRange && (()=>{const count=lineup.slots.filter(s=>s.player.team===contract.teamCountRange!.team).length;
        return count<contract.teamCountRange!.min || count>contract.teamCountRange!.max;})())))throw new Error('Candidate violates frozen archetype construction rules.');
    candidates.set(key,lineup);
  }
  const keys=[...candidates.keys()].sort();
  let chosen=baseline.lineups.map(l=>validateNflLineup(slate,roster(l)).key);
  if(new Set(chosen).size!==chosen.length) throw new Error('Duplicate baseline rosters.');
  const cap=Math.min(settings.maxPairwiseOverlap??Infinity,(slate.format==='classic'?9:6)-settings.minUnique);
  const archetypeKey=(lineup:NflGeneratedLineup)=>JSON.stringify([lineup.archetype?.id??'standard',lineup.constructionContract??null]);
  const archetypes=new Map<string,number>();
  for(const key of chosen) {const id=archetypeKey(candidates.get(key)!);archetypes.set(id,(archetypes.get(id)??0)+1);}
  const bands=settings.salaryPolicy?.salaryLeftBands??[];
  const bandCount=(rows:NflGeneratedLineup[],band:typeof bands[number])=>rows.filter(l=>50000-l.totalSalary>=band.min && 50000-l.totalSalary<=band.max).length;
  const baselineBands=bands.map(b=>bandCount(baseline.lineups,b));
  const valid=(proposed:string[])=>{
    if(new Set(proposed).size!==proposed.length) return false;
    const rows=proposed.map(k=>candidates.get(k)!);
    if(rows.some(l=>l.totalSalary<(settings.salaryPolicy?.minSalaryUsed??settings.minSalary)
      || l.totalSalary>(settings.salaryPolicy?.maxSalaryUsed??50000)
      || (settings.salaryPolicy && (50000-l.totalSalary<settings.salaryPolicy.minSalaryLeft || 50000-l.totalSalary>settings.salaryPolicy.maxSalaryLeft))))return false;
    if(rows.some(l=>settings.lockedPlayerIds.some(id=>!l.playerIds.includes(id)) || settings.excludedPlayerIds.some(id=>l.playerIds.includes(id)))) return false;
    if((baseline.topProjectedPlayerIds??[]).some(id=>!rows.some(l=>l.playerIds.includes(id))))return false;
    const tagged=(l:NflGeneratedLineup,codes:Parameters<typeof isNflGppSignalPlayer>[1],rbOnly=false)=>l.slots.some(s=>(!rbOnly||s.player.position==='RB')&&isNflGppSignalPlayer(s.player.playerSignals,codes));
    if(settings.gppSignalMinPerLineup && rows.some(l=>!tagged(l,settings.gppSignalCodes)))return false;
    if(rows.filter(l=>tagged(l,['AIR_MATCHUP'])).length<Math.ceil(settings.nLineups*(settings.gppAirMatchupMinPct??0)/100))return false;
    if(rows.filter(l=>tagged(l,['INSIDE_FIVE'],true)).length<Math.ceil(settings.nLineups*(settings.gppGoalLineMinPct??0)/100))return false;
    if(settings.format==='classic' && settings.mode==='gpp' && settings.stackPassCatchers>0 && rows.some(l=>{
      const qb=l.slots.find(s=>s.player.position==='QB')?.player;
      return !qb || l.slots.filter(s=>['WR','TE'].includes(s.player.position)&&s.player.team===qb.team).length<settings.stackPassCatchers
        || (settings.bringBack&&!l.slots.some(s=>['RB','WR','TE'].includes(s.player.position)&&s.player.team===qb.opponent));
    }))return false;
    for(let i=0;i<rows.length;i++)for(let j=i+1;j<rows.length;j++)if(rows[i].playerIds.filter(id=>rows[j].playerIds.includes(id)).length>cap)return false;
    for(const report of baseline.exposureReport??[]) {
      const overall=rows.filter(l=>l.playerIds.includes(report.dkPlayerId)).length;
      const captain=rows.filter(l=>l.slots.some(s=>s.slot==='CPT' && s.player.dkPlayerId===report.dkPlayerId)).length;
      const flex=overall-captain;
      if(overall<report.overallMin||overall>report.overallMax||captain<report.captainMin||captain>report.captainMax||flex<report.flexMin||flex>report.flexMax)return false;
    }
    // Players new to the candidate pool still obey the overall cap.
    for(const id of new Set(rows.flatMap(l=>l.playerIds))) if(!(baseline.exposureReport??[]).some(r=>r.dkPlayerId===id)
      && rows.filter(l=>l.playerIds.includes(id)).length>Math.floor(settings.maxExposure*settings.nLineups+1e-9))return false;
    if(rows.some(l=>!archetypes.has(archetypeKey(l)))) return false;
    for(const [id,count] of archetypes)if(rows.filter(l=>archetypeKey(l)===id).length!==count)return false;
    return bands.every((band,i)=>bandCount(rows,band)===baselineBands[i]);
  };
  if(!valid(chosen)) throw new Error('Baseline violates frozen joint-selection constraints.');
  const draws=new Map(keys.map(k=>[k,scoreNflLineupDraws(slate,roster(candidates.get(k)!),selection)]));
  const cash=new Map(keys.map(k=>[k,summarizeNflDraws(draws.get(k)!,selection.weights,0,true).p10]));
  const objective=(proposed:string[])=>settings.mode==='cash'
    ? proposed.reduce((sum,k)=>sum+cash.get(k)!,0)/proposed.length
    : selection.weights.reduce((sum,w,i)=>sum+w*Math.max(...proposed.map(k=>draws.get(k)![i])),0);
  const baselineSelection=objective(chosen);
  let bestValue=baselineSelection, evaluations=0, improvements=0;
  // Start with a feasible complete portfolio. Every accepted swap preserves
  // the entire contract. Bounded search can never strand a Captain minimum.
  while(evaluations<budget) {
    let winner:string[]|null=null, value=bestValue;
    for(let slot=0;slot<chosen.length && evaluations<budget;slot++)for(const key of keys) {
      if(evaluations>=budget)break;
      if(chosen.includes(key))continue;
      evaluations++;
      const proposed=[...chosen];proposed[slot]=key;
      if(!valid(proposed))continue;
      const candidateValue=objective(proposed);
      if(candidateValue>value+1e-9){winner=proposed;value=candidateValue;}
    }
    if(!winner)break;
    chosen=winner;bestValue=value;improvements++;
  }
  const evaluate=(proposed:string[])=>{
    const rows=proposed.map(k=>scoreNflLineupDraws(slate,roster(candidates.get(k)!),evaluation));
    return {lineups:rows.map(row=>summarizeNflDraws(row,evaluation.weights,0,true)),
      bestLineup:summarizeNflDraws(evaluation.weights.map((_,i)=>Math.max(...rows.map(row=>row[i]))),evaluation.weights,0,true)};
  };
  const baselineKeys=baseline.lineups.map(l=>validateNflLineup(slate,roster(l)).key);
  return {version:JOINT_PORTFOLIO_VERSION,status:'shadow_complete' as const,productionChanged:false,exportAuthorized:false,
    objective:settings.mode==='cash'?'mean_joint_lineup_p10':'expected_best_joint_lineup_score',
    selected:chosen.map(k=>candidates.get(k)!),baselineSelection,selectedSelection:bestValue,
    baselineEvaluation:evaluate(baselineKeys),selectedEvaluation:evaluate(chosen),
    improvements,evaluations,searchLimitReached:evaluations>=budget,
    selectionManifest:selection.metadata,evaluationManifest:evaluation.metadata,
    limitations:['Supplied joint banks still require their registered forecast and portfolio qualifications.',
      'Bounded single-roster swaps preserve construction constraints; this is not proof of optimality.',
      'Joint score distributions are not a contest finish, payout or ROI estimate.']};
}

export function compareJointPortfolioIfAvailable(input:Parameters<typeof compareJointPortfolio>[0]) {
  try{return {status:'evaluated' as const,report:compareJointPortfolio(input),reason:null};}
  catch(error){return {status:'unavailable' as const,report:null,reason:error instanceof Error?error.message:'Joint scenarios are unavailable.',
    productionChanged:false,exportAuthorized:false};}
}
