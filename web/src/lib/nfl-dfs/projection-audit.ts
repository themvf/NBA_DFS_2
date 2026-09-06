import { selectedWorkload, type WorkloadPositions, type PositionWorkloadPlayer } from './workload-selection';
import { allocateRoles, type RoleSettings } from './role-allocation';
import { benchmarkTeam } from './competitor-benchmark';
import { redistributeInjuryTargets, type InjuryRole } from './injury-redistribution';
import type { EfficiencyRate } from './efficiency';
import { PASS_YARD_PTS, PASS_TD_PTS, INTERCEPTION_PTS, RUSH_YARD_PTS, RUSH_TD_PTS, REC_YARD_PTS, REC_TD_PTS, RECEPTION_PTS } from './scoring';

export type TeamAssumption = { profile: 'all'|'neutral'|'leading'|'trailing'; roles: RoleSettings; reason: string; passingEfficiency: number };
export type SituationSettings = { enabled: boolean; teams: Record<string, TeamAssumption> };
export const DEFAULT_SITUATIONS: SituationSettings = { enabled: true, teams: {} };
export type TeamProfile = { designed_run_rate:number; scramble_rate:number; sack_rate:number; target_rate:number; plays_per_game:number };
export type SituationTeam = {
  team:string; capturedAt:string; recipeDigest:string; rosterDigest:string; coachingDigest:string;
  continuity:string; coaching:unknown; profiles:Record<string,TeamProfile|null>; members:InjuryRole[];
  currentQb:string|null; historicalQb:string|null; reason:string|null;
};
export type SituationEvidence = { team:SituationTeam|null; rates:Record<string,EfficiencyRate>|null; ratesDigest:string|null; ratesAsOf:string|null; reason:string|null };
export type AuditStep = { label:string; status:'applied'|'not_applied'; points:number; reason:string; calculation?:{before:number;after:number;pointsPerUnit:number;unit:string} };
export type ProjectionAudit = {
  version:'nfl-projection-audit-v1'; baseline:number|null; final:number|null; steps:AuditStep[];
  modelSnapshot:unknown; evidence:unknown; assumption:TeamAssumption|null;
  rangeMethod:string; source:string; excluded:boolean;
  opportunities?:{targets?:number;carries?:number;attempts?:number};
};
export type AuditPlayer = PositionWorkloadPlayer & { dkPlayerId:number; ffPlayerId?:number|null; team:string; floorFpts:number|null; ceilingFpts:number|null; situationEvidence?:SituationEvidence; projectionAudit?:ProjectionAudit };

export function validateSituations(settings:SituationSettings|undefined, teams:string[]) {
  if(settings===undefined)return;
  if(!settings||typeof settings.enabled!=='boolean'||!settings.teams||typeof settings.teams!=='object'||Array.isArray(settings.teams))throw new Error('Invalid situation settings.');
  if(new Set(Object.keys(settings.teams).map(benchmarkTeam)).size!==Object.keys(settings.teams).length)throw new Error('Duplicate situation team alias.');
  for(const [team,a] of Object.entries(settings.teams)) {
    if(!teams.some(t=>benchmarkTeam(t)===benchmarkTeam(team)))throw new Error('Situation team is not in this slate.');
    if(!a||!['all','neutral','leading','trailing'].includes(a.profile)||typeof a.reason!=='string'||a.reason.trim().length<10||a.reason.length>2000||!Number.isFinite(a.passingEfficiency)||a.passingEfficiency<.5||a.passingEfficiency>1.5||!a.roles||typeof a.roles!=='object'||Array.isArray(a.roles))throw new Error('Team assumptions require a reason (10–2000 characters), a profile, role shares and a passing-efficiency multiplier from 0.5 to 1.5.');
    for(const r of Object.values(a.roles))if(!r||typeof r!=='object'||Object.keys(r).some(k=>!['targets','carries'].includes(k))||Object.values(r).some(v=>!Number.isFinite(v)||v!<0||v!>1))throw new Error('Role shares must be finite fractions from zero to one.');
  }
}

export function prepareProjectionAudits<T extends AuditPlayer>(players:T[], teams:SituationTeam[], positions:WorkloadPositions|undefined, settings:SituationSettings|undefined, now:number):T[] {
  validateSituations(settings,players.map(p=>p.team));
  if(settings?.enabled)for(const [key,a] of Object.entries(settings.teams)) {
    const team=teams.find(t=>t.team===benchmarkTeam(key));
    if(!team||team.reason||!Number.isFinite(Date.parse(team.capturedAt))||Date.parse(team.capturedAt)>now||now-Date.parse(team.capturedAt)>72*3600000)throw new Error(`${key}: refresh the team-context snapshot before applying assumptions.`);
    for(const [id,role] of Object.entries(a.roles)) {
      const m=team.members.find(m=>m.id===id);
      if(!m)throw new Error(`${key}: saved role no longer matches the full roster.`);
      if(role.carries!==undefined&&!['RB','FB'].includes(m.position)||role.targets!==undefined&&!['RB','FB','WR','TE'].includes(m.position))throw new Error(`${m.name}: this position does not support that role override. QB rushing remains at baseline.`);
    }
    const all=team.profiles.all,next=team.profiles[a.profile];if(!all||!next)throw new Error(`${key}: profile unavailable.`);
    const b=budgets(next,all.plays_per_game);
    allocateRoles(team.members.map(m=>({...m,out:!!m.availability.blockedReason,evidence_current:m.availability.fresh,captured_at:m.availability.capturedAt,role:m.availability.role==='Expected starter · QB1'?'Listed QB1':m.role})),a.roles,b.targets,b.carries,now);
    if(!players.some(p=>benchmarkTeam(p.team)===team.team&&!p.isOut&&p.workloadEligible!==false&&selectedWorkload(p,positions)&&p.situationEvidence?.rates&&!p.situationEvidence.reason))throw new Error(`${key}: no enabled player has matching workload and efficiency evidence. Assumption cannot affect this run.`);
  }
  return players.map(p=>auditProjection({...p,situationEvidence:{...(p.situationEvidence??{rates:null,ratesAsOf:null,ratesDigest:null,reason:'Efficiency evidence unavailable.'}),team:teams.find(t=>t.team===benchmarkTeam(p.team))??null}},positions,settings,now));
}

function budgets(p:TeamProfile, plays:number) {
  if(![plays,p.designed_run_rate,p.scramble_rate,p.sack_rate,p.target_rate].every(Number.isFinite)||plays<=0||[p.designed_run_rate,p.scramble_rate,p.sack_rate,p.target_rate].some(v=>v<0||v>1)||p.scramble_rate+p.sack_rate>1)throw new Error('Invalid team play profile.');
  const dropbacks=plays*(1-p.designed_run_rate), attempts=dropbacks*(1-p.scramble_rate-p.sack_rate);
  // Historical carry shares include QB scrambles, so the carry budget must too.
  return {attempts,targets:attempts*p.target_rate,carries:plays*p.designed_run_rate+dropbacks*p.scramble_rate};
}

/** Linear marginal scoring only. Bonuses/turnovers outside these rates are held fixed. */
function coefficients(r:Record<string,EfficiencyRate>) {
  const get=(k:string)=>{const v=r[k]?.mean;if(!Number.isFinite(v)||v<0)throw new Error(`Missing efficiency rate: ${k}`);return v;};
  return {
    targets:()=>get('catch_rate')*(RECEPTION_PTS+get('receiving_yards_per_reception')*REC_YARD_PTS+get('receiving_td_rate')*REC_TD_PTS),
    carries:()=>get('rushing_yards_per_carry')*RUSH_YARD_PTS+get('rushing_td_rate')*RUSH_TD_PTS,
    attempts:()=>get('completion_rate')*(get('passing_yards_per_completion')*PASS_YARD_PTS+get('passing_td_rate')*PASS_TD_PTS)+get('interception_rate')*INTERCEPTION_PTS,
  };
}

/** Never mutate the source forecasts: repeat previews must not compound adjustments. */
export function auditProjection<T extends AuditPlayer>(player:T, positions:WorkloadPositions|undefined, settings:SituationSettings|undefined, now:number):T {
  const candidate=selectedWorkload(player,positions), baseline=player.ourProj;
  const evidence=player.situationEvidence??null, team=evidence?.team;
  const assumption=settings?.teams[player.team]??settings?.teams[benchmarkTeam(player.team)]??null;
  const steps:AuditStep[]=[];
  const add=(label:string,reason:string,points=0,calculation?:AuditStep['calculation'])=>steps.push({label,reason,points,calculation,status:points!==0?'applied':'not_applied'});
  if(candidate) {
    const reference='baselineMean' in candidate?candidate.baselineMean:candidate.referenceMean??baseline;
    if(reference!=null&&baseline!=null)add('Baseline snapshot alignment','Difference between the saved slate projection and the workload study’s market-free historical reference. Includes snapshot/model differences; not attributed to injury or recent workload.',reference-baseline);
    if('explanationTerms' in candidate&&candidate.explanationTerms?.length)for(const term of candidate.explanationTerms) {
      const names:Record<string,string>={baseline:'Baseline calibration',history_games:'History sample-size calibration',prior_opportunity:`Recent ${player.position==='QB'?'attempts':player.position==='RB'?'carries + targets':'targets'} calibration`};
      add(names[term.name]??term.name,`Statistical correction, not a causal situation effect: (${term.input.toFixed(4)} − ${term.center.toFixed(4)}) ÷ ${term.scale.toFixed(4)} × ${term.coefficient.toFixed(4)}.`,term.points);
    }else add('Workload model',player.position==='WR'?'Team pass volume × same-team target share; this model change is not an injury boost.':'Pinned regression on baseline, history count and recent opportunities; this difference is not a causal injury or matchup effect.',candidate.mean-(reference??0));
  }
  else add('Workload model','Position disabled or candidate unavailable; retain the selected fallback.');
  let delta=0;
  const opportunities:ProjectionAudit['opportunities']={};
  let rangeMethod='Original model player ranges. Lineup sums are not lineup percentiles.';
  const audit=(adjusted:T=player):T=>({...adjusted,situationEvidence:evidence?{...evidence,team:null}:undefined,projectionAudit:{version:'nfl-projection-audit-v1',baseline,final:candidate?candidate.mean+delta:baseline,steps,opportunities,modelSnapshot:candidate,evidence:evidence?{...evidence,team:team?{team:team.team,capturedAt:team.capturedAt,recipeDigest:team.recipeDigest,rosterDigest:team.rosterDigest,coachingDigest:team.coachingDigest,continuity:team.continuity,coaching:team.coaching,currentQb:team.currentQb,historicalQb:team.historicalQb,member:team.members.find(m=>m.id===String(player.ffPlayerId))}:null}:null,assumption,rangeMethod,source:candidate?'workload':'historical fallback',excluded:player.isOut||player.workloadEligible===false}});
  if(!settings?.enabled||!candidate||player.isOut||player.workloadEligible===false) {
    add('Situation adjustments',!settings?.enabled?'Situation adjustments disabled (legacy saved settings remain unchanged).':'No eligible selected workload forecast; no situation uplift.');
    return audit();
  }
  const knownMember=team?.members.find(m=>m.id===String(player.ffPlayerId));
  add('Roster and role',knownMember?`${knownMember.rookie?'Rookie':knownMember.new_team?'New team':'Same-team player'}; ${knownMember.availability.role}. Prior-team shares are never transferred.`:'No current full-roster identity match.');
  add('Coaching and scheme',team?`${team.continuity}. ${assumption?`Reviewed ${assumption.profile} profile assumption: ${assumption.reason}`:'Historical profiles are references; no automatic coaching boost.'}`:'No team context.');
  add('Starting quarterback',team?.currentQb&&team.currentQb===team.historicalQb?'Current starter matches the historical reference.':`Starter changed or unresolved. Automatic injury redistribution blocked; efficiency requires an explicit assumption. Current: ${team?.currentQb??'unknown'}; reference: ${team?.historicalQb??'unknown'}.`);
  add('Defender injuries and matchup','No calibrated defender/scheme interaction coefficient. No automatic point adjustment; reviewed passing-efficiency assumptions are labeled separately.');
  if(!team||team.reason||!evidence?.rates||evidence.reason||!Number.isFinite(now)||!Number.isFinite(Date.parse(evidence.ratesAsOf??''))||Date.parse(evidence.ratesAsOf!)>now||now-Date.parse(evidence.ratesAsOf!)>72*3600000||!Number.isFinite(Date.parse(team.capturedAt))||Date.parse(team.capturedAt)>now||now-Date.parse(team.capturedAt)>72*3600000||Date.parse(candidate.kickoff)<=now) {
    add('Situation adjustments',team?.reason??evidence?.reason??'Team context or efficiency evidence is unavailable, stale or locked.');return audit();
  }
  const member=team.members.find(m=>m.id===String(player.ffPlayerId));
  const stamp=Date.parse(member?.availability.evaluatedAt??'');
  if(!member||!member.availability.fresh||member.availability.blockedReason||!Number.isFinite(stamp)||stamp>now||now-stamp>60000||Date.parse(member.availability.kickoff??'')!==Date.parse(candidate.kickoff)) {
    add('Roster and role','Current full-roster identity, availability or kickoff is unresolved.');return audit();
  }
  try {
    const all=team.profiles.all;if(!all)throw new Error('Historical team budget unavailable.');
    const baseBudget=budgets(all,all.plays_per_game), nextProfile=team.profiles[assumption?.profile??'all'];
    if(!nextProfile)throw new Error('Selected historical profile unavailable.');
    const nextBudget=budgets(nextProfile,all.plays_per_game);
    // Full roster is conserved even when recipients are absent from the salary pool.
    const refreshed=team.members.map(m=>({...m,out:Boolean(m.availability.blockedReason),evidence_current:m.availability.fresh,captured_at:m.availability.capturedAt,role:m.availability.role==='Expected starter · QB1'?'Listed QB1':m.role}));
    const before=allocateRoles(refreshed,{},baseBudget.targets,baseBudget.carries,now);
    const after=allocateRoles(refreshed,assumption?.roles??{},nextBudget.targets,nextBudget.carries,now);
    const b=before.rows.find(r=>r.id===member.id)!, a=after.rows.find(r=>r.id===member.id)!;
    let injuryGain=0, hasInjury=false;
    if(Object.keys(assumption?.roles??{}).length)add('Teammate injury','Manual full-roster shares replace automatic redistribution to prevent double counting.');
    else try {
      const injury=redistributeInjuryTargets(team.members,nextBudget.targets,team.currentQb,team.historicalQb,now);
      hasInjury=true;
      injuryGain=injury.rows.find(r=>r.id===member.id)?.gain??0;
      add('Teammate injury evidence',`Verified inactive ${team.members.find(m=>m.id===injury.absentId)?.name}. Fixed hypothesis: redistribute 50% of removed share pro rata; ${injury.reservedTargets.toFixed(2)} team targets remain unassigned. Not calibrated.`);
    }catch(e){add('Teammate injury',e instanceof Error?e.message:'Redistribution unavailable.');}
    if(!assumption&&!hasInjury)return audit();
    const rate=coefficients(evidence.rates);
    const opp='targets' in candidate?candidate.targets:candidate.priorOpportunity;
    if(opp===undefined||!Number.isFinite(opp)||opp<0)throw new Error('Starting workload reference unavailable.');
    // RB's existing regression uses carries + targets. Split only using known same-team role evidence.
    const rbTotal=b.targets+b.carries;
    const startTargets=player.position==='RB'?(rbTotal>0?opp*b.targets/rbTotal:0):['WR','TE'].includes(player.position)?opp:0;
    const startCarries=player.position==='RB'?(rbTotal>0?opp*b.carries/rbTotal:0):0;
    if((member.rookie||member.new_team)&&!assumption?.roles[member.id])throw new Error('Rookie/arrival needs reviewed role shares; no inherited team-share adjustment.');
    const marginal=(label:string,from:number,to:number,unit:'targets'|'carries'|'attempts',reason:string)=>{
      if(Math.abs(to-from)<1e-12)return;
      const c=rate[unit]();if(!Number.isFinite(c))throw new Error('Invalid scoring coefficient.');
      const points=(to-from)*c;delta+=points;add(label,reason,points,{before:from,after:to,pointsPerUnit:c,unit});
    };
    if(['RB','WR','TE'].includes(player.position)) {
      if(player.position==='RB'&&rbTotal===0&&opp>0)throw new Error('RB carry/target split is unresolved; cannot subtract matching baseline components.');
      const schemeTargets=b.targetShare*nextBudget.targets;
      const roleTargets=a.targets;
      opportunities.targets=roleTargets+injuryGain;
      marginal('Full-roster target reference',startTargets,b.targets,'targets','Align the selected workload reference to the full-roster historical budget before applying situation changes; this is a model-reference change, not an injury effect.');
      marginal('Team passing volume',b.targets,schemeTargets,'targets','Same share with the reviewed team passing budget.');
      marginal('Reviewed target role',schemeTargets,roleTargets,'targets',assumption?.reason??'No role override.');
      marginal('Injury target redistribution',roleTargets,roleTargets+injuryGain,'targets','Additional targets from the verified inactive teammate; 50% redistribution hypothesis.');
      marginal('Reviewed passing efficiency',roleTargets+injuryGain,(roleTargets+injuryGain)*(assumption?.passingEfficiency??1),'targets',`Equivalent-target scoring multiplier, not extra team targets: ${assumption?.reason??''}`);
      if(player.position==='RB') {
        opportunities.carries=a.carries;
        const schemeCarries=b.carryShare*nextBudget.carries;
        marginal('Full-roster carry reference',startCarries,b.carries,'carries','RB combined opportunities split by same-team shares, then aligned to the full-roster carry budget.');
        marginal('Team rushing volume',b.carries,schemeCarries,'carries','Same carry share with the reviewed team rushing budget, including scrambles.');
        marginal('Reviewed carry role',schemeCarries,a.carries,'carries',assumption?.reason??'No override.');
      }
    }else if(player.position==='QB') {
      if(!assumption)return audit(); // A receiver absence alone does not create extra QB attempts.
      if(team.currentQb!==member.identity)throw new Error('A unique current starting QB is required for the team passing budget.');
      const attempts=nextBudget.attempts;
      opportunities.attempts=attempts;
      marginal('Full-roster pass reference',opp,baseBudget.attempts,'attempts','Current starting QB takes the team passing budget in this reviewed scenario; backup attempts are not added.');
      marginal('Team passing volume',baseBudget.attempts,attempts,'attempts','Reviewed team pass-attempt budget. QB rushing baseline held fixed.');
      marginal('Reviewed QB efficiency',attempts,attempts*(assumption?.passingEfficiency??1),'attempts',`Equivalent-attempt scoring multiplier: ${assumption?.reason??''}`);
    }
    if(delta!==0) {
      if(candidate.mean+delta<=0)throw new Error('Adjustment produces a nonpositive projection; reduce assumptions.');
      rangeMethod='Experimental location shift: same point delta added to player P10/P50/P90. Spread, bonuses and fumble rates held fixed; boom bonus disabled. These are uncalibrated scenario ranges, not new simulated quantiles.';
      const updated={...candidate,mean:candidate.mean+delta,p10:candidate.p10+delta,p50:candidate.p50+delta,p90:candidate.p90+delta,...('boom' in candidate?{boom:0}:{targets:opportunities.targets??candidate.targets,injuryAdjusted:injuryGain>0})};
      return audit({...player,...(player.position==='WR'?{workload:updated}:{positionWorkload:updated})} as T);
    }
  }catch(e) {
    // Atomic per-player application: never retain half of a failed adjustment chain.
    const reason=e instanceof Error?e.message:'Situation calculation unavailable.';
    for(const s of steps)if(s.calculation){s.points=0;s.status='not_applied';s.reason=`Entire situation adjustment withheld: ${reason}`;}
    delta=0;add('Situation adjustment withheld',reason);
    delete opportunities.targets;delete opportunities.carries;delete opportunities.attempts;
    if(assumption)throw new Error(`${player.team}: ${reason}`);
  }
  return audit();
}
