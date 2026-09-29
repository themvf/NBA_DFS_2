import { availabilityCurrent, toDecisionClock, type Availability, type DecisionClock } from './availability';
import { salaryMatchesKickoff, benchmarkTeam } from './competitor-benchmark';

export type WorkloadProjection = {
  mean: number; p10: number; p50: number; p90: number; targets: number;
  baselineTargets: number; historyGames: number; snapshotId: string;
  referenceMean?:number;
  recipeDigest: string; rosterDigest: string; capturedAt: string; kickoff: string;
  identity: string; season: number; week: number; injuryAdjusted: boolean;
};
/** Model recipe version (model/nfl_dfs_target_share.py) and run container version (ingest/nfl_dfs_target_share.py). */
export const VOLUME_SHARE_MODEL_VERSION = 'nfl-dfs-volume-share-v1';
export const VOLUME_SHARE_RUN_VERSION = 'nfl-dfs-volume-share-run-v2';
export const WORKLOAD_MAX_AGE_MS = 72 * 3600000;
export type WorkloadReport = {
  version: string; report_version?: string; season: number; week: number; history_cutoff_exclusive: number[];
  /** Latest (season, week) in the history used; must be strictly before the target week. */
  history_through?: number[] | null;
  as_of: string; snapshot_digest: string; recipe_digest: string; roster_evidence_digest: string;
  sources: {season:number; latest_week?:number}[];
  forward: {team:string; kickoff:string; players: {identity:string; history_games:number;
    targets_baseline:number; targets_volume:number; fpts_baseline?:number; fpts_volume:number; p10:number; p50:number; p90:number}[]}[];
};
export type WorkloadTarget = {identity:string|null; position:string; team:string; gameInfo:string|null; isOut:boolean; availability?:Availability};

const before = (key: unknown, season: number, week: number) =>
  Array.isArray(key) && key.length === 2 && key.every(Number.isInteger) && (key[0] < season || key[0] === season && key[1] < week);

/**
 * History rule (run v2): every row strictly before the target week. The only
 * evidence for this source is a walk-forward replay over every regular-season
 * week of 2024-2025 (ingest/nfl_dfs_volume_benchmark.py), i.e. forecasts made at
 * in-season cutoffs from same-season history. The week-1 snapshot's
 * "prior seasons only" rule was a property of week 1, not of that evidence.
 * The v1 committed JSON is no longer accepted.
 *
 * Forecast times: captured at or before the slate's decision time (never a later
 * run), unexpired and unstarted at request time -- the same checks generation
 * repeats before saving.
 */
export function readWorkloadProjection(report:WorkloadReport, target:WorkloadTarget, season:number, week:number, nowOrClock:number|DecisionClock): {projection:WorkloadProjection|null; reason:string} {
  const clock = toDecisionClock(nowOrClock), now = clock.now;
  const no=(reason:string)=>({projection:null,reason});
  if(target.position!=='WR')return no('Historical baseline: this workload source currently supports WR.');
  if(!target.identity)return no('Canonical workload identity unresolved.');
  if(report.version!==VOLUME_SHARE_MODEL_VERSION||report.report_version!==VOLUME_SHARE_RUN_VERSION)return no('Workload snapshot predates the weekly database-backed refresh.');
  if(report.season!==season||report.week!==week||report.history_cutoff_exclusive?.[0]!==season||report.history_cutoff_exclusive?.[1]!==week||report.history_cutoff_exclusive.length!==2)return no('Workload snapshot does not match the target week.');
  if(!before(report.history_through,season,week)||!report.sources.length||report.sources.some(s=>!Number.isInteger(s.season)||!Number.isInteger(s.latest_week)||!before([s.season,s.latest_week],season,week)))return no('Workload history is not strictly before the target week.');
  const captured=Date.parse(report.as_of), decision=Date.parse(clock.decisionAt??'');
  if(!Number.isFinite(now)||!Number.isFinite(captured)||captured>now)return no('Workload snapshot time is invalid.');
  if(Number.isFinite(decision)&&captured>decision)return no('Workload snapshot was captured after this slate\'s projection cutoff.');
  if(now-captured>WORKLOAD_MAX_AGE_MS)return no('Workload snapshot expired; refresh the workload forecast.');
  if(![report.snapshot_digest,report.recipe_digest,report.roster_evidence_digest].every(s=>/^[a-f0-9]{64}$/.test(s)))return no('Workload provenance incomplete.');
  const games=report.forward.filter(g=>benchmarkTeam(g.team)===benchmarkTeam(target.team));
  if(games.length!==1)return no('Workload team/game is missing or ambiguous.');
  const game=games[0], kickoff=Date.parse(game.kickoff);
  if(!Number.isFinite(kickoff)||captured>=kickoff||now>=kickoff||!salaryMatchesKickoff(target.gameInfo,game.kickoff)||Date.parse(target.availability?.kickoff??'')!==kickoff)return no('Workload kickoff does not match an unstarted salary game.');
  if(target.isOut)return no('Excluded by availability.');
  const current=availabilityCurrent(target.availability,clock);
  if(!current.ok)return no(`Current roster eligibility is unavailable or stale: ${current.reason}`);
  const matches=game.players.filter(p=>p.identity===target.identity);
  if(matches.length!==1)return no('No unique same-team historical workload forecast.');
  const p=matches[0];
  if(![p.fpts_volume,p.p10,p.p50,p.p90,p.targets_volume,p.targets_baseline,p.history_games].every(Number.isFinite)||p.fpts_volume<=0||p.targets_volume<0||p.targets_baseline<0||p.history_games<4||p.p10>p.p50||p.p50>p.p90)return no('Workload projection or distribution is invalid.');
  return {projection:{mean:p.fpts_volume,p10:p.p10,p50:p.p50,p90:p.p90,targets:p.targets_volume,baselineTargets:p.targets_baseline,...(Number.isFinite(p.fpts_baseline)?{referenceMean:p.fpts_baseline}:{}),historyGames:p.history_games,snapshotId:report.snapshot_digest,recipeDigest:report.recipe_digest,rosterDigest:report.roster_evidence_digest,capturedAt:report.as_of,kickoff:game.kickoff,identity:target.identity,season,week,injuryAdjusted:false},reason:'Unadjusted WR workload forecast; experimental.'};
}

/** Why a player is (not) in the shared pregame cohort for workload runs. */
export function workloadPoolReason(target:Omit<WorkloadTarget,'identity'>,nowOrClock:number|DecisionClock):{ok:boolean;reason:string} {
  const clock=toDecisionClock(nowOrClock), now=clock.now;
  const a=target.availability, kickoff=a?.kickoff??'';
  if(!Number.isFinite(now))return {ok:false,reason:'Request time is invalid.'};
  if(target.isOut)return {ok:false,reason:'Excluded by availability.'};
  if(a?.blockedReason)return {ok:false,reason:a.blockedReason};
  if(!(Date.parse(kickoff)>now))return {ok:false,reason:Number.isFinite(Date.parse(kickoff))?'The game has started.':'Kickoff is unresolved.'};
  if(!salaryMatchesKickoff(target.gameInfo,kickoff))return {ok:false,reason:'Salary game does not match the scheduled kickoff.'};
  if(target.position==='DST')return {ok:true,reason:'Defense: no roster-role evidence required.'};
  const current=availabilityCurrent(a,clock);
  if(!current.ok)return current;
  if(target.position==='QB'&&a!.role!=='Expected starter · QB1')return {ok:false,reason:'Quarterback is not the resolved expected starter (QB1).'};
  return {ok:true,reason:current.reason};
}

/** Shared pregame cohort for both arms; unresolved QB roles must not become starters. */
export function workloadPoolEligible(target:Omit<WorkloadTarget,'identity'>,nowOrClock:number|DecisionClock):boolean {
  return workloadPoolReason(target,nowOrClock).ok;
}
