import type { Availability } from './availability';
import { salaryMatchesKickoff, benchmarkTeam } from './competitor-benchmark';

export type WorkloadProjection = {
  mean: number; p10: number; p50: number; p90: number; targets: number;
  baselineTargets: number; historyGames: number; snapshotId: string;
  recipeDigest: string; rosterDigest: string; capturedAt: string; kickoff: string;
  identity: string; season: number; week: number; injuryAdjusted: false;
};
export type WorkloadReport = {
  version: string; season: number; week: number; history_cutoff_exclusive: number[];
  as_of: string; snapshot_digest: string; recipe_digest: string; roster_evidence_digest: string;
  sources: {season:number}[];
  forward: {team:string; kickoff:string; players: {identity:string; history_games:number;
    targets_baseline:number; targets_volume:number; fpts_volume:number; p10:number; p50:number; p90:number}[]}[];
};
export type WorkloadTarget = {identity:string|null; position:string; team:string; gameInfo:string|null; isOut:boolean; availability?:Availability};
export function readWorkloadProjection(report:WorkloadReport, target:WorkloadTarget, season:number, week:number, now:number): {projection:WorkloadProjection|null; reason:string} {
  const no=(reason:string)=>({projection:null,reason});
  if(target.position!=='WR')return no('Historical baseline: this workload source currently supports WR.');
  if(!target.identity)return no('Canonical workload identity unresolved.');
  if(report.version!=='nfl-dfs-volume-share-v1'||report.season!==season||report.week!==week||report.history_cutoff_exclusive?.[0]!==season||report.history_cutoff_exclusive?.[1]!==week||report.history_cutoff_exclusive.length!==2)return no('Workload snapshot does not match the target week.');
  if(!report.sources.length||report.sources.some(s=>!Number.isInteger(s.season)||s.season>=season))return no('Workload source history is not from prior seasons.');
  const captured=Date.parse(report.as_of);
  if(!Number.isFinite(now)||!Number.isFinite(captured)||captured>now||now-captured>72*3600000)return no('Workload snapshot expired; refresh the workload forecast.');
  if(![report.snapshot_digest,report.recipe_digest,report.roster_evidence_digest].every(s=>/^[a-f0-9]{64}$/.test(s)))return no('Workload provenance incomplete.');
  const games=report.forward.filter(g=>benchmarkTeam(g.team)===benchmarkTeam(target.team));
  if(games.length!==1)return no('Workload team/game is missing or ambiguous.');
  const game=games[0], kickoff=Date.parse(game.kickoff);
  if(!Number.isFinite(kickoff)||captured>=kickoff||now>=kickoff||!salaryMatchesKickoff(target.gameInfo,game.kickoff)||Date.parse(target.availability?.kickoff??'')!==kickoff)return no('Workload kickoff does not match an unstarted salary game.');
  const evaluated=Date.parse(target.availability?.evaluatedAt??'');
  if(target.isOut||!target.availability?.fresh||target.availability.blockedReason||!Number.isFinite(evaluated)||evaluated>now||now-evaluated>60000)return no('Current roster eligibility is unavailable or stale.');
  const matches=game.players.filter(p=>p.identity===target.identity);
  if(matches.length!==1)return no('No unique same-team historical workload forecast.');
  const p=matches[0];
  if(![p.fpts_volume,p.p10,p.p50,p.p90,p.targets_volume,p.targets_baseline,p.history_games].every(Number.isFinite)||p.fpts_volume<=0||p.targets_volume<0||p.targets_baseline<0||p.history_games<4||p.p10>p.p50||p.p50>p.p90)return no('Workload projection or distribution is invalid.');
  return {projection:{mean:p.fpts_volume,p10:p.p10,p50:p.p50,p90:p.p90,targets:p.targets_volume,baselineTargets:p.targets_baseline,historyGames:p.history_games,snapshotId:report.snapshot_digest,recipeDigest:report.recipe_digest,rosterDigest:report.roster_evidence_digest,capturedAt:report.as_of,kickoff:game.kickoff,identity:target.identity,season,week,injuryAdjusted:false},reason:'Unadjusted WR workload forecast; experimental.'};
}

/** Shared pregame cohort for both arms; unresolved QB roles must not become starters. */
export function workloadPoolEligible(target:Omit<WorkloadTarget,'identity'>,now:number):boolean {
  const a=target.availability, kickoff=a?.kickoff??'';
  if(!Number.isFinite(now)||target.isOut||a?.blockedReason||Date.parse(kickoff)<=now||!salaryMatchesKickoff(target.gameInfo,kickoff))return false;
  if(target.position==='DST')return true;
  const evaluated=Date.parse(a?.evaluatedAt??'');
  if(!a?.fresh||!Number.isFinite(evaluated)||evaluated>now||now-evaluated>60000)return false;
  return target.position!=='QB'||a.role==='Expected starter · QB1';
}
