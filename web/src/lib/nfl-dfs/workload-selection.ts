import type {CalibratedProjection} from './calibrated-projection';
import type {WorkloadProjection} from './workload-projection';
export const WORKLOAD_POSITIONS = ['QB','RB','WR','TE'] as const;
export type WorkloadPosition = typeof WORKLOAD_POSITIONS[number];
export type WorkloadPositions = Record<WorkloadPosition,boolean>;
// Preserve old saved WR-only settings when the new control is absent.
export const LEGACY_WORKLOAD_POSITIONS:WorkloadPositions = {QB:false,RB:false,WR:true,TE:false};
export const DEFAULT_WORKLOAD_POSITIONS:WorkloadPositions = {QB:true,RB:false,WR:true,TE:false};
export type PositionWorkloadPlayer = {position:string; workload?:WorkloadProjection|null; workloadReason?:string; positionWorkload?:CalibratedProjection|null; positionWorkloadReason?:string; ourProj:number|null;avgFptsDk:number|null;isOut:boolean;workloadEligible?:boolean};
export function selectedWorkload(player:PositionWorkloadPlayer, controls?:WorkloadPositions):WorkloadProjection|CalibratedProjection|null {
  if(!(controls??LEGACY_WORKLOAD_POSITIONS)[player.position as WorkloadPosition])return null;
  return player.position==='WR'?player.workload??null:['QB','RB','TE'].includes(player.position)?player.positionWorkload??null:null;
}
export function workloadSourceReason(player:PositionWorkloadPlayer, controls:WorkloadPositions|undefined, allowDkFallback:boolean):string {
  if(player.isOut)return 'Excluded by availability.';
  if(player.workloadEligible===false)return 'Excluded from workload runs: roster, QB starter or kickoff evidence unresolved.';
  const supported=WORKLOAD_POSITIONS.includes(player.position as WorkloadPosition);
  const enabled=supported&&(controls??LEGACY_WORKLOAD_POSITIONS)[player.position as WorkloadPosition];
  if(selectedWorkload(player,controls))return `${player.position} workload candidate`;
  const reason=!supported?'Position retains baseline.':!enabled?'Workload disabled for this position.':player.position==='WR'?player.workloadReason:player.positionWorkloadReason;
  const fallback=player.ourProj!==null&&player.ourProj>0?'Historical fallback':allowDkFallback&&player.avgFptsDk!==null&&player.avgFptsDk>0?'DK average fallback':'No usable fallback; excluded';
  return `${fallback}: ${reason??'Candidate unavailable.'}`;
}
export function validateWorkloadPositions(controls:WorkloadPositions|undefined) {
  if(controls===undefined)return;
  if(!controls||typeof controls!=='object'||Object.keys(controls).length!==4||!WORKLOAD_POSITIONS.every(p=>typeof controls[p]==='boolean'))throw new Error('Workload controls require true/false for QB, RB, WR and TE.');
  if(!WORKLOAD_POSITIONS.some(p=>controls[p]))throw new Error('Enable at least one workload position or choose the historical source.');
}
