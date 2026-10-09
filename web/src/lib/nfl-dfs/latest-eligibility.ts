import { presentPinnedGameAvailability, nflTeamKey, type Availability, type PinnedGameAvailabilityDecision } from './availability';
import { parseDkGameInfoKickoff } from './workspace-stage';

type ReviewPlayer={ffPlayerId:number|null;team:string;gameInfo:string|null;isOut:boolean;availability?:Availability;
  ourProj:number|null;floorFpts:number|null;medianFpts?:number|null;ceilingFpts:number|null;boomRate:number|null};
export type LatestEligibilityRow={playerId:number;team:string|null;decision:PinnedGameAvailabilityDecision|null};
export type LatestEligibilityReview={runId:string;asOf:string;decision:PinnedGameAvailabilityDecision};

/** A current eligibility overlay is separate from the saved forecast source.
 * Later exclusions can tighten an open slate; no later active row clears OUT.
 * No opportunity/efficiency transfer and no historical artifact rewrite.
 */
export function applyLatestEligibility<T extends ReviewPlayer>(players:T[], rows:LatestEligibilityRow[],
  input:{baselineAt:string|null;reviewRunId:string;reviewAt:string;now:number}):Array<T & {availability?:Availability;latestEligibilityReview?:LatestEligibilityReview}> {
  const at=Date.parse(input.reviewAt),baseline=Date.parse(input.baselineAt??'');
  if(!Number.isFinite(input.now)||!Number.isFinite(at)||!Number.isFinite(baseline)||at<=baseline||at>input.now)return players;
  if(new Set(rows.map(row=>row.playerId)).size!==rows.length) throw new Error('Duplicate latest availability decisions. Refresh data before building.');
  const decisions=new Map(rows.map(row=>[row.playerId,row]));
  return players.map(player=>{
    const row=decisions.get(player.ffPlayerId??-1),decision=row?.decision;
    if(!decision || !row?.team || nflTeamKey(row.team)!==nflTeamKey(player.team))return player;
    const kickoff=Date.parse(parseDkGameInfoKickoff(player.gameInfo)??'');
    const captured=Date.parse(decision.available_at??''),resolved=Date.parse(decision.as_of_at);
    if(!Number.isFinite(kickoff)||input.now>=kickoff||Date.parse(decision.kickoff??'')!==kickoff
      || !Number.isFinite(captured)||!Number.isFinite(resolved)||captured>resolved||resolved>at||resolved<=baseline
      || input.now-captured>72*3600000 || captured>input.now
      || !Array.isArray(decision.qualifying_observation_ids) || !decision.qualifying_observation_ids.length || decision.source_snapshot_id==null)return player;
    if(!['OUT_CONFIRMED','QUESTIONABLE','DOUBTFUL'].includes(decision.state)||player.isOut
      || player.availability?.blockedReason?.startsWith('Unavailable'))return player;
    const availability=presentPinnedGameAvailability(decision,player.availability?.role??'Role unresolved',player.availability?.roleBlockedReason??null);
    const review={runId:input.reviewRunId,asOf:input.reviewAt,decision};
    return {...player,availability,availabilityStatus:availability.status,latestEligibilityReview:review,
      ...(decision.state==='OUT_CONFIRMED'?{isOut:true,ruledOut:true,ourProj:0,floorFpts:0,medianFpts:0,ceilingFpts:0,boomRate:0}: {})};
  });
}
