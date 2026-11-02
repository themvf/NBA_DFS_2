import type {BoomBoard} from "./nba-boom-lineups";
export const AVAILABILITY_POLICY="nba-boom-exposure-v2-availability";
export type AvailabilityEvidence={sha:string;publishedAt:string;observedAt:string;sourceUrl:string;states:Record<string,{status:string;reason:string}>};
export type InjuryState={game_date:string;player_key:string;home_key:string;away_key:string;status:string;reason:string};
export type Identity={event_id:string;player_key:string;home_team:string;away_team:string};
const key=(s:string)=>{const k=s.normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase().replace(/[^a-z0-9]/g,"");return k==="laclippers"?"losangelesclippers":k;};
export function matchAvailability(date:string,candidates:Identity[],rows:InjuryState[]):AvailabilityEvidence["states"] {
 const result:AvailabilityEvidence["states"]={};
 for(const c of candidates){
  const hits=rows.filter(r=>r.game_date===date&&r.player_key===c.player_key&&r.home_key===key(c.home_team)&&r.away_key===key(c.away_team));
  result[`${c.event_id}:${c.player_key}`]=hits.length===1?{status:hits[0].status,reason:hits[0].reason}:{status:"unverified",reason:hits.length?"Ambiguous identity":"Not listed in the latest observed report; participation unverified"};
 }
 return result;
}
/** New discretionary selection policy; never changes the original snapshot or registered boom trial. */
export function availabilityBoard(board:BoomBoard|null,evidence:AvailabilityEvidence|null):BoomBoard|null {
 if(!board)return null;
 const players=board.players.map(p=>({...p,availability:evidence?.states[p.id]?.status??"unverified"})).filter(p=>p.availability.toLowerCase()!=="out");
 return {...board,players,excluded:board.excluded+board.players.length-players.length};
}
