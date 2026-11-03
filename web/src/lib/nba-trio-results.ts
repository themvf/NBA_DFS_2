import type {BoomGradeRow} from './nba-boom-trios';

/** Two distinct outcomes. Any unresolved/DNP member keeps the entire comparison incomplete. */
export function trioPortfolioResult(lineups:string[][],rows:BoomGradeRow[]|null) {
  const totals:(number|null)[]=[],unresolved=new Set<string>();
  const byId=new Map<string,BoomGradeRow>();
  for(const r of rows??[]){const id=`${r.event_id}:${r.player_key}`;if(byId.has(id))return {status:'ambiguous' as const,bestOfFive:null,allFive:null,totals:[],unresolved:[id]};byId.set(id,r)}
  if(lineups.length!==5 || new Set(lineups.map(t=>[...t].sort().join('|'))).size!==5 || lineups.some(t=>t.length!==3||new Set(t).size!==3))return {status:'invalid' as const,bestOfFive:null,allFive:null,totals:[],unresolved:[]};
  for(const trio of lineups){let sum=0,complete=true;for(const id of trio){const r=byId.get(id);if(!r || !['graded','push'].includes(r.status??'') || typeof r.actual_points!=='number' || !Number.isFinite(r.actual_points)){unresolved.add(id);complete=false}else sum+=r.actual_points}totals.push(complete?sum:null)}
  const complete=unresolved.size===0;
  return {status:complete?'resolved' as const:'incomplete' as const,bestOfFive:complete?Math.max(...totals as number[]):null,allFive:complete?(totals as number[]).reduce((s,n)=>s+n,0):null,totals,unresolved:[...unresolved]};
}

type SavedPayload={version?:string;board?:{snapshot?:string;players?:{id:string;points:number;boom:number|null;name?:string}[]}};
type FrozenRow={event_id:string|number;player_key:string;history_status?:string;predictions:{market_line?:number|null};features?:{shots_growth?:number|null;ft_growth?:number|null}|null};

/** Older ceiling-watch builds share a table; do not grade them as the shot-growth policy. */
export function verifySavedShotGrowth(payload:SavedPayload|null|undefined,snapshotSha:string,source:FrozenRow[]):boolean {
  if(!payload || !['nba-boom-exposure-v1','nba-boom-exposure-v2-availability'].includes(payload.version??'') || payload.board?.snapshot!==snapshotSha || !Array.isArray(payload.board.players) || !payload.board.players.length)return false;
  const byId=new Map(source.map(c=>[`${c.event_id}:${c.player_key}`,c]));
  const seen=new Set<string>();
  return payload.board.players.every(p=>{
    const c=byId.get(p.id);if(!c || seen.has(p.id))return false;seen.add(p.id);
    const f=c.features,boom=c.history_status==='ready'&&typeof f?.shots_growth==='number'&&typeof f?.ft_growth==='number'?f.shots_growth+.44*f.ft_growth:null;
    return Number.isFinite(p.points)&&p.points===c.predictions.market_line && (boom===null?p.boom===null:typeof p.boom==='number'&&Math.abs(p.boom-boom)<1e-8);
  });
}
