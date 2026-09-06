import type { NflWorkspacePlayer } from '@/app/dfs/nfl/actions';

export type Competitor = 'fantasypros' | 'linestar';
export type ImportEvidence = { importedAt: string; digest: string; fileName: string };
export type BenchmarkRow = { player: NflWorkspacePlayer; kickoff: string; competitor: number; evidence: ImportEvidence };
export type BenchmarkSnapshot = {
  version: 'nfl-competitor-benchmark-v1'; capturedAt: string; source: Competitor; uploadId: string;
  modelAsOf: string; projectionRunId: string; optimizerVersion: string;
  rows: BenchmarkRow[]; exclusions: {name:string;reason:string}[];
  settings: unknown; lineups: {source:'our'|'competitor';slots:{id:number;multiplier:number}[]}[];
  lineupWarnings: string[]; salaryDigest: string; sourcePublicationTime: 'unknown';
};
export type BenchmarkOutcome = {playerId:number;team:string;position:string;kickoff:string;actual:number;resultId:string;digest:string};
const finite = (x: unknown): x is number => typeof x==='number'&&Number.isFinite(x);
export const benchmarkTeam = (s:string) => ({WAS:'WSH',LA:'LAR',JAC:'JAX',AZ:'ARI'}[s]??s);

export function salaryMatchesKickoff(info:string|null,kickoff:string) {
  const date=info?.match(/\b(\d{2}\/\d{2}\/\d{4})\b/)?.[1];
  const time=info?.match(/\b(\d{1,2}:\d{2})\s*(AM|PM)\s+ET\b/i);
  const ms=Date.parse(kickoff); if(!Number.isFinite(ms)||!date||!time)return false;
  return date===new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',month:'2-digit',day:'2-digit',year:'numeric'}).format(ms)
    && `${time[1].padStart(5,'0')}${time[2].toUpperCase()}`===new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',hour12:true}).format(ms).replace(/\s/g,'');
}

export function benchmarkPool(players:NflWorkspacePlayer[], evidence:Record<string,Partial<Record<Competitor,ImportEvidence>>>, source:Competitor, now:number, modelAsOf:string) {
  if(!['fantasypros','linestar'].includes(source))throw new Error('Choose FantasyPros or LineStar.');
  const modelTime=Date.parse(modelAsOf);
  if(!Number.isFinite(modelTime)||modelTime>now||now-modelTime>72*3600000)throw new Error('Refresh our model: the benchmark requires a pregame model snapshot under 72 hours old.');
  const rows:BenchmarkRow[]=[],exclusions:{name:string;reason:string}[]=[];
  for(const p of [...players].sort((a,b)=>a.dkPlayerId-b.dkPlayerId)) {
    const value=source==='fantasypros'?p.fantasyprosProj:p.linestarProj;
    const e=evidence[String(p.dkPlayerId)]?.[source];const imported=Date.parse(e?.importedAt??'');
    const kickoff=p.availability?.kickoff??'';
    const reason=p.isOut?'Excluded at freeze':p.ffPlayerId===null?'Missing canonical identity':p.position!=='DST'&&!p.availability?.fresh?'Current roster unresolved':!salaryMatchesKickoff(p.gameInfo,kickoff)?'Salary/schedule kickoff mismatch':Date.parse(kickoff)<=now?'Game started':!finite(p.ourProj)||p.ourProj<=0?'Our projection unavailable':!finite(value)||value<=0?'Competitor projection unavailable':!e?.digest||!Number.isFinite(imported)||imported>now||now-imported>24*3600000?'Competitor import missing or older than 24 hours':!Number.isInteger(p.salary)||p.salary<=0?'Invalid salary':null;
    if(reason)exclusions.push({name:p.name,reason});else rows.push({player:p,kickoff,competitor:value!,evidence:e!});
  }
  if(new Set(rows.map(r=>r.player.ffPlayerId)).size!==rows.length)throw new Error('Duplicate canonical players in benchmark pool.');
  return {rows,exclusions};
}

/** Missing scores never become zero; the two sources always use exactly paired outcomes. */
export function gradeBenchmark(snapshot:BenchmarkSnapshot,outcomes:BenchmarkOutcome[]) {
  const resolved=snapshot.rows.map(r=>{
    const matches=outcomes.filter(o=>o.playerId===r.player.ffPlayerId&&benchmarkTeam(o.team)===benchmarkTeam(r.player.team)&&o.position===r.player.position&&Date.parse(o.kickoff)===Date.parse(r.kickoff)&&finite(o.actual));
    return {row:r,outcome:matches.length===1?matches[0]:null};
  });
  const scored=resolved.filter(x=>x.outcome!==null);
  const mean=(v:number[])=>v.length?v.reduce((s,x)=>s+x,0)/v.length:null;
  const sources=(['our','competitor'] as const).map(source=>{
    const prediction=(r:BenchmarkRow)=>source==='our'?r.player.ourProj!:r.competitor;
    // Select BEFORE outcome availability. Do not backfill missing outcomes with lower-ranked plays.
    const selected=[...snapshot.rows].sort((a,b)=>prediction(b)/(b.player.salary/1000)-prediction(a)/(a.player.salary/1000)||a.player.dkPlayerId-b.player.dkPlayerId).slice(0,10);
    const selectedScores=resolved.filter(x=>selected.includes(x.row)&&x.outcome);
    const lines=snapshot.lineups.filter(l=>l.source===source).map(l=>{
      const scores=l.slots.map(s=>{const o=resolved.find(r=>r.row.player.dkPlayerId===s.id)?.outcome;return o?o.actual*s.multiplier:null;});
      return scores.every(s=>s!==null)?(scores as number[]).reduce((a,b)=>a+b,0):null;
    });
    return {source,n:scored.length,mae:mean(scored.map(x=>Math.abs(prediction(x.row)-x.outcome!.actual))),bias:mean(scored.map(x=>prediction(x.row)-x.outcome!.actual)),
      selected:selected.length,selectedScored:selectedScores.length,salaryHits:[3,4].map(m=>({multiple:m,hits:selectedScores.filter(x=>x.outcome!.actual>=m*x.row.player.salary/1000).length,n:selectedScores.length})),
      lineups:lines.length,scoredLineups:lines.filter(finite).length,meanLineup:mean(lines.filter(finite)),bestLineup:lines.some(finite)?Math.max(...lines.filter(finite)):null};
  });
  const complete = sources.every(s=>s.lineups>0&&s.scoredLineups===s.lineups)&&sources[0].lineups===sources[1].lineups;
  if(!complete)for(const source of sources){source.meanLineup=null;source.bestLineup=null;}
  return {paired:scored.length,pending:resolved.length-scored.length,sources,resultEvidence:scored.map(x=>({playerId:x.outcome!.playerId,resultId:x.outcome!.resultId,digest:x.outcome!.digest}))};
}
