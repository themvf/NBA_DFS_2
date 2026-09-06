import { contextPoints, type PlayerContext, type ContextRow } from './player-context';

export type Presence = 'present' | 'absent' | 'unknown';
export function teammatePresence(data: PlayerContext, row: ContextRow, teammateId: string): Presence {
  const game = data.games[row.gameKey];
  const member = game?.roster.find(m => m.id === teammateId);
  if (!member || member.recordedPlays === null) return 'unknown';
  if (member.recordedPlays > 0) return 'present';
  return game.plays > 0 && game.covered === game.plays ? 'absent' : 'unknown';
}

function weightedRange(values: { points: number; weight: number }[], salary?: number) {
  const sorted = [...values].sort((a,b) => a.points-b.points);
  const total = sorted.reduce((s,v) => s+v.weight,0);
  const q = (p: number) => { let cumulative=0; for (const v of sorted) { cumulative+=v.weight; if (cumulative >= p*total) return v.points; } return sorted.at(-1)!.points; };
  const mean = sorted.reduce((s,v)=>s+v.points*v.weight,0)/total;
  const hit = (target: number) => sorted.reduce((s,v)=>s+(v.points>=target?v.weight:0),0)/total;
  return {mean, p10:q(.1),p50:q(.5),p90:q(.9), salaryHits: salary === undefined ? null : [2,3,4].map(multiple=>({multiple,target:multiple*salary/1000,probability:hit(multiple*salary/1000)}))};
}

/** Condition empirical draws, not sums of quantiles. Unknown personnel never implies absence. */
export function workloadScenario(data: PlayerContext, playerId: string, teammateId: string, team: string, beforeWeek: number, state: Exclude<Presence,'unknown'>, salary?: number) {
  if (salary !== undefined && (!Number.isFinite(salary) || salary <= 0)) throw new Error('Invalid salary');
  if (playerId === teammateId) throw new Error('Choose a different teammate');
  const rows = data.rows.filter(r => r.playerId===playerId && data.games[r.gameKey]?.team===team && data.games[r.gameKey].week<beforeWeek && contextPoints(r)!==null && r.targets!==null && Number.isFinite(r.targets));
  const paired = rows.filter(r=>teammatePresence(data,r,teammateId)!=='unknown');
  const matches = paired.filter(r=>teammatePresence(data,r,teammateId)===state);
  const other = paired.length-matches.length;
  if (rows.length<6 || matches.length<3 || other<3) return {available:false as const, history:rows.length, matching:matches.length, other, reason:'Needs 6 scored prior games and at least 3 observed games in each teammate state.'};
  const weight=matches.length/(matches.length+8);
  const meanTargets=(rs:ContextRow[])=>rs.reduce((s,r)=>s+r.targets!,0)/rs.length;
  const baseline = weightedRange(rows.map(r=>({points:contextPoints(r)!,weight:1/rows.length})), salary);
  const scenario = weightedRange([...rows.map(r=>({points:contextPoints(r)!,weight:(1-weight)/rows.length})),...matches.map(r=>({points:contextPoints(r)!,weight:weight/matches.length}))], salary);
  return {available:true as const, history:rows.length,matching:matches.length,other,weight, baseline,scenario,baselineTargets:meanTargets(rows),scenarioTargets:(1-weight)*meanTargets(rows)+weight*meanTargets(matches),beforeWeek};
}

export function replayWorkload(data: PlayerContext) {
  const positions=new Map(data.players.map(p=>[p.id,p.position]));
  const results: {baselineError:number;candidateError:number;baselineInterval:number;candidateInterval:number;state:string}[]=[];
  const excluded:Record<string,number>={};
  const skip=(reason:string)=>{excluded[reason]=(excluded[reason]??0)+1;};
  const score=(actual:number,lo:number,hi:number)=>hi-lo+10*(Math.max(lo-actual,0)+Math.max(actual-hi,0));
  for (const target of data.rows) {
    if (positions.get(target.playerId)!=='WR' || contextPoints(target)===null) continue;
    const game=data.games[target.gameKey];
    // Select the leading other receiver using earlier team games only, before reading target personnel.
    const totals=new Map<string,number>();
    for (const r of data.rows) {
      const g=data.games[r.gameKey];
      if (g.team!==game.team || g.week>=game.week || r.playerId===target.playerId || r.targets===null) continue;
      if (!['WR','TE'].includes(positions.get(r.playerId)??'')) continue;
      totals.set(r.playerId,(totals.get(r.playerId)??0)+r.targets);
    }
    const teammate=[...totals].sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0]))[0]?.[0];
    if (!teammate) {skip('no_prior_teammate');continue;}
    const state=teammatePresence(data,target,teammate);
    if (state==='unknown') {skip('unknown_target_participation');continue;}
    const estimate=workloadScenario(data,target.playerId,teammate,game.team,game.week,state);
    if (!estimate.available) {skip('insufficient_split_history');continue;}
    const actual=contextPoints(target)!;
    results.push({state,baselineError:Math.abs(actual-estimate.baseline.mean),candidateError:Math.abs(actual-estimate.scenario.mean),baselineInterval:score(actual,estimate.baseline.p10,estimate.baseline.p90),candidateInterval:score(actual,estimate.scenario.p10,estimate.scenario.p90)});
  }
  const metrics=(rows:typeof results)=>Object.fromEntries(['baselineError','candidateError','baselineInterval','candidateInterval'].map(key=>[key,rows.length?rows.reduce((s,r)=>s+r[key as 'baselineError'],0)/rows.length:null]));
  return {version:'wr-workload-scenario-v1',season:data.season,n:results.length,metrics:metrics(results),absent:{n:results.filter(r=>r.state==='absent').length,...metrics(results.filter(r=>r.state==='absent'))},excluded,optimizerEnabled:false,limits:['Target-week participation is an oracle, not a pregame injury report.','2025 retrospective diagnostic, not untouched validation.','No verified routes; no causal injury effect or automatic redistribution.','Small samples shrink toward player baseline with 8 prior-equivalent games.']};
}
