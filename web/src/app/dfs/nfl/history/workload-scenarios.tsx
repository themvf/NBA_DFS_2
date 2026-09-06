"use client";
import { useState } from 'react';
import type { PlayerContext } from '@/lib/nfl-dfs/player-context';
import { workloadScenario } from '@/lib/nfl-dfs/workload-scenario';
import replay from '@/data/nfl-wr-workload-replay.json';

export default function WorkloadScenarios({data,playerId}:{data:PlayerContext;playerId:string}) {
  const teams=[...new Set(data.rows.filter(r=>r.playerId===playerId).map(r=>data.games[r.gameKey].team))];
  const [team,setTeam]=useState(teams.at(-1)??'');
  const members=[...new Map(Object.values(data.games).filter(g=>g.team===team).flatMap(g=>g.roster).filter(m=>m.id!==playerId&&['QB','WR','TE'].includes(m.position)).map(m=>[m.id,m])).values()].sort((a,b)=>a.name.localeCompare(b.name));
  const [chosen,setChosen]=useState('');
  const teammate=members.find(m=>m.id===chosen)??members[0];
  const estimates=teammate?(['present','absent'] as const).map(state=>({state,estimate:workloadScenario(data,playerId,teammate.id,team,19,state)})):[];
  return <section className="my-6 space-y-4 rounded-2xl border border-teal-200 bg-white p-5" aria-label="WR workload scenarios">
    <h2 className="text-xl font-bold">WR workload scenarios</h2>
    <p>Compare a receiver&apos;s targets and DK scoring distribution with a selected teammate present or absent. Small samples blend toward the receiver&apos;s normal distribution. These are research estimates for a hypothetical repeat of the 2025 role, not calibrated next-game projections.</p>
    <div className="flex flex-wrap gap-4"><label>Historical team <select className="rounded border p-2" value={team} onChange={e=>{setTeam(e.target.value);setChosen('');}}>{teams.map(t=><option key={t}>{t}</option>)}</select></label><label>Teammate <select className="rounded border p-2" value={teammate?.id??''} onChange={e=>setChosen(e.target.value)}>{members.map(m=><option key={m.id} value={m.id}>{m.name} ({m.position})</option>)}</select></label></div>
    <div className="grid gap-4 md:grid-cols-2">{estimates.map(({state,estimate:e})=><article className="space-y-2 rounded-xl bg-slate-50 p-4" key={state}><h3 className="font-bold">Teammate {state}</h3><p>{e.matching} matching games / {e.history} scored games</p>{e.available?<><p>Targets: {e.baselineTargets.toFixed(1)} baseline → <strong>{e.scenarioTargets.toFixed(1)}</strong></p><p>Mean: {e.baseline.mean.toFixed(1)} → <strong>{e.scenario.mean.toFixed(1)} DK</strong></p><div aria-label={`Scenario sample weight ${Math.round(e.weight*100)} percent`} className="h-2 overflow-hidden rounded bg-slate-200"><div className="h-full bg-teal-600" style={{width:`${e.weight*100}%`}} /></div><p className="text-xs">{Math.round(e.weight*100)}% scenario evidence; remainder baseline.</p><table className="w-full text-sm"><thead><tr><th>DK estimate</th><th>P10</th><th>P50</th><th>P90</th></tr></thead><tbody><tr><th>Baseline</th>{[e.baseline.p10,e.baseline.p50,e.baseline.p90].map((v,i)=><td className="text-center" key={i}>{v.toFixed(1)}</td>)}</tr><tr><th>Scenario</th>{[e.scenario.p10,e.scenario.p50,e.scenario.p90].map((v,i)=><td className="text-center" key={i}>{v.toFixed(1)}</td>)}</tr></tbody></table></>:<p>{e.reason}</p>}</article>)}</div>
    <p className="rounded border border-amber-300 bg-amber-50 p-3"><strong>Optimizer activation withheld.</strong> The chronological 2025 diagnostic had {replay.n} eligible cases: mean error {replay.metrics.baselineError?.toFixed(2)} → {replay.metrics.candidateError?.toFixed(2)} and 80% interval score {replay.metrics.baselineInterval?.toFixed(2)} → {replay.metrics.candidateInterval?.toFixed(2)} (lower is better). Both worsened.</p>
    <p className="text-xs text-slate-600">Absence requires complete scrimmage-personnel coverage and a roster entry with zero recorded plays. Unknown participation is excluded from splits. Participation is not an injury reason, route count, or designated starter. Replay uses only earlier weeks for estimates and teammate selection, but target-week participation is known after the game. It is an oracle diagnostic, not a deployable pregame backtest. Opponent filters above do not apply to these same-team scenarios.</p>
  </section>;
}
