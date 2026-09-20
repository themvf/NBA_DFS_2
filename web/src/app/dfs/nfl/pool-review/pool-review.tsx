'use client';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useMemo,useState,useTransition } from 'react';
import type { poolAuditIndex,readPoolReview } from '@/db/nfl-dfs-pool-audit';
import { summarizePool, type PoolReviewRow } from '@/lib/nfl-dfs/pool-audit';
import { captureCurrentPool } from './actions';
type Index=Awaited<ReturnType<typeof poolAuditIndex>>;
type Review=Awaited<ReturnType<typeof readPoolReview>>;
const num=(n:number|null)=>n===null?'—':n.toFixed(2);
const time=(v:string)=>new Date(v).toLocaleString('en-US',{timeZone:'America/New_York',month:'short',day:'numeric',hour:'numeric',minute:'2-digit',second:'2-digit'})+' ET';
function download(name:string,value:string,type:string) {
  const url=URL.createObjectURL(new Blob([value],{type}));const a=document.createElement('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
export default function PoolReview({index,selected,review,latest}:{index:Index;selected:Index['uploads'][number]|null;review:Review|null;latest:boolean}) {
  const router=useRouter();const [search,setSearch]=useState('');const [position,setPosition]=useState('');const [status,setStatus]=useState('');
  const [pending,start]=useTransition();const [message,setMessage]=useState('');
  const rows=review?.rows??[];
  const filtered=useMemo(()=>rows.filter(r=>(!position||r.player.position===position)&&(!status||r.status===status)
    &&`${r.player.name} ${r.player.team}`.toLowerCase().includes(search.toLowerCase())),[rows,position,status,search]);
  const summary=summarizePool(filtered);
  const missing=selected?selected.players-rows.length:0;
  const errors=index.health?.summary.errors??[];
  const stale=!index.health?.finishedAt || (review&&Date.parse(review.evaluatedAt)-Date.parse(index.health.finishedAt)>5*60000);
  const csv=()=>{
    const headers=['dk_player_id','player_id','name','team','position','salary','out','projection','actual','actual_minus_projection','status','game_id','kickoff','observed_at','archived_at','capture_digest','projection_scenario','result_id','result_digest','result_scoring_version'];
    const values=rows.map(r=>[r.player.dkPlayerId,r.player.playerId,r.player.name,r.player.team,r.player.position,r.player.salary,r.player.isOut,r.player.projection,r.actual,r.error,r.status,r.gameId,r.kickoff,r.observedAt,r.capturedAt,r.captureDigest,r.player.scenario,r.result?.id,r.result?.digest,r.result?.scoringVersion]);
    const escape=(v:unknown)=>{const s=String(v??'');return '"'+(/^[=+@\-]/.test(s)&&typeof v==='string'?"'":'')+s.replaceAll('"','""')+'"';};
    download(`nfl-pool-${selected?.id}.csv`,[headers,...values].map(r=>r.map(escape).join(',')).join('\r\n'),'text/csv');
  };
  return <main className="mx-auto max-w-screen-2xl space-y-5 p-4 md:p-8">
    <nav className="flex gap-5 text-sm text-blue-700"><Link href="/dfs/nfl">← NFL DFS workspace</Link><Link href="/dfs/nfl/review">Weekly model review</Link></nav>
    <div><h1 className="text-3xl font-bold">Full Pool Audit</h1><p className="mt-2 text-slate-600">Every salary-pool player, including OUT and unmatched players. Compare the last preserved pregame projection for each game with recorded DraftKings results.</p></div>
    <div className="flex flex-wrap items-end gap-3"><label className="grid gap-1 text-sm">Saved pool revision
      <select className="max-w-full rounded border p-2" value={selected?.id??''} onChange={e=>router.push(`/dfs/nfl/pool-review?upload=${e.target.value}`)}>
        {!index.uploads.length&&<option>No saved pools</option>}{index.uploads.map(u=><option key={u.id} value={u.id}>{u.season} W{u.week} · {u.fileName} · {u.players} players · {u.pregameGames} pregame games · {u.id.slice(0,8)}</option>)}
      </select></label>
      <label className="grid gap-1 text-sm">Observation selection<select className="rounded border p-2" value={latest?'latest':'pregame'} onChange={e=>router.push(`/dfs/nfl/pool-review?upload=${selected?.id}&view=${e.target.value}`)}><option value="pregame">Last pregame (audit baseline)</option><option value="latest">Latest saved (may be late)</option></select></label>
      <button className="rounded border px-3 py-2 text-sm" onClick={()=>router.refresh()}>Refresh results</button>
      <button disabled={!selected||pending} className="rounded border px-3 py-2 text-sm disabled:opacity-50" onClick={()=>start(async()=>{try{const r=await captureCurrentPool(selected!.id);setMessage(`Saved ${r.saved} game snapshots (${r.players} players). Captures after kickoff are marked late.`);router.refresh();}catch(e){setMessage(e instanceof Error?e.message:'Capture failed');}})}>{pending?'Saving…':'Save current full pool'}</button>
      <button disabled={!rows.length} className="rounded border px-3 py-2 text-sm disabled:opacity-50" onClick={csv}>Download full CSV</button>
      <button disabled={!rows.length} className="rounded border px-3 py-2 text-sm disabled:opacity-50" onClick={()=>download(`nfl-pool-evidence-${selected?.id}.json`,JSON.stringify({selection:latest?'latest observation per game; late rows excluded from metrics':'latest pregame per game, earliest late if unavailable',upload:selected,...review},null,2),'application/json')}>Download audit JSON</button>
    </div>
    {message&&<p role="status">{message}</p>}
    <div className="rounded-xl border bg-slate-50 p-4 text-sm space-y-2">
      <p><strong>{rows.length} / {selected?.players??0} players preserved in this revision.</strong> {missing>0?`${missing} salary rows have no captured game evidence yet.`:'All salary rows accounted for.'} Captures never replace earlier evidence.</p>
      <p>Automatic capture checks every minute, takes an initial snapshot within 24 hours, and captures each minute during the final 20 minutes before kickoff. Timing is per game; the latest successful capture strictly before kickoff is used. A scheduled check can fail or be delayed—coverage below shows what was actually saved.</p>
      <p className={stale||errors.length?'text-amber-800':'text-slate-600'}>Capture job: {index.health?`${index.health.finishedAt?'finished':'started'} ${time(index.health.finishedAt??index.health.startedAt)}`:'no run recorded'}{stale?' · heartbeat missing or more than 5 minutes old':''}{errors.length?` · ${errors.length} errors`:''}.</p>
      {errors.map((e,i)=><p role="alert" key={i}>{e.uploadId}: {e.error}</p>)}
      <p>Only completed games with exact recorded results and pregame projections enter accuracy metrics. Missing results are never zero. Results may be revised; exports retain the exact result IDs and scoring digests used. Legacy mixed simulation ranges and workload estimates are excluded from interval coverage.</p>
      <p>Exports include the entire selected pool regardless of table filters. JSON includes full frozen inputs, model/source provenance, game times, capture digests and scoring evidence. Results evaluated {review?time(review.evaluatedAt):'—'}.</p>
    </div>
    <details className="rounded-xl border p-4" open><summary className="cursor-pointer font-semibold">Game capture coverage</summary>
      <div className="mt-3 overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr>{['Game / kickoff','Players','Observation time','Evidence','Model run'].map(h=><th className="p-2" key={h}>{h}</th>)}</tr></thead>
      <tbody>{review?.captures.map(c=>{const pre=Date.parse(c.observedAt)<Date.parse(c.payload.game.kickoff);return <tr key={c.digest} className="border-t"><td className="p-2">{c.payload.game.away}@{c.payload.game.home}<br/>{time(c.payload.game.kickoff)}</td><td className="p-2">{c.payload.players.length}</td><td className="p-2">{time(c.observedAt)}<br/><span className={pre?'text-emerald-700':'text-amber-800'}>{pre?`${Math.ceil((Date.parse(c.payload.game.kickoff)-Date.parse(c.observedAt))/60000)} min before kickoff`:'Late — excluded from accuracy'}</span></td><td className="p-2">{c.payload.origin==='saved_optimizer'?'Recovered saved optimizer pool':'Captured workspace pool'}<br/><span className="text-xs text-slate-500">Archived {time(c.capturedAt)}</span></td><td className="p-2 font-mono text-xs">{c.payload.projectionRunId??'Unknown'}<br/>{c.payload.codeRevision}</td></tr>;})}</tbody></table></div>
    </details>
    <div className="flex flex-wrap gap-3"><input aria-label="Search players" placeholder="Search player or team" value={search} onChange={e=>setSearch(e.target.value)} className="rounded border p-2"/>
      <select aria-label="Position" className="rounded border p-2" value={position} onChange={e=>setPosition(e.target.value)}><option value="">All positions</option>{[...new Set(rows.map(r=>r.player.position))].sort().map(p=><option key={p}>{p}</option>)}</select>
      <select aria-label="Result status" className="rounded border p-2" value={status} onChange={e=>setStatus(e.target.value)}><option value="">All statuses</option>{[...new Set(rows.map(r=>r.status))].sort().map(s=><option key={s}>{s}</option>)}</select>
    </div>
    <div className="flex flex-wrap gap-6 rounded-xl border p-4 text-sm"><span>Filtered players <strong>{summary.players}</strong></span><span>Scored <strong>{summary.scored}</strong></span><span>MAE <strong>{num(summary.mae)}</strong></span><span>Bias (actual − projected) <strong>{num(summary.bias)}</strong></span><span>Interval coverage <strong>{summary.coverage===null?'—':(100*summary.coverage).toFixed(1)+'%'} ({summary.intervals} eligible)</strong></span></div>
    <div className="overflow-x-auto rounded-xl border"><table className="w-full text-left text-sm"><thead className="bg-slate-100"><tr>{['Player','Team / Pos','Salary','Projected','Actual DK','Actual − proj','Status','Evidence'].map(h=><th className="p-3" key={h}>{h}</th>)}</tr></thead><tbody>{filtered.map(r=><tr key={`${r.gameId}:${r.player.dkPlayerId}`} className="border-t align-top"><td className="p-3 font-medium">{r.player.name}{r.player.isOut&&<span className="ml-2 text-xs text-red-700">OUT</span>}</td><td className="p-3">{r.player.team} · {r.player.position}</td><td className="p-3">${r.player.salary.toLocaleString('en-US')}</td><td className="p-3">{num(r.player.projection)}</td><td className="p-3">{num(r.actual)}</td><td className="p-3">{num(r.error)}</td><td className="p-3">{r.status.replaceAll('_',' ')}</td><td className="p-3"><Evidence row={r}/></td></tr>)}</tbody></table>{!filtered.length&&<p className="p-6 text-slate-500">No captured players match this view.</p>}</div>
  </main>;
}

function Evidence({row}:{row:PoolReviewRow}) {
  const [open,setOpen]=useState(false);
  return <details onToggle={e=>setOpen(e.currentTarget.open)}><summary className="cursor-pointer text-blue-700">Frozen inputs</summary>{open&&<><p className="my-2">{row.player.scenario} | {time(row.observedAt)} | {row.resultRevisions} result revisions</p><pre className="max-h-80 max-w-lg overflow-auto whitespace-pre-wrap text-xs">{JSON.stringify(row,null,2)}</pre></>}</details>;
}
