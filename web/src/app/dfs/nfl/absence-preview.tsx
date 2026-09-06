"use client";
import { useState, useTransition } from 'react';
import { previewNflAbsence, type NflWorkspaceSlate } from './actions';

export default function AbsencePreview({slate}:{slate:NflWorkspaceSlate}) {
  const [receiverId,setReceiver]=useState(''),[teammateId,setTeammate]=useState('');
  const [result,setResult]=useState<Extract<Awaited<ReturnType<typeof previewNflAbsence>>,{ok:true}>['result']|null>(null);
  const [error,setError]=useState(''),[pending,startTransition]=useTransition();
  const receivers=slate.players.filter(p=>p.position==='WR'&&!p.isOut);
  const receiver=receivers.find(p=>String(p.dkPlayerId)===receiverId);
  const teammates=slate.players.filter(p=>p.dkPlayerId!==receiver?.dkPlayerId&&p.team===receiver?.team&&p.gameKey===receiver?.gameKey&&['QB','WR','TE'].includes(p.position));
  const e=result?.estimate;
  function download() {
    if (!result) return;
    const url=URL.createObjectURL(new Blob([JSON.stringify(result,null,2)],{type:'application/json'}));
    const a=document.createElement('a');a.href=url;a.download=`nfl-absence-${result.digest.slice(0,12)}.json`;a.click();URL.revokeObjectURL(url);
  }
  return <section className="space-y-3 rounded-xl border border-teal-200 bg-white p-5" aria-label="Verified absence scenarios">
    <h2 className="font-bold">Verified absence â†’ WR workload and DFS range</h2>
    <p className="text-sm">Select a receiver and an absent teammate to inspect prior same-team targets, floor and ceiling, and 2Ã—/3Ã—/4Ã— salary outcomes. The server rechecks official inactive evidence before calculating. This first increment covers players in the salary slate.</p>
    <p className="rounded bg-amber-50 p-3 text-sm">Research preview: the earlier historical split model failed its accuracy checks. These comparisons do not change optimizer projections. No verified inactive report means no preview; an injury label alone does not prove extra targets.</p>
    <div className="flex flex-wrap gap-3"><label className="text-sm">Receiver<select disabled={pending} className="block max-w-64 rounded border p-2" value={receiverId} onChange={ev=>{setReceiver(ev.target.value);setTeammate('');setResult(null);setError('');}}><option value="">Select WR</option>{receivers.map(p=><option key={p.dkPlayerId} value={p.dkPlayerId}>{p.name} Â· {p.team}</option>)}</select></label>
      <label className="text-sm">Absent teammate<select disabled={pending} className="block max-w-72 rounded border p-2" value={teammateId} onChange={ev=>{setTeammate(ev.target.value);setResult(null);setError('');}}><option value="">Select teammate</option>{teammates.map(p=><option key={p.dkPlayerId} value={p.dkPlayerId}>{p.name} Â· {p.availability?.officialConfirmed&&p.availability.status==='INACTIVE'?'Verified inactive':'Unverified absence'}</option>)}</select></label>
      <button disabled={pending||!receiverId||!teammateId} className="self-end rounded bg-teal-800 px-4 py-2 text-white disabled:opacity-40" onClick={()=>{setResult(null);setError('');startTransition(async()=>{try{const response=await previewNflAbsence(slate.uploadId,Number(receiverId),Number(teammateId));if(response.ok)setResult(response.result);else setError(response.error);}catch(err){setError(err instanceof Error?err.message:'Preview unavailable.');}});}}>{pending?'Checking evidenceâ€¦':'Check and preview'}</button></div>
    {error&&<p role="status" className="text-sm text-amber-900">{error}</p>}
    {result&&e&&<div className="space-y-3"><p className="text-sm">{result.receiver.name} with {result.teammate.name} absent Â· {result.historicalSeason} history Â· {e.matching} absent / {e.other} present / {e.history} scored games. Evaluated {result.evaluatedAt}.</p>
      {e.available?<><p>Targets: {e.baselineTargets.toFixed(1)} â†’ <strong>{e.scenarioTargets.toFixed(1)}</strong>. Matching-state weight: {Math.round(e.weight*100)}%; remainder historical baseline.</p>
        <div className="grid gap-4 md:grid-cols-2">{(['baseline','scenario'] as const).map(key=><article className="rounded bg-slate-50 p-3" key={key}><h3 className="font-semibold">{key==='baseline'?'Historical baseline':'Absence-conditioned history'}</h3><p className="my-2 text-sm">Mean {e[key].mean.toFixed(1)} Â· P10 {e[key].p10.toFixed(1)} Â· P50 {e[key].p50.toFixed(1)} Â· P90 {e[key].p90.toFixed(1)}</p>{e[key].salaryHits?.map(h=><div className="my-2" key={h.multiple}><p className="text-xs">{h.multiple}Ã— salary (â‰¥{h.target.toFixed(1)} DK): {(h.probability*100).toFixed(1)}%</p><div role="img" aria-label={`${h.multiple} times salary: ${(h.probability*100).toFixed(1)} percent`} className="mt-1 h-2 rounded bg-slate-200"><div className="h-full rounded bg-teal-600" style={{width:`${h.probability*100}%`}}/></div></div>)}</article>)}</div></>:<p>{e.reason}</p>}
      <ul className="list-disc space-y-1 pl-5 text-xs text-slate-600">{result.limits.map(s=><li key={s}>{s}</li>)}</ul><button className="text-sm text-teal-800 underline" onClick={download}>Download evidence and scenario JSON</button></div>}
  </section>;
}
