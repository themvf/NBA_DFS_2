"use client";
import {useState} from 'react';
import {allocateRoles,type RoleMember,type RoleSettings} from '@/lib/nfl-dfs/role-allocation';

export default function RoleAllocation({members,team,targets,carries,now,rosterDigest,scenario}:{members:RoleMember[];team:string;targets:number;carries:number;now:number;rosterDigest:string;scenario:string}){
  const [inputs,setInputs]=useState<Record<string,{targets?:string;carries?:string}>>({});
  const overrides:RoleSettings={};
  for(const [id,values] of Object.entries(inputs)) for(const type of ['targets','carries'] as const){
    const value=values[type];
    if(value!==undefined&&value.trim()!=='') overrides[id]={...overrides[id],[type]:Number(value)/100};
  }
  let result:ReturnType<typeof allocateRoles>|null=null,error='';
  try{result=allocateRoles(members,overrides,targets,carries,now);}catch(e){error=e instanceof Error?e.message:'Invalid allocation';}
  const save=()=>{
    if(!result)return;
    const payload={version:'nfl-role-scenario-v1',team,scenario,as_of:new Date(now).toISOString(),roster_digest:rosterDigest,target_budget:targets,carry_budget:carries,overrides,members,result,optimizer_enabled:false};
    const url=URL.createObjectURL(new Blob([JSON.stringify(payload,null,2)],{type:'application/json'}));
    const link=document.createElement('a');link.href=url;link.download=`${team}-role-scenario.json`;link.click();URL.revokeObjectURL(url);
  };
  return <section className="rounded-2xl border border-slate-200 bg-white p-5">
    <div className="flex flex-wrap items-center justify-between gap-3"><h2 className="text-xl font-bold">Allocate the whole team’s workload</h2><div className="flex gap-3"><button className="rounded border px-3 py-2 text-sm" onClick={()=>setInputs({})}>Reset assumptions</button><button disabled={!result} className="rounded bg-teal-800 px-3 py-2 text-sm text-white disabled:opacity-40" onClick={save}>Save scenario</button></div></div>
    <p className="my-3 text-sm">Returning players start from their recent team usage reference. Rookies, arrivals, unavailable and unresolved players receive no automatic share. Unassigned means unknown opportunity, not a zero projection. These allocations are research assumptions, not optimizer forecasts.</p>
    <p className="mb-3 text-sm">Uses the {scenario} historical play mix and total plays entered above. Edit shares for current eligible players; both columns must stay at or below 100%. An injury exclusion leaves opportunity unassigned—it does not automatically promote another player.</p>
    {error?<p role="alert" className="my-3 rounded bg-red-50 p-3 text-red-800">{error}</p>:result&&<><div className="grid gap-3 sm:grid-cols-2">{[['Targets',result.targetShare,targets,result.unassignedTargets],['Designed carries',result.carryShare,carries,result.unassignedCarries]].map(([label,share,budget,reserve])=><div key={String(label)} className="rounded-xl bg-slate-50 p-4"><div className="flex justify-between"><strong>{label}</strong><span>{Number(budget).toFixed(1)} team opportunities</span></div><div className="my-2 h-3 overflow-hidden rounded bg-amber-200"><div className="h-3 bg-teal-600" style={{width:`${Number(share)*100}%`}}/></div><p className="text-sm">{(Number(share)*100).toFixed(1)}% allocated · {Number(reserve).toFixed(1)} unassigned</p></div>)}</div></>}
    <div className="mt-4 max-h-96 overflow-auto"><table className="w-full min-w-[700px] text-left text-sm"><thead><tr>{['Player / evidence','Target share override %','Carry share override %','Allocated targets / carries'].map(h=><th key={h} className="p-2">{h}</th>)}</tr></thead><tbody>{members.map(p=>{
      const row=result?.rows.find(r=>r.id===p.id);
      const captured=Date.parse(p.captured_at??'');
      const eligible=p.evidence_current&&!p.out&&Number.isFinite(captured)&&captured<=now&&now-captured<=72*3600000;
      return <tr key={p.id} className="border-t"><td className="p-2 font-semibold">{p.name} · {p.position}<span className="block text-xs font-normal text-slate-500">{eligible?(p.rookie?'Rookie: role required':p.new_team?'Arrival: role required':'Current roster'):p.out?'Unavailable':'Roster unresolved / stale'}</span></td>{(['targets','carries'] as const).map(type=><td key={type}><input aria-label={`${p.name} ${type} share`} className="w-28 rounded border p-2 disabled:bg-slate-100" type="number" min="0" max="100" step="0.1" disabled={!eligible} value={inputs[p.id]?.[type]??''} placeholder={row?((type==='targets'?row.targetShare:row.carryShare)*100).toFixed(1):'Share'} onChange={e=>setInputs({...inputs,[p.id]:{...inputs[p.id],[type]:e.target.value}})}/><span className="block text-xs text-slate-500">{row?(type==='targets'?row.targetSource:row.carrySource):'Invalid scenario'}</span></td>)}<td>{row?`${row.targets.toFixed(1)} / ${row.carries.toFixed(1)}`:'—'}</td></tr>;
    })}</tbody></table></div>
  </section>;
}
