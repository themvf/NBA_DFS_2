"use client";
import { useState } from 'react';
import report from '@/data/nfl-workload-ranges.json';

const percent=(n:number)=>`${(100*n).toFixed(1)}%`;
const quantile=(a:number[],p:number)=>{const index=(a.length-1)*p;const low=Math.floor(index);return a[low]+(a[Math.ceil(index)]-a[low])*(index-low);};

export default function SalaryUpside() {
  const [year,setYear]=useState<'2024'|'2025'>('2025');
  const [threshold,setThreshold]=useState<'10'|'15'|'20'|'25'>('20');
  const [salary,setSalary]=useState('5000');
  const [mean,setMean]=useState('12');
  const [targets,setTargets]=useState('6');
  const group=Number(targets)<4?'0':Number(targets)<7?'1':'2';
  const valid=[salary,mean,targets].every(v=>v.trim()!==''&&Number.isFinite(Number(v)))&&Number(salary)>0&&Number(mean)>=0&&Number(targets)>=0;
  const draws=valid?report.calculator_residuals[group].map(r=>Number(mean)+r).sort((a,b)=>a-b):[];
  const expected=draws.length?draws.reduce((a,b)=>a+b,0)/draws.length:0;
  const hit=(target:number)=>draws.filter(d=>d>=target).length/draws.length;
  const season=report.seasons[year];
  return <section className="space-y-4 rounded-2xl border border-slate-200 bg-white p-5">
    <h2 className="text-xl font-bold">Salary upside / workload range experiment</h2>
    <p className="rounded-lg bg-amber-50 p-3 text-sm text-amber-950">Still experimental: workload-specific ranges correct much of the tail imbalance, but do not beat the historical model on average error and interval score. Optimizer projections remain unchanged.</p>
    <div className="grid gap-4 md:grid-cols-2">{Object.entries(report.seasons).map(([y,s])=><article key={y} className="rounded-xl bg-slate-50 p-4"><h3 className="font-bold">{y} · {s.models.workload.n.toLocaleString()} matched games</h3><p className="my-2 text-sm">Outcomes below P10 / above P90 (target: about 10% each)</p>{(['candidate','workload'] as const).map(m=><div key={m} className="my-2 text-sm"><div className="flex justify-between"><span>{m==='candidate'?'Earlier pooled ranges':'Workload ranges'}</span><span>{percent(s.models[m].below_p10)} / {percent(s.models[m].above_p90)}</span></div><div className="mt-1 flex h-3 overflow-hidden rounded bg-slate-200"><div className="bg-amber-500" style={{width:percent(s.models[m].below_p10)}}/><div className="bg-teal-600" style={{width:percent(1-s.models[m].below_p10-s.models[m].above_p90)}}/><div className="bg-indigo-500" style={{width:percent(s.models[m].above_p90)}}/></div></div>)}<p className="text-xs text-slate-600">Amber: below floor · teal: inside range · indigo: above ceiling</p><p className="mt-3 text-sm">MAE historical → workload: {s.models.production.mae.toFixed(2)} → {s.models.workload.mae.toFixed(2)}<br/>Interval score: {s.models.production.interval80.toFixed(2)} → {s.models.workload.interval80.toFixed(2)} (lower is better)</p></article>)}</div>
    <div className="flex flex-wrap gap-4"><label>Season <select value={year} onChange={e=>setYear(e.target.value as typeof year)} className="rounded border p-2"><option>2024</option><option>2025</option></select></label><label>Score target <select value={threshold} onChange={e=>setThreshold(e.target.value as typeof threshold)} className="rounded border p-2">{['10','15','20','25'].map(t=><option key={t}>{t}</option>)}</select></label></div>
    <h3 className="font-semibold">Do the predicted chances match actual results?</h3>
    <p className="text-sm">Each row groups similar predicted probabilities. Small groups provide weak evidence. Fixed point targets are not historical salary multiples.</p>
    <div className="space-y-3">{season.targets[threshold].bins.map(b=><div key={b.lower} className="grid items-center gap-2 text-sm sm:grid-cols-[180px_1fr]"><span>{percent(b.lower)}–{percent(b.upper)} · n={b.n}{b.n<30?' · small sample':''}</span><div><div className="mb-1 flex justify-between"><span>Predicted {percent(b.predicted)}</span><span>Actual {percent(b.observed)}</span></div><div className="h-2 bg-slate-100"><div className="h-2 bg-slate-500" style={{width:percent(b.predicted)}}/></div><div className="mt-1 h-2 bg-slate-100"><div className="h-2 bg-teal-600" style={{width:percent(b.observed)}}/></div></div></div>)}</div>
    <p className="text-xs">25-point probability error (Brier; lower is better): historical {season.production_25_brier.toFixed(4)} → workload {season.targets['25'].brier.toFixed(4)}. Similar aggregate error can hide overconfidence in the highest-probability groups.</p>
    <details className="rounded-xl border p-4"><summary className="cursor-pointer font-semibold">Explore a hypothetical salary target</summary><p className="my-3 text-sm">Manual assumptions, not a current player recommendation. Residuals come from completed 2023–2025 games and cannot be used to backtest those games. No injury or defensive adjustment is included. Classic/FLEX scoring only.</p>
      <div className="flex flex-wrap gap-4">{[['Salary ($)',salary,setSalary],['Base volume projection',mean,setMean],['Prior weighted targets',targets,setTargets]].map(([label,value,setter])=><label key={String(label)} className="text-sm">{String(label)}<input type="number" min="0" className="mt-1 block w-40 rounded border p-2" value={String(value)} onChange={e=>(setter as (v:string)=>void)(e.target.value)}/></label>)}</div>
      {!valid?<p className="mt-3 text-red-700">Enter a positive salary and nonnegative projection and targets.</p>:<><div className="my-4 grid gap-3 sm:grid-cols-3">{[2,3,4].map(x=><div key={x} className="rounded-lg bg-teal-50 p-3"><strong className="text-xl">{percent(hit(x*Number(salary)/1000))}</strong><p>{x}× salary / {(x*Number(salary)/1000).toFixed(1)} DK points</p></div>)}</div><p className="text-sm">Adjusted mean {expected.toFixed(1)} · P10 / P50 / P90: {quantile(draws,.1).toFixed(1)} / {quantile(draws,.5).toFixed(1)} / {quantile(draws,.9).toFixed(1)} · chance strictly above adjusted mean: {percent(draws.filter(d=>d>expected).length/draws.length)}</p><p className="mt-2 text-xs">{draws.length.toLocaleString()} prior workload-group errors. These residuals can change both the mean and the range. Negative draws are retained; this is not a full stat simulator.</p></>}
    </details>
    <p className="text-sm text-amber-900">Historical salary validation unavailable: {report.salary_audit.verified_replay_salary_rows} verified salary mappings for 2024–2025. Audited NFL uploads contain {report.salary_audit.uploaded_salary_rows.rows.toLocaleString()} rows from 2026; those salaries are not substituted into historical games.</p>
    <details><summary className="cursor-pointer text-sm">Evidence limits</summary><ul className="list-disc pl-5 text-sm">{report.limits.map(l=><li key={l}>{l}</li>)}</ul><p className="break-all text-xs">Paired predictions: {report.paired_sha256}</p></details>
  </section>;
}
