"use client";

import Link from "next/link";
import { useState } from "react";
import { captureAgeHours, coverage, type FeatureAudit, type AuditCell } from "@/lib/nfl-dfs/feature-audit";
import type { EfficiencyReport } from "@/lib/nfl-dfs/efficiency";
import type { WorkloadReport } from "@/lib/nfl-dfs/workload";
import ResearchStages from "./research-stages";

const names: Record<string, string> = { frozen_history: "Frozen research history", working_source: "Working source rows" };
const integer = (n: number) => n.toLocaleString("en-US");
const panel = "rounded-2xl border border-slate-700 bg-slate-900/70 p-5";

export default function ModelLab({ report, digest, viewedAt, workload, workloadDigest, workloadFailed, efficiency, efficiencyDigest, efficiencyFailed }: {
  report: FeatureAudit; digest: string; viewedAt: number; workload: WorkloadReport | null; workloadDigest: string | null;
  workloadFailed: boolean; efficiency: EfficiencyReport | null; efficiencyDigest: string | null; efficiencyFailed: boolean;
}) {
  const [dataset, setDataset] = useState(report.datasets[0]?.dataset);
  const [position, setPosition] = useState("QB");
  const [group, setGroup] = useState("All");
  const [selected, setSelected] = useState<AuditCell | null>(null);
  const [view, setView] = useState<"Coverage" | "Workload" | "Efficiency">(efficiency ? "Efficiency" : workload ? "Workload" : "Coverage");
  const data = report.datasets.find(d => d.dataset === dataset);
  const age = captureAgeHours(report.evaluated_at, viewedAt);
  const captureAge = captureAgeHours(data?.latest_capture ?? null, viewedAt);
  const fields = report.fields.filter(f => f.positions.includes(position) && (group === "All" || f.group === group));
  function download() {
    const evidence = { audit_digest: digest, input_coverage: report, workload_run_digest: workloadDigest, workload, efficiency_run_digest: efficiencyDigest, efficiency };
    const url = URL.createObjectURL(new Blob([JSON.stringify(evidence, null, 2)], { type: "application/json" }));
    const a = document.createElement("a"); a.href = url; a.download = `nfl-model-lab-evidence-${digest.slice(0, 12)}.json`; a.click(); URL.revokeObjectURL(url);
  }
  return <main className="min-h-screen bg-slate-950 px-4 py-7 text-slate-100 sm:px-8">
    <div className="mx-auto max-w-7xl space-y-6">
      <nav className="flex flex-wrap gap-5 text-sm text-teal-300"><Link href="/dfs/nfl">← NFL DFS</Link><Link href="/dfs/nfl/scenarios">Scenario Lab →</Link><Link href="/dfs/nfl/review">Weekly player review →</Link></nav>
      <header className="flex flex-wrap items-start justify-between gap-5">
        <div><p className="text-xs font-bold uppercase tracking-[.25em] text-teal-400">NFL / Model development</p>
          <h1 className="mt-2 text-4xl font-semibold tracking-tight">Model Lab</h1>
          <p className="mt-3 max-w-2xl text-slate-300">The inputs behind the next projection. Coverage first; workload and efficiency follow.</p></div>
        <div className="space-y-3 text-right"><span className="inline-block rounded-full border border-teal-800 bg-teal-950 px-3 py-1 text-xs text-teal-200">RESEARCH · PRODUCTION UNCHANGED</span>
          <div><button className="rounded-lg border border-slate-500 px-4 py-2 text-sm hover:bg-slate-800" onClick={download}>Export evidence JSON</button></div></div>
      </header>
      <section aria-label="Development stages" className="grid gap-2 sm:grid-cols-5">
        {["01 / Input coverage", "02 / Workload", "03 / Efficiency", "04 / Distributions", "05 / Comparison"].map((label, i) => {
          const target = i === 0 ? "Coverage" : i === 1 ? "Workload" : "Efficiency";
          const enabled = i === 0 || i === 1 && !!workload || i === 2 && !!efficiency;
          const status = i === 0 ? "Saved evidence" : i === 1 ? workload ? "Saved evidence" : workloadFailed ? "Unavailable" : "No saved run" : i === 2 ? efficiency ? "Saved · shadow research" : efficiencyFailed ? "Unavailable" : "No saved run" : "Not implemented";
          return <button type="button" disabled={!enabled} onClick={() => enabled && setView(target)} key={label} className={`rounded-xl border p-3 text-left ${view === target ? "border-teal-600 bg-teal-950" : "border-slate-800 text-slate-400"}`}><p className="text-sm font-semibold">{label}</p><p className="mt-1 text-xs">{status}</p></button>;
        })}
      </section>
      {view !== "Coverage" && workload && <ResearchStages mode={view} workload={workload} workloadDigest={workloadDigest} efficiency={efficiency} efficiencyDigest={efficiencyDigest} />}
      {view === "Coverage" && <>
      {age !== null && age > 36 && <p role="alert" className="rounded-lg border border-amber-700 bg-amber-950 p-4 text-amber-200">This audit is over 36 hours old. It does not confirm current input coverage.</p>}
      <div className="flex flex-wrap gap-4">
        <label className="text-sm">Dataset<select value={dataset} onChange={e => { setDataset(e.target.value); setSelected(null); }} className="ml-3 rounded-lg border border-slate-600 bg-slate-900 p-2">{report.datasets.map(d => <option key={d.dataset} value={d.dataset}>{names[d.dataset] ?? d.dataset}</option>)}</select></label>
        <label className="text-sm">Position<select value={position} onChange={e => { setPosition(e.target.value); setSelected(null); }} className="ml-3 rounded-lg border border-slate-600 bg-slate-900 p-2">{["QB", "RB", "WR", "TE", "DST"].map(p => <option key={p}>{p}</option>)}</select></label>
        <label className="text-sm">Field group<select value={group} onChange={e => { setGroup(e.target.value); setSelected(null); }} className="ml-3 rounded-lg border border-slate-600 bg-slate-900 p-2">{["All", ...new Set(report.fields.map(f => f.group))].map(g => <option key={g}>{g}</option>)}</select></label>
      </div>
      {data && <>
        <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4" aria-label="Audit counts">
          {[['Stored rows inspected', integer(data.scanned)], ['In-scope identity-valid rows', integer(data.eligible)], ['Excluded rows', integer(Object.values(data.excluded).reduce((a, b) => a + b, 0))], ['Latest source capture', captureAge === null ? 'Unknown' : `${captureAge.toFixed(1)}h ago`]].map(([label, value]) => <div key={label} className={panel}><p className="text-xs text-slate-400">{label}</p><p className="mt-2 text-2xl font-semibold tabular-nums">{value}</p></div>)}
        </section>
        <p className="text-sm text-amber-200">Retrospective evidence only. Field presence is not proof of historical pregame availability. The two datasets overlap; their counts are not additive.</p>
        {(data.normalization_warning || dataset === "frozen_history") && <p className="rounded-lg border border-amber-800 bg-amber-950/50 p-4 text-sm text-amber-200">{data.normalization_warning ?? "Frozen history includes legacy missing-to-zero defaults. Numeric presence is not original-source completeness."}</p>}
        <section className={panel}>
          <div className="mb-5 flex flex-wrap justify-between gap-3"><div><h2 className="text-xl font-semibold">Input coverage / {position}</h2><p className="mt-1 text-sm text-slate-400">Valid numeric values ÷ applicable stored rows. Select a cell for counts and source timing.</p></div><span className="text-xs text-slate-400">Teal: ≥95% · Amber: partial · Rose: missing · Gray: deferred</span></div>
          <div className="overflow-x-auto"><table className="w-full text-left text-sm"><caption className="sr-only">Field-by-season coverage for {names[dataset]} and {position}</caption>
            <thead><tr className="border-b border-slate-700 text-slate-400"><th className="min-w-48 py-3">Input / units</th>{data.seasons.map(s => <th key={s} className="min-w-24 px-2 text-center">{s}</th>)}</tr></thead>
            <tbody>{fields.map(f => <tr key={f.id} className="border-b border-slate-800"><th scope="row" className="py-3 font-medium">{f.label}<span className="block text-xs font-normal text-slate-400">{f.group} · {f.unit}{!f.supported ? " · contract deferred" : ""}</span></th>{data.seasons.map(s => {
              const c = data.cells.find(c => c.field_id === f.id && c.position === position && c.season === s); const pct = coverage(c);
              return <td key={s} className="p-1"><button disabled={!c} onClick={() => setSelected(c ?? null)} aria-label={`${f.label}, ${s}, ${pct === null ? f.supported ? "no rows" : "deferred" : `${pct.toFixed(1)} percent`}`} className={`w-full rounded-lg border px-2 py-3 text-center tabular-nums hover:ring-2 hover:ring-slate-300 focus-visible:ring-2 focus-visible:ring-white ${pct === null ? "border-slate-700 bg-slate-800 text-slate-400" : pct >= 95 ? "border-teal-800 bg-teal-950 text-teal-200" : pct > 0 ? "border-amber-800 bg-amber-950 text-amber-200" : "border-rose-900 bg-rose-950 text-rose-200"}`}>
                {pct === null ? f.supported ? "—" : "Deferred" : `${pct.toFixed(0)}%`}<span className="mt-1 block text-[10px]">{c ? `n=${integer(c.n)}` : "no rows"}</span></button></td>;
            })}</tr>)}</tbody></table></div>
          {!data.seasons.length && <p className="py-4 text-slate-400">No in-scope rows in this dataset.</p>}
          {!fields.length && <p className="py-4 text-slate-400">No fields apply to this position and group.</p>}
        </section>
        {selected && <section aria-live="polite" className="rounded-2xl border border-teal-700 bg-teal-950/40 p-5"><h2 className="font-semibold">{report.fields.find(f => f.id === selected.field_id)?.label} · {selected.position} · {selected.season}</h2>
          <dl className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-6">{[["Applicable", selected.n], ["Present", selected.present], ["Valid", selected.valid], ["Missing", selected.missing], ["Invalid", selected.invalid], ["Valid zeros", selected.zero]].map(([k, v]) => <div key={k}><dt className="text-xs text-slate-400">{k}</dt><dd className="text-xl">{v}</dd></div>)}</dl>
          <p className="mt-3 text-sm text-slate-300">State: {selected.status.replaceAll("_", " ")} · Capture timestamp on {selected.captured} valid rows · Latest: {selected.latest_capture ?? "unknown"}</p>
          <p className="mt-1 text-xs text-slate-400">Canonical key: {report.fields.find(f => f.id === selected.field_id)?.key}. No unverified field aliases are substituted.</p>
        </section>}
        <section className={panel}><h2 className="text-xl font-semibold">Component cohort readiness</h2><p className="mt-1 text-sm text-slate-400">Core-field completeness, not full model eligibility or scoring coverage.</p>
          <div className="mt-5 grid gap-5 sm:grid-cols-2 lg:grid-cols-5">{data.cohorts.map(c => <div key={c.position}><div className="flex justify-between text-sm"><span>{c.position}</span><span>{integer(c.complete)} / {integer(c.rows)}</span></div><div className="mt-2 h-2 overflow-hidden rounded-full bg-slate-700" role="img" aria-label={`${c.position}: ${c.complete} of ${c.rows} rows have required fields`}><div className="h-full bg-teal-400" style={{ width: `${c.rows ? 100 * c.complete / c.rows : 0}%` }} /></div><p className="mt-2 text-xs text-slate-400">{c.required.join(" · ")}</p></div>)}</div>
        </section>
        <details className={panel}><summary className="cursor-pointer font-semibold">Exclusions, provenance & limitations</summary><div className="mt-4 space-y-3 text-sm text-slate-300">
          <p>Excluded: {Object.entries(data.excluded).map(([k, v]) => `${k}: ${integer(v)}`).join(" · ") || "none"}. Position from canonical fallback: {integer(data.canonical_position_fallback)}.</p>
          <p>Sources: {Object.entries(data.sources).map(([k, v]) => `${k}: ${integer(v)}`).join(" · ")}</p>
          <p>Audit observed: {report.evaluated_at} · {report.version}</p>
          <p className="break-all">Study: {report.study_id}<br />Input digest: {data.input_digest}<br />Audit: {digest}</p>
          <Link className="inline-block text-teal-300 underline" href={`/dfs/nfl/model?audit=${digest}`}>Permalink to this saved audit</Link>
          <ul className="list-disc space-y-2 pl-5">{report.limits.map(limit => <li key={limit}>{limit}</li>)}</ul>
        </div></details>
      </>}
      </>}
    </div>
  </main>;
}
