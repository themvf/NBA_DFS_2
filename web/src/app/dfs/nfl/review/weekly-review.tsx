"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { loadPlayerHistory } from "./actions";
import { CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { reportSummary, VARIANT_LABELS, type ReportVariant, type WeeklyReport } from "@/lib/nfl-dfs/report-card";

const fmt = (v: number | null | undefined) => v == null ? "—" : v.toFixed(1);
const date = (v: string) => new Date(v).toLocaleString("en-US", { timeZone: "America/New_York", dateStyle: "short", timeStyle: "short" });
const label = (v: string) => v.replaceAll("_", " ");

export default function WeeklyReview({ reports, availableWeeks, season, viewedAt }: { reports: WeeklyReport[]; availableWeeks: number[]; season: number; viewedAt: number }) {
  const router = useRouter();
  const week = reports.at(-1)?.week ?? 1;
  const [variant, setVariant] = useState<ReportVariant>("production");
  const [position, setPosition] = useState("ALL");
  const [query, setQuery] = useState("");
  const [playerId, setPlayerId] = useState<number | null>(null);
  const report = reports.find(r => r.week === week);
  const rows = useMemo(() => (report?.rows ?? []).filter(r => r.variant === variant &&
    (position === "ALL" || r.position === position) && `${r.name} ${r.team}`.toLowerCase().includes(query.toLowerCase()))
    .sort((a,b) => Number(b.overdue)-Number(a.overdue) || (b.absolute_error ?? -1)-(a.absolute_error ?? -1) || a.name.localeCompare(b.name)), [report, variant, position, query]);
  const summary = reportSummary(rows);
  const selected = rows.find(r => r.player_id === playerId) ?? rows[0];
  const initialTrajectory = reports.flatMap(r => r.rows.filter(p => p.player_id === selected?.player_id && p.variant === variant)
    .map(p => ({ week: p.week, expected: p.forecast?.mean, P10: p.forecast?.p10, P90: p.forecast?.p90, actual: p.actual })));
  const selectedId = selected?.player_id;
  const historyKey = `${season}:${variant}:${selectedId}`;
  const [history, setHistory] = useState<{ key: string; rows: Awaited<ReturnType<typeof loadPlayerHistory>>; error?: string } | null>(null);
  useEffect(() => {
    let active = true;
    if (selectedId) loadPlayerHistory(season, selectedId, variant).then(rows => {
      if (active) setHistory({ key: historyKey, rows });
    }).catch(() => { if (active) setHistory({ key: historyKey, rows: [], error: "Earlier weekly history could not be loaded." }); });
    return () => { active = false; };
  }, [season, selectedId, variant, historyKey]);
  const trajectory = history?.key === historyKey && !history.error ? history.rows : initialTrajectory;
  const stale = report ? viewedAt - new Date(report.evaluated_at).getTime() > 36*3600000 : false;
  function download() {
    const blob = new Blob([JSON.stringify({ ...report, filtered_rows: rows }, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob), anchor = document.createElement("a");
    anchor.href = url; anchor.download = `nfl-${season}-week-${week}-${variant}-audit.json`; anchor.click(); URL.revokeObjectURL(url);
  }
  return <main className="mx-auto max-w-[1600px] space-y-5 p-4 sm:p-6">
    <header className="rounded-2xl bg-slate-950 p-6 text-white">
      <Link className="text-sm text-emerald-300" href="/dfs/nfl">← NFL DFS workspace</Link>
      <p className="mt-5 text-xs font-bold uppercase tracking-widest text-emerald-300">Forecast accountability · {season}</p>
      <h1 className="mt-1 text-3xl font-black">Weekly Player Review</h1>
      <p className="mt-2 max-w-3xl text-sm text-slate-300">Frozen pregame forecasts, realized scores and every unresolved result. Production and research models stay separate. These are projection metrics, not lineup ROI.</p>
    </header>
    <form className="flex gap-2 text-sm"><label>Season <input aria-label="Season" className="w-24 rounded border p-2" name="season" type="number" min="2000" max="2099" defaultValue={season}/></label><button className="rounded border px-3">Load season</button></form>
    {!report ? <section className="rounded-xl border p-8"><h2 className="font-bold">No saved weekly reports yet</h2><p className="mt-2">The daily report-card job must run for this season. No sample outcomes are substituted.</p></section> : <>
      <section className="flex flex-wrap gap-3 rounded-xl border bg-white p-4">
        <label className="text-xs font-semibold">Week<select aria-label="Week" className="mt-1 block rounded border p-2" value={week} onChange={e => router.push(`/dfs/nfl/review?season=${season}&week=${e.target.value}`)}>{availableWeeks.map(w => <option key={w} value={w}>Week {w}</option>)}</select></label>
        <label className="text-xs font-semibold">Model<select aria-label="Model" className="mt-1 block rounded border p-2" value={variant} onChange={e => setVariant(e.target.value as ReportVariant)}>{Object.entries(VARIANT_LABELS).map(([v,l]) => <option key={v} value={v}>{l}</option>)}</select></label>
        <label className="text-xs font-semibold">Position<select aria-label="Position" className="mt-1 block rounded border p-2" value={position} onChange={e => setPosition(e.target.value)}>{["ALL","QB","RB","WR","TE","DST"].map(p => <option key={p}>{p}</option>)}</select></label>
        <label className="flex-1 text-xs font-semibold">Player or team<input aria-label="Player or team" className="mt-1 block w-full rounded border p-2" value={query} onChange={e => setQuery(e.target.value)} placeholder="Search player"/></label>
        <button onClick={download} className="self-end rounded border px-3 py-2 text-sm font-semibold">Download audit</button>
      </section>
      <p className="text-xs text-slate-600">Saved {date(report.evaluated_at)} ET · {report.completed_games}/{report.scheduled_games} games completed · {report.rejected_non_pregame_snapshots} invalid/unmapped snapshots excluded · {report.checkpoint}</p>
      {(stale || summary.overdue > 0) && <div role="alert" className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm">{stale ? "Report is older than 36 hours. " : ""}{summary.overdue > 0 ? `${summary.overdue} player rows lack scorable results more than 48 hours after kickoff. Check source coverage.` : ""}</div>}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-6">{[
        ["Forecast coverage", `${summary.forecasted}/${summary.players}`], ["Scored / unscored", `${summary.scored} / ${summary.unscored}`],
        ["Average absolute error", fmt(summary.mae)], ["Actual − projected", fmt(summary.bias)],
        ["P10–P90 coverage", summary.coverage === null ? "—" : `${(summary.coverage*100).toFixed(0)}%`], ["Overdue results", String(summary.overdue)],
      ].map(([title,value]) => <div key={title} className="rounded-xl border bg-white p-4"><p className="text-xs text-slate-500">{title}</p><p className="mt-2 text-2xl font-bold">{value}</p></div>)}</div>
      <p className="text-xs text-slate-500">Metrics reflect the filters above. P10–P90 targets roughly 80% coverage, not guaranteed bounds. {report.population}. {report.missing_policy}</p>
      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)]">
        <div className="max-h-[650px] overflow-auto rounded-xl border bg-white"><table className="w-full text-left text-xs"><thead className="sticky top-0 bg-slate-100"><tr>{["Player", "Expected", "P10–P90", "Actual", "Error", "Status"].map(c => <th className="p-3" key={c}>{c}</th>)}</tr></thead><tbody>{rows.map(r => <tr key={`${r.player_id}:${r.game_id}`} className={`border-t ${selected === r ? "bg-emerald-50" : ""}`}><td className="p-3"><button className="text-left font-bold hover:underline" onClick={() => setPlayerId(r.player_id)}>{r.name}</button><div className="text-slate-500">{r.position} · {r.team} vs {r.opponent}</div></td><td className="p-3">{fmt(r.forecast?.mean)}</td><td className="whitespace-nowrap p-3">{fmt(r.forecast?.p10)} – {fmt(r.forecast?.p90)}</td><td className="p-3">{fmt(r.actual)}</td><td className="p-3">{fmt(r.error)}</td><td className="p-3">{label(r.status)}{r.overdue ? " · overdue" : ""}</td></tr>)}</tbody></table>{!rows.length && <p className="p-5">No matching player rows.</p>}</div>
        {selected && <aside className="space-y-4 rounded-xl border bg-white p-5"><div><h2 className="text-xl font-bold">{selected.name}</h2><p className="text-xs text-slate-500">{VARIANT_LABELS[variant]} · {selected.forecast?.history_games ?? 0} prior games</p></div>
          <div className="h-64" role="img" aria-label={`Weekly projected score, P10, P90 and actual score for ${selected.name}`}><ResponsiveContainer width="100%" height="100%"><ComposedChart data={trajectory}><CartesianGrid strokeDasharray="3 3"/><XAxis dataKey="week"/><YAxis/><Tooltip/><Line dataKey="P10" stroke="#94a3b8" strokeDasharray="3 3" connectNulls={false}/><Line dataKey="P90" stroke="#64748b" strokeDasharray="3 3" connectNulls={false}/><Line dataKey="expected" stroke="#2563eb" strokeWidth={2} connectNulls={false}/><Line dataKey="actual" stroke="#059669" strokeWidth={2} connectNulls={false}/></ComposedChart></ResponsiveContainer></div>
          <p className="text-xs text-slate-500">Blue: expected · Green: actual · Dashed: P10/P90. Dots remain visible with one week. No actual point is drawn while results are missing.</p>
          {history?.key === historyKey && history.error && <p role="alert" className="text-xs text-amber-800">{history.error} Showing the selected week only.</p>}
          <div className="grid grid-cols-3 gap-2 text-xs"><div>Median<strong className="block">{fmt(selected.forecast?.median)}</strong></div><div>Boom probability<strong className="block">{selected.forecast?.boom_probability == null ? "—" : `${(selected.forecast.boom_probability*100).toFixed(1)}%`}</strong></div><div>Within range<strong className="block">{selected.interval_hit === null ? "Pending" : selected.interval_hit ? "Yes" : "No"}</strong></div></div>
          <h3 className="font-semibold">Component breakdown</h3>{selected.components.length ? <table className="w-full text-xs"><thead><tr><th className="text-left">Stat</th><th>Projected</th><th>Actual</th></tr></thead><tbody>{selected.components.map(c => <tr key={c.stat} className="border-t"><td className="py-1">{label(c.stat)}</td><td className="text-center">{fmt(c.projected)}</td><td className="text-center">{fmt(c.actual)}</td></tr>)}</tbody></table> : <p className="text-xs text-slate-500">Component forecasts were not frozen for this snapshot/model. They are not reconstructed after the game.</p>}
          <details className="rounded border p-3 text-xs"><summary className="cursor-pointer font-semibold">Audit evidence & revisions</summary><p className="mt-2">Kickoff: {date(selected.kickoff)} ET</p><p>Captured: {selected.forecast ? `${date(selected.forecast.captured_at)} ET` : "No accepted forecast"}</p><p>Outcome revisions: {selected.result_revision_count} · Scorer: {selected.scoring_version ?? "pending"}</p><pre className="mt-2 max-h-80 overflow-auto whitespace-pre-wrap break-all">{JSON.stringify(selected, null, 2)}</pre></details>
        </aside>}
      </section>
    </>}
  </main>;
}
