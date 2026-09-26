"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { importCfbContest, readCfbResults, type CfbResults } from "./actions";

const fmt = (n: number | null | undefined, d = 1) => (n == null ? "—" : n.toFixed(d));
const rankText = (rank: number | null, exact: boolean) => (rank == null ? "—" : `${exact ? "" : "~"}${rank.toLocaleString()}`);

/**
 * CFB contest results: upload DraftKings' standings file, then see how every
 * saved lineup set did, how far the projections were off, and where the
 * field's ownership differed from ours.
 */
export default function CfbResultsPanel({ uploadId, runId, runsVersion }: { uploadId: string; runId: string | null; runsVersion: number }) {
  const [results, setResults] = useState<CfbResults | null>(null);
  const [loadedFor, setLoadedFor] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  const [pending, startTransition] = useTransition();
  const fileRef = useRef<HTMLInputElement>(null);
  const key = `${uploadId}:${runId ?? ""}:${runsVersion}:${version}`;

  useEffect(() => {
    let live = true;
    readCfbResults(uploadId, runId)
      .then((next) => { if (live) { setResults(next); setError(null); setLoadedFor(key); } })
      .catch((reason) => { if (live) { setError(reason instanceof Error ? reason.message : "Results could not be read."); setLoadedFor(key); } });
    return () => { live = false; };
  }, [uploadId, runId, key]);

  function upload(file: File | null) {
    if (!file) return;
    const form = new FormData(); form.set("file", file);
    setError(null); setMessage(null);
    startTransition(async () => {
      try {
        const done = await importCfbContest(uploadId, form);
        setMessage(`Imported contest ${done.contestId}: ${done.entries.toLocaleString()} entries.`);
        setVersion((v) => v + 1);
      } catch (reason) { setError(reason instanceof Error ? reason.message : "Import failed."); }
      if (fileRef.current) fileRef.current.value = "";
    });
  }

  const loading = loadedFor !== key;
  const best = results?.sets.find((s) => s.runId === results.runId)?.best ?? null;

  return <section className="space-y-4 rounded-xl border-2 border-orange-200 bg-orange-50/40 p-4">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <h2 className="text-lg font-bold">Contest results</h2>
        <p className="mt-1 max-w-3xl text-xs text-slate-600">
          In DraftKings, open the finished contest and use its export link. It downloads contest-standings-NNNN.zip; unzip it and upload the .csv.
          That one file scores every saved lineup set, estimates where each lineup would have finished, and grades our projections.
        </p>
      </div>
      <div>
        <input ref={fileRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => upload(e.target.files?.[0] ?? null)} />
        <button disabled={pending} onClick={() => fileRef.current?.click()} className="min-h-10 rounded-lg bg-orange-600 px-4 text-sm font-bold text-white disabled:opacity-50">
          {results ? "Replace contest file" : "Upload contest results"}
        </button>
      </div>
    </div>
    {message ? <p className="rounded-lg border border-emerald-200 bg-emerald-50 p-2 text-sm text-emerald-900">{message}</p> : null}
    {error ? <p role="alert" className="rounded-lg border border-red-300 bg-red-50 p-2 text-sm text-red-900">{error}</p> : null}
    {pending || loading ? <p className="text-sm text-slate-500">Working…</p> : null}
    {!results && !loading ? <p className="text-sm text-slate-600">No contest imported for this slate yet.</p> : null}

    {results ? <>
      <div className="grid gap-3 md:grid-cols-4">
        <Card label="Best lineup" value={fmt(best?.actual)} note={best ? `#${best.lineupNumber} · rank ${rankText(best.rank, best.exactRank)} of ${results.contest.entryCount.toLocaleString()}` : "no lineup could be scored"} />
        <Card label="Field median" value={fmt(results.contest.medianScore)} note={`winner ${fmt(results.contest.winningScore)}`} />
        <Card label="Contest" value={results.contest.entryCount.toLocaleString()} note={`entries · ${results.contest.contestId}`} />
        <Card label="Lineup sets" value={String(results.sets.length)} note="scored below" />
      </div>

      {results.sets.length ? <Panel title="Compare lineup sets" note="Every saved set for this slate, scored against this contest. Sets built from the same pool overlap, and one slate is a few games: this describes tonight, it does not rank the settings.">
        <table className="w-full text-left text-sm">
          <thead className="text-xs uppercase text-slate-500"><tr>
            <th className="p-2">Built</th><th className="p-2">Rules</th><th className="p-2 text-right">Best</th><th className="p-2 text-right">Best rank</th>
            <th className="p-2 text-right">Average</th><th className="p-2 text-right">Projected</th><th className="p-2 text-right">Beat median</th><th className="p-2 text-right">Top 20%</th>
          </tr></thead>
          <tbody>{results.sets.map((s) => <tr key={s.runId} className={`border-t ${s.runId === results.runId ? "bg-orange-100/60 font-semibold" : ""}`}>
            <td className="p-2 whitespace-nowrap">{new Date(s.createdAt).toLocaleString([], { weekday: "short", hour: "numeric", minute: "2-digit" })}</td>
            <td className="p-2 text-slate-600">{s.rules}</td>
            <td className="p-2 text-right">{fmt(s.best?.actual)}</td><td className="p-2 text-right">{s.best ? rankText(s.best.rank, s.best.exactRank) : "—"}</td>
            <td className="p-2 text-right">{fmt(s.averageActual)}</td><td className="p-2 text-right text-slate-500">{fmt(s.averageProjected)}</td>
            <td className="p-2 text-right">{s.scored ? `${s.aboveMedian}/${s.scored}` : "—"}</td>
            <td className="p-2 text-right">{s.ranked ? `${s.topFifth}/${s.ranked}` : "—"}</td>
          </tr>)}</tbody>
        </table>
      </Panel> : <p className="text-sm text-slate-600">No saved lineup sets for this slate, so there is nothing of yours to score.</p>}

      {results.lineups.length ? <Panel title="Lineups in the loaded set" note="Rank is exact in the top 100 and estimated (~) below. A lineup with a player nobody in the contest drafted cannot be scored from this file.">
        <div className="max-h-96 overflow-auto"><table className="w-full text-left text-sm">
          <thead className="sticky top-0 bg-white text-xs uppercase text-slate-500"><tr>
            <th className="p-2">#</th><th className="p-2">QBs</th><th className="p-2 text-right">Actual</th><th className="p-2 text-right">Projected</th><th className="p-2 text-right">Rank</th><th className="p-2 text-right">Beat</th>
          </tr></thead>
          <tbody>{[...results.lineups].sort((a, b) => (b.actual ?? -1) - (a.actual ?? -1)).map((l) => <tr key={l.lineupNumber} className="border-t">
            <td className="p-2">{l.lineupNumber}</td><td className="p-2">{l.qbs.join(" + ")}</td>
            <td className="p-2 text-right font-semibold">{l.actual == null ? <span className="text-amber-700" title={`Not in the file: ${l.missing.join(", ")}`}>unknown</span> : fmt(l.actual)}</td>
            <td className="p-2 text-right text-slate-500">{fmt(l.projected)}</td><td className="p-2 text-right">{rankText(l.rank, l.exactRank)}</td>
            <td className="p-2 text-right text-slate-500">{l.beatShare == null ? "—" : `${Math.round(l.beatShare * 100)}%`}</td>
          </tr>)}</tbody>
        </table></div>
      </Panel> : null}

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Field's most-drafted vs ours" note="Total % drafted across all slots, what each scored, and our exposure in the loaded set.">
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase text-slate-500"><tr><th className="p-2">Player</th><th className="p-2 text-right">Field</th><th className="p-2 text-right">Ours</th><th className="p-2 text-right">Scored</th></tr></thead>
            <tbody>{results.fieldChalk.map((p) => <tr key={p.name} className="border-t">
              <td className="p-2">{p.name}</td><td className="p-2 text-right">{fmt(p.field)}%</td>
              <td className={`p-2 text-right ${p.ours + 10 < p.field ? "text-red-700" : p.ours > p.field + 10 ? "text-emerald-700" : ""}`}>{fmt(p.ours)}%</td>
              <td className="p-2 text-right font-semibold">{fmt(p.fpts)}</td>
            </tr>)}</tbody>
          </table>
        </Panel>
        <Panel title="How far our projections were off" note="Players we projected above zero who were drafted in this contest. Bias below zero means we projected too high.">
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase text-slate-500"><tr><th className="p-2">Position</th><th className="p-2 text-right">Players</th><th className="p-2 text-right">Avg miss</th><th className="p-2 text-right">Bias</th></tr></thead>
            <tbody>{results.positionError.map((e) => <tr key={e.position} className={`border-t ${e.position === "All" ? "font-semibold" : ""}`}>
              <td className="p-2">{e.position}</td><td className="p-2 text-right">{e.n}</td><td className="p-2 text-right">{fmt(e.mae)}</td>
              <td className={`p-2 text-right ${e.bias < 0 ? "text-red-700" : "text-emerald-700"}`}>{e.bias > 0 ? "+" : ""}{fmt(e.bias)}</td>
            </tr>)}</tbody>
          </table>
        </Panel>
      </div>
    </> : null}
  </section>;
}

function Card({ label, value, note }: { label: string; value: string; note: string }) {
  return <div className="rounded-xl border bg-white p-3 shadow-sm">
    <div className="text-[11px] font-bold uppercase text-slate-500">{label}</div>
    <div className="mt-1 text-2xl font-black">{value}</div>
    <div className="mt-0.5 text-xs text-slate-500">{note}</div>
  </div>;
}

function Panel({ title, note, children }: { title: string; note: string; children: React.ReactNode }) {
  return <div className="rounded-xl border bg-white p-4 shadow-sm">
    <h3 className="font-bold">{title}</h3>
    <p className="mt-1 text-xs text-slate-500">{note}</p>
    <div className="mt-3 overflow-auto">{children}</div>
  </div>;
}
