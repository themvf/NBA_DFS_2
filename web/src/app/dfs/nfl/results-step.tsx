"use client";

import { useEffect, useState } from "react";
import FieldAuditPanel from "./field-audit-panel";
import { readNflSlateResults, type NflSlateResults } from "./actions";

/**
 * Step 4 of the workspace: what happened.
 *
 * One file drives all of it -- DraftKings' contest standings export, which
 * carries every entry's score and the points DraftKings paid each player. From
 * it: how the saved lineup set scored and where it would have ranked, how far
 * our projections were off by position, and (below) what the field knew that
 * we did not. The upload used to sit 38 panels deep in a research tab.
 */
export default function ResultsStep({ uploadId, runId, lineupCount, locked, poolReviewHref }: {
  uploadId: string;
  runId: string | null;
  lineupCount: number;
  locked: boolean;
  poolReviewHref: string;
}) {
  const [results, setResults] = useState<NflSlateResults | null>(null);
  const [loadedFor, setLoadedFor] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  const key = `${uploadId}:${runId ?? ""}:${version}`;
  const loading = loadedFor !== key;

  useEffect(() => {
    let live = true;
    readNflSlateResults(uploadId, runId)
      .then((next) => { if (live) { setResults(next); setError(null); setLoadedFor(key); } })
      .catch((reason) => { if (live) { setError(reason instanceof Error ? reason.message : "Results could not be read."); setLoadedFor(key); } });
    return () => { live = false; };
  }, [uploadId, runId, key]);
  // A fresh upload re-reads the numbers above it.
  const reload = () => setVersion((v) => v + 1);

  const fmt = (n: number | null | undefined, d = 1) => (n == null ? "—" : n.toFixed(d));
  const rankText = (rank: number | null, exact: boolean) =>
    rank == null ? "—" : `${exact ? "" : "~"}${rank.toLocaleString()}`;

  return <div className="space-y-4">
    {!results && !loading ? <section className="rounded-xl border border-blue-200 bg-blue-50 p-5">
      <h2 className="font-bold text-blue-950">{locked ? "Upload your contest results" : "Results arrive after the game"}</h2>
      <p className="mt-1 max-w-2xl text-sm text-blue-900">
        {locked
          ? "In DraftKings, open the finished contest and use its export link. It downloads contest-standings-NNNN.zip; unzip it and upload the .csv in the panel below. That one file scores your saved lineups, shows where they would have finished, and grades our projections."
          : "Once the games are over, download the contest standings from DraftKings and upload them here to see how your lineups did."}
      </p>
    </section> : null}

    {error ? <p role="alert" className="rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-900">{error}</p> : null}

    {results ? <>
      <section className="grid gap-3 md:grid-cols-4">
        <Card label="Best lineup" value={results.best ? fmt(results.best.actual) : "—"}
          note={results.best ? `#${results.best.lineupNumber} · rank ${rankText(results.best.rank, results.best.exactRank)} of ${results.contest.entryCount.toLocaleString()}` : (runId ? "No lineup could be scored" : "No saved lineup set")} />
        <Card label="Your average" value={fmt(results.averageActual)}
          note={results.averageProjected != null ? `projected ${fmt(results.averageProjected)}` : "—"} />
        <Card label="Field median" value={fmt(results.contest.medianScore)}
          note={`winner ${fmt(results.contest.winningScore)}`} />
        <Card label="Lineups scored" value={`${results.lineups.filter((l) => l.actual != null).length}/${lineupCount || results.lineups.length}`}
          note={`contest ${results.contest.contestId}`} />
      </section>

      {results.lineups.length ? <section className="rounded-xl border bg-white p-4 shadow-sm">
        <h2 className="font-bold">Your lineups</h2>
        <p className="mt-1 text-xs text-slate-500">
          Scored with the points DraftKings paid, captain at 1.5×. Rank is where the lineup would have finished in this contest
          {results.rankAvailable ? " — exact in the top 100, estimated (~) below that." : " — unavailable for this import; re-upload the file to add it."}
        </p>
        <div className="mt-3 max-h-[420px] overflow-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase text-slate-500"><tr>
              <th className="p-2">#</th><th className="p-2">Captain</th><th className="p-2 text-right">Actual</th>
              <th className="p-2 text-right">Projected</th><th className="p-2 text-right">Rank</th><th className="p-2 text-right">Beat</th>
            </tr></thead>
            <tbody>{[...results.lineups].sort((a, b) => (b.actual ?? -1) - (a.actual ?? -1)).map((l) => <tr key={l.lineupNumber} className="border-t">
              <td className="p-2">{l.lineupNumber}</td>
              <td className="p-2">{l.captain ?? "—"}</td>
              <td className="p-2 text-right font-semibold">{l.actual == null ? <span title={`Not in the export: ${l.missing.join(", ")}`} className="text-amber-700">unknown</span> : fmt(l.actual)}</td>
              <td className="p-2 text-right text-slate-500">{fmt(l.projected)}</td>
              <td className="p-2 text-right">{rankText(l.rank, l.exactRank)}</td>
              <td className="p-2 text-right text-slate-500">{l.beatShare == null ? "—" : `${Math.round(l.beatShare * 100)}%`}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </section> : <p className="rounded-lg border border-dashed bg-white p-4 text-sm text-slate-600">
        No saved lineup set is loaded for this slate, so there is nothing of yours to score. Choose one in the Review step.
      </p>}

      {results.positionError.length ? <section className="rounded-xl border bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h2 className="font-bold">How far our projections were off</h2>
          <a href={poolReviewHref} className="text-sm font-semibold text-blue-700 underline">Full projection review</a>
        </div>
        <p className="mt-1 text-xs text-slate-500">Average miss per player against what DraftKings paid. Bias below zero means we projected too high. Ruled-out players are not graded.</p>
        <table className="mt-3 w-full max-w-xl text-left text-sm">
          <thead className="text-xs uppercase text-slate-500"><tr><th className="p-2">Position</th><th className="p-2 text-right">Players</th><th className="p-2 text-right">Avg miss</th><th className="p-2 text-right">Bias</th></tr></thead>
          <tbody>{results.positionError.map((e) => <tr key={e.position} className={`border-t ${e.position === "All" ? "font-semibold" : ""}`}>
            <td className="p-2">{e.position}</td><td className="p-2 text-right">{e.n}</td>
            <td className="p-2 text-right">{fmt(e.mae)}</td>
            <td className={`p-2 text-right ${e.bias < 0 ? "text-red-700" : "text-emerald-700"}`}>{e.bias > 0 ? "+" : ""}{fmt(e.bias)}</td>
          </tr>)}</tbody>
        </table>
      </section> : null}
    </> : null}

    <FieldAuditPanel uploadId={uploadId} onImported={reload} />
  </div>;
}

function Card({ label, value, note }: { label: string; value: string; note: string }) {
  return <div className="rounded-xl border bg-white p-4 shadow-sm">
    <div className="text-[11px] font-bold uppercase text-slate-500">{label}</div>
    <div className="mt-1 text-2xl font-black">{value}</div>
    <div className="mt-0.5 text-xs text-slate-500">{note}</div>
  </div>;
}
