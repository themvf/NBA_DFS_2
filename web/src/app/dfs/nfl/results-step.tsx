"use client";

import { useEffect, useMemo, useState } from "react";
import { scoreByShape, type BreakdownLineup, type PortfolioFormat, type ShapeScoreRow } from "@/lib/nfl-dfs/portfolio-breakdown";
import FieldAuditPanel from "./field-audit-panel";
import { readNflSlateResults, type NflSlateResults } from "./client-actions";

const PLAN_LABELS: Record<string, string> = {
  balanced: "Balanced mix", chalk_leverage: "Chalk captain, rotating leverage", standard: "Standard ceiling", custom: "Custom plan",
};
const SOURCE_LABELS: Record<string, string> = {
  our: "Our model", workload: "Workload", calibrated: "Calibrated", dk_avg: "DK average", fantasypros: "FantasyPros", linestar: "LineStar", custom: "Custom",
};

/**
 * Step 4 of the workspace: what happened.
 *
 * One file drives all of it -- DraftKings' contest standings export, which
 * carries every entry's score and the points DraftKings paid each player. From
 * it: how the saved lineup set scored and where it would have ranked, how far
 * our projections were off by position, and (below) what the field knew that
 * we did not. The upload used to sit 38 panels deep in a research tab.
 */
export default function ResultsStep({ uploadId, runId, lineupCount, locked, poolReviewHref, lineups, format }: {
  uploadId: string;
  runId: string | null;
  lineupCount: number;
  locked: boolean;
  poolReviewHref: string;
  /** The loaded run's lineups, to group the scores by captain, split and structure. */
  lineups?: readonly BreakdownLineup[];
  format?: PortfolioFormat;
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
  const shapeScores = useMemo(() => results && lineups?.length && format && results.runId === runId
    ? scoreByShape(lineups, format, new Map(results.lineups.map((l) => [l.lineupNumber, l.actual])), results.contest.medianScore)
    : null, [results, lineups, format, runId]);
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

      {results.sets?.length ? <section className="rounded-xl border bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h2 className="font-bold">Compare lineup sets</h2>
          <a href="/dfs/nfl/results" className="text-sm font-semibold text-blue-700 underline">Results across all slates</a>
        </div>
        <p className="mt-1 text-xs text-slate-500">
          Every set you built for this slate, scored against the same contest. Median {fmt(results.contest.medianScore)};
          top 20% means beating at least 80% of the {results.contest.entryCount.toLocaleString()} entries.
          {(results.sets?.length ?? 0) > 1 ? " Sets built from the same player pool overlap, and one slate is one game — this describes Thursday, it does not rank the plans." : ""}
        </p>
        <div className="mt-3 overflow-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase text-slate-500"><tr>
              <th className="p-2">Built</th><th className="p-2">Plan</th><th className="p-2">Source</th><th className="p-2 text-right">Lineups</th>
              <th className="p-2 text-right">Best</th><th className="p-2 text-right">Best rank</th><th className="p-2 text-right">Average</th>
              <th className="p-2 text-right">Projected</th><th className="p-2 text-right">Beat median</th><th className="p-2 text-right">Top 20%</th>
            </tr></thead>
            <tbody>{results.sets.map((set) => <tr key={set.runId} className={`border-t ${set.runId === results.runId ? "bg-emerald-50 font-semibold" : ""}`}>
              <td className="p-2 whitespace-nowrap">{new Date(set.createdAt).toLocaleString([], { weekday: "short", hour: "numeric", minute: "2-digit" })}{set.runId === results.runId ? <span className="ml-1 text-[10px] font-bold uppercase text-emerald-700">loaded</span> : null}</td>
              <td className="p-2">{PLAN_LABELS[set.planMode ?? ""] ?? "Standard ceiling"} <span className="text-slate-400">{set.mode.toUpperCase()}</span></td>
              <td className="p-2 text-slate-600">{SOURCE_LABELS[set.source] ?? set.source}</td>
              <td className="p-2 text-right">{set.scored === set.lineups ? set.lineups : `${set.scored}/${set.lineups}`}</td>
              <td className="p-2 text-right">{set.best ? fmt(set.best.actual) : "—"}</td>
              <td className="p-2 text-right">{set.best ? rankText(set.best.rank, set.best.exactRank) : "—"}</td>
              <td className="p-2 text-right">{fmt(set.averageActual)}</td>
              <td className="p-2 text-right text-slate-500">{fmt(set.averageProjected)}</td>
              <td className="p-2 text-right">{set.scored ? `${set.aboveMedian}/${set.scored}` : "—"}</td>
              <td className="p-2 text-right">{set.ranked ? `${set.topFifth}/${set.ranked}` : "—"}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </section> : null}

      {results.captains?.length ? <section className="rounded-xl border bg-white p-4 shadow-sm">
        <h2 className="font-bold">Captains in the loaded set</h2>
        <table className="mt-3 w-full max-w-2xl text-left text-sm">
          <thead className="text-xs uppercase text-slate-500"><tr>
            <th className="p-2">Captain</th><th className="p-2 text-right">Lineups</th><th className="p-2 text-right">Average</th>
            <th className="p-2 text-right">Best</th><th className="p-2 text-right">Beat median</th>
          </tr></thead>
          <tbody>{results.captains.map((c) => <tr key={c.captain} className="border-t">
            <td className="p-2">{c.captain}</td><td className="p-2 text-right">{c.lineups}</td>
            <td className="p-2 text-right font-semibold">{fmt(c.average)}</td><td className="p-2 text-right">{fmt(c.best)}</td>
            <td className="p-2 text-right">{c.aboveMedian}/{c.lineups}</td>
          </tr>)}</tbody>
        </table>
      </section> : null}

      {shapeScores ? <section className="rounded-xl border bg-white p-4 shadow-sm">
        <h2 className="font-bold">How each part of the build scored</h2>
        <p className="mt-1 text-xs text-slate-500">The loaded set&apos;s lineups grouped the way the Review step breaks them down, scored with the points DraftKings paid. One game is one sample: this says what happened, not which shape is better.</p>
        <div className={`mt-3 grid gap-3 ${format === "showdown" ? "lg:grid-cols-3" : "lg:grid-cols-2"}`}>
          {format === "showdown" ? <ShapeTable title="Captain" rows={shapeScores.captains} fmt={fmt} /> : null}
          <ShapeTable title={format === "showdown" ? "Team split" : "Stack"} rows={[...shapeScores.splitKinds, ...shapeScores.splits]} fmt={fmt} />
          <ShapeTable title="Structure" rows={[...shapeScores.quarterbacks, ...(format === "showdown" ? shapeScores.kicker : [])]} fmt={fmt} />
        </div>
      </section> : null}

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
        <p className="mt-1 text-xs text-slate-500">Average miss per player against what DraftKings paid, using the projections the page showed. Bias below zero means we projected too high. Players the page marked out or blocked are not graded.</p>
        {results.positionErrorNote ? <p className="mt-1 text-xs font-semibold text-amber-800">{results.positionErrorNote}</p> : null}
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

function ShapeTable({ title, rows, fmt }: { title: string; rows: ShapeScoreRow[]; fmt: (n: number | null | undefined, d?: number) => string }) {
  const best = Math.max(...rows.map((r) => r.average ?? -Infinity));
  return <div className="rounded-lg border p-2">
    <h3 className="text-xs font-bold text-slate-700">{title}</h3>
    <table className="mt-1 w-full text-left text-xs">
      <thead className="text-[10px] uppercase text-slate-400"><tr><th className="py-1" /><th className="text-right">Lineups</th><th className="text-right">Average</th><th className="text-right">Best</th><th className="text-right">Beat median</th></tr></thead>
      <tbody>{rows.map((r) => <tr key={`${title}-${r.key}`} className="border-t">
        <td className="py-1 pr-1 font-semibold text-slate-800">{r.label}</td>
        <td className="text-right tabular-nums">{r.scored === r.lineups ? r.lineups : `${r.scored}/${r.lineups}`}</td>
        <td className={`text-right tabular-nums font-semibold ${r.average != null && r.average === best ? "text-emerald-700" : ""}`}>{fmt(r.average)}</td>
        <td className="text-right tabular-nums">{fmt(r.best)}</td>
        <td className="text-right tabular-nums text-slate-500">{r.aboveMedian == null ? "—" : `${r.aboveMedian}/${r.scored}`}</td>
      </tr>)}</tbody>
    </table>
  </div>;
}
