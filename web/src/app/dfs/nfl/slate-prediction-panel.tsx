"use client";

import { useMemo, useState } from "react";
import { breakdownPortfolio, SHOWDOWN_SPLIT_REFERENCE, type BreakdownLineup } from "@/lib/nfl-dfs/portfolio-breakdown";
import { DEFAULT_SHAPE_DRAWS, predictShowdownShapes, type PredictionPlayer, type SlateShapePrediction } from "@/lib/nfl-dfs/slate-shape-prediction";

/**
 * "Which lineup type wins this slate?" The best possible lineup in each of
 * 1,000 simulated games, counted by shape, beside the loaded build's own mix.
 * Showdown only; the Results step grades it against the real best lineup.
 */
export default function SlatePredictionPanel({ slateKey, players, format, lineups }: {
  /** Changes when the slate or its projections change, so a stale answer is dropped. */
  slateKey: string;
  players: readonly PredictionPlayer[];
  format: "classic" | "showdown";
  lineups?: readonly BreakdownLineup[];
}) {
  const [result, setResult] = useState<{ key: string; prediction: SlateShapePrediction } | null>(null);
  const [progress, setProgress] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const prediction = result?.key === slateKey ? result.prediction : null;
  const ours = useMemo(() => (lineups?.length ? breakdownPortfolio(lineups, "showdown") : null), [lineups]);

  if (format !== "showdown") return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <h2 className="font-bold">Which lineup type wins this slate?</h2>
    <p className="mt-1 text-xs text-slate-500">Showdown only for now. Classic&apos;s best lineup needs a different solver.</p>
  </section>;

  function run() {
    setError(null); setProgress(0);
    predictShowdownShapes(players, { onProgress: setProgress })
      .then((p) => setResult({ key: slateKey, prediction: p }))
      .catch((reason) => setError(reason instanceof Error ? reason.message : "The prediction could not run."))
      .finally(() => setProgress(null));
  }

  const ourPct = (rows: { key: string; pct: number }[] | undefined, key: string) => rows?.find((r) => r.key === key)?.pct ?? (ours ? 0 : null);
  const top = prediction?.splits[0] && prediction.splitKinds[0]
    ? (prediction.splits[0].pct >= prediction.splitKinds[0].pct * 0.6 ? prediction.splits[0] : prediction.splitKinds[0]) : null;
  return <section className="rounded-xl border border-violet-200 bg-white p-4 shadow-sm">
    <div className="flex flex-wrap items-start justify-between gap-2">
      <div>
        <h2 className="font-bold">Which lineup type wins this slate? <a href="/dfs/nfl/results#showdown-types" className="ml-2 text-xs font-semibold text-blue-700 underline">Which types have won so far</a></h2>
        <p className="mt-1 max-w-2xl text-xs text-slate-500">We simulate the game {DEFAULT_SHAPE_DRAWS.toLocaleString()} times with the same projections, ranges and teammate correlations the optimizer uses, and find the best possible lineup under the cap in each. The shares below are how often each lineup type was the best one. This is the model&apos;s prediction, not a measured rate; Results grades it after the game.</p>
      </div>
      <button type="button" onClick={run} disabled={progress != null} className="rounded-lg bg-violet-700 px-3 py-2 text-sm font-bold text-white disabled:opacity-50">
        {progress != null ? `Simulating… ${Math.round((100 * progress) / DEFAULT_SHAPE_DRAWS)}%` : prediction ? "Run again" : "Predict the winning lineup type"}
      </button>
    </div>
    {error ? <p role="alert" className="mt-2 text-xs font-semibold text-red-700">{error}</p> : null}
    {prediction ? <>
      {top ? <p className="mt-3 rounded-lg bg-violet-50 px-3 py-2 text-sm text-violet-950">
        Most likely best lineup: <b>{top.label}</b> ({Math.round(top.pct)}% of simulated games), captain <b>{prediction.captains[0]?.label}</b> ({Math.round(prediction.captains[0]?.pct ?? 0)}%).
        The best possible score ran {prediction.score.p10.toFixed(0)}–{prediction.score.p90.toFixed(0)} (middle 80%), median {prediction.score.median.toFixed(0)}.
      </p> : null}
      <div className="mt-3 grid gap-3 md:grid-cols-3">
        <Table title="Team split" ours={!!ours} rows={[...prediction.splitKinds.map((r) => ({ ...r, ours: ourPct(ours?.splitKinds, r.key), note: SHOWDOWN_SPLIT_REFERENCE[r.key] ? `past winners ${SHOWDOWN_SPLIT_REFERENCE[r.key].winners}%` : undefined })),
          ...prediction.splits.map((r) => ({ ...r, ours: ourPct(ours?.splits, r.key), sub: true }))]} />
        <Table title="Captain" ours={!!ours} rows={prediction.captains.slice(0, 10).map((r) => ({ ...r, label: r.label, detail: `${r.position} ${r.team}`, ours: ourPct(ours?.captains, r.key) }))} />
        <Table title="Structure" ours={!!ours} rows={[...prediction.quarterbacks.map((r) => ({ ...r, ours: ourPct(ours?.quarterbacks, r.key) })),
          { key: "kicker", label: "With a kicker", pct: prediction.kicker, ours: ours ? (100 * ours.kicker) / Math.max(1, ours.total) : null },
          { key: "dst", label: "With a defense", pct: prediction.dst, ours: ours ? (100 * ours.dst) / Math.max(1, ours.total) : null }]} />
      </div>
      <p className="mt-2 text-[10px] text-slate-500">
        {prediction.solved.toLocaleString()} simulated games, seed {prediction.seed}, {prediction.version}. Our projections only (not LineStar or other sources). A contest is won by a lineup close to the best possible one, but the right type is not enough on its own: the more the field plays a type, the more ways it is split.
        {prediction.fixed.length ? ` Drawn at projection (no range): ${prediction.fixed.join(", ")}.` : ""}
      </p>
    </> : null}
  </section>;
}

function Table({ title, rows, ours }: { title: string; ours: boolean; rows: { key: string; label: string; pct: number; ours?: number | null; note?: string; sub?: boolean; detail?: string }[] }) {
  return <div className="rounded-lg border p-2">
    <h3 className="text-xs font-bold text-slate-700">{title}</h3>
    <table className="mt-1 w-full text-[11px]">
      <thead className="text-slate-400"><tr><th className="text-left font-normal" /><th className="text-right font-normal">Best lineup</th>{ours ? <th className="text-right font-normal">Your build</th> : null}</tr></thead>
      <tbody>{rows.map((r) => {
        const gap = r.ours == null ? null : r.ours - r.pct;
        return <tr key={`${title}-${r.key}`} className="border-t align-top">
          <td className={`py-1 pr-1 ${r.sub ? "pl-2 text-slate-600" : "font-semibold text-slate-800"}`}>{r.label}{r.detail ? <span className="ml-1 text-[10px] font-normal text-slate-400">{r.detail}</span> : null}{r.note ? <div className="text-[10px] font-normal text-slate-400">{r.note}</div> : null}
            <div className="mt-0.5 h-1 w-full max-w-[140px] rounded bg-slate-100"><div className="h-full rounded bg-violet-500" style={{ width: `${Math.min(100, r.pct)}%` }} /></div></td>
          <td className="text-right tabular-nums font-semibold">{Math.round(r.pct)}%</td>
          {ours ? <td className={`text-right tabular-nums ${gap == null ? "" : gap >= 10 ? "text-blue-700" : gap <= -10 ? "text-amber-700" : "text-slate-600"}`}>{r.ours == null ? "—" : `${Math.round(r.ours)}%`}</td> : null}
        </tr>;
      })}</tbody>
    </table>
  </div>;
}
