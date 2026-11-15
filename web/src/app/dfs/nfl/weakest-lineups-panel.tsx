"use client";

import { useMemo } from "react";
import type { NflGeneratedLineup } from "./nfl-optimizer";
import { weakestLineups } from "@/lib/nfl-dfs/lineup-upgrade";

const dollars = (value: number) => `$${value.toLocaleString()}`;

/**
 * The lowest-projected lineups of this set, each with the player who drags it
 * and a Replace button. Replace swaps one or two players for the legal pair
 * that adds the most projected points and keeps the lineup's shape; the result
 * is saved as a new lineup set, so the one on screen is never lost.
 */
export default function WeakestLineupsPanel({ lineups, disabled, busy, report, onReplace }: {
  lineups: NflGeneratedLineup[];
  disabled: boolean;
  busy: boolean;
  /** What the last Replace did, in sentences. */
  report: string[] | null;
  onReplace: (lineupNumbers: number[]) => void;
}) {
  const weak = useMemo(() => weakestLineups(lineups), [lineups]);
  if (!weak.length && !report) return null;
  return <section aria-label="Weakest lineups" className="rounded-xl border bg-white p-4 shadow-sm">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <div><h2 className="font-bold">Weakest lineups</h2><p className="text-xs text-slate-500">Lowest projected in this set. Replace swaps one or two players for the best legal ones and keeps the lineup&apos;s shape.</p></div>
      {weak.length > 1 ? <button type="button" disabled={disabled || busy} onClick={() => onReplace(weak.map((w) => w.lineupNumber))} className="min-h-10 rounded-lg border border-slate-700 bg-white px-3 text-xs font-bold disabled:opacity-40">{busy ? "Working…" : `Replace all ${weak.length}`}</button> : null}
    </div>
    {report ? <ul role="status" className="mt-3 list-disc space-y-1 rounded-lg border border-emerald-200 bg-emerald-50 p-3 pl-7 text-xs text-emerald-950">{report.map((line) => <li key={line}>{line}</li>)}</ul> : null}
    {weak.length ? <ul className="mt-3 divide-y text-sm">{weak.map((w) => <li key={w.lineupNumber} className="flex flex-wrap items-center justify-between gap-3 py-2">
      <span className="min-w-0"><b>Lineup {w.lineupNumber}</b> · {w.projectedFpts.toFixed(1)} projected, {w.belowMedian.toFixed(1)} under the middle lineup
        <span className="block text-xs text-slate-600">Thinnest spot: {w.weakestPlayer.name} ({w.weakestPlayer.slot.replace(/[0-9]+$/, "")}, {dollars(w.weakestPlayer.salary)}, {w.weakestPlayer.projection.toFixed(1)} points){w.salaryLeft >= 500 ? ` · ${dollars(w.salaryLeft)} unspent` : ""}</span></span>
      <button type="button" disabled={disabled || busy} onClick={() => onReplace([w.lineupNumber])} className="min-h-10 shrink-0 rounded-lg border bg-white px-3 text-xs font-bold disabled:opacity-40">Replace</button>
    </li>)}</ul> : null}
  </section>;
}
