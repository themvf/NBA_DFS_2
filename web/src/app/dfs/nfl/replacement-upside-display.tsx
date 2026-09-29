"use client";

import type { ReplacementUpside } from "@/lib/nfl-dfs/replacement-upside";

/*
 * Replacement upside, shown BESIDE the baseline and never instead of it
 * (`nfl-replacement-upside-v2`, display only). Violet is the only colour this
 * adds: it marks "a second range", not a better or worse player, so it stays
 * off the green/red trust colours the rest of the workspace uses.
 */

const one = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(1));
const pct = (v: number | null | undefined) => (v == null ? "—" : `${Math.round(v * 100)}%`);

export function upsideTitle(u: ReplacementUpside): string {
  return `${u.note} If he gets the job: ${one(u.ifStarterRole.mean)} projected, ${one(u.ifStarterRole.p90)} ceiling, `
    + `${pct(u.ifStarterRole.boom)} boom. Display only: the optimizer still uses the baseline.`;
}

/** Name-cell chip: says why the row has a second line. */
export function ReplacementUpsideChip({ upside }: { upside: ReplacementUpside }) {
  return (
    <span title={upsideTitle(upside)}
      className="ml-2 inline-flex items-center rounded-full border border-violet-300 bg-violet-50 px-2 py-0.5 align-middle text-[10px] font-bold text-violet-800">
      {upside.from.name} out · {Math.round(upside.pi * 100)}% job
    </span>
  );
}

/** Name-cell chip for a player behind a ruled-out starter whose role gets no adjustment. */
export function UpsideUnchangedChip({ from, reason }: { from: string; reason: string }) {
  return (
    <span title={reason}
      className="ml-2 inline-flex items-center rounded-full border border-slate-300 bg-slate-50 px-2 py-0.5 align-middle text-[10px] font-semibold text-slate-600">
      {from} out · no change
    </span>
  );
}

/** Second line under a pool-table number: the same field if he gets the job. */
export function UpsideLine({ upside, field }: { upside: ReplacementUpside | null | undefined; field: "mean" | "p10" | "p90" }) {
  if (!upside) return null;
  return (
    <div className="text-[10px] font-semibold text-violet-700" title={upsideTitle(upside)}>
      if job {one(upside.ifStarterRole[field])}
    </div>
  );
}

/** Explanation-panel section: baseline and if-he-gets-the-job side by side. */
export function ReplacementUpsideTable({ upside }: { upside: ReplacementUpside }) {
  const rows: { label: string; key: "mean" | "p10" | "median" | "p90" | "boom" }[] = [
    { label: "Projection (mean)", key: "mean" },
    { label: "Floor (P10)", key: "p10" },
    { label: "Median", key: "median" },
    { label: "Ceiling (P90)", key: "p90" },
    { label: "Boom rate", key: "boom" },
  ];
  const show = (key: typeof rows[number]["key"], v: number | null) => (key === "boom" ? pct(v) : one(v));
  return (
    <div>
      <p className="text-xs text-slate-700">{upside.note}</p>
      <table className="mt-2 w-full text-xs">
        <thead>
          <tr className="text-left text-[10px] uppercase tracking-wide text-slate-500">
            <th className="py-1 font-bold">DK points</th>
            <th className="py-1 text-right font-bold">Baseline</th>
            <th className="py-1 text-right font-bold text-violet-800">If he gets the job</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(({ label, key }) => (
            <tr key={key} className="border-t border-slate-100">
              <td className="py-1 text-slate-700">{label}</td>
              <td className="py-1 text-right font-semibold text-slate-900">{show(key, upside.baseline[key])}</td>
              <td className="py-1 text-right font-bold text-violet-800">{show(key, upside.ifStarterRole[key])}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-2 text-[11px] leading-snug text-slate-500">
        {Math.round(upside.pi * 100)}% of simulated games use {upside.from.name}&apos;s projected range, the rest use this
        player&apos;s own. The chance by role is a stated prior fitted on 2020–25 and rechecked on 2014–18, not yet graded on
        2026 slates. Display only: projections, the optimizer and ownership keep the baseline. {upside.version}
      </p>
    </div>
  );
}
