"use client";

import { AlertTriangle, CheckCircle2 } from "lucide-react";

export type ReviewProblem = {
  id: string;
  /** "stop": worth fixing before you enter. "check": look, then decide. */
  tone: "stop" | "check";
  title: string;
  detail: string;
  fix?: { label: string; run: () => void };
};

/**
 * The first thing on Review (owner, 2026-11-15): is anything wrong with this
 * set, and what fixes it. Nothing here stops the export; it only says what to
 * look at, worst first, with the button that deals with it.
 */
export default function ReviewProblems({ problems, lineupCount, disabled }: { problems: ReviewProblem[]; lineupCount: number; disabled: boolean }) {
  const sorted = [...problems].sort((a, b) => Number(b.tone === "stop") - Number(a.tone === "stop"));
  if (!sorted.length) return <section aria-label="Is anything wrong" className="flex items-center gap-2 rounded-xl border border-emerald-300 bg-emerald-50 p-4 text-sm font-semibold text-emerald-950">
    <CheckCircle2 className="h-5 w-5 shrink-0" />Nothing to fix. {lineupCount} lineup{lineupCount === 1 ? "" : "s"} ready to export.
  </section>;
  const stops = sorted.filter((p) => p.tone === "stop").length;
  return <section aria-label="Is anything wrong" className="rounded-xl border bg-white p-4 shadow-sm">
    <h2 className="font-bold">{stops ? `${stops} thing${stops === 1 ? "" : "s"} to fix before you enter` : "Worth a look before you enter"}{sorted.length > stops && stops ? `, ${sorted.length - stops} to look at` : ""}</h2>
    <ul className="mt-3 space-y-2">{sorted.map((p) => <li key={p.id} className={`flex flex-wrap items-start justify-between gap-3 rounded-lg border p-3 text-sm ${p.tone === "stop" ? "border-red-300 bg-red-50" : "border-amber-200 bg-amber-50"}`}>
      <span className="flex min-w-0 flex-1 items-start gap-2"><AlertTriangle className={`mt-0.5 h-4 w-4 shrink-0 ${p.tone === "stop" ? "text-red-700" : "text-amber-700"}`} />
        <span className="min-w-0"><b>{p.title}</b><span className="mt-0.5 block text-xs text-slate-700">{p.detail}</span></span></span>
      {p.fix ? <button type="button" disabled={disabled} onClick={p.fix.run} className="min-h-10 shrink-0 rounded-lg border border-slate-700 bg-white px-3 text-xs font-bold disabled:opacity-40">{p.fix.label}</button> : null}
    </li>)}</ul>
    <p className="mt-2 text-[11px] text-slate-500">None of these stops the export.</p>
  </section>;
}
