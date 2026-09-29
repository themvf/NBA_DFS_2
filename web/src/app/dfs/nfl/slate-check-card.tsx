"use client";

import { AlertTriangle, CheckCircle2, Info, XCircle } from "lucide-react";
import type { SlateCheck, SlateCheckAction, SlateCheckItem } from "@/lib/nfl-dfs/slate-check";

const ACTION_LABELS: Record<SlateCheckAction, string> = {
  refresh_projections: "Refresh projections",
  pick_starter: "Pick the starter",
};

/**
 * The Slate Check: what needs you first, in plain words with a fix button;
 * notes below; everything that passed folded away. Computed on the server
 * from the same evidence the build uses (see lib/nfl-dfs/slate-check).
 */
export default function SlateCheckCard({ check, pending, onAction }: {
  check: SlateCheck | undefined;
  pending: boolean;
  onAction: (action: SlateCheckAction, item: SlateCheckItem) => void;
}) {
  if (!check) return null;
  const needs = check.items.filter((i) => i.level === "blocked" || i.level === "attention");
  const notes = check.items.filter((i) => i.level === "info");
  const passed = check.items.filter((i) => i.level === "ok");
  const blocked = needs.some((i) => i.level === "blocked");
  const tone = check.needs === 0 ? "border-emerald-200 bg-emerald-50" : blocked ? "border-red-200 bg-red-50" : "border-amber-200 bg-amber-50";
  const HeadIcon = check.needs === 0 ? CheckCircle2 : blocked ? XCircle : AlertTriangle;
  return <section aria-label="Slate check" className={`rounded-xl border p-4 ${tone}`}>
    <h2 className="flex items-center gap-2 font-bold text-slate-900"><HeadIcon className="h-5 w-5 shrink-0" />{check.headline}</h2>
    {check.needs > 0 ? <ul className="mt-3 space-y-2">
      {needs.map((item) => <li key={item.id} className="flex flex-wrap items-start gap-2 rounded-lg bg-white/80 p-2 text-sm">
        {item.level === "blocked" ? <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-red-600" /> : <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />}
        <span className="min-w-0 flex-1 text-slate-800">{item.text}{item.href ? <> <a href={item.href} target="_blank" rel="noopener noreferrer" className="font-semibold underline">View run</a></> : null}</span>
        {item.action ? <button type="button" disabled={pending} onClick={() => onAction(item.action!, item)}
          className="shrink-0 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-semibold text-slate-800 hover:bg-slate-50 disabled:opacity-50">{ACTION_LABELS[item.action]}</button> : null}
      </li>)}
    </ul> : null}
    {check.needs === 0 && needs.length ? <ul className="mt-3 space-y-1 text-xs text-slate-600">{needs.map((item) => <li key={item.id}>{item.text}</li>)}</ul> : null}
    {notes.length ? <ul className="mt-3 space-y-1">{notes.map((item) => <li key={item.id} className="flex items-start gap-2 text-xs text-slate-600">
      <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" /><span>{item.text}{item.href ? <> <a href={item.href} target="_blank" rel="noopener noreferrer" className="underline">View run</a></> : null}</span></li>)}</ul> : null}
    {passed.length ? <details className="mt-3 text-xs text-slate-600"><summary className="cursor-pointer font-semibold">{passed.length} check{passed.length === 1 ? "" : "s"} passed</summary>
      <ul className="mt-2 space-y-1">{passed.map((item) => <li key={item.id} className="flex items-start gap-2">
        <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-600" /><span>{item.text}</span></li>)}</ul>
    </details> : null}
  </section>;
}
