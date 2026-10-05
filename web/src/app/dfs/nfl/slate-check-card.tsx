"use client";

import { AlertTriangle, CheckCircle2, Info, XCircle } from "lucide-react";
import type { SlateCheck, SlateCheckAction, SlateCheckItem } from "@/lib/nfl-dfs/slate-check";

const ACTION_LABELS: Record<SlateCheckAction, string> = {
  refresh_projections: "Refresh projections", pick_starter: "Pick the starter",
  retry_capture: "Retry capture", update_data: "Update data",
};

function Notes({ items }: { items: SlateCheckItem[] }) {
  return <ul className="mt-2 space-y-2">{items.map(item => <li key={item.id} className="flex items-start gap-2 text-xs text-slate-600">
    <Info aria-hidden="true" className="mt-0.5 h-3.5 w-3.5 shrink-0" />
    <span className="min-w-0 break-words">{item.text}{item.href ? <> <a href={item.href} target="_blank" rel="noopener noreferrer" className="underline">View run</a></> : null}</span>
  </li>)}</ul>;
}

/** Current build problems first; archive and research evidence remain inspectable. */
export default function SlateCheckCard({ check, pending, onAction }: {
  check: SlateCheck | undefined; pending: boolean;
  onAction: (action: SlateCheckAction, item: SlateCheckItem) => void;
}) {
  if (!check) return null;
  const archived = check.archived ?? check.items.some(item => item.id === 'started');
  const research = check.items.filter(item => item.category === 'research');
  const ordinary = check.items.filter(item => item.category !== 'research');
  const needs = ordinary.filter(item => item.level === 'blocked' || item.level === 'attention');
  const notes = ordinary.filter(item => item.level === 'info');
  const passed = ordinary.filter(item => item.level === 'ok');
  const blocked = needs.some(item => item.level === 'blocked');
  const tone = archived ? 'border-slate-200 bg-slate-50' : check.needs === 0 ? 'border-emerald-200 bg-emerald-50' : blocked ? 'border-red-200 bg-red-50' : 'border-amber-200 bg-amber-50';
  const HeadIcon = archived ? Info : check.needs === 0 ? CheckCircle2 : blocked ? XCircle : AlertTriangle;
  return <section aria-label="Slate check" className={`rounded-xl border p-4 ${tone}`}>
    <h2 className="flex items-center gap-2 font-bold text-slate-900"><HeadIcon aria-hidden="true" className="h-5 w-5 shrink-0" />{check.headline}</h2>
    {archived ? <>
      <Notes items={ordinary.filter(item => item.id === 'started' || item.id === 'special-teams')} />
      <details className="mt-3 text-xs text-slate-600"><summary className="min-h-11 cursor-pointer content-center font-semibold">Saved slate checks</summary>
        <Notes items={ordinary.filter(item => item.id !== 'started' && item.id !== 'special-teams')} />
      </details>
    </> : <>
      {needs.length ? <ul className="mt-3 space-y-2">{needs.map(item => <li key={item.id} className="flex flex-wrap items-start gap-2 rounded-lg bg-white/80 p-2 text-sm">
        {item.level === 'blocked' ? <XCircle aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0 text-red-600" /> : <AlertTriangle aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />}
        <span className="min-w-0 flex-1 break-words text-slate-800">{item.text}{item.href ? <> <a href={item.href} target="_blank" rel="noopener noreferrer" className="font-semibold underline">View run</a></> : null}</span>
        {item.action ? <button type="button" disabled={pending} onClick={() => onAction(item.action!, item)} className="min-h-11 shrink-0 rounded-md border border-slate-300 bg-white px-3 text-xs font-semibold text-slate-800 hover:bg-slate-50 disabled:opacity-50">{ACTION_LABELS[item.action]}</button> : null}
      </li>)}</ul> : null}
      {notes.length ? <Notes items={notes} /> : null}
      {passed.length ? <details className="mt-3 text-xs text-slate-600"><summary className="min-h-11 cursor-pointer content-center font-semibold">{passed.length} check{passed.length === 1 ? '' : 's'} passed</summary><Notes items={passed} /></details> : null}
    </>}
    {research.length ? <details className="mt-3 text-xs text-slate-600"><summary className="min-h-11 cursor-pointer content-center font-semibold">Research diagnostics · does not change this build</summary><Notes items={research} /></details> : null}
  </section>;
}
