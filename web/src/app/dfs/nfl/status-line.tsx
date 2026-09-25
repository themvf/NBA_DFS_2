"use client";

import { useState } from "react";
import type { StatusItem } from "@/lib/nfl-dfs/workspace-stage";

const TONE = {
  danger: "border-red-300 bg-red-50 text-red-900",
  warning: "border-amber-300 bg-amber-50 text-amber-900",
  info: "border-slate-200 bg-white text-slate-700",
} as const;

/**
 * One status line instead of a stack of notices. The most urgent item shows;
 * the rest fold behind "N more". Items are already prioritized and filtered
 * for lock by `prioritizeStatus`.
 */
export default function StatusLine({ items, onAction }: {
  items: StatusItem[];
  onAction: (target: NonNullable<StatusItem["action"]>["target"]) => void;
}) {
  const [open, setOpen] = useState(false);
  if (!items.length) return null;
  const [first, ...rest] = items;
  const row = (item: StatusItem) => <div key={item.id} className={`flex flex-wrap items-center justify-between gap-2 rounded-lg border px-3 py-2 text-sm ${TONE[item.tone]}`}>
    <span>{item.text}</span>
    {item.action ? <button type="button" onClick={() => onAction(item.action!.target)}
      className="shrink-0 rounded border border-current px-2 py-0.5 text-xs font-semibold">{item.action.label}</button> : null}
  </div>;
  return <section aria-label="Slate status" className="space-y-1.5">
    {row(first)}
    {rest.length ? <button type="button" onClick={() => setOpen((v) => !v)} className="text-xs font-semibold text-slate-600 underline">
      {open ? "Hide" : `${rest.length} more`}
    </button> : null}
    {open ? rest.map(row) : null}
  </section>;
}
