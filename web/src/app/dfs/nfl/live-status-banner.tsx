"use client";

import type { NflLiveDkStatus } from "./actions";

/**
 * How stale is the availability you are about to draft against?
 *
 * The salary file answers "what did DraftKings list when I downloaded it".
 * This answers "what does DraftKings list now", which is a different question
 * whenever a slate is uploaded early and drafted later -- the normal case.
 *
 * Two timestamps, deliberately kept apart. `capturedAt` is when DraftKings last
 * CHANGED something; `lastPolledAt` is when we last LOOKED. A feed that reports
 * only the first makes a quiet week indistinguishable from a broken poller.
 */
export default function LiveStatusBanner({ live }: { live?: NflLiveDkStatus }) {
  if (!live) return null;

  const polled = live.lastPolledAt ? new Date(live.lastPolledAt) : null;
  const ageMinutes = polled ? Math.max(0, Math.round((Date.now() - polled.getTime()) / 60000)) : null;
  const age = ageMinutes === null ? null
    : ageMinutes < 60 ? `${ageMinutes} min ago`
    : ageMinutes < 60 * 36 ? `${Math.round(ageMinutes / 60)} h ago`
    : `${Math.round(ageMinutes / 1440)} days ago`;

  if (!live.applied) {
    return <div className="rounded-lg border border-slate-300 bg-slate-50 p-3 text-sm">
      <strong className="font-bold">Live DraftKings status: not applied.</strong>{" "}
      <span className="text-slate-700">{live.reason}</span>{" "}
      <span className="text-slate-500">
        Availability below is whatever the uploaded salary file said.
        {age ? ` Last checked DraftKings ${age}.` : ""}
      </span>
    </div>;
  }

  const outs = live.changes.filter((c) => ["O", "OUT", "IR", "PUP", "SUSP", "NA"].includes(c.to ?? ""));
  const tone = outs.length
    ? "border-red-300 bg-red-50 text-red-900"
    : live.changes.length ? "border-amber-300 bg-amber-50 text-amber-900"
    : "border-emerald-300 bg-emerald-50 text-emerald-900";

  return <div className={`rounded-lg border p-3 text-sm ${tone}`}>
    <strong className="font-bold">
      {live.changes.length === 0
        ? "DraftKings status is unchanged since your salary file."
        : `${live.changes.length} status change${live.changes.length === 1 ? "" : "s"} since your salary file`}
      {outs.length ? ` — ${outs.length} now ruled out.` : "."}
    </strong>{" "}
    <span className="opacity-80">
      Checked {age ?? "at an unknown time"}; matched {live.matched} players
      {live.draftGroupId ? ` (draft group ${live.draftGroupId})` : ""}.
      {live.changes.length ? " Projections and eligibility below already use the newer value." : ""}
    </span>
    {live.changes.length ? <details className="mt-2">
      <summary className="cursor-pointer text-xs font-bold">Show what changed</summary>
      <ul className="mt-1 max-h-48 space-y-0.5 overflow-auto text-xs">
        {live.changes.map((c) => <li key={`${c.name}-${c.to}`}>
          <b>{c.name}</b>{c.team ? ` (${c.team})` : ""}: {c.from ?? "no tag"} → <b>{c.to ?? "no tag"}</b>
        </li>)}
      </ul>
    </details> : null}
    {live.ambiguousNames.length ? <p className="mt-1 text-xs opacity-80">
      {live.ambiguousNames.length} name(s) appear more than once and were left on the uploaded value rather than guessed:{" "}
      {live.ambiguousNames.join(", ")}.
    </p> : null}
  </div>;
}
