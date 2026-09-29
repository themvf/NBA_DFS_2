"use client";

import type { NflLiveDkStatus } from "./actions";
import { describeLastDkCheck } from "@/lib/nfl-dfs/live-dk-status";

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

  // "Checked" means the last check that WORKED. A failed latest check used to
  // read as "Checked 2 min ago" while the statuses were hours old.
  const { lastGood, failure: failureText } = describeLastDkCheck(live, Date.now());
  const failure = failureText ? <p className="mt-1 text-xs font-semibold text-red-800">{failureText}</p> : null;

  if (!live.applied) {
    return <div className="rounded-lg border border-slate-300 bg-slate-50 p-3 text-sm">
      <strong className="font-bold">Live DraftKings status: not applied.</strong>{" "}
      <span className="text-slate-700">{live.reason}</span>{" "}
      <span className="text-slate-500">
        Availability below is whatever the uploaded salary file said.
        {lastGood ? ` Last successful check of DraftKings ${lastGood}.` : ""}
      </span>
      {failure}
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
        ? "DraftKings status is unchanged since your salary file"
        : `${live.changes.length} status change${live.changes.length === 1 ? "" : "s"} since your salary file`}
      {outs.length ? ` — ${outs.length} now ruled out.` : "."}
    </strong>{" "}
    <span className="opacity-80">
      Last successful check {lastGood ?? "at an unknown time"}; matched {live.matched} players
      {live.draftGroupId ? ` (draft group ${live.draftGroupId})` : ""}.
      {live.changes.length ? " Projections and eligibility below already use the newer value." : ""}
    </span>
    {failure}
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
