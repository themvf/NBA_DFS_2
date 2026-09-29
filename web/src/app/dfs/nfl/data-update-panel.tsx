"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { AlertTriangle, CheckCircle2, ExternalLink, Loader2, RefreshCw, XCircle, Clock } from "lucide-react";
import { describeDataAsOf, type DataAsOf, type DataUpdateOutcome } from "@/lib/nfl-dfs/data-update";
import { readNflDataUpdate, startNflDataUpdate, type NflDataUpdateResult } from "./client-actions";

const POLL_MS = 8_000;

/**
 * "Update data": pull fresh injuries, depth charts, projections and DraftKings
 * statuses now, instead of waiting for the schedule. Shows when each input was
 * last captured, follows the runs it started, and hands off to the page when
 * they finish so the slate can move to the new projections.
 */
export default function DataUpdatePanel({ uploadId, asOf, onFinished }: {
  uploadId: string;
  asOf: DataAsOf;
  /** Called once when an update this panel watched finishes: succeeded, failed, or started but not followable. */
  onFinished: (outcome: DataUpdateOutcome) => void | Promise<void>;
}) {
  const [result, setResult] = useState<NflDataUpdateResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const watching = useRef<string | null>(null);
  const onFinishedRef = useRef(onFinished);
  useEffect(() => { onFinishedRef.current = onFinished; }, [onFinished]);

  const apply = useCallback((next: NflDataUpdateResult) => {
    setResult(next);
    const id = next.update?.id ?? null;
    const state = next.view?.state;
    if (state === "running") { watching.current = id; return; }
    // Only an update seen running in this session hands off; a finished one
    // found on load is history, and the Slate Check already says what is stale.
    if (id && watching.current === id && (state === "succeeded" || state === "failed" || state === "untracked")) {
      watching.current = null;
      void onFinishedRef.current(state);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    readNflDataUpdate(uploadId).then((next) => { if (!cancelled) apply(next); })
      .catch((reason) => { if (!cancelled) setError(reason instanceof Error ? reason.message : "Could not check for a data update."); });
    return () => { cancelled = true; };
  }, [uploadId, apply]);

  const running = result?.view?.state === "running";
  useEffect(() => {
    if (!running) return;
    const timer = setInterval(() => {
      readNflDataUpdate(uploadId).then(apply).catch(() => { /* keep polling; the next read reports a lasting failure */ });
    }, POLL_MS);
    return () => clearInterval(timer);
  }, [running, uploadId, apply]);

  async function start() {
    setBusy(true); setError(null);
    try { apply(await startNflDataUpdate(uploadId)); }
    catch (reason) { setError(reason instanceof Error ? reason.message : "The data update could not start."); }
    finally { setBusy(false); }
  }

  const view = result?.view ?? null;
  const recent = view && result?.update && (running || Date.now() - Date.parse(result.update.requestedAt) < 60 * 60 * 1000);
  const blocked = result?.blockedReason ?? null;
  return <section aria-label="Update data" className="rounded-xl border border-slate-200 bg-white p-4">
    <div className="flex flex-wrap items-center gap-3">
      <button type="button" onClick={start} disabled={busy || running || Boolean(blocked)}
        title={blocked ?? "Pull the latest injuries, depth charts, projections and DraftKings statuses now."}
        className="inline-flex items-center gap-2 rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50">
        {busy || running ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
        {running ? "Updating…" : "Update data"}
      </button>
      <p className="min-w-0 flex-1 text-xs text-slate-600"><span className="font-semibold text-slate-700">Data as of:</span> {describeDataAsOf(asOf)}</p>
    </div>
    {blocked && !running ? <p className="mt-2 text-xs text-slate-500">{blocked}</p> : null}
    {error ? <p className="mt-2 text-xs font-semibold text-red-700">{error}</p> : null}
    {recent && view ? <div className="mt-3 rounded-lg bg-slate-50 p-3">
      <p className={`text-sm font-semibold ${view.state === "failed" || view.state === "stuck" ? "text-red-800" : view.state === "succeeded" ? "text-emerald-800" : view.state === "untracked" ? "text-amber-800" : "text-slate-800"}`}>{view.headline}</p>
      <ul className="mt-2 space-y-1.5">{view.lines.map((line) => <li key={line.key} className="flex flex-wrap items-start gap-2 text-xs text-slate-700">
        {line.state === "done" ? <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-600" />
          : line.state === "failed" ? <XCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-red-600" />
          : line.state === "untracked" ? <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600" />
          : line.state === "running" ? <Loader2 className="mt-0.5 h-3.5 w-3.5 shrink-0 animate-spin text-slate-500" />
          : <Clock className="mt-0.5 h-3.5 w-3.5 shrink-0 text-slate-400" />}
        <span className="font-semibold">{line.label}:</span><span className="min-w-0 flex-1">{line.text}</span>
        {line.href ? <a href={line.href} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-slate-500 underline hover:text-slate-800">View run<ExternalLink className="h-3 w-3" /></a> : null}
      </li>)}</ul>
    </div> : null}
  </section>;
}
