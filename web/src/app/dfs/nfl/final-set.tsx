"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Download } from "lucide-react";
import type { NflGeneratedLineup } from "./nfl-optimizer";
import { exportNflLineupPool, loadSavedNflLineups } from "./client-actions";
import { addToPool, MAX_POOL_LINEUPS, parsePool, pickKey, poolOverlap, poolStorageKey, removeFromPool, type PoolPick } from "@/lib/nfl-dfs/lineup-pool";
import { rosterKey, type PortfolioFormat } from "@/lib/nfl-dfs/portfolio-breakdown";
import type { QaOverride } from "@/lib/nfl-dfs/pre-export-qa";
import LineupReview from "./lineup-review";
import ExposurePanel from "./exposure-panel";
import PairsPanel from "./pairs-panel";
import { runLabel, type RunOption } from "./compare-runs-panel";

export interface LineupPool {
  picks: PoolPick[];
  /** Saved lineups by run id, for every run the pool or the page has read. */
  cache: ReadonlyMap<string, NflGeneratedLineup[]>;
  loading: boolean;
  error: string | null;
  /** Remember a run's lineups without a server read (the run loaded on the page). */
  prime: (runId: string, lineups: NflGeneratedLineup[]) => void;
  add: (runId: string, lineups: readonly NflGeneratedLineup[]) => { added: number; duplicates: number; overLimit: number };
  remove: (picks: readonly PoolPick[]) => void;
  clear: () => void;
  has: (runId: string, lineupNumber: number) => boolean;
}

/** The final set for one saved slate, kept in this browser. */
export function useLineupPool(uploadId: string | null): LineupPool {
  const [picks, setPicks] = useState<PoolPick[]>([]);
  const [loadedFor, setLoadedFor] = useState<string | null>(null);
  const [cache, setCache] = useState<Map<string, NflGeneratedLineup[]>>(new Map());
  const [failed, setFailed] = useState<ReadonlySet<string>>(new Set());
  const requested = useRef(new Set<string>());

  // A different slate: read its stored picks (state adjusted during render, not in an effect).
  if (uploadId && loadedFor !== uploadId) {
    let stored: string | null = null;
    try { stored = localStorage.getItem(poolStorageKey(uploadId)); } catch { /* No storage: the pool lives for this page only. */ }
    setLoadedFor(uploadId); setPicks(parsePool(stored)); setCache(new Map()); setFailed(new Set());
  }
  useEffect(() => { requested.current = new Set(); }, [uploadId]);
  useEffect(() => {
    if (!uploadId || loadedFor !== uploadId) return;
    try { localStorage.setItem(poolStorageKey(uploadId), JSON.stringify(picks)); } catch { /* Storage full or blocked. */ }
  }, [uploadId, loadedFor, picks]);

  // Read every run the pool refers to that this page has not seen yet.
  const missing = useMemo(() => [...new Set(picks.map((p) => p.runId))].filter((id) => !cache.has(id) && !failed.has(id)), [picks, cache, failed]);
  useEffect(() => {
    if (!uploadId || loadedFor !== uploadId) return;
    const toRead = missing.filter((id) => !requested.current.has(id));
    if (!toRead.length) return;
    toRead.forEach((id) => requested.current.add(id));
    let live = true;
    Promise.allSettled(toRead.map((id) => loadSavedNflLineups(uploadId, id).then((saved) => [id, saved.lineups] as const)))
      .then((results) => {
        if (!live) return;
        const read = results.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
        const bad = toRead.filter((_, i) => results[i].status === "rejected");
        if (read.length) setCache((current) => { const next = new Map(current); read.forEach(([id, l]) => next.set(id, l)); return next; });
        if (bad.length) setFailed((current) => new Set([...current, ...bad]));
      });
    return () => { live = false; toRead.forEach((id) => requested.current.delete(id)); };
  }, [uploadId, loadedFor, missing]);
  const loading = missing.length > 0;
  const error = failed.size ? `Run${failed.size === 1 ? "" : "s"} ${[...failed].map((id) => id.slice(0, 8)).join(", ")} could not be read; ${failed.size === 1 ? "its" : "their"} lineups are not shown, but the export still checks them.` : null;

  const prime = useCallback((runId: string, lineups: NflGeneratedLineup[]) => {
    setCache((current) => (current.get(runId) === lineups ? current : new Map(current).set(runId, lineups)));
  }, []);
  const rosterOf = useCallback((pick: PoolPick) => {
    const lineup = cache.get(pick.runId)?.find((l) => l.lineupNumber === pick.lineupNumber);
    return lineup ? rosterKey(lineup) : null;
  }, [cache]);
  const add = useCallback((runId: string, lineups: readonly NflGeneratedLineup[]) => {
    const result = addToPool(picks, runId, lineups, rosterOf);
    setPicks(result.picks);
    return result;
  }, [picks, rosterOf]);
  const keys = useMemo(() => new Set(picks.map(pickKey)), [picks]);
  return {
    picks, cache, loading, error, prime, add,
    remove: (remove) => setPicks((current) => removeFromPool(current, remove)),
    clear: () => setPicks([]),
    has: (runId, lineupNumber) => keys.has(pickKey({ runId, lineupNumber })),
  };
}

function downloadText(name: string, content: string) {
  const url = URL.createObjectURL(new Blob([content], { type: "text/csv" }));
  const a = document.createElement("a"); a.href = url; a.download = name; a.click();
  URL.revokeObjectURL(url);
}

/**
 * The final set: lineups chosen from any saved runs of this slate, reviewed
 * together and exported as one file. The server re-reads every lineup and runs
 * each source run's own pre-export checks before writing it.
 */
export default function FinalSetView({ pool, uploadId, runs, format, players, minUnique, entryFile, onChooseEntryFile, locked, overridesByRun, onCaps }: {
  pool: LineupPool;
  uploadId: string;
  runs: readonly RunOption[];
  format: PortfolioFormat;
  players: Parameters<typeof ExposurePanel>[0]["players"];
  /** The loaded build's min-unique, for the near-duplicate count. */
  minUnique: number;
  entryFile: File | null;
  onChooseEntryFile: () => void;
  locked: boolean;
  overridesByRun: Record<string, QaOverride[]>;
  onCaps?: Parameters<typeof ExposurePanel>[0]["caps"];
}) {
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<{ ok: boolean; text: string } | null>(null);
  const resolved = useMemo(() => pool.picks.flatMap((pick) => {
    const lineup = pool.cache.get(pick.runId)?.find((l) => l.lineupNumber === pick.lineupNumber);
    return lineup ? [{ pick, lineup }] : [];
  }), [pool.picks, pool.cache]);
  const lineups = useMemo(() => resolved.map(({ lineup }, i) => ({ ...lineup, lineupNumber: i + 1 })), [resolved]);
  const pickByNumber = useMemo(() => new Map(resolved.map(({ pick }, i) => [i + 1, pick])), [resolved]);
  const runById = useMemo(() => new Map(runs.map((r) => [r.runId, r])), [runs]);
  const perRun = useMemo(() => {
    const counts = new Map<string, number>();
    for (const p of pool.picks) counts.set(p.runId, (counts.get(p.runId) ?? 0) + 1);
    return [...counts];
  }, [pool.picks]);
  const runIndex = useMemo(() => new Map(perRun.map(([id], i) => [id, i + 1])), [perRun]);
  const rosterSize = format === "showdown" ? 6 : 9;
  const overlap = useMemo(() => poolOverlap(lineups, rosterSize - 1), [lineups, rosterSize]);
  const unresolved = pool.picks.length - resolved.length;

  function exportPool(withEntries: boolean) {
    if (withEntries && !entryFile) { onChooseEntryFile(); return; }
    setBusy(true); setStatus(null);
    (async () => {
      try {
        const template = withEntries && entryFile ? await entryFile.text() : null;
        const result = await exportNflLineupPool(uploadId, pool.picks, template, overridesByRun);
        downloadText(`nfl-final-set-${uploadId.slice(0, 8)}${withEntries ? "" : "-rosters"}.csv`, result.csv);
        setStatus({ ok: true, text: `Exported ${result.lineups} lineups from ${result.runs} run${result.runs === 1 ? "" : "s"}${withEntries ? " into the entry file" : ""}.` });
      } catch (reason) {
        setStatus({ ok: false, text: reason instanceof Error ? reason.message : "The final set could not be exported." });
      } finally { setBusy(false); }
    })();
  }

  if (!pool.picks.length) return <section className="rounded-xl border border-dashed bg-white p-6">
    <h2 className="font-semibold">Your final set is empty</h2>
    <p className="mt-2 max-w-2xl text-sm text-slate-600">Build several runs with different settings. In each run&apos;s lineup list, add the lineups you like (one at a time, or every lineup a filter shows). They collect here, from any number of runs, and export together as one file.</p>
  </section>;

  return <div className="space-y-3">
    <section className="rounded-xl border bg-white p-4 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="font-bold">Final set · {pool.picks.length} lineup{pool.picks.length === 1 ? "" : "s"} from {perRun.length} run{perRun.length === 1 ? "" : "s"}</h2>
          <p className="mt-1 text-xs text-slate-500">Kept in this browser for this slate. Up to {MAX_POOL_LINEUPS}. The export re-reads every lineup and runs each source run&apos;s pre-export checks.</p>
        </div>
        <button type="button" onClick={() => { if (window.confirm(`Remove all ${pool.picks.length} lineups from the final set?`)) pool.clear(); }} className="rounded border px-2 py-1 text-xs text-slate-600">Clear final set</button>
      </div>
      <ul className="mt-2 space-y-1 text-xs">
        {perRun.map(([id, count]) => <li key={id} className="flex flex-wrap items-center gap-2">
          <span className="rounded bg-slate-100 px-1.5 font-bold text-slate-700">R{runIndex.get(id)}</span>
          <span className="text-slate-700">{runLabel(runById.get(id), id)}</span>
          <span className="font-semibold">{count} picked</span>
          <button type="button" className="text-blue-700 underline" onClick={() => pool.remove(pool.picks.filter((p) => p.runId === id))}>remove</button>
        </li>)}
      </ul>
      {pool.loading ? <p className="mt-2 text-xs text-slate-500">Reading saved runs…</p> : null}
      {pool.error ? <p role="alert" className="mt-2 text-xs font-semibold text-red-700">{pool.error}</p> : null}
      {unresolved && !pool.loading ? <p className="mt-2 text-xs font-semibold text-amber-800">{unresolved} picked lineup{unresolved === 1 ? " is" : "s are"} not loaded and not shown below; the export still checks and includes {unresolved === 1 ? "it" : "them"}.</p> : null}
      <p className={`mt-2 text-xs ${overlap.closePairs ? "font-semibold text-amber-800" : "text-slate-600"}`}>
        {lineups.length < 2 ? "Add more lineups to measure overlap." : overlap.closePairs
          ? `${overlap.closePairs} pair${overlap.closePairs === 1 ? "" : "s"} of lineups differ by only one player (most shared: ${overlap.maxShared} of ${rosterSize}). Lineups from different runs are not checked against each other's min-unique (${minUnique}).`
          : `No two lineups differ by only one player; the most any two share is ${overlap.maxShared} of ${rosterSize}.`}
      </p>
      <div className="mt-3 grid gap-2 sm:grid-cols-3">
        <button type="button" onClick={onChooseEntryFile} className="min-h-10 rounded-lg border text-sm font-bold">{entryFile?.name ?? "Select entry template"}</button>
        <button type="button" disabled={busy || locked || !entryFile} onClick={() => exportPool(true)} className="inline-flex min-h-10 items-center justify-center gap-2 rounded-lg bg-blue-600 text-sm font-bold text-white disabled:opacity-40"><Download className="h-4 w-4" />{locked ? "Games have started" : "Export final set into entry file"}</button>
        <button type="button" disabled={busy || locked} onClick={() => exportPool(false)} className="inline-flex min-h-10 items-center justify-center gap-2 rounded-lg border border-blue-600 bg-white text-sm font-bold text-blue-700 disabled:opacity-40"><Download className="h-4 w-4" />{locked ? "Games have started" : "Export final set CSV (no entry file)"}</button>
      </div>
      {busy ? <p className="mt-2 text-xs text-slate-500">Checking every source run and writing the file…</p> : null}
      {status ? <p role={status.ok ? "status" : "alert"} className={`mt-2 rounded border p-2 text-xs font-semibold ${status.ok ? "border-emerald-300 bg-emerald-50 text-emerald-900" : "border-red-300 bg-red-50 text-red-800"}`}>{status.text}</p> : null}
    </section>
    <ExposurePanel title="Final set exposure" lineups={lineups} players={players} format={format} caps={onCaps} />
    <PairsPanel lineups={lineups} format={format} />
    <LineupReview lineups={lineups} runId={null} format={format} players={players}
      title={`Final set · ${lineups.length} lineups`}
      subtitle="Numbered in export order. The tag shows which run each lineup came from."
      lineupTag={(n) => { const p = pickByNumber.get(n); return p ? `R${runIndex.get(p.runId)} #${p.lineupNumber}` : null; }}
      pickControl={{
        isPicked: () => true,
        toggle: (n) => { const p = pickByNumber.get(n); if (p) pool.remove([p]); },
        pickedLabel: "Remove",
        bulk: [{ label: "Remove the lineups shown", run: (shown) => pool.remove(shown.flatMap((l) => { const p = pickByNumber.get(l.lineupNumber); return p ? [p] : []; })) }],
      }} />
  </div>;
}
