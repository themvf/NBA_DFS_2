"use client";

import { useMemo, useRef, useState } from "react";
import { comparePortfolios, type BreakdownLineup, type CountRow, type PortfolioFormat } from "@/lib/nfl-dfs/portfolio-breakdown";
import { loadSavedNflLineups } from "./client-actions";

export interface RunOption { runId: string; createdAt: string; count: number; mode: string }

export const runLabel = (run: RunOption | undefined, runId: string) => run
  ? `${new Date(run.createdAt).toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit" })} ET · ${run.count} lineups · ${run.mode.toUpperCase()}`
  : `Run ${runId.slice(0, 8)}`;

/**
 * Two saved runs of the same slate side by side: captains, splits and the
 * players whose exposure moved most. "This run" is the one loaded on the page;
 * the other is read from its saved record.
 */
export default function CompareRunsPanel({ uploadId, runId, lineups, runs, format, players }: {
  uploadId: string;
  runId: string | null;
  lineups: readonly BreakdownLineup[];
  runs: readonly RunOption[];
  format: PortfolioFormat;
  players: readonly { dkPlayerId: number; captainOwnPct?: number | null }[];
}) {
  const others = runs.filter((r) => r.runId !== runId);
  const [otherId, setOtherId] = useState<string>("");
  const [other, setOther] = useState<{ runId: string; lineups: BreakdownLineup[] } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const latest = useRef("");
  function choose(id: string) {
    setOtherId(id); setOther(null); setError(null); latest.current = id;
    if (!id) return;
    setLoading(true);
    loadSavedNflLineups(uploadId, id)
      .then((saved) => { if (latest.current === id) setOther({ runId: id, lineups: saved.lineups }); })
      .catch((reason) => { if (latest.current === id) setError(reason instanceof Error ? reason.message : "That run could not be loaded."); })
      .finally(() => { if (latest.current === id) setLoading(false); });
  }
  const field = useMemo(() => new Map(players.map((p) => [p.dkPlayerId, p])), [players]);
  const comparison = useMemo(() => other && other.runId === otherId ? comparePortfolios(other.lineups, lineups, format, field) : null,
    [other, otherId, lineups, format, field]);
  const showdown = format === "showdown";
  const otherRun = runs.find((r) => r.runId === otherId);
  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <h2 className="font-bold">Compare with another run</h2>
      <select aria-label="Run to compare" className="rounded border bg-white px-2 py-1 text-xs" value={otherId} disabled={!others.length}
        onChange={(e) => choose(e.target.value)}>
        <option value="">{others.length ? "Choose an earlier or later run" : "No other saved run for this slate"}</option>
        {others.map((r) => <option key={r.runId} value={r.runId}>{runLabel(r, r.runId)}</option>)}
      </select>
    </div>
    {loading ? <p className="mt-2 text-xs text-slate-500">Loading…</p> : null}
    {error ? <p role="alert" className="mt-2 text-xs font-semibold text-red-700">{error}</p> : null}
    {comparison ? <>
      <p className="mt-2 text-xs text-slate-600">
        <b>Before</b>: {runLabel(otherRun, otherId)}. <b>Now</b>: the loaded run, {lineups.length} lineups.
        {" "}{comparison.sharedLineups ? `${comparison.sharedLineups} of the loaded lineups also appear in the other run.` : "No lineup appears in both."}
      </p>
      <div className={`mt-3 grid gap-3 ${showdown ? "md:grid-cols-3" : "md:grid-cols-2"}`}>
        {showdown ? <Side title="Captains" before={comparison.a.captains} after={comparison.b.captains} /> : null}
        <Side title={showdown ? "Team split" : "Stacks"} before={comparison.a.splitKinds} after={comparison.b.splitKinds} />
        <Side title="Quarterbacks" before={comparison.a.quarterbacks} after={comparison.b.quarterbacks} />
      </div>
      <h3 className="mt-3 text-sm font-bold">Biggest exposure changes</h3>
      <div className="mt-1 max-h-72 overflow-auto">
        <table className="w-full text-[11px]">
          <thead className="sticky top-0 bg-white text-left text-slate-500"><tr>
            <th className="py-1">Player</th><th className="text-right">Before</th><th className="text-right">Now</th><th className="text-right">Change</th>
            {showdown ? <><th className="text-right">CPT before</th><th className="text-right">CPT now</th></> : null}
          </tr></thead>
          <tbody>{comparison.players.filter((p) => Math.round(p.pctA) !== Math.round(p.pctB) || Math.round(p.captainPctA) !== Math.round(p.captainPctB)).slice(0, 40).map((p) => {
            const change = p.pctB - p.pctA;
            return <tr key={p.dkPlayerId} className="border-t">
              <td className="py-1"><span className="font-semibold text-slate-800">{p.name}</span> <span className="text-slate-400">{p.position} {p.team}</span></td>
              <td className="text-right tabular-nums text-slate-500">{Math.round(p.pctA)}%</td>
              <td className="text-right tabular-nums font-semibold">{Math.round(p.pctB)}%</td>
              <td className={`text-right tabular-nums font-semibold ${change > 0 ? "text-blue-700" : change < 0 ? "text-amber-700" : "text-slate-400"}`}>{change > 0 ? "+" : ""}{Math.round(change)}</td>
              {showdown ? <><td className="text-right tabular-nums text-slate-500">{Math.round(p.captainPctA)}%</td><td className="text-right tabular-nums">{Math.round(p.captainPctB)}%</td></> : null}
            </tr>;
          })}</tbody>
        </table>
      </div>
    </> : !loading && !error ? <p className="mt-2 text-xs text-slate-500">Pick a run to see how this one differs: captains, splits, and the players you moved most.</p> : null}
  </section>;
}

function Side({ title, before, after }: { title: string; before: CountRow[]; after: CountRow[] }) {
  const keys = [...new Set([...after.map((r) => r.key), ...before.map((r) => r.key)])];
  const byKey = (rows: CountRow[]) => new Map(rows.map((r) => [r.key, r]));
  const b = byKey(before), a = byKey(after);
  return <div className="rounded-lg border p-2">
    <h4 className="text-xs font-bold text-slate-700">{title}</h4>
    <table className="mt-1 w-full text-[11px]">
      <thead className="text-slate-400"><tr><th className="text-left font-normal" /><th className="text-right font-normal">Before</th><th className="text-right font-normal">Now</th></tr></thead>
      <tbody>{keys.slice(0, 12).map((k) => <tr key={k} className="border-t">
        <td className="py-0.5 pr-1 font-semibold text-slate-800">{(a.get(k) ?? b.get(k))!.label}</td>
        <td className="text-right tabular-nums text-slate-500">{b.has(k) ? `${Math.round(b.get(k)!.pct)}%` : "—"}</td>
        <td className="text-right tabular-nums font-semibold">{a.has(k) ? `${Math.round(a.get(k)!.pct)}%` : "—"}</td>
      </tr>)}</tbody>
    </table>
  </div>;
}
