"use client";

import { useMemo, useState } from "react";
import { WTA, optimizeContest, type ContestPlayer, type OptimizerResult } from "@/lib/nba-contest-optimizer";
import { sharePct } from "@/lib/nba-contest-trios";

/** `started` is decided on the server, so server and browser render the same toggles. */
export type OptimizerGame = { eventId: string; game: string; commenceTime: string; players: number; started: boolean };

const tip = (s: string) => new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }).format(new Date(s));

/** Players by how many of the entries they are in, most first. */
function exposure(run: OptimizerResult): [string, number][] {
  const n = new Map<string, number>();
  for (const e of run.entries) for (const k of e.players) n.set(k, (n.get(k) ?? 0) + 1);
  return [...n.entries()].sort((a, b) => b[1] - a[1]);
}

export function ContestOptimizer({ players, games, capturedAt }: { players: ContestPlayer[]; games: OptimizerGame[]; capturedAt: string | null }) {
  const started = useMemo(() => new Set(games.filter((g) => g.started).map((g) => g.eventId)), [games]);
  const [selected, setSelected] = useState<Set<string>>(() => new Set(games.filter((g) => !started.has(g.eventId)).map((g) => g.eventId)));
  const [fieldSize, setFieldSize] = useState<number>(WTA.FIELD_SIZE);
  const [entries, setEntries] = useState<number>(5);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<{ run: OptimizerResult; games: string[] } | null>(null);

  const byKey = new Map(players.map((p) => [p.key, p]));
  const allSelected = games.length > 0 && games.every((g) => selected.has(g.eventId));
  const toggle = (id: string) => setSelected((prev) => {
    const next = new Set(prev);
    if (next.has(id)) next.delete(id); else next.add(id);
    return next;
  });

  const run = () => {
    setBusy(true);
    setError(null);
    // Let the button repaint before the ~0.3 s of simulation blocks the thread.
    setTimeout(() => {
      try {
        const chosen = players.filter((p) => selected.has(p.eventId));
        const out = optimizeContest(chosen, { fieldSize, entries });
        setResult({ run: out, games: games.filter((g) => selected.has(g.eventId)).map((g) => g.game) });
      } catch (e) {
        setResult(null);
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(false);
      }
    }, 20);
  };

  if (games.length === 0) {
    return (
      <section className="rounded-xl border bg-white p-4 shadow-sm">
        <h2 className="font-bold">Build entries for your contest</h2>
        <p className="mt-1 text-sm text-slate-600">No lines captured for this date yet. Lines arrive after the 11:07 AM ET capture.</p>
      </section>
    );
  }

  const last = result?.run.entries.at(-1);
  const baseLast = result?.run.baseline.at(-1);

  return (
    <section className="space-y-3 rounded-xl border bg-white p-4 shadow-sm">
      <div>
        <h2 className="font-bold">Build entries for your contest</h2>
        <p className="text-xs text-muted-foreground">
          Untick games your contest leaves out, set its size, then Optimize. Same rules as the entries below, run in your browser on the latest
          lines{capturedAt ? ` (captured ${new Date(capturedAt).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" })} ET)` : ""}.
          Custom runs are not tracked. One difference: each player&apos;s big nights are made rarer to match what has happened (the
          books&apos; alternate lines overstate them); the tracked entries below keep the original rules.
        </p>
      </div>

      <div className="flex flex-wrap gap-2">
        {games.map((g) => {
          const on = selected.has(g.eventId), live = started.has(g.eventId);
          return (
            <label key={g.eventId}
              className={`flex cursor-pointer items-center gap-2 rounded-lg border px-3 py-2 text-sm ${on ? "border-slate-900 bg-slate-50" : "border-slate-200 text-slate-400"}`}>
              <input type="checkbox" className="h-4 w-4" checked={on} onChange={() => toggle(g.eventId)} />
              <span>{g.game}</span>
              <span className="font-mono text-xs text-slate-500">{live ? "started" : tip(g.commenceTime)}</span>
            </label>
          );
        })}
      </div>

      <div className="flex flex-wrap items-end gap-4 text-sm">
        <div className="flex gap-2">
          <button type="button" className="rounded border px-2 py-1 text-xs hover:bg-slate-100" onClick={() => setSelected(new Set(games.map((g) => g.eventId)))}>All games</button>
          <button type="button" className="rounded border px-2 py-1 text-xs hover:bg-slate-100" onClick={() => setSelected(new Set())}>None</button>
        </div>
        <label className="flex flex-col text-xs text-slate-600">Entries in the contest
          <input type="number" min={20} max={100000} step={10} value={fieldSize}
            onChange={(e) => setFieldSize(Math.max(20, Math.min(100000, Math.round(Number(e.target.value) || WTA.FIELD_SIZE))))}
            className="mt-1 w-28 rounded border px-2 py-1 font-mono text-sm text-slate-900" />
        </label>
        <label className="flex flex-col text-xs text-slate-600">Your entries
          <input type="number" min={1} max={20} value={entries}
            onChange={(e) => setEntries(Math.max(1, Math.min(20, Math.round(Number(e.target.value) || 5))))}
            className="mt-1 w-20 rounded border px-2 py-1 font-mono text-sm text-slate-900" />
        </label>
        <button type="button" onClick={run} disabled={busy || selected.size === 0}
          className="rounded-lg bg-slate-900 px-4 py-2 font-semibold text-white disabled:cursor-not-allowed disabled:bg-slate-400">
          {busy ? "Optimizing..." : "Optimize"}
        </button>
      </div>

      {error && <p className="text-sm text-rose-700">{error}</p>}

      {result && last && baseLast && (
        <div className="space-y-2 border-t pt-3">
          <p className="text-sm">
            <b>{result.games.length} game{result.games.length === 1 ? "" : "s"}</b>, field of {result.run.fieldSize}: these {result.run.entries.length} entries
            win <b>{sharePct(last.portfolioShare)}</b> of the prize in the model, against {sharePct(baseLast.portfolioShare)} for the {result.run.baseline.length} biggest-line
            trios. Likely winning score {result.run.winningScore.median.toFixed(0)} (middle half {result.run.winningScore.p25.toFixed(0)}-{result.run.winningScore.p75.toFixed(0)}).
          </p>
          {allSelected && (
            <p className="text-xs text-muted-foreground">All of tonight&apos;s games: the frozen entries below are the tracked set. They use the original simulation, so they can differ from this run.</p>
          )}
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="text-left text-[10px] uppercase text-slate-500">
                <th>#</th><th>Players (line)</th><th className="text-right">Line sum</th><th className="text-right">Wins alone</th><th className="text-right">Adds</th>
              </tr></thead>
              <tbody>{result.run.entries.map((e) => (
                <tr key={e.players.join("|")} className="border-t">
                  <td className="py-2 pr-2 font-mono text-xs">{e.rank}</td>
                  <td className="py-2 pr-3">
                    <div className="flex flex-wrap gap-x-3 gap-y-1">{e.players.map((k) => {
                      const p = byKey.get(k);
                      return (
                        <span key={k} className="whitespace-nowrap">
                          {p?.player ?? k} <span className="font-mono text-xs text-slate-500">{p?.line.toFixed(1)}</span>
                          {p?.books === 1 && <span className="ml-1 rounded border border-orange-400 px-1 font-mono text-[10px] text-orange-700">1 BOOK</span>}
                        </span>
                      );
                    })}</div>
                  </td>
                  <td className="pr-3 text-right font-mono">{e.lineSum.toFixed(1)}</td>
                  <td className="pr-3 text-right font-mono">{sharePct(e.standaloneShare)}</td>
                  <td className="text-right font-mono">{sharePct(e.marginalShare)}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
          <p className="text-sm">
            <span className="text-xs uppercase text-slate-500">In your entries: </span>
            {exposure(result.run).map(([k, n]) => `${byKey.get(k)?.player ?? k} ${n}`).join(" · ")}
          </p>
          <p className="text-xs text-muted-foreground">
            Many trios are nearly as good as each other, so the exact entries change with the random draws while the prize share and this mix
            of players hold steady. Swapping an entry for a close equivalent costs little; adding the three biggest lines costs a lot.
          </p>
          <p className="text-xs text-muted-foreground">
            Pool: the {result.run.pool.length} biggest lines in the chosen games. The field is a guess (10% on the three biggest lines, the rest leaning to big lines).
          </p>
        </div>
      )}
    </section>
  );
}
