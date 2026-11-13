"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { RECAL_VERSION, WTA, optimizeContest, type ContestPlayer, type OptimizerResult } from "@/lib/nba-contest-optimizer";
import { runRequest } from "@/lib/nba-contest-runs";
import { sharePct } from "@/lib/nba-contest-trios";

/** `started` is decided on the server, so server and browser render the same toggles. */
export type OptimizerGame = { eventId: string; game: string; commenceTime: string; players: number; started: boolean };

/** What happened to the save of the run on screen. A failure is shown, never swallowed. */
type SaveState =
  | { state: "saving" }
  | { state: "saved"; createdAt: string; late: boolean; repeat: boolean; firstTip: string }
  | { state: "failed"; error: string };

/** Lines older than this at page load get a warning: sitting chances rise once lines are read more than 90 minutes before a tip. */
const STALE_LINES_MINUTES = 60;
/** A player at or above this sitting chance is tagged in the entries. */
const SIT_TAG_AT = 0.015;
const pct1 = (v: number) => `${(v * 100).toFixed(v < 0.1 ? 1 : 0)}%`;
const age = (minutes: number) => (minutes < 120 ? `${minutes} minutes` : `${(minutes / 60).toFixed(1)} hours`);

const tip = (s: string) => new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }).format(new Date(s));

/** Players by how many of the entries they are in, most first. */
function exposure(run: OptimizerResult): [string, number][] {
  const n = new Map<string, number>();
  for (const e of run.entries) for (const k of e.players) n.set(k, (n.get(k) ?? 0) + 1);
  return [...n.entries()].sort((a, b) => b[1] - a[1]);
}

export function ContestOptimizer({ date, players, games, capturedAt, linesAgeMinutes }: {
  date: string; players: ContestPlayer[]; games: OptimizerGame[]; capturedAt: string | null; linesAgeMinutes: number | null;
}) {
  const started = useMemo(() => new Set(games.filter((g) => g.started).map((g) => g.eventId)), [games]);
  const [selected, setSelected] = useState<Set<string>>(() => new Set(games.filter((g) => !started.has(g.eventId)).map((g) => g.eventId)));
  const [fieldSize, setFieldSize] = useState<number>(WTA.FIELD_SIZE);
  const [entries, setEntries] = useState<number>(5);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<{ run: OptimizerResult; games: string[] } | null>(null);
  const [save, setSave] = useState<SaveState | null>(null);
  const router = useRouter();

  const byKey = new Map(players.map((p) => [p.key, p]));
  const allSelected = games.length > 0 && games.every((g) => selected.has(g.eventId));
  const toggle = (id: string) => setSelected((prev) => {
    const next = new Set(prev);
    if (next.has(id)) next.delete(id); else next.add(id);
    return next;
  });

  /** Every run is stored so the entries actually submitted exist somewhere; the server stamps the time. */
  const store = async (eventIds: string[], requested: number, out: OptimizerResult) => {
    setSave({ state: "saving" });
    try {
      const response = await fetch("/api/nba/contest-runs", {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify(runRequest(date, eventIds, requested, capturedAt, RECAL_VERSION, out)),
      });
      const body = await response.json().catch(() => null) as { createdAt?: string; late?: boolean; repeat?: boolean; firstTip?: string; error?: string } | null;
      if (!response.ok || !body?.createdAt || !body.firstTip) {
        setSave({ state: "failed", error: body?.error ?? `Run not saved (the server answered ${response.status}).` });
        return;
      }
      setSave({ state: "saved", createdAt: body.createdAt, late: body.late === true, repeat: body.repeat === true, firstTip: body.firstTip });
      router.refresh();   // the saved-runs list below is rendered on the server
    } catch {
      setSave({ state: "failed", error: "Run not saved: the server could not be reached." });
    }
  };

  const run = () => {
    setBusy(true);
    setError(null);
    setSave(null);
    // Let the button repaint before the ~0.3 s of simulation blocks the thread.
    setTimeout(() => {
      try {
        const chosen = players.filter((p) => selected.has(p.eventId));
        const out = optimizeContest(chosen, { fieldSize, entries });
        setResult({ run: out, games: games.filter((g) => selected.has(g.eventId)).map((g) => g.game) });
        void store([...selected], entries, out);
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
        <p className="mt-1 text-sm text-slate-600">No lines captured for this date yet. Lines arrive after the morning capture (15:07 UTC: 10:07 AM Eastern in winter, 11:07 AM in summer).</p>
      </section>
    );
  }

  const last = result?.run.entries.at(-1);
  const baseLast = result?.run.baseline.at(-1);
  const sitOf = new Map((result?.run.pool ?? []).map((p, i) => [p.key, result!.run.sit[i]]));
  // The three biggest lines and the share of the modeled field holding each, for the owner to check against his contest.
  const heldBy = (result?.run.pool ?? []).slice(0, 3).map((p, i) => `${p.player} ${Math.round(result!.run.fieldOwnership[i] * 100)}%`);

  return (
    <section className="space-y-3 rounded-xl border bg-white p-4 shadow-sm">
      <div>
        <h2 className="font-bold">Build entries for your contest</h2>
        <p className="text-xs text-muted-foreground">
          Untick games your contest leaves out, set its size, then Optimize. Same rules as the entries below, run in your browser on the latest
          lines{capturedAt ? ` (captured ${new Date(capturedAt).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" })} ET)` : ""}.
          Every run is saved with its time, so the entries you submit are on record. One difference: each player&apos;s big nights are made rarer to match what has happened (the
          books&apos; alternate lines overstate them); the tracked entries below keep the original rules.
        </p>
        {linesAgeMinutes != null && linesAgeMinutes > STALE_LINES_MINUTES && (
          <p className="mt-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900">
            These lines were read {age(linesAgeMinutes)} ago. Players are ruled out between reads, and a player whose lines were read more than 90 minutes
            before his tip sits about ten times as often as one read inside 90 minutes. Reload this page and run again as close to your lock as you can.
          </p>
        )}
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
      {save?.state === "saving" && <p className="text-xs text-slate-500">Saving this run...</p>}
      {save?.state === "failed" && <p className="text-sm font-medium text-rose-700">{save.error}</p>}
      {save?.state === "saved" && (
        <p className={`text-sm ${save.late ? "font-medium text-amber-700" : "text-emerald-700"}`}>
          {save.repeat ? "Same run as one already saved at " : "Saved at "}{tip(save.createdAt)} ET
          {save.late ? `, after the first tip of these games (${tip(save.firstTip)} ET): kept, and marked late.` : `, before the first tip (${tip(save.firstTip)} ET).`}
        </p>
      )}

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
                          {(sitOf.get(k) ?? 0) >= SIT_TAG_AT && (p?.books ?? 0) > 1 && (
                            <span className="ml-1 rounded border border-slate-300 px-1 font-mono text-[10px] text-slate-600">MAY SIT {pct1(sitOf.get(k) ?? 0)}</span>
                          )}
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
            Pool: the {result.run.pool.length} biggest lines in the chosen games. The field is a guess: 10% on the exact three biggest lines, the rest leaning to big
            lines. Here that puts these shares of the field on each of them: {heldBy.join(", ")}. If your contest looks different, the entries should change.
          </p>
          <p className="text-xs text-muted-foreground">
            {result.run.sitVersion
              ? "A player who sits scores 0. His chance of sitting depends on how long before his own tip the lines were read: 0.2% inside 90 minutes, 2.1% from 90 minutes to 6 hours, 3.9% beyond (four or more books; every capture Oct 20 to Nov 11). Players in late games carry more of it, because your contest locks at the first tip."
              : "A player who sits scores 0. His chance of sitting here is the older rate by book count, because a lead time was missing."}
          </p>
        </div>
      )}
    </section>
  );
}
