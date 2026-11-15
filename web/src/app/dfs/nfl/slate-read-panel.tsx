"use client";

import { useEffect, useMemo, useState } from "react";
import { buildClassicSlateRead, buildShowdownSlateRead, type GameLine, type SlateReadPlayer } from "@/lib/nfl-dfs/slate-read";
import { readNflSlateGameLines } from "./slate-read-actions";

/**
 * Before the build: what the lines and the field's estimated ownership say
 * about this slate. Loads by itself when a slate opens; changes no lineup.
 */
export default function SlateReadPanel({ slateKey, firstKickoff, players, format }: {
  slateKey: string; firstKickoff: string | null; players: readonly SlateReadPlayer[]; format: "classic" | "showdown";
}) {
  const [loaded, setLoaded] = useState<{ key: string; lines: GameLine[]; error: string | null } | null>(null);
  useEffect(() => {
    let stopped = false;
    readNflSlateGameLines(firstKickoff)
      .then((r) => { if (!stopped) setLoaded({ key: slateKey, ...r }); })
      .catch((reason) => { if (!stopped) setLoaded({ key: slateKey, lines: [], error: reason instanceof Error ? reason.message : "Game lines could not be read." }); });
    return () => { stopped = true; };
  }, [slateKey, firstKickoff]);
  const lines = loaded?.key === slateKey ? loaded.lines : null;
  const classic = useMemo(() => (format === "classic" && lines ? buildClassicSlateRead(players, lines) : null), [format, lines, players]);
  const showdown = useMemo(() => (format === "showdown" && lines ? buildShowdownSlateRead(players, lines) : null), [format, lines, players]);
  const read = classic ?? showdown;
  const maxShare = classic ? Math.max(1, ...classic.games.map((g) => g.fieldSharePct ?? 0)) : 1;
  const asOf = (lines ?? []).map((l) => l.capturedAt).filter((v): v is string => Boolean(v)).sort().at(-1) ?? null;
  return <section className="rounded-xl border border-sky-200 bg-white p-4 shadow-sm">
    <h2 className="font-bold">Slate read <span className="ml-2 text-xs font-normal text-slate-500">before you build</span></h2>
    {!lines ? <p className="mt-2 text-xs text-slate-500">Reading the lines…</p> : null}
    {loaded?.key === slateKey && loaded.error ? <p role="alert" className="mt-2 text-xs font-semibold text-amber-800">{loaded.error}</p> : null}
    {read ? <ul className="mt-2 space-y-1.5 text-sm text-slate-900">{read.statements.map((s) => <li key={s} className="rounded-lg bg-sky-50 px-3 py-2">{s}</li>)}</ul> : null}
    {classic && classic.games.length ? <div className="mt-3 overflow-x-auto"><table className="w-full min-w-[560px] text-xs">
      <thead className="text-left text-slate-500"><tr><th className="py-1 font-normal">Game</th><th className="font-normal">Total</th><th className="font-normal">Favorite</th><th className="font-normal">Field roster spots (est.)</th><th className="font-normal">Field QBs (est.)</th><th className="font-normal">Most owned</th></tr></thead>
      <tbody>{classic.games.map((g) => <tr key={g.key} className="border-t align-top">
        <td className="py-1.5 font-semibold">{g.away} @ {g.home}</td>
        <td className={g.band === "under 43" ? "font-semibold text-amber-800" : g.band === "48+" ? "font-semibold text-sky-800" : ""}>{g.total ?? "—"}</td>
        <td>{g.favorite && g.homeSpread != null ? `${g.favorite} −${Math.abs(g.homeSpread)}` : "—"}</td>
        <td>{g.fieldSharePct == null ? "—" : <span className="flex items-center gap-2"><span className="inline-block h-2 rounded bg-sky-500" style={{ width: `${Math.round((90 * g.fieldSharePct) / maxShare)}px` }} /><span className="tabular-nums">{Math.round(g.fieldSharePct)}%</span></span>}</td>
        <td className="tabular-nums">{g.fieldQbPct == null ? "—" : `${Math.round(g.fieldQbPct)}%`}</td>
        <td className="text-slate-600">{g.chalk.map((c) => `${c.name} ${Math.round(c.ownPct)}%`).join(", ") || "—"}</td>
      </tr>)}</tbody>
    </table></div> : null}
    {read ? <p className="mt-2 text-[10px] text-slate-500">
      {classic ? `An even split over ${classic.games.length} games is ${Math.round(classic.evenSharePct)}% a game. ` : ""}
      Field shares are our ownership estimates, not measured. Past returns are the field&apos;s own results on a small number of slates: leads, not rules.
      {asOf ? ` Lines as of ${new Date(asOf).toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit" })} ET.` : ""}
    </p> : null}
  </section>;
}
