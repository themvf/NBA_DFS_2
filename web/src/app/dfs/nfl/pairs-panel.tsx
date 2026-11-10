"use client";

import { useMemo, useState } from "react";
import { pairBreakdown, type BreakdownLineup, type PairRow } from "@/lib/nfl-dfs/portfolio-breakdown";

/**
 * Which players the portfolio puts together. "Together" counts any two players
 * in one lineup; in Showdown, "Captain with" counts a captain and a flex
 * player. Beside each pair: the share expected if the two exposures were
 * independent, so a stack shows as more often than chance and an avoided pair
 * as less.
 */
export default function PairsPanel({ lineups, format }: { lineups: readonly BreakdownLineup[]; format: "classic" | "showdown" }) {
  const showdown = format === "showdown";
  const [view, setView] = useState<"together" | "captainWith">(showdown ? "captainWith" : "together");
  const [player, setPlayer] = useState<number | null>(null);
  const pairs = useMemo(() => pairBreakdown(lineups, 2, 400), [lineups]);
  const players = useMemo(() => {
    const seen = new Map<number, string>();
    for (const l of lineups) for (const s of l.slots) seen.set(s.player.dkPlayerId, s.player.name);
    return [...seen].sort((a, b) => a[1].localeCompare(b[1]));
  }, [lineups]);
  const list = (view === "captainWith" ? pairs.captainWith : pairs.together)
    .filter((r) => player == null || r.a.dkPlayerId === player || r.b.dkPlayerId === player).slice(0, 30);
  const total = lineups.length;
  const select = "rounded border bg-white px-1.5 py-1 text-[11px]";
  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <h2 className="font-bold">Pairs <span className="text-xs font-normal text-slate-400">who goes together · {total} lineups</span></h2>
    <div className="mt-2 flex flex-wrap items-center gap-1.5">
      {showdown ? <select aria-label="Pair type" className={select} value={view} onChange={(e) => setView(e.target.value as typeof view)}>
        <option value="captainWith">Captain with flex</option>
        <option value="together">Any two players</option>
      </select> : null}
      <select aria-label="Pairs with player" className={select} value={player ?? ""} onChange={(e) => setPlayer(e.target.value === "" ? null : Number(e.target.value))}>
        <option value="">Every player</option>
        {players.map(([id, name]) => <option key={id} value={id}>With {name}</option>)}
      </select>
    </div>
    <div className="mt-2 max-h-80 overflow-auto">
      <table className="w-full text-[11px]">
        <thead className="sticky top-0 bg-white text-left text-slate-500"><tr>
          <th className="py-1">{view === "captainWith" ? "Captain + flex" : "Pair"}</th><th className="text-right">Lineups</th>
          <th className="text-right" title="If the two players' exposures were independent">By chance</th><th className="text-right">Read</th>
        </tr></thead>
        <tbody>{list.map((r) => <PairLine key={r.key} row={r} total={total} captain={view === "captainWith"} />)}</tbody>
      </table>
      {!list.length ? <p className="py-4 text-center text-xs text-slate-500">No pair appears in two or more lineups.</p> : null}
    </div>
    <p className="mt-2 text-[10px] text-slate-500">Pairs in at least two lineups, most common first. &quot;By chance&quot; is what the two players&apos; own exposures would give if they were placed independently; well above it is a deliberate stack, well below it is a pair the build keeps apart.</p>
  </section>;
}

function PairLine({ row, total, captain }: { row: PairRow; total: number; captain: boolean }) {
  const ratio = row.independentPct > 0 ? row.pct / row.independentPct : null;
  const read = ratio == null ? "—" : ratio >= 1.4 ? "stacked" : ratio <= 0.7 ? "kept apart" : "about chance";
  const tone = ratio == null ? "text-slate-400" : ratio >= 1.4 ? "text-blue-700" : ratio <= 0.7 ? "text-amber-700" : "text-slate-500";
  return <tr className="border-t align-top">
    <td className="py-1 pr-2">
      {captain ? <span className="mr-1 rounded bg-slate-800 px-1 text-[9px] font-bold text-white">CPT</span> : null}
      <span className="font-semibold text-slate-800">{row.a.name}</span> <span className="text-slate-400">{row.a.position} {row.a.team}</span>
      <span className="mx-1 text-slate-400">+</span>
      <span className="font-semibold text-slate-800">{row.b.name}</span> <span className="text-slate-400">{row.b.position} {row.b.team}</span>
    </td>
    <td className="text-right tabular-nums"><b>{Math.round(row.pct)}%</b> <span className="text-slate-400">{row.count}/{total}</span></td>
    <td className="text-right tabular-nums text-slate-500">{Math.round(row.independentPct)}%</td>
    <td className={`text-right font-semibold ${tone}`}>{read}</td>
  </tr>;
}
