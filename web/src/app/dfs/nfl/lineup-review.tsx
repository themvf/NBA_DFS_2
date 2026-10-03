"use client";

import { useMemo, useState } from "react";
import type { NflGeneratedLineup } from "./nfl-optimizer";

const money = (value: number) => `$${value.toLocaleString()}`;

export default function LineupReview({ lineups, runId, airMatchupMinimumPct = 0, goalLineMinimumPct = 0, plannedLineups }: {
  lineups: NflGeneratedLineup[];
  runId: string | null;
  airMatchupMinimumPct?: number;
  goalLineMinimumPct?: number;
  plannedLineups?: number;
}) {
  const [order, setOrder] = useState("original");
  const sorted = useMemo(() => [...lineups].sort((a, b) => {
    if (order === "projection") return b.projectedFpts - a.projectedFpts || a.lineupNumber - b.lineupNumber;
    if (order === "salary") return b.totalSalary - a.totalSalary || a.lineupNumber - b.lineupNumber;
    if (order === "ceiling") return b.ceilingFpts - a.ceilingFpts || a.lineupNumber - b.lineupNumber;
    return a.lineupNumber - b.lineupNumber;
  }), [lineups, order]);
  const airMatchupCount = lineups.filter(lineup => lineup.slots.some(({ player }) => player.playerSignals?.some(signal => signal.code === "AIR_MATCHUP"))).length;
  const goalLineCount = lineups.filter(lineup => lineup.slots.some(({ player }) => player.position === "RB" && player.playerSignals?.some(signal => signal.code === "INSIDE_FIVE"))).length;
  const adjustedRbCount = lineups.filter(lineup => lineup.slots.some(({ player }) => player.position === "RB" && player.defensiveForecast?.status === "applied" && player.defensiveForecast.profile === "allowed-rushing-volume")).length;

  return <section className="rounded-xl border bg-white">
    <div className="flex flex-wrap items-center justify-between gap-3 border-b p-4">
      <div><h2 className="font-semibold">Portfolio · {lineups.length} lineups</h2>
        <p className="mt-1 text-xs text-slate-500">Run {runId?.slice(0, 8) ?? "unsaved"} · Select a lineup to inspect its roster.</p></div>
      {airMatchupMinimumPct > 0 ? <p className="text-xs font-semibold text-blue-800">Air-yard matchup: {airMatchupCount}/{lineups.length} lineups · minimum {Math.ceil((plannedLineups ?? lineups.length) * airMatchupMinimumPct / 100)} ({airMatchupMinimumPct}%)</p> : null}
      {goalLineMinimumPct > 0 ? <p className="text-xs font-semibold text-blue-800">Goal-line RB: {goalLineCount}/{lineups.length} lineups · minimum {Math.ceil((plannedLineups ?? lineups.length) * goalLineMinimumPct / 100)} ({goalLineMinimumPct}%)</p> : null}
      {adjustedRbCount > 0 ? <p className="text-xs font-semibold text-amber-800">Opponent-adjusted RB forecast: {adjustedRbCount}/{lineups.length} lineups</p> : null}
      <label className="text-sm">Sort <select className="ml-2 rounded-lg border px-2 py-2" value={order} onChange={e => setOrder(e.target.value)}>
        <option value="original">Original order</option><option value="projection">Projection ↓</option><option value="salary">Salary ↓</option><option value="ceiling">Ceiling sum ↓</option>
      </select></label>
    </div>
    <p className="border-b px-4 py-2 text-xs text-slate-500">Sorting changes this view only; export order stays unchanged. Floor and ceiling sums add player estimates, not lineup percentiles.</p>
    <div className="max-h-[720px] overflow-auto divide-y">
      {sorted.map(lineup => <details key={`${runId}:${lineup.lineupNumber}`} className="nfl-lineup-detail">
        <summary className="grid cursor-pointer grid-cols-2 items-center gap-3 p-4 sm:grid-cols-4">
          <span className="font-semibold">Lineup {lineup.lineupNumber}</span>
          <span className="text-sm text-slate-500">Projection <b className="text-emerald-800">{lineup.projectedFpts.toFixed(1)}</b></span>
          <span className="text-sm text-slate-500">Salary <b className="text-slate-900">{money(lineup.totalSalary)}</b></span>
          <span className="text-xs text-slate-500">View roster ↓</span>
        </summary>
        <div className="px-4 pb-4">
          <div className="mb-3 flex flex-wrap gap-4 text-xs text-slate-600">
            <span>Floor sum: {lineup.floorFpts.toFixed(1)}</span><span>Ceiling sum: {lineup.ceilingFpts.toFixed(1)}</span>
            <span>Stack: {lineup.stackSummary.quarterback ? [lineup.stackSummary.quarterback, ...lineup.stackSummary.passCatchers].join(" + ") : "None"}{lineup.stackSummary.bringBack ? ` / ${lineup.stackSummary.bringBack}` : ""}</span>
            {lineup.slots.filter(({ player }) => player.playerSignals?.some(signal => signal.code === "AIR_MATCHUP")).map(({ player }) =>
              <span key={player.dkPlayerId} title={player.playerSignals?.find(signal => signal.code === "AIR_MATCHUP")?.detail}>Air-yard matchup: {player.name}</span>)}
            {lineup.slots.filter(({ player }) => player.position === "RB" && player.playerSignals?.some(signal => signal.code === "INSIDE_FIVE")).map(({ player }) =>
              <span key={player.dkPlayerId} title={player.playerSignals?.find(signal => signal.code === "INSIDE_FIVE")?.detail}>Goal-line RB: {player.name}</span>)}
            {lineup.slots.filter(({ player }) => player.position === "RB" && player.defensiveForecast?.status === "applied" && player.defensiveForecast.profile === "allowed-rushing-volume").map(({ player }) => {
              const forecast = player.defensiveForecast!;
              return <span key={`adjusted-${player.dkPlayerId}`} title={`Frozen opponent capture ${forecast.capturedAt ?? "unknown"}; ${forecast.version}`}>
                Opponent-adjusted: {player.name} ({(forecast.selected.mean - forecast.baseline.mean) >= 0 ? "+" : ""}{(forecast.selected.mean - forecast.baseline.mean).toFixed(1)} mean, {(forecast.selected.p90 - forecast.baseline.p90) >= 0 ? "+" : ""}{(forecast.selected.p90 - forecast.baseline.p90).toFixed(1)} P90)
              </span>;
            })}
          </div>
          <table className="w-full text-sm">
            <thead className="text-left text-xs text-slate-500"><tr><th className="py-2">Slot</th><th>Player</th><th className="text-right">Slot salary</th></tr></thead>
            <tbody>{lineup.slots.map(({ slot, player, salary }) => <tr key={slot} className="border-t"><td className="py-2 text-xs font-semibold text-slate-500">{slot}</td><td className="font-medium">{player.name}<span className="ml-2 text-xs text-slate-500">{player.team}</span></td><td className="text-right">{money(salary)}</td></tr>)}</tbody>
          </table>
          {lineup.slots.some(({ slot }) => slot === "CPT") && <p className="mt-2 text-xs text-slate-500">Captain salary includes the 1.5× multiplier. Slot salaries add up to the lineup total.</p>}
        </div>
      </details>)}
    </div>
  </section>;
}
