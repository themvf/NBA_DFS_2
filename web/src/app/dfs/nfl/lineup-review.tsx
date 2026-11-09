"use client";

import { useMemo, useState } from "react";
import type { NflGeneratedLineup } from "./nfl-optimizer";
import { breakdownPortfolio, isFilterActive, lineupShape, matchesFilter, type LineupFilter, type PortfolioFormat } from "@/lib/nfl-dfs/portfolio-breakdown";
import PortfolioBreakdownPanel from "./portfolio-breakdown-panel";

const money = (value: number) => `$${value.toLocaleString()}`;
const select = "rounded-lg border bg-white px-2 py-1.5 text-xs";

export default function LineupReview({ lineups, runId, airMatchupMinimumPct = 0, goalLineMinimumPct = 0, plannedLineups, format, players }: {
  lineups: NflGeneratedLineup[];
  runId: string | null;
  airMatchupMinimumPct?: number;
  goalLineMinimumPct?: number;
  plannedLineups?: number;
  format?: PortfolioFormat;
  /** Current slate players, for field captain ownership on restored runs. */
  players?: readonly { dkPlayerId: number; captainOwnPct?: number | null }[];
}) {
  const [order, setOrder] = useState("original");
  const [filter, setFilter] = useState<LineupFilter>({});
  const shapeFormat: PortfolioFormat = format ?? (lineups[0]?.slots.some(({ slot }) => slot === "CPT") ? "showdown" : "classic");
  const shapes = useMemo(() => new Map(lineups.map((l) => [l.lineupNumber, lineupShape(l, shapeFormat)])), [lineups, shapeFormat]);
  const breakdown = useMemo(() => breakdownPortfolio(lineups, shapeFormat, players ? new Map(players.map((p) => [p.dkPlayerId, p])) : undefined), [lineups, shapeFormat, players]);
  const playerOptions = useMemo(() => {
    const seen = new Map<number, string>();
    for (const l of lineups) for (const { player } of l.slots) if (!seen.has(player.dkPlayerId)) seen.set(player.dkPlayerId, player.name);
    return [...seen].sort((a, b) => a[1].localeCompare(b[1]));
  }, [lineups]);
  const visible = useMemo(() => lineups.filter((l) => matchesFilter(shapes.get(l.lineupNumber)!, filter)), [lineups, shapes, filter]);
  const filtering = isFilterActive(filter);
  const numberOr = (value: string) => (value === "" ? null : Number(value));
  const sorted = useMemo(() => [...visible].sort((a, b) => {
    if (order === "projection") return b.projectedFpts - a.projectedFpts || a.lineupNumber - b.lineupNumber;
    if (order === "salary") return b.totalSalary - a.totalSalary || a.lineupNumber - b.lineupNumber;
    if (order === "ceiling") return b.ceilingFpts - a.ceilingFpts || a.lineupNumber - b.lineupNumber;
    return a.lineupNumber - b.lineupNumber;
  }), [visible, order]);
  const airMatchupCount = lineups.filter(lineup => lineup.slots.some(({ player }) => player.playerSignals?.some(signal => signal.code === "AIR_MATCHUP"))).length;
  const goalLineCount = lineups.filter(lineup => lineup.slots.some(({ player }) => player.position === "RB" && player.playerSignals?.some(signal => signal.code === "INSIDE_FIVE"))).length;
  const adjustedRbCount = lineups.filter(lineup => lineup.slots.some(({ player }) => player.position === "RB" && player.defensiveForecast?.status === "applied" && player.defensiveForecast.profile === "allowed-rushing-volume")).length;

  return <div className="space-y-3">
  <PortfolioBreakdownPanel breakdown={breakdown} filter={filter} onFilter={setFilter} />
  <section className="rounded-xl border bg-white">
    <div className="flex flex-wrap items-center justify-between gap-3 border-b p-4">
      <div><h2 className="font-semibold">Portfolio · {filtering ? `${visible.length} of ${lineups.length}` : lineups.length} lineups</h2>
        <p className="mt-1 text-xs text-slate-500">Run {runId?.slice(0, 8) ?? "unsaved"} · Select a lineup to inspect its roster.</p></div>
      {airMatchupMinimumPct > 0 ? <p className="text-xs font-semibold text-blue-800">Air-yard matchup: {airMatchupCount}/{lineups.length} lineups · minimum {Math.ceil((plannedLineups ?? lineups.length) * airMatchupMinimumPct / 100)} ({airMatchupMinimumPct}%)</p> : null}
      {goalLineMinimumPct > 0 ? <p className="text-xs font-semibold text-blue-800">Goal-line RB: {goalLineCount}/{lineups.length} lineups · minimum {Math.ceil((plannedLineups ?? lineups.length) * goalLineMinimumPct / 100)} ({goalLineMinimumPct}%)</p> : null}
      {adjustedRbCount > 0 ? <p className="text-xs font-semibold text-amber-800">Opponent-adjusted RB forecast: {adjustedRbCount}/{lineups.length} lineups</p> : null}
      <label className="text-sm">Sort <select className="ml-2 rounded-lg border px-2 py-2" value={order} onChange={e => setOrder(e.target.value)}>
        <option value="original">Original order</option><option value="projection">Projection ↓</option><option value="salary">Salary ↓</option><option value="ceiling">Ceiling sum ↓</option>
      </select></label>
    </div>
    <div className="flex flex-wrap items-center gap-2 border-b bg-slate-50 px-4 py-2" role="group" aria-label="Filter lineups">
      <span className="text-xs font-semibold text-slate-600">Show</span>
      {shapeFormat === "showdown" ? <select aria-label="Captain" className={select} value={filter.captainId ?? ""} onChange={(e) => setFilter((f) => ({ ...f, captainId: numberOr(e.target.value) }))}>
        <option value="">Any captain</option>
        {breakdown.captains.map((c) => <option key={c.key} value={c.dkPlayerId}>CPT {c.label} ({c.count})</option>)}
      </select> : null}
      <select aria-label={shapeFormat === "showdown" ? "Team split" : "Stack"} className={select} value={filter.split ?? filter.splitKind ?? ""} onChange={(e) => {
        const value = e.target.value, isKind = breakdown.splitKinds.some((k) => k.key === value);
        setFilter((f) => ({ ...f, splitKind: isKind ? value : null, split: isKind || !value ? null : value }));
      }}>
        <option value="">{shapeFormat === "showdown" ? "Any split" : "Any stack"}</option>
        {breakdown.splitKinds.map((k) => <option key={`kind-${k.key}`} value={k.key}>{k.label} ({k.count})</option>)}
        {shapeFormat === "showdown" ? breakdown.splits.filter((s) => !breakdown.splitKinds.some((k) => k.key === s.key))
          .map((s) => <option key={`team-${s.key}`} value={s.key}>{s.label} ({s.count})</option>) : null}
      </select>
      <select aria-label="Quarterbacks" className={select} value={filter.quarterbacks ?? ""} onChange={(e) => setFilter((f) => ({ ...f, quarterbacks: (e.target.value || null) as LineupFilter["quarterbacks"] }))}>
        <option value="">Any QBs</option>
        {breakdown.quarterbacks.map((q) => <option key={q.key} value={q.key}>{q.label} ({q.count})</option>)}
      </select>
      <select aria-label="Kicker" className={select} value={filter.kicker == null ? "" : filter.kicker ? "yes" : "no"} onChange={(e) => setFilter((f) => ({ ...f, kicker: e.target.value === "" ? null : e.target.value === "yes" }))}>
        <option value="">Kicker or not</option>
        <option value="yes">With a kicker ({breakdown.kicker})</option>
        <option value="no">No kicker ({breakdown.total - breakdown.kicker})</option>
      </select>
      <select aria-label="Includes player" className={select} value={filter.includes ?? ""} onChange={(e) => setFilter((f) => ({ ...f, includes: numberOr(e.target.value) }))}>
        <option value="">With anyone</option>
        {playerOptions.map(([id, name]) => <option key={id} value={id}>With {name}</option>)}
      </select>
      <select aria-label="Excludes player" className={select} value={filter.excludes ?? ""} onChange={(e) => setFilter((f) => ({ ...f, excludes: numberOr(e.target.value) }))}>
        <option value="">Without no one</option>
        {playerOptions.map(([id, name]) => <option key={id} value={id}>Without {name}</option>)}
      </select>
      {filtering ? <button type="button" onClick={() => setFilter({})} className="rounded-lg border border-blue-300 bg-white px-2 py-1.5 text-xs font-semibold text-blue-800">Clear filters</button> : null}
    </div>
    <p className="border-b px-4 py-2 text-xs text-slate-500">Sorting and filtering change this view only; the export always has every lineup in its original order. Floor and ceiling sums add player estimates, not lineup percentiles.</p>
    {filtering && !visible.length ? <p className="p-6 text-center text-sm text-slate-500">No lineups match these filters.</p> : null}
    <div className="max-h-[720px] overflow-auto divide-y">
      {sorted.map(lineup => <details key={`${runId}:${lineup.lineupNumber}`} className="nfl-lineup-detail">
        <summary className="grid cursor-pointer grid-cols-2 items-center gap-3 p-4 sm:grid-cols-5">
          <span className="font-semibold">Lineup {lineup.lineupNumber}</span>
          <ShapeLabel lineup={lineup} split={shapes.get(lineup.lineupNumber)!.split} />
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
  </section>
  </div>;
}

function ShapeLabel({ lineup, split }: { lineup: NflGeneratedLineup; split: string }) {
  const captain = lineup.slots.find(({ slot }) => slot === "CPT")?.player;
  return <span className="text-xs text-slate-600">
    {captain ? <b className="text-slate-900">CPT {captain.name}</b> : null}
    <span className={captain ? "ml-1 text-slate-500" : ""}>{captain ? "· " : ""}{split}</span>
  </span>;
}
