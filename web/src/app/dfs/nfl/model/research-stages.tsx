"use client";

import { useMemo, useState } from "react";
import type { EfficiencyReport } from "@/lib/nfl-dfs/efficiency";
import type { WorkloadReport } from "@/lib/nfl-dfs/workload";

const panel = "rounded-2xl border border-slate-700 bg-slate-900/70 p-5";
const integer = (value: number) => value.toLocaleString("en-US");
const rateValue = (key: string, value: number | null) => value === null ? "—" : /(rate|completion|catch)/.test(key) ? `${(100 * value).toFixed(1)}%` : value.toFixed(2);

export default function ResearchStages({ mode, workload, workloadDigest, efficiency, efficiencyDigest }: {
  mode: "Workload" | "Efficiency";
  workload: WorkloadReport;
  workloadDigest: string | null;
  efficiency: EfficiencyReport | null;
  efficiencyDigest: string | null;
}) {
  const teams = workload.forecasts.map(forecast => forecast.team);
  const [team, setTeam] = useState(teams[0] ?? "");
  const [playerId, setPlayerId] = useState("");
  const workloadForecast = workload.forecasts.find(forecast => forecast.team === team);
  const efficiencyForecast = efficiency?.forecasts.find(forecast => forecast.team === team);
  const workloadPlayers = useMemo(() => [...(workloadForecast?.players.filter(player => Object.keys(player.components).length) ?? [])]
    .sort((left, right) => Math.max(0, ...Object.values(right.components).map(component => component.mean)) - Math.max(0, ...Object.values(left.components).map(component => component.mean))), [workloadForecast]);
  const efficiencyPlayers = useMemo(() => [...(efficiencyForecast?.players ?? [])].sort((left, right) => right.mean_fpts - left.mean_fpts), [efficiencyForecast]);
  const playerOptions = mode === "Efficiency" ? efficiencyPlayers : workloadPlayers;
  const selectedWorkload = workloadPlayers.find(player => player.identity === playerId) ?? workloadPlayers[0];
  const selectedEfficiency = efficiencyPlayers.find(player => player.identity === playerId) ?? efficiencyPlayers[0];

  function selectors() {
    return <div className="flex flex-wrap items-center gap-4">
      <label>Team<select aria-label="Team" className="ml-2 rounded-lg border border-slate-600 bg-slate-900 p-2" value={team} onChange={event => { setTeam(event.target.value); setPlayerId(""); }}>
        {teams.map(name => <option key={name}>{name}</option>)}
      </select></label>
      <label>Player<select aria-label="Player" className="ml-2 rounded-lg border border-slate-600 bg-slate-900 p-2" value={(mode === "Efficiency" ? selectedEfficiency?.identity : selectedWorkload?.identity) ?? ""} onChange={event => setPlayerId(event.target.value)}>
        {playerOptions.map(player => <option key={player.identity ?? player.name} value={player.identity ?? ""}>{player.name} · {player.position}</option>)}
      </select></label>
    </div>;
  }

  if (mode === "Workload" && workloadForecast) return <section className="space-y-5">
    <div className="flex flex-wrap items-center justify-between gap-4">{selectors()}<span className="rounded-full border border-amber-700 px-3 py-1 text-xs text-amber-200">ROLE UNRESOLVED · RESEARCH ONLY · {workload.version}</span></div>
    <p role="alert" className="rounded-lg border border-amber-800 bg-amber-950/50 p-4 text-sm text-amber-100">No verified depth chart or historical weekly roster is integrated. Multiple quarterbacks and stale active-roster candidates can appear. These allocations expose the formula and missing role evidence; they are not lineup-ready projections.</p>
    <div className="grid gap-4 lg:grid-cols-3">{Object.entries(workloadForecast.budgets).map(([field, budget]) => <div className={panel} key={field}>
      <p className="text-xs uppercase text-slate-400">Team {field}</p><p className="mt-2 text-3xl font-semibold">{budget ? budget.mean.toFixed(1) : "Unavailable"}</p>
      {budget && <><p className="mt-2 text-sm text-slate-300">Recent {budget.history_mean.toFixed(1)} · league prior {budget.prior.toFixed(1)}</p>
        <div className="mt-4 h-2 rounded bg-slate-700"><div className="h-full rounded bg-teal-400" style={{ width: `${100 * (budget.allocated_share ?? 0)}%` }} /></div>
        <p className="mt-2 text-xs text-slate-400">Known players {(100 * (budget.allocated_share ?? 0)).toFixed(0)}% · unallocated {(100 * (budget.unallocated_share ?? 1)).toFixed(0)}%</p></>}
    </div>)}</div>
    <div className={panel}><h2 className="text-xl font-semibold">{team} player allocation</h2><p className="mt-1 text-sm text-slate-400">Expected opportunities; unallocated work remains visible.</p>
      {(["attempts", "carries", "targets"] as const).map(field => <div key={field} className="mt-5"><h3 className="mb-2 capitalize">{field}</h3>
        {workloadForecast.players.filter(player => player.components[field]).sort((left, right) => right.components[field].mean - left.components[field].mean).slice(0, 12).map(player => <div className="grid grid-cols-[10rem_1fr_4rem] items-center gap-3 py-1 text-sm" key={`${player.identity ?? player.name}-${field}`}>
          <span className="truncate">{player.name}</span><div className="h-3 rounded bg-slate-700"><div className="h-full rounded bg-cyan-400" style={{ width: `${100 * player.components[field].share}%` }} /></div><span className="text-right tabular-nums">{player.components[field].mean.toFixed(1)}</span>
        </div>)}</div>)}</div>
    {selectedWorkload && <div className={panel}><h2 className="text-xl font-semibold">{selectedWorkload.name} · workload history</h2><p className="mt-1 text-sm text-slate-400">Recorded games only; absent weeks are not zeros.</p>
      <div className="mt-6 grid gap-6 lg:grid-cols-2">{Object.entries(selectedWorkload.components).map(([field, component]) => { const maximum = Math.max(component.mean, ...component.recent.map(row => row.actual)); return <div key={field}>
        <div className="flex justify-between"><h3 className="capitalize">{field}</h3><strong>{component.mean.toFixed(1)} projected</strong></div>
        <div className="mt-3 flex h-32 items-end gap-2 border-b border-slate-600">{component.recent.map(row => <div title={`${row.season} W${row.week}: ${row.actual}`} className="min-w-5 flex-1 bg-cyan-500" key={`${row.season}-${row.week}`} style={{ height: `${Math.max(3, 100 * row.actual / maximum)}%` }} />)}<div title={`Forecast: ${component.mean.toFixed(1)}`} className="min-w-5 flex-1 bg-amber-400" style={{ height: `${Math.max(3, 100 * component.mean / maximum)}%` }} /></div>
        <p className="mt-2 text-xs text-slate-400">{component.recent.map(row => `${row.season} W${row.week}`).join(" · ")} · FORECAST</p>
        <p className="mt-1 text-xs text-slate-400">{component.games} modeled games · {(100 * component.share).toFixed(1)}% team share · {component.normalization.replaceAll("_", " ")}</p>
      </div>; })}</div></div>}
    <div className={panel}><h2 className="text-xl font-semibold">Retrospective team-budget check</h2><div className="mt-4 grid gap-4 sm:grid-cols-3">{workload.backtest.metrics.map(metric => <div key={metric.field}><p className="capitalize">{metric.field}</p><p className="mt-2 text-sm">Candidate MAE <strong>{metric.candidate_mae?.toFixed(2) ?? "—"}</strong></p><p className="text-sm">Recency MAE <strong>{metric.baseline_mae?.toFixed(2) ?? "—"}</strong></p><p className="text-xs text-slate-400">n={integer(metric.n)} · bias actual−projected {metric.candidate_bias_actual_minus_projected?.toFixed(2) ?? "—"}</p></div>)}</div>
      <p className="mt-4 text-xs text-amber-200">Previously inspected 2024–2025 team games. Descriptive—not a promotion gate or lineup return claim.</p><p className="mt-2 break-all text-xs text-slate-500">Run {workloadDigest} · dataset {workload.dataset_digest}</p></div>
  </section>;

  if (mode !== "Efficiency" || !efficiency || !efficiencyForecast || !selectedEfficiency) return null;
  const contributions = Object.entries(selectedEfficiency.scoring_contributions).filter(([, value]) => Math.abs(value) >= .005).sort((left, right) => Math.abs(right[1]) - Math.abs(left[1]));
  const maximum = Math.max(1, ...contributions.map(([, value]) => Math.abs(value)));
  return <section className="space-y-5">
    <div className="flex flex-wrap items-center justify-between gap-4">{selectors()}<span className="rounded-full border border-amber-700 px-3 py-1 text-xs text-amber-200">TEAM-COUPLED OFFENSE + DST · {efficiency.version}</span></div>
    <p role="alert" className="rounded-lg border border-amber-800 bg-amber-950/50 p-4 text-sm text-amber-100">This inherits the unresolved active-roster workload. Offensive draws share one team state and reconcile to exact DraftKings scoring. DST separately resamples whole-game components using defense history, opponent DST-points-allowed history, and a league prior; richer opponent context is not yet modeled.</p>
    <div className={panel}><h2 className="text-xl font-semibold">{team} simulation integrity</h2><p className="mt-1 text-sm text-slate-400">Maximum mismatch across {integer(efficiencyForecast.team_coherence.draws)} shared team draws. Zero is the required result.</p>
      <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">{Object.entries(efficiencyForecast.team_coherence.max_absolute_mismatch).map(([key, value]) => <div className="rounded-lg border border-slate-700 p-3" key={key}><p className="text-xs uppercase text-slate-400">{key.replaceAll("_", " ")}</p><p className={`mt-2 text-2xl font-semibold ${value <= 1e-8 ? "text-teal-300" : "text-rose-300"}`}>{value.toExponential(1)}</p></div>)}</div>
      <p className="mt-4 text-xs text-slate-400">Mean unresolved team bucket · {Object.entries(efficiencyForecast.team_coherence.mean_unallocated).map(([key, value]) => `${key} ${value.toFixed(1)}`).join(" · ")}</p>
    </div>
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">{[
      ["Mean", selectedEfficiency.mean_fpts, false], ["P10", selectedEfficiency.p10_fpts, false], ["Median", selectedEfficiency.median_fpts, false], ["P90", selectedEfficiency.p90_fpts, false], [`Boom ≥ ${selectedEfficiency.boom_threshold}`, 100 * selectedEfficiency.boom_rate, true],
    ].map(([label, value, percent]) => <div className={panel} key={String(label)}><p className="text-xs uppercase text-slate-400">{label}</p><p className="mt-2 text-2xl font-semibold tabular-nums">{Number(value).toFixed(1)}{percent ? "%" : ""}</p></div>)}</div>
    <div className="grid gap-5 lg:grid-cols-2"><div className={panel}><h2 className="text-xl font-semibold">{selectedEfficiency.name} · scoring bridge</h2><p className="mt-1 text-sm text-slate-400">Mean DraftKings points from {integer(selectedEfficiency.draws)} saved deterministic draws.</p>
      <div className="mt-5 space-y-3">{contributions.map(([component, value]) => <div className="grid grid-cols-[9rem_1fr_4rem] items-center gap-3 text-sm" key={component}><span className="truncate capitalize">{component.replaceAll("_", " ")}</span><div className="h-3 rounded bg-slate-800"><div className={`h-full rounded ${value < 0 ? "bg-rose-400" : "bg-teal-400"}`} style={{ width: `${100 * Math.abs(value) / maximum}%` }} /></div><span className="text-right tabular-nums">{value >= 0 ? "+" : ""}{value.toFixed(2)}</span></div>)}</div>
      <p className="mt-5 border-t border-slate-700 pt-3 text-sm">Components <strong>{Object.values(selectedEfficiency.scoring_contributions).reduce((sum, value) => sum + value, 0).toFixed(2)}</strong> · saved mean <strong>{selectedEfficiency.mean_fpts.toFixed(2)}</strong></p>
    </div><div className={panel}><h2 className="text-xl font-semibold">{selectedEfficiency.opponent_context ? "DST evidence mix" : "Conditional efficiency"}</h2><p className="mt-1 text-sm text-slate-400">{selectedEfficiency.opponent_context ? "Whole-game exact components preserve scoring bands and rare-event dependence." : "Player history shrunk toward the position prior. Zero opportunities are undefined, not zero efficiency."}</p>
      {selectedEfficiency.opponent_context ? <div className="mt-4 grid gap-3 sm:grid-cols-3">{[
        ["Defense history", selectedEfficiency.opponent_context.defense_games], [`${selectedEfficiency.opponent_context.opponent} allowed`, selectedEfficiency.opponent_context.opponent_allowed_games], ["League prior", selectedEfficiency.opponent_context.league_prior_equivalent_games],
      ].map(([label, value]) => <div className="rounded-lg border border-slate-700 p-3" key={String(label)}><p className="text-xs uppercase text-slate-400">{label}</p><p className="mt-2 text-2xl font-semibold text-cyan-300">{value}</p><p className="text-xs text-slate-500">whole-game observations</p></div>)}</div> : <div className="mt-4 space-y-3">{Object.entries(selectedEfficiency.rates).map(([key, rate]) => <div key={key} className="rounded-lg border border-slate-700 p-3"><div className="flex items-start justify-between gap-3"><p className="font-medium">{rate.label}</p><strong className="tabular-nums text-cyan-300">{rateValue(key, rate.mean)}</strong></div><p className="mt-1 text-xs text-slate-400">Player {rateValue(key, rate.player_rate)} · prior {rateValue(key, rate.position_prior)} · {rate.games} games / {rate.player_opportunities.toFixed(1)} opportunities</p></div>)}</div>}
    </div></div>
    <div className={panel}><h2 className="text-xl font-semibold">Retrospective efficiency check</h2><p className="mt-1 text-sm text-slate-400">Actual opportunity denominators isolate conversion accuracy; this is not a full projection backtest.</p>
      <div className="mt-4 overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr className="border-b border-slate-700 text-slate-400"><th className="py-2">Outcome</th><th>Candidate</th><th>Recency</th><th>Δ</th><th>n</th><th>Bias</th></tr></thead><tbody>{efficiency.backtest.metrics.map(metric => { const delta = metric.candidate_mae !== null && metric.baseline_mae !== null ? metric.candidate_mae - metric.baseline_mae : null; return <tr className="border-b border-slate-800" key={metric.rate}><th className="py-2 font-medium">{metric.label}</th><td>{metric.candidate_mae?.toFixed(3) ?? "—"}</td><td>{metric.baseline_mae?.toFixed(3) ?? "—"}</td><td className={delta === null ? "" : delta <= 0 ? "text-teal-300" : "text-rose-300"}>{delta === null ? "—" : `${delta > 0 ? "+" : ""}${delta.toFixed(3)}`}</td><td>{integer(metric.n)}</td><td>{metric.candidate_bias_actual_minus_projected?.toFixed(3) ?? "—"}</td></tr>; })}</tbody></table></div>
      <p className="mt-4 text-xs text-amber-200">Previously inspected 2024–2025 rows. Negative Δ favors the candidate; bias is actual − projected. No promotion gate is inferred.</p><p className="mt-2 break-all text-xs text-slate-500">Run {efficiencyDigest} · dataset {efficiency.dataset_digest} · workload {efficiency.workload_run_digest}</p>
    </div>
  </section>;
}
