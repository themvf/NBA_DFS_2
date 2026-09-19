"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { loadPlayerHistory } from "./actions";
import { CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { reportSummary, VARIANT_LABELS, type ReportRow, type ReportVariant, type WeeklyReport } from "@/lib/nfl-dfs/report-card";
import { DELTA_FILL, DELTA_POLE, delta as rowDelta, deltaBucket, deltaScales,
  matchesReviewPosition, REVIEW_POSITIONS, topMovers, type ReviewPosition } from "@/lib/nfl-dfs/review-insights";
import { appearances, classifyRemoval, findAppearance, injuriesFor, injuryEvents,
  NEEDS_ATTENTION, teamOffensivePlays, VERDICT_LABEL, VERDICT_MARK,
  type ParticipantRow, type PlayRow, type Proposal } from "@/lib/nfl-dfs/removal";

const fmt = (v: number | null | undefined) => v == null ? "—" : v.toFixed(1);
const date = (v: string) => new Date(v).toLocaleString("en-US", { timeZone: "America/New_York", dateStyle: "short", timeStyle: "short" });
const label = (v: string) => v.replaceAll("_", " ");
const signed = (v: number | null | undefined) => v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(1)}`;

function MoverList({ title, note, rows, tone, scales, widest }: {
  title: string; note: string; rows: ReportRow[];
  tone: "exceeded" | "disappointed"; scales: Record<string, number>; widest: number;
}) {
  // `widest` spans BOTH panels. Scaling each list to its own maximum would draw
  // a quiet week's -4 as long as the other panel's +25.
  return <div className="rounded-xl border bg-white p-4">
    <h2 className="text-sm font-bold">{title}</h2>
    <p className="mt-1 text-xs text-slate-500">{note}</p>
    {!rows.length ? <p className="mt-4 text-xs text-slate-500">No scored player has a delta in this direction yet.</p>
      : <ol className="mt-3 list-none space-y-2">{rows.map((r, i) => {
        const d = rowDelta(r)!;
        return <li key={r.player_id} className="grid grid-cols-[1.25rem_minmax(0,1fr)_auto] items-center gap-2 text-xs">
          <span className="text-right tabular-nums text-slate-400">{i + 1}</span>
          <span className="min-w-0">
            <span className="block truncate font-semibold">{r.name}</span>
            <span className="block truncate text-slate-500">{r.position} · {r.team} vs {r.opponent} · {fmt(r.forecast?.mean)} → {fmt(r.actual)}</span>
            <span aria-hidden className="mt-1 block h-1 rounded-full" style={{ width: `${Math.abs(d) / widest * 100}%`, backgroundColor: DELTA_POLE[tone] }}/>
          </span>
          <span className="rounded px-1.5 py-0.5 text-right font-bold tabular-nums"
            style={{ backgroundColor: DELTA_FILL[deltaBucket(d, scales[r.position])] }}>{signed(d)}</span>
        </li>;
      })}</ol>}
  </div>;
}

/** Was he available, or did he just not produce?
 *
 * Deliberately coarse: we are removing players who were not available to earn
 * their projection, not players who were uncomfortable. A man who tweaked
 * something and came back is, here, a man who played.
 *
 * A proposal, never a finding. */
function AvailabilityNote({ proposal }: { proposal: Proposal | null }) {
  if (!proposal) {
    return <p className="text-xs text-slate-500">Play-by-play has not been loaded for this week, so
      no read on availability is offered. This is not a finding of &ldquo;played normally&rdquo;.</p>;
  }
  const e = proposal.evidence;
  return <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
    <p className="text-sm font-semibold">{VERDICT_MARK[proposal.verdict]} {VERDICT_LABEL[proposal.verdict]}</p>
    <p className="mt-1 text-xs text-slate-600">{proposal.reason}</p>
    <p className="mt-2 text-xs tabular-nums text-slate-500">
      {e.targets} targets · {e.carries} carries · {e.dropbacks} dropbacks
      {e.lastQuarter != null ? ` · last touch Q${e.lastQuarter}` : ""}
      {e.playsAfter != null ? ` · ${e.playsAfter} team plays after` : ""}
    </p>
    {e.injuries.length > 0 && <p className="mt-1 text-xs text-slate-600">
      Play-by-play names him injured: {e.injuries.map(i =>
        `${i.name} in Q${i.quarter ?? "?"}${i.clock ? ` (${i.clock})` : ""}`).join("; ")}.
    </p>}
    <p className="mt-2 text-[11px] text-slate-500">Proposed by {proposal.version} ({proposal.confidence} confidence).
      Nothing is tagged until you confirm it. Snap-level presence is not available in season —
      nflverse publishes it after the postseason — so a player on the field but never thrown to
      is invisible here.</p>
  </div>;
}

export default function WeeklyReview({ reports, availableWeeks, season, viewedAt, participants, playContext }: { reports: WeeklyReport[]; availableWeeks: number[]; season: number; viewedAt: number; participants: ParticipantRow[] | null; playContext: PlayRow[] | null }) {
  const router = useRouter();
  const week = reports.at(-1)?.week ?? 1;
  const [variant, setVariant] = useState<ReportVariant>("production");
  const [position, setPosition] = useState<ReviewPosition>("ALL");
  const [query, setQuery] = useState("");
  const [playerId, setPlayerId] = useState<number | null>(null);
  const report = reports.find(r => r.week === week);
  const variantRows = useMemo(() => (report?.rows ?? []).filter(r => r.variant === variant), [report, variant]);
  // Scales come from the whole variant, never the filtered set: a search box
  // that repaints the surviving rows would make the colour mean two things.
  const scales = useMemo(() => deltaScales(variantRows), [variantRows]);
  const positionRows = useMemo(() => variantRows.filter(r => matchesReviewPosition(r.position, position)), [variantRows, position]);
  const movers = useMemo(() => topMovers(positionRows, 10), [positionRows]);
  // Built once per week, not per row: each is a full scan of a week of plays.
  const quarters = useMemo(() => new Map(
    (playContext ?? []).map(p => [`${p.gameId}:${p.playId}`, p.quarter])), [playContext]);
  const injuries = useMemo(() => injuryEvents(playContext ?? []), [playContext]);
  const appearanceIndex = useMemo(
    () => participants ? appearances(participants, quarters) : null, [participants, quarters]);
  const teamPlays = useMemo(
    () => participants ? teamOffensivePlays(participants) : null, [participants]);
  // The report card's game_id is our internal numeric id, not the nflverse
  // text one, so a player is located by (team, name) across the week's games.
  const gamesByTeam = useMemo(() => {
    const map = new Map<string, string[]>();
    for (const row of participants ?? []) {
      if (row.team == null) continue;
      const games = map.get(row.team) ?? [];
      if (!games.includes(row.gameId)) map.set(row.team, [...games, row.gameId]);
    }
    return map;
  }, [participants]);
  const proposalFor = useMemo(() => (row: ReportRow): Proposal | null => {
    if (!appearanceIndex || !teamPlays) return null;
    let gameId: string | null = null;
    let appearance = null;
    for (const candidate of gamesByTeam.get(row.team) ?? []) {
      const found = findAppearance(appearanceIndex, candidate, row.name);
      if (found) { gameId = candidate; appearance = found; break; }
    }
    return classifyRemoval({
      position: row.position, appearance,
      teamPlays: teamPlays.get(`${gameId ?? ""}:${appearance?.team ?? row.team}`) ?? [],
      injuries: gameId ? injuriesFor(injuries, gameId, row.name) : [],
    });
  }, [appearanceIndex, teamPlays, gamesByTeam, injuries]);
  const moverBarScale = useMemo(() => Math.max(
    ...[...movers.exceeded, ...movers.disappointed].map(r => Math.abs(rowDelta(r)!)), 1), [movers]);
  const rows = useMemo(() => positionRows.filter(r => `${r.name} ${r.team}`.toLowerCase().includes(query.toLowerCase()))
    .sort((a,b) => Number(b.overdue)-Number(a.overdue) || (b.absolute_error ?? -1)-(a.absolute_error ?? -1) || a.name.localeCompare(b.name)), [positionRows, query]);
  const summary = reportSummary(rows);
  const selected = rows.find(r => r.player_id === playerId) ?? rows[0];
  const initialTrajectory = reports.flatMap(r => r.rows.filter(p => p.player_id === selected?.player_id && p.variant === variant)
    .map(p => ({ week: p.week, expected: p.forecast?.mean, P10: p.forecast?.p10, P90: p.forecast?.p90, actual: p.actual })));
  const selectedId = selected?.player_id;
  const historyKey = `${season}:${variant}:${selectedId}`;
  const [history, setHistory] = useState<{ key: string; rows: Awaited<ReturnType<typeof loadPlayerHistory>>; error?: string } | null>(null);
  useEffect(() => {
    let active = true;
    if (selectedId) loadPlayerHistory(season, selectedId, variant).then(rows => {
      if (active) setHistory({ key: historyKey, rows });
    }).catch(() => { if (active) setHistory({ key: historyKey, rows: [], error: "Earlier weekly history could not be loaded." }); });
    return () => { active = false; };
  }, [season, selectedId, variant, historyKey]);
  const trajectory = history?.key === historyKey && !history.error ? history.rows : initialTrajectory;
  const stale = report ? viewedAt - new Date(report.evaluated_at).getTime() > 36*3600000 : false;
  function download() {
    const blob = new Blob([JSON.stringify({ ...report, filtered_rows: rows }, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob), anchor = document.createElement("a");
    anchor.href = url; anchor.download = `nfl-${season}-week-${week}-${variant}-audit.json`; anchor.click(); URL.revokeObjectURL(url);
  }
  return <main className="mx-auto max-w-[1600px] space-y-5 p-4 sm:p-6">
    <header className="rounded-2xl bg-slate-950 p-6 text-white">
      <Link className="text-sm text-emerald-300" href="/dfs/nfl">← NFL DFS workspace</Link>
      <p className="mt-5 text-xs font-bold uppercase tracking-widest text-emerald-300">Forecast accountability · {season}</p>
      <h1 className="mt-1 text-3xl font-black">Weekly Player Review</h1>
      <p className="mt-2 max-w-3xl text-sm text-slate-300">Frozen pregame forecasts, realized scores and every unresolved result. Production and research models stay separate. These are projection metrics, not lineup ROI.</p>
    </header>
    <form className="flex gap-2 text-sm"><label>Season <input aria-label="Season" className="w-24 rounded border p-2" name="season" type="number" min="2000" max="2099" defaultValue={season}/></label><button className="rounded border px-3">Load season</button></form>
    {!report ? <section className="rounded-xl border p-8"><h2 className="font-bold">No saved weekly reports yet</h2><p className="mt-2">The daily report-card job must run for this season. No sample outcomes are substituted.</p></section> : <>
      <section className="flex flex-wrap gap-3 rounded-xl border bg-white p-4">
        <label className="text-xs font-semibold">Week<select aria-label="Week" className="mt-1 block rounded border p-2" value={week} onChange={e => router.push(`/dfs/nfl/review?season=${season}&week=${e.target.value}`)}>{availableWeeks.map(w => <option key={w} value={w}>Week {w}</option>)}</select></label>
        <label className="text-xs font-semibold">Model<select aria-label="Model" className="mt-1 block rounded border p-2" value={variant} onChange={e => setVariant(e.target.value as ReportVariant)}>{Object.entries(VARIANT_LABELS).map(([v,l]) => <option key={v} value={v}>{l}</option>)}</select></label>
        <label className="text-xs font-semibold">Position<select aria-label="Position" className="mt-1 block rounded border p-2" value={position} onChange={e => setPosition(e.target.value as ReviewPosition)}>{REVIEW_POSITIONS.map(p => <option key={p}>{p}</option>)}</select></label>
        <label className="flex-1 text-xs font-semibold">Player or team<input aria-label="Player or team" className="mt-1 block w-full rounded border p-2" value={query} onChange={e => setQuery(e.target.value)} placeholder="Search player"/></label>
        <button onClick={download} className="self-end rounded border px-3 py-2 text-sm font-semibold">Download audit</button>
      </section>
      <p className="text-xs text-slate-600">Saved {date(report.evaluated_at)} ET · {report.completed_games}/{report.scheduled_games} games completed · {report.rejected_non_pregame_snapshots} invalid/unmapped snapshots excluded · {report.checkpoint}</p>
      {(stale || summary.overdue > 0) && <div role="alert" className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm">{stale ? "Report is older than 36 hours. " : ""}{summary.overdue > 0 ? `${summary.overdue} player rows lack scorable results more than 48 hours after kickoff. Check source coverage.` : ""}</div>}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-6">{[
        ["Forecast coverage", `${summary.forecasted}/${summary.players}`], ["Scored / unscored", `${summary.scored} / ${summary.unscored}`],
        ["Average absolute error", fmt(summary.mae)], ["Actual − projected", fmt(summary.bias)],
        ["P10–P90 coverage", summary.coverage === null ? "—" : `${(summary.coverage*100).toFixed(0)}%`], ["Overdue results", String(summary.overdue)],
      ].map(([title,value]) => <div key={title} className="rounded-xl border bg-white p-4"><p className="text-xs text-slate-500">{title}</p><p className="mt-2 text-2xl font-bold">{value}</p></div>)}</div>
      <p className="text-xs text-slate-500">Metrics reflect the filters above. P10–P90 targets roughly 80% coverage, not guaranteed bounds. {report.population}. {report.missing_policy}</p>
      <section className="grid gap-4 md:grid-cols-2">
        <MoverList title="Top 10 — exceeded projection" note="Largest positive delta (final minus projected) among scored players." rows={movers.exceeded} tone="exceeded" scales={scales} widest={moverBarScale}/>
        <MoverList title="Top 10 — disappointed" note="Largest negative delta among scored players." rows={movers.disappointed} tone="disappointed" scales={scales} widest={moverBarScale}/>
      </section>
      <p className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
        <span className="font-semibold text-slate-700">Delta heat</span>
        <span>worse</span>
        {([-2,-1,0,1,2] as const).map(b => <span key={b} aria-hidden className="h-4 w-6 rounded border"
          style={{ backgroundColor: DELTA_FILL[b] ?? "transparent" }}/>)}
        <span>better</span>
        <span>· shaded relative to the biggest swing at that player&apos;s own position, so a tight-end week is not judged on a quarterback&apos;s scale. Unscored rows are never shaded.</span>
      </p>
      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)]">
        <div className="max-h-[650px] overflow-auto rounded-xl border bg-white"><table className="w-full text-left text-xs"><thead className="sticky top-0 bg-slate-100"><tr>{["Player", "Team", "Projected", "P10–P90", "Final", "Delta", "Status"].map(c => <th className="p-3" key={c}>{c}</th>)}</tr></thead><tbody>{rows.map(r => <tr key={`${r.player_id}:${r.game_id}`} className={`border-t ${selected === r ? "bg-slate-100" : ""}`}><td className="p-3"><button className="text-left font-bold hover:underline" onClick={() => setPlayerId(r.player_id)}>{r.name}</button><div className="text-slate-500">{r.position} · vs {r.opponent}</div></td><td className="p-3 font-medium">{r.team}</td><td className="p-3 tabular-nums">{fmt(r.forecast?.mean)}</td><td className="whitespace-nowrap p-3 tabular-nums text-slate-500">{fmt(r.forecast?.p10)} – {fmt(r.forecast?.p90)}</td><td className="p-3 tabular-nums">{fmt(r.actual)}</td><td className="p-3 font-semibold tabular-nums" style={{ backgroundColor: DELTA_FILL[deltaBucket(rowDelta(r), scales[r.position])] }}>{signed(rowDelta(r))}{(() => { const p = proposalFor(r); return p && NEEDS_ATTENTION.has(p.verdict) ? <span className="ml-1" title={`${VERDICT_LABEL[p.verdict]} — ${p.reason}`}>{VERDICT_MARK[p.verdict]}</span> : null; })()}</td><td className="p-3">{label(r.status)}{r.overdue ? " · overdue" : ""}</td></tr>)}</tbody></table>{!rows.length && <p className="p-5">No matching player rows.</p>}</div>
        {selected && <aside className="space-y-4 rounded-xl border bg-white p-5"><div><h2 className="text-xl font-bold">{selected.name}</h2><p className="text-xs text-slate-500">{VARIANT_LABELS[variant]} · {selected.forecast?.history_games ?? 0} prior games</p></div>
          <div className="h-64" role="img" aria-label={`Weekly projected score, P10, P90 and actual score for ${selected.name}`}><ResponsiveContainer width="100%" height="100%"><ComposedChart data={trajectory}><CartesianGrid strokeDasharray="3 3"/><XAxis dataKey="week"/><YAxis/><Tooltip/><Line dataKey="P10" stroke="#94a3b8" strokeDasharray="3 3" connectNulls={false}/><Line dataKey="P90" stroke="#64748b" strokeDasharray="3 3" connectNulls={false}/><Line dataKey="expected" stroke="#2563eb" strokeWidth={2} connectNulls={false}/><Line dataKey="actual" stroke="#059669" strokeWidth={2} connectNulls={false}/></ComposedChart></ResponsiveContainer></div>
          <p className="text-xs text-slate-500">Blue: expected · Green: actual · Dashed: P10/P90. Dots remain visible with one week. No actual point is drawn while results are missing.</p>
          {history?.key === historyKey && history.error && <p role="alert" className="text-xs text-amber-800">{history.error} Showing the selected week only.</p>}
          <div className="grid grid-cols-3 gap-2 text-xs"><div>Median<strong className="block">{fmt(selected.forecast?.median)}</strong></div><div>Boom probability<strong className="block">{selected.forecast?.boom_probability == null ? "—" : `${(selected.forecast.boom_probability*100).toFixed(1)}%`}</strong></div><div>Within range<strong className="block">{selected.interval_hit === null ? "Pending" : selected.interval_hit ? "Yes" : "No"}</strong></div></div>
          <h3 className="font-semibold">Availability read</h3><AvailabilityNote proposal={proposalFor(selected)}/>
          <h3 className="font-semibold">Component breakdown</h3>{selected.components.length ? <table className="w-full text-xs"><thead><tr><th className="text-left">Stat</th><th>Projected</th><th>Actual</th></tr></thead><tbody>{selected.components.map(c => <tr key={c.stat} className="border-t"><td className="py-1">{label(c.stat)}</td><td className="text-center">{fmt(c.projected)}</td><td className="text-center">{fmt(c.actual)}</td></tr>)}</tbody></table> : <p className="text-xs text-slate-500">Component forecasts were not frozen for this snapshot/model. They are not reconstructed after the game.</p>}
          <details className="rounded border p-3 text-xs"><summary className="cursor-pointer font-semibold">Audit evidence & revisions</summary><p className="mt-2">Kickoff: {date(selected.kickoff)} ET</p><p>Captured: {selected.forecast ? `${date(selected.forecast.captured_at)} ET` : "No accepted forecast"}</p><p>Outcome revisions: {selected.result_revision_count} · Scorer: {selected.scoring_version ?? "pending"}</p><pre className="mt-2 max-h-80 overflow-auto whitespace-pre-wrap break-all">{JSON.stringify(selected, null, 2)}</pre></details>
        </aside>}
      </section>
    </>}
  </main>;
}
