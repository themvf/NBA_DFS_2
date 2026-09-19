"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { VARIANT_LABELS, type ReportRow, type ReportVariant, type WeeklyReport } from "@/lib/nfl-dfs/report-card";
import { delta as rowDelta, matchesReviewPosition, REVIEW_POSITIONS, type ReviewPosition } from "@/lib/nfl-dfs/review-insights";
import { ALL_VERDICTS, ATTENTION_VERDICTS, buildAvailability, countByVerdict,
  sortAvailability, type AvailabilityEntry } from "@/lib/nfl-dfs/availability-board";
import { VERDICT_LABEL, VERDICT_MARK, type Verdict } from "@/lib/nfl-dfs/removal";

const fmt = (v: number | null | undefined) => v == null ? "—" : v.toFixed(1);
const signed = (v: number | null | undefined) => {
  if (v == null) return "—";
  // Round first, then sign: a value that rounds to zero is neither positive
  // nor negative at this precision, and "-0.0" reads as a bias that isn't there.
  const r = round(v);
  return r === 0 ? "0.0" : `${r > 0 ? "+" : ""}${r.toFixed(1)}`;
};
const round = (v: number) => Number((Math.round(v * 10) / 10).toFixed(1)) + 0;

// Tone carries meaning, so it is keyed on the verdict rather than on the
// delta: a big miss that the player was present for is not a concern here.
const TONE: Record<Verdict, string> = {
  INJURED_OUT: "bg-rose-50 text-rose-900",
  LAST_SEEN_EARLY: "bg-amber-50 text-amber-900",
  INJURED_RETURNED: "bg-yellow-50 text-yellow-900",
  NO_OPPORTUNITY: "bg-slate-50 text-slate-700",
  PLAYED_LATE: "bg-emerald-50 text-emerald-900",
};

export default function AvailabilityBoard({ reports, availableWeeks, season, participants, playContext }: {
  reports: WeeklyReport[]; availableWeeks: number[]; season: number;
  participants: Parameters<typeof buildAvailability>[1];
  playContext: Parameters<typeof buildAvailability>[2];
}) {
  const router = useRouter();
  const week = reports.at(-1)?.week ?? 1;
  const [variant, setVariant] = useState<ReportVariant>("production");
  const [position, setPosition] = useState<ReviewPosition>("ALL");
  const [verdicts, setVerdicts] = useState<Set<Verdict>>(() => new Set(ATTENTION_VERDICTS));
  const [query, setQuery] = useState("");

  const report = reports.find(r => r.week === week);
  const variantRows = useMemo(
    () => (report?.rows ?? []).filter(r => r.variant === variant), [report, variant]);
  // One full scan of the week's plays, reused by every row and every repaint.
  const entries = useMemo(
    () => buildAvailability(variantRows, participants, playContext), [variantRows, participants, playContext]);
  // Counts describe the whole week, never the filtered view: a search box that
  // moved the totals would make the headline mean two different things.
  const counts = useMemo(() => entries ? countByVerdict(entries) : null, [entries]);

  const shown = useMemo(() => {
    if (!entries) return [];
    const needle = query.trim().toLowerCase();
    return sortAvailability(entries.filter(e =>
      verdicts.has(e.proposal.verdict)
      && matchesReviewPosition(e.row.position, position)
      && (!needle || `${e.row.name} ${e.row.team}`.toLowerCase().includes(needle))));
  }, [entries, verdicts, position, query]);

  const toggle = (verdict: Verdict) => setVerdicts(prev => {
    const next = new Set(prev);
    if (next.has(verdict)) next.delete(verdict); else next.add(verdict);
    return next;
  });

  return <main className="mx-auto max-w-[1500px] space-y-5 p-6 text-sm">
    <nav className="flex flex-wrap gap-4 text-sm text-emerald-800">
      <Link href="/dfs/nfl">← NFL DFS workspace</Link>
      <Link href="/dfs/nfl/review">Weekly player review →</Link>
    </nav>

    <header className="space-y-2">
      <h1 className="text-2xl font-bold">Availability Review</h1>
      <p className="max-w-3xl text-slate-600">Which players were not available to earn their
        projection. Read from play-by-play: nflverse names the injured player in the play text, and a
        touch late in the game is near-proof someone finished it. Deliberately coarse — a player who
        tweaked something and came back counts as having played.</p>
      <p className="max-w-3xl text-xs text-slate-500">Every row is a <strong>proposal</strong>, not a
        finding. Nothing here changes a projection or a score. Snap-level presence is unavailable in
        season (nflverse publishes it after the postseason), so a player who was on the field but
        never thrown to is invisible to this page.</p>
    </header>

    <section className="flex flex-wrap items-end gap-3 rounded-xl border bg-white p-4">
      <label className="text-xs font-semibold">Week
        <select aria-label="Week" className="mt-1 block rounded border p-2" value={week}
          onChange={e => router.push(`/dfs/nfl/availability?season=${season}&week=${e.target.value}`)}>
          {availableWeeks.map(w => <option key={w} value={w}>Week {w}</option>)}
        </select></label>
      <label className="text-xs font-semibold">Model
        <select aria-label="Model variant" className="mt-1 block rounded border p-2" value={variant}
          onChange={e => setVariant(e.target.value as ReportVariant)}>
          {Object.entries(VARIANT_LABELS).map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select></label>
      <label className="text-xs font-semibold">Position
        <select aria-label="Position" className="mt-1 block rounded border p-2" value={position}
          onChange={e => setPosition(e.target.value as ReviewPosition)}>
          {REVIEW_POSITIONS.map(p => <option key={p} value={p}>{p}</option>)}
        </select></label>
      <label className="text-xs font-semibold">Player
        <input aria-label="Search player" className="mt-1 block rounded border p-2" value={query}
          placeholder="Loveland" onChange={e => setQuery(e.target.value)} /></label>
    </section>

    {!entries
      ? <section className="rounded-xl border bg-white p-8">
          <h2 className="font-bold">Play-by-play not loaded for week {week}</h2>
          <p className="mt-2 text-slate-600">No availability read is offered. This is not a finding
            that everyone played — run <code>refresh_nfl_pbp_archetypes</code> for this season.</p>
        </section>
      : <>
        <section className="flex flex-wrap gap-2">
          {ALL_VERDICTS.map(v => {
            const active = verdicts.has(v);
            return <button key={v} onClick={() => toggle(v)} aria-pressed={active}
              className={`rounded-full border px-3 py-1.5 text-xs font-semibold ${active ? TONE[v] : "bg-white text-slate-400"}`}>
              {VERDICT_MARK[v]} {VERDICT_LABEL[v]} · {counts?.[v] ?? 0}
            </button>;
          })}
        </section>

        <section className="overflow-auto rounded-xl border bg-white">
          <table className="w-full text-left text-xs">
            <thead className="sticky top-0 bg-slate-100">
              <tr>{["Verdict", "Player", "Team", "Proj", "Final", "Delta", "Tgt", "Car", "Last Q", "Why"]
                .map(c => <th className="p-3" key={c}>{c}</th>)}</tr>
            </thead>
            <tbody>{shown.map(({ row, proposal }: AvailabilityEntry) => {
              const e = proposal.evidence;
              return <tr key={`${row.player_id}:${row.game_id}`} className="border-t align-top">
                <td className={`whitespace-nowrap p-3 font-semibold ${TONE[proposal.verdict]}`}>
                  {VERDICT_MARK[proposal.verdict]} {VERDICT_LABEL[proposal.verdict]}</td>
                <td className="p-3"><span className="font-bold">{row.name}</span>
                  <div className="text-slate-500">{row.position} · vs {row.opponent}</div></td>
                <td className="p-3 font-medium">{row.team}</td>
                <td className="p-3 tabular-nums">{fmt(row.forecast?.mean)}</td>
                <td className="p-3 tabular-nums">{fmt(row.actual)}</td>
                <td className="p-3 font-semibold tabular-nums">{signed(rowDelta(row))}</td>
                <td className="p-3 tabular-nums">{e.targets}</td>
                <td className="p-3 tabular-nums">{e.carries}</td>
                <td className="p-3 tabular-nums">{e.lastQuarter ?? "—"}</td>
                <td className="max-w-md p-3 text-slate-600">{proposal.reason}
                  {e.injuries.length > 0 && <div className="mt-1 text-slate-500">
                    Play-by-play names him injured: {e.injuries.map(i =>
                      `${i.name} in Q${i.quarter ?? "?"}${i.clock ? ` (${i.clock})` : ""}`).join("; ")}.
                  </div>}
                  <div className="mt-1 text-[11px] text-slate-400">{proposal.version} · {proposal.confidence} confidence</div>
                </td>
              </tr>;
            })}</tbody>
          </table>
          {!shown.length && <p className="p-6 text-slate-500">No player matches these filters.
            {verdicts.size < ALL_VERDICTS.length && " Some verdicts are switched off above."}</p>}
        </section>
      </>}
  </main>;
}
