export const dynamic = "force-dynamic";

import Link from "next/link";
import type { Metadata } from "next";
import { easternDateString } from "@/lib/eastern-date";
import { getBoomTrioDays } from "@/db/nba-boom-trios";
import {
  BOOM_MIN, ONE_SWAP_MAX_DROP, REVIEW_AFTER_DATES, TWO_SWAP_MAX_DROP,
  gradeBoomTrios, selectBoomTrios, type BoomGrade, type BoomGradeRow, type BoomSelection, type Trio, type TrioResult,
} from "@/lib/nba-boom-trios";

export const metadata: Metadata = {
  title: "NBA Boom Trios",
  description: "Tonight's top-three scorers beside alternatives that give a seat to players with rising shot volume.",
};

function longDate(d: string) {
  return new Date(`${d}T12:00:00`).toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric" });
}

const signed = (v: number) => `${v > 0 ? "+" : ""}${v.toFixed(1)}`;

function actualOf(rows: BoomGradeRow[] | null, eventId: string | number, key: string): { pts: number | null; status: string | null } {
  const r = rows?.find((x) => String(x.event_id) === String(eventId) && x.player_key === key);
  if (!r) return { pts: null, status: null };
  return { pts: r.status === "graded" || r.status === "push" ? (r.actual_points ?? null) : null, status: r.status ?? null };
}

function ResultLine({ result, isBaseline, baseline }: { result?: TrioResult; isBaseline: boolean; baseline: number | null }) {
  if (isBaseline) return baseline == null ? null : <p className="text-sm font-semibold">Actual {baseline.toFixed(0)}</p>;
  if (!result) return null;
  if (result.status === "graded") {
    const tone = result.delta > 0 ? "text-emerald-700" : result.delta < 0 ? "text-rose-700" : "text-slate-600";
    return <p className={`text-sm font-semibold ${tone}`}>Actual {result.actual.toFixed(0)} ({signed(result.delta)} vs baseline)</p>;
  }
  return result.status === "incomplete" ? <p className="text-xs text-muted-foreground">Not graded: a player didn&apos;t play or isn&apos;t resolved yet.</p> : null;
}

function TrioCard({ title, subtitle, trio, rows, result, baseline, isBaseline = false }: {
  title: string; subtitle: string; trio: Trio | null; rows: BoomGradeRow[] | null; result?: TrioResult; baseline: number | null; isBaseline?: boolean;
}) {
  return (
    <section className="rounded-xl border bg-white p-4 shadow-sm">
      <h2 className="font-bold">{title}</h2>
      <p className="mb-3 text-xs text-muted-foreground">{subtitle}</p>
      {!trio ? <p className="text-sm text-muted-foreground">No rising-volume player inside the points limit tonight, so no alternative.</p> : (
        <>
          <table className="w-full text-sm">
            <thead><tr className="text-left text-[10px] uppercase text-slate-500"><th>Player</th><th className="whitespace-nowrap pl-2 text-right">Line</th><th className="whitespace-nowrap pl-2 text-right" title="Shot attempts per game (free throws x 0.44), last 3 games / last 20">Shots 3g / 20g</th>{rows ? <th className="pl-2 text-right">Pts</th> : null}</tr></thead>
            <tbody>
              {trio.members.map((m) => {
                const a = actualOf(rows, m.event_id, m.player_key);
                return (
                  <tr key={m.player_key} className="border-t">
                    <td className="py-1">{m.player}{m.role === "boom" ? <span className="ml-1 rounded bg-violet-100 px-1 text-[10px] font-semibold text-violet-800">boom</span> : null}</td>
                    <td className="pl-2 text-right">{m.line.toFixed(1)}</td>
                    <td className="whitespace-nowrap pl-2 text-right text-xs text-slate-600">{m.shots_last3 != null && m.shots_last20 != null ? `${m.shots_last3.toFixed(1)} / ${m.shots_last20.toFixed(1)}` : "—"}</td>
                    {rows ? <td className="pl-2 text-right">{a.pts != null ? a.pts.toFixed(0) : a.status === "dnp" ? "DNP" : "—"}</td> : null}
                  </tr>
                );
              })}
            </tbody>
          </table>
          <p className="mt-2 text-sm">Projected {trio.projected_total.toFixed(1)}{trio.projected_drop != null ? ` (${trio.projected_drop > 0 ? "−" : ""}${trio.projected_drop.toFixed(1)})` : ""}</p>
          {trio.why ? <p className="mt-1 text-xs text-slate-700">{trio.why}</p> : null}
          <div className="mt-2"><ResultLine result={result} isBaseline={isBaseline} baseline={baseline} /></div>
        </>
      )}
    </section>
  );
}

function recordSummary(days: { selection: BoomSelection; grade: BoomGrade | null }[], name: "boom" | "aggressive") {
  const graded = days.filter((d) => d.selection.counts && d.grade?.[name].status === "graded").map((d) => (d.grade![name] as Extract<TrioResult, { status: "graded" }>).delta);
  return { n: graded.length, helped: graded.filter((x) => x > 0).length, hurt: graded.filter((x) => x < 0).length,
    mean: graded.length ? graded.reduce((a, b) => a + b, 0) / graded.length : null };
}

export default async function BoomTriosPage({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const params = await searchParams;
  const loaded = await getBoomTrioDays();
  const days = loaded.map(({ snapshot, rows }) => {
    const selection = selectBoomTrios(snapshot);
    return { selection, rows, grade: rows ? gradeBoomTrios(selection, rows) : null };
  });
  const today = easternDateString();
  const dates = days.map((d) => d.selection.game_date);
  const date = params.date && dates.includes(params.date) ? params.date : dates.includes(today) ? today : dates[dates.length - 1];
  const day = days.find((d) => d.selection.game_date === date);
  const counted = days.filter((d) => d.selection.counts).length;
  const boomRec = recordSummary(days, "boom"), aggRec = recordSummary(days, "aggressive");
  const tag = (s: BoomSelection) => (s.counts ? "counts" : s.snapshot_mode === "prospective" ? "before the rules were frozen" : "rebuilt after the fact");

  return (
    <main className="mx-auto max-w-6xl space-y-5 p-4">
      <header className="space-y-1">
        <h1 className="text-2xl font-black">NBA Boom Trios</h1>
        <p className="text-sm text-slate-700">
          The three highest points lines, beside two alternatives that give a seat to players whose shot volume is rising: shot attempts
          per game (each free throw counted as 0.44) over the last 3 games minus the last 20, at least {BOOM_MIN.toFixed(0)}. That only
          decides who replaces whom; it never adds to the projection. Boom swaps the third player for one within {ONE_SWAP_MAX_DROP.toFixed(0)} projected
          points; aggressive keeps the top player and takes the best pair within {TWO_SWAP_MAX_DROP.toFixed(0)}.
        </p>
        <p className="text-xs text-muted-foreground">
          Rules frozen 2026-10-31 (nba-boom-trios-v1, docs/nba-boom-trios.md). Nights from Nov 1 count; {counted} counted so far, first review at {REVIEW_AFTER_DATES}.
          An estimate from the frozen pregame snapshot, not a recommendation.
        </p>
      </header>

      {!day ? <p className="rounded-lg border p-4 text-sm">No frozen pregame snapshot yet. Snapshots are taken each game day before the first tip.</p> : (
        <>
          <nav className="flex flex-wrap items-center gap-2 text-sm">
            {dates.map((d) => <Link key={d} href={`/nba/boom-trios?date=${d}`} className={`rounded border px-2 py-1 ${d === date ? "bg-slate-900 text-white" : "hover:bg-slate-100"}`}>{d.slice(5)}</Link>)}
          </nav>
          <p className="text-sm"><b>{longDate(date)}</b> · {day.selection.pool} players with a line · <span className="text-muted-foreground">{tag(day.selection)}</span>{day.grade ? "" : " · results not in yet"}</p>
          <div className="grid gap-4 lg:grid-cols-3">
            <TrioCard title="Baseline" subtitle="The three highest points lines." trio={day.selection.baseline} rows={day.rows} baseline={day.grade?.baseline ?? null} isBaseline />
            <TrioCard title="Boom" subtitle={`Keep the top two; swap the third for a rising-volume player within ${ONE_SWAP_MAX_DROP.toFixed(0)} points.`} trio={day.selection.boom} rows={day.rows} result={day.grade?.boom} baseline={day.grade?.baseline ?? null} />
            <TrioCard title="Aggressive" subtitle={`Keep the top player; two rising-volume players within ${TWO_SWAP_MAX_DROP.toFixed(0)} points.`} trio={day.selection.aggressive} rows={day.rows} result={day.grade?.aggressive} baseline={day.grade?.baseline ?? null} />
          </div>
        </>
      )}

      <section className="rounded-xl border bg-white p-4 shadow-sm">
        <h2 className="font-bold">Record against the baseline</h2>
        <p className="mb-2 text-xs text-muted-foreground">
          Counted nights only: boom {boomRec.n ? `helped ${boomRec.helped}, hurt ${boomRec.hurt}, average ${signed(boomRec.mean!)}` : "none graded yet"};
          aggressive {aggRec.n ? `helped ${aggRec.helped}, hurt ${aggRec.hurt}, average ${signed(aggRec.mean!)}` : "none graded yet"}. Earlier nights are listed for reference and do not count.
        </p>
        <table className="w-full text-sm">
          <thead><tr className="text-left text-[10px] uppercase text-slate-500"><th>Night</th><th>Status</th><th className="text-right">Baseline</th><th className="text-right">Boom</th><th className="text-right">Aggressive</th></tr></thead>
          <tbody>
            {[...days].reverse().map((d) => {
              const cell = (r?: TrioResult) => !r ? "—" : r.status === "graded" ? signed(r.delta) : r.status === "no_alternative" ? "no alt" : "—";
              return (
                <tr key={d.selection.game_date} className="border-t">
                  <td className="py-1"><Link className="text-blue-700 underline" href={`/nba/boom-trios?date=${d.selection.game_date}`}>{d.selection.game_date}</Link></td>
                  <td className="text-xs text-muted-foreground">{tag(d.selection)}</td>
                  <td className="text-right">{d.grade?.baseline != null ? d.grade.baseline.toFixed(0) : "—"}</td>
                  <td className="text-right">{cell(d.grade?.boom)}</td>
                  <td className="text-right">{cell(d.grade?.aggressive)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>
    </main>
  );
}
