import type { Metadata } from "next";
import Link from "next/link";
import { readNflResultsHistory } from "../actions";
import { builtAfterStart, CHALK_CAPTAIN_PCT, CONTRARIAN_CAPTAIN_PCT, distinctSets, summarizeHistory, type Tally } from "@/lib/nfl-dfs/results-history";
import { summarizeSet } from "@/lib/nfl-dfs/slate-results";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "NFL DFS · Results history",
  description: "Every scored NFL DFS slate, with running totals by portfolio plan and captain ownership.",
};

const PLAN_LABELS: Record<string, string> = {
  balanced: "Balanced mix", chalk_leverage: "Chalk captain, rotating leverage", standard: "Standard ceiling",
  custom: "Custom plan", Classic: "Classic (all plans)",
};
/** Below this many slates a rate describes what happened; it does not predict. */
const DESCRIPTIVE_SLATES = 10;

const fmt = (n: number | null | undefined, d = 1) => (n == null ? "—" : n.toFixed(d));
const signed = (n: number | null) => (n == null ? "—" : `${n > 0 ? "+" : ""}${n.toFixed(1)}`);
const share = (part: number, whole: number) => (whole ? `${part}/${whole} (${Math.round((part / whole) * 100)}%)` : "—");

/**
 * Results across slates. One slate describes one game; this is where a plan or
 * a captain habit accumulates enough lineups to say something. Every rate is
 * shown with its count and the number of slates behind it.
 */
export default async function ResultsHistoryPage() {
  const slates = await readNflResultsHistory();
  const history = summarizeHistory(slates);
  const { overall } = history;
  return <main className="mx-auto max-w-[1400px] space-y-4">
    <Link href="/dfs/nfl" className="text-sm font-semibold text-blue-700 underline">← Lineup workspace</Link>
    <header>
      <h1 className="text-2xl font-bold tracking-tight">Results history</h1>
      <p className="mt-1 max-w-3xl text-sm text-slate-500">
        Every slate you uploaded contest results for, with each saved lineup set scored against that contest.
        A set built twice with identical lineups counts once, and a set saved after the first kickoff is shown but never counted.
      </p>
    </header>

    {!slates.length ? <section className="rounded-xl border border-dashed bg-white p-8 text-sm text-slate-600">
      No contest results yet. Upload a DraftKings contest standings file in a slate&apos;s Results step.
    </section> : <>
      {overall.slates < DESCRIPTIVE_SLATES ? <p className="rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900">
        {overall.slates} slate{overall.slates === 1 ? "" : "s"} so far. Lineups built for the same game rise and fall together,
        so treat these rates as a description of those games, not a measured edge, until there are at least {DESCRIPTIVE_SLATES} slates.
      </p> : null}

      <section className="grid gap-3 md:grid-cols-4">
        <Card label="Slates" value={String(overall.slates)} note={[`${overall.sets} lineup sets counted`, history.duplicatesDropped ? `${history.duplicatesDropped} repeat${history.duplicatesDropped === 1 ? "" : "s"}` : null, history.builtAfterStart ? `${history.builtAfterStart} built after kickoff` : null].filter(Boolean).join(" · ")} />
        <Card label="Beat the median" value={share(overall.aboveMedian, overall.lineups)} note="lineups above the contest median" />
        <Card label="Top 20%" value={share(overall.topFifth, overall.ranked)} note={overall.ranked < overall.lineups ? `of ${overall.ranked} lineups with a rank; ${overall.lineups - overall.ranked} from contests uploaded before ranks were stored` : "lineups beating 80% of the field"} />
        <Card label="Versus median" value={signed(overall.averageMargin)} note="average points above the median" />
      </section>

      <TallyTable title="By portfolio plan"
        note="Showdown plans are listed separately; Classic sets are grouped under Classic."
        rows={history.byPlan.map((row) => ({ ...row, label: PLAN_LABELS[row.group] ?? row.group }))} />

      {history.byCaptain.length ? <TallyTable title="By captain ownership (Showdown)"
        note={`Grouped by the captain's CPT ownership in the actual contest. The ${CHALK_CAPTAIN_PCT}% and ${CONTRARIAN_CAPTAIN_PCT}% cut-offs are set by judgment, not fitted to results.`}
        rows={history.byCaptain.map((row) => ({ ...row, label: row.group }))} /> : null}

      <section className="rounded-xl border bg-white p-4 shadow-sm">
        <h2 className="font-bold">Slates</h2>
        <div className="mt-3 overflow-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase text-slate-500"><tr>
              <th className="p-2">Slate</th><th className="p-2 text-right">Entries</th><th className="p-2 text-right">Median</th>
              <th className="p-2 text-right">Winner</th><th className="p-2">Set</th><th className="p-2 text-right">Best</th>
              <th className="p-2 text-right">Best rank</th><th className="p-2 text-right">Average</th>
              <th className="p-2 text-right">Beat median</th><th className="p-2 text-right">Top 20%</th>
            </tr></thead>
            <tbody>{slates.flatMap((slate) => {
              const sets = distinctSets(slate.sets).kept;
              const unranked = sets.length > 0 && sets.every((set) => set.lineups.every((l) => l.beatShare == null));
              const head = <td rowSpan={Math.max(1, sets.length)} className="p-2 align-top">
                <Link href={`/dfs/nfl?upload=${slate.uploadId}`} className="font-semibold text-blue-700 underline">{slate.label}</Link>
                <div className="text-xs text-slate-500">contest {slate.contestId}</div>
                {unranked ? <div className="text-xs text-amber-700">No ranks: re-upload the standings file to add them.</div> : null}
              </td>;
              const contest = <>
                <td rowSpan={Math.max(1, sets.length)} className="p-2 text-right align-top">{slate.entryCount.toLocaleString()}</td>
                <td rowSpan={Math.max(1, sets.length)} className="p-2 text-right align-top">{fmt(slate.medianScore)}</td>
                <td rowSpan={Math.max(1, sets.length)} className="p-2 text-right align-top">{fmt(slate.winningScore)}</td>
              </>;
              if (!sets.length) return [<tr key={slate.uploadId} className="border-t">{head}{contest}
                <td colSpan={6} className="p-2 text-slate-500">No saved lineup sets for this slate.</td></tr>];
              return sets.map((set, i) => {
                const summary = summarizeSet(set.lineups, slate.medianScore);
                const late = builtAfterStart(set, slate);
                return <tr key={set.runId} className={`${i === 0 ? "border-t" : ""} ${late ? "text-slate-400" : ""}`}>
                  {i === 0 ? <>{head}{contest}</> : null}
                  <td className="p-2">{slate.format === "classic" ? "Classic" : PLAN_LABELS[set.planKey] ?? set.planKey}
                    <span className="ml-1 text-xs text-slate-400">{new Date(set.createdAt).toLocaleString([], { weekday: "short", hour: "numeric", minute: "2-digit" })}</span>
                    {late ? <span className="ml-1 rounded bg-slate-100 px-1 text-[10px] font-bold uppercase text-slate-500">built after kickoff · not counted</span> : null}</td>
                  <td className="p-2 text-right">{summary.best ? fmt(summary.best.actual) : "—"}</td>
                  <td className="p-2 text-right">{summary.best?.rank == null ? "—" : `${summary.best.exactRank ? "" : "~"}${summary.best.rank.toLocaleString()}`}</td>
                  <td className="p-2 text-right">{fmt(summary.averageActual)}</td>
                  <td className="p-2 text-right">{summary.scored ? `${summary.aboveMedian}/${summary.scored}` : "—"}</td>
                  <td className="p-2 text-right">{summary.ranked ? `${summary.topFifth}/${summary.ranked}` : "—"}</td>
                </tr>;
              });
            })}</tbody>
          </table>
        </div>
      </section>
    </>}
  </main>;
}

function Card({ label, value, note }: { label: string; value: string; note: string }) {
  return <div className="rounded-xl border bg-white p-4 shadow-sm">
    <div className="text-[11px] font-bold uppercase text-slate-500">{label}</div>
    <div className="mt-1 text-2xl font-black">{value}</div>
    <div className="mt-0.5 text-xs text-slate-500">{note}</div>
  </div>;
}

function TallyTable({ title, note, rows }: { title: string; note: string; rows: Array<Tally & { label: string }> }) {
  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <h2 className="font-bold">{title}</h2>
    <p className="mt-1 text-xs text-slate-500">{note}</p>
    <table className="mt-3 w-full max-w-4xl text-left text-sm">
      <thead className="text-xs uppercase text-slate-500"><tr>
        <th className="p-2">Group</th><th className="p-2 text-right">Slates</th><th className="p-2 text-right">Sets</th>
        <th className="p-2 text-right">Lineups</th><th className="p-2 text-right">Beat median</th>
        <th className="p-2 text-right">Top 20%</th><th className="p-2 text-right">Versus median</th>
      </tr></thead>
      <tbody>{rows.map((row) => <tr key={row.label} className="border-t">
        <td className="p-2 font-semibold">{row.label}</td><td className="p-2 text-right">{row.slates}</td>
        <td className="p-2 text-right">{row.sets}</td><td className="p-2 text-right">{row.lineups}</td>
        <td className="p-2 text-right">{share(row.aboveMedian, row.lineups)}</td>
        <td className="p-2 text-right">{share(row.topFifth, row.ranked)}</td>
        <td className="p-2 text-right">{signed(row.averageMargin)}</td>
      </tr>)}</tbody>
    </table>
  </section>;
}
