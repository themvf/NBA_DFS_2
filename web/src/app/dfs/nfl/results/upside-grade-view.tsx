import { UPSIDE_GRADE_VERSION } from "@/lib/nfl-dfs/replacement-upside-grade";

/*
 * Status of the automatic weekly grade of "if he gets the job"
 * (docs/nfl-replacement-upside-grading.md). Blinded runs carry counts only, so
 * this card shows progress toward the floors and never an outcome until the
 * frozen verdict exists.
 */

type Floors = Record<"events" | "flagged" | "weeks", { required: number; have: number }>;
type Interval = { n: number; mean: number | null; lo: number | null; hi: number | null };

const VERDICT_LABELS: Record<string, string> = {
  PROMOTE: "Passed: promote ceiling and boom", PROMOTE_CEILING_ONLY: "Passed: promote ceiling only",
  NOT_PROMOTED_GENERIC: "Not promoted: a generic widening did as well", NOT_PROMOTED: "Not promoted",
  RETIRE: "Failed: remove the second range",
};
const FLOOR_LABELS: Record<keyof Floors, string> = {
  events: "Starter absences", flagged: "Flagged player-games", weeks: "Weeks",
};
const when = (iso: string) => new Date(iso).toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) + " ET";
const ci = (i: Interval | undefined) => (i?.mean == null ? "—" : `${i.mean.toFixed(2)} [${i.lo?.toFixed(2) ?? "—"}, ${i.hi?.toFixed(2) ?? "—"}]`);

export interface UpsideGradeStatus {
  latest: { evaluatedAt: string; report: Record<string, unknown> } | null;
  verdict: { verdict: string; frozenAt: string; payload: Record<string, unknown> } | null;
  runs: number;
}

/** Pure view (no database), so every state can be rendered and checked. */
export function UpsideGradeView({ status, error }: { status: UpsideGradeStatus | null; error: string | null }) {
  const report = status?.latest?.report as { floors?: Floors; health?: { completedGamesInWindow: number; gamesWithFeatureCapture: number } } | undefined;
  const verdict = status?.verdict;
  const metrics = verdict?.payload.metrics as Record<string, Interval> | undefined;

  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <div className="flex flex-wrap items-baseline justify-between gap-2">
      <h2 className="font-bold">&quot;If he gets the job&quot; grade</h2>
      <span className="text-xs text-slate-500">{UPSIDE_GRADE_VERSION} · runs itself Tuesday and Wednesday after DraftKings results</span>
    </div>

    {error ? <p className="mt-2 text-sm text-rose-700">Grade status unavailable: {error}</p>
      : !status?.latest ? <p className="mt-2 text-sm text-slate-600">
        Not run yet. The first automatic run that can count anything is Tuesday 6 October, after week 4&apos;s Monday game.
      </p>
      : verdict ? <div className="mt-2 space-y-2 text-sm">
        <p><span title={verdict.verdict} className="rounded bg-violet-100 px-2 py-0.5 font-bold text-violet-900">{VERDICT_LABELS[verdict.verdict] ?? verdict.verdict}</span>
          <span className="ml-2 text-slate-600">frozen {when(verdict.frozenAt)}; later runs cannot change it.</span></p>
        <p className="text-slate-700">{String(verdict.payload.meaning ?? "")}</p>
        <table className="text-xs"><tbody>
          <tr><td className="pr-3 text-slate-500">Ceiling vs baseline (G1)</td><td>{ci(metrics?.g1CeilingVsBaseline)}</td></tr>
          <tr><td className="pr-3 text-slate-500">Ceiling vs generic widening (G2)</td><td>{ci(metrics?.g2CeilingVsWidened)}</td></tr>
          <tr><td className="pr-3 text-slate-500">Boom log loss (G3)</td><td>{ci(metrics?.g3BoomLogLoss)}</td></tr>
        </tbody></table>
        <p className="text-xs text-slate-500">Negative means the &quot;if he gets the job&quot; range forecast better. G1 and G2 pass when the whole interval is below zero.</p>
      </div>
      : <div className="mt-2 space-y-2 text-sm">
        <p className="text-slate-700">Blinded: no results are shown until every count below is reached, so the test cannot be stopped early on a good or bad week.</p>
        <div className="grid gap-2 md:grid-cols-3">{report?.floors ? (Object.keys(FLOOR_LABELS) as (keyof Floors)[]).map((key) => {
          const f = report.floors![key];
          const pct = Math.min(100, Math.round((f.have / f.required) * 100));
          return <div key={key}>
            <div className="flex justify-between text-xs"><span className="text-slate-500">{FLOOR_LABELS[key]}</span><span className="font-semibold">{f.have} of {f.required}</span></div>
            <div className="mt-1 h-1.5 rounded bg-slate-100"><div className="h-1.5 rounded bg-violet-400" style={{ width: `${pct}%` }} /></div>
          </div>;
        }) : null}</div>
        <p className="text-xs text-slate-500">
          Last run {when(status.latest.evaluatedAt)} ({status.runs} so far).
          {report?.health ? ` Finished games with a pregame copy of the upside: ${report.health.gamesWithFeatureCapture} of ${report.health.completedGamesInWindow}.` : ""}
        </p>
      </div>}
  </section>;
}
