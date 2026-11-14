import { sql } from "drizzle-orm";
import { db } from "@/db";
import { FAMILY_LABEL, headToHead, NFL_SHOWDOWN_OWNERSHIP_GRADE_VERSION, orderedModels, slatesByFamily,
  type GradedModel, type OwnershipGradeRow, type WaitingSlate } from "@/lib/nfl-dfs/showdown-ownership-grade";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Row = Record<string, any>;
const rows = (r: unknown) => (r as { rows: Row[] }).rows;
const day = (iso: string) => new Date(iso).toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric" });
const when = (iso: string) => new Date(iso).toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
const pts = (n: number | null | undefined) => (n == null ? "—" : n.toFixed(2));
const ROLE_NOTE: Record<GradedModel["role"], string> = { shown: "the build read this", ours: "ours, replaced by stat-api", shadow: "kept for comparison" };
/** Slates shown in the table; every graded slate still counts in the head-to-head rows. */
const SLATES_LISTED = 12;

/**
 * Showdown ownership after-slate grade (`nfl-showdown-ownership-grade-v1`,
 * docs/nfl-ownership-model.md): for each Showdown with real ownership
 * imported, how far every estimate stored before kickoff was from the field.
 * Written by the weekly post-week job; this card only reads.
 */
export async function OwnershipGradeCard() {
  let grades: OwnershipGradeRow[] = [], run: Row | null = null;
  try {
    const present = rows(await db.execute(sql`SELECT to_regclass('public.nfl_showdown_ownership_grades') IS NOT NULL AS grades,
      to_regclass('public.nfl_showdown_ownership_grade_runs') IS NOT NULL AS runs`))[0];
    if (present?.grades) {
      grades = rows(await db.execute(sql`SELECT DISTINCT ON (slate_key) slate_key, game, first_kickoff, season, week, contest_id, contest_name,
          entry_count, labels_complete, capture_source, captured_at, models
        FROM nfl_showdown_ownership_grades WHERE grade_version = ${NFL_SHOWDOWN_OWNERSHIP_GRADE_VERSION}
        ORDER BY slate_key, graded_at DESC, id DESC`)).map((r): OwnershipGradeRow => ({
        slateKey: String(r.slate_key), game: String(r.game), firstKickoff: new Date(r.first_kickoff).toISOString(),
        season: r.season ?? null, week: r.week ?? null, contestId: String(r.contest_id), contestName: r.contest_name ?? null,
        entryCount: r.entry_count ?? null, labelsComplete: Boolean(r.labels_complete), captureSource: String(r.capture_source),
        capturedAt: new Date(r.captured_at).toISOString(), models: Array.isArray(r.models) ? r.models : [] }))
        .sort((a, b) => b.firstKickoff.localeCompare(a.firstKickoff));
    }
    if (present?.runs) {
      run = rows(await db.execute(sql`SELECT slates, graded, new_grades, waiting, ran_at FROM nfl_showdown_ownership_grade_runs
        WHERE grade_version = ${NFL_SHOWDOWN_OWNERSHIP_GRADE_VERSION} ORDER BY id DESC LIMIT 1`))[0] ?? null;
    }
  } catch (error) {
    return <section className="rounded-xl border border-rose-300 bg-rose-50 p-4 text-sm text-rose-900">
      Showdown ownership grade could not be read: {error instanceof Error ? error.message : "unknown error"}
    </section>;
  }
  const pairs = headToHead(grades);
  const counts = slatesByFamily(grades);
  const waiting: WaitingSlate[] = Array.isArray(run?.waiting) ? run.waiting : [];
  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <h2 className="font-bold">Showdown ownership: how close were we</h2>
    <p className="mt-1 max-w-3xl text-sm text-slate-500">
      After each Showdown with real ownership imported, every ownership estimate stored before kickoff is compared with what the field
      played. Miss is the average gap per player in percentage points, captain and flex apart; lower is closer. Nothing is recomputed
      afterwards, so a model that produced no number before kickoff has no grade for that game. One row is one game.
    </p>
    {!grades.length ? <p className="mt-3 text-sm text-slate-500">
      No Showdown has been graded yet. A slate is graded once a contest&apos;s ownership is imported for it and a build or automatic freeze
      was saved at least 20 minutes before kickoff.
    </p> : <>
      <p className="mt-3 text-sm text-slate-600">
        {grades.length} slate{grades.length === 1 ? "" : "s"} graded. Stored on: {counts.map((c) => `${FAMILY_LABEL[c.family]} ${c.slates}`).join(" · ")}.
        {counts.some((c) => c.family === "lineup_model") ? "" : " The lineup model has not been stored on a graded slate yet."}
      </p>
      {pairs.length ? <>
        <h3 className="mt-4 text-sm font-bold">Head to head, on the slates where both were stored</h3>
        <table className="mt-2 w-full max-w-4xl text-left text-sm">
          <thead className="text-xs uppercase text-slate-500"><tr>
            <th className="p-2">Models</th><th className="p-2 text-right">Slates</th><th className="p-2 text-right">Flex miss</th>
            <th className="p-2 text-right">Captain miss</th><th className="p-2">Closer on flex</th>
          </tr></thead>
          <tbody>{pairs.map((p) => <tr key={`${p.a}-${p.b}`} className="border-t">
            <td className="p-2 font-semibold">{FAMILY_LABEL[p.a]} <span className="font-normal text-slate-400">vs</span> {FAMILY_LABEL[p.b]}</td>
            <td className="p-2 text-right">{p.slates}</td>
            <td className="p-2 text-right">{pts(p.aFlex)} vs {pts(p.bFlex)}</td>
            <td className="p-2 text-right">{pts(p.aCaptain)} vs {pts(p.bCaptain)}</td>
            <td className="p-2">{FAMILY_LABEL[p.a]} {p.aCloser}, {FAMILY_LABEL[p.b]} {p.bCloser}{p.slates - p.aCloser - p.bCloser ? `, level ${p.slates - p.aCloser - p.bCloser}` : ""}</td>
          </tr>)}</tbody>
        </table>
        <p className="mt-1 text-xs text-slate-500">Each slate counts once. A handful of slates describes those games; it does not settle which model is closer.</p>
      </> : null}
      <h3 className="mt-4 text-sm font-bold">By slate</h3>
      <div className="mt-2 overflow-auto">
        <table className="w-full text-left text-sm">
          <thead className="text-xs uppercase text-slate-500"><tr>
            <th className="p-2">Slate</th><th className="p-2 text-right">Field</th><th className="p-2 text-right">Players</th>
            <th className="p-2">Estimate</th><th className="p-2 text-right">Captain miss</th><th className="p-2 text-right">Flex miss</th>
            <th className="p-2">Biggest flex miss</th>
          </tr></thead>
          <tbody>{grades.slice(0, SLATES_LISTED).flatMap((g) => {
            const models = orderedModels(g);
            return models.map((m, i) => {
              const worst = m.flex?.worst?.[0];
              return <tr key={`${g.slateKey}-${m.role}`} className={i === 0 ? "border-t" : ""}>
                {i === 0 ? <>
                  <td rowSpan={models.length} className="p-2 align-top"><span className="font-semibold">{g.game.replace("@", " @ ")}</span>
                    <div className="text-xs text-slate-500">{day(g.firstKickoff)}{g.week != null ? ` · week ${g.week}` : ""}</div>
                    {g.labelsComplete ? null : <div className="text-xs text-amber-700">Real ownership is incomplete for this contest.</div>}</td>
                  <td rowSpan={models.length} className="p-2 text-right align-top">{g.entryCount == null ? "—" : g.entryCount.toLocaleString()}</td>
                  <td rowSpan={models.length} className="p-2 text-right align-top">{m.flex?.n ?? "—"}</td>
                </> : null}
                <td className="p-2">{FAMILY_LABEL[m.family]}<span className="ml-1 text-xs text-slate-400">{ROLE_NOTE[m.role]}</span></td>
                <td className="p-2 text-right">{pts(m.captain?.miss)}</td>
                <td className="p-2 text-right">{pts(m.flex?.miss)}</td>
                <td className="p-2 text-slate-600">{worst ? `${worst.name}: ${worst.estimate.toFixed(1)}% against ${worst.real.toFixed(1)}%` : "—"}</td>
              </tr>;
            });
          })}</tbody>
        </table>
      </div>
      {grades.length > SLATES_LISTED ? <p className="mt-1 text-xs text-slate-500">The {SLATES_LISTED} most recent of {grades.length} graded slates.</p> : null}
    </>}
    <p className="mt-3 text-xs text-slate-500">
      {run ? `Last checked ${when(new Date(run.ran_at).toISOString())} ET.` : "The grade has not run yet."}
      {waiting.length ? ` Waiting: ${waiting.map((w) => `${w.game.replace("@", " @ ")} (${day(w.kickoff)}): ${w.reason}`).join("; ")}.` : ""}
      {" "}Thursday and Monday flagship ownership arrives with the weekly review; any other Showdown needs its contest standings imported.
    </p>
  </section>;
}
