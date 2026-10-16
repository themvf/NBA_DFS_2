import { sql } from "drizzle-orm";
import { db } from "@/db";
import { GHOST_ARMS, GHOST_DESIGN_VERSION } from "@/lib/nfl-dfs/ghost/design";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Row = Record<string, any>;
const rows = (r: unknown) => (r as { rows: Row[] }).rows;
const money = (n: number | null | undefined) => (n == null ? "—" : `$${Math.round(n).toLocaleString()}`);
const et = (iso: string) => new Date(iso).toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });

/**
 * The automatic ghost-lineup study (docs/nfl-ghost-factorial-study.md): what
 * was built for each gradable slate, how the house, the arms and your own
 * builds did against the flagship, and where the factor screening stands.
 * Your builds appear as their own rows; the house never reads them.
 */
export async function GhostStudyCard() {
  let freezes: Row[] = [], results: Row[] = [], reports: Row[] = [], failures: Row[] = [];
  try {
    freezes = rows(await db.execute(sql`SELECT f.freeze_id, f.format, f.season, f.week, f.first_kickoff, f.teams,
        (SELECT count(*)::int FROM nfl_dfs_ghost_arms a WHERE a.freeze_id = f.freeze_id) AS arms,
        (SELECT count(*)::int FROM nfl_dfs_ghost_arms a WHERE a.freeze_id = f.freeze_id AND a.built < a.requested) AS short
      FROM nfl_dfs_ghost_freezes f WHERE f.design_version = ${GHOST_DESIGN_VERSION} ORDER BY f.first_kickoff DESC LIMIT 12`));
    results = rows(await db.execute(sql`SELECT r.freeze_id, r.subject, r.scored, r.top1_hits, r.cashes, r.payout, r.entry_fee, r.best_score,
        (r.detail->>'built')::int AS built FROM nfl_dfs_ghost_results r
      JOIN nfl_dfs_ghost_freezes f ON f.freeze_id = r.freeze_id WHERE f.design_version = ${GHOST_DESIGN_VERSION}`));
    reports = rows(await db.execute(sql`SELECT DISTINCT ON (format) format, slates, milestone, report, created_at FROM nfl_dfs_ghost_reports
      WHERE design_version = ${GHOST_DESIGN_VERSION} ORDER BY format, created_at DESC`));
    failures = rows(await db.execute(sql`SELECT at, stage, detail FROM nfl_dfs_ghost_events WHERE design_version = ${GHOST_DESIGN_VERSION}
      AND NOT ok AND at > now() - interval '8 days' ORDER BY at DESC LIMIT 3`));
  } catch (error) {
    return <section className="rounded-xl border border-rose-300 bg-rose-50 p-4 text-sm text-rose-900">
      Ghost-lineup study could not be read: {error instanceof Error ? error.message : "unknown error"}
    </section>;
  }
  const bySlate = new Map<string, Row[]>();
  for (const r of results) bySlate.set(String(r.freeze_id), [...(bySlate.get(String(r.freeze_id)) ?? []), r]);
  const roi = (r: Row) => (r.payout != null && r.entry_fee && r.scored ? r.payout / (r.entry_fee * r.scored) - 1 : null);
  const pct = (n: number | null) => (n == null ? "—" : `${n > 0 ? "+" : ""}${Math.round(n * 100)}%`);
  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <h2 className="font-bold">Ghost lineups (automatic)</h2>
    <p className="mt-1 max-w-3xl text-sm text-slate-500">
      Before every Sunday main slate and primetime Showdown, the system builds a house portfolio (the current framework) and {GHOST_ARMS} test
      portfolios that each change several settings at once, then grades them all against the flagship contest on Tuesday. Nothing here
      is entered, and your own builds are graded beside them without being used.
    </p>
    <div className="mt-3 grid gap-2 md:grid-cols-2">
      {(["classic", "showdown"] as const).map(f => {
        const r = reports.find(x => x.format === f);
        return <div key={f} className="rounded-lg border bg-slate-50 p-3 text-sm">
          <div className="font-semibold capitalize">{f}</div>
          <div className="text-slate-600">{r ? `${r.slates} of ${r.report?.milestone_slates ?? 8} slates graded` : "No graded slate yet"}{r?.milestone ? ", screening reached" : ""}</div>
          {r?.report?.screening?.top1_rate?.candidates?.length ? <div className="mt-1 text-emerald-800">Candidates to confirm: {r.report.screening.top1_rate.candidates.join(", ")}</div>
            : <div className="mt-1 text-slate-500">Setting effects are withheld until 8 graded slates, then any candidate is re-tested on 8 fresh slates before a default changes.</div>}
        </div>;
      })}
    </div>
    {failures.length ? <div className="mt-3 rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900">
      Recent problem{failures.length > 1 ? "s" : ""}: {failures.map(f => `${et(new Date(f.at).toISOString())} ${f.stage}: ${f.detail}`).join(" · ")}
    </div> : null}
    {!freezes.length ? <p className="mt-3 text-sm text-slate-500">No slate has been frozen yet. The first runs about 90 minutes before the next gradable kickoff.</p>
      : <table className="mt-3 w-full text-sm">
        <thead className="text-left text-xs uppercase text-slate-500"><tr>
          <th className="py-1">Slate</th><th>Built</th><th>House top 1%</th><th>House return</th><th>Best arm return</th><th>Your builds</th></tr></thead>
        <tbody>{freezes.map(f => {
          const rs = bySlate.get(String(f.freeze_id)) ?? [];
          const house = rs.find(r => r.subject === "arm:0");
          const arms = rs.filter(r => String(r.subject).startsWith("arm:") && r.subject !== "arm:0");
          const best = arms.map(roi).filter((v): v is number => v != null).sort((a, b) => b - a)[0] ?? null;
          const owners = rs.filter(r => String(r.subject).startsWith("owner:"));
          return <tr key={String(f.freeze_id)} className="border-t">
            <td className="py-1">{f.season} wk {f.week} {f.format === "classic" ? "main Classic" : (f.teams as string[]).join(" / ")} · {et(new Date(f.first_kickoff).toISOString())}</td>
            <td>{f.arms}/{GHOST_ARMS + 1}{f.short ? ` (${f.short} short)` : ""}</td>
            <td>{house ? `${house.top1_hits}/${house.scored}` : "not graded"}</td>
            <td>{house ? `${money(house.payout)} · ${pct(roi(house))}` : "—"}</td>
            <td>{pct(best)}</td>
            <td>{owners.length ? owners.map(o => `${o.top1_hits}/${o.scored} top 1%, ${pct(roi(o))}`).join("; ") : "none"}</td>
          </tr>;
        })}</tbody>
      </table>}
  </section>;
}
