import Link from "next/link";
import { getNflMatchupResearch } from "@/db/nfl-matchup-research";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL DFS · Matchup research", description: "Compare frozen opponent-data forecasts and complete-lineup research outcomes." };
const points = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "Unavailable";
const time = (value:string) => Number.isFinite(Date.parse(value)) ? new Intl.DateTimeFormat("en-US",{timeZone:"America/New_York",dateStyle:"medium",timeStyle:"short"}).format(new Date(value))+" ET" : "Unavailable";
const reason = (step:any) => step.family === "contact" ? "Rushing efficiency" : step.family === "pressure" ? "Passing efficiency" : "Matchup adjustment";
const salary = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? new Intl.NumberFormat("en-US", {style:"currency",currency:"USD",maximumFractionDigits:0}).format(value) : "Unavailable";

function ResearchLineupDetails({row}: {row:any}) {
  const comparison = row.shadowComparison;
  return <details className="rounded border p-3 text-sm">
    <summary>Research lineup details · {row.mode.replaceAll("_", " ")} ({row.settings.nLineups} entries)</summary>
    <p className="mt-3 text-slate-600">These ranges describe the best-scoring lineup in each portfolio across the same independent research scenarios. They are simulated portfolio outcomes, not individual player ranges or prize forecasts.</p>
    <div className="mt-2 overflow-x-auto"><table className="w-full text-left"><caption className="sr-only">Best-lineup score ranges under the research model</caption><thead><tr className="border-b"><th className="p-2">Selection</th><th>P10</th><th>Median</th><th>P90</th></tr></thead><tbody>
      {[["Baseline selection", comparison.baseline], ["Research selection", comparison.challenger]].map(([label, arm]:any) => <tr key={label} className="border-b"><th scope="row" className="p-2 font-normal">{label}</th><td>{points(arm?.construction?.p10)}</td><td>{points(arm?.construction?.p50)}</td><td>{points(arm?.construction?.p90)}</td></tr>)}
    </tbody></table></div>
    <p className="mt-2 text-xs text-slate-600">P10 and P90 mark the lower and upper edges of the middle 80% of simulated best-lineup scores.</p>
    <div className="mt-3 space-y-2">{(comparison.selectedGenerated ?? []).map((lineup:any, index:number) => <details key={lineup.lineupNumber ?? index} className="rounded border p-3">
      <summary>Research lineup {index + 1} · {salary(lineup.totalSalary)}</summary>
      <div className="mt-2 overflow-x-auto"><table className="w-full text-left"><caption className="sr-only">Complete roster for research lineup {index + 1}</caption><thead><tr className="border-b"><th className="py-2">Slot</th><th>Player</th><th>Position</th><th>Team</th><th>Salary</th></tr></thead><tbody>
        {(lineup.slots ?? []).map((slot:any, slotIndex:number) => <tr key={`${slot.slot}:${slotIndex}`} className="border-b"><td className="py-2">{slot.slot}{slot.multiplier === 1.5 ? " (1.5×)" : ""}</td><td>{slot.player?.name ?? "Unavailable"}</td><td>{slot.player?.position ?? "Unavailable"}</td><td>{slot.player?.team ?? "Unavailable"}</td><td>{salary(slot.salary)}</td></tr>)}
      </tbody></table></div>
    </details>)}</div>
    {!comparison.selectedGenerated?.length && <p className="mt-3 text-slate-600">Complete selected rosters are unavailable in this saved report.</p>}
  </details>;
}

export default async function Page({searchParams}: {searchParams:Promise<{upload?:string}>}) {
  const {upload}=await searchParams;
  const report = await getNflMatchupResearch(upload);
  return <div className="mx-auto max-w-6xl space-y-6 p-5 text-slate-900">
    <Link href="/dfs/nfl" className="text-sm text-blue-700">← NFL workspace</Link>
    <header><h1 className="text-2xl font-semibold">What the matchup data changes</h1>
      <p className="mt-2 text-sm text-slate-600">Compare frozen player forecasts, opponent-data challengers, and complete-lineup research. Production projections and saved entries stay under their approved policies.</p></header>
    {report.status === "unavailable" ? <p role="status" className="rounded-lg border p-4">{report.reason}</p> : <>
      <div className="rounded-lg border bg-amber-50 p-4 text-sm"><strong>Under evaluation</strong><p>These opponent adjustments and coherent scenarios need forward grading before becoming defaults. Contest ownership, field and payout coverage determine which tournament claims can be supported.</p>
        <p className="mt-2">Frozen {time(report.capturedAt)} · Published {time(report.publishedAt)} · {report.salaryRows} salary rows</p>
        <details className="mt-2"><summary>Saved report identity</summary><div className="break-all text-xs"><p>Report {report.reportId}</p><p>Salary upload {report.uploadId}</p><p>Baseline {report.baselineVersion}</p></div></details></div>
      <section className="space-y-3"><h2 className="text-lg font-semibold">Player projection comparison</h2>
        <p className="text-sm text-slate-600">Largest numerical research changes. A zero active adjustment preserves the current approved forecast.</p>
        <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr className="border-b"><th className="p-2">Player</th><th>Position</th><th>Frozen baseline</th><th>Research</th><th>Change</th><th>Why</th></tr></thead><tbody>
          {[...report.players].filter((row) => row.status === "under_evaluation").sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta)).slice(0, 24).map((row) => <tr key={`${row.team}:${row.name}`} className="border-b"><td className="p-2">{row.name}<span className="ml-2 text-slate-500">{row.team}</span></td><td>{row.position}</td><td>{points(row.baseline)}</td><td>{points(row.candidate)}</td><td>{row.delta > 0 ? "+" : ""}{points(row.delta)}</td><td className="py-2 text-xs">{row.ledger.map(reason).join("; ")}</td></tr>)}
        </tbody></table></div></section>
      <section className="space-y-3"><h2 className="text-lg font-semibold">Coherent game scenarios</h2>
        {report.coherent ? <><p className="text-sm">{report.coherent.coverage.modeledPlayers} modeled players · {report.coherent.manifest.history_games} paired historical games · {report.coherent.manifest.draws} draws in each independent stream</p>
          <p className="text-sm text-slate-600">Passing and receiving statistics reconcile; sacks and turnovers feed the opposing defense. This is a separate research model with measured changes to player distributions, not a preserved production forecast.</p>
          <details className="rounded border p-3 text-sm"><summary>Coverage and limitations</summary><ul className="mt-2 list-disc pl-5">{report.coherent.limitations.map((item: string) => <li key={item}>{item}</li>)}</ul></details></> : <p className="text-sm text-slate-600">A matching coherent report has not been published.</p>}
      </section>
      <section className="space-y-3"><h2 className="text-lg font-semibold">Separate tournament entry modes</h2>
        {report.portfolios ? <><p className="text-sm text-slate-600">Construction comparison only while actual contest fields and payouts are missing. Values below are simulated average best-lineup scores in each portfolio; they are not win probabilities or prize forecasts.</p>
          <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr className="border-b"><th className="p-2">Entries</th><th>Baseline selection</th><th>Research selection</th><th>Status</th></tr></thead><tbody>{report.portfolios.records.map((row: any) => <tr className="border-b" key={row.mode}><td className="p-2">{row.mode.replaceAll("_", " ")} ({row.settings.nLineups})</td><td>{points(row.baselinePortfolioUnderShadowModel.construction.mean)}</td><td>{points(row.shadowComparison.challenger?.construction.mean)}</td><td>{row.shadowComparison.status.replaceAll("_", " ")}</td></tr>)}</tbody></table></div>
          {report.portfolios.records.map((row:any) => <ResearchLineupDetails key={row.mode} row={row} />)}
          <details className="rounded border p-3 text-sm"><summary>Portfolio assumptions</summary><ul className="mt-2 list-disc pl-5">{report.portfolios.assumptions.map((item: string) => <li key={item}>{item}</li>)}</ul></details></> : <p className="text-sm text-slate-600">The matching portfolio comparison is not available yet.</p>}
      </section>
      {report.archived && <section className="space-y-3"><h2 className="text-lg font-semibold">What the saved entries actually scored</h2>
        <p className="text-sm text-slate-600">Past contest results are evaluation evidence only. Percentiles compare saved lineups with the archived field; they do not prove that a lineup was submitted or paid. These outcomes and observed ownership never enter the pregame forecast.</p>
        <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr className="border-b"><th className="p-2">Past contest</th><th>Entries graded</th><th>Best score</th><th>Mean score</th><th>Status</th></tr></thead><tbody>
          {report.archived.contests.flatMap((contest:any)=>(contest.saved_portfolio_grades ?? []).map((run:any)=><tr key={`${contest.contest_id}:${run.run_id}`} className="border-b"><td className="p-2">{contest.format} · {contest.entries.toLocaleString()} field entries</td><td>{run.graded ?? 0} / {run.generated ?? 0}</td><td>{points(run.best_points)}</td><td>{points(run.mean_points)}</td><td>{String(run.status).replaceAll("_"," ")}</td></tr>))}
        </tbody></table></div>
        <details className="rounded border p-3 text-sm"><summary>Archived result coverage</summary>{report.archived.contests.map((contest:any)=><p key={contest.contest_id}>Contest {contest.contest_id}: {contest.observed_ownership_rows} observed ownership rows. {(contest.missing ?? []).join(" ")}</p>)}</details>
      </section>}
      {report.missing.length > 0 && <p className="text-sm text-slate-600">{report.missing.join(" ")}</p>}
    </>}
  </div>;
}
