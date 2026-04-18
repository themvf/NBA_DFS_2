import { getCachedMlbBlowupCandidateReport } from "@/db/analytics-cache";

const fmt1 = (v: number | null | undefined) => (v == null ? "-" : v.toFixed(1));
const fmt2 = (v: number | null | undefined) => (v == null ? "-" : v.toFixed(2));
const fmtPct = (v: number | null | undefined) => (v == null ? "-" : `${v.toFixed(1)}%`);
const fmtDt = (v: string | null | undefined) => (v ? new Date(v).toLocaleString() : "-");

export default async function MlbBlowupCandidatePanel() {
  let report;
  try {
    report = await getCachedMlbBlowupCandidateReport();
  } catch {
    return null;
  }

  if (!report) {
    return (
      <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
        <h2 className="mb-1 text-sm font-semibold">MLB Blowup Candidate Tracking</h2>
        <p className="text-xs text-slate-700">
          No tracked MLB blowup snapshots yet. Load or refresh an MLB slate, then upload results to evaluate how the blowup list performs.
        </p>
      </div>
    );
  }

  return (
    <div className="mx-auto mt-8 max-w-5xl space-y-6 rounded-lg border bg-card p-4 text-slate-900">
      <div>
        <h2 className="mb-1 text-sm font-semibold">MLB Blowup Candidate Tracking</h2>
        <p className="text-xs text-slate-700">
          Top-12 lock-time blowup candidates from the full MLB slate. This tracks whether the live DFS page list is actually surfacing tournament-winning hitters.
        </p>
        <p className="mt-2 text-xs text-slate-600">
          Sample: {report.sample.rows} candidate rows across {report.sample.slates} completed slates
          {" | "}
          Version {report.sample.latestVersion ?? "-"}
          {" | "}
          Latest source {report.sample.latestSource ?? "-"}
          {" | "}
          Last capture {fmtDt(report.sample.latestCapturedAt)}
        </p>
      </div>

      {report.findings.length > 0 ? (
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">What The Tracking Is Saying</p>
          <div className="grid gap-2 md:grid-cols-2">
            {report.findings.map((finding) => (
              <div key={finding} className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
                {finding}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div>
        <p className="mb-2 text-xs font-medium text-slate-700">Performance By Rank</p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-slate-600">
                <th className="py-1 text-left">Rank</th>
                <th className="py-1 text-right">Rows</th>
                <th className="py-1 text-right">Avg Score</th>
                <th className="py-1 text-right">Avg Proj</th>
                <th className="py-1 text-right">Avg Actual</th>
                <th className="py-1 text-right">Beat</th>
                <th className="py-1 text-right">Avg Own</th>
                <th className="py-1 text-right">15+</th>
                <th className="py-1 text-right">20+</th>
                <th className="py-1 text-right">25+</th>
              </tr>
            </thead>
            <tbody>
              {report.rankSummary.map((row) => (
                <tr key={row.candidateRank} className="border-b border-slate-100">
                  <td className="py-1 font-medium">#{row.candidateRank}</td>
                  <td className="py-1 text-right">{row.rows}</td>
                  <td className="py-1 text-right">{fmt2(row.avgScore)}</td>
                  <td className="py-1 text-right">{fmt2(row.avgProj)}</td>
                  <td className="py-1 text-right">{fmt2(row.avgActual)}</td>
                  <td className={`py-1 text-right ${(row.avgBeat ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700"}`}>{fmt2(row.avgBeat)}</td>
                  <td className="py-1 text-right">{fmtPct(row.avgActualOwn)}</td>
                  <td className="py-1 text-right">{fmtPct(row.hit15Rate)}</td>
                  <td className="py-1 text-right">{fmtPct(row.hit20Rate)}</td>
                  <td className="py-1 text-right">{fmtPct(row.hit25Rate)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">Recent Tracked Slates</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Slate</th>
                  <th className="py-1 text-left">Version</th>
                  <th className="py-1 text-right">Avg Actual</th>
                  <th className="py-1 text-right">Avg Own</th>
                  <th className="py-1 text-right">15+</th>
                  <th className="py-1 text-right">20+</th>
                  <th className="py-1 text-right">Best</th>
                </tr>
              </thead>
              <tbody>
                {report.recentSlates.map((row) => (
                  <tr key={row.slateId} className="border-b border-slate-100">
                    <td className="py-1">
                      <div className="font-medium">{row.slateDate}</div>
                      <div className="text-[11px] text-slate-500">{fmtDt(row.capturedAt)}</div>
                    </td>
                    <td className="py-1">
                      <div className="font-medium">{row.analysisVersion}</div>
                      <div className="text-[11px] text-slate-500">{row.source}</div>
                    </td>
                    <td className="py-1 text-right">{fmt2(row.avgActual)}</td>
                    <td className="py-1 text-right">{fmtPct(row.avgActualOwn)}</td>
                    <td className="py-1 text-right">{row.hits15}</td>
                    <td className="py-1 text-right">{row.hits20}</td>
                    <td className="py-1 text-right">
                      <div>{fmt2(row.bestActual)}</div>
                      <div className="text-[11px] text-slate-500">{row.bestPlayer ?? "-"}</div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">
            Latest Completed Slate
            {report.latestSlate ? ` | ${report.latestSlate.slateDate}` : ""}
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Rank</th>
                  <th className="py-1 text-left">Player</th>
                  <th className="py-1 text-right">Order</th>
                  <th className="py-1 text-right">Salary</th>
                  <th className="py-1 text-right">Proj</th>
                  <th className="py-1 text-right">Score</th>
                  <th className="py-1 text-right">Actual</th>
                  <th className="py-1 text-right">Own</th>
                </tr>
              </thead>
              <tbody>
                {report.latestSlateRows.map((row) => (
                  <tr key={`${row.slateId}-${row.candidateRank}-${row.name}`} className="border-b border-slate-100">
                    <td className="py-1 font-medium">#{row.candidateRank}</td>
                    <td className="py-1">
                      <div className="font-medium">{row.name}</div>
                      <div className="text-[11px] text-slate-500">
                        {row.teamAbbrev ?? "-"} | {row.eligiblePositions ?? "-"}
                      </div>
                    </td>
                    <td className="py-1 text-right">{row.lineupOrder ?? "-"}</td>
                    <td className="py-1 text-right">${row.salary.toLocaleString()}</td>
                    <td className="py-1 text-right">{fmt2(row.projectedFpts)}</td>
                    <td className="py-1 text-right">{fmt2(row.blowupScore)}</td>
                    <td className="py-1 text-right">{fmt2(row.actualFpts)}</td>
                    <td className="py-1 text-right">{fmtPct(row.actualOwnPct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
