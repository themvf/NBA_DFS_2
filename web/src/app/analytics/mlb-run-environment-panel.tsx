import { getCachedMlbRunEnvironmentReport } from "@/db/analytics-cache";

const fmt2 = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(2));

export default async function MlbRunEnvironmentPanel() {
  const report = await getCachedMlbRunEnvironmentReport();

  if (!report) return null;

  return (
    <div className="mx-auto mt-8 max-w-5xl space-y-6 rounded-lg border bg-card p-4 text-slate-900">
      <div>
        <h2 className="mb-1 text-sm font-semibold">MLB Pitcher And Park Environment</h2>
        <p className="text-xs text-slate-700">
          Actual scoring environment from historical main-slate MLB results. Pitcher rows are starting-pitcher context on opposing hitter outcomes, not pitch-by-pitch attribution.
        </p>
        <p className="mt-2 text-xs text-slate-600">
          Pitcher sample: {report.sample.pitcherStarts} tracked starts across {report.sample.pitcherHitterRows} hitter rows
          {" · "}
          Park sample: {report.sample.parkGames} park games across {report.sample.parkTeamGames} offense samples
        </p>
      </div>

      {report.findings.length > 0 ? (
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">What The Environment Is Saying</p>
          <div className="grid gap-2 md:grid-cols-3">
            {report.findings.map((finding) => (
              <div key={finding} className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
                {finding}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-2">
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">Pitchers Allowing The Most Hitter Points</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Pitcher</th>
                  <th className="py-1 text-right">Starts</th>
                  <th className="py-1 text-right">Team FPTS</th>
                  <th className="py-1 text-right">Shrunk</th>
                  <th className="py-1 text-right">Top Bat</th>
                  <th className="py-1 text-right">15+</th>
                  <th className="py-1 text-right">K/9</th>
                  <th className="py-1 text-right">xFIP</th>
                </tr>
              </thead>
              <tbody>
                {report.pitcherAllow.map((row) => (
                  <tr key={row.pitcherId} className="border-b border-slate-100">
                    <td className="py-1">
                      <div className="font-medium">{row.pitcherName}</div>
                      <div className="text-[11px] text-slate-500">{row.hand ?? "—"} hand · {row.hitterRows} hitter rows</div>
                    </td>
                    <td className="py-1 text-right">{row.starts}</td>
                    <td className="py-1 text-right">{fmt2(row.avgTeamFptsAllowed)}</td>
                    <td className="py-1 text-right">{fmt2(row.shrunkAvgTeamFptsAllowed)}</td>
                    <td className="py-1 text-right">{fmt2(row.avgTopHitterFptsAllowed)}</td>
                    <td className="py-1 text-right">{fmt2(row.avg15PlusHitters)}</td>
                    <td className="py-1 text-right">{fmt2(row.kPer9)}</td>
                    <td className="py-1 text-right">{fmt2(row.xfip)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">Parks Producing The Most Hitter Points</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Park</th>
                  <th className="py-1 text-right">Games</th>
                  <th className="py-1 text-right">Team FPTS</th>
                  <th className="py-1 text-right">Shrunk</th>
                  <th className="py-1 text-right">Runs</th>
                  <th className="py-1 text-right">SP FPTS</th>
                  <th className="py-1 text-right">Runs PF</th>
                  <th className="py-1 text-right">HR PF</th>
                </tr>
              </thead>
              <tbody>
                {report.parkEnvironment.map((row) => (
                  <tr key={`${row.parkTeamId}-${row.parkName}`} className="border-b border-slate-100">
                    <td className="py-1">
                      <div className="font-medium">{row.parkName}</div>
                      <div className="text-[11px] text-slate-500">{row.homeTeamAbbrev} · {row.teamGames} offense samples</div>
                    </td>
                    <td className="py-1 text-right">{row.games}</td>
                    <td className="py-1 text-right">{fmt2(row.avgTeamHitterFpts)}</td>
                    <td className="py-1 text-right">{fmt2(row.shrunkAvgTeamHitterFpts)}</td>
                    <td className="py-1 text-right">{fmt2(row.avgCombinedRuns)}</td>
                    <td className="py-1 text-right">{fmt2(row.avgSpFpts)}</td>
                    <td className="py-1 text-right">{fmt2(row.runsFactor)}</td>
                    <td className="py-1 text-right">{fmt2(row.hrFactor)}</td>
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
