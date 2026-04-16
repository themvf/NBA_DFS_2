import {
  getCachedMlbPerfectLineupAnalytics,
  getCachedNbaPerfectLineupAnalytics,
} from "@/db/analytics-cache";
import type {
  MlbPerfectLineupAnalytics,
  NbaPerfectLineupAnalytics,
  Sport,
} from "@/db/queries";

const fmt1 = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(1));
const fmt2 = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(2));

function NbaPerfectLineupTables({ analytics }: { analytics: NbaPerfectLineupAnalytics }) {
  return (
    <>
      <div>
        <p className="text-xs text-gray-400 mb-2 font-medium">Stack Tendencies By Slate Size</p>
        <table className="w-full text-xs">
          <thead>
            <tr className="text-gray-400 border-b">
              <th className="py-1 text-left">Bucket</th>
              <th className="py-1 text-right">Slates</th>
              <th className="py-1 text-right">Avg Salary</th>
              <th className="py-1 text-right">Avg Points</th>
              <th className="py-1 text-right">2+ Stack</th>
              <th className="py-1 text-right">3+ Stack</th>
              <th className="py-1 text-right">4+ Stack</th>
              <th className="py-1 text-right">Multi-Team</th>
            </tr>
          </thead>
          <tbody>
            {analytics.summary.map((row) => (
              <tr key={row.slateSizeBucket} className="border-b border-gray-50">
                <td className="py-1 font-medium">{row.slateSizeBucket}</td>
                <td className="py-1 text-right">{row.slateCount}</td>
                <td className="py-1 text-right">${row.avgSalary.toLocaleString()}</td>
                <td className="py-1 text-right">{fmt2(row.avgPoints)}</td>
                <td className="py-1 text-right">{fmt1(row.anyTwoStackRate)}%</td>
                <td className="py-1 text-right">{fmt1(row.anyThreeStackRate)}%</td>
                <td className="py-1 text-right">{fmt1(row.anyFourStackRate)}%</td>
                <td className="py-1 text-right">{fmt1(row.multiTeamStackRate)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div>
        <p className="text-xs text-gray-400 mb-2 font-medium">Most Common Perfect Shapes</p>
        <table className="w-full text-xs">
          <thead>
            <tr className="text-gray-400 border-b">
              <th className="py-1 text-left">Bucket</th>
              <th className="py-1 text-left">Shape</th>
              <th className="py-1 text-right">Slates</th>
              <th className="py-1 text-right">Rate</th>
            </tr>
          </thead>
          <tbody>
            {analytics.shapes.slice(0, 12).map((row) => (
              <tr key={`${row.slateSizeBucket}-${row.shape}`} className="border-b border-gray-50">
                <td className="py-1">{row.slateSizeBucket}</td>
                <td className="py-1 font-medium">{row.shape}</td>
                <td className="py-1 text-right">{row.slateCount}</td>
                <td className="py-1 text-right">{fmt1(row.rate)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div>
          <p className="text-xs text-gray-400 mb-2 font-medium">Teams In Perfect Lineups</p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Team</th>
                <th className="py-1 text-right">Slate App</th>
                <th className="py-1 text-right">Perfect App</th>
                <th className="py-1 text-right">Avg / Slate</th>
                <th className="py-1 text-right">Shrunk</th>
              </tr>
            </thead>
            <tbody>
              {analytics.teamRates.slice(0, 15).map((row) => (
                <tr key={row.teamAbbrev} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.teamAbbrev}</td>
                  <td className="py-1 text-right">{row.slateAppearances}</td>
                  <td className="py-1 text-right">{row.perfectAppearances}</td>
                  <td className="py-1 text-right">{fmt2(row.avgPerfectPlayers)}</td>
                  <td className="py-1 text-right">{fmt2(row.shrunkAvgPerfectPlayers)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div>
          <p className="text-xs text-gray-400 mb-2 font-medium">Defenses Allowing Perfect Slots</p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Defense</th>
                <th className="py-1 text-left">Pos</th>
                <th className="py-1 text-right">Slate App</th>
                <th className="py-1 text-right">Perfect App</th>
                <th className="py-1 text-right">Avg Allowed</th>
                <th className="py-1 text-right">Shrunk</th>
              </tr>
            </thead>
            <tbody>
              {analytics.opponentAllow.slice(0, 20).map((row) => (
                <tr key={`${row.defenseAbbrev}-${row.position}`} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.defenseAbbrev}</td>
                  <td className="py-1">{row.position}</td>
                  <td className="py-1 text-right">{row.slateAppearances}</td>
                  <td className="py-1 text-right">{row.perfectAppearances}</td>
                  <td className="py-1 text-right">{fmt2(row.avgAllowed)}</td>
                  <td className="py-1 text-right">{fmt2(row.shrunkAvgAllowed)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function MlbPerfectLineupTables({ analytics }: { analytics: MlbPerfectLineupAnalytics }) {
  return (
    <>
      <div>
        <p className="text-xs text-gray-400 mb-2 font-medium">Hitter Stack Tendencies By Slate Size</p>
        <table className="w-full text-xs">
          <thead>
            <tr className="text-gray-400 border-b">
              <th className="py-1 text-left">Bucket</th>
              <th className="py-1 text-right">Slates</th>
              <th className="py-1 text-right">Avg Salary</th>
              <th className="py-1 text-right">Avg Points</th>
              <th className="py-1 text-right">2+ Stack</th>
              <th className="py-1 text-right">3+ Stack</th>
              <th className="py-1 text-right">4+ Stack</th>
              <th className="py-1 text-right">5+ Stack</th>
              <th className="py-1 text-right">Multi-Team</th>
            </tr>
          </thead>
          <tbody>
            {analytics.summary.map((row) => (
              <tr key={row.slateSizeBucket} className="border-b border-gray-50">
                <td className="py-1 font-medium">{row.slateSizeBucket}</td>
                <td className="py-1 text-right">{row.slateCount}</td>
                <td className="py-1 text-right">${row.avgSalary.toLocaleString()}</td>
                <td className="py-1 text-right">{fmt2(row.avgPoints)}</td>
                <td className="py-1 text-right">{fmt1(row.anyTwoStackRate)}%</td>
                <td className="py-1 text-right">{fmt1(row.anyThreeStackRate)}%</td>
                <td className="py-1 text-right">{fmt1(row.anyFourStackRate)}%</td>
                <td className="py-1 text-right">{fmt1(row.anyFiveStackRate)}%</td>
                <td className="py-1 text-right">{fmt1(row.multiTeamStackRate)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div>
        <p className="text-xs text-gray-400 mb-2 font-medium">Most Common Perfect Hitter Shapes</p>
        <table className="w-full text-xs">
          <thead>
            <tr className="text-gray-400 border-b">
              <th className="py-1 text-left">Bucket</th>
              <th className="py-1 text-left">Shape</th>
              <th className="py-1 text-right">Slates</th>
              <th className="py-1 text-right">Rate</th>
            </tr>
          </thead>
          <tbody>
            {analytics.shapes.slice(0, 12).map((row) => (
              <tr key={`${row.slateSizeBucket}-${row.shape}`} className="border-b border-gray-50">
                <td className="py-1">{row.slateSizeBucket}</td>
                <td className="py-1 font-medium">{row.shape}</td>
                <td className="py-1 text-right">{row.slateCount}</td>
                <td className="py-1 text-right">{fmt1(row.rate)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div>
          <p className="text-xs text-gray-400 mb-2 font-medium">Offenses In Perfect Lineups</p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Team</th>
                <th className="py-1 text-right">Slate App</th>
                <th className="py-1 text-right">Perfect App</th>
                <th className="py-1 text-right">Avg Hitters</th>
                <th className="py-1 text-right">Shrunk</th>
              </tr>
            </thead>
            <tbody>
              {analytics.teamRates.slice(0, 15).map((row) => (
                <tr key={row.teamAbbrev} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.teamAbbrev}</td>
                  <td className="py-1 text-right">{row.slateAppearances}</td>
                  <td className="py-1 text-right">{row.perfectAppearances}</td>
                  <td className="py-1 text-right">{fmt2(row.avgPerfectHitters)}</td>
                  <td className="py-1 text-right">{fmt2(row.shrunkAvgPerfectHitters)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div>
          <p className="text-xs text-gray-400 mb-2 font-medium">Defenses Allowing Perfect Hitter Slots</p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Defense</th>
                <th className="py-1 text-left">Pos</th>
                <th className="py-1 text-right">Slate App</th>
                <th className="py-1 text-right">Perfect App</th>
                <th className="py-1 text-right">Avg Allowed</th>
                <th className="py-1 text-right">Shrunk</th>
              </tr>
            </thead>
            <tbody>
              {analytics.opponentAllow.slice(0, 20).map((row) => (
                <tr key={`${row.defenseAbbrev}-${row.position}`} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.defenseAbbrev}</td>
                  <td className="py-1">{row.position}</td>
                  <td className="py-1 text-right">{row.slateAppearances}</td>
                  <td className="py-1 text-right">{row.perfectAppearances}</td>
                  <td className="py-1 text-right">{fmt2(row.avgAllowed)}</td>
                  <td className="py-1 text-right">{fmt2(row.shrunkAvgAllowed)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

export default async function PerfectLineupPanel({ sport }: { sport: Sport }) {
  const analytics = sport === "mlb"
    ? await getCachedMlbPerfectLineupAnalytics()
    : await getCachedNbaPerfectLineupAnalytics();

  if (!analytics) return null;

  return (
    <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 space-y-6 text-slate-900 [&_.text-gray-100]:text-slate-900 [&_.text-gray-300]:text-slate-700 [&_.text-gray-400]:text-slate-600 [&_.text-gray-500]:text-slate-700">
      <div>
        <h2 className="text-sm font-semibold mb-1">Perfect Lineup Structure</h2>
        <p className="text-xs text-gray-500">
          {sport === "mlb"
            ? "Exact optimal MLB lineups are recomputed from saved historical slates using actual fantasy points, salary, and DK roster rules. Stack metrics count hitters only."
            : "Exact optimal NBA lineups are recomputed from saved historical slates using actual fantasy points, salary, and DK roster rules. Team and defense tables are normalized per slate appearance."}
        </p>
        <p className="mt-2 text-xs text-gray-400">
          Slates analyzed: {analytics.slateCount}
          {" · "}
          Opponent-context slates: {analytics.opponentContextSlateCount}
        </p>
      </div>

      {sport === "mlb"
        ? <MlbPerfectLineupTables analytics={analytics as MlbPerfectLineupAnalytics} />
        : <NbaPerfectLineupTables analytics={analytics as NbaPerfectLineupAnalytics} />}
    </div>
  );
}
