import { getDfsAccuracy, getDkLineupComparison, getDkStrategySummary, type Sport } from "@/db/queries";
import { getMlbOptimizerFeatureImpactSummary } from "./optimizer-jobs";

function fmt1(v: number | null | undefined): string {
  return v != null ? v.toFixed(1) : "-";
}

function fmtPct(v: number | null | undefined): string {
  return v != null ? `${v.toFixed(1)}%` : "-";
}

function fmtSigned(v: number | null | undefined): string {
  return v != null ? `${v >= 0 ? "+" : ""}${v.toFixed(1)}` : "-";
}

export default async function DfsSecondaryPanels({ sport }: { sport: Sport }) {
  const [accuracy, comparison, strategySummary, optimizerFeatureImpact] = await Promise.all([
    getDfsAccuracy(sport),
    getDkLineupComparison(sport),
    getDkStrategySummary(sport),
    sport === "mlb" ? getMlbOptimizerFeatureImpactSummary() : Promise.resolve(null),
  ]);

  const hasOptimizerFeatureImpact = Boolean(
    optimizerFeatureImpact &&
    (optimizerFeatureImpact.hrCorrelation.length > 0 ||
      optimizerFeatureImpact.pitcherCeiling.length > 0 ||
      optimizerFeatureImpact.antiCorrelation.length > 0),
  );

  if (!accuracy && comparison.length === 0 && strategySummary.length === 0 && !hasOptimizerFeatureImpact) {
    return null;
  }

  return (
    <div className="space-y-6">
      {accuracy && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="mb-3 text-sm font-semibold">Projection Accuracy - {accuracy.metrics.slateDate}</h2>
          {accuracy.metrics.nOutProjected > 0 && (
            <p className="mb-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
              {accuracy.metrics.nOutProjected} projected players were marked OUT on this slate. They are still included
              below, but the active-only metrics separate them from real on-court misses.
            </p>
          )}
          <div className="mb-4 grid grid-cols-2 gap-4 lg:grid-cols-4">
            <div className="rounded border p-3">
              <p className="mb-1 text-xs text-gray-500">Our Model (n={accuracy.metrics.nOur})</p>
              <p className="text-lg font-bold">{fmt1(accuracy.metrics.ourMAE)} MAE</p>
              <p className="text-xs text-gray-500">
                Bias: {accuracy.metrics.ourBias != null ? (accuracy.metrics.ourBias >= 0 ? "+" : "") + accuracy.metrics.ourBias.toFixed(2) : "-"}
              </p>
            </div>
            <div className="rounded border p-3">
              <p className="mb-1 text-xs text-gray-500">Our Active Only (n={accuracy.metrics.nOurActive})</p>
              <p className="text-lg font-bold">{fmt1(accuracy.metrics.ourActiveMAE)} MAE</p>
              <p className="text-xs text-gray-500">
                Bias: {accuracy.metrics.ourActiveBias != null ? (accuracy.metrics.ourActiveBias >= 0 ? "+" : "") + accuracy.metrics.ourActiveBias.toFixed(2) : "-"}
              </p>
            </div>
            {accuracy.metrics.nLinestar > 0 && (
              <div className="rounded border p-3">
                <p className="mb-1 text-xs text-gray-500">LineStar (n={accuracy.metrics.nLinestar})</p>
                <p className="text-lg font-bold">{fmt1(accuracy.metrics.linestarMAE)} MAE</p>
                <p className="text-xs text-gray-500">
                  Bias: {accuracy.metrics.linestarBias != null ? (accuracy.metrics.linestarBias >= 0 ? "+" : "") + accuracy.metrics.linestarBias.toFixed(2) : "-"}
                </p>
              </div>
            )}
            {accuracy.metrics.nLinestarActive > 0 && (
              <div className="rounded border p-3">
                <p className="mb-1 text-xs text-gray-500">LS Active Only (n={accuracy.metrics.nLinestarActive})</p>
                <p className="text-lg font-bold">{fmt1(accuracy.metrics.linestarActiveMAE)} MAE</p>
                <p className="text-xs text-gray-500">
                  Bias: {accuracy.metrics.linestarActiveBias != null ? (accuracy.metrics.linestarActiveBias >= 0 ? "+" : "") + accuracy.metrics.linestarActiveBias.toFixed(2) : "-"}
                </p>
              </div>
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-gray-400">
                  <th className="py-1 text-left">Player</th>
                  <th className="py-1 text-right">Our</th>
                  <th className="py-1 text-right">LS</th>
                  <th className="py-1 text-right">Actual</th>
                  <th className="py-1 text-right">Err</th>
                </tr>
              </thead>
              <tbody>
                {accuracy.players.slice(0, 15).map((p) => {
                  const err = p.ourProj != null && p.actualFpts != null ? p.ourProj - p.actualFpts : null;
                  return (
                    <tr key={p.id} className="border-b border-gray-50">
                      <td className="py-1 font-medium">
                        {p.name} <span className="text-gray-400">{p.teamAbbrev}</span>
                        {p.isOut && (
                          <span className="ml-2 rounded bg-amber-100 px-1.5 py-0.5 text-[10px] font-semibold text-amber-800">
                            OUT
                          </span>
                        )}
                      </td>
                      <td className="py-1 text-right">{fmt1(p.ourProj)}</td>
                      <td className="py-1 text-right text-gray-400">{fmt1(p.linestarProj)}</td>
                      <td className="py-1 text-right font-medium">{fmt1(p.actualFpts)}</td>
                      <td className={`py-1 text-right font-medium ${err == null ? "" : err > 0 ? "text-red-500" : "text-green-600"}`}>
                        {err != null ? (err >= 0 ? "+" : "") + err.toFixed(1) : "-"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {comparison.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="mb-3 text-sm font-semibold">Strategy Comparison - Latest Slate</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-gray-400">
                <th className="py-1 text-left">Strategy</th>
                <th className="py-1 text-right">N</th>
                <th className="py-1 text-right">Avg Proj</th>
                <th className="py-1 text-right">Avg Actual</th>
                <th className="py-1 text-right">Top Stack</th>
              </tr>
            </thead>
            <tbody>
              {comparison.map((row) => (
                <tr key={row.strategy} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.strategy}</td>
                  <td className="py-1 text-right">{row.nLineups}</td>
                  <td className="py-1 text-right">{fmt1(row.avgProjFpts)}</td>
                  <td className="py-1 text-right font-medium">{row.avgActualFpts != null ? fmt1(row.avgActualFpts) : "-"}</td>
                  <td className="py-1 text-right text-gray-400">{row.topStack ?? "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {strategySummary.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="mb-3 text-sm font-semibold">Strategy Leaderboard - All Slates</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-gray-400">
                <th className="py-1 text-left">Strategy</th>
                <th className="py-1 text-right">Slates</th>
                <th className="py-1 text-right">Lineups</th>
                <th className="py-1 text-right">Avg FPTS</th>
                <th className="py-1 text-right">Cash%</th>
                <th className="py-1 text-right">Best</th>
              </tr>
            </thead>
            <tbody>
              {strategySummary.map((row) => (
                <tr key={row.strategy} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.strategy}</td>
                  <td className="py-1 text-right">{row.nSlates}</td>
                  <td className="py-1 text-right">{row.totalLineups}</td>
                  <td className="py-1 text-right font-medium">{fmt1(row.avgActualFpts)}</td>
                  <td className="py-1 text-right">{row.cashRate != null ? `${Number(row.cashRate).toFixed(1)}%` : "-"}</td>
                  <td className="py-1 text-right font-medium text-green-600">{fmt1(row.bestSingleLineup)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {sport === "mlb" && optimizerFeatureImpact && hasOptimizerFeatureImpact && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="mb-2 text-sm font-semibold">Optimizer Feature Impact - Completed MLB Job Lineups</h2>
          <p className="mb-4 text-xs text-gray-500">
            Only durable optimizer jobs with full lineup actuals loaded are included. Beat = actual minus projected lineup score.
          </p>

          <div className="grid gap-4 xl:grid-cols-3">
            <div className="overflow-x-auto">
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">HR Correlation</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b text-gray-400">
                    <th className="py-1 text-left">Setting</th>
                    <th className="py-1 text-right">Slates</th>
                    <th className="py-1 text-right">Lineups</th>
                    <th className="py-1 text-right">Avg Act</th>
                    <th className="py-1 text-right">Beat</th>
                    <th className="py-1 text-right">Cash%</th>
                  </tr>
                </thead>
                <tbody>
                  {optimizerFeatureImpact.hrCorrelation.map((row) => (
                    <tr key={`${row.hrCorrelation}-${row.hrCorrelationThreshold ?? "off"}`} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.hrCorrelation ? `On @ ${fmt1(row.hrCorrelationThreshold)}` : "Off"}</td>
                      <td className="py-1 text-right">{row.nSlates}</td>
                      <td className="py-1 text-right">{row.totalLineups}</td>
                      <td className="py-1 text-right">{fmt1(row.avgActualFpts)}</td>
                      <td className={`py-1 text-right ${row.avgBeat != null && row.avgBeat >= 0 ? "text-green-600" : "text-red-500"}`}>
                        {fmtSigned(row.avgBeat)}
                      </td>
                      <td className="py-1 text-right">{fmtPct(row.cashRate)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="overflow-x-auto">
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">Pitcher Ceiling</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b text-gray-400">
                    <th className="py-1 text-left">Setting</th>
                    <th className="py-1 text-right">Slates</th>
                    <th className="py-1 text-right">Lineups</th>
                    <th className="py-1 text-right">Avg Act</th>
                    <th className="py-1 text-right">Beat</th>
                    <th className="py-1 text-right">Cash%</th>
                  </tr>
                </thead>
                <tbody>
                  {optimizerFeatureImpact.pitcherCeiling.map((row) => (
                    <tr key={`${row.pitcherCeilingBoost}-${row.pitcherCeilingCount ?? "off"}`} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.pitcherCeilingBoost ? `On top ${row.pitcherCeilingCount ?? "-"}` : "Off"}</td>
                      <td className="py-1 text-right">{row.nSlates}</td>
                      <td className="py-1 text-right">{row.totalLineups}</td>
                      <td className="py-1 text-right">{fmt1(row.avgActualFpts)}</td>
                      <td className={`py-1 text-right ${row.avgBeat != null && row.avgBeat >= 0 ? "text-green-600" : "text-red-500"}`}>
                        {fmtSigned(row.avgBeat)}
                      </td>
                      <td className="py-1 text-right">{fmtPct(row.cashRate)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="overflow-x-auto">
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">Anti-Correlation Limit</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b text-gray-400">
                    <th className="py-1 text-left">Max Opp Bats</th>
                    <th className="py-1 text-right">Slates</th>
                    <th className="py-1 text-right">Lineups</th>
                    <th className="py-1 text-right">Avg Act</th>
                    <th className="py-1 text-right">Beat</th>
                    <th className="py-1 text-right">Cash%</th>
                  </tr>
                </thead>
                <tbody>
                  {optimizerFeatureImpact.antiCorrelation.map((row) => (
                    <tr key={row.effectiveAntiCorrMax} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.effectiveAntiCorrMax}</td>
                      <td className="py-1 text-right">{row.nSlates}</td>
                      <td className="py-1 text-right">{row.totalLineups}</td>
                      <td className="py-1 text-right">{fmt1(row.avgActualFpts)}</td>
                      <td className={`py-1 text-right ${row.avgBeat != null && row.avgBeat >= 0 ? "text-green-600" : "text-red-500"}`}>
                        {fmtSigned(row.avgBeat)}
                      </td>
                      <td className="py-1 text-right">{fmtPct(row.cashRate)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {optimizerFeatureImpact.combinations.length > 0 && (
            <div className="mt-4 overflow-x-auto">
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">Most Used Setting Combos</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b text-gray-400">
                    <th className="py-1 text-left">HR Corr</th>
                    <th className="py-1 text-left">Pitch Ceiling</th>
                    <th className="py-1 text-right">Anti</th>
                    <th className="py-1 text-right">Slates</th>
                    <th className="py-1 text-right">Lineups</th>
                    <th className="py-1 text-right">Avg Act</th>
                    <th className="py-1 text-right">Beat</th>
                    <th className="py-1 text-right">Cash%</th>
                    <th className="py-1 text-right">Best</th>
                  </tr>
                </thead>
                <tbody>
                  {[...optimizerFeatureImpact.combinations]
                    .sort((a, b) => (
                      b.nSlates - a.nSlates ||
                      b.totalLineups - a.totalLineups ||
                      (b.avgActualFpts ?? Number.NEGATIVE_INFINITY) - (a.avgActualFpts ?? Number.NEGATIVE_INFINITY)
                    ))
                    .slice(0, 8)
                    .map((row) => (
                      <tr
                        key={[
                          row.hrCorrelation,
                          row.hrCorrelationThreshold ?? "off",
                          row.pitcherCeilingBoost,
                          row.pitcherCeilingCount ?? "off",
                          row.effectiveAntiCorrMax,
                        ].join(":")}
                        className="border-b border-gray-50"
                      >
                        <td className="py-1 font-medium">{row.hrCorrelation ? `On @ ${fmt1(row.hrCorrelationThreshold)}` : "Off"}</td>
                        <td className="py-1 font-medium">{row.pitcherCeilingBoost ? `On top ${row.pitcherCeilingCount ?? "-"}` : "Off"}</td>
                        <td className="py-1 text-right">{row.effectiveAntiCorrMax}</td>
                        <td className="py-1 text-right">{row.nSlates}</td>
                        <td className="py-1 text-right">{row.totalLineups}</td>
                        <td className="py-1 text-right">{fmt1(row.avgActualFpts)}</td>
                        <td className={`py-1 text-right ${row.avgBeat != null && row.avgBeat >= 0 ? "text-green-600" : "text-red-500"}`}>
                          {fmtSigned(row.avgBeat)}
                        </td>
                        <td className="py-1 text-right">{fmtPct(row.cashRate)}</td>
                        <td className="py-1 text-right text-green-600">{fmt1(row.bestSingleLineup)}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
