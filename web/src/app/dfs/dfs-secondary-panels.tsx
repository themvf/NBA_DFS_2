import { getDfsAccuracy, getDkLineupComparison, getDkStrategySummary, type Sport } from "@/db/queries";

function fmt1(v: number | null | undefined): string {
  return v != null ? v.toFixed(1) : "—";
}

export default async function DfsSecondaryPanels({ sport }: { sport: Sport }) {
  const [accuracy, comparison, strategySummary] = await Promise.all([
    getDfsAccuracy(sport),
    getDkLineupComparison(sport),
    getDkStrategySummary(sport),
  ]);

  if (!accuracy && comparison.length === 0 && strategySummary.length === 0) {
    return null;
  }

  return (
    <div className="space-y-6">
      {accuracy && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3">Projection Accuracy — {accuracy.metrics.slateDate}</h2>
          {accuracy.metrics.nOutProjected > 0 && (
            <p className="mb-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
              {accuracy.metrics.nOutProjected} projected players were marked OUT on this slate. They are still included below, but the active-only metrics separate them from real on-court misses.
            </p>
          )}
          <div className="grid grid-cols-2 gap-4 mb-4 lg:grid-cols-4">
            <div className="rounded border p-3">
              <p className="text-xs text-gray-500 mb-1">Our Model (n={accuracy.metrics.nOur})</p>
              <p className="text-lg font-bold">{fmt1(accuracy.metrics.ourMAE)} MAE</p>
              <p className="text-xs text-gray-500">Bias: {accuracy.metrics.ourBias != null ? (accuracy.metrics.ourBias >= 0 ? "+" : "") + accuracy.metrics.ourBias.toFixed(2) : "—"}</p>
            </div>
            <div className="rounded border p-3">
              <p className="text-xs text-gray-500 mb-1">Our Active Only (n={accuracy.metrics.nOurActive})</p>
              <p className="text-lg font-bold">{fmt1(accuracy.metrics.ourActiveMAE)} MAE</p>
              <p className="text-xs text-gray-500">Bias: {accuracy.metrics.ourActiveBias != null ? (accuracy.metrics.ourActiveBias >= 0 ? "+" : "") + accuracy.metrics.ourActiveBias.toFixed(2) : "—"}</p>
            </div>
            {accuracy.metrics.nLinestar > 0 && (
              <div className="rounded border p-3">
                <p className="text-xs text-gray-500 mb-1">LineStar (n={accuracy.metrics.nLinestar})</p>
                <p className="text-lg font-bold">{fmt1(accuracy.metrics.linestarMAE)} MAE</p>
                <p className="text-xs text-gray-500">Bias: {accuracy.metrics.linestarBias != null ? (accuracy.metrics.linestarBias >= 0 ? "+" : "") + accuracy.metrics.linestarBias.toFixed(2) : "—"}</p>
              </div>
            )}
            {accuracy.metrics.nLinestarActive > 0 && (
              <div className="rounded border p-3">
                <p className="text-xs text-gray-500 mb-1">LS Active Only (n={accuracy.metrics.nLinestarActive})</p>
                <p className="text-lg font-bold">{fmt1(accuracy.metrics.linestarActiveMAE)} MAE</p>
                <p className="text-xs text-gray-500">Bias: {accuracy.metrics.linestarActiveBias != null ? (accuracy.metrics.linestarActiveBias >= 0 ? "+" : "") + accuracy.metrics.linestarActiveBias.toFixed(2) : "—"}</p>
              </div>
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400 border-b">
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
                        {err != null ? (err >= 0 ? "+" : "") + err.toFixed(1) : "—"}
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
          <h2 className="text-sm font-semibold mb-3">Strategy Comparison — Latest Slate</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
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
                  <td className="py-1 text-right font-medium">{row.avgActualFpts != null ? fmt1(row.avgActualFpts) : "—"}</td>
                  <td className="py-1 text-right text-gray-400">{row.topStack ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {strategySummary.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3">Strategy Leaderboard — All Slates</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
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
                  <td className="py-1 text-right">{row.cashRate != null ? Number(row.cashRate).toFixed(1) + "%" : "—"}</td>
                  <td className="py-1 text-right text-green-600 font-medium">{fmt1(row.bestSingleLineup)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
