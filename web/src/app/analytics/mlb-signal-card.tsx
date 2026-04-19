import { getCachedMlbPostmortemReport } from "@/db/analytics-cache";
import MlbSignalChart from "./mlb-signal-chart";

const fmtPct = (v: number | null | undefined) =>
  v == null ? "—" : `${v.toFixed(1)}%`;

export default async function MlbSignalCard() {
  let report;
  try {
    report = await getCachedMlbPostmortemReport();
  } catch {
    return null;
  }

  if (!report || report.signalFollowThrough.length === 0) return null;

  // Signals that have at least a lift value, sorted by 25+ lift descending.
  const signals = [...report.signalFollowThrough]
    .filter((s) => s.lift25Rate != null || s.lift20Rate != null || s.hit20Rate != null)
    .sort(
      (a, b) =>
        (b.lift25Rate ?? b.lift20Rate ?? -999) -
        (a.lift25Rate ?? a.lift20Rate ?? -999),
    );

  if (signals.length === 0) return null;

  const sampleSlates = report.sample.recentSlateCount + report.sample.priorSlateCount;

  return (
    <div className="mx-auto mt-8 max-w-5xl space-y-4 rounded-lg border bg-card p-4 text-slate-900">
      <div>
        <h2 className="text-sm font-semibold">Signal Follow-Through</h2>
        <p className="text-xs text-slate-600 mt-0.5">
          How much each DFS signal lifts a player&apos;s probability of scoring 20+ or 25+
          FPTS vs. the slate baseline. Sorted by 25+ lift. Positive = signal is
          predictive; negative or zero = no edge.
          Sample: {sampleSlates} slates (recent + prior windows).
        </p>
      </div>

      <MlbSignalChart signals={signals} />

      {/* Reference table */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Signal</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Slates</th>
              <th className="py-1 text-right">Avg Actual</th>
              <th className="py-1 text-right">Avg Beat</th>
              <th className="py-1 text-right">Hit 20+</th>
              <th className="py-1 text-right">Lift 20+</th>
              <th className="py-1 text-right">Hit 25+</th>
              <th className="py-1 text-right">Lift 25+</th>
            </tr>
          </thead>
          <tbody>
            {signals.map((row) => {
              const lift25Color =
                row.lift25Rate == null
                  ? ""
                  : row.lift25Rate > 5
                    ? "text-emerald-700 font-semibold"
                    : row.lift25Rate < -2
                      ? "text-rose-700"
                      : "";
              return (
                <tr key={row.signal} className="border-b border-slate-100">
                  <td className="py-1 font-medium text-slate-800">{row.signal}</td>
                  <td className="py-1 text-right text-slate-500">{row.rows}</td>
                  <td className="py-1 text-right text-slate-500">{row.slates}</td>
                  <td className="py-1 text-right">{row.avgActual?.toFixed(2) ?? "—"}</td>
                  <td className={`py-1 text-right ${(row.avgBeat ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700"}`}>
                    {row.avgBeat != null ? `${row.avgBeat >= 0 ? "+" : ""}${row.avgBeat.toFixed(2)}` : "—"}
                  </td>
                  <td className="py-1 text-right">{fmtPct(row.hit20Rate)}</td>
                  <td className={`py-1 text-right ${(row.lift20Rate ?? 0) > 0 ? "text-emerald-700" : "text-rose-600"}`}>
                    {row.lift20Rate != null ? `${row.lift20Rate >= 0 ? "+" : ""}${row.lift20Rate.toFixed(1)}%` : "—"}
                  </td>
                  <td className="py-1 text-right">{fmtPct(row.hit25Rate)}</td>
                  <td className={`py-1 text-right ${lift25Color}`}>
                    {row.lift25Rate != null ? `${row.lift25Rate >= 0 ? "+" : ""}${row.lift25Rate.toFixed(1)}%` : "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
