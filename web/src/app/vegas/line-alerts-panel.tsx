"use client";

import type { LineAlertRow, LineAlertBacktestRow } from "@/db/queries";

// Sharp line-movement alerts feed + its running audit (shared across the
// MLB / soccer / tennis vegas views). Every alert is an immutable ledger row
// graded on CLV (did the market close toward the flagged side?) and outcome
// (did the side win?) — the backtest header is what tells you whether the
// alert type has earned trust or its thresholds are noise.
export default function LineAlertsPanel({
  alerts,
  backtest,
}: {
  alerts: LineAlertRow[];
  backtest: LineAlertBacktestRow[];
}) {
  const pp = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}pp`;
  const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
  const typeLabel = (t: string) =>
    t === "pinnacle_divergence" ? "Pin divergence" : t === "steam" ? "Steam" : t;

  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
        Sharp Line Alerts
      </h3>
      <p className="text-xs text-gray-500 mb-2">
        <span className="font-medium">Pin divergence</span> = Pinnacle prices a side ≥2pp above retail
        consensus (the sharp side, at a stale retail price). <span className="font-medium">Steam</span> =
        ≥3 books moved a side ≥1.5pp together between captures. Alerts freeze at trigger and are
        audited on CLV + outcomes below — an alert type with no positive CLV is noise, and this
        table will say so.
      </p>

      {backtest.length > 0 && (
        <table className="w-full text-xs border-collapse mb-3">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">Audit</th>
              <th className="py-1 text-right">n</th>
              <th className="py-1 text-right">Avg CLV</th>
              <th className="py-1 text-right">Beat close</th>
              <th className="py-1 text-right">Outcomes</th>
              <th className="py-1 text-right">Win rate</th>
              <th className="py-1 text-right">Implied</th>
            </tr>
          </thead>
          <tbody>
            {backtest.map((b) => (
              <tr key={b.alertType} className="border-b border-gray-50">
                <td className="py-1 font-medium">{typeLabel(b.alertType)}</td>
                <td className="py-1 text-right text-gray-500">{b.n}</td>
                <td className={`py-1 text-right tabular-nums ${
                  (b.avgClvPp ?? 0) > 0 ? "text-emerald-600" : (b.avgClvPp ?? 0) < 0 ? "text-red-500" : "text-gray-400"
                }`}>
                  {b.nClv > 0 && b.avgClvPp != null ? pp(b.avgClvPp) : "accruing"}
                </td>
                <td className="py-1 text-right tabular-nums text-gray-500">{pct(b.beatClose)}</td>
                <td className="py-1 text-right text-gray-500">{b.nOutcomes}</td>
                <td className="py-1 text-right tabular-nums">{pct(b.winRate)}</td>
                <td className="py-1 text-right tabular-nums text-gray-400">{pct(b.impliedRate)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {alerts.length === 0 ? (
        <div className="rounded bg-gray-50 border px-3 py-2 text-xs text-gray-500">
          No alerts yet — the scanner runs after every odds capture.
        </div>
      ) : (
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">When (UTC)</th>
              <th className="py-1 text-left">Type</th>
              <th className="py-1 text-left">Game</th>
              <th className="py-1 text-left">Side</th>
              <th className="py-1 text-right">Retail</th>
              <th className="py-1 text-right">Sharp</th>
              <th className="py-1 text-right">CLV</th>
              <th className="py-1 text-right">Result</th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((a, i) => (
              <tr key={i} className={`border-b border-gray-50 ${a.outcome == null && a.clvPp == null ? "bg-amber-50/40" : ""}`}>
                <td className="py-1 text-gray-500 whitespace-nowrap">{a.createdAt.slice(5, 16)}</td>
                <td className="py-1">{typeLabel(a.alertType)}</td>
                <td className="py-1">{a.matchup}</td>
                <td className="py-1 font-medium">{a.side}</td>
                <td className="py-1 text-right tabular-nums">{pct(a.alertProb)}</td>
                <td className="py-1 text-right tabular-nums text-indigo-600">{pct(a.sharpProb)}</td>
                <td className={`py-1 text-right tabular-nums ${
                  a.clvPp == null ? "text-gray-400" : a.clvPp > 0 ? "text-emerald-600" : "text-red-500"
                }`}>
                  {a.clvPp != null ? pp(a.clvPp) : "open"}
                </td>
                <td className={`py-1 text-right ${
                  a.outcome === "won" ? "text-emerald-600" : a.outcome === "lost" ? "text-red-500" : "text-gray-400"
                }`}>
                  {a.outcome ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
