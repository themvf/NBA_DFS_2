"use client";

import type { LineAlertRow, LineAlertBacktestRow } from "@/db/queries";

// Sharp line-movement alerts feed + its running audit (shared across the
// MLB / soccer / tennis vegas views). Every alert is an immutable ledger row
// graded on CLV (did the market close toward the flagged side?) and outcome
// (did the side win?). Sides are translated to actual team names and each
// alert carries an Action chip that says, in plain language, what a user can
// do with it — honestly graded: Pin divergence is a usable PRICE rule today;
// steam stays a WATCH signal until the audit shows it carries CLV.

const sideName = (a: LineAlertRow): string => {
  const [away, home] = a.matchup.split(" @ ");
  if (a.side === "home") return home ?? "home";
  if (a.side === "away") return away ?? "away";
  return "Draw";
};

function ActionChip({ a }: { a: LineAlertRow }) {
  const name = sideName(a);
  if (a.alertType === "pinnacle_divergence") {
    const gap = a.sharpProb != null && a.alertProb != null
      ? ((a.sharpProb - a.alertProb) * 100).toFixed(1) : "?";
    return (
      <span
        className="cursor-help rounded-full bg-indigo-100 px-2 py-0.5 text-[10px] font-semibold text-indigo-700 whitespace-nowrap"
        title={`Pinnacle prices ${name} ${gap}pp above retail consensus — retail books are offering ${name} at better than sharp fair value. ` +
               `Usable now as a price rule: if you bet this game, only take ${name}, at the best retail price you can find. ` +
               `Never take the opposite side at a retail price the sharp book says is too short.`}
      >
        Prefer {name} @ retail
      </span>
    );
  }
  return (
    <span
      className="cursor-help rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-700 whitespace-nowrap"
      title={`Multiple books moved toward ${name} at the same time — the signature of informed money. ` +
             `WATCH signal: don't bet it on faith. The audit above is measuring whether steam alerts keep beating the close; ` +
             `if the Avg CLV turns and stays positive, this chip graduates to actionable.`}
    >
      Watch {name} (steam)
    </span>
  );
}

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
        Alerts freeze the market state at trigger time and are audited below: <span className="font-medium">CLV</span> =
        did the market keep moving toward the flagged team after the alert (positive = the alert was early enough to act
        on), <span className="font-medium">win rate vs implied</span> = did flagged teams win more often than their price
        said. An alert type with no positive CLV is noise — this table will say so. Hover any Action chip for exactly
        what to do with it.
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
              <th className="py-1 text-right">Verdict</th>
            </tr>
          </thead>
          <tbody>
            {backtest.map((b) => {
              const verdict =
                b.nClv < 10
                  ? { label: "accruing", cls: "bg-gray-100 text-gray-500",
                      tip: `Needs ~10+ graded alerts before the CLV average means anything (${b.nClv} so far).` }
                  : (b.avgClvPp ?? 0) > 0.5
                    ? { label: "has signal", cls: "bg-emerald-100 text-emerald-700",
                        tip: "Alerts have been early — the market kept moving toward the flagged side after we fired. Worth acting on." }
                    : { label: "noise so far", cls: "bg-red-100 text-red-600",
                        tip: "The move was already absorbed by the time we detected it. Thresholds or capture cadence need work before betting this." };
              return (
                <tr key={b.alertType} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{typeLabel(b.alertType)}</td>
                  <td className="py-1 text-right text-gray-500">{b.n}</td>
                  <td className={`py-1 text-right tabular-nums ${
                    (b.avgClvPp ?? 0) > 0 ? "text-emerald-600" : (b.avgClvPp ?? 0) < 0 ? "text-red-500" : "text-gray-400"
                  }`}>
                    {b.nClv > 0 && b.avgClvPp != null ? pp(b.avgClvPp) : "—"}
                  </td>
                  <td className="py-1 text-right tabular-nums text-gray-500">{pct(b.beatClose)}</td>
                  <td className="py-1 text-right text-gray-500">{b.nOutcomes}</td>
                  <td className="py-1 text-right tabular-nums">{pct(b.winRate)}</td>
                  <td className="py-1 text-right tabular-nums text-gray-400">{pct(b.impliedRate)}</td>
                  <td className="py-1 text-right">
                    <span title={verdict.tip}
                          className={`cursor-help rounded-full px-2 py-0.5 text-[10px] font-semibold ${verdict.cls}`}>
                      {verdict.label}
                    </span>
                  </td>
                </tr>
              );
            })}
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
              <th className="py-1 text-left">Game</th>
              <th className="py-1 text-left">Action</th>
              <th className="py-1 text-right">Retail</th>
              <th className="py-1 text-right">Sharp</th>
              <th className="py-1 text-right">CLV since alert</th>
              <th className="py-1 text-right">Result</th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((a, i) => (
              <tr key={i} className={`border-b border-gray-50 ${a.outcome == null && a.clvPp == null ? "bg-amber-50/40" : ""}`}>
                <td className="py-1 text-gray-500 whitespace-nowrap">{a.createdAt.slice(5, 16)}</td>
                <td className="py-1">{a.matchup}</td>
                <td className="py-1"><ActionChip a={a} /></td>
                <td className="py-1 text-right tabular-nums"
                    title={`Retail consensus P(${sideName(a)}) when the alert fired`}>{pct(a.alertProb)}</td>
                <td className="py-1 text-right tabular-nums text-indigo-600"
                    title={`Pinnacle's vig-free P(${sideName(a)}) when the alert fired`}>{pct(a.sharpProb)}</td>
                <td className={`py-1 text-right tabular-nums ${
                  a.clvPp == null ? "text-gray-400" : a.clvPp > 0 ? "text-emerald-600" : "text-red-500"
                }`}
                    title={a.clvPp == null
                      ? "Grades at game start: positive = the market kept moving toward the flagged team after the alert"
                      : `The market ${a.clvPp > 0 ? "kept moving toward" : "moved away from"} ${sideName(a)} between the alert and the close`}>
                  {a.clvPp != null ? pp(a.clvPp) : "open"}
                </td>
                <td className={`py-1 text-right ${
                  a.outcome === "won" ? "text-emerald-600" : a.outcome === "lost" ? "text-red-500" : "text-gray-400"
                }`}
                    title={a.outcome ? `${sideName(a)} ${a.outcome} the game` : "Settles when the game finishes"}>
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
