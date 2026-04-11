"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import type {
  CrossSlateAccuracyRow,
  PositionAccuracyRow,
  SalaryTierAccuracyRow,
  LeverageCalibrationRow,
  OwnershipVsTeamTotalRow,
  Sport,
} from "@/db/queries";
import { saveHistoricalSlate } from "@/app/dfs/actions";

const fmt1 = (v: number | null | undefined) =>
  v == null ? "—" : v.toFixed(1);
const fmt2 = (v: number | null | undefined) =>
  v == null ? "—" : v.toFixed(2);

function BiasChip({ bias }: { bias: number | null }) {
  if (bias == null) return <span className="text-gray-400">—</span>;
  const pos = bias > 0;
  return (
    <span className={pos ? "text-red-600" : "text-blue-600"}>
      {pos ? "+" : ""}
      {bias.toFixed(2)}
    </span>
  );
}

type Props = {
  crossSlate: CrossSlateAccuracyRow[];
  posAccuracy: PositionAccuracyRow[];
  salaryTier: SalaryTierAccuracyRow[];
  leverageCalib: LeverageCalibrationRow[];
  ownVsTotal: OwnershipVsTeamTotalRow[];
  sport: Sport;
};

export default function AnalyticsClient({
  crossSlate,
  posAccuracy,
  salaryTier,
  leverageCalib,
  ownVsTotal,
  sport,
}: Props) {
  const router = useRouter();
  const hasData = crossSlate.length > 0;
  const sparseOurCoverage = crossSlate.some((row) => row.nOur < row.nLinestar);

  const [historicalDate, setHistoricalDate] = useState("");
  const [historicalText, setHistoricalText] = useState("");
  const [historicalMsg, setHistoricalMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [contestTiming, setContestTiming] = useState<"early" | "main" | "late">("main");
  const [contestFormat, setContestFormat] = useState<"gpp" | "cash">("gpp");
  const [fieldSizeInput, setFieldSizeInput] = useState("");

  async function handleSaveHistorical() {
    if (!historicalDate) {
      setHistoricalMsg({ ok: false, text: "Pick a date first" });
      return;
    }
    if (!historicalText.trim()) {
      setHistoricalMsg({ ok: false, text: "Paste LineStar data first" });
      return;
    }

    setIsSaving(true);
    setHistoricalMsg(null);
    try {
      const fieldSize = fieldSizeInput ? parseInt(fieldSizeInput, 10) : undefined;
      const res = await saveHistoricalSlate(
        sport,
        historicalDate,
        historicalText,
        contestTiming,
        fieldSize && !isNaN(fieldSize) ? fieldSize : undefined,
        contestFormat,
      );
      setHistoricalMsg({ ok: res.ok, text: res.message });
      if (res.ok) {
        setHistoricalText("");
        router.refresh();
      }
    } catch (error) {
      setHistoricalMsg({
        ok: false,
        text: error instanceof Error ? error.message : String(error),
      });
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="space-y-8 p-6 max-w-5xl mx-auto">
      <div>
        <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
        <p className="text-sm text-gray-500 mt-1">
          Cross-slate projection accuracy, position breakdown, salary tier miscalibration, and leverage validation.
        </p>
        {sparseOurCoverage && (
          <p className="mt-2 text-xs text-amber-700">
            Some slates have broader LineStar coverage than `our_proj`. The tables below show both so MLB hitter
            results do not disappear when model coverage is sparse.
          </p>
        )}
      </div>

      <div className="rounded-lg border bg-card p-4 space-y-3">
        <div>
          <h2 className="text-sm font-semibold">Save Historical Slate</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            Navigate to a past date on LineStar, select all rows, and paste below. If the slate was already loaded,
            this updates existing records. Otherwise it creates a new historical slate.
          </p>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div className="flex items-center gap-2">
            <label className="text-xs text-gray-500 whitespace-nowrap">Date</label>
            <input
              type="date"
              value={historicalDate}
              onChange={(e) => setHistoricalDate(e.target.value)}
              className="rounded border px-2 py-1 text-xs"
            />
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Timing</label>
            <div className="flex rounded border text-xs overflow-hidden">
              {(["early", "main", "late"] as const).map((t, i) => (
                <button
                  key={t}
                  onClick={() => setContestTiming(t)}
                  className={`px-3 py-1 capitalize ${i > 0 ? "border-l" : ""} ${
                    contestTiming === t ? "bg-slate-700 text-white" : "text-gray-500 hover:bg-gray-50"
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Format</label>
            <div className="flex rounded border text-xs overflow-hidden">
              {(["gpp", "cash"] as const).map((f, i) => (
                <button
                  key={f}
                  onClick={() => setContestFormat(f)}
                  className={`px-3 py-1 uppercase ${i > 0 ? "border-l" : ""} ${
                    contestFormat === f ? "bg-slate-700 text-white" : "text-gray-500 hover:bg-gray-50"
                  }`}
                >
                  {f}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">
              Field Size <span className="text-gray-400">(opt)</span>
            </label>
            <input
              type="number"
              value={fieldSizeInput}
              onChange={(e) => setFieldSizeInput(e.target.value)}
              placeholder="e.g. 12500"
              className="w-28 rounded border px-2 py-1 text-xs"
            />
          </div>
        </div>
        <textarea
          value={historicalText}
          onChange={(e) => setHistoricalText(e.target.value)}
          rows={5}
          placeholder={
            sport === "mlb"
              ? "OF\tPHI\tBryce Harper\t$5000\t18.2%\t22.1%\t+3.9%\t9.6\t14.0"
              : "C\t\tNikola Jokic\t$12500\t35.1%\t38.2%\t+3.1%\t54.2\t61.5"
          }
          className="w-full rounded border px-2 py-1.5 text-xs font-mono resize-y"
        />
        <div className="flex items-center gap-3">
          <button
            onClick={handleSaveHistorical}
            disabled={isSaving || !historicalDate || !historicalText.trim()}
            className="rounded bg-slate-600 px-4 py-1.5 text-sm text-white hover:bg-slate-700 disabled:opacity-50"
          >
            {isSaving ? "Saving…" : "Save Historical Data"}
          </button>
          {historicalMsg && (
            <span className={`text-sm ${historicalMsg.ok ? "text-green-700" : "text-red-600"}`}>
              {historicalMsg.text}
            </span>
          )}
        </div>
      </div>

      {!hasData && (
        <div className="rounded-lg border bg-card p-6 text-center text-sm text-gray-400">
          <>
            No {sport.toUpperCase()} accuracy data yet — save a historical slate above, or upload a DK results CSV
            from the <a href={`/dfs?sport=${sport}`} className="underline">DFS page</a>.
          </>
        </div>
      )}

      {hasData && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Accuracy Trend - Slate Over Slate</h2>
          <p className="text-xs text-gray-500 mb-4">
            MAE (lower = better). Bias &gt; 0 means over-projection. Ownership correlation closer to 1 = better
            ownership prediction.
          </p>
          <ResponsiveContainer width="100%" height={280}>
            <ComposedChart data={crossSlate} margin={{ top: 4, right: 24, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="slateDate"
                tick={{ fontSize: 11 }}
                tickFormatter={(v: string) => v.slice(5)}
              />
              <YAxis
                yAxisId="mae"
                tick={{ fontSize: 11 }}
                label={{ value: "MAE", angle: -90, position: "insideLeft", fontSize: 11 }}
              />
              <YAxis
                yAxisId="corr"
                orientation="right"
                domain={[0, 1]}
                tick={{ fontSize: 11 }}
                label={{ value: "Own Corr", angle: 90, position: "insideRight", fontSize: 11 }}
              />
              <Tooltip
                formatter={(value) =>
                  [typeof value === "number" ? value.toFixed(2) : "—"]
                }
              />
              <Legend />
              <Line
                yAxisId="mae"
                type="monotone"
                dataKey="ourMAE"
                name="Our MAE"
                stroke="#6366f1"
                strokeWidth={2}
                dot={{ r: 4 }}
                connectNulls={false}
              />
              <Line
                yAxisId="mae"
                type="monotone"
                dataKey="lsMAE"
                name="LineStar MAE"
                stroke="#10b981"
                strokeWidth={2}
                strokeDasharray="4 2"
                dot={{ r: 3 }}
                connectNulls={false}
              />
              <Line
                yAxisId="corr"
                type="monotone"
                dataKey="ownCorr"
                name="Own Correlation"
                stroke="#f59e0b"
                strokeWidth={1.5}
                dot={{ r: 3 }}
                connectNulls={false}
              />
            </ComposedChart>
          </ResponsiveContainer>
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400 border-b">
                  <th className="py-1 text-left">Date</th>
                  <th className="py-1 text-right">Our n</th>
                  <th className="py-1 text-right">LS n</th>
                  <th className="py-1 text-right">Our MAE</th>
                  <th className="py-1 text-right">Our Bias</th>
                  <th className="py-1 text-right">LS MAE</th>
                  <th className="py-1 text-right">LS Bias</th>
                  <th className="py-1 text-right">Own Corr</th>
                </tr>
              </thead>
              <tbody>
                {crossSlate.map((row) => (
                  <tr key={row.slateDate} className="border-b border-gray-50">
                    <td className="py-1">{row.slateDate}</td>
                    <td className="py-1 text-right">{row.nOur}</td>
                    <td className="py-1 text-right">{row.nLinestar}</td>
                    <td className="py-1 text-right font-medium">{fmt1(row.ourMAE)}</td>
                    <td className="py-1 text-right"><BiasChip bias={row.ourBias} /></td>
                    <td className="py-1 text-right">{fmt1(row.lsMAE)}</td>
                    <td className="py-1 text-right"><BiasChip bias={row.lsBias} /></td>
                    <td className="py-1 text-right">{fmt2(row.ownCorr)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {posAccuracy.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Position Accuracy Breakdown</h2>
          <p className="text-xs text-gray-500 mb-4">
            Sorted worst to best MAE. Positive bias = over-projection. `Our` and `LineStar` are separated so sparse
            MLB model coverage does not hide hitter results.
          </p>
          <div className="flex gap-8 flex-wrap">
            <div className="flex-1 min-w-[260px]">
              <ResponsiveContainer width="100%" height={220}>
                <ComposedChart
                  layout="vertical"
                  data={[...posAccuracy].reverse()}
                  margin={{ top: 4, right: 24, bottom: 0, left: 24 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis type="number" tick={{ fontSize: 11 }} />
                  <YAxis dataKey="position" type="category" tick={{ fontSize: 11 }} width={32} />
                  <Tooltip formatter={(v) => [typeof v === "number" ? v.toFixed(2) : "—"]} />
                  <Legend />
                  <Bar dataKey="ourMae" name="Our MAE" fill="#6366f1" radius={[0, 4, 4, 0]} />
                  <Bar dataKey="lsMae" name="LS MAE" fill="#10b981" radius={[0, 4, 4, 0]} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
            <div className="flex-1 min-w-[200px]">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-gray-400 border-b">
                    <th className="py-1 text-left">Pos</th>
                    <th className="py-1 text-right">Our n</th>
                    <th className="py-1 text-right">Our MAE</th>
                    <th className="py-1 text-right">Our Bias</th>
                    <th className="py-1 text-right">LS n</th>
                    <th className="py-1 text-right">LS MAE</th>
                    <th className="py-1 text-right">LS Bias</th>
                  </tr>
                </thead>
                <tbody>
                  {posAccuracy.map((row) => (
                    <tr key={row.position} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.position}</td>
                      <td className="py-1 text-right">{row.ourN}</td>
                      <td className="py-1 text-right">{fmt2(row.ourMae)}</td>
                      <td className="py-1 text-right"><BiasChip bias={row.ourBias} /></td>
                      <td className="py-1 text-right">{row.lsN}</td>
                      <td className="py-1 text-right">{fmt2(row.lsMae)}</td>
                      <td className="py-1 text-right"><BiasChip bias={row.lsBias} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {salaryTier.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Salary Tier Accuracy</h2>
          <p className="text-xs text-gray-500 mb-3">
            Where is the model most miscalibrated? High MAE at $9k+ matters most for GPP. LineStar columns are shown
            separately when MLB model coverage is sparse.
          </p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Tier</th>
                <th className="py-1 text-right">Our n</th>
                <th className="py-1 text-right">Our MAE</th>
                <th className="py-1 text-right">Our Bias</th>
                <th className="py-1 text-right">LS n</th>
                <th className="py-1 text-right">LS MAE</th>
                <th className="py-1 text-right">LS Bias</th>
              </tr>
            </thead>
            <tbody>
              {salaryTier.map((row) => (
                <tr key={row.salaryTier} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.salaryTier}</td>
                  <td className="py-1 text-right">{row.ourN}</td>
                  <td className="py-1 text-right">{fmt2(row.ourMae)}</td>
                  <td className="py-1 text-right"><BiasChip bias={row.ourBias} /></td>
                  <td className="py-1 text-right">{row.lsN}</td>
                  <td className="py-1 text-right">{fmt2(row.lsMae)}</td>
                  <td className="py-1 text-right"><BiasChip bias={row.lsBias} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {ownVsTotal.length > 0 && (
        <div className="rounded-lg border bg-card p-4 space-y-6">
          <div>
            <h2 className="text-sm font-semibold mb-1">Ownership &amp; FPTS vs Team Implied Total</h2>
            <p className="text-xs text-gray-500">
              Do players on high-total teams earn their ownership premium? If ownership rises faster than actual FPTS,
              the market is over-pricing game environment — a systematic GPP fade opportunity.
              Implied totals derived from moneylines + O/U.
            </p>
          </div>

          <div>
            <p className="text-xs text-gray-400 mb-2 font-medium">Ownership by Implied Total</p>
            <ResponsiveContainer width="100%" height={220}>
              <ComposedChart data={ownVsTotal} margin={{ top: 4, right: 24, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="impliedBucket" tick={{ fontSize: 11 }} />
                <YAxis
                  tick={{ fontSize: 11 }}
                  tickFormatter={(v: number) => `${v.toFixed(1)}%`}
                  label={{ value: "Avg Own %", angle: -90, position: "insideLeft", fontSize: 11 }}
                />
                <Tooltip formatter={(value, name) => [typeof value === "number" ? `${value.toFixed(2)}%` : "—", name]} />
                <Legend />
                <Bar dataKey="avgProjOwn" name="Proj Own %" fill="#6366f1" radius={[4, 4, 0, 0]} />
                <Bar dataKey="avgActualOwn" name="Actual Own %" fill="#f59e0b" radius={[4, 4, 0, 0]} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          <div>
            <p className="text-xs text-gray-400 mb-2 font-medium">FPTS by Implied Total</p>
            <ResponsiveContainer width="100%" height={220}>
              <ComposedChart data={ownVsTotal} margin={{ top: 4, right: 24, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="impliedBucket" tick={{ fontSize: 11 }} />
                <YAxis
                  tick={{ fontSize: 11 }}
                  tickFormatter={(v: number) => v.toFixed(1)}
                  label={{ value: "Avg FPTS", angle: -90, position: "insideLeft", fontSize: 11 }}
                />
                <Tooltip formatter={(value, name) => [typeof value === "number" ? value.toFixed(2) : "—", name]} />
                <Legend />
                <Bar dataKey="avgProjFpts" name="Proj FPTS" fill="#6366f1" radius={[4, 4, 0, 0]} />
                <Bar dataKey="avgActualFpts" name="Actual FPTS" fill="#10b981" radius={[4, 4, 0, 0]} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400 border-b">
                  <th className="py-1 text-left">Implied Total</th>
                  <th className="py-1 text-right">n (own)</th>
                  <th className="py-1 text-right">n (fpts)</th>
                  <th className="py-1 text-right">Proj Own</th>
                  <th className="py-1 text-right">Actual Own</th>
                  <th className="py-1 text-right">Own Δ</th>
                  <th className="py-1 text-right">Proj FPTS</th>
                  <th className="py-1 text-right">Actual FPTS</th>
                  <th className="py-1 text-right">FPTS Δ</th>
                </tr>
              </thead>
              <tbody>
                {ownVsTotal.map((row) => {
                  const ownDelta =
                    row.avgActualOwn != null && row.avgProjOwn != null
                      ? row.avgActualOwn - row.avgProjOwn
                      : null;
                  const fptsDelta =
                    row.avgActualFpts != null && row.avgProjFpts != null
                      ? row.avgActualFpts - row.avgProjFpts
                      : null;
                  return (
                    <tr key={row.impliedBucket} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.impliedBucket}</td>
                      <td className="py-1 text-right">{row.nActual}</td>
                      <td className="py-1 text-right">{row.nFpts}</td>
                      <td className="py-1 text-right">
                        {row.avgProjOwn != null ? `${row.avgProjOwn.toFixed(2)}%` : "—"}
                      </td>
                      <td className="py-1 text-right">
                        {row.avgActualOwn != null ? `${row.avgActualOwn.toFixed(2)}%` : "—"}
                      </td>
                      <td className={`py-1 text-right ${ownDelta == null ? "" : ownDelta > 0 ? "text-red-600" : "text-blue-600"}`}>
                        {ownDelta != null ? `${ownDelta > 0 ? "+" : ""}${ownDelta.toFixed(2)}%` : "—"}
                      </td>
                      <td className="py-1 text-right">{fmt1(row.avgProjFpts)}</td>
                      <td className="py-1 text-right">{fmt1(row.avgActualFpts)}</td>
                      <td className={`py-1 text-right ${fptsDelta == null ? "" : fptsDelta > 0 ? "text-green-700" : "text-red-600"}`}>
                        {fptsDelta != null ? `${fptsDelta > 0 ? "+" : ""}${fptsDelta.toFixed(2)}` : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {leverageCalib.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Leverage Model Calibration</h2>
          <p className="text-xs text-gray-500 mb-3">
            Q4 = highest leverage. Positive avg beat = high-leverage players outperformed, which means the leverage
            model is working as intended.
          </p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Quartile</th>
                <th className="py-1 text-right">n</th>
                <th className="py-1 text-right">Avg Leverage</th>
                <th className="py-1 text-right">Avg Proj</th>
                <th className="py-1 text-right">Avg Actual</th>
                <th className="py-1 text-right">Avg Beat</th>
              </tr>
            </thead>
            <tbody>
              {leverageCalib.map((row) => {
                const beatColor =
                  row.avgBeat == null ? "" : row.avgBeat > 0 ? "text-green-700 font-semibold" : "text-red-600";
                return (
                  <tr key={row.leverageQuartile} className="border-b border-gray-50">
                    <td className="py-1 font-medium">
                      {["Q1 (low)", "Q2", "Q3", "Q4 (high)"][row.leverageQuartile - 1] ?? `Q${row.leverageQuartile}`}
                    </td>
                    <td className="py-1 text-right">{row.n}</td>
                    <td className="py-1 text-right">{fmt2(row.avgLeverage)}</td>
                    <td className="py-1 text-right">{fmt1(row.avgProj)}</td>
                    <td className="py-1 text-right">{fmt1(row.avgActual)}</td>
                    <td className={`py-1 text-right ${beatColor}`}>
                      {row.avgBeat != null ? `${row.avgBeat > 0 ? "+" : ""}${row.avgBeat.toFixed(2)}` : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {leverageCalib.length === 4 && (() => {
            const q4 = leverageCalib[3];
            const q1 = leverageCalib[0];
            if (q4?.avgBeat != null && q1?.avgBeat != null) {
              const working = q4.avgBeat > q1.avgBeat;
              return (
                <p className={`mt-2 text-xs ${working ? "text-green-700" : "text-orange-600"}`}>
                  {working
                    ? `Leverage model appears to be working - Q4 beats Q1 by ${(q4.avgBeat - q1.avgBeat).toFixed(2)} FPTS.`
                    : "Leverage model may need recalibration - Q4 does not outperform Q1."}
                </p>
              );
            }
            return null;
          })()}
        </div>
      )}
    </div>
  );
}
