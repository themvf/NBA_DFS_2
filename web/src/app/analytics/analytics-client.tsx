"use client";

import { useState } from "react";
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
  Sport,
} from "@/db/queries";
import { saveHistoricalSlate } from "@/app/dfs/actions";

const fmt1 = (v: number | null | undefined) =>
  v == null ? "—" : v.toFixed(1);
const fmt2 = (v: number | null | undefined) =>
  v == null ? "—" : v.toFixed(2);
const fmtPct = (v: number | null | undefined) =>
  v == null ? "—" : (v * 100).toFixed(1) + "%";

function BiasChip({ bias }: { bias: number | null }) {
  if (bias == null) return <span className="text-gray-400">—</span>;
  const pos = bias > 0;
  return (
    <span className={pos ? "text-red-600" : "text-blue-600"}>
      {pos ? "+" : ""}{bias.toFixed(2)}
    </span>
  );
}

type Props = {
  crossSlate: CrossSlateAccuracyRow[];
  posAccuracy: PositionAccuracyRow[];
  salaryTier: SalaryTierAccuracyRow[];
  leverageCalib: LeverageCalibrationRow[];
  sport: Sport;
};

export default function AnalyticsClient({
  crossSlate,
  posAccuracy,
  salaryTier,
  leverageCalib,
  sport,
}: Props) {
  const hasData = crossSlate.length > 0;

  // ── Historical slate import ───────────────────────────────
  const [historicalDate, setHistoricalDate] = useState("");
  const [historicalText, setHistoricalText] = useState("");
  const [historicalMsg, setHistoricalMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [contestTiming, setContestTiming] = useState<"early" | "main" | "late">("main");
  const [contestFormat, setContestFormat] = useState<"gpp" | "cash">("gpp");
  const [fieldSizeInput, setFieldSizeInput] = useState("");

  async function handleSaveHistorical() {
    if (!historicalDate) { setHistoricalMsg({ ok: false, text: "Pick a date first" }); return; }
    if (!historicalText.trim()) { setHistoricalMsg({ ok: false, text: "Paste LineStar data first" }); return; }
    setIsSaving(true);
    setHistoricalMsg(null);
    try {
      const fieldSize = fieldSizeInput ? parseInt(fieldSizeInput, 10) : undefined;
      const res = await saveHistoricalSlate(
        sport,
        historicalDate, historicalText,
        contestTiming,
        fieldSize && !isNaN(fieldSize) ? fieldSize : undefined,
        contestFormat,
      );
      setHistoricalMsg({ ok: res.ok, text: res.message });
      if (res.ok) setHistoricalText("");
    } catch (error) {
      setHistoricalMsg({ ok: false, text: error instanceof Error ? error.message : String(error) });
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
      </div>

      {/* Save Historical Slate */}
      <div className="rounded-lg border bg-card p-4 space-y-3">
        <div>
          <h2 className="text-sm font-semibold">Save Historical Slate</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            Navigate to a past date on LineStar → select all rows → paste below.
            If the slate was already loaded, updates existing records. Otherwise creates a new historical slate.
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
          placeholder={sport === "mlb"
            ? "OF\tPHI\tBryce Harper\t$5000\t18.2%\t22.1%\t+3.9%\t9.6\t14.0"
            : "C\t\tNikola Jokic\t$12500\t35.1%\t38.2%\t+3.1%\t54.2\t61.5"}
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
          <>No {sport.toUpperCase()} accuracy data yet — save a historical slate above, or upload a DK results CSV from the <a href={`/dfs?sport=${sport}`} className="underline">DFS page</a>.</>
        </div>
      )}

      {/* 1. Accuracy Trend */}
      {hasData && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Accuracy Trend — Slate Over Slate</h2>
          <p className="text-xs text-gray-500 mb-4">
            MAE (lower = better). Bias &gt; 0 means over-projection. Ownership correlation closer to 1 = better ownership prediction.
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
          {/* Bias table */}
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400 border-b">
                  <th className="py-1 text-left">Date</th>
                  <th className="py-1 text-right">n</th>
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
                    <td className="py-1 text-right">{row.n}</td>
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

      {/* 2. Position Breakdown */}
      {posAccuracy.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Position Accuracy Breakdown</h2>
          <p className="text-xs text-gray-500 mb-4">
            Sorted worst → best MAE. Positive bias = over-projection.
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
                  <Bar dataKey="mae" name="MAE" fill="#6366f1" radius={[0, 4, 4, 0]} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
            <div className="flex-1 min-w-[200px]">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-gray-400 border-b">
                    <th className="py-1 text-left">Pos</th>
                    <th className="py-1 text-right">n</th>
                    <th className="py-1 text-right">MAE</th>
                    <th className="py-1 text-right">Bias</th>
                  </tr>
                </thead>
                <tbody>
                  {posAccuracy.map((row) => (
                    <tr key={row.position} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.position}</td>
                      <td className="py-1 text-right">{row.n}</td>
                      <td className="py-1 text-right">{fmt2(row.mae)}</td>
                      <td className="py-1 text-right"><BiasChip bias={row.bias} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* 3. Salary Tier Accuracy */}
      {salaryTier.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Salary Tier Accuracy</h2>
          <p className="text-xs text-gray-500 mb-3">
            Where is the model most miscalibrated? High MAE at $9k+ matters most for GPP.
          </p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Tier</th>
                <th className="py-1 text-right">n</th>
                <th className="py-1 text-right">MAE</th>
                <th className="py-1 text-right">Bias</th>
              </tr>
            </thead>
            <tbody>
              {salaryTier.map((row) => (
                <tr key={row.salaryTier} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.salaryTier}</td>
                  <td className="py-1 text-right">{row.n}</td>
                  <td className="py-1 text-right">{fmt2(row.mae)}</td>
                  <td className="py-1 text-right"><BiasChip bias={row.bias} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* 4. Leverage Calibration */}
      {leverageCalib.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-1">Leverage Model Calibration</h2>
          <p className="text-xs text-gray-500 mb-3">
            Q4 = highest leverage. Positive avg beat = high-leverage players outperformed — leverage model working as intended.
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
                  row.avgBeat == null ? "" :
                  row.avgBeat > 0 ? "text-green-700 font-semibold" : "text-red-600";
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
                      {row.avgBeat != null ? (row.avgBeat > 0 ? "+" : "") + row.avgBeat.toFixed(2) : "—"}
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
                    ? `Leverage model appears to be working — Q4 beats Q1 by ${(q4.avgBeat - q1.avgBeat).toFixed(2)} FPTS.`
                    : `Leverage model may need recalibration — Q4 does not outperform Q1.`}
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
