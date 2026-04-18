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
  MlbBattingOrderCalibrationRow,
  ProjectionSourceRow,
  StatLevelAccuracyRow,
  GameTotalModelRow,
  Sport,
} from "@/db/queries";
import { saveHistoricalSlate, uploadResults, fetchPlayerStatsAction } from "@/app/dfs/actions";

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
  battingOrderCalib: MlbBattingOrderCalibrationRow[];
  projSourceBreakdown: ProjectionSourceRow[];
  statLevelAccuracy: StatLevelAccuracyRow[];
  gameTotalModel: GameTotalModelRow[];
  sport: Sport;
};

export default function AnalyticsClient({
  crossSlate,
  posAccuracy,
  salaryTier,
  leverageCalib,
  ownVsTotal,
  battingOrderCalib,
  projSourceBreakdown,
  statLevelAccuracy,
  gameTotalModel,
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

  // Upload Results state
  const resultsFileRef = { current: null as HTMLInputElement | null };
  const [resultsMsg, setResultsMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isUploadingResults, setIsUploadingResults] = useState(false);

  // Fetch Player Stats state
  const [statsDate, setStatsDate] = useState(new Date().toISOString().slice(0, 10));
  const [statsMsg, setStatsMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isFetchingStats, setIsFetchingStats] = useState(false);

  async function handleUploadResults(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    setIsUploadingResults(true);
    setResultsMsg(null);
    const fd = new FormData();
    fd.append("resultsFile", file);
    try {
      const res = await uploadResults(fd);
      setResultsMsg({ ok: res.ok, text: res.message });
      if (res.ok) router.refresh();
    } catch (err) {
      setResultsMsg({ ok: false, text: String(err) });
    } finally {
      setIsUploadingResults(false);
      e.target.value = "";
    }
  }

  async function handleFetchPlayerStats() {
    setIsFetchingStats(true);
    setStatsMsg(null);
    try {
      const res = await fetchPlayerStatsAction(statsDate);
      setStatsMsg({ ok: res.ok, text: res.message });
      if (res.ok) router.refresh();
    } catch (err) {
      setStatsMsg({ ok: false, text: String(err) });
    } finally {
      setIsFetchingStats(false);
    }
  }

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
    <div className="mx-auto max-w-5xl space-y-8 p-6 text-slate-900 [&_.text-gray-100]:text-slate-900 [&_.text-gray-300]:text-slate-700 [&_.text-gray-400]:text-slate-600 [&_.text-gray-500]:text-slate-700">
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

      {/* ── Results Ingestion ────────────────────────────────── */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">

        {/* Upload DK Results CSV */}
        <div className="rounded-lg border bg-card p-4 space-y-2">
          <div>
            <h2 className="text-sm font-semibold">Upload DK Results</h2>
            <p className="text-xs text-gray-500 mt-0.5">
              Upload a DK results or contest-standings CSV after a slate to record{" "}
              <code className="font-mono text-xs bg-gray-100 px-0.5 rounded">actual_fpts</code> and{" "}
              <code className="font-mono text-xs bg-gray-100 px-0.5 rounded">actual_own_pct</code>.
              Targets the most recent {sport.toUpperCase()} slate.
            </p>
          </div>
          <label
            className={`flex items-center justify-center gap-2 rounded border px-3 py-2 text-sm cursor-pointer
              ${isUploadingResults
                ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                : "bg-blue-600 text-white hover:bg-blue-700 border-blue-700"}`}
          >
            {isUploadingResults ? "Uploading…" : "Choose Results CSV"}
            <input
              type="file"
              accept=".csv"
              className="hidden"
              disabled={isUploadingResults}
              onChange={handleUploadResults}
              ref={(el) => { resultsFileRef.current = el; }}
            />
          </label>
          {resultsMsg && (
            <p className={`text-xs rounded px-2 py-1 ${resultsMsg.ok ? "bg-green-50 text-green-800" : "bg-red-50 text-red-800"}`}>
              {resultsMsg.text}
            </p>
          )}
        </div>

        {/* Fetch Player Stats (ESPN) */}
        {sport === "nba" && (
          <div className="rounded-lg border bg-card p-4 space-y-2">
            <div>
              <h2 className="text-sm font-semibold">Fetch Player Stats</h2>
              <p className="text-xs text-gray-500 mt-0.5">
                Pull per-game stat lines (pts/reb/ast/stl/blk/tov/3pm) from ESPN
                box scores for a completed game date. Populates the new stat columns
                for prop accuracy calibration.
              </p>
            </div>
            <div className="flex items-center gap-2">
              <input
                type="date"
                value={statsDate}
                onChange={(e) => setStatsDate(e.target.value)}
                className="rounded border px-2 py-1 text-xs"
              />
              <button
                onClick={handleFetchPlayerStats}
                disabled={isFetchingStats}
                className="rounded border px-3 py-1.5 text-sm bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
              >
                {isFetchingStats ? "Fetching…" : "Fetch Stats"}
              </button>
            </div>
            {statsMsg && (
              <p className={`text-xs rounded px-2 py-1 ${statsMsg.ok ? "bg-green-50 text-green-800" : "bg-red-50 text-red-800"}`}>
                {statsMsg.text}
              </p>
            )}
          </div>
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
                yAxisId="mae"
                type="monotone"
                dataKey="vegasTeamTotalMAE"
                name="Vegas Team MAE"
                stroke="#f97316"
                strokeWidth={1.5}
                strokeDasharray="6 3"
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
                  <th className="py-1 text-right text-orange-600">Vegas Team</th>
                  <th className="py-1 text-right text-orange-600">Vegas Game</th>
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
                    <td className="py-1 text-right text-orange-600">{fmt2(row.vegasTeamTotalMAE)}</td>
                    <td className="py-1 text-right text-orange-600">{fmt2(row.vegasGameTotalMAE)}</td>
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
          <h2 className="text-sm font-semibold mb-1">Optimizer Leverage Calibration</h2>
          <p className="text-xs text-gray-500 mb-3">
            Q4 = highest leverage. This now audits the optimizer-facing leverage/projection fields, not just the legacy
            internal model. Positive avg beat means high-leverage players outperformed.
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

      {/* ------------------------------------------------------------------ */}
      {/* Projection Source Breakdown                                          */}
      {/* ------------------------------------------------------------------ */}
      {projSourceBreakdown.length > 0 && (
        <div className="rounded-lg border bg-card p-6 text-sm space-y-3">
          <h2 className="font-semibold">Projection Source Breakdown (last 20 slates)</h2>
          <p className="text-xs text-gray-500">
            MAE and bias per slate for Live (v2), Our (v1), and LineStar projections.
            Excludes DNPs. Bias: positive = over-projected.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Date</th>
                  <th className="py-1">n(L)</th>
                  <th className="py-1">Live MAE</th>
                  <th className="py-1">Live Bias</th>
                  <th className="py-1">n(O)</th>
                  <th className="py-1">Our MAE</th>
                  <th className="py-1">Our Bias</th>
                  <th className="py-1">n(LS)</th>
                  <th className="py-1">LS MAE</th>
                  <th className="py-1">LS Bias</th>
                </tr>
              </thead>
              <tbody>
                {projSourceBreakdown.map((row) => (
                  <tr key={`${row.slateDate}-${row.sport}`} className="border-b border-gray-50">
                    <td className="py-1 text-left font-medium">{row.slateDate}</td>
                    <td className="py-1 text-right text-gray-400">{row.nLive}</td>
                    <td className="py-1 text-right">{fmt2(row.liveMae)}</td>
                    <td className="py-1 text-right"><BiasChip bias={row.liveBias} /></td>
                    <td className="py-1 text-right text-gray-400">{row.nOur}</td>
                    <td className="py-1 text-right">{fmt2(row.ourMae)}</td>
                    <td className="py-1 text-right"><BiasChip bias={row.ourBias} /></td>
                    <td className="py-1 text-right text-gray-400">{row.nLs}</td>
                    <td className="py-1 text-right">{fmt2(row.lsMae)}</td>
                    <td className="py-1 text-right"><BiasChip bias={row.lsBias} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* MLB Batting Order Calibration                                        */}
      {/* ------------------------------------------------------------------ */}
      {sport === "mlb" && battingOrderCalib.length > 0 && (
        <div className="rounded-lg border bg-card p-6 text-sm space-y-3">
          <h2 className="font-semibold">MLB Batting Order Calibration</h2>
          <p className="text-xs text-gray-500">
            Avg projected vs actual FPTS and ownership by batting order slot.
            Positive delta = under-projected (model was low). Excludes SP/RP.
          </p>
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="border-b text-gray-500 text-right">
                <th className="py-1 text-left">Order</th>
                <th className="py-1">n</th>
                <th className="py-1">Avg Proj</th>
                <th className="py-1">Avg Actual</th>
                <th className="py-1">Delta</th>
                <th className="py-1">Proj Own%</th>
                <th className="py-1">Actual Own%</th>
              </tr>
            </thead>
            <tbody>
              {battingOrderCalib.map((row) => {
                const delta = row.avgDelta;
                const deltaColor =
                  delta == null ? "" : delta > 0.5 ? "text-green-700 font-semibold" : delta < -0.5 ? "text-red-600" : "";
                return (
                  <tr key={row.orderSlot} className="border-b border-gray-50">
                    <td className="py-1 text-left font-medium">#{row.orderSlot}</td>
                    <td className="py-1 text-right text-gray-400">{row.n}</td>
                    <td className="py-1 text-right">{fmt1(row.avgProj)}</td>
                    <td className="py-1 text-right">{fmt1(row.avgActual)}</td>
                    <td className={`py-1 text-right ${deltaColor}`}>
                      {delta != null ? `${delta > 0 ? "+" : ""}${delta.toFixed(2)}` : "—"}
                    </td>
                    <td className="py-1 text-right">
                      {row.avgProjOwn != null ? `${row.avgProjOwn.toFixed(1)}%` : "—"}
                    </td>
                    <td className="py-1 text-right">
                      {row.avgActualOwn != null ? `${row.avgActualOwn.toFixed(1)}%` : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* ── Prop Line Accuracy (stat-level) ──────────────────── */}
      {statLevelAccuracy.length > 0 && statLevelAccuracy.some((r) => r.n > 0) && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div>
            <h2 className="font-semibold">Prop Line Accuracy</h2>
            <p className="text-xs text-gray-500 mt-0.5">
              How accurate are the market prop lines (pts/reb/ast) vs actual stats?
              Bias &gt; 0 = props overestimate. Formula column = games where no prop was available.
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Stat</th>
                  <th className="py-1">Prop Games</th>
                  <th className="py-1">MAE</th>
                  <th className="py-1">Bias</th>
                  <th className="py-1">Formula Games</th>
                </tr>
              </thead>
              <tbody>
                {statLevelAccuracy.map((row) => (
                  <tr key={row.stat} className="border-b border-gray-50">
                    <td className="py-1.5 font-semibold uppercase">{row.stat}</td>
                    <td className="py-1.5 text-right text-gray-400">{row.n}</td>
                    <td className="py-1.5 text-right font-medium">{fmt2(row.mae)}</td>
                    <td className="py-1.5 text-right">
                      <BiasChip bias={row.bias} />
                    </td>
                    <td className="py-1.5 text-right text-gray-400">{row.nFormula}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Game Total Model vs Vegas ─────────────────────────── */}
      {gameTotalModel.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div>
            <h2 className="font-semibold">Game Total Model — Our Prediction vs Vegas</h2>
            <p className="text-xs text-gray-500 mt-0.5">
              Our Ridge model prediction stored before game time vs the Vegas line and actual total.
              Miss = actual − prediction (positive = went over). Populated by{" "}
              <code className="font-mono bg-gray-100 px-1 rounded">python -m ingest.nba_schedule</code>.
            </p>
          </div>
          {(() => {
            const completed = gameTotalModel.filter((r) => r.actualTotal != null);
            const vegasMae = completed.length > 0
              ? completed.reduce((s, r) => s + Math.abs(r.vegasMiss!), 0) / completed.length
              : null;
            const ourMae = completed.length > 0
              ? completed.reduce((s, r) => s + Math.abs(r.ourMiss!), 0) / completed.length
              : null;
            return completed.length > 0 ? (
              <div className="flex gap-6 text-xs mb-1">
                <span>
                  <span className="text-gray-500">Vegas MAE: </span>
                  <span className="font-semibold">{vegasMae?.toFixed(2)}</span>
                </span>
                <span>
                  <span className="text-gray-500">Our MAE: </span>
                  <span className="font-semibold">{ourMae?.toFixed(2)}</span>
                </span>
                <span>
                  <span className="text-gray-500">Improvement: </span>
                  <span className={`font-semibold ${ourMae != null && vegasMae != null && ourMae < vegasMae ? "text-green-700" : "text-red-600"}`}>
                    {vegasMae != null && ourMae != null
                      ? `${(vegasMae - ourMae) > 0 ? "+" : ""}${(vegasMae - ourMae).toFixed(2)}`
                      : "—"}
                  </span>
                </span>
                <span className="text-gray-400">({completed.length} completed games)</span>
              </div>
            ) : null;
          })()}
          <div className="overflow-x-auto max-h-72 overflow-y-auto">
            <table className="w-full text-xs border-collapse">
              <thead className="sticky top-0 bg-white">
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Date</th>
                  <th className="py-1 text-left">Matchup</th>
                  <th className="py-1 text-right">Vegas</th>
                  <th className="py-1 text-right">Our Pred</th>
                  <th className="py-1 text-right">Actual</th>
                  <th className="py-1 text-right">Vegas Miss</th>
                  <th className="py-1 text-right">Our Miss</th>
                </tr>
              </thead>
              <tbody>
                {gameTotalModel.map((row, i) => {
                  const vmColor = row.vegasMiss == null ? "" : Math.abs(row.vegasMiss) > 15 ? "text-red-600 font-semibold" : "";
                  const omColor = row.ourMiss == null ? "" : Math.abs(row.ourMiss) > 15 ? "text-orange-600 font-semibold" : "";
                  return (
                    <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="py-1.5 text-gray-500">{row.gameDate}</td>
                      <td className="py-1.5 font-medium">{row.awayAbbrev} @ {row.homeAbbrev}</td>
                      <td className="py-1.5 text-right">{row.vegasTotal.toFixed(1)}</td>
                      <td className="py-1.5 text-right text-blue-700">{row.ourPred.toFixed(1)}</td>
                      <td className="py-1.5 text-right">{row.actualTotal?.toFixed(0) ?? "—"}</td>
                      <td className={`py-1.5 text-right ${vmColor}`}>
                        {row.vegasMiss != null ? `${row.vegasMiss > 0 ? "+" : ""}${row.vegasMiss.toFixed(1)}` : "—"}
                      </td>
                      <td className={`py-1.5 text-right ${omColor}`}>
                        {row.ourMiss != null ? `${row.ourMiss > 0 ? "+" : ""}${row.ourMiss.toFixed(1)}` : "pending"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
