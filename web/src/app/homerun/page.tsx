export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { loadHomerunBoardAction } from "@/app/homerun/actions";
import {
  getMlbHomerunBoard,
  getMlbHomerunTrackingReport,
  type MlbHomerunCandidate,
  type MlbHomerunBoard,
  type MlbHomerunBoardView,
  type MlbHomerunTrackingReport,
} from "@/db/queries";

export const metadata: Metadata = {
  title: "MLB Homerun Board",
  description: "Top MLB hitters by 1+ home run probability.",
};

const fmtPct = (value: number | null | undefined, digits = 1) =>
  value == null ? "-" : `${(value * 100).toFixed(digits)}%`;

const fmtNum = (value: number | null | undefined, digits = 2) =>
  value == null ? "-" : value.toFixed(digits);

const fmtWholePct = (value: number | null | undefined, digits = 1) =>
  value == null ? "-" : `${value.toFixed(digits)}%`;

const fmtSignedPct = (value: number | null | undefined, digits = 1) =>
  value == null ? "-" : `${value >= 0 ? "+" : ""}${value.toFixed(digits)} pts`;

const fmtSalary = (salary: number) => `$${salary.toLocaleString()}`;

function fmtAmericanOdds(value: number | null | undefined): string {
  if (value == null) return "-";
  return value > 0 ? `+${value}` : String(value);
}

function fmtDateTime(value: string | null | undefined): string {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function displayPos(positions: string): string {
  return positions.replace(/^UTIL\/?/, "") || positions;
}

function cleanDate(value: string | string[] | undefined): string | null {
  const raw = Array.isArray(value) ? value[0] : value;
  return raw && /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
}

function cleanDkId(value: string | string[] | undefined): number | null {
  const raw = Array.isArray(value) ? value[0] : value;
  if (!raw) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

function cleanView(value: string | string[] | undefined): MlbHomerunBoardView {
  const raw = Array.isArray(value) ? value[0] : value;
  return raw === "edge" || raw === "leverage" || raw === "longshots" ? raw : "likely";
}

function cleanLoadError(value: string | string[] | undefined): string | null {
  const raw = Array.isArray(value) ? value[0] : value;
  return raw ? raw.slice(0, 300) : null;
}

function edgeClass(value: number | null | undefined): string {
  if (value == null) return "text-slate-500";
  if (value >= 2) return "text-emerald-700";
  if (value <= -2) return "text-rose-700";
  return "text-slate-700";
}

function marketLabel(candidate: MlbHomerunCandidate): string {
  if (candidate.marketHrPrice == null && candidate.marketHrImpliedPct == null) return "-";
  const price = fmtAmericanOdds(candidate.marketHrPrice);
  const implied = fmtWholePct(candidate.marketHrImpliedPct);
  return `${price} | ${implied}`;
}

function csvEscape(value: unknown): string {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function buildCsv(candidates: MlbHomerunCandidate[]): string {
  const rows = [
    ["Rank", "Player", "Team", "Opponent", "Salary", "Model HR%", "Market HR%", "Edge Pts", "Book", "Odds", "Batting Order", "Pitcher", "Park"],
    ...candidates.map((candidate, index) => [
      index + 1,
      candidate.name,
      candidate.teamAbbrev,
      candidate.opponentAbbrev ?? "",
      candidate.salary,
      candidate.hrProb1Plus == null ? "" : (candidate.hrProb1Plus * 100).toFixed(2),
      candidate.marketHrImpliedPct == null ? "" : candidate.marketHrImpliedPct.toFixed(2),
      candidate.hrEdgePct == null ? "" : candidate.hrEdgePct.toFixed(2),
      candidate.marketHrBook ?? "",
      candidate.marketHrPrice ?? "",
      candidate.battingOrder ?? "",
      candidate.opposingPitcherName ?? "",
      candidate.ballpark ?? "",
    ]),
  ];
  return rows.map((row) => row.map(csvEscape).join(",")).join("\n");
}

function viewHref(view: MlbHomerunBoardView, date: string | null, dkId: number | null): string {
  const params = new URLSearchParams({ sport: "mlb", view });
  if (dkId != null) params.set("dkId", String(dkId));
  if (date) params.set("date", date);
  return `/homerun?${params.toString()}`;
}

function confidence(candidate: MlbHomerunCandidate): { label: string; className: string } {
  const available = [
    candidate.battingOrder != null,
    candidate.lineupConfirmed === true,
    candidate.hitterHrPg != null,
    candidate.parkHrFactor != null,
    candidate.opposingPitcherName != null,
    hasPitcherStat(candidate),
    candidate.teamTotal != null,
    candidate.weatherTemp != null || candidate.windSpeed != null,
  ].filter(Boolean).length;

  if (available >= 6) return { label: "Strong", className: "border-emerald-200 bg-emerald-50 text-emerald-700" };
  if (available >= 4) return { label: "Good", className: "border-sky-200 bg-sky-50 text-sky-700" };
  return { label: "Thin", className: "border-amber-200 bg-amber-50 text-amber-700" };
}

function hasPitcherStat(candidate: MlbHomerunCandidate): boolean {
  return candidate.opposingPitcherHrPer9 != null
    || candidate.opposingPitcherXfip != null
    || candidate.opposingPitcherEra != null
    || candidate.opposingPitcherHrFbPct != null;
}

function pitcherMetricLine(candidate: MlbHomerunCandidate): string {
  const metrics = [
    candidate.opposingPitcherHand,
    candidate.opposingPitcherHrPer9 != null ? `${candidate.opposingPitcherHrPer9.toFixed(2)} HR/9` : null,
    candidate.opposingPitcherXfip != null ? `xFIP ${candidate.opposingPitcherXfip.toFixed(2)}` : null,
    candidate.opposingPitcherEra != null && candidate.opposingPitcherXfip == null ? `ERA ${candidate.opposingPitcherEra.toFixed(2)}` : null,
  ].filter(Boolean);
  return metrics.length > 0 ? metrics.join(" | ") : "Pitcher stats -";
}

function pitcherSampleLabel(candidate: MlbHomerunCandidate): string {
  const games = candidate.opposingPitcherGames != null && candidate.opposingPitcherGames > 0
    ? `${candidate.opposingPitcherGames} G`
    : null;
  const ipPg = candidate.opposingPitcherIpPg != null && candidate.opposingPitcherIpPg > 0
    ? `${candidate.opposingPitcherIpPg.toFixed(1)} IP/G`
    : null;
  return [games, ipPg].filter(Boolean).join(" | ") || "Pitcher sample -";
}

function pitcherRisk(candidate: MlbHomerunCandidate): { label: string; className: string } {
  const hrPer9 = candidate.opposingPitcherHrPer9;
  const xfip = candidate.opposingPitcherXfip;
  if (hrPer9 == null && xfip == null) return { label: "Pitcher ?", className: "border-slate-200 bg-slate-50 text-slate-600" };
  if ((hrPer9 != null && hrPer9 >= 1.35) || (xfip != null && xfip >= 4.7)) {
    return { label: "Pitcher HR+", className: "border-rose-200 bg-rose-50 text-rose-700" };
  }
  if (hrPer9 != null && xfip != null && hrPer9 <= 0.85 && xfip <= 3.8) {
    return { label: "Pitcher HR-", className: "border-blue-200 bg-blue-50 text-blue-700" };
  }
  return { label: "Pitcher Mid", className: "border-slate-200 bg-slate-50 text-slate-700" };
}

function parkRisk(candidate: MlbHomerunCandidate): { label: string; className: string } {
  const factor = candidate.parkHrFactor;
  if (factor == null) return { label: "Park ?", className: "border-slate-200 bg-slate-50 text-slate-600" };
  if (factor >= 1.06) return { label: `Park ${factor.toFixed(2)}x`, className: "border-rose-200 bg-rose-50 text-rose-700" };
  if (factor <= 0.94) return { label: `Park ${factor.toFixed(2)}x`, className: "border-blue-200 bg-blue-50 text-blue-700" };
  return { label: `Park ${factor.toFixed(2)}x`, className: "border-slate-200 bg-slate-50 text-slate-700" };
}

function lineupStatus(candidate: MlbHomerunCandidate): { label: string; className: string } {
  if (candidate.lineupConfirmed && candidate.battingOrder != null) {
    return { label: `Order ${candidate.battingOrder}`, className: "border-emerald-200 bg-emerald-50 text-emerald-700" };
  }
  if (candidate.battingOrder != null) {
    return { label: `Order ${candidate.battingOrder}`, className: "border-sky-200 bg-sky-50 text-sky-700" };
  }
  return { label: "Order ?", className: "border-amber-200 bg-amber-50 text-amber-700" };
}

function weatherLabel(candidate: MlbHomerunCandidate): string {
  const temp = candidate.weatherTemp != null ? `${candidate.weatherTemp}F` : null;
  const wind = candidate.windSpeed != null
    ? `${candidate.windSpeed} mph${candidate.windDirection ? ` ${candidate.windDirection}` : ""}`
    : null;
  return [temp, wind].filter(Boolean).join(" | ") || "Weather -";
}

function FactorChip({ label, className }: { label: string; className: string }) {
  return (
    <span className={`inline-flex items-center rounded border px-2 py-0.5 text-[11px] font-medium ${className}`}>
      {label}
    </span>
  );
}

function TrackingPanel({ tracking }: { tracking: MlbHomerunTrackingReport | null }) {
  if (!tracking) return null;
  const summary = tracking.summary;
  const hasActualHr = summary.actualHrRows > 0;

  return (
    <section className="border-t pt-5">
      <div className="flex flex-col gap-1 border-b pb-3 md:flex-row md:items-end md:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-slate-950">Homerun Model Tracking</h2>
          <p className="text-xs text-slate-500">
            {summary.rows.toLocaleString()} snapshots | {summary.slates} slates | {summary.latestVersion ?? "model"}
            {summary.latestSlateDate ? ` | latest slate ${summary.latestSlateDate}` : ""}
          </p>
        </div>
        <div className="text-xs text-slate-500">
          HR outcomes {summary.actualHrRows.toLocaleString()} known | {summary.pendingHrRows.toLocaleString()} pending
        </div>
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-4">
        <div className="rounded-md border border-slate-200 p-3">
          <div className="text-xs text-slate-500">Avg Prediction</div>
          <div className="mt-1 text-2xl font-semibold text-slate-950">{fmtWholePct(summary.avgPredictedPct)}</div>
          <div className="mt-1 text-xs text-slate-500">All tracked hitters</div>
        </div>
        <div className="rounded-md border border-slate-200 p-3">
          <div className="text-xs text-slate-500">Actual HR Rate</div>
          <div className="mt-1 text-2xl font-semibold text-slate-950">{fmtWholePct(summary.hitRate)}</div>
          <div className="mt-1 text-xs text-slate-500">{hasActualHr ? "Known outcomes" : "Awaiting HR results"}</div>
        </div>
        <div className="rounded-md border border-slate-200 p-3">
          <div className="text-xs text-slate-500">Top-15 Hit Rate</div>
          <div className="mt-1 text-2xl font-semibold text-rose-700">{fmtWholePct(summary.top15HitRate)}</div>
          <div className="mt-1 text-xs text-slate-500">
            {summary.top15ActualHrRows.toLocaleString()} result rows
          </div>
        </div>
        <div className="rounded-md border border-slate-200 p-3">
          <div className="text-xs text-slate-500">Brier Score</div>
          <div className="mt-1 text-2xl font-semibold text-slate-950">{fmtNum(summary.brierScore, 3)}</div>
          <div className="mt-1 text-xs text-slate-500">Lower is better</div>
        </div>
      </div>

      {tracking.buckets.length > 0 && (
        <div className="mt-4 overflow-x-auto">
          <table className="w-full min-w-[620px] border-collapse text-sm">
            <thead>
              <tr className="border-b bg-slate-50 text-right text-xs uppercase tracking-wide text-slate-500">
                <th className="py-2 pl-3 pr-3 text-left">Prediction Bucket</th>
                <th className="py-2 pr-3">Rows</th>
                <th className="py-2 pr-3">Actual Rows</th>
                <th className="py-2 pr-3">Avg Pred</th>
                <th className="py-2 pr-3">Hit Rate</th>
                <th className="py-2 pr-3">Brier</th>
              </tr>
            </thead>
            <tbody>
              {tracking.buckets.map((bucket) => (
                <tr key={bucket.bucket} className="border-b border-slate-100 text-right">
                  <td className="py-2 pl-3 pr-3 text-left font-medium text-slate-900">{bucket.bucket}</td>
                  <td className="py-2 pr-3 text-slate-700">{bucket.rows.toLocaleString()}</td>
                  <td className="py-2 pr-3 text-slate-700">{bucket.actualHrRows.toLocaleString()}</td>
                  <td className="py-2 pr-3 text-slate-700">{fmtWholePct(bucket.avgPredictedPct)}</td>
                  <td className="py-2 pr-3 text-slate-700">{fmtWholePct(bucket.hitRate)}</td>
                  <td className="py-2 pr-3 text-slate-700">{fmtNum(bucket.brierScore, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

const VIEW_LABELS: Record<MlbHomerunBoardView, string> = {
  likely: "Most Likely",
  edge: "Best Edge",
  leverage: "Leverage",
  longshots: "Longshots",
};

const VIEW_TITLES: Record<MlbHomerunBoardView, string> = {
  likely: "Top 15 by 1+ HR chance",
  edge: "Top 15 by market edge",
  leverage: "Top 15 by leverage-adjusted edge",
  longshots: "Top 15 HR longshots",
};

function StoryPanel({ board }: { board: MlbHomerunBoard }) {
  if (board.candidates.length === 0) return null;
  const topModel = [...board.candidates].sort((a, b) => (b.hrProb1Plus ?? 0) - (a.hrProb1Plus ?? 0))[0];
  const marketCandidates = board.candidates.filter((candidate) => candidate.marketHrImpliedPct != null);
  const bestEdge = [...marketCandidates].sort((a, b) => (b.hrEdgePct ?? -999) - (a.hrEdgePct ?? -999))[0] ?? null;
  const positiveEdges = marketCandidates.filter((candidate) => (candidate.hrEdgePct ?? -999) > 0).length;
  const marketCoverage = board.candidates.length ? (marketCandidates.length / board.candidates.length) * 100 : null;

  return (
    <section className="rounded-lg border border-slate-200 bg-slate-50 p-4">
      <div className="grid gap-3 md:grid-cols-4">
        <div>
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">Board Mode</div>
          <div className="mt-1 text-lg font-semibold text-slate-950">{VIEW_LABELS[board.view]}</div>
          <div className="mt-1 text-xs text-slate-500">
            Market latest {fmtDateTime(board.latestMarketCapturedAt)}
          </div>
        </div>
        <div>
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">Best Model</div>
          <div className="mt-1 truncate text-lg font-semibold text-rose-700">{topModel.name}</div>
          <div className="mt-1 text-xs text-slate-500">{fmtPct(topModel.hrProb1Plus)} HR chance</div>
        </div>
        <div>
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">Best Edge</div>
          <div className="mt-1 truncate text-lg font-semibold text-emerald-700">{bestEdge?.name ?? "-"}</div>
          <div className="mt-1 text-xs text-slate-500">{bestEdge ? fmtSignedPct(bestEdge.hrEdgePct) : "No HR market"}</div>
        </div>
        <div>
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">Market Coverage</div>
          <div className="mt-1 text-lg font-semibold text-slate-950">{fmtWholePct(marketCoverage, 0)}</div>
          <div className="mt-1 text-xs text-slate-500">{positiveEdges} visible positive edges</div>
        </div>
      </div>
    </section>
  );
}

function scatterPointColor(edge: number | null | undefined): string {
  if (edge == null) return "#64748b";
  if (edge >= 2) return "#059669";
  if (edge > 0) return "#0284c7";
  if (edge <= -2) return "#e11d48";
  return "#64748b";
}

type HomerunScatterPoint = {
  candidate: MlbHomerunCandidate;
  market: number | null;
  model: number;
  edge: number | null;
};

function scatterPointFill(point: HomerunScatterPoint): string {
  if (point.market != null) return scatterPointColor(point.edge);
  if (point.model >= 10) return "#334155";
  if (point.model >= 7) return "#64748b";
  return "#94a3b8";
}

function scatterParkStroke(candidate: MlbHomerunCandidate): string {
  const factor = candidate.parkHrFactor;
  if (factor == null) return "#ffffff";
  if (factor >= 1.06) return "#fb7185";
  if (factor <= 0.94) return "#2563eb";
  return "#ffffff";
}

function scatterParkStrokeWidth(candidate: MlbHomerunCandidate, isTopTable: boolean): number {
  const factor = candidate.parkHrFactor;
  if (factor == null) return isTopTable ? 1.5 : 1;
  return factor >= 1.06 || factor <= 0.94 ? 2.75 : isTopTable ? 1.5 : 1;
}

function pitcherHrRiskLevel(candidate: MlbHomerunCandidate): "high" | "mid" | "low" | "unknown" {
  const hrPer9 = candidate.opposingPitcherHrPer9;
  const xfip = candidate.opposingPitcherXfip;
  if (hrPer9 == null && xfip == null) return "unknown";
  if ((hrPer9 != null && hrPer9 >= 1.35) || (xfip != null && xfip >= 4.7)) return "high";
  if (hrPer9 != null && xfip != null && hrPer9 <= 0.85 && xfip <= 3.8) return "low";
  return "mid";
}

function scatterRadius(candidate: MlbHomerunCandidate, isTopTable: boolean): number {
  const risk = pitcherHrRiskLevel(candidate);
  const base = isTopTable ? 5.5 : 4;
  if (risk === "high") return base + 2;
  if (risk === "low") return Math.max(3.25, base - 1);
  return base;
}

function scatterUncertaintyStroke(point: HomerunScatterPoint): string | null {
  if (point.market == null) return "#f59e0b";
  if (pitcherHrRiskLevel(point.candidate) === "unknown") return "#64748b";
  return null;
}

function scatterTooltip(point: HomerunScatterPoint): string {
  const c = point.candidate;
  return [
    `${c.name} (${c.teamAbbrev}${c.opponentAbbrev ? ` vs ${c.opponentAbbrev}` : ""})`,
    `Model ${fmtWholePct(point.model)} | Market ${point.market == null ? "No HR odds" : fmtWholePct(point.market)} | Edge ${fmtSignedPct(point.edge)}`,
    point.market == null ? "No HR market found; edge cannot be calculated." : null,
    `Park ${c.ballpark ?? "-"}${c.parkHrFactor != null ? ` (${c.parkHrFactor.toFixed(2)}x HR)` : ""}`,
    c.opposingPitcherName ? `Pitcher ${c.opposingPitcherName}` : "Pitcher not announced; model may move when starter posts.",
    `${pitcherMetricLine(c)} | ${pitcherSampleLabel(c)}`,
    `Order ${c.battingOrder ?? "-"} | Book ${c.marketHrBook ?? "-"} | Odds ${fmtAmericanOdds(c.marketHrPrice)}`,
  ].filter(Boolean).join("\n");
}

function ScatterEdgeListItem({ candidate, rank }: { candidate: MlbHomerunCandidate; rank: number }) {
  return (
    <li className="flex items-start justify-between gap-3 border-b border-slate-100 py-2 last:border-0">
      <div className="min-w-0">
        <div className="truncate text-sm font-medium text-slate-950">
          {rank}. {candidate.name}
        </div>
        <div className="mt-0.5 text-xs text-slate-500">
          {candidate.teamAbbrev}
          {candidate.opponentAbbrev ? ` vs ${candidate.opponentAbbrev}` : ""}
          {candidate.battingOrder != null ? ` | Order ${candidate.battingOrder}` : ""}
        </div>
      </div>
      <div className="shrink-0 text-right">
        <div className={`text-sm font-semibold ${edgeClass(candidate.hrEdgePct)}`}>{fmtSignedPct(candidate.hrEdgePct)}</div>
        <div className="text-xs text-slate-500">
          {fmtPct(candidate.hrProb1Plus)} vs {fmtWholePct(candidate.marketHrImpliedPct)}
        </div>
      </div>
    </li>
  );
}

function ScatterNoMarketListItem({ candidate, rank }: { candidate: MlbHomerunCandidate; rank: number }) {
  return (
    <li className="flex items-start justify-between gap-3 border-b border-slate-100 py-2 last:border-0">
      <div className="min-w-0">
        <div className="truncate text-sm font-medium text-slate-950">
          {rank}. {candidate.name}
        </div>
        <div className="mt-0.5 text-xs text-slate-500">
          {candidate.teamAbbrev}
          {candidate.opponentAbbrev ? ` vs ${candidate.opponentAbbrev}` : ""}
          {candidate.opposingPitcherName ? ` | ${candidate.opposingPitcherName}` : " | SP ?"}
        </div>
      </div>
      <div className="shrink-0 text-right">
        <div className="text-sm font-semibold text-slate-800">{fmtPct(candidate.hrProb1Plus)}</div>
        <div className="text-xs text-amber-700">No market</div>
      </div>
    </li>
  );
}

function HomerunScatterPlot({ board }: { board: MlbHomerunBoard }) {
  const points = board.scatterCandidates
    .filter((candidate) => candidate.hrProb1Plus != null)
    .map((candidate) => ({
      candidate,
      market: candidate.marketHrImpliedPct,
      model: (candidate.hrProb1Plus ?? 0) * 100,
      edge: candidate.hrEdgePct ?? null,
    }));

  const width = 760;
  const height = 420;
  const pad = { left: 58, right: 24, top: 30, bottom: 64 };
  const noMarketRailWidth = 72;
  const marketAxisLeft = pad.left + noMarketRailWidth;
  const noMarketX = pad.left + noMarketRailWidth / 2;
  const plotWidth = width - marketAxisLeft - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const marketPoints = points.filter((point) => point.market != null);
  const noMarketPoints = points.filter((point) => point.market == null);
  const rawMax = points.length > 0
    ? Math.max(12, ...points.flatMap((point) => [point.market ?? 0, point.model])) + 1
    : 12;
  const axisMax = Math.ceil(rawMax / 2) * 2;
  const xScale = (value: number) => marketAxisLeft + (Math.max(0, Math.min(axisMax, value)) / axisMax) * plotWidth;
  const yScale = (value: number) => pad.top + plotHeight - (Math.max(0, Math.min(axisMax, value)) / axisMax) * plotHeight;
  const ticks = Array.from({ length: 5 }, (_, index) => (axisMax / 4) * index);

  if (points.length === 0) {
    return (
      <section className="rounded-lg border border-slate-200 bg-white p-4">
        <div className="flex flex-col gap-1 md:flex-row md:items-end md:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-950">Model vs Market</h2>
            <p className="text-sm text-slate-500">
              No same-date `batter_home_runs` odds rows were found for these {board.totalQualified.toLocaleString()} qualified hitters.
            </p>
          </div>
          <div className="text-xs text-slate-500">Use Load to refresh HR odds from the Odds API.</div>
        </div>
        <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div className="overflow-hidden rounded-md border border-slate-100 bg-slate-50 p-2">
            <svg
              role="img"
              aria-label="Empty scatterplot waiting for MLB homerun market odds"
              viewBox={`0 0 ${width} ${height}`}
              className="h-auto w-full"
            >
              <rect x="0" y="0" width={width} height={height} fill="#f8fafc" />
              {ticks.map((tick) => (
                <g key={`empty-grid-${tick}`}>
                  <line x1={xScale(tick)} x2={xScale(tick)} y1={pad.top} y2={pad.top + plotHeight} stroke="#e2e8f0" />
                  <line x1={pad.left} x2={marketAxisLeft + plotWidth} y1={yScale(tick)} y2={yScale(tick)} stroke="#e2e8f0" />
                  <text x={xScale(tick)} y={pad.top + plotHeight + 22} textAnchor="middle" fontSize="12" fill="#64748b">
                    {fmtWholePct(tick, tick % 1 === 0 ? 0 : 1)}
                  </text>
                  <text x={pad.left - 10} y={yScale(tick) + 4} textAnchor="end" fontSize="12" fill="#64748b">
                    {fmtWholePct(tick, tick % 1 === 0 ? 0 : 1)}
                  </text>
                </g>
              ))}
              <line x1={xScale(0)} x2={xScale(axisMax)} y1={yScale(0)} y2={yScale(axisMax)} stroke="#334155" strokeDasharray="5 5" strokeWidth="1.5" />
              <line x1={pad.left} x2={marketAxisLeft + plotWidth} y1={pad.top + plotHeight} y2={pad.top + plotHeight} stroke="#94a3b8" />
              <line x1={pad.left} x2={pad.left} y1={pad.top} y2={pad.top + plotHeight} stroke="#94a3b8" />
              <line x1={marketAxisLeft - 12} x2={marketAxisLeft - 12} y1={pad.top} y2={pad.top + plotHeight} stroke="#f59e0b" strokeDasharray="4 4" opacity="0.7" />
              <text x={noMarketX} y={pad.top + plotHeight + 22} textAnchor="middle" fontSize="12" fontWeight="600" fill="#b45309">
                No market
              </text>
              <text x={marketAxisLeft + plotWidth / 2} y={height - 14} textAnchor="middle" fontSize="13" fontWeight="600" fill="#334155">
                Market implied HR probability
              </text>
              <text x="18" y={pad.top + plotHeight / 2} textAnchor="middle" fontSize="13" fontWeight="600" fill="#334155" transform={`rotate(-90 18 ${pad.top + plotHeight / 2})`}>
                Model HR probability
              </text>
              <rect x={pad.left + 92} y={pad.top + 118} width="494" height="92" rx="8" fill="#ffffff" stroke="#cbd5e1" />
              <text x={pad.left + 339} y={pad.top + 154} textAnchor="middle" fontSize="16" fontWeight="700" fill="#0f172a">
                HR market odds not loaded yet
              </text>
              <text x={pad.left + 339} y={pad.top + 181} textAnchor="middle" fontSize="13" fill="#475569">
                Click Load, then this chart will plot model probability against market probability.
              </text>
            </svg>
          </div>
          <aside className="rounded-md border border-slate-200 p-3">
            <h3 className="text-sm font-semibold text-slate-950">What This Needs</h3>
            <p className="mt-2 text-sm text-slate-600">
              The model has hitter probabilities, but the market side needs Odds API `batter_home_runs` rows for the same MLB date.
            </p>
            <div className="mt-3 rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
              If Load was already clicked and this remains empty, the Odds API did not return HR props for this slate yet.
            </div>
          </aside>
        </div>
      </section>
    );
  }

  const topTableIds = new Set(board.candidates.map((candidate) => candidate.id));
  const positiveEdges = marketPoints
    .filter((point) => (point.edge ?? -999) > 0)
    .sort((a, b) => (b.edge ?? -999) - (a.edge ?? -999));
  const bestEdges = positiveEdges.slice(0, 6).map((point) => point.candidate);
  const biggestDiscount = marketPoints
    .filter((point) => (point.edge ?? 999) < 0)
    .sort((a, b) => (a.edge ?? 999) - (b.edge ?? 999))[0]?.candidate ?? null;
  const medianEdge = marketPoints
    .map((point) => point.edge)
    .filter((edge): edge is number => edge != null)
    .sort((a, b) => a - b);
  const medianEdgeValue = medianEdge.length ? medianEdge[Math.floor(medianEdge.length / 2)] : null;
  const bestNoMarket = noMarketPoints
    .sort((a, b) => b.model - a.model)
    .slice(0, 5)
    .map((point) => point.candidate);

  return (
    <section className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="flex flex-col gap-1 md:flex-row md:items-end md:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-slate-950">Model vs Market</h2>
          <p className="text-sm text-slate-500">
            {marketPoints.length} hitters with HR odds | {noMarketPoints.length} no-market | {positiveEdges.length} positive edges | median edge {fmtSignedPct(medianEdgeValue)}
          </p>
        </div>
        <div className="flex flex-wrap justify-start gap-x-3 gap-y-2 text-xs md:justify-end">
          <span className="inline-flex items-center gap-1 text-emerald-700"><span className="h-2.5 w-2.5 rounded-full bg-emerald-600" />+2 edge</span>
          <span className="inline-flex items-center gap-1 text-sky-700"><span className="h-2.5 w-2.5 rounded-full bg-sky-600" />positive edge</span>
          <span className="inline-flex items-center gap-1 text-rose-700"><span className="h-2.5 w-2.5 rounded-full bg-rose-600" />negative edge</span>
          <span className="inline-flex items-center gap-1 text-rose-700"><span className="h-3 w-3 rounded-full border-2 border-rose-400 bg-white" />HR park</span>
          <span className="inline-flex items-center gap-1 text-blue-700"><span className="h-3 w-3 rounded-full border-2 border-blue-600 bg-white" />cold park</span>
          <span className="inline-flex items-center gap-1 text-slate-600">
            <svg viewBox="0 0 32 12" className="h-3 w-8" aria-hidden="true">
              <circle cx="6" cy="6" r="3" fill="#64748b" />
              <circle cx="19" cy="6" r="5" fill="#64748b" />
            </svg>
            pitcher HR risk
          </span>
          <span className="inline-flex items-center gap-1 text-amber-700"><span className="h-3 w-3 rounded-full border-2 border-dashed border-amber-500 bg-slate-400" />no HR odds</span>
          <span className="inline-flex items-center gap-1 text-slate-600"><span className="h-3 w-3 rounded-full border-2 border-dashed border-slate-500 bg-slate-400" />SP unknown</span>
        </div>
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
        <div className="overflow-hidden rounded-md border border-slate-100 bg-slate-50 p-2">
          <svg
            role="img"
            aria-label="Scatterplot of MLB homerun model probability versus market implied probability"
            viewBox={`0 0 ${width} ${height}`}
            className="h-auto w-full"
          >
            <rect x="0" y="0" width={width} height={height} fill="#f8fafc" />
            <rect x={pad.left} y={pad.top} width={noMarketRailWidth - 12} height={plotHeight} fill="#f59e0b" opacity="0.05" />
            <polygon
              points={`${xScale(0)},${yScale(0)} ${xScale(0)},${yScale(axisMax)} ${xScale(axisMax)},${yScale(axisMax)}`}
              fill="#10b981"
              opacity="0.08"
            />
            {ticks.map((tick) => (
              <g key={`grid-${tick}`}>
                <line x1={xScale(tick)} x2={xScale(tick)} y1={pad.top} y2={pad.top + plotHeight} stroke="#e2e8f0" />
                <line x1={pad.left} x2={marketAxisLeft + plotWidth} y1={yScale(tick)} y2={yScale(tick)} stroke="#e2e8f0" />
                <text x={xScale(tick)} y={pad.top + plotHeight + 22} textAnchor="middle" fontSize="12" fill="#64748b">
                  {fmtWholePct(tick, tick % 1 === 0 ? 0 : 1)}
                </text>
                <text x={pad.left - 10} y={yScale(tick) + 4} textAnchor="end" fontSize="12" fill="#64748b">
                  {fmtWholePct(tick, tick % 1 === 0 ? 0 : 1)}
                </text>
              </g>
            ))}
            <line x1={xScale(0)} x2={xScale(axisMax)} y1={yScale(0)} y2={yScale(axisMax)} stroke="#334155" strokeDasharray="5 5" strokeWidth="1.5" />
            <text x={xScale(axisMax) - 8} y={pad.top + 16} textAnchor="end" fontSize="12" fill="#334155">
              fair line
            </text>
            <line x1={pad.left} x2={marketAxisLeft + plotWidth} y1={pad.top + plotHeight} y2={pad.top + plotHeight} stroke="#94a3b8" />
            <line x1={pad.left} x2={pad.left} y1={pad.top} y2={pad.top + plotHeight} stroke="#94a3b8" />
            <line x1={marketAxisLeft - 12} x2={marketAxisLeft - 12} y1={pad.top} y2={pad.top + plotHeight} stroke="#f59e0b" strokeDasharray="4 4" opacity="0.7" />
            <text x={noMarketX} y={pad.top + plotHeight + 22} textAnchor="middle" fontSize="12" fontWeight="600" fill="#b45309">
              No market
            </text>
            <text x={marketAxisLeft + plotWidth / 2} y={height - 14} textAnchor="middle" fontSize="13" fontWeight="600" fill="#334155">
              Market implied HR probability
            </text>
            <text x="18" y={pad.top + plotHeight / 2} textAnchor="middle" fontSize="13" fontWeight="600" fill="#334155" transform={`rotate(-90 18 ${pad.top + plotHeight / 2})`}>
              Model HR probability
            </text>
            {points.map((point) => {
              const isTopTable = topTableIds.has(point.candidate.id);
              const x = point.market == null ? noMarketX : xScale(point.market);
              const radius = scatterRadius(point.candidate, isTopTable);
              const uncertaintyStroke = scatterUncertaintyStroke(point);
              return (
                <g key={point.candidate.id}>
                  <circle
                    cx={x}
                    cy={yScale(point.model)}
                    r={radius}
                    fill={scatterPointFill(point)}
                    opacity={isTopTable ? 0.95 : 0.72}
                    stroke={scatterParkStroke(point.candidate)}
                    strokeWidth={scatterParkStrokeWidth(point.candidate, isTopTable)}
                  >
                    <title>{scatterTooltip(point)}</title>
                  </circle>
                  {uncertaintyStroke && (
                    <circle
                      cx={x}
                      cy={yScale(point.model)}
                      r={radius + 3}
                      fill="none"
                      stroke={uncertaintyStroke}
                      strokeDasharray="4 3"
                      strokeWidth="1.75"
                      opacity="0.9"
                    >
                      <title>{scatterTooltip(point)}</title>
                    </circle>
                  )}
                </g>
              );
            })}
          </svg>
        </div>

        <aside className="rounded-md border border-slate-200 p-3">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold text-slate-950">Largest Positive Edges</h3>
              <p className="mt-0.5 text-xs text-slate-500">Dot size reflects pitcher HR risk; ring color marks park context.</p>
            </div>
            {biggestDiscount && (
              <div className="text-right text-xs text-slate-500">
                Biggest fade
                <div className="font-semibold text-rose-700">{biggestDiscount.name}</div>
              </div>
            )}
          </div>
          {bestEdges.length > 0 ? (
            <ol className="mt-2">
              {bestEdges.map((candidate, index) => (
                <ScatterEdgeListItem key={candidate.id} candidate={candidate} rank={index + 1} />
              ))}
            </ol>
          ) : (
            <div className="mt-3 rounded border border-slate-200 bg-slate-50 p-3 text-sm text-slate-600">
              No positive model-vs-market HR edges are visible on this slate.
            </div>
          )}
          {bestNoMarket.length > 0 && (
            <div className="mt-4 border-t border-slate-200 pt-3">
              <h3 className="text-sm font-semibold text-slate-950">Top No-Market Model Likes</h3>
              <ol className="mt-2">
                {bestNoMarket.map((candidate, index) => (
                  <ScatterNoMarketListItem key={candidate.id} candidate={candidate} rank={index + 1} />
                ))}
              </ol>
            </div>
          )}
        </aside>
      </div>
    </section>
  );
}

function CandidateCard({ candidate, rank }: { candidate: MlbHomerunCandidate; rank: number }) {
  const conf = confidence(candidate);
  const pitch = pitcherRisk(candidate);
  const park = parkRisk(candidate);
  const lineup = lineupStatus(candidate);
  return (
    <article className="rounded-lg border bg-card p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-xs font-semibold text-slate-500">#{rank}</div>
          <h2 className="mt-1 truncate text-lg font-semibold text-slate-950">{candidate.name}</h2>
          <p className="mt-0.5 text-xs text-slate-500">
            {candidate.teamAbbrev}
            {candidate.opponentAbbrev ? ` vs ${candidate.opponentAbbrev}` : ""}
            {" | "}
            {displayPos(candidate.eligiblePositions)}
            {" | "}
            DK {candidate.dkPlayerId}
          </p>
        </div>
        <div className="text-right">
          <div className="text-3xl font-semibold text-rose-700">{fmtPct(candidate.hrProb1Plus)}</div>
          <div className="text-[11px] text-slate-500">1+ HR</div>
          <div className={`mt-1 text-xs font-semibold ${edgeClass(candidate.hrEdgePct)}`}>
            {fmtSignedPct(candidate.hrEdgePct)}
          </div>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-4 gap-2 text-xs">
        <div>
          <div className="text-slate-400">Exp HR</div>
          <div className="font-semibold text-slate-900">{fmtNum(candidate.expectedHr, 3)}</div>
        </div>
        <div>
          <div className="text-slate-400">HR/G</div>
          <div className="font-semibold text-slate-900">{fmtNum(candidate.hitterHrPg, 2)}</div>
        </div>
        <div>
          <div className="text-slate-400">Team Tot</div>
          <div className="font-semibold text-slate-900">{fmtNum(candidate.teamTotal, 1)}</div>
        </div>
        <div>
          <div className="text-slate-400">Market</div>
          <div className="font-semibold text-slate-900">{fmtWholePct(candidate.marketHrImpliedPct)}</div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-1.5">
        <FactorChip {...lineup} />
        <FactorChip {...park} />
        <FactorChip {...pitch} />
        <FactorChip label={conf.label} className={conf.className} />
      </div>

      <div className="mt-3 border-t border-slate-100 pt-3 text-xs text-slate-600">
        <div className="font-medium text-slate-900">{candidate.opposingPitcherName ?? "Pitcher TBA"}</div>
        <div>{pitcherMetricLine(candidate)}</div>
        <div>{pitcherSampleLabel(candidate)}</div>
      </div>
    </article>
  );
}

function CandidateRow({ candidate, rank }: { candidate: MlbHomerunCandidate; rank: number }) {
  const conf = confidence(candidate);
  const pitch = pitcherRisk(candidate);
  const park = parkRisk(candidate);
  const lineup = lineupStatus(candidate);
  return (
    <tr className="border-b border-slate-100 align-top hover:bg-slate-50">
      <td className="py-3 pr-3 text-sm font-semibold text-slate-500">{rank}</td>
      <td className="py-3 pr-3">
        <div className="font-medium text-slate-950">{candidate.name}</div>
        <div className="mt-0.5 text-xs text-slate-500">
          {candidate.teamAbbrev}
          {candidate.opponentAbbrev ? ` vs ${candidate.opponentAbbrev}` : ""}
          {" | "}
          {displayPos(candidate.eligiblePositions)}
          {" | "}
          {fmtSalary(candidate.salary)}
          {" | "}
          DK {candidate.dkPlayerId}
        </div>
      </td>
      <td className="py-3 pr-3 text-right">
        <div className="text-base font-semibold text-rose-700">{fmtPct(candidate.hrProb1Plus)}</div>
        <div className="text-xs text-slate-500">Exp {fmtNum(candidate.expectedHr, 3)}</div>
      </td>
      <td className="py-3 pr-3 text-right text-xs text-slate-700">
        <div className="font-semibold text-slate-900">{marketLabel(candidate)}</div>
        <div>{candidate.marketHrBook ?? "No market"}</div>
      </td>
      <td className="py-3 pr-3 text-right">
        <div className={`text-base font-semibold ${edgeClass(candidate.hrEdgePct)}`}>{fmtSignedPct(candidate.hrEdgePct)}</div>
        <div className="text-xs text-slate-500">Model - market</div>
      </td>
      <td className="py-3 pr-3 text-right text-xs text-slate-700">
        <div>{candidate.hitterHrPg != null ? `${candidate.hitterHrPg.toFixed(2)} HR/G` : "-"}</div>
        <div>{candidate.hitterIso != null ? `${candidate.hitterIso.toFixed(3)} ISO` : "-"}</div>
      </td>
      <td className="py-3 pr-3 text-right text-xs text-slate-700">
        <div>{candidate.opposingPitcherName ?? "-"}</div>
        <div>{pitcherMetricLine(candidate)}</div>
        <div className="text-slate-500">{pitcherSampleLabel(candidate)}</div>
      </td>
      <td className="py-3 pr-3 text-right text-xs text-slate-700">
        <div>{candidate.ballpark ?? "-"}</div>
        <div>{weatherLabel(candidate)}</div>
      </td>
      <td className="py-3">
        <div className="flex max-w-56 flex-wrap justify-end gap-1.5">
          <FactorChip {...lineup} />
          <FactorChip {...park} />
          <FactorChip {...pitch} />
          <FactorChip label={conf.label} className={conf.className} />
        </div>
      </td>
    </tr>
  );
}

export default async function HomerunPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string; date?: string | string[]; dkId?: string | string[]; view?: string | string[]; loadError?: string | string[] }>;
}) {
  const params = await searchParams;
  const date = cleanDate(params.date);
  const dkId = cleanDkId(params.dkId);
  const view = cleanView(params.view);
  const loadError = cleanLoadError(params.loadError);
  if (params.sport !== "mlb") {
    const nextParams = new URLSearchParams({ sport: "mlb", view });
    if (dkId != null) nextParams.set("dkId", String(dkId));
    if (date) nextParams.set("date", date);
    redirect(`/homerun?${nextParams.toString()}`);
  }

  const board = await getMlbHomerunBoard({ date, dkId, view });
  const podium = board.candidates.slice(0, 3);
  const csvHref = `data:text/csv;charset=utf-8,${encodeURIComponent(buildCsv(board.candidates))}`;
  const tracking = await getMlbHomerunTrackingReport();

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-4 border-b pb-5 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-rose-700">MLB Homerun Board</p>
          <h1 className="mt-1 text-3xl font-semibold tracking-tight text-slate-950">{VIEW_TITLES[board.view]}</h1>
          <p className="mt-1 text-sm text-slate-500">
            {board.slateDate ? `Slate ${board.slateDate}` : "No MLB slate found"}
            {board.requestedDkId != null ? ` | DK ID ${board.requestedDkId}` : ""}
            {board.dkDraftGroupId != null ? ` | DK draft group ${board.dkDraftGroupId}` : ""}
            {board.contestType ? ` | ${board.contestType}` : ""}
            {board.gameCount != null ? ` | ${board.gameCount} games` : ""}
            {board.totalQualified > 0 ? ` | ${board.totalQualified} qualified hitters` : ""}
            {board.latestMarketCapturedAt ? ` | HR odds ${fmtDateTime(board.latestMarketCapturedAt)}` : ""}
          </p>
        </div>
        <form action={loadHomerunBoardAction} className="flex items-end gap-2">
          <input type="hidden" name="sport" value="mlb" />
          <input type="hidden" name="view" value={board.view} />
          <label className="block">
            <span className="mb-1 block text-xs text-slate-500">DK ID</span>
            <input
              type="number"
              name="dkId"
              min="1"
              inputMode="numeric"
              placeholder={board.dkDraftGroupId != null ? String(board.dkDraftGroupId) : "Contest or draft group"}
              defaultValue={board.requestedDkId ?? ""}
              className="h-9 w-40 rounded border px-2 text-sm"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs text-slate-500">Date</span>
            <input
              type="date"
              name="date"
              defaultValue={board.slateDate ?? board.requestedDate ?? ""}
              className="h-9 rounded border px-2 text-sm"
            />
          </label>
          <button className="h-9 rounded bg-slate-950 px-3 text-sm font-medium text-white hover:bg-slate-800">
            Load
          </button>
        </form>
      </div>

      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div className="inline-flex w-full rounded-md border border-slate-200 bg-white p-1 md:w-auto">
          {(["likely", "edge", "leverage", "longshots"] as const).map((targetView) => (
            <a
              key={targetView}
              href={viewHref(targetView, board.slateDate ?? board.requestedDate, board.requestedDkId)}
              className={`flex-1 rounded px-3 py-1.5 text-center text-sm font-medium md:flex-none ${
                board.view === targetView
                  ? "bg-slate-950 text-white"
                  : "text-slate-600 hover:bg-slate-100 hover:text-slate-950"
              }`}
            >
              {VIEW_LABELS[targetView]}
            </a>
          ))}
        </div>
        {board.candidates.length > 0 && (
          <a
            href={csvHref}
            download={`mlb-homerun-${board.slateDate ?? "slate"}-${board.view}.csv`}
            className="rounded border border-slate-300 px-3 py-1.5 text-center text-sm font-medium text-slate-700 hover:bg-slate-50"
          >
            Export CSV
          </a>
        )}
      </div>

      <StoryPanel board={board} />

      <HomerunScatterPlot board={board} />

      {podium.length > 0 ? (
        <div className="grid gap-3 md:grid-cols-3">
          {podium.map((candidate, index) => (
            <CandidateCard key={candidate.id} candidate={candidate} rank={index + 1} />
          ))}
        </div>
      ) : (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800">
          {loadError ?? board.dkIdError ?? "No MLB hitters with home run probabilities were found for this slate."}
        </div>
      )}

      {board.candidates.length > 0 && (
        <div className="overflow-hidden rounded-lg border bg-card">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[1180px] border-collapse text-sm">
              <thead>
                <tr className="border-b bg-slate-50 text-right text-xs uppercase tracking-wide text-slate-500">
                  <th className="py-2 pl-4 pr-3 text-left">Rank</th>
                  <th className="py-2 pr-3 text-left">Player</th>
                  <th className="py-2 pr-3">HR Chance</th>
                  <th className="py-2 pr-3">Market</th>
                  <th className="py-2 pr-3">Edge</th>
                  <th className="py-2 pr-3">Power</th>
                  <th className="py-2 pr-3">Pitcher</th>
                  <th className="py-2 pr-3">Environment</th>
                  <th className="py-2 pr-4">Factors</th>
                </tr>
              </thead>
              <tbody>
                {board.candidates.map((candidate, index) => (
                  <CandidateRow key={candidate.id} candidate={candidate} rank={index + 1} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <TrackingPanel tracking={tracking} />
    </div>
  );
}
