export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { loadMlbSlateFromDraftGroupId, snapshotMlbHomerunSlateFromStoredRows } from "@/app/dfs/actions";
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
    candidate.opposingPitcherHrPer9 != null || candidate.opposingPitcherXfip != null,
    candidate.teamTotal != null,
    candidate.weatherTemp != null || candidate.windSpeed != null,
  ].filter(Boolean).length;

  if (available >= 6) return { label: "Strong", className: "border-emerald-200 bg-emerald-50 text-emerald-700" };
  if (available >= 4) return { label: "Good", className: "border-sky-200 bg-sky-50 text-sky-700" };
  return { label: "Thin", className: "border-amber-200 bg-amber-50 text-amber-700" };
}

function pitcherRisk(candidate: MlbHomerunCandidate): { label: string; className: string } {
  const hrPer9 = candidate.opposingPitcherHrPer9;
  const xfip = candidate.opposingPitcherXfip;
  if (hrPer9 == null && xfip == null) return { label: "Pitcher ?", className: "border-slate-200 bg-slate-50 text-slate-600" };
  if ((hrPer9 ?? 0) >= 1.35 || (xfip ?? 0) >= 4.7) {
    return { label: "Pitcher HR+", className: "border-rose-200 bg-rose-50 text-rose-700" };
  }
  if ((hrPer9 ?? 0) <= 0.85 && (xfip ?? 99) <= 3.8) {
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
        <div>
          {candidate.opposingPitcherHand ? `${candidate.opposingPitcherHand} | ` : ""}
          {candidate.opposingPitcherHrPer9 != null ? `${candidate.opposingPitcherHrPer9.toFixed(2)} HR/9` : "HR/9 -"}
        </div>
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
  searchParams: Promise<{ sport?: string; date?: string | string[]; dkId?: string | string[]; view?: string | string[] }>;
}) {
  const params = await searchParams;
  const date = cleanDate(params.date);
  const dkId = cleanDkId(params.dkId);
  const view = cleanView(params.view);
  if (params.sport !== "mlb") {
    const nextParams = new URLSearchParams({ sport: "mlb", view });
    if (dkId != null) nextParams.set("dkId", String(dkId));
    if (date) nextParams.set("date", date);
    redirect(`/homerun?${nextParams.toString()}`);
  }

  let board = await getMlbHomerunBoard({ date, dkId, view });
  let loadError: string | null = null;
  if (dkId != null && board.candidates.length === 0 && board.dkDraftGroupId != null && board.dkIdError == null) {
    const loadResult = await loadMlbSlateFromDraftGroupId(board.dkDraftGroupId, undefined, "homerun", undefined, "gpp");
    if (loadResult.ok) {
      board = await getMlbHomerunBoard({ date, dkId, view });
    } else {
      loadError = loadResult.message;
    }
  }
  const podium = board.candidates.slice(0, 3);
  const csvHref = `data:text/csv;charset=utf-8,${encodeURIComponent(buildCsv(board.candidates))}`;
  if (board.slateId != null && board.candidates.length > 0) {
    await snapshotMlbHomerunSlateFromStoredRows(board.slateId);
  }
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
        <form method="get" action="/homerun" className="flex items-end gap-2">
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
