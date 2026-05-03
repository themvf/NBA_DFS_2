"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type {
  VegasMatchupRow,
  OuHitRateRow,
  TeamTotalAccuracyRow,
  SpreadCoverageRow,
  MlbVegasCoverageStatus,
  VegasSummaryStatsRow,
  BiggestMissRow,
  TeamVegasInsightRow,
  MoneylineBacktestReport,
} from "@/db/queries";
import { fetchVegasOdds } from "./actions";
import type { Sport } from "@/db/queries";

const fmt1 = (v: number | null | undefined) =>
  v == null ? "—" : v.toFixed(1);
const fmtPct = (v: number | null | undefined) =>
  v == null ? "—" : `${(v * 100).toFixed(1)}%`;
const fmtMl = (ml: number | null) => {
  if (ml == null) return "—";
  return ml > 0 ? `+${ml}` : String(ml);
};
const fmtSignedMoney = (v: number | null | undefined) => {
  if (v == null) return "—";
  const rounded = Math.round(v);
  return `${rounded >= 0 ? "+" : "-"}$${Math.abs(rounded).toLocaleString()}`;
};
const fmtSignedPct = (v: number | null | undefined) => {
  if (v == null) return "—";
  return `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`;
};

// ── Betting intelligence helpers ────────────────────────────────────────────

const fmtDate = (value: string | null | undefined) => value ?? "None";

function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}

/**
 * Bayesian shrinkage of a rate toward a prior.
 *   adjusted = (k + α × prior) / (n + α)
 * where k = rate × n. `alpha` is the effective prior sample size.
 * A rate of 5/6 (83%) with α=20, prior=0.5 → (5 + 10) / (6 + 20) = 0.577 — pulls
 * a small-sample fluke back toward the mean.
 */
function shrinkRate(
  rate: number | null | undefined,
  n: number | null | undefined,
  prior: number,
  alpha: number
): number | null {
  if (rate == null) return null;
  const N = n ?? 0;
  if (N <= 0) return prior;
  return (rate * N + alpha * prior) / (N + alpha);
}

/** MLB league-average xFIP (2025) — used to center the SP-quality factor. */
const MLB_LEAGUE_AVG_XFIP = 4.2;

/** Per-factor contribution to a blended probability score. */
export type ScoreSignal = { label: string; value: number; weight: number };

/**
 * Blend a list of weighted signals into a single probability.
 * Renormalizes weights so missing signals don't bias the blend.
 */
function blendSignals(signals: ScoreSignal[]): number | null {
  if (signals.length === 0) return null;
  const totalW = signals.reduce((s, r) => s + r.weight, 0);
  if (totalW <= 0) return null;
  return signals.reduce((s, r) => s + r.value * r.weight, 0) / totalW;
}

/** Map a game total to the historical O/U tier key (must match DB CASE labels). */
function getOuTierKey(total: number | null, sport: Sport): string | null {
  if (total == null) return null;
  if (sport === "mlb") {
    if (total < 7.5) return "Under 7.5";
    if (total < 8.0) return "7.5";
    if (total < 8.5) return "8.0";
    if (total < 9.0) return "8.5";
    if (total < 9.5) return "9.0";
    if (total < 10.0) return "9.5";
    if (total < 10.5) return "10.0";
    return "10.5+";
  }
  // NBA
  if (total < 215) return "Under 215";
  if (total < 220) return "215\u2013220";
  if (total < 225) return "220\u2013225";
  if (total < 230) return "225\u2013230";
  if (total < 235) return "230\u2013235";
  if (total < 240) return "235\u2013240";
  return "240+";
}

/** Map an ABS(home_spread) to the historical spread/run-line tier key. */
function getSpreadTierKey(spread: number | null, sport: Sport): string | null {
  if (spread == null) return null;
  const abs = Math.abs(spread);
  if (sport === "mlb") {
    if (abs < 1.0) return "Pick";
    if (abs < 2.0) return "\u00b11.5 (Run Line)";
    return "2.0+";
  }
  // NBA
  if (abs <= 1.5) return "Pick / \u00b11.5";
  if (abs <= 3.5) return "2\u20133.5";
  if (abs <= 6.5) return "4\u20136.5";
  if (abs <= 9.5) return "7\u20139.5";
  if (abs <= 13.5) return "10\u201313.5";
  return "14+";
}

/**
 * O/U score = probability OVER hits. Blended from:
 *   MLB only:
 *     25% SP-quality factor (avg SP xFIP vs league) — lower xFIP → UNDER lean
 *     30% historical tier over-rate (shrunk toward 0.5, α=50)
 *     15% home team game over-rate (shrunk, α=20)
 *     15% away team game over-rate (shrunk, α=20)
 *   NBA (no SP factor):
 *     40% tier / 30% home / 30% away (shrunk same way)
 * Weights renormalize when a signal is missing. Returns null when no signal exists.
 */
function computeOuScore(
  m: VegasMatchupRow,
  ouHitRate: OuHitRateRow[],
  teamInsights: TeamVegasInsightRow[],
  sport: Sport,
): { score: number; signals: ScoreSignal[] } | null {
  const tierRow = ouHitRate.find((r) => r.totalTier === getOuTierKey(m.vegasTotal, sport));
  const home = teamInsights.find((t) => t.teamAbbrev === m.homeAbbrev);
  const away = teamInsights.find((t) => t.teamAbbrev === m.awayAbbrev);

  const signals: ScoreSignal[] = [];

  // SP-quality factor — MLB only, when both SPs are known
  if (sport === "mlb" && m.homeSpXfip != null && m.awaySpXfip != null) {
    const avgXfip = (m.homeSpXfip + m.awaySpXfip) / 2;
    // Map xFIP vs league average to a probability centered at 0.5.
    // xFIP 1 run above average → 0.65 (over lean); 1 run below → 0.35 (under lean).
    // Slope calibrated so a 0.5-run gap shifts score by ~7.5 pp.
    const xfipEdge = (avgXfip - MLB_LEAGUE_AVG_XFIP) / MLB_LEAGUE_AVG_XFIP;
    const spValue = clamp(0.5 + xfipEdge * 1.5, 0.3, 0.7);
    signals.push({ label: "SP quality", value: spValue, weight: 0.25 });
  }

  const tierShrunk = shrinkRate(tierRow?.overRate, tierRow?.n, 0.5, 50);
  if (tierShrunk != null) {
    signals.push({ label: "Total tier", value: tierShrunk, weight: sport === "mlb" ? 0.30 : 0.40 });
  }

  const homeShrunk = shrinkRate(home?.gameOverRate, home?.n, 0.5, 20);
  if (homeShrunk != null) {
    signals.push({ label: `${m.homeAbbrev} history`, value: homeShrunk, weight: sport === "mlb" ? 0.15 : 0.30 });
  }

  const awayShrunk = shrinkRate(away?.gameOverRate, away?.n, 0.5, 20);
  if (awayShrunk != null) {
    signals.push({ label: `${m.awayAbbrev} history`, value: awayShrunk, weight: sport === "mlb" ? 0.15 : 0.30 });
  }

  const score = blendSignals(signals);
  return score != null ? { score, signals } : null;
}

/**
 * Spread score = probability the HOME team covers.
 *   40% historical tier cover rate (flipped if home is the dog; shrunk, α=50)
 *   35% home team ATS cover rate (shrunk, α=20)
 *   25% (1 − away team ATS cover rate) (shrunk, α=20)
 * Note: for MLB run lines (±1.5) ATS cover rate is noisy — the shrinkage pulls
 * small-sample team rates toward 0.5 so they don't swing the score.
 */
function computeSpreadScore(
  m: VegasMatchupRow,
  spreadCoverage: SpreadCoverageRow[],
  teamInsights: TeamVegasInsightRow[],
  sport: Sport,
): { score: number; signals: ScoreSignal[] } | null {
  const tierRow = spreadCoverage.find((r) => r.spreadTier === getSpreadTierKey(m.homeSpread, sport));
  const home = teamInsights.find((t) => t.teamAbbrev === m.homeAbbrev);
  const away = teamInsights.find((t) => t.teamAbbrev === m.awayAbbrev);

  const signals: ScoreSignal[] = [];

  // tierCoverRate = "favorite covers"; flip if home is the dog; shrink toward 0.5
  const tierShrunk = shrinkRate(tierRow?.coverRate, tierRow?.n, 0.5, 50);
  if (tierShrunk != null && m.homeSpread != null) {
    const baseCoverRate =
      m.homeSpread < 0
        ? tierShrunk
        : Math.abs(m.homeSpread) < 0.5
        ? 0.5
        : 1 - tierShrunk;
    signals.push({ label: "Tier cover rate", value: baseCoverRate, weight: 0.40 });
  }

  const homeShrunk = shrinkRate(home?.atsCoverRate, home?.atsN, 0.5, 20);
  if (homeShrunk != null) {
    signals.push({ label: `${m.homeAbbrev} ATS`, value: homeShrunk, weight: 0.35 });
  }

  const awayShrunk = shrinkRate(away?.atsCoverRate, away?.atsN, 0.5, 20);
  if (awayShrunk != null) {
    signals.push({ label: `${m.awayAbbrev} ATS (inverted)`, value: 1 - awayShrunk, weight: 0.25 });
  }

  const score = blendSignals(signals);
  return score != null ? { score, signals } : null;
}

/**
 * ML score = Vegas home-win probability adjusted by bounded team-total mean reversion.
 * Previously added a redundant `ovrAdj` that was strongly correlated with `biasAdj`
 * (both derive from "actual vs implied"), double-counting the same signal — dropped.
 *
 * The bias divisor is sport-aware: NBA team-total bias is ±4–5 pts (÷30 → meaningful),
 * MLB bias is ±0.3 runs (÷3 → meaningful). Max shift capped at ±5%.
 */
function computeMlScore(
  m: VegasMatchupRow,
  teamInsights: TeamVegasInsightRow[],
  sport: Sport,
): { score: number; signals: ScoreSignal[] } | null {
  if (m.homeWinProb == null) return null;
  const home = teamInsights.find((t) => t.teamAbbrev === m.homeAbbrev);
  const away = teamInsights.find((t) => t.teamAbbrev === m.awayAbbrev);

  const biasDivisor = sport === "mlb" ? 3 : 30;
  const homeBias = home?.bias ?? 0;
  const awayBias = away?.bias ?? 0;
  const biasAdj = clamp((homeBias - awayBias) / biasDivisor, -0.05, 0.05);

  const score = clamp(m.homeWinProb + biasAdj, 0.05, 0.95);
  const signals: ScoreSignal[] = [
    { label: "Vegas home win%", value: m.homeWinProb, weight: 1 },
    { label: `Net bias adj (÷${biasDivisor})`, value: biasAdj, weight: 0 },
  ];
  return { score, signals };
}

/** Color-coded badge showing a probability score, with per-signal breakdown on hover. */
function ScoreBadge({
  score,
  label,
  signals,
}: {
  score: number | null;
  label?: string;
  signals?: ScoreSignal[];
}) {
  if (score == null) return <span className="text-gray-300 text-xs">—</span>;
  const pct = score * 100;
  const cls =
    pct > 57
      ? "bg-green-100 text-green-800 border-green-200"
      : pct > 52
      ? "bg-green-50 text-green-700 border-green-100"
      : pct < 43
      ? "bg-red-100 text-red-800 border-red-200"
      : pct < 48
      ? "bg-orange-50 text-orange-700 border-orange-100"
      : "bg-gray-50 text-gray-500 border-gray-200";
  const tooltip = signals && signals.length
    ? signals
        .map((s) => {
          const pctStr = (s.value * 100).toFixed(0) + "%";
          const wStr = s.weight > 0 ? ` · w=${(s.weight * 100).toFixed(0)}%` : "";
          return `${s.label}: ${pctStr}${wStr}`;
        })
        .join("\n")
    : undefined;
  return (
    <span
      className={`inline-flex cursor-help items-center gap-0.5 rounded border px-1.5 py-0.5 text-xs font-medium ${cls}`}
      title={tooltip}
    >
      {label && <span className="opacity-75">{label}&nbsp;</span>}
      {pct.toFixed(0)}%
    </span>
  );
}

// ── end helpers ─────────────────────────────────────────────────────────────

function BiasChip({ bias }: { bias: number | null }) {
  if (bias == null) return <span className="text-gray-400">—</span>;
  const pos = bias > 0;
  return (
    <span className={pos ? "text-red-600" : "text-blue-600"}>
      {pos ? "+" : ""}
      {bias.toFixed(1)}
    </span>
  );
}

type Props = {
  matchups: VegasMatchupRow[];
  ouHitRate: OuHitRateRow[];
  teamTotalAccuracy: TeamTotalAccuracyRow[];
  spreadCoverage: SpreadCoverageRow[];
  mlbCoverageStatus: MlbVegasCoverageStatus | null;
  vegasSummary: VegasSummaryStatsRow | null;
  biggestMisses: BiggestMissRow[];
  teamInsights: TeamVegasInsightRow[];
  moneylineBacktest: MoneylineBacktestReport;
  queryDate: string;
  sport: Sport;
};

export default function VegasClient({
  matchups,
  ouHitRate,
  teamTotalAccuracy,
  spreadCoverage,
  mlbCoverageStatus,
  vegasSummary,
  biggestMisses,
  teamInsights,
  moneylineBacktest,
  queryDate,
  sport,
}: Props) {
  const router = useRouter();
  const [dateInput, setDateInput] = useState(queryDate);
  const [isPending, startTransition] = useTransition();
  const [fetchMsg, setFetchMsg] = useState<{ ok: boolean; text: string } | null>(null);

  const handleFetchLines = () => {
    setFetchMsg(null);
    startTransition(async () => {
      const result = await fetchVegasOdds(queryDate, sport);
      setFetchMsg({ ok: result.ok, text: result.message });
      if (result.ok) router.refresh();
    });
  };

  const hasScores = ouHitRate.length > 0 || teamTotalAccuracy.length > 0;

  const handleDateChange = (d: string) => {
    setDateInput(d);
    router.push(`/vegas?date=${d}`);
  };

  // Compute overall O/U stats
  const totalN = ouHitRate.reduce((s, r) => s + r.n, 0);
  const totalOvers = ouHitRate.reduce((s, r) => s + r.overCount, 0);
  const overallOverRate = totalN > 0 ? totalOvers / totalN : null;
  const missingScoreDates = mlbCoverageStatus?.missingScoreDates ?? [];
  const missingOddsDates = mlbCoverageStatus?.missingOddsDates ?? [];
  const unattemptedOddsDates = mlbCoverageStatus?.unattemptedMissingOddsDates ?? [];
  const providerPartialOddsDates = mlbCoverageStatus?.providerPartialOddsDates ?? [];
  const hasActionableBackfill = sport === "mlb" && mlbCoverageStatus?.recommendedBackfillStart != null;
  const hasProviderPartialOdds = providerPartialOddsDates.length > 0;
  const coverageLooksComplete = sport === "mlb"
    && mlbCoverageStatus?.historicalEndDate != null
    && !hasActionableBackfill
    && !hasProviderPartialOdds;

  return (
    <div className="space-y-8 p-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="flex flex-wrap items-end gap-4">
        <div>
          <h1 className="text-xl font-bold">Vegas Analysis — {sport.toUpperCase()}</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {sport === "mlb"
              ? "Matchup lines and historical O/U + run line calibration"
              : "Matchup lines and historical O/U + spread calibration"}
          </p>
        </div>
        <div className="flex items-center gap-2 ml-auto">
          <label className="text-xs text-gray-500">Date</label>
          <input
            type="date"
            value={dateInput}
            onChange={(e) => handleDateChange(e.target.value)}
            className="rounded border px-2 py-1 text-sm"
          />
          <button
            onClick={handleFetchLines}
            disabled={isPending}
            className="rounded border px-3 py-1 text-sm bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isPending ? "Fetching…" : matchups.length === 0 ? "Fetch Lines" : "Refresh Lines"}
          </button>
        </div>
      </div>

      {/* ── Fetch feedback ───────────────────────────────────── */}
      {sport === "mlb" && mlbCoverageStatus && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div className="flex flex-wrap items-baseline gap-3">
            <h2 className="font-semibold">MLB Backfill Coverage</h2>
            <span className={`text-xs ${coverageLooksComplete ? "text-emerald-700" : "text-amber-700"}`}>
              {hasActionableBackfill
                ? `Backfill is needed from ${fmtDate(mlbCoverageStatus.recommendedBackfillStart)} through ${fmtDate(mlbCoverageStatus.recommendedBackfillEnd)}.`
                : hasProviderPartialOdds
                  ? `Backfill is current through ${fmtDate(mlbCoverageStatus.historicalEndDate)}; some odds markets remain partial from provider coverage.`
                  : `Historical MLB dates are complete through ${fmtDate(mlbCoverageStatus.historicalEndDate)}.`}
            </span>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded border border-slate-200 bg-slate-50 p-3">
              <div className="text-[11px] uppercase tracking-wide text-slate-500">Schedule In DB</div>
              <div className="mt-1 text-sm font-medium text-slate-900">
                {fmtDate(mlbCoverageStatus.availableStartDate)} to {fmtDate(mlbCoverageStatus.availableEndDate)}
              </div>
              <div className="mt-1 text-xs text-slate-600">
                {mlbCoverageStatus.dateCount} dates | {mlbCoverageStatus.gameCount} games
              </div>
            </div>
            <div className="rounded border border-slate-200 bg-slate-50 p-3">
              <div className="text-[11px] uppercase tracking-wide text-slate-500">Scores</div>
              <div className="mt-1 text-sm font-medium text-slate-900">
                Complete through {fmtDate(mlbCoverageStatus.latestScoreCompleteDate)}
              </div>
              <div className="mt-1 text-xs text-slate-600">
                {mlbCoverageStatus.firstMissingScoreDate
                  ? `First missing score date: ${fmtDate(mlbCoverageStatus.firstMissingScoreDate)}`
                  : "No missing score dates through yesterday"}
              </div>
            </div>
            <div className="rounded border border-slate-200 bg-slate-50 p-3">
              <div className="text-[11px] uppercase tracking-wide text-slate-500">Full Odds</div>
              <div className="mt-1 text-sm font-medium text-slate-900">
                Complete through {fmtDate(mlbCoverageStatus.latestOddsCompleteDate)}
              </div>
              <div className="mt-1 text-xs text-slate-600">
                Attempted through {fmtDate(mlbCoverageStatus.oddsBackfillAttemptedThroughDate)}
              </div>
            </div>
          </div>

          <div className="rounded border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="font-medium text-slate-900">
              {hasActionableBackfill
                ? `Suggested workflow window: ${fmtDate(mlbCoverageStatus.recommendedBackfillStart)} to ${fmtDate(mlbCoverageStatus.recommendedBackfillEnd)}`
                : `Scheduled backfill is caught up through ${fmtDate(mlbCoverageStatus.historicalEndDate)}`}
            </div>
            <div className="mt-1">
              Scores lane: {hasActionableBackfill && mlbCoverageStatus.firstMissingScoreDate
                ? `${fmtDate(mlbCoverageStatus.firstMissingScoreDate)} to ${fmtDate(mlbCoverageStatus.recommendedBackfillEnd)}`
                : "caught up"}
              {" | "}
              Odds lane: {hasActionableBackfill && mlbCoverageStatus.firstUnattemptedOddsDate
                ? `${fmtDate(mlbCoverageStatus.firstUnattemptedOddsDate)} to ${fmtDate(mlbCoverageStatus.recommendedBackfillEnd)}`
                : "no unattempted dates"}
            </div>
            <div className="mt-1">
              Yesterday check ({fmtDate(mlbCoverageStatus.yesterdayDate)}):{" "}
              {mlbCoverageStatus.yesterdayHadGames
                ? `scores ${mlbCoverageStatus.yesterdayScoresComplete ? "complete" : "missing"}, odds ${mlbCoverageStatus.yesterdayOddsComplete ? "complete" : "missing"}`
                : "no MLB games"}
            </div>
          </div>

          {(missingScoreDates.length > 0 || unattemptedOddsDates.length > 0 || providerPartialOddsDates.length > 0 || missingOddsDates.length > 0) && (
            <div className="grid gap-3 md:grid-cols-3">
              <div className="rounded border border-slate-200 bg-white p-3">
                <div className="text-[11px] uppercase tracking-wide text-slate-500">Missing Score Dates</div>
                <div className="mt-1 text-xs text-slate-700">
                  {missingScoreDates.length > 0 ? missingScoreDates.join(", ") : "None through yesterday"}
                </div>
              </div>
              <div className="rounded border border-slate-200 bg-white p-3">
                <div className="text-[11px] uppercase tracking-wide text-slate-500">Unattempted Odds Dates</div>
                <div className="mt-1 text-xs text-slate-700">
                  {unattemptedOddsDates.length > 0 ? unattemptedOddsDates.join(", ") : "None through yesterday"}
                </div>
              </div>
              <div className="rounded border border-slate-200 bg-white p-3">
                <div className="text-[11px] uppercase tracking-wide text-slate-500">Provider Partial Odds Dates</div>
                <div className="mt-1 text-xs text-slate-700">
                  {providerPartialOddsDates.length > 0 ? providerPartialOddsDates.join(", ") : "None through yesterday"}
                </div>
                {providerPartialOddsDates.length > 0 && (
                  <div className="mt-2 text-[11px] leading-snug text-slate-500">
                    Backfill ran for these dates, but the provider returned incomplete total or moneyline markets.
                  </div>
                )}
                {missingOddsDates.length > providerPartialOddsDates.length + unattemptedOddsDates.length && (
                  <div className="mt-2 text-[11px] leading-snug text-slate-500">
                    Additional partial odds dates are hidden by the display limit.
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {fetchMsg && (
        <div
          className={`rounded border px-3 py-2 text-sm ${
            fetchMsg.ok
              ? "border-green-200 bg-green-50 text-green-800"
              : "border-red-200 bg-red-50 text-red-800"
          }`}
        >
          {fetchMsg.text}
        </div>
      )}

      {/* ── Vegas MAE Summary ────────────────────────────────── */}
      {vegasSummary != null && vegasSummary.n > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3">
            Season Vegas Accuracy — {sport.toUpperCase()}
            <span className="ml-2 text-xs font-normal text-gray-400">
              {vegasSummary.n} games with lines + scores
            </span>
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Game Total MAE</div>
              <div className="text-xl font-bold">
                {vegasSummary.gameTotalMae != null ? vegasSummary.gameTotalMae.toFixed(2) : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.gameTotalBias != null
                  ? `bias ${vegasSummary.gameTotalBias > 0 ? "+" : ""}${vegasSummary.gameTotalBias.toFixed(2)}`
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Team Total MAE</div>
              <div className="text-xl font-bold">
                {vegasSummary.teamTotalMae != null ? vegasSummary.teamTotalMae.toFixed(2) : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.teamTotalBias != null
                  ? `bias ${vegasSummary.teamTotalBias > 0 ? "+" : ""}${vegasSummary.teamTotalBias.toFixed(2)}`
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Over Rate</div>
              <div className={`text-xl font-bold ${vegasSummary.ouOverRate != null && vegasSummary.ouOverRate > 0.52 ? "text-green-700" : vegasSummary.ouOverRate != null && vegasSummary.ouOverRate < 0.48 ? "text-blue-600" : ""}`}>
                {vegasSummary.ouOverRate != null ? `${(vegasSummary.ouOverRate * 100).toFixed(1)}%` : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.ouOverRate != null
                  ? vegasSummary.ouOverRate > 0.52 ? "overs dominate" : vegasSummary.ouOverRate < 0.48 ? "unders dominate" : "balanced"
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Game Bias</div>
              <div className={`text-xl font-bold ${vegasSummary.gameTotalBias != null && vegasSummary.gameTotalBias > 0 ? "text-red-600" : "text-blue-600"}`}>
                {vegasSummary.gameTotalBias != null
                  ? `${vegasSummary.gameTotalBias > 0 ? "+" : ""}${vegasSummary.gameTotalBias.toFixed(2)}`
                  : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.gameTotalBias != null
                  ? vegasSummary.gameTotalBias > 0 ? "actuals beat lines" : "lines beat actuals"
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Games Tracked</div>
              <div className="text-xl font-bold">{vegasSummary.n}</div>
              <div className="text-xs text-gray-400 mt-0.5">w/ lines + scores</div>
            </div>
          </div>
        </div>
      )}

      {/* ── Today's Matchups ─────────────────────────────────── */}
      {moneylineBacktest.completedGames > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div className="flex flex-wrap items-baseline gap-3">
            <h2 className="font-semibold">Moneyline Backtest</h2>
            <span className="text-xs text-gray-500">
              {moneylineBacktest.completedGames} games through {fmtDate(moneylineBacktest.completedThrough)}
            </span>
            {moneylineBacktest.pendingOddsNoScore > 0 && (
              <span className="text-xs text-amber-700">
                {moneylineBacktest.pendingOddsNoScore} odds {moneylineBacktest.pendingOddsNoScore === 1 ? "game" : "games"} still missing scores
                {moneylineBacktest.pendingOddsNoScoreStart && moneylineBacktest.pendingOddsNoScoreEnd
                  ? ` (${moneylineBacktest.pendingOddsNoScoreStart} to ${moneylineBacktest.pendingOddsNoScoreEnd})`
                  : ""}
              </span>
            )}
          </div>
          <p className="text-xs text-gray-500">
            Walk-forward results use only prior team scoring bias at each game. Profit assumes $100 risk per bet at the stored consensus moneyline.
            Value edge compares our ML probability to the raw breakeven price.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Window</th>
                  <th className="py-1 text-left">Strategy</th>
                  <th className="py-1">Bets</th>
                  <th className="py-1">W-L</th>
                  <th className="py-1">Win%</th>
                  <th className="py-1">Profit</th>
                  <th className="py-1">ROI</th>
                  <th className="py-1">Avg Edge</th>
                  <th className="py-1">Fav/Dog</th>
                </tr>
              </thead>
              <tbody>
                {moneylineBacktest.windows.flatMap((window) =>
                  window.rows.map((row, index) => {
                    const roiClass = row.roi == null
                      ? ""
                      : row.roi > 0
                      ? "text-green-700 font-semibold"
                      : row.roi < 0
                      ? "text-red-600 font-semibold"
                      : "";
                    const edgeClass = row.avgEdge == null
                      ? ""
                      : row.avgEdge > 0
                      ? "text-green-700"
                      : row.avgEdge < 0
                      ? "text-red-600"
                      : "";
                    return (
                      <tr key={`${window.key}-${row.strategy}`} className="border-b border-gray-50">
                        <td className="py-1.5 text-left font-medium">
                          {index === 0 ? window.label : ""}
                        </td>
                        <td className="py-1.5 text-left">{row.label}</td>
                        <td className="py-1.5 text-right">{row.n}</td>
                        <td className="py-1.5 text-right">{row.wins}-{row.losses}</td>
                        <td className="py-1.5 text-right">{fmtPct(row.winRate)}</td>
                        <td className={`py-1.5 text-right ${roiClass}`}>{fmtSignedMoney(row.profit)}</td>
                        <td className={`py-1.5 text-right ${roiClass}`}>{fmtSignedPct(row.roi)}</td>
                        <td className={`py-1.5 text-right ${edgeClass}`}>{fmtSignedPct(row.avgEdge)}</td>
                        <td className="py-1.5 text-right text-gray-500">
                          {row.favorites}/{row.underdogs}
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="rounded-lg border bg-card p-4 text-sm">
        <h2 className="font-semibold mb-3">
          Matchups — {queryDate}
          {matchups.length === 0 && (
            <span className="ml-2 text-xs font-normal text-gray-400">
              No games found for this date
            </span>
          )}
        </h2>
        {matchups.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Matchup</th>
                  <th className="py-1 text-right">Total</th>
                  <th className="py-1 text-right">Spread</th>
                  <th className="py-1 text-right">Home ML</th>
                  <th className="py-1 text-right">Away ML</th>
                  <th className="py-1 text-right">Home Imp</th>
                  <th className="py-1 text-right">Away Imp</th>
                  <th className="py-1 text-right">Home Win%</th>
                  <th className="py-1 text-right">Score</th>
                  <th className="py-1 text-right">O/U</th>
                </tr>
              </thead>
              <tbody>
                {matchups.map((m) => {
                  const actual = m.homeScore != null && m.awayScore != null
                    ? m.homeScore + m.awayScore
                    : null;
                  const ouResult = actual != null && m.vegasTotal != null
                    ? actual > m.vegasTotal ? "O" : actual < m.vegasTotal ? "U" : "P"
                    : null;
                  const ouColor = ouResult === "O" ? "text-green-700 font-semibold"
                    : ouResult === "U" ? "text-red-600 font-semibold"
                    : "";
                  const homeSpreadStr = m.homeSpread == null ? "—"
                    : m.homeSpread > 0 ? `+${m.homeSpread}` : String(m.homeSpread);
                  return (
                    <tr key={m.matchupId} className="border-b border-gray-50">
                      <td className="py-1.5 font-medium">
                        {m.awayAbbrev} @ {m.homeAbbrev}
                        {sport === "mlb" && (m.awaySpName || m.homeSpName) && (
                          <div className="text-[10px] font-normal text-gray-500 mt-0.5">
                            {m.awaySpName
                              ? `${m.awaySpName}${m.awaySpHand ? ` (${m.awaySpHand})` : ""}${
                                  m.awaySpXfip != null ? ` · xFIP ${m.awaySpXfip.toFixed(2)}` : ""
                                }`
                              : "—"}
                            {" vs "}
                            {m.homeSpName
                              ? `${m.homeSpName}${m.homeSpHand ? ` (${m.homeSpHand})` : ""}${
                                  m.homeSpXfip != null ? ` · xFIP ${m.homeSpXfip.toFixed(2)}` : ""
                                }`
                              : "—"}
                          </div>
                        )}
                      </td>
                      <td className="py-1.5 text-right">{fmt1(m.vegasTotal)}</td>
                      <td className="py-1.5 text-right">{homeSpreadStr}</td>
                      <td className="py-1.5 text-right">{fmtMl(m.homeMl)}</td>
                      <td className="py-1.5 text-right">{fmtMl(m.awayMl)}</td>
                      <td className="py-1.5 text-right text-blue-700">{fmt1(m.homeImplied)}</td>
                      <td className="py-1.5 text-right text-blue-700">{fmt1(m.awayImplied)}</td>
                      <td className="py-1.5 text-right">
                        {m.homeWinProb != null ? `${(m.homeWinProb * 100).toFixed(0)}%` : "—"}
                      </td>
                      <td className="py-1.5 text-right">
                        {m.homeScore != null && m.awayScore != null
                          ? `${m.awayScore}–${m.homeScore}`
                          : "—"}
                      </td>
                      <td className={`py-1.5 text-right ${ouColor}`}>
                        {ouResult ?? "—"}
                        {actual != null && m.vegasTotal != null && (
                          <span className="ml-1 text-gray-400 font-normal">
                            ({actual > m.vegasTotal ? "+" : ""}
                            {(actual - m.vegasTotal).toFixed(1)})
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* ── Betting Intelligence ─────────────────────────────── */}
      {matchups.length > 0 && hasScores && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div>
            <h2 className="font-semibold">Betting Intelligence</h2>
            <p className="text-xs text-gray-500 mt-0.5">
              Scores derived from historical O/U tiers,{" "}
              {sport === "mlb" ? "run line" : "spread"} tiers, team implied accuracy, and ATS cover rates.
              O/U = lean over probability.{" "}
              {sport === "mlb" ? "Run line" : "Spread"} = home-covers probability.
              ML = adjusted win probability; price edge is tracked in the moneyline backtest.
              Scores near 50% are neutral; above 57% or below 43% suggest a meaningful lean.
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Matchup</th>
                  <th className="py-1 text-right">Total</th>
                  <th className="py-1 text-right">O/U Score</th>
                  <th className="py-1 text-right">{sport === "mlb" ? "Run Line" : "Spread"}</th>
                  <th className="py-1 text-right">{sport === "mlb" ? "RL Score" : "Spread Score"}</th>
                  <th className="py-1 text-right">ML Score</th>
                </tr>
              </thead>
              <tbody>
                {matchups.map((m) => {
                  const ou = computeOuScore(m, ouHitRate, teamInsights, sport);
                  const spread = computeSpreadScore(m, spreadCoverage, teamInsights, sport);
                  const ml = computeMlScore(m, teamInsights, sport);

                  const ouScore = ou?.score ?? null;
                  const spreadScore = spread?.score ?? null;
                  const mlScore = ml?.score ?? null;

                  const ouLabel =
                    ouScore != null ? (ouScore >= 0.5 ? "O" : "U") : undefined;
                  const spreadLabel =
                    spreadScore != null
                      ? spreadScore >= 0.5
                        ? m.homeAbbrev
                        : m.awayAbbrev
                      : undefined;
                  const mlLabel =
                    mlScore != null
                      ? mlScore >= 0.5
                        ? m.homeAbbrev
                        : m.awayAbbrev
                      : undefined;

                  // Flip display for away lean (show away-covers / away-win %)
                  const spreadDisplay =
                    spreadScore != null && spreadScore < 0.5 ? 1 - spreadScore : spreadScore;
                  const mlDisplay =
                    mlScore != null && mlScore < 0.5 ? 1 - mlScore : mlScore;

                  const homeSpreadStr =
                    m.homeSpread == null
                      ? "—"
                      : m.homeSpread > 0
                      ? `+${m.homeSpread}`
                      : String(m.homeSpread);

                  return (
                    <tr key={m.matchupId} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="py-1.5 font-medium">
                        {m.awayAbbrev} @ {m.homeAbbrev}
                        {sport === "mlb" && (m.awaySpName || m.homeSpName) && (
                          <div className="text-[10px] font-normal text-gray-500 mt-0.5">
                            {m.awaySpName
                              ? `${m.awaySpName}${m.awaySpHand ? ` (${m.awaySpHand})` : ""}${
                                  m.awaySpXfip != null ? ` · xFIP ${m.awaySpXfip.toFixed(2)}` : ""
                                }`
                              : "—"}
                            {" vs "}
                            {m.homeSpName
                              ? `${m.homeSpName}${m.homeSpHand ? ` (${m.homeSpHand})` : ""}${
                                  m.homeSpXfip != null ? ` · xFIP ${m.homeSpXfip.toFixed(2)}` : ""
                                }`
                              : "—"}
                          </div>
                        )}
                      </td>
                      <td className="py-1.5 text-right text-gray-500">{fmt1(m.vegasTotal)}</td>
                      <td className="py-1.5 text-right">
                        <ScoreBadge score={ouScore} label={ouLabel} signals={ou?.signals} />
                      </td>
                      <td className="py-1.5 text-right text-gray-500">{homeSpreadStr}</td>
                      <td className="py-1.5 text-right">
                        <ScoreBadge
                          score={spreadDisplay}
                          label={spreadLabel}
                          signals={spread?.signals}
                        />
                      </td>
                      <td className="py-1.5 text-right">
                        <ScoreBadge score={mlDisplay} label={mlLabel} signals={ml?.signals} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="text-xs text-gray-400">
            Scores are statistical summaries of historical patterns — not betting recommendations.
            Sample sizes vary; interpret with caution on low-game-count tiers.
          </p>
        </div>
      )}

      {!hasScores && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800">
          <strong>No historical score data yet.</strong>{" "}
          {sport === "mlb" ? (
            <>
              Run{" "}
              <code className="font-mono bg-amber-100 px-1 rounded">
                python -m ingest.backfill_mlb_schedule --start YYYY-MM-DD --end YYYY-MM-DD
              </code>{" "}
              once to populate historical MLB schedule and scores, then keep it current with{" "}
              <code className="font-mono bg-amber-100 px-1 rounded">
                python -m ingest.mlb_schedule --date YYYY-MM-DD
              </code>{" "}
              or the daily GitHub Action workflow{" "}
              <code className="font-mono bg-amber-100 px-1 rounded">
                Backfill MLB History
              </code>
              .
            </>
          ) : (
            <>
              Run{" "}
              <code className="font-mono bg-amber-100 px-1 rounded">
                python -m ingest.backfill_scores
              </code>{" "}
              once to populate final scores, then re-run{" "}
              <code className="font-mono bg-amber-100 px-1 rounded">
                python -m ingest.nba_schedule
              </code>{" "}
              after each game day to keep scores current.
            </>
          )}
        </div>
      )}

      {/* ── O/U Hit Rate ──────────────────────────────────────── */}
      {ouHitRate.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div className="flex flex-wrap items-baseline gap-4">
            <h2 className="font-semibold">Over/Under Hit Rate by Total</h2>
            {overallOverRate != null && (
              <span className="text-xs text-gray-500">
                Overall: {fmtPct(overallOverRate)} over ({totalOvers}/{totalN} games)
              </span>
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Total Tier</th>
                  <th className="py-1">Games</th>
                  <th className="py-1">Overs</th>
                  <th className="py-1">Unders</th>
                  <th className="py-1">Pushes</th>
                  <th className="py-1">Over%</th>
                  <th className="py-1">Avg Line</th>
                  <th className="py-1">Avg Actual</th>
                  <th className="py-1">Avg Error</th>
                </tr>
              </thead>
              <tbody>
                {ouHitRate.map((row) => {
                  const avgError = row.avgActual != null && row.avgTotal != null
                    ? row.avgActual - row.avgTotal
                    : null;
                  const overColor = row.overRate == null ? ""
                    : row.overRate > 0.55 ? "text-green-700 font-semibold"
                    : row.overRate < 0.45 ? "text-red-600 font-semibold"
                    : "";
                  return (
                    <tr key={row.totalTier} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.totalTier}</td>
                      <td className="py-1 text-right">{row.n}</td>
                      <td className="py-1 text-right text-green-700">{row.overCount}</td>
                      <td className="py-1 text-right text-red-600">{row.underCount}</td>
                      <td className="py-1 text-right text-gray-400">{row.pushCount}</td>
                      <td className={`py-1 text-right ${overColor}`}>
                        {fmtPct(row.overRate)}
                      </td>
                      <td className="py-1 text-right">{fmt1(row.avgTotal)}</td>
                      <td className="py-1 text-right">{fmt1(row.avgActual)}</td>
                      <td className="py-1 text-right">
                        <BiasChip bias={avgError} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Spread Coverage ───────────────────────────────────── */}
      {spreadCoverage.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <h2 className="font-semibold">{sport === "mlb" ? "Run Line Coverage" : "Spread Coverage by Tier"}</h2>
          <p className="text-xs text-gray-500">
            {sport === "mlb"
              ? "Did the favorite cover the run line (±1.5)? Margin = avg actual run differential."
              : "Did the favorite cover? Margin = avg actual point differential."}
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Spread</th>
                  <th className="py-1">Games</th>
                  <th className="py-1">Covers</th>
                  <th className="py-1">Cover%</th>
                  <th className="py-1">Avg Spread</th>
                  <th className="py-1">Avg Margin</th>
                </tr>
              </thead>
              <tbody>
                {spreadCoverage.map((row) => {
                  const coverColor = row.coverRate == null ? ""
                    : row.coverRate > 0.55 ? "text-green-700 font-semibold"
                    : row.coverRate < 0.45 ? "text-red-600 font-semibold"
                    : "";
                  return (
                    <tr key={row.spreadTier} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.spreadTier}</td>
                      <td className="py-1 text-right">{row.n}</td>
                      <td className="py-1 text-right">{row.coverCount}</td>
                      <td className={`py-1 text-right ${coverColor}`}>
                        {fmtPct(row.coverRate)}
                      </td>
                      <td className="py-1 text-right">{fmt1(row.avgSpread)}</td>
                      <td className="py-1 text-right">{fmt1(row.avgMargin)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Team Total Accuracy ───────────────────────────────── */}
      {teamTotalAccuracy.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <h2 className="font-semibold">Team Implied Total Accuracy</h2>
          <p className="text-xs text-gray-500">
            Implied {sport === "mlb" ? "run total" : "total"} (derived from moneylines + O/U) vs actual team score.
            Bias: positive = market over-projected this team. Sorted by worst MAE.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Team</th>
                  <th className="py-1">Games</th>
                  <th className="py-1">Avg Implied</th>
                  <th className="py-1">Avg Actual</th>
                  <th className="py-1">MAE</th>
                  <th className="py-1">Bias</th>
                </tr>
              </thead>
              <tbody>
                {teamTotalAccuracy.map((row) => (
                  <tr key={row.teamAbbrev} className="border-b border-gray-50">
                    <td className="py-1 font-medium">
                      {row.teamAbbrev}
                      <span className="ml-1.5 text-gray-400 font-normal hidden sm:inline">
                        {row.teamName}
                      </span>
                    </td>
                    <td className="py-1 text-right text-gray-400">{row.n}</td>
                    <td className="py-1 text-right">{fmt1(row.avgImplied)}</td>
                    <td className="py-1 text-right">{fmt1(row.avgActual)}</td>
                    <td className="py-1 text-right font-medium">{fmt1(row.mae)}</td>
                    <td className="py-1 text-right">
                      <BiasChip bias={row.bias} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Team Vegas Insights ──────────────────────────────── */}
      {teamInsights.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div>
            <h2 className="font-semibold">Team Vegas Insights</h2>
            <p className="text-xs text-gray-500 mt-0.5">
              Sorted by scoring bias: teams scoring most above implied at top.
              Bias = implied minus actual; negative means the team scores more than expected.
              Over Imp% = how often the team beats their own implied total.
              ATS% = against the spread cover rate.
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Team</th>
                  <th className="py-1 text-right">G</th>
                  <th className="py-1 text-right">Avg Imp</th>
                  <th className="py-1 text-right">Avg Actual</th>
                  <th className="py-1 text-right">MAE</th>
                  <th className="py-1 text-right">Bias</th>
                  <th className="py-1 text-right">Over Imp%</th>
                  <th className="py-1 text-right">Game O%</th>
                  <th className="py-1 text-right">ATS%</th>
                </tr>
              </thead>
              <tbody>
                {teamInsights.map((row) => {
                  // Bias is implied - actual; negative means the market has been low on this team.
                  const biasColor = row.bias == null ? "" : row.bias < -1 ? "text-green-700 font-semibold" : row.bias > 1 ? "text-red-600 font-semibold" : "";
                  const overImpColor = row.overImpliedRate == null ? "" : row.overImpliedRate > 0.53 ? "text-green-700" : row.overImpliedRate < 0.47 ? "text-red-600" : "";
                  const gameOColor = row.gameOverRate == null ? "" : row.gameOverRate > 0.53 ? "text-green-700" : row.gameOverRate < 0.47 ? "text-red-600" : "";
                  const atsColor = row.atsCoverRate == null ? "" : row.atsCoverRate > 0.53 ? "text-green-700" : row.atsCoverRate < 0.47 ? "text-red-600" : "";
                  return (
                    <tr key={row.teamAbbrev} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="py-1.5 font-medium">{row.teamAbbrev}</td>
                      <td className="py-1.5 text-right text-gray-400">{row.n}</td>
                      <td className="py-1.5 text-right">{row.avgImplied != null ? row.avgImplied.toFixed(1) : "—"}</td>
                      <td className="py-1.5 text-right">{row.avgActual != null ? row.avgActual.toFixed(1) : "—"}</td>
                      <td className="py-1.5 text-right">{row.mae != null ? row.mae.toFixed(1) : "—"}</td>
                      <td className={`py-1.5 text-right ${biasColor}`}>
                        {row.bias != null ? `${row.bias > 0 ? "+" : ""}${row.bias.toFixed(1)}` : "—"}
                      </td>
                      <td className={`py-1.5 text-right ${overImpColor}`}>
                        {row.overImpliedRate != null ? `${(row.overImpliedRate * 100).toFixed(0)}%` : "—"}
                      </td>
                      <td className={`py-1.5 text-right ${gameOColor}`}>
                        {row.gameOverRate != null ? `${(row.gameOverRate * 100).toFixed(0)}%` : "—"}
                      </td>
                      <td className={`py-1.5 text-right ${atsColor}`}>
                        {row.atsCoverRate != null ? `${(row.atsCoverRate * 100).toFixed(0)}%` : "—"}
                        {row.atsN > 0 && <span className="ml-1 text-gray-400">({row.atsN})</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Biggest Misses ────────────────────────────────────── */}
      {biggestMisses.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div>
            <h2 className="font-semibold">
              Biggest {sport === "mlb" ? "Run Total" : "Game Total"} Misses — Top 20
            </h2>
            <p className="text-xs text-gray-500 mt-0.5">
              Games where {sport === "mlb" ? "run" : "point"} total deviated most from the Vegas line.
              Positive miss = over, negative = under.
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Date</th>
                  <th className="py-1 text-left">Matchup</th>
                  <th className="py-1 text-right">Line</th>
                  <th className="py-1 text-right">Actual</th>
                  <th className="py-1 text-right">Miss</th>
                  <th className="py-1 text-right">Spread</th>
                  <th className="py-1 text-right">Home Win%</th>
                </tr>
              </thead>
              <tbody>
                {biggestMisses.map((row, i) => {
                  const isOver = row.miss > 0;
                  const missColor = isOver ? "text-green-700 font-semibold" : "text-red-600 font-semibold";
                  const spreadStr = row.homeSpread == null ? "—"
                    : row.homeSpread > 0 ? `+${row.homeSpread}` : String(row.homeSpread);
                  return (
                    <tr key={i} className="border-b border-gray-50">
                      <td className="py-1.5 text-gray-500">{row.gameDate}</td>
                      <td className="py-1.5 font-medium">
                        {row.awayAbbrev} @ {row.homeAbbrev}
                      </td>
                      <td className="py-1.5 text-right">{row.vegasTotal.toFixed(1)}</td>
                      <td className="py-1.5 text-right">{row.actualTotal}</td>
                      <td className={`py-1.5 text-right ${missColor}`}>
                        {isOver ? "+" : ""}{row.miss.toFixed(1)}
                      </td>
                      <td className="py-1.5 text-right">{spreadStr}</td>
                      <td className="py-1.5 text-right">
                        {row.vegasProbHome != null ? `${(row.vegasProbHome * 100).toFixed(0)}%` : "—"}
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
