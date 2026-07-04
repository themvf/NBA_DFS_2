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
  MlbTotalBacktest,
  MlbMlBacktest,
  MlbBetRow,
  MlbBetBacktestRow,
  MlbBetSideRow,
  MlbClvRow,
  MlbLineMovementRow,
  LineAlertRow,
  LineAlertBacktestRow,
  MlbHealthIssue,
} from "@/db/queries";
import { fetchVegasOdds } from "./actions";
import LineMovementPanel from "./line-movement-panel";
import LineAlertsPanel from "./line-alerts-panel";
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
const fmtSignedPp = (v: number | null | undefined) => {
  if (v == null) return "—";
  return `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}pp`;
};

// ── Betting intelligence helpers ────────────────────────────────────────────

const fmtDate = (value: string | null | undefined) => value ?? "None";

function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}

function scaleSignalConfidence(
  n: number | null | undefined,
  stableSample: number,
): number {
  const sample = n ?? 0;
  if (sample <= 0) return 0;
  return clamp(sample / stableSample, 0, 1);
}

function blendTowardNeutral(
  value: number | null,
  confidence: number,
  neutral = 0.5,
): number | null {
  if (value == null) return null;
  return neutral + (value - neutral) * clamp(confidence, 0, 1);
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
const MLB_LEAGUE_AVG_K9 = 8.6;

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

const MLB_DEFAULT_OUTFIELD_BEARING = 45;
const MLB_OUTFIELD_BEARINGS: Partial<Record<string, number>> = {
  BOS: 35,
  CHC: 40,
  CIN: 30,
  COL: 25,
  DET: 20,
  LAD: 55,
  MIA: 10,
  MIN: 30,
  PIT: 25,
  SF: 60,
};

function getCompassDegrees(direction: string): number | null {
  const normalized = direction.trim().toUpperCase();
  const mapping: Record<string, number> = {
    N: 0,
    NE: 45,
    E: 90,
    SE: 135,
    S: 180,
    SW: 225,
    W: 270,
    NW: 315,
  };
  return mapping[normalized] ?? null;
}

function angularDifference(a: number, b: number): number {
  const diff = Math.abs(a - b) % 360;
  return diff > 180 ? 360 - diff : diff;
}

function getMlbWindDirectionalLean(
  homeAbbrev: string,
  windDirection: string | null | undefined,
): number {
  if (!windDirection) return 0;
  const normalized = windDirection.toLowerCase();
  if (normalized.includes("out")) return 1;
  if (normalized.includes("in")) return -1;
  const fromDegrees = getCompassDegrees(windDirection);
  if (fromDegrees == null) return 0;
  const outfieldBearing = MLB_OUTFIELD_BEARINGS[homeAbbrev] ?? MLB_DEFAULT_OUTFIELD_BEARING;
  const toDegrees = (fromDegrees + 180) % 360;
  const diff = angularDifference(toDegrees, outfieldBearing);
  if (diff <= 45) return 1;
  if (diff >= 135) return -1;
  return 0;
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

  // Model total — MLB only. The residual-over-Vegas Ridge model
  // (model/mlb_game_total_model.py) is our strongest single O/U signal
  // (~55% side accuracy on holdout), and it already folds in SP xFIP/K9, park,
  // weather, team wRC+/ISO, and bullpen FIP. When present it dominates the
  // blend; the legacy historical-rate signals become secondary context.
  const hasModelTotal =
    sport === "mlb" && m.ourTotalPred != null && m.vegasTotal != null;
  if (hasModelTotal) {
    const edge = (m.ourTotalPred as number) - (m.vegasTotal as number);
    // Runs of edge → over probability. ~1 run edge ≈ 0.62; capped to avoid
    // overconfidence from a single noisy feature row.
    const modelValue = clamp(1 / (1 + Math.exp(-edge / 2.0)), 0.3, 0.7);
    signals.push({ label: "Model total", value: modelValue, weight: 0.45 });
  }

  // SP-quality factor — MLB only, when both SPs are known
  if (sport === "mlb" && m.homeSpXfip != null && m.awaySpXfip != null) {
    const avgXfip = (m.homeSpXfip + m.awaySpXfip) / 2;
    // Map xFIP vs league average to a probability centered at 0.5.
    // xFIP 1 run above average → 0.65 (over lean); 1 run below → 0.35 (under lean).
    // Slope calibrated so a 0.5-run gap shifts score by ~7.5 pp.
    const xfipEdge = (avgXfip - MLB_LEAGUE_AVG_XFIP) / MLB_LEAGUE_AVG_XFIP;
    const spValue = clamp(0.5 + xfipEdge * 1.5, 0.3, 0.7);
    signals.push({ label: "SP quality", value: spValue, weight: 0.15 });
  }

  if (sport === "mlb" && m.homeSpKPer9 != null && m.awaySpKPer9 != null) {
    const avgK9 = (m.homeSpKPer9 + m.awaySpKPer9) / 2;
    const kEdge = (avgK9 - MLB_LEAGUE_AVG_K9) / MLB_LEAGUE_AVG_K9;
    const strikeoutValue = clamp(0.5 - kEdge * 0.35, 0.4, 0.6);
    signals.push({ label: "SP strikeout profile", value: strikeoutValue, weight: 0.1 });
  }

  if (sport === "mlb" && m.parkRunsFactor != null) {
    const parkValue = clamp(0.5 + (m.parkRunsFactor - 1) * 0.35, 0.42, 0.58);
    signals.push({ label: "Park run factor", value: parkValue, weight: 0.08 });
  }

  if (sport === "mlb" && m.weatherTemp != null) {
    const tempValue = clamp(0.5 + ((m.weatherTemp - 72) / 25) * 0.05, 0.44, 0.56);
    signals.push({ label: "Temperature", value: tempValue, weight: 0.04 });
  }

  if (sport === "mlb" && m.windSpeed != null) {
    const directionalLean = getMlbWindDirectionalLean(m.homeAbbrev, m.windDirection);
    if (directionalLean !== 0) {
      const cappedWind = clamp(m.windSpeed, 0, 20);
      const windValue = clamp(0.5 + directionalLean * (cappedWind / 20) * 0.08, 0.42, 0.58);
      signals.push({ label: "Wind", value: windValue, weight: 0.06 });
    }
  }

  const tierShrunk = shrinkRate(tierRow?.overRate, tierRow?.n, 0.5, sport === "mlb" ? 60 : 50);
  if (tierShrunk != null) {
    const confidence = sport === "mlb"
      ? scaleSignalConfidence(tierRow?.n, 12)
      : scaleSignalConfidence(tierRow?.n, 60);
    const calibratedTier = blendTowardNeutral(tierShrunk, confidence);
    if (calibratedTier != null) {
      signals.push({
        label: sport === "mlb" ? "Total tier" : "Total tier (calibrated)",
        value: calibratedTier,
        weight: sport === "mlb" ? (hasModelTotal ? 0.10 : 0.20) : 0.20,
      });
    }
  }

  const homeBaseRate = home?.gameOverRate;
  const homeShrunk = shrinkRate(homeBaseRate, home?.n, 0.5, sport === "mlb" ? 40 : 40);
  if (homeShrunk != null) {
    const confidence = sport === "mlb"
      ? scaleSignalConfidence(home?.n, 30)
      : scaleSignalConfidence(home?.n, 40);
    const calibratedHome = blendTowardNeutral(homeShrunk, confidence);
    if (calibratedHome != null) {
      signals.push({
        label: `${m.homeAbbrev} history`,
        value: calibratedHome,
        weight: sport === "mlb" ? (hasModelTotal ? 0.10 : 0.20) : 0.20,
      });
    }
  }

  const awayBaseRate = away?.gameOverRate;
  const awayShrunk = shrinkRate(awayBaseRate, away?.n, 0.5, sport === "mlb" ? 40 : 40);
  if (awayShrunk != null) {
    const confidence = sport === "mlb"
      ? scaleSignalConfidence(away?.n, 30)
      : scaleSignalConfidence(away?.n, 40);
    const calibratedAway = blendTowardNeutral(awayShrunk, confidence);
    if (calibratedAway != null) {
      signals.push({
        label: `${m.awayAbbrev} history`,
        value: calibratedAway,
        weight: sport === "mlb" ? (hasModelTotal ? 0.10 : 0.20) : 0.20,
      });
    }
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
  if (sport === "nba") {
    return null;
  }

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
// eslint-disable-next-line @typescript-eslint/no-unused-vars
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

function computeMlScoreCalibrated(
  m: VegasMatchupRow,
  teamInsights: TeamVegasInsightRow[],
  sport: Sport,
): { score: number; signals: ScoreSignal[] } | null {
  if (m.homeWinProb == null) return null;
  const home = teamInsights.find((t) => t.teamAbbrev === m.homeAbbrev);
  const away = teamInsights.find((t) => t.teamAbbrev === m.awayAbbrev);

  if (sport === "nba") {
    return {
      score: m.homeWinProb,
      signals: [{ label: "Vegas home win%", value: m.homeWinProb, weight: 1 }],
    };
  }

  const biasDivisor = 3;
  const homeBias = home?.bias ?? 0;
  const awayBias = away?.bias ?? 0;
  const rawBiasAdj = clamp((homeBias - awayBias) / biasDivisor, -0.05, 0.05);
  const confidence = Math.min(
    scaleSignalConfidence(home?.nImplied, 20),
    scaleSignalConfidence(away?.nImplied, 20),
  );
  const biasAdj = rawBiasAdj * confidence;

  const score = clamp(m.homeWinProb + biasAdj, 0.05, 0.95);
  const signals: ScoreSignal[] = [
    { label: "Vegas home win%", value: m.homeWinProb, weight: 1 },
    { label: `Net bias adj (div ${biasDivisor})`, value: biasAdj, weight: 0 },
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

/**
 * MLB "Our Total" cell — our model number plus a calibrated strength chip.
 * Tiers come straight from the walk-forward backtest:
 *   ≥1.5 run edge → Strong (≈56%, +7% ROI)
 *   ≥1.0 run edge → Lean   (≈56%, +7% ROI)   ← actionable threshold
 *   0.5–1.0       → sub-threshold (≈49–53%, skip)
 *   <0.5          → no lean
 */
function OurTotalCell({ edge, ourTotal }: { edge: number | null; ourTotal: number | null }) {
  if (edge == null || ourTotal == null) {
    return <span className="text-gray-400">—</span>;
  }
  const abs = Math.abs(edge);
  const side = edge > 0 ? "O" : "U";
  const signed = `${edge > 0 ? "+" : ""}${edge.toFixed(1)}`;

  let chip: { text: string; cls: string } | null = null;
  if (abs >= 1.5) {
    chip = { text: `Strong ${side}`, cls: "bg-emerald-600 text-white" };
  } else if (abs >= MLB_TOTAL_ACTIONABLE_EDGE) {
    chip = { text: `Lean ${side}`, cls: "bg-emerald-100 text-emerald-800 border border-emerald-300" };
  } else if (abs >= 0.5) {
    chip = { text: `${side} ${signed}`, cls: "bg-gray-100 text-gray-500" };
  }

  return (
    <span className="inline-flex items-center justify-end gap-1.5 tabular-nums">
      <span className="text-gray-700">{ourTotal.toFixed(1)}</span>
      {chip ? (
        <span
          className={`rounded px-1.5 py-0.5 text-[10px] font-semibold ${chip.cls}`}
          title={`Model ${edge > 0 ? "over" : "under"} by ${abs.toFixed(1)} runs`}
        >
          {abs >= MLB_TOTAL_ACTIONABLE_EDGE ? `${chip.text} ${signed}` : chip.text}
        </span>
      ) : (
        <span className="text-[10px] text-gray-400">{signed}</span>
      )}
    </span>
  );
}

/**
 * MLB total-model backtest panel — walk-forward calibration of our O/U number
 * vs the line, bucketed by edge magnitude. Makes the "trust the ≥1-run edges,
 * skip the small ones" finding legible at a glance.
 */
function MlbTotalBacktestPanel({ backtest }: { backtest: MlbTotalBacktest }) {
  const { overall, tiers } = backtest;
  if (overall.bets === 0) {
    return (
      <div className="rounded-lg border bg-white p-4">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
          Model O/U Backtest
        </h3>
        <p className="text-xs text-gray-500">
          No settled model predictions yet. Track record builds as games with{" "}
          <code className="rounded bg-gray-100 px-1">our_total_pred</code> complete.
        </p>
      </div>
    );
  }

  const BREAKEVEN = 0.5238; // −110 vig
  const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
  const roiStr = (v: number | null) =>
    v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`;

  return (
    <div className="rounded-lg border bg-white p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2 mb-1">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Model O/U Backtest
        </h3>
        <span className="text-xs text-gray-500">
          {overall.wins}–{overall.losses} ({pct(overall.winRate)}),{" "}
          <span className={overall.roi != null && overall.roi >= 0 ? "text-emerald-600" : "text-red-600"}>
            {roiStr(overall.roi)} ROI
          </span>{" "}
          on {overall.bets} graded bets
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-3">
        Walk-forward (train on prior games only). Our lean = side of the line our number takes;
        graded vs the actual total. <strong>Edges ≥ 1 run are the actionable tier</strong> —
        the page only flags those as O/U leans.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">Edge (|our − line|)</th>
              <th className="py-1 text-right">Bets</th>
              <th className="py-1 text-right">W–L</th>
              <th className="py-1 text-right">Win%</th>
              <th className="py-1 text-right">ROI</th>
            </tr>
          </thead>
          <tbody>
            {tiers.map((t) => {
              const actionable = t.tierMin != null && t.tierMin >= MLB_TOTAL_ACTIONABLE_EDGE;
              const beatBreakeven = t.winRate != null && t.winRate >= BREAKEVEN;
              return (
                <tr
                  key={t.tier}
                  className={`border-b border-gray-50 ${actionable ? "bg-emerald-50/40" : ""}`}
                >
                  <td className="py-1.5">
                    {t.tier}
                    {actionable && (
                      <span className="ml-1.5 rounded bg-emerald-100 px-1 py-0.5 text-[9px] font-semibold text-emerald-700">
                        ACTIONABLE
                      </span>
                    )}
                  </td>
                  <td className="py-1.5 text-right text-gray-500">{t.bets}</td>
                  <td className="py-1.5 text-right tabular-nums">{t.wins}–{t.losses}</td>
                  <td className={`py-1.5 text-right tabular-nums font-medium ${beatBreakeven ? "text-emerald-600" : "text-gray-500"}`}>
                    {pct(t.winRate)}
                  </td>
                  <td className={`py-1.5 text-right tabular-nums ${t.roi != null && t.roi >= 0 ? "text-emerald-600" : "text-red-500"}`}>
                    {roiStr(t.roi)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-[11px] text-gray-400">
        Win% ≥ 52.4% (green) clears the −110 breakeven. Settlement is automatic: the prediction
        freezes pre-game in <code className="rounded bg-gray-100 px-1">mlb_matchups</code> and is
        graded once the final score lands.
      </p>
    </div>
  );
}

/**
 * MLB moneyline-model backtest. Unlike totals, the ML market is efficient: our
 * win-prob model does not beat it out of sample, so this panel's job is to make
 * that *visible* and stop us betting noise. Edges are informational, not
 * actionable; the high-edge ROI is dog-variance, not a repeatable signal.
 */
function MlbMoneylineBacktestPanel({ backtest }: { backtest: MlbMlBacktest }) {
  const { overall, tiers } = backtest;
  if (overall.bets === 0) {
    return (
      <div className="rounded-lg border bg-white p-4">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
          Model Moneyline Backtest
        </h3>
        <p className="text-xs text-gray-500">No settled moneyline predictions yet.</p>
      </div>
    );
  }
  const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
  const roiStr = (v: number | null) =>
    v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`;

  return (
    <div className="rounded-lg border bg-white p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2 mb-1">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Model Moneyline Backtest
        </h3>
        <span className="rounded bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-800">
          INFORMATIONAL — market efficient, no stable edge
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-3">
        Walk-forward; bet the side our win-prob favours vs the vig-free line, by
        edge size, priced at the real moneyline. Out of sample our model does{" "}
        <strong>not</strong> beat the market (logloss slightly worse), so — unlike
        the totals model — <strong>ML edges are not flagged as bets.</strong> Any
        positive ROI below is dominated by a few big-underdog hits (see Dog%),
        not a repeatable signal.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">Edge (|our − mkt| win%)</th>
              <th className="py-1 text-right">Bets</th>
              <th className="py-1 text-right">W–L</th>
              <th className="py-1 text-right">Win%</th>
              <th className="py-1 text-right">ROI</th>
              <th className="py-1 text-right">Dog%</th>
            </tr>
          </thead>
          <tbody>
            {tiers.map((t) => (
              <tr key={t.tier} className="border-b border-gray-50">
                <td className="py-1.5">{t.tier}</td>
                <td className="py-1.5 text-right text-gray-500">{t.bets}</td>
                <td className="py-1.5 text-right tabular-nums">{t.wins}–{t.losses}</td>
                <td className="py-1.5 text-right tabular-nums">{pct(t.winRate)}</td>
                <td className={`py-1.5 text-right tabular-nums ${t.roi != null && t.roi >= 0 ? "text-gray-600" : "text-red-500"}`}>
                  {roiStr(t.roi)}
                </td>
                <td className="py-1.5 text-right tabular-nums text-gray-400">
                  {t.bets > 0 ? `${Math.round((t.dogBets / t.bets) * 100)}%` : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-[11px] text-gray-400">
        Overall: {overall.wins}–{overall.losses} ({pct(overall.winRate)}), ROI{" "}
        {roiStr(overall.roi)} across {overall.bets} bets. ROI uses true ML prices.
        The model number (Our Win%) is kept as context — where we disagree with the
        line — not as a bet recommendation.
      </p>
    </div>
  );
}

function StarChips({ n }: { n: number }) {
  return (
    <span className="text-amber-500" title={`${n}★`}>
      {"★".repeat(n)}
      <span className="text-gray-300">{"★".repeat(Math.max(0, 5 - n))}</span>
    </span>
  );
}

function BetStatusPill({ status, locked }: { status: string; locked: boolean }) {
  const cls =
    status === "won" ? "bg-emerald-100 text-emerald-700"
    : status === "lost" ? "bg-red-100 text-red-600"
    : status === "void" ? "bg-gray-100 text-gray-500"
    : locked ? "bg-blue-100 text-blue-700" : "bg-amber-100 text-amber-700";
  const label = status === "pending" ? (locked ? "locked" : "pending") : status;
  return <span className={`rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase ${cls}`}>{label}</span>;
}

/**
 * MLB rated bet ledger — parity with the soccer accountability framework.
 * Immutable, model_version-stamped, lock-at-first-pitch rows from mlb_bets.
 */
// The current MLB gameline model (mlb_game_bets.py) hard-caps moneyline AND
// totals at 2★ — walk-forward evidence showed neither market beats the price
// (see model docstring). Any row above this in the ledger PREDATES that cap
// (mostly mlb-gameline-v1, which had no market anchoring at all and produced
// wildly overconfident probabilities on plus-money underdogs — our_prob 64%
// against a ~15% market-implied price is the pattern). Locked/settled rows are
// never rewritten (the frozen-closing-recommendation rule), so these persist
// in the ledger as historical/audit evidence, not as live recommendations —
// they are the DATA that justified the cap. Flagged as "Legacy", not deleted.
const _MLB_GAMELINE_STAR_CAP = 2;

function MlbBetLedgerPanel({ bets }: { bets: MlbBetRow[] }) {
  // Default 1★ (not 3★): the current model caps ML/totals at 2★, so with
  // legacy rows hidden by default, a 3★+ default would show an empty table.
  const [minStars, setMinStars] = useState(1);
  const [hideLegacy, setHideLegacy] = useState(true);
  const fmtOdds = (ml: number | null) => (ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml));
  const isLegacy = (b: MlbBetRow) => b.stars > _MLB_GAMELINE_STAR_CAP;
  const shown = bets.filter((b) => b.stars >= minStars && !(hideLegacy && isLegacy(b)));
  const legacyHidden = bets.filter((b) => b.stars >= minStars && isLegacy(b)).length;
  const pending = bets.filter((b) => b.status === "pending").length;

  return (
    <div className="rounded-lg border bg-white p-4">
      <div className="flex flex-wrap items-center justify-between gap-2 mb-1">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Rated Bet Ledger
        </h3>
        <div className="flex items-center gap-2 text-xs">
          {pending > 0 && <span className="text-gray-500">{pending} pending</span>}
          <label className="flex items-center gap-1 text-gray-500" title="Rows above 2★ predate the current model's star cap (mlb-gameline-v1 / early v2, no market anchoring). Kept for the audit trail, never rewritten once locked.">
            <input type="checkbox" checked={hideLegacy} onChange={(e) => setHideLegacy(e.target.checked)} />
            Hide legacy (pre-cap)
          </label>
          <label className="flex items-center gap-1">
            <span className="text-gray-500">Min ★</span>
            <select
              value={minStars}
              onChange={(e) => setMinStars(Number(e.target.value))}
              className="rounded border bg-white px-1.5 py-0.5"
            >
              {[1, 2, 3, 4, 5].map((s) => <option key={s} value={s}>{s}★+</option>)}
            </select>
          </label>
        </div>
      </div>
      <p className="text-xs text-gray-500 mb-3">
        Every rated bet is logged immutably with its model version and{" "}
        <strong>locks at first pitch</strong>, so the backtest uses the number we
        actually committed to. Stars combine EV (vs the price) and edge (vs the
        vig-free market). The current model caps moneyline <strong>and</strong>{" "}
        totals at 2★ — walk-forward showed neither beats the market (see the
        Backtest panel below). Anything rated above 2★ here is a{" "}
        <strong>legacy</strong> row from before that cap (mostly the
        unanchored v1 model on plus-money underdogs) — real historical
        evidence, kept for the audit, not a live recommendation.
      </p>
      {legacyHidden > 0 && (
        <p className="text-xs text-amber-600 mb-2">
          {legacyHidden} legacy pre-cap row{legacyHidden === 1 ? "" : "s"} hidden — uncheck &ldquo;Hide legacy&rdquo; to view.
        </p>
      )}
      {shown.length === 0 ? (
        <p className="text-xs text-gray-400">No bets at {minStars}★+.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="border-b text-gray-500">
                <th className="py-1 text-left">Rating</th>
                <th className="py-1 text-left">Type</th>
                <th className="py-1 text-left">Selection</th>
                <th className="py-1 text-right">Our %</th>
                <th className="py-1 text-right">Odds</th>
                <th className="py-1 text-right">EV</th>
                <th className="py-1 text-right">Result</th>
              </tr>
            </thead>
            <tbody>
              {shown.slice(0, 60).map((b) => {
                const legacy = isLegacy(b);
                return (
                  <tr key={b.id} className={`border-b border-gray-50 hover:bg-gray-50 ${legacy ? "opacity-60" : ""}`}>
                    <td className="py-1.5">
                      <StarChips n={b.stars} />
                      {legacy && (
                        <span className="ml-1 rounded bg-amber-100 px-1 py-0.5 text-[9px] font-semibold uppercase text-amber-700"
                              title="Predates the 2★ gameline cap — historical/audit row, not a live recommendation">
                          Legacy
                        </span>
                      )}
                    </td>
                    <td className="py-1.5 text-gray-500">{b.betType === "moneyline" ? "ML" : "O/U"}</td>
                    <td className="py-1.5">
                      <span className="font-medium">{b.selectionLabel}</span>
                      {b.fixture && <span className="block text-[10px] text-gray-400">{b.fixture}</span>}
                    </td>
                    <td className="py-1.5 text-right tabular-nums">{(b.ourProb * 100).toFixed(0)}%</td>
                    <td className="py-1.5 text-right tabular-nums">{fmtOdds(b.marketOdds)}</td>
                    <td className={`py-1.5 text-right tabular-nums ${b.ev != null && b.ev > 0 ? "text-emerald-600" : "text-gray-500"}`}>
                      {b.ev != null ? `${b.ev >= 0 ? "+" : ""}${(b.ev * 100).toFixed(0)}%` : "—"}
                    </td>
                    <td className="py-1.5 text-right"><BetStatusPill status={b.status} locked={b.locked} /></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

/** Calibration of the bet ledger by star tier — do the stars mean what we claim? */
function MlbBetLedgerBacktestPanel({ rows }: { rows: MlbBetBacktestRow[] }) {
  const overall = rows.find((r) => r.betType === "all");
  const byType = (t: string) => rows.filter((r) => r.betType === t && r.stars > 0).sort((a, b) => b.stars - a.stars);
  const subtotal = (t: string) => rows.find((r) => r.betType === t && r.stars === 0);
  const pct = (v: number) => `${(v * 100).toFixed(1)}%`;
  const roiStr = (v: number | null) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`);

  const section = (t: string, label: string) => {
    const tiers = byType(t);
    const sub = subtotal(t);
    if (!sub || sub.n === 0) return null;
    return (
      <div key={t}>
        <div className="text-xs font-semibold text-gray-600 mt-2 mb-1">{label}</div>
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">Stars</th>
              <th className="py-1 text-right">n</th>
              <th className="py-1 text-right">Exp win%</th>
              <th className="py-1 text-right">Real win%</th>
              <th className="py-1 text-right">ROI</th>
              <th className="py-1 text-right">Brier</th>
            </tr>
          </thead>
          <tbody>
            {tiers.map((r) => (
              <tr key={r.stars} className={`border-b border-gray-50 ${r.stars >= 4 ? "bg-emerald-50/40" : ""}`}>
                <td className="py-1.5"><StarChips n={r.stars} /></td>
                <td className="py-1.5 text-right text-gray-500">{r.n}</td>
                <td className="py-1.5 text-right tabular-nums text-gray-500">{pct(r.expectedWinRate)}</td>
                <td className="py-1.5 text-right tabular-nums font-medium">{pct(r.realizedWinRate)}</td>
                <td className={`py-1.5 text-right tabular-nums ${r.roi != null && r.roi >= 0 ? "text-emerald-600" : "text-red-500"}`}>{roiStr(r.roi)}</td>
                <td className="py-1.5 text-right tabular-nums text-gray-400">{r.brier != null ? r.brier.toFixed(3) : "—"}</td>
              </tr>
            ))}
            <tr className="border-t font-medium">
              <td className="py-1.5 text-gray-500">All</td>
              <td className="py-1.5 text-right text-gray-500">{sub.n}</td>
              <td className="py-1.5 text-right tabular-nums text-gray-500">{pct(sub.expectedWinRate)}</td>
              <td className="py-1.5 text-right tabular-nums">{pct(sub.realizedWinRate)}</td>
              <td className={`py-1.5 text-right tabular-nums ${sub.roi != null && sub.roi >= 0 ? "text-emerald-600" : "text-red-500"}`}>{roiStr(sub.roi)}</td>
              <td className="py-1.5 text-right tabular-nums text-gray-400">{sub.brier != null ? sub.brier.toFixed(3) : "—"}</td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <div className="rounded-lg border bg-white p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2 mb-1">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Bet Ledger Backtest</h3>
        {overall && overall.n > 0 && (
          <span className="text-xs text-gray-500">
            {overall.n} settled · realized {pct(overall.realizedWinRate)} vs expected {pct(overall.expectedWinRate)}
            {overall.roi != null && (
              <span className={overall.roi >= 0 ? " text-emerald-600" : " text-red-600"}> · {roiStr(overall.roi)} ROI</span>
            )}
          </span>
        )}
      </div>
      <p className="text-xs text-gray-500 mb-1">
        Calibration on settled bets: realized win% should meet or beat expected
        (our_prob) in each star tier. ROI is priced at each bet&rsquo;s true odds.
      </p>
      {section("total", "Totals (O/U)")}
      {section("moneyline", "Moneyline")}
    </div>
  );
}

// Anti-longshot-illusion guardrail: ROI split by favorite/dog & over/under, per
// model version. The favorite + over sides are the low-variance honest skill test
// — a model with real edge shouldn't lose there. "Selected" = positive-EV subset
// (the bets we'd actually place). Side-by-side model versions show v1→v2 progress.
function MlbBetSideBreakdownPanel({ rows }: { rows: MlbBetSideRow[] }) {
  if (rows.length === 0) return null;
  const pct = (v: number) => `${(v * 100).toFixed(1)}%`;
  const roiStr = (v: number | null) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`);
  const roiCls = (v: number | null) => (v == null ? "text-gray-400" : v >= 0 ? "text-emerald-600" : "text-red-500");
  const versions = Array.from(new Set(rows.map((r) => r.modelVersion))).sort();
  const BUCKETS = ["favorite", "underdog", "over", "under"];
  const LOW_VAR = new Set(["favorite", "over"]); // the honest skill test

  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
        Bet ROI by side (longshot guardrail)
      </h3>
      <p className="text-xs text-gray-500 mb-2">
        Profit on plus-money <b>underdogs</b> is high-variance and can look great by luck.
        The honest skill test is the low-variance side (<b>favorites</b>, <b>overs</b>) —
        a model with real edge shouldn&rsquo;t lose there. &ldquo;Sel ROI&rdquo; restricts to
        positive-EV bets (what we&rsquo;d actually place).
      </p>
      {versions.map((mv) => {
        const vr = rows.filter((r) => r.modelVersion === mv);
        return (
          <div key={mv} className="mb-2">
            <div className="text-xs font-semibold text-gray-600 mt-2 mb-1">{mv}</div>
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Side</th>
                  <th className="py-1 text-right">n</th>
                  <th className="py-1 text-right">Real win%</th>
                  <th className="py-1 text-right">Cal gap</th>
                  <th className="py-1 text-right">ROI</th>
                  <th className="py-1 text-right">Sel n</th>
                  <th className="py-1 text-right">Sel ROI</th>
                </tr>
              </thead>
              <tbody>
                {BUCKETS.map((bucket) => {
                  const r = vr.find((x) => x.sideBucket === bucket);
                  if (!r) return null;
                  const gap = r.winRate - r.expectedWinRate;
                  return (
                    <tr key={bucket} className={`border-b border-gray-50 ${LOW_VAR.has(bucket) ? "bg-amber-50/40" : ""}`}>
                      <td className="py-1.5 capitalize font-medium">
                        {bucket}{LOW_VAR.has(bucket) && <span className="ml-1 text-amber-600" title="low-variance skill test">★</span>}
                      </td>
                      <td className="py-1.5 text-right text-gray-500">{r.n}</td>
                      <td className="py-1.5 text-right tabular-nums">{pct(r.winRate)}</td>
                      <td className={`py-1.5 text-right tabular-nums ${gap >= 0 ? "text-emerald-600" : "text-red-500"}`}>{`${gap >= 0 ? "+" : ""}${(gap * 100).toFixed(1)}`}</td>
                      <td className={`py-1.5 text-right tabular-nums ${roiCls(r.roi)}`}>{roiStr(r.roi)}</td>
                      <td className="py-1.5 text-right text-gray-500">{r.selectedN}</td>
                      <td className={`py-1.5 text-right tabular-nums ${roiCls(r.selectedRoi)}`}>{roiStr(r.selectedRoi)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
}

// Pipeline health banner — renders ONLY when an operational check trips, so it's
// a real alert, not chrome. Catches the silent-failure modes (dead refresh cron,
// unsettled finals, an unrated slate) that would otherwise sit unnoticed.
function MlbPipelineHealthBanner({ issues }: { issues: MlbHealthIssue[] }) {
  if (issues.length === 0) return null;
  const hasError = issues.some((i) => i.severity === "error");
  const tone = hasError
    ? "border-red-300 bg-red-50/70"
    : "border-amber-300 bg-amber-50/70";
  const dot = (sev: string) => (sev === "error" ? "text-red-500" : "text-amber-500");
  return (
    <div className={`rounded-lg border p-4 text-sm ${tone}`}>
      <div className="font-semibold text-gray-800 mb-1.5">
        ⚠ Pipeline needs attention ({issues.length})
      </div>
      <ul className="space-y-1.5">
        {issues.map((i) => (
          <li key={i.kind} className="flex gap-2">
            <span className={dot(i.severity)}>●</span>
            <span>
              <span className="font-medium text-gray-800">{i.title}</span>
              <span className="text-gray-600"> — {i.detail}</span>
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

// Closing Line Value (moneyline) — the sharpest small-sample edge signal: did
// the market move toward our side between open and the last pre-kickoff snapshot?
// Needs ≥2 snapshots per bet, so it reads "accruing" until the refresh cadence
// captures intra-day line movement.
function MlbClvPanel({ rows }: { rows: MlbClvRow[] }) {
  const all = rows.find((r) => r.tier === "all");
  const rated = rows.find((r) => r.tier === "rated");
  const hasData = (all?.n ?? 0) > 0;
  const clvStr = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(2)}pp`;
  const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(0)}%`);

  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
        Closing Line Value (moneyline)
      </h3>
      <p className="text-xs text-gray-500 mb-2">
        Did the market move toward our side between when we rated the bet and first pitch?
        Positive CLV = we beat the close = real edge, detectable long before win/loss ROI
        stabilizes. Totals are excluded (fixed −110 price; only the line moves).
      </p>
      {!hasData ? (
        <div className="rounded bg-amber-50/60 border border-amber-200 px-3 py-2 text-xs text-amber-700">
          Accruing — CLV needs ≥2 snapshots per bet (open vs close). The refresh currently
          captures one snapshot per bet before it locks at first pitch, so there&rsquo;s no
          intra-day line movement to measure yet. Populates once the slate is re-rated
          multiple times before first pitch.
        </div>
      ) : (
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">Tier</th>
              <th className="py-1 text-right">n</th>
              <th className="py-1 text-right">Avg CLV</th>
              <th className="py-1 text-right">Beat close %</th>
            </tr>
          </thead>
          <tbody>
            {[all, rated].map((r, i) =>
              r ? (
                <tr key={r.tier} className={`border-b border-gray-50 ${i === 1 ? "bg-emerald-50/40" : ""}`}>
                  <td className="py-1.5 font-medium">{r.tier === "rated" ? "Rated (3★+)" : "All"}</td>
                  <td className="py-1.5 text-right text-gray-500">{r.n}</td>
                  <td className={`py-1.5 text-right tabular-nums ${r.avgClvPp >= 0 ? "text-emerald-600" : "text-red-500"}`}>{clvStr(r.avgClvPp)}</td>
                  <td className="py-1.5 text-right tabular-nums">{pct(r.beatRate)}</td>
                </tr>
              ) : null,
            )}
          </tbody>
        </table>
      )}
    </div>
  );
}

type Props = {
  matchups: VegasMatchupRow[];
  ouHitRate: OuHitRateRow[];
  teamTotalAccuracy: TeamTotalAccuracyRow[];
  spreadCoverage: SpreadCoverageRow[];
  mlbCoverageStatus: MlbVegasCoverageStatus | null;
  mlbTotalBacktest: MlbTotalBacktest | null;
  mlbMoneylineBacktest: MlbMlBacktest | null;
  mlbBets: MlbBetRow[] | null;
  mlbBetBacktest: MlbBetBacktestRow[] | null;
  mlbBetBySide: MlbBetSideRow[] | null;
  mlbClv: MlbClvRow[] | null;
  mlbLineMovement: MlbLineMovementRow[] | null;
  mlbLineAlerts: LineAlertRow[] | null;
  mlbLineAlertBacktest: LineAlertBacktestRow[] | null;
  mlbHealth: MlbHealthIssue[] | null;
  vegasSummary: VegasSummaryStatsRow | null;
  biggestMisses: BiggestMissRow[];
  teamInsights: TeamVegasInsightRow[];
  moneylineBacktest: MoneylineBacktestReport;
  queryDate: string;
  sport: Sport;
};

type RecommendationMarket = "ou" | "spread" | "ml";
type BettingRow = {
  matchup: VegasMatchupRow;
  ou: ReturnType<typeof computeOuScore>;
  spread: ReturnType<typeof computeSpreadScore>;
  ml: ReturnType<typeof computeMlScoreCalibrated>;
  ouScore: number | null;
  spreadScore: number | null;
  mlScore: number | null;
  ouActionable: boolean;
  actionableCount: number;
  strongestEdge: number;
};

function getRecommendationBand(sport: Sport, market: RecommendationMarket): number {
  if (sport === "nba") {
    if (market === "ou") return 0.03;
    return 0.04;
  }
  if (market === "ou") return 0.025;
  return 0.03;
}

function isActionableScore(
  score: number | null | undefined,
  sport: Sport,
  market: RecommendationMarket,
): score is number {
  if (score == null) return false;
  return Math.abs(score - 0.5) >= getRecommendationBand(sport, market);
}

// Calibrated MLB O/U actionability. The walk-forward backtest
// (getMlbTotalModelBacktest) shows our_total_pred edges < 1.0 run are
// coin-flips (49–53%) while edges ≥ 1.0 run hit ~56% / +7% ROI. So a MLB O/U
// lean is only "actionable" when the model disagrees with the line by ≥ 1 run.
const MLB_TOTAL_ACTIONABLE_EDGE = 1.0;

function mlbTotalEdge(m: VegasMatchupRow): number | null {
  if (m.ourTotalPred == null || m.vegasTotal == null) return null;
  return m.ourTotalPred - m.vegasTotal;
}

function isOuActionable(
  m: VegasMatchupRow,
  ouScore: number | null | undefined,
  sport: Sport,
): boolean {
  if (sport === "mlb") {
    const edge = mlbTotalEdge(m);
    // With a model number, gate on the calibrated edge; otherwise fall back
    // to the blended-score band (e.g. a game missing our_total_pred).
    if (edge != null) return Math.abs(edge) >= MLB_TOTAL_ACTIONABLE_EDGE;
  }
  return isActionableScore(ouScore, sport, "ou");
}

function renderRecommendationScore(
  score: number | null | undefined,
  displayScore: number | null | undefined,
  label: string | undefined,
  signals: ScoreSignal[] | undefined,
  sport: Sport,
  market: RecommendationMarket,
  forceActionable?: boolean,
) {
  if (score == null || displayScore == null) {
    return <span className="text-gray-300 text-xs">—</span>;
  }
  const actionable = forceActionable ?? isActionableScore(score, sport, market);
  if (!actionable) {
    return <span className="text-gray-400 text-xs">No edge</span>;
  }
  return <ScoreBadge score={displayScore} label={label} signals={signals} />;
}

export default function VegasClient({
  matchups,
  ouHitRate,
  teamTotalAccuracy,
  spreadCoverage,
  mlbCoverageStatus,
  mlbTotalBacktest,
  mlbMoneylineBacktest,
  mlbBets,
  mlbBetBacktest,
  mlbBetBySide,
  mlbClv,
  mlbLineMovement,
  mlbLineAlerts,
  mlbLineAlertBacktest,
  mlbHealth,
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
  const bettingRows: BettingRow[] = matchups
    .map((matchup) => {
      const ou = computeOuScore(matchup, ouHitRate, teamInsights, sport);
      const spread = computeSpreadScore(matchup, spreadCoverage, teamInsights, sport);
      const ml = computeMlScoreCalibrated(matchup, teamInsights, sport);
      const ouScore = ou?.score ?? null;
      const spreadScore = spread?.score ?? null;
      const mlScore = ml?.score ?? null;
      const ouActionable = isOuActionable(matchup, ouScore, sport);
      const actionableEdges = [
        ouActionable && ouScore != null ? Math.abs(ouScore - 0.5) : 0,
        isActionableScore(spreadScore, sport, "spread") ? Math.abs(spreadScore - 0.5) : 0,
        isActionableScore(mlScore, sport, "ml") ? Math.abs(mlScore - 0.5) : 0,
      ].filter((edge) => edge > 0);
      return {
        matchup,
        ou,
        spread,
        ml,
        ouScore,
        spreadScore,
        mlScore,
        ouActionable,
        actionableCount: actionableEdges.length,
        strongestEdge: actionableEdges.length > 0 ? Math.max(...actionableEdges) : 0,
      };
    })
    .sort((a, b) =>
      b.actionableCount - a.actionableCount
      || b.strongestEdge - a.strongestEdge
      || a.matchup.homeAbbrev.localeCompare(b.matchup.homeAbbrev)
    );
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

      {/* ── Pipeline health (only renders when something is off) ── */}
      {sport === "mlb" && mlbHealth && mlbHealth.length > 0 && (
        <MlbPipelineHealthBanner issues={mlbHealth} />
      )}

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
            Value edge compares our ML probability to the raw breakeven price. `pp` means percentage points, so `+3.0pp` means our win rate is 3.0 points above market breakeven.
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
                  <th className="py-1">Avg Edge (pp)</th>
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
                        <td className={`py-1.5 text-right ${edgeClass}`}>{fmtSignedPp(row.avgEdge)}</td>
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
              {sport === "nba"
                ? "NBA keeps ML value as the primary live signal and selected O/U leans as secondary context. Spread recommendations are suppressed because recent backtests did not support a stable edge."
                : "Scores derived from historical O/U tiers, run line tiers, team implied accuracy, and ATS cover rates. O/U = lean over probability. Run line = home-covers probability. ML = adjusted win probability; price edge is tracked in the moneyline backtest."}
              {" "}
              Low-edge rows are suppressed as no-edge calls.
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Matchup</th>
                  <th className="py-1 text-left">Actionable</th>
                  <th className="py-1 text-right">Total</th>
                  {sport === "mlb" && <th className="py-1 text-right">Our Total</th>}
                  <th className="py-1 text-right">O/U Score</th>
                  <th className="py-1 text-right">{sport === "mlb" ? "Run Line" : "Spread"}</th>
                  <th className="py-1 text-right">{sport === "mlb" ? "RL Score" : "Spread Status"}</th>
                  <th className="py-1 text-right">ML Score</th>
                </tr>
              </thead>
              <tbody>
                {bettingRows.map((row) => {
                  const { matchup: m, ou, spread, ml, ouScore, spreadScore, mlScore, ouActionable } = row;

                  // MLB O/U lean direction comes from the model edge (calibrated
                  // ≥1-run gate); other sports/markets use the blended score.
                  const mlbEdge = sport === "mlb" ? mlbTotalEdge(m) : null;
                  const ouLabel = ouActionable
                    ? sport === "mlb" && mlbEdge != null
                      ? mlbEdge > 0 ? "O" : "U"
                      : ouScore != null && ouScore >= 0.5
                        ? "O"
                        : "U"
                    : undefined;
                  const spreadLabel =
                    isActionableScore(spreadScore, sport, "spread")
                      ? spreadScore >= 0.5
                        ? m.homeAbbrev
                        : m.awayAbbrev
                      : undefined;
                  const mlLabel =
                    isActionableScore(mlScore, sport, "ml")
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
                  const actionableLabels = [
                    ouLabel ? `O/U ${ouLabel}` : null,
                    spreadLabel ? `${sport === "mlb" ? "RL" : "Spread"} ${spreadLabel}` : null,
                    mlLabel ? `ML ${mlLabel}` : null,
                  ].filter((label): label is string => label != null);
                  const rowClass = row.actionableCount > 0
                    ? "border-b border-emerald-100 bg-emerald-50/40 hover:bg-emerald-50"
                    : "border-b border-gray-50 text-gray-400 hover:bg-gray-50";

                  return (
                    <tr key={m.matchupId} className={rowClass}>
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
                        {sport === "mlb" && m.ourProbHome != null && m.homeWinProb != null && (
                          <div className="text-[10px] font-normal text-gray-400 mt-0.5" title="Model win prob is context only — the moneyline market is efficient and these edges are not actionable.">
                            Model: {m.homeAbbrev} {(m.ourProbHome * 100).toFixed(0)}%
                            <span className="text-gray-300">
                              {" "}(mkt {(m.homeWinProb * 100).toFixed(0)}%,{" "}
                              {m.ourProbHome - m.homeWinProb >= 0 ? "+" : ""}
                              {((m.ourProbHome - m.homeWinProb) * 100).toFixed(0)}pp)
                            </span>
                          </div>
                        )}
                      </td>
                      <td className="py-1.5">
                        {actionableLabels.length > 0 ? (
                          <div className="flex flex-wrap gap-1">
                            {actionableLabels.map((label) => (
                              <span
                                key={`${m.matchupId}-${label}`}
                                className="inline-flex rounded border border-emerald-200 bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-800"
                              >
                                {label}
                              </span>
                            ))}
                          </div>
                        ) : (
                          <span className="text-[10px] uppercase tracking-wide text-gray-400">No edge</span>
                        )}
                      </td>
                      <td className="py-1.5 text-right text-gray-500">{fmt1(m.vegasTotal)}</td>
                      {sport === "mlb" && (
                        <td className="py-1.5 text-right tabular-nums">
                          <OurTotalCell edge={mlbEdge} ourTotal={m.ourTotalPred} />
                        </td>
                      )}
                      <td className="py-1.5 text-right">
                        {renderRecommendationScore(
                          ouScore,
                          ouScore,
                          ouLabel,
                          ou?.signals,
                          sport,
                          "ou",
                          ouActionable,
                        )}
                      </td>
                      <td className="py-1.5 text-right text-gray-500">{homeSpreadStr}</td>
                      <td className="py-1.5 text-right">
                        {sport === "nba" ? (
                          <span className="text-[10px] uppercase tracking-wide text-amber-700">
                            Suppressed
                          </span>
                        ) : renderRecommendationScore(
                          spreadScore,
                          spreadDisplay,
                          spreadLabel,
                          spread?.signals,
                          sport,
                          "spread",
                        )}
                      </td>
                      <td className="py-1.5 text-right">
                        {renderRecommendationScore(
                          mlScore,
                          mlDisplay,
                          mlLabel,
                          ml?.signals,
                          sport,
                          "ml",
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="text-xs text-gray-400">
            Scores are statistical summaries of historical patterns — not betting recommendations.
            {sport === "nba"
              ? " NBA rows are ordered to show ML/O-U spots first, while spread remains intentionally off."
              : " Sample sizes vary; actionable rows are surfaced first and low-edge rows are muted as no-edge."}
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

      {/* ── Model O/U Backtest (MLB) ──────────────────────────── */}
      {sport === "mlb" && mlbTotalBacktest && (
        <MlbTotalBacktestPanel backtest={mlbTotalBacktest} />
      )}

      {/* ── Model Moneyline Backtest (MLB) ────────────────────── */}
      {sport === "mlb" && mlbMoneylineBacktest && (
        <MlbMoneylineBacktestPanel backtest={mlbMoneylineBacktest} />
      )}

      {/* ── Rated Bet Ledger + calibration (MLB) ──────────────── */}
      {sport === "mlb" && mlbBets && mlbBets.length > 0 && (
        <MlbBetLedgerPanel bets={mlbBets} />
      )}
      {sport === "mlb" && mlbBetBacktest && mlbBetBacktest.length > 0 && (
        <MlbBetLedgerBacktestPanel rows={mlbBetBacktest} />
      )}

      {sport === "mlb" && mlbBetBySide && mlbBetBySide.length > 0 && (
        <MlbBetSideBreakdownPanel rows={mlbBetBySide} />
      )}

      {sport === "mlb" && mlbClv && (
        <MlbClvPanel rows={mlbClv} />
      )}
      {sport === "mlb" && mlbLineMovement && (
        <LineMovementPanel rows={mlbLineMovement} cadenceNote="the 30-min odds captures" />
      )}
      {sport === "mlb" && mlbLineAlerts && mlbLineAlertBacktest && (
        <LineAlertsPanel alerts={mlbLineAlerts} backtest={mlbLineAlertBacktest} />
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
