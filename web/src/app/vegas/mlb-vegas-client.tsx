"use client";

import {
  Activity,
  AlertTriangle,
  CalendarDays,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  RefreshCw,
  Target,
  TrendingDown,
  TrendingUp,
} from "lucide-react";
import { useEffect, useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type {
  LineAlertBacktestRow,
  LineAlertRow,
  LineMovementHistoryRow,
  MlbHealthIssue,
  MlbLineMovementRow,
  VegasMatchupRow,
} from "@/db/queries";
import {
  buildMlbMovementSignal,
  type MlbCombinedSignal,
  type MlbMovementSignal,
} from "@/lib/mlb-movement-signals";

type Props = {
  queryDate: string;
  evaluatedAt: string;
  matchups: VegasMatchupRow[];
  health: MlbHealthIssue[];
  lineMovement: MlbLineMovementRow[];
  lineAlerts: LineAlertRow[];
  lineAlertBacktest: LineAlertBacktestRow[];
  lineMovementHistory: LineMovementHistoryRow[];
};

type BoardRow = {
  matchup: VegasMatchupRow;
  movement: MlbLineMovementRow | null;
  signal: MlbMovementSignal | null;
  alerts: LineAlertRow[];
  modelSuppressed: boolean;
};

type ModelDiagnostic = {
  check: string;
  driver: string | null;
};

const GAME_LINE_ALERTS = new Set(["pinnacle_divergence", "steam", "walking", "dk_value"]);

function pct(value: number | null, digits = 1): string {
  return value == null || !Number.isFinite(value) ? "—" : `${(value * 100).toFixed(digits)}%`;
}

function pp(value: number | null, digits = 1): string {
  return value == null || !Number.isFinite(value) ? "—" : `${value >= 0 ? "+" : ""}${value.toFixed(digits)}pp`;
}

const MONEYLINE_FEATURE_LABELS: Record<string, string> = {
  market_home_prob: "market anchor",
  sp_xfip_adv: "starting-pitcher xFIP advantage",
  sp_k9_adv: "starting-pitcher K/9 advantage",
  wrc_adv: "offense wRC+ advantage",
  iso_adv: "offense ISO advantage",
  bullpen_adv: "bullpen FIP advantage",
};

function numericRecord(value: unknown): Record<string, number> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value).flatMap(([key, item]) => (
      typeof item === "number" && Number.isFinite(item) ? [[key, item]] : []
    )),
  );
}

function formatFeatureValue(feature: string, value: number): string {
  if (feature === "market_home_prob") return pct(value);
  if (feature === "iso_adv") return value.toFixed(3);
  return value.toFixed(2);
}

function getModelDiagnostic(matchup: VegasMatchupRow, signal: MlbMovementSignal | null): ModelDiagnostic | null {
  if (!signal?.suppressionReason) return null;

  const prediction = pct(signal.evaluatedModelProbability);
  const market = pct(signal.currentProbability);
  const gap = pp(signal.evaluatedModelGapPp);
  const check = signal.suppressionReason === "probability_out_of_range"
    ? `Model produced ${prediction}; outside the credible 2%–98% range.`
    : signal.suppressionReason === "gap_exceeds_limit"
      ? `Model produced ${prediction} versus ${market} market (${gap}); exceeds the 15pp gap limit.`
      : "The model output was not a finite probability.";

  const contributions = numericRecord(matchup.moneylineFeatureValues?.contributions);
  const coefficients = numericRecord(matchup.moneylineRunConfig?.standardized_coefficients);
  const standardizedValues = numericRecord(matchup.moneylineFeatureValues?.standardized_values);
  const trainingMin = numericRecord(matchup.moneylineRunConfig?.training_feature_min);
  const trainingMax = numericRecord(matchup.moneylineRunConfig?.training_feature_max);
  const featureValues = numericRecord(matchup.moneylineFeatureValues);
  const ranked = Object.entries(contributions).sort(([, left], [, right]) => Math.abs(right) - Math.abs(left));
  const [feature, homeContribution] = ranked[0] ?? [];
  if (!feature || homeContribution == null) return { check, driver: null };

  const sideMultiplier = signal.movementSide === "away" ? -1 : 1;
  const teamContribution = homeContribution * sideMultiplier;
  const direction = teamContribution >= 0 ? `toward ${signal.movementTeam}` : `away from ${signal.movementTeam}`;
  const coefficient = coefficients[feature];
  const zScore = standardizedValues[feature] ?? (
    coefficient != null && Math.abs(coefficient) > 1e-9
      ? homeContribution / coefficient
      : null
  );
  const rawValue = featureValues[feature];
  const usedFallback = matchup.moneylineMissingness?.[feature] === true;
  const trainedRange = trainingMin[feature] != null && trainingMax[feature] != null
    ? `${formatFeatureValue(feature, trainingMin[feature])}–${formatFeatureValue(feature, trainingMax[feature])} trained range`
    : null;
  const details = [
    rawValue != null ? `value ${formatFeatureValue(feature, rawValue)}` : null,
    trainedRange,
    zScore != null && Number.isFinite(zScore) ? `${Math.abs(zScore).toFixed(1)} standard deviations from training mean` : null,
    usedFallback ? "league-average fallback used" : null,
  ].filter(Boolean).join("; ");
  const driver = `Largest recorded driver: ${MONEYLINE_FEATURE_LABELS[feature] ?? feature} (${direction}, ${Math.abs(teamContribution).toFixed(2)} log-odds${details ? `; ${details}` : ""}).`;
  return { check, driver };
}

function fmtEt(value: string | null, withDate = false): string {
  if (!value) return "—";
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return "—";
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    ...(withDate ? { month: "short", day: "numeric" } : {}),
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(parsed);
}

function fmtClock(value: string): string {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
  }).format(new Date(value));
}

function shiftDate(date: string, days: number): string {
  const parsed = new Date(`${date}T12:00:00Z`);
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return parsed.toISOString().slice(0, 10);
}

function ageMinutes(value: string | null, nowIso: string): number | null {
  if (!value) return null;
  const then = Date.parse(value);
  const now = Date.parse(nowIso);
  return Number.isFinite(then) && Number.isFinite(now) ? Math.max(0, (now - then) / 60_000) : null;
}

function captureAge(value: string | null, nowIso: string): { text: string; className: string } {
  const age = ageMinutes(value, nowIso);
  if (age == null) return { text: "No capture", className: "text-red-700" };
  const rounded = Math.floor(age);
  if (age > 45) return { text: `${rounded}m old`, className: "text-red-700" };
  if (age > 30) return { text: `${rounded}m old`, className: "text-amber-700" };
  return { text: `${rounded}m old`, className: "text-emerald-700" };
}

function teamForSide(matchup: VegasMatchupRow, side: string): string {
  return side === "home" ? matchup.homeAbbrev : side === "away" ? matchup.awayAbbrev : side;
}

function alertLabel(alert: LineAlertRow, matchup: VegasMatchupRow): string {
  const team = teamForSide(matchup, alert.side);
  if (alert.alertType === "pinnacle_divergence") return `Sharp side → ${team}`;
  if (alert.alertType === "steam") return `Steam → ${team}`;
  if (alert.alertType === "walking") return `Walking → ${team}`;
  if (alert.alertType === "dk_value") return `DK value → ${team}`;
  return `${alert.alertType.replaceAll("_", " ")} → ${team}`;
}

function alertTone(alertType: string): string {
  if (alertType === "pinnacle_divergence") return "border-violet-200 bg-violet-100 text-violet-900";
  if (alertType === "steam") return "border-orange-200 bg-orange-100 text-orange-900";
  if (alertType === "walking") return "border-blue-200 bg-blue-100 text-blue-900";
  return "border-emerald-200 bg-emerald-100 text-emerald-900";
}

const COMBINED_SIGNAL_META: Record<MlbCombinedSignal, { label: string; detail: string; className: string }> = {
  strong_confirmation: { label: "STRONG CONFIRMATION", detail: "Move + model agree", className: "border-emerald-200 bg-emerald-100 text-emerald-900" },
  contrarian: { label: "CONTRARIAN", detail: "Move + model disagree", className: "border-red-200 bg-red-100 text-red-900" },
  market_only: { label: "MARKET ONLY", detail: "Move without model support", className: "border-amber-200 bg-amber-100 text-amber-900" },
  quiet: { label: "QUIET", detail: "No meaningful move", className: "border-slate-200 bg-white text-slate-500" },
};

function CombinedSignalBadge({ signal }: { signal: MlbCombinedSignal }) {
  const meta = COMBINED_SIGNAL_META[signal];
  return <div><span className={`inline-flex rounded-full border px-2 py-1 text-[10px] font-bold ${meta.className}`}>{meta.label}</span><div className="mt-1 text-[10px] text-slate-500">{meta.detail}</div></div>;
}

function AuditTable({ rows }: { rows: LineAlertBacktestRow[] }) {
  const gameRows = rows.filter((row) => GAME_LINE_ALERTS.has(row.alertType));
  return (
    <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-sm">
      <table className="min-w-[760px] w-full text-xs">
        <thead className="bg-slate-50 text-left text-[10px] uppercase tracking-wide text-slate-500">
          <tr><th className="px-3 py-3">Signal</th><th className="px-3 py-3 text-right">Tracked</th><th className="px-3 py-3 text-right">Avg CLV</th><th className="px-3 py-3 text-right">Beat close</th><th className="px-3 py-3 text-right">W-L-P</th><th className="px-3 py-3 text-right">Win rate</th></tr>
        </thead>
        <tbody>
          {gameRows.map((row) => (
            <tr key={row.alertType} className="border-t border-slate-100">
              <td className="px-3 py-3 font-semibold capitalize">{row.alertType.replaceAll("_", " ")}</td>
              <td className="px-3 py-3 text-right">{row.n}</td>
              <td className={`px-3 py-3 text-right font-semibold ${(row.avgClvPp ?? 0) > 0 ? "text-emerald-700" : (row.avgClvPp ?? 0) < 0 ? "text-red-700" : "text-slate-500"}`}>{pp(row.avgClvPp)}</td>
              <td className="px-3 py-3 text-right">{pct(row.beatClose, 0)}</td>
              <td className="px-3 py-3 text-right">{row.wins}-{row.losses}-{row.pushes}</td>
              <td className="px-3 py-3 text-right">{pct(row.winRate, 1)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {gameRows.length === 0 ? <div className="px-4 py-10 text-center text-sm text-slate-500">Sharp signals are being tracked; no settled MLB audit rows yet.</div> : null}
    </div>
  );
}

export default function MlbVegasClient({
  queryDate,
  evaluatedAt,
  matchups,
  health,
  lineMovement,
  lineAlerts,
  lineAlertBacktest,
  lineMovementHistory,
}: Props) {
  const router = useRouter();
  const [isRefreshing, startRefresh] = useTransition();
  const [nowIso, setNowIso] = useState(evaluatedAt);

  useEffect(() => {
    const id = window.setInterval(() => setNowIso(new Date().toISOString()), 30_000);
    return () => window.clearInterval(id);
  }, []);

  const movementByMatchup = useMemo(
    () => new Map(lineMovement.map((row) => [row.matchupId, row])),
    [lineMovement],
  );
  const alertsByMatchup = useMemo(() => {
    const grouped = new Map<number, LineAlertRow[]>();
    for (const alert of lineAlerts) {
      if (!GAME_LINE_ALERTS.has(alert.alertType)) continue;
      grouped.set(alert.matchupId, [...(grouped.get(alert.matchupId) ?? []), alert]);
    }
    return grouped;
  }, [lineAlerts]);

  const rows: BoardRow[] = useMemo(() => matchups.map((matchup) => {
    const movement = movementByMatchup.get(matchup.matchupId) ?? null;
    // The current isotonic layer can collapse sparse probability regions to
    // 0.1%/99.9%. Until SCRUM-29 replaces the point-in-time training artifact,
    // compare movement with the better-behaved raw market-anchored logistic.
    const modelHome = matchup.moneylinePrediction ?? matchup.ourProbHome;
    const signal = movement ? buildMlbMovementSignal({
      openHomeProbability: movement.openProb,
      currentHomeProbability: movement.closeProb,
      modelHomeProbability: modelHome,
      homeTeam: matchup.homeAbbrev,
      awayTeam: matchup.awayAbbrev,
    }) : null;
    const modelSuppressed = modelHome != null && signal?.movementSide != null && signal.modelProbability == null;
    return { matchup, movement, signal, alerts: alertsByMatchup.get(matchup.matchupId) ?? [], modelSuppressed };
  }).sort((a, b) => {
    const signalRank: Record<MlbCombinedSignal, number> = { strong_confirmation: 0, contrarian: 1, market_only: 2, quiet: 3 };
    const rank = (row: BoardRow) => signalRank[row.signal?.combinedSignal ?? "quiet"];
    return rank(a) - rank(b)
      || (b.signal?.movementPp ?? 0) - (a.signal?.movementPp ?? 0)
      || (Date.parse(a.matchup.commenceTime ?? "") || Infinity) - (Date.parse(b.matchup.commenceTime ?? "") || Infinity);
  }), [matchups, movementByMatchup, alertsByMatchup]);

  const confirmationCount = rows.filter((row) => row.signal?.combinedSignal === "strong_confirmation").length;
  const contrarianCount = rows.filter((row) => row.signal?.combinedSignal === "contrarian").length;
  const marketOnlyCount = rows.filter((row) => row.signal?.combinedSignal === "market_only").length;
  const sharpCount = rows.filter((row) => row.alerts.length > 0).length;
  const latestCapture = lineMovement.map((row) => Date.parse(row.closeCapturedAt)).filter(Number.isFinite).sort((a, b) => b - a)[0];
  const latestCaptureIso = latestCapture ? new Date(latestCapture).toISOString() : null;
  const latestAge = captureAge(latestCaptureIso, nowIso);
  const navigateDate = (date: string) => router.push(`/vegas?sport=mlb&date=${date}`);
  const reload = () => startRefresh(() => router.refresh());

  return (
    <div className="mx-auto max-w-7xl space-y-5 p-4 sm:p-6">
      <header className="rounded-2xl border border-slate-200 bg-gradient-to-br from-slate-950 via-slate-900 to-blue-950 p-5 text-white shadow-lg sm:p-6">
        <div className="flex flex-wrap items-start justify-between gap-5">
          <div>
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.18em] text-blue-200"><Target className="h-4 w-4" aria-hidden="true" /> Vegas Analysis — MLB</div>
            <h1 className="mt-2 text-2xl font-black tracking-tight sm:text-3xl">MLB Line Movement</h1>
            <p className="mt-2 max-w-3xl text-sm text-slate-300">Compare market movement with our model edge, then use the combined signal to find confirmation, disagreement, or market-only action.</p>
          </div>
          <div className="rounded-xl border border-white/15 bg-white/5 px-4 py-3 text-right">
            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Eastern time</div>
            <div className="mt-1 text-sm font-bold tabular-nums">{fmtClock(nowIso)}</div>
          </div>
        </div>

        <div className="mt-5 flex flex-wrap items-center gap-2">
          <button type="button" onClick={() => navigateDate(shiftDate(queryDate, -1))} className="inline-flex h-11 w-11 items-center justify-center rounded-lg border border-white/20 bg-white/10 hover:bg-white/15" aria-label="Previous date"><ChevronLeft className="h-5 w-5" /></button>
          <label className="flex min-h-11 items-center gap-2 rounded-lg border border-white/20 bg-white/10 px-3 text-xs font-semibold"><CalendarDays className="h-4 w-4" aria-hidden="true" /><input type="date" value={queryDate} onChange={(event) => navigateDate(event.target.value)} className="bg-transparent text-sm font-semibold text-white [color-scheme:dark]" /></label>
          <button type="button" onClick={() => navigateDate(shiftDate(queryDate, 1))} className="inline-flex h-11 w-11 items-center justify-center rounded-lg border border-white/20 bg-white/10 hover:bg-white/15" aria-label="Next date"><ChevronRight className="h-5 w-5" /></button>
          <button type="button" onClick={reload} disabled={isRefreshing} className="ml-auto inline-flex min-h-11 items-center gap-2 rounded-lg bg-blue-500 px-4 py-2 text-xs font-bold text-white hover:bg-blue-400 disabled:opacity-60"><RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} aria-hidden="true" />{isRefreshing ? "Checking" : "Check latest capture"}</button>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-300">
          <span>30-minute capture cadence</span>
          <span>Latest capture: <strong className="text-white">{fmtEt(latestCaptureIso, true)}</strong></span>
          <span className={latestAge.className}>{latestAge.text}</span>
        </div>
      </header>

      {health.filter((issue) => issue.severity === "error").length > 0 ? (
        <section className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-950">
          <div className="flex items-start gap-2"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" /><div><strong>Some model inputs are incomplete.</strong><div className="mt-1 text-xs">Movement still comes directly from the odds capture trail. The model column remains labeled market-anchored.</div></div></div>
        </section>
      ) : null}

      <section className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4 shadow-sm"><div className="text-[10px] font-bold uppercase tracking-wide text-emerald-800">Strong confirmation</div><div className="mt-1 text-2xl font-black text-emerald-950">{confirmationCount}</div><div className="mt-1 text-xs text-emerald-800">Movement and model agree</div></div>
        <div className="rounded-xl border border-red-200 bg-red-50 p-4 shadow-sm"><div className="text-[10px] font-bold uppercase tracking-wide text-red-800">Contrarian</div><div className="mt-1 text-2xl font-black text-red-950">{contrarianCount}</div><div className="mt-1 text-xs text-red-800">Movement and model disagree</div></div>
        <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 shadow-sm"><div className="text-[10px] font-bold uppercase tracking-wide text-amber-800">Market only</div><div className="mt-1 text-2xl font-black text-amber-950">{marketOnlyCount}</div><div className="mt-1 text-xs text-amber-800">Neutral or unavailable model</div></div>
        <div className="rounded-xl border border-violet-200 bg-violet-50 p-4 shadow-sm"><div className="text-[10px] font-bold uppercase tracking-wide text-violet-800">Sharp signals</div><div className="mt-1 text-2xl font-black text-violet-950">{sharpCount}</div><div className="mt-1 text-xs text-violet-800">Pinnacle, steam, walking, or DK value</div></div>
      </section>

      <section>
        <div className="mb-3">
          <h2 className="text-lg font-bold text-slate-950">Today’s movement board</h2>
          <p className="mt-1 text-sm text-slate-600">Movement measures open-to-current market change. Model edge measures model probability minus the current vig-free market probability. Both use percentage points.</p>
        </div>
        <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-sm">
          <table className="min-w-[1320px] w-full text-xs">
            <thead className="bg-slate-50 text-left text-[10px] uppercase tracking-wide text-slate-500">
              <tr><th className="px-3 py-3">Start</th><th className="px-3 py-3">Game</th><th className="px-3 py-3">Moved toward</th><th className="px-3 py-3">Open → current</th><th className="px-3 py-3 text-right">Movement</th><th className="px-3 py-3">Sharp signal</th><th className="px-3 py-3 text-right">Our model</th><th className="px-3 py-3 text-right">Model edge</th><th className="px-3 py-3">Combined signal</th><th className="px-3 py-3">Updated</th></tr>
            </thead>
            <tbody>
              {rows.map(({ matchup, movement, signal, alerts, modelSuppressed }) => {
                const age = captureAge(movement?.closeCapturedAt ?? null, nowIso);
                const Icon = signal?.movementSide === "home" ? TrendingUp : signal?.movementSide === "away" ? TrendingDown : Activity;
                const diagnostic = getModelDiagnostic(matchup, signal);
                return (
                  <tr key={matchup.matchupId} className="border-t border-slate-100 align-top hover:bg-slate-50/80">
                    <td className="whitespace-nowrap px-3 py-3 text-slate-600">{fmtEt(matchup.commenceTime)}</td>
                    <td className="px-3 py-3"><div className="font-bold text-slate-950">{matchup.awayAbbrev} @ {matchup.homeAbbrev}</div><div className="mt-0.5 text-[10px] text-slate-500">{matchup.awaySpName && matchup.homeSpName ? `${matchup.awaySpName} vs ${matchup.homeSpName}` : "Starters incomplete"}</div></td>
                    <td className="px-3 py-3">{signal?.movementTeam ? <div className="flex items-center gap-1.5 font-bold text-slate-900"><Icon className="h-4 w-4 text-blue-700" />{signal.movementTeam}</div> : <span className="text-slate-400">No clear move</span>}</td>
                    <td className="whitespace-nowrap px-3 py-3 tabular-nums">{signal?.movementSide ? `${pct(signal.openProbability)} → ${pct(signal.currentProbability)}` : movement ? `${pct(movement.openProb)} → ${pct(movement.closeProb)}` : "Waiting for second capture"}</td>
                    <td className="px-3 py-3 text-right font-bold tabular-nums">{signal?.movementSide ? `+${signal.movementPp.toFixed(1)}pp` : "—"}</td>
                    <td className="max-w-[220px] px-3 py-3"><div className="flex flex-wrap gap-1">{alerts.map((alert) => <span key={`${alert.alertType}-${alert.side}`} className={`rounded-full border px-2 py-1 text-[10px] font-bold ${alertTone(alert.alertType)}`}>{alertLabel(alert, matchup)}</span>)}{alerts.length === 0 && movement && Math.abs(movement.pinGapPp ?? 0) >= 2 ? <span className="rounded-full border border-violet-200 bg-violet-100 px-2 py-1 text-[10px] font-bold text-violet-900">Pinnacle gap {pp(movement.pinGapPp)}</span> : null}{alerts.length === 0 && Math.abs(movement?.pinGapPp ?? 0) < 2 ? <span className="text-slate-400">None</span> : null}</div></td>
                    <td className="min-w-[300px] max-w-[340px] px-3 py-3 text-right">
                      <div className="font-bold tabular-nums">{modelSuppressed ? "Invalid" : pct(signal?.modelProbability ?? null)}</div>
                      {diagnostic ? (
                        <details className="mt-1 text-left text-[10px] text-amber-800">
                          <summary className="cursor-pointer font-bold underline decoration-dotted underline-offset-2">{diagnostic.check}</summary>
                          {diagnostic.driver ? <p className="mt-1 rounded-md bg-amber-50 p-2 leading-relaxed">{diagnostic.driver}</p> : null}
                        </details>
                      ) : <div className="mt-0.5 text-[10px] text-slate-500">Raw market-anchored</div>}
                    </td>
                    <td className={`px-3 py-3 text-right text-sm font-black tabular-nums ${(signal?.modelGapPp ?? 0) > 0.5 ? "text-emerald-700" : (signal?.modelGapPp ?? 0) < -0.5 ? "text-red-700" : "text-slate-500"}`}>{pp(signal?.modelGapPp ?? null)}</td>
                    <td className="min-w-[150px] px-3 py-3"><CombinedSignalBadge signal={signal?.combinedSignal ?? "quiet"} /></td>
                    <td className="px-3 py-3"><div>{fmtEt(movement?.closeCapturedAt ?? null)}</div><div className={`mt-0.5 text-[10px] font-semibold ${age.className}`}>{age.text}</div></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {rows.length === 0 ? <div className="px-4 py-14 text-center text-sm text-slate-500">No MLB games are scheduled for this date.</div> : null}
        </div>
      </section>

      <section>
        <div className="mb-3 flex items-end justify-between gap-3"><div><h2 className="text-lg font-bold text-slate-950">Sharp-signal tracking</h2><p className="mt-1 text-sm text-slate-600">Signals are frozen at first breach and graded against the final pregame close and game result. No notifications are included in this release.</p></div><CheckCircle2 className="h-5 w-5 text-slate-400" /></div>
        <AuditTable rows={lineAlertBacktest} />
      </section>

      <details className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <summary className="cursor-pointer font-bold text-slate-950">Recent open-to-close results ({lineMovementHistory.length})</summary>
        <div className="mt-4 overflow-x-auto">
          <table className="min-w-[760px] w-full text-xs"><thead className="text-left text-[10px] uppercase tracking-wide text-slate-500"><tr><th className="py-2">Date</th><th className="py-2">Game</th><th className="py-2 text-right">Open</th><th className="py-2 text-right">Close</th><th className="py-2">Moved toward</th><th className="py-2">Score</th><th className="py-2">Moved side won</th></tr></thead><tbody>{lineMovementHistory.slice(0, 50).map((row) => <tr key={`${row.matchupId}-${row.gameDate}`} className="border-t border-slate-100"><td className="py-2">{row.gameDate}</td><td className="py-2 font-semibold">{row.matchup}</td><td className="py-2 text-right">{pct(row.openProb)}</td><td className="py-2 text-right">{pct(row.closeProb)}</td><td className="py-2 capitalize">{row.movedToward ?? "quiet"}</td><td className="py-2">{row.score ?? "pending"}</td><td className="py-2">{row.movedSideWon == null ? "—" : row.movedSideWon ? "Yes" : "No"}</td></tr>)}</tbody></table>
        </div>
      </details>
    </div>
  );
}
