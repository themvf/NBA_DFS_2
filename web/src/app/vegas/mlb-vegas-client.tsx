"use client";

import {
  Activity,
  AlertTriangle,
  Bell,
  CalendarDays,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleSlash2,
  Clock3,
  Database,
  ExternalLink,
  Eye,
  Layers3,
  RefreshCw,
  ShieldCheck,
  SlidersHorizontal,
  Target,
  X,
  XCircle,
} from "lucide-react";
import { useEffect, useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type {
  LineAlertBacktestRow,
  LineAlertRow,
  LineMovementHistoryRow,
  MlbActionabilityEvidenceRow,
  MlbBetBacktestRow,
  MlbBetRow,
  MlbClvRow,
  MlbHealthIssue,
  MlbLineMovementRow,
  MlbMlBacktest,
  MlbTotalBacktest,
  MlbVegasCoverageStatus,
} from "@/db/queries";
import {
  MLB_BOOK_LABELS,
  MLB_DEFAULT_BOOKS,
  type MlbDecisionMarket,
  type MlbMarketDecision,
  type MlbPrimaryStatus,
} from "@/lib/mlb-vegas-decisions";
import type { MlbActionabilityDecision } from "@/lib/mlb-vegas-trust";

type Props = {
  queryDate: string;
  evaluatedAt: string;
  decisions: MlbMarketDecision[];
  trustDecisions: MlbActionabilityDecision[];
  actionabilityEvidence: MlbActionabilityEvidenceRow[];
  coverage: MlbVegasCoverageStatus | null;
  health: MlbHealthIssue[];
  totalBacktest: MlbTotalBacktest;
  moneylineBacktest: MlbMlBacktest;
  bets: MlbBetRow[];
  betBacktest: MlbBetBacktestRow[];
  clv: MlbClvRow[];
  lineMovement: MlbLineMovementRow[];
  lineAlerts: LineAlertRow[];
  lineAlertBacktest: LineAlertBacktestRow[];
  lineMovementHistory: LineMovementHistoryRow[];
};

type MainView = "board" | "parlay" | "evidence";
type MarketFilter = "all" | MlbDecisionMarket;
type StatusFilter = "all" | MlbPrimaryStatus;

const STATUS_ORDER: Record<MlbPrimaryStatus, number> = {
  take_now: 0,
  watch: 1,
  pass: 2,
  blocked: 3,
  closed: 4,
};

const STATUS_META: Record<MlbPrimaryStatus, {
  label: string;
  chip: string;
  border: string;
  panel: string;
  icon: typeof CheckCircle2;
}> = {
  take_now: {
    label: "TAKE NOW",
    chip: "border-emerald-300 bg-emerald-100 text-emerald-900",
    border: "border-emerald-300",
    panel: "bg-emerald-50/50",
    icon: CheckCircle2,
  },
  watch: {
    label: "WATCH",
    chip: "border-amber-300 bg-amber-100 text-amber-900",
    border: "border-amber-300",
    panel: "bg-amber-50/50",
    icon: Clock3,
  },
  pass: {
    label: "PASS",
    chip: "border-slate-300 bg-slate-100 text-slate-800",
    border: "border-slate-300",
    panel: "bg-slate-50/60",
    icon: CircleSlash2,
  },
  blocked: {
    label: "BLOCKED",
    chip: "border-red-300 bg-red-100 text-red-900",
    border: "border-red-300",
    panel: "bg-red-50/50",
    icon: XCircle,
  },
  closed: {
    label: "CLOSED",
    chip: "border-zinc-400 bg-zinc-200 text-zinc-900",
    border: "border-zinc-400",
    panel: "bg-zinc-100/70",
    icon: CircleSlash2,
  },
};

const BOOK_URLS: Record<string, string> = {
  draftkings: "https://sportsbook.draftkings.com/",
  fanduel: "https://sportsbook.fanduel.com/",
  betmgm: "https://sports.betmgm.com/",
  williamhill_us: "https://www.caesars.com/sportsbook-and-casino",
  fanatics: "https://sportsbook.fanatics.com/",
  betrivers: "https://www.betrivers.com/",
  espnbet: "https://espnbet.com/",
  hardrockbet: "https://www.hardrock.bet/",
};

const MY_BOOKS_STORAGE = "mlb-vegas-my-books-v1";
const PRICE_WATCH_STORAGE = "mlb-vegas-price-watches-v1";

function fmtAmerican(price: number | null): string {
  if (price == null) return "-";
  return price > 0 ? `+${price}` : String(price);
}

function fmtPct(value: number | null, digits = 1): string {
  return value == null ? "-" : `${(value * 100).toFixed(digits)}%`;
}

function fmtPp(value: number | null): string {
  return value == null ? "-" : `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)}pp`;
}

function fmtRoi(value: number | null): string {
  return value == null ? "-" : `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)}%`;
}

function fmtEt(value: string | null, includeDate = false): string {
  if (!value) return "Time unavailable";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "Time unavailable";
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    ...(includeDate ? { month: "short", day: "numeric" } : {}),
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(parsed);
}

function fmtEtClock(value: string): string {
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

function ageText(minutes: number | null): string {
  if (minutes == null) return "age unavailable";
  if (minutes < 1) return "under 1m old";
  if (minutes < 60) return `${Math.floor(minutes)}m old`;
  return `${Math.floor(minutes / 60)}h ${Math.floor(minutes % 60)}m old`;
}

function startText(value: string | null, nowIso: string): string {
  if (!value) return "Start unavailable";
  const start = Date.parse(value);
  const now = Date.parse(nowIso);
  if (!Number.isFinite(start) || !Number.isFinite(now)) return "Start unavailable";
  const minutes = Math.round((start - now) / 60_000);
  if (minutes <= 0) return fmtEt(value);
  if (minutes < 60) return `${fmtEt(value)} - ${minutes}m`;
  return `${fmtEt(value)} - ${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function shiftDate(date: string, days: number): string {
  const parsed = new Date(`${date}T12:00:00Z`);
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return parsed.toISOString().slice(0, 10);
}

function priceWatchKey(decision: MlbMarketDecision): string {
  return [decision.matchupId, decision.market, decision.side ?? "none"].join(":");
}

function decisionGroupKey(decision: MlbMarketDecision): string {
  return `${decision.matchupId}:${decision.market}`;
}

function compareDecisionQuality(a: MlbMarketDecision, b: MlbMarketDecision): number {
  return STATUS_ORDER[a.primaryStatus] - STATUS_ORDER[b.primaryStatus]
    || (b.estimatedRoi ?? -Infinity) - (a.estimatedRoi ?? -Infinity)
    || (a.quoteAgeMinutes ?? Infinity) - (b.quoteAgeMinutes ?? Infinity)
    || (a.bookLabel ?? "").localeCompare(b.bookLabel ?? "");
}

function StatusBadge({ status, compact = false }: { status: MlbPrimaryStatus; compact?: boolean }) {
  const meta = STATUS_META[status];
  const Icon = meta.icon;
  return (
    <span className={`inline-flex items-center gap-1 rounded-full border font-bold tracking-wide ${meta.chip} ${compact ? "px-2 py-0.5 text-[10px]" : "px-2.5 py-1 text-xs"}`}>
      <Icon className={compact ? "h-3 w-3" : "h-3.5 w-3.5"} aria-hidden="true" />
      {meta.label}
    </span>
  );
}

function SmallPill({ children, tone = "slate" }: { children: React.ReactNode; tone?: "slate" | "blue" | "amber" | "red" | "green" }) {
  const classes = {
    slate: "border-slate-200 bg-slate-50 text-slate-700",
    blue: "border-blue-200 bg-blue-50 text-blue-800",
    amber: "border-amber-200 bg-amber-50 text-amber-900",
    red: "border-red-200 bg-red-50 text-red-800",
    green: "border-emerald-200 bg-emerald-50 text-emerald-800",
  }[tone];
  return <span className={`rounded-full border px-2 py-0.5 text-[10px] font-medium ${classes}`}>{children}</span>;
}

function Metric({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="min-w-0 rounded-lg border border-slate-200 bg-white px-3 py-2">
      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-0.5 truncate text-sm font-bold tabular-nums text-slate-950">{value}</div>
      {detail ? <div className="mt-0.5 truncate text-[10px] text-slate-500">{detail}</div> : null}
    </div>
  );
}

function ExactQuote({ decision }: { decision: MlbMarketDecision }) {
  if (!decision.selection || !decision.bookLabel || decision.price == null) {
    return <span className="text-slate-500">Exact quote unavailable</span>;
  }
  return (
    <span className="font-semibold text-slate-950">
      {decision.selection} at {decision.bookLabel} {fmtAmerican(decision.price)}
    </span>
  );
}

function DecisionCard({
  decision,
  nowIso,
  tracked,
  onDetails,
  onTrack,
  onAddParlay,
}: {
  decision: MlbMarketDecision;
  nowIso: string;
  tracked: boolean;
  onDetails: () => void;
  onTrack: () => void;
  onAddParlay: () => void;
}) {
  const meta = STATUS_META[decision.primaryStatus];
  return (
    <article className={`rounded-xl border ${meta.border} ${meta.panel} p-4 shadow-sm`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge status={decision.primaryStatus} />
            <span className="text-sm font-bold text-slate-950">{decision.matchup}</span>
            {decision.doubleheaderGameNumber ? <SmallPill>Game {decision.doubleheaderGameNumber}</SmallPill> : null}
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-600">
            <span>{decision.market === "moneyline" ? "Moneyline" : "Full-game total"}</span>
            <span>{startText(decision.commenceTime, nowIso)}</span>
            <span>{decision.homeSpName && decision.awaySpName ? `${decision.awaySpName} vs ${decision.homeSpName}` : "Starter detail incomplete"}</span>
          </div>
        </div>
        <div className="text-right text-sm">
          <ExactQuote decision={decision} />
          <div className="mt-0.5 text-[11px] text-slate-500">
            {ageText(decision.quoteAgeMinutes)}
            {decision.validUntil ? ` - decision expires ${fmtEt(decision.validUntil)}` : ""}
          </div>
        </div>
      </div>

      <p className="mt-3 text-sm font-semibold text-slate-900">{decision.primaryReason}</p>
      {decision.nextAction ? <p className="mt-1 text-xs text-slate-600">Next: {decision.nextAction}</p> : null}

      <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
        <Metric
          label={decision.market === "total" ? "Model mean" : decision.probabilityKind === "calibrated" ? "Model probability" : "Raw model estimate"}
          value={decision.market === "total" ? decision.modelTotal?.toFixed(1) ?? "-" : fmtPct(decision.modelProbability)}
        />
        <Metric
          label={decision.market === "total" ? "Exact line" : "Fixed reference"}
          value={decision.market === "total" ? decision.line?.toFixed(1) ?? "-" : fmtPct(decision.referenceProbability)}
        />
        <Metric label="Break-even" value={fmtPct(decision.offeredBreakEven)} />
        <Metric label="Price margin" value={fmtPp(decision.priceMargin)} />
        <Metric label="Modeled ROI" value={fmtRoi(decision.estimatedRoi)} />
        <Metric
          label="Take to"
          value={decision.targetAmericanPrice == null ? "Unavailable" : `${fmtAmerican(decision.targetAmericanPrice)} or better`}
        />
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-1.5">
        <SmallPill tone="blue">{decision.relationshipLabel}</SmallPill>
        <SmallPill tone={decision.priceSupport === "passes" ? "green" : decision.priceSupport === "unavailable" ? "red" : "amber"}>
          Price: {decision.priceSupport.replaceAll("_", " ")}
        </SmallPill>
        <SmallPill tone={decision.fragility === "blocked" ? "red" : decision.fragility === "high" ? "amber" : "slate"}>
          Fragility: {decision.fragility}
        </SmallPill>
        <SmallPill>Policy {decision.policyVersion}</SmallPill>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        <button type="button" onClick={onDetails} className="inline-flex min-h-10 items-center gap-1.5 rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs font-semibold text-slate-800 hover:bg-slate-50">
          <Eye className="h-4 w-4" aria-hidden="true" /> Details
        </button>
        {decision.targetAmericanPrice != null && decision.primaryStatus !== "closed" ? (
          <button type="button" onClick={onTrack} className={`inline-flex min-h-10 items-center gap-1.5 rounded-lg border px-3 py-2 text-xs font-semibold ${tracked ? "border-amber-300 bg-amber-100 text-amber-900" : "border-slate-300 bg-white text-slate-800 hover:bg-slate-50"}`}>
            <Bell className="h-4 w-4" aria-hidden="true" /> {tracked ? "Tracking target" : "Track target"}
          </button>
        ) : null}
        {decision.parlayEligible ? (
          <button type="button" onClick={onAddParlay} className="inline-flex min-h-10 items-center gap-1.5 rounded-lg border border-indigo-300 bg-indigo-50 px-3 py-2 text-xs font-semibold text-indigo-800 hover:bg-indigo-100">
            <Layers3 className="h-4 w-4" aria-hidden="true" /> Add to parlay
          </button>
        ) : null}
        {decision.primaryStatus === "take_now" && decision.bookKey && BOOK_URLS[decision.bookKey] ? (
          <a href={BOOK_URLS[decision.bookKey]} target="_blank" rel="noreferrer" className="inline-flex min-h-10 items-center gap-1.5 rounded-lg bg-emerald-700 px-3 py-2 text-xs font-bold text-white hover:bg-emerald-800">
            Verify at {decision.bookLabel} <ExternalLink className="h-4 w-4" aria-hidden="true" />
          </a>
        ) : null}
      </div>
    </article>
  );
}

function DecisionTable({
  decisions,
  nowIso,
  onDetails,
}: {
  decisions: MlbMarketDecision[];
  nowIso: string;
  onDetails: (decision: MlbMarketDecision) => void;
}) {
  return (
    <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-sm">
      <table className="min-w-[1180px] w-full border-collapse text-xs">
        <thead className="bg-slate-50 text-left text-[10px] uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-3 py-3">Start</th>
            <th className="px-3 py-3">Matchup</th>
            <th className="px-3 py-3">Market</th>
            <th className="px-3 py-3">Answer</th>
            <th className="px-3 py-3">Exact quote</th>
            <th className="px-3 py-3 text-right">Model</th>
            <th className="px-3 py-3 text-right">Reference</th>
            <th className="px-3 py-3 text-right">Break-even</th>
            <th className="px-3 py-3 text-right">ROI</th>
            <th className="px-3 py-3">Take to / stability</th>
            <th className="px-3 py-3">Why</th>
            <th className="px-3 py-3"><span className="sr-only">Open</span></th>
          </tr>
        </thead>
        <tbody>
          {decisions.map((decision) => (
            <tr key={decision.decisionId} className="border-t border-slate-100 align-top hover:bg-slate-50/80">
              <td className="whitespace-nowrap px-3 py-3 text-slate-700">{startText(decision.commenceTime, nowIso)}</td>
              <td className="px-3 py-3">
                <div className="font-bold text-slate-900">{decision.matchup}</div>
                <div className="mt-0.5 text-[10px] text-slate-500">
                  {decision.doubleheaderGameNumber ? `Game ${decision.doubleheaderGameNumber} - ` : ""}
                  {decision.awaySpName && decision.homeSpName ? `${decision.awaySpName} vs ${decision.homeSpName}` : "Starters incomplete"}
                </div>
              </td>
              <td className="px-3 py-3 capitalize text-slate-700">{decision.market}</td>
              <td className="px-3 py-3"><StatusBadge status={decision.primaryStatus} compact /></td>
              <td className="px-3 py-3">
                <ExactQuote decision={decision} />
                <div className="mt-0.5 text-[10px] text-slate-500">{ageText(decision.quoteAgeMinutes)}</div>
              </td>
              <td className="px-3 py-3 text-right font-semibold tabular-nums">
                {decision.market === "total" ? decision.modelTotal?.toFixed(1) ?? "-" : fmtPct(decision.modelProbability)}
              </td>
              <td className="px-3 py-3 text-right tabular-nums">
                {decision.market === "total" ? decision.line?.toFixed(1) ?? "-" : fmtPct(decision.referenceProbability)}
              </td>
              <td className="px-3 py-3 text-right tabular-nums">{fmtPct(decision.offeredBreakEven)}</td>
              <td className={`px-3 py-3 text-right font-bold tabular-nums ${(decision.estimatedRoi ?? 0) > 0 ? "text-emerald-700" : (decision.estimatedRoi ?? 0) < 0 ? "text-red-700" : "text-slate-500"}`}>
                {fmtRoi(decision.estimatedRoi)}
              </td>
              <td className="px-3 py-3">
                <div className="font-medium text-slate-800">{decision.targetAmericanPrice == null ? "Target unavailable" : `${fmtAmerican(decision.targetAmericanPrice)} or better`}</div>
                <div className="mt-0.5 text-[10px] text-slate-500">Stability {fmtPct(decision.resamplePositiveRate, 0)}</div>
              </td>
              <td className="max-w-[260px] px-3 py-3 text-slate-700">
                <div className="line-clamp-2">{decision.primaryReason}</div>
                <div className="mt-1 text-[10px] font-medium text-blue-700">{decision.relationshipLabel}</div>
              </td>
              <td className="px-3 py-3 text-right">
                <button type="button" onClick={() => onDetails(decision)} className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-300 bg-white text-slate-700 hover:bg-slate-100" aria-label={`Open details for ${decision.matchup} ${decision.market}`}>
                  <ChevronRight className="h-4 w-4" aria-hidden="true" />
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {decisions.length === 0 ? <div className="px-4 py-10 text-center text-sm text-slate-500">No decisions match the current filters.</div> : null}
    </div>
  );
}

function DecisionDrawer({
  decision,
  priceShop,
  tracked,
  onClose,
  onTrack,
}: {
  decision: MlbMarketDecision;
  priceShop: MlbMarketDecision[];
  tracked: boolean;
  onClose: () => void;
  onTrack: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/40" role="dialog" aria-modal="true" aria-label={`${decision.matchup} ${decision.market} decision details`}>
      <button type="button" className="absolute inset-0 cursor-default" onClick={onClose} aria-label="Close details" />
      <div className="relative h-full w-full max-w-2xl overflow-y-auto bg-slate-50 shadow-2xl">
        <div className="sticky top-0 z-10 border-b border-slate-200 bg-white/95 px-5 py-4 backdrop-blur">
          <div className="flex items-start justify-between gap-3">
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <StatusBadge status={decision.primaryStatus} />
                <span className="text-sm font-bold text-slate-950">{decision.matchup}</span>
              </div>
              <div className="mt-2 text-lg"><ExactQuote decision={decision} /></div>
              <div className="mt-1 text-xs text-slate-500">
                Observed {fmtEt(decision.oddsCapturedAt, true)} - {ageText(decision.quoteAgeMinutes)}
              </div>
            </div>
            <button type="button" onClick={onClose} className="inline-flex h-11 w-11 items-center justify-center rounded-lg border border-slate-300 bg-white text-slate-700 hover:bg-slate-100" aria-label="Close details">
              <X className="h-5 w-5" aria-hidden="true" />
            </button>
          </div>
        </div>

        <div className="space-y-5 p-5">
          <section className={`rounded-xl border p-4 ${STATUS_META[decision.primaryStatus].border} ${STATUS_META[decision.primaryStatus].panel}`}>
            <h2 className="font-bold text-slate-950">{decision.headline}</h2>
            <p className="mt-2 text-sm text-slate-800">{decision.primaryReason}</p>
            {decision.nextAction ? <p className="mt-2 text-xs font-medium text-slate-600">Next: {decision.nextAction}</p> : null}
            <div className="mt-4 flex flex-wrap gap-2">
              {decision.targetAmericanPrice != null && decision.primaryStatus !== "closed" ? (
                <button type="button" onClick={onTrack} className={`inline-flex min-h-11 items-center gap-2 rounded-lg border px-3 py-2 text-xs font-bold ${tracked ? "border-amber-300 bg-amber-100 text-amber-900" : "border-slate-300 bg-white text-slate-800"}`}>
                  <Bell className="h-4 w-4" aria-hidden="true" /> {tracked ? "Tracking target" : `Track ${fmtAmerican(decision.targetAmericanPrice)} or better`}
                </button>
              ) : null}
              {decision.primaryStatus === "take_now" && decision.bookKey && BOOK_URLS[decision.bookKey] ? (
                <a href={BOOK_URLS[decision.bookKey]} target="_blank" rel="noreferrer" className="inline-flex min-h-11 items-center gap-2 rounded-lg bg-emerald-700 px-3 py-2 text-xs font-bold text-white">
                  Verify at {decision.bookLabel} <ExternalLink className="h-4 w-4" aria-hidden="true" />
                </a>
              ) : null}
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="font-bold text-slate-950">Model vs market</h2>
            <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4">
              <Metric label={decision.market === "total" ? "Model mean" : "Model probability"} value={decision.market === "total" ? decision.modelTotal?.toFixed(1) ?? "-" : fmtPct(decision.modelProbability)} detail={decision.probabilityKind === "raw" ? "Raw output" : decision.probabilityKind} />
              <Metric label={decision.market === "total" ? "Offered line" : "Fixed reference"} value={decision.market === "total" ? decision.line?.toFixed(1) ?? "-" : fmtPct(decision.referenceProbability)} />
              <Metric label="Offered break-even" value={fmtPct(decision.offeredBreakEven)} />
              <Metric label="Modeled ROI" value={fmtRoi(decision.estimatedRoi)} />
              <Metric label="Price margin" value={fmtPp(decision.priceMargin)} />
              <Metric label="Positive resamples" value={fmtPct(decision.resamplePositiveRate, 0)} />
              <Metric label="Probability range" value={decision.uncertaintyLow == null ? "Unavailable" : `${fmtPct(decision.uncertaintyLow)} to ${fmtPct(decision.uncertaintyHigh)}`} />
              <Metric label="Take to" value={decision.targetAmericanPrice == null ? "Unavailable" : `${fmtAmerican(decision.targetAmericanPrice)} or better`} />
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h2 className="font-bold text-slate-950">Price shop</h2>
                <p className="mt-0.5 text-xs text-slate-500">Each row is evaluated at its own exact book, line, paired price, and update time.</p>
              </div>
              <Target className="h-5 w-5 text-slate-400" aria-hidden="true" />
            </div>
            <div className="mt-3 overflow-x-auto">
              <table className="w-full min-w-[560px] text-xs">
                <thead className="text-left text-[10px] uppercase tracking-wide text-slate-500">
                  <tr><th className="py-2">Book</th><th className="py-2">Selection</th><th className="py-2 text-right">Price</th><th className="py-2 text-right">Age</th><th className="py-2 text-right">ROI</th><th className="py-2 text-right">Answer</th></tr>
                </thead>
                <tbody>
                  {priceShop.map((row) => (
                    <tr key={row.decisionId} className="border-t border-slate-100">
                      <td className="py-2 font-semibold">{row.bookLabel ?? "No book"}</td>
                      <td className="py-2">{row.selection ?? "-"}</td>
                      <td className="py-2 text-right tabular-nums">{fmtAmerican(row.price)}</td>
                      <td className="py-2 text-right">{ageText(row.quoteAgeMinutes)}</td>
                      <td className="py-2 text-right tabular-nums">{fmtRoi(row.estimatedRoi)}</td>
                      <td className="py-2 text-right"><StatusBadge status={row.primaryStatus} compact /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="grid gap-4 sm:grid-cols-2">
            <div className="rounded-xl border border-slate-200 bg-white p-4">
              <h2 className="font-bold text-slate-950">Why this answer</h2>
              <ul className="mt-3 space-y-3 text-xs">
                {decision.reasons.length > 0 ? decision.reasons.map((reason, index) => (
                  <li key={`${reason.label}-${index}`} className="flex gap-2">
                    <span className={`mt-1.5 h-2 w-2 shrink-0 rounded-full ${reason.direction === "against" ? "bg-rose-500" : reason.direction === "for" ? "bg-emerald-500" : "bg-blue-500"}`} />
                    <span><strong className="block text-slate-800">{reason.label}</strong><span className="text-slate-500">{reason.detail}</span></span>
                  </li>
                )) : <li className="text-slate-500">No fitted-model associations were stored for this snapshot.</li>}
              </ul>
            </div>
            <div className="rounded-xl border border-slate-200 bg-white p-4">
              <h2 className="font-bold text-slate-950">Against this bet</h2>
              <ul className="mt-3 space-y-2 text-xs text-slate-700">
                {decision.fragilityReasons.length > 0 ? decision.fragilityReasons.map((reason) => (
                  <li key={reason} className="flex gap-2"><AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600" aria-hidden="true" /><span>{reason}</span></li>
                )) : <li>No material counter-signal is recorded for this snapshot.</li>}
              </ul>
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="font-bold text-slate-950">Snapshot identity</h2>
            <dl className="mt-3 grid gap-x-6 gap-y-3 text-xs sm:grid-cols-2">
              {[
                ["Event revision", decision.eventRevisionId],
                ["Observed quote snapshot", decision.observedOddsSnapshotId?.toString() ?? "Missing"],
                ["Prediction snapshot", decision.predictionSnapshotId?.toString() ?? "Missing"],
                ["Reference quote snapshot", decision.referenceOddsSnapshotId?.toString() ?? "Missing"],
                ["Model version", decision.modelVersion ?? "Missing"],
                ["Canonical horizon", decision.canonicalHorizon ?? "Missing"],
                ["Trust evaluation", decision.trustEvaluationId ?? "Missing"],
                ["Policy", decision.policyVersion],
              ].map(([label, value]) => (
                <div key={label}><dt className="text-slate-500">{label}</dt><dd className="mt-0.5 break-all font-mono text-[11px] text-slate-800">{value}</dd></div>
              ))}
            </dl>
          </section>
        </div>
      </div>
    </div>
  );
}

function ParlayView({
  decisions,
  selectedBooks,
  legs,
  setLegs,
}: {
  decisions: MlbMarketDecision[];
  selectedBooks: string[];
  legs: string[];
  setLegs: React.Dispatch<React.SetStateAction<string[]>>;
}) {
  const availableBooks = selectedBooks.filter((book) => decisions.some((decision) => decision.bookKey === book));
  const [book, setBook] = useState(availableBooks[0] ?? "");
  const effectiveBook = availableBooks.includes(book) ? book : availableBooks[0] ?? "";
  const candidates = decisions
    .filter((decision) => decision.market === "moneyline" && decision.bookKey === effectiveBook && decision.price != null && decision.price < 0)
    .sort(compareDecisionQuality);
  const selected = legs.map((id) => decisions.find((decision) => decision.decisionId === id)).filter((decision): decision is MlbMarketDecision => Boolean(decision));
  const payout = selected.length > 0 ? selected.reduce((product, decision) => product * (decision.decimalPrice ?? 1), 1) : null;
  const modelJoint = selected.length > 0 && selected.every((decision) => decision.modelProbability != null)
    ? selected.reduce((product, decision) => product * (decision.modelProbability ?? 1), 1)
    : null;

  const toggleLeg = (decision: MlbMarketDecision) => {
    setLegs((current) => current.includes(decision.decisionId)
      ? current.filter((id) => id !== decision.decisionId)
      : current.length >= 3
        ? current
        : [...current, decision.decisionId]);
  };

  return (
    <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_360px]">
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h2 className="text-lg font-bold text-slate-950">Favorites parlay builder</h2>
            <p className="mt-1 text-sm text-slate-600">Choose one book first. Only individually qualified favorites can become legs.</p>
          </div>
          <label className="text-xs font-semibold text-slate-700">
            Sportsbook
            <select value={effectiveBook} onChange={(event) => { setBook(event.target.value); setLegs([]); }} className="ml-2 rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm">
              {availableBooks.map((key) => <option key={key} value={key}>{MLB_BOOK_LABELS[key] ?? key}</option>)}
            </select>
          </label>
        </div>

        <div className="mt-5 space-y-3">
          {candidates.length > 0 ? candidates.map((decision) => {
            const selectedLeg = legs.includes(decision.decisionId);
            return (
              <div key={decision.decisionId} className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-slate-200 bg-slate-50 p-4">
                <div>
                  <div className="flex flex-wrap items-center gap-2"><StatusBadge status={decision.primaryStatus} compact /><strong>{decision.selection} {fmtAmerican(decision.price)}</strong><span className="text-slate-500">{decision.matchup}</span></div>
                  <div className="mt-1 text-xs text-slate-600">{decision.primaryReason}</div>
                  <div className="mt-2 flex flex-wrap gap-1.5"><SmallPill tone="blue">{decision.relationshipLabel}</SmallPill><SmallPill>ROI {fmtRoi(decision.estimatedRoi)}</SmallPill><SmallPill>Stability {fmtPct(decision.resamplePositiveRate, 0)}</SmallPill></div>
                </div>
                <button type="button" disabled={!decision.parlayEligible} onClick={() => toggleLeg(decision)} className={`min-h-11 rounded-lg px-4 py-2 text-xs font-bold ${decision.parlayEligible ? selectedLeg ? "bg-indigo-700 text-white" : "border border-indigo-300 bg-white text-indigo-800" : "cursor-not-allowed border border-slate-200 bg-slate-100 text-slate-400"}`}>
                  {selectedLeg ? "Remove leg" : decision.parlayEligible ? "Add leg" : "Leg blocked"}
                </button>
              </div>
            );
          }) : <div className="rounded-xl border border-dashed border-slate-300 px-4 py-10 text-center text-sm text-slate-500">No favorite moneylines are available at the selected book.</div>}
        </div>
      </section>

      <aside className="h-fit rounded-xl border border-slate-300 bg-slate-950 p-5 text-white shadow-lg lg:sticky lg:top-4">
        <div className="flex items-center justify-between gap-3"><h2 className="font-bold">Current combination</h2><SmallPill tone="red">BLOCKED</SmallPill></div>
        <p className="mt-2 text-xs text-slate-300">Parlays require their own promoted joint-probability and dependence policy. Individual-leg approval cannot promote the combination.</p>
        <div className="mt-4 space-y-2">
          {selected.length > 0 ? selected.map((decision) => (
            <div key={decision.decisionId} className="rounded-lg border border-slate-700 bg-slate-900 p-3 text-xs">
              <div className="font-semibold">{decision.selection} {fmtAmerican(decision.price)}</div>
              <div className="mt-0.5 text-slate-400">{decision.matchup} - {decision.bookLabel}</div>
            </div>
          )) : <div className="rounded-lg border border-dashed border-slate-700 px-3 py-8 text-center text-xs text-slate-400">No eligible legs selected.</div>}
        </div>
        <dl className="mt-4 space-y-2 border-t border-slate-700 pt-4 text-xs">
          <div className="flex justify-between gap-3"><dt className="text-slate-400">Observed multiplied payout</dt><dd className="font-semibold">{payout == null ? "-" : `${payout.toFixed(2)} decimal`}</dd></div>
          <div className="flex justify-between gap-3"><dt className="text-slate-400">Model joint (independence)</dt><dd className="font-semibold">{fmtPct(modelJoint)}</dd></div>
          <div className="flex justify-between gap-3"><dt className="text-slate-400">Direct sportsbook quote</dt><dd className="font-semibold">Not entered</dd></div>
          <div className="flex justify-between gap-3"><dt className="text-slate-400">Fragility</dt><dd className="font-semibold text-amber-300">High - dependence open</dd></div>
        </dl>
        <button type="button" disabled className="mt-5 w-full cursor-not-allowed rounded-lg bg-slate-700 px-4 py-3 text-sm font-bold text-slate-400">Verify parlay after policy promotion</button>
      </aside>
    </div>
  );
}

function EvidenceView(props: Pick<Props,
  "trustDecisions" | "actionabilityEvidence" | "coverage" | "health" | "totalBacktest" | "moneylineBacktest" | "bets" | "betBacktest" | "clv" | "lineMovement" | "lineAlerts" | "lineAlertBacktest" | "lineMovementHistory"
>) {
  const { trustDecisions, actionabilityEvidence, coverage, health, totalBacktest, moneylineBacktest, bets, betBacktest, clv, lineMovement, lineAlerts, lineAlertBacktest, lineMovementHistory } = props;
  return (
    <div className="space-y-5">
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <div className="flex items-start justify-between gap-3"><div><h2 className="text-lg font-bold text-slate-950">Trust gates</h2><p className="mt-1 text-sm text-slate-600">Promotion is evaluated separately for each market. Every failed gate shows its count and remedy.</p></div><ShieldCheck className="h-6 w-6 text-slate-400" aria-hidden="true" /></div>
        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          {trustDecisions.map((decision) => (
            <div key={decision.market} className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-2"><h3 className="font-bold capitalize text-slate-950">{decision.market}</h3><span className={`rounded-full px-2 py-1 text-[10px] font-bold uppercase ${decision.state === "actionable" ? "bg-emerald-100 text-emerald-800" : decision.state === "blocked" ? "bg-red-100 text-red-800" : "bg-amber-100 text-amber-900"}`}>{decision.state}</span></div>
              <div className="mt-1 text-xs text-slate-600">{decision.passed}/{decision.total} gates pass - {decision.summary}</div>
              <ul className="mt-3 space-y-2">
                {decision.gates.map((gate) => (
                  <li key={gate.key} className="flex gap-2 text-xs"><span className={gate.passed ? "text-emerald-700" : gate.blocking ? "text-red-700" : "text-amber-700"}>{gate.passed ? "PASS" : "OPEN"}</span><span><strong className="block text-slate-800">{gate.label}</strong><span className="text-slate-500">{gate.detail}</span></span></li>
                ))}
                <li className="flex gap-2 text-xs"><span className="text-amber-700">OPEN</span><span><strong className="block text-slate-800">Canonical horizon evidence</strong><span className="text-slate-500">Legacy evidence is not keyed to an exact prediction horizon.</span></span></li>
              </ul>
            </div>
          ))}
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
          <div className="flex items-center gap-2"><Database className="h-5 w-5 text-slate-400" aria-hidden="true" /><h2 className="font-bold text-slate-950">Data health and coverage</h2></div>
          {health.length > 0 ? <ul className="mt-3 space-y-2">{health.map((issue) => <li key={issue.kind} className={`rounded-lg border px-3 py-2 text-xs ${issue.severity === "error" ? "border-red-200 bg-red-50 text-red-900" : "border-amber-200 bg-amber-50 text-amber-900"}`}><strong>{issue.title}</strong><span className="block mt-0.5">{issue.detail}</span></li>)}</ul> : <div className="mt-3 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-900">No current pipeline-health alerts.</div>}
          {coverage ? <div className="mt-4 grid gap-2 sm:grid-cols-3"><Metric label="Schedule in DB" value={`${coverage.dateCount} dates`} detail={`${coverage.gameCount} games`} /><Metric label="Scores complete" value={coverage.latestScoreCompleteDate ?? "None"} detail={coverage.firstMissingScoreDate ? `First gap ${coverage.firstMissingScoreDate}` : "No known gap"} /><Metric label="Full odds complete" value={coverage.latestOddsCompleteDate ?? "None"} detail={coverage.firstMissingOddsDate ? `First gap ${coverage.firstMissingOddsDate}` : "No known gap"} /></div> : null}
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="font-bold text-slate-950">Historical model checks</h2>
          <div className="mt-3 space-y-3"><Metric label="Moneyline" value={`${moneylineBacktest.overall.wins}-${moneylineBacktest.overall.losses}`} detail={`${fmtRoi(moneylineBacktest.overall.roi)} ROI over ${moneylineBacktest.overall.bets} bets`} /><Metric label="Totals" value={`${totalBacktest.overall.wins}-${totalBacktest.overall.losses}-${totalBacktest.overall.pushes}`} detail={`${fmtRoi(totalBacktest.overall.roi)} ROI over ${totalBacktest.overall.bets} decisions`} /><Metric label="Tracked CLV" value={`${clv.find((row) => row.tier === "all")?.n ?? 0} bets`} detail={`${(clv.find((row) => row.tier === "all")?.avgClvPp ?? 0).toFixed(2)}pp average`} /></div>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="font-bold text-slate-950">Prospective decision ledger</h2>
        <p className="mt-1 text-xs text-slate-500">Complete model-ledger population remains separate from personal tickets.</p>
        <div className="mt-3 overflow-x-auto"><table className="min-w-[900px] w-full text-xs"><thead className="text-left text-[10px] uppercase tracking-wide text-slate-500"><tr><th className="py-2">Start</th><th className="py-2">Game</th><th className="py-2">Market</th><th className="py-2">Selection</th><th className="py-2 text-right">Price</th><th className="py-2 text-right">Model</th><th className="py-2 text-right">Market</th><th className="py-2 text-right">EV</th><th className="py-2">Result</th><th className="py-2">Version</th></tr></thead><tbody>{bets.slice(0, 50).map((bet) => <tr key={bet.id} className="border-t border-slate-100"><td className="py-2">{fmtEt(bet.eventCommence, true)}</td><td className="py-2">{bet.fixture ?? "-"}</td><td className="py-2 capitalize">{bet.betType}</td><td className="py-2 font-semibold">{bet.selectionLabel}</td><td className="py-2 text-right">{fmtAmerican(bet.marketOdds)}</td><td className="py-2 text-right">{fmtPct(bet.ourProb)}</td><td className="py-2 text-right">{fmtPct(bet.marketProb)}</td><td className="py-2 text-right">{fmtRoi(bet.ev)}</td><td className="py-2 capitalize">{bet.status}</td><td className="py-2 font-mono text-[10px]">{bet.modelVersion}</td></tr>)}</tbody></table>{bets.length === 0 ? <div className="py-8 text-center text-sm text-slate-500">No prospective model-ledger rows yet.</div> : null}</div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="flex items-center gap-2"><Activity className="h-5 w-5 text-slate-400" aria-hidden="true" /><h2 className="font-bold text-slate-950">Current line movement</h2></div>
          <div className="mt-3 space-y-2">{lineMovement.slice(0, 12).map((row) => <div key={row.matchupId} className="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 text-xs"><div className="flex justify-between gap-3"><strong>{row.matchup}</strong><span>{((row.closeProb - row.openProb) * 100).toFixed(1)}pp ML move</span></div><div className="mt-1 text-slate-500">{row.captures} captures - total move {row.totalMove == null ? "-" : `${row.totalMove >= 0 ? "+" : ""}${row.totalMove.toFixed(1)}`} - sharp gap {row.pinGapPp == null ? "-" : `${row.pinGapPp.toFixed(1)}pp`}</div></div>)}{lineMovement.length === 0 ? <div className="py-6 text-center text-xs text-slate-500">No multi-capture upcoming games.</div> : null}</div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="font-bold text-slate-950">Recent market alerts</h2>
          <div className="mt-3 space-y-2">{lineAlerts.slice(0, 12).map((alert, index) => <div key={`${alert.createdAt}-${alert.matchup}-${index}`} className="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 text-xs"><div className="flex justify-between gap-3"><strong>{alert.matchup}</strong><span className="uppercase text-slate-500">{alert.alertType.replaceAll("_", " ")}</span></div><div className="mt-1 text-slate-600">Side {alert.side} - alert {fmtPct(alert.alertProb)} - sharp {fmtPct(alert.sharpProb)} - CLV {alert.clvPp == null ? "pending" : `${alert.clvPp.toFixed(2)}pp`}</div></div>)}{lineAlerts.length === 0 ? <div className="py-6 text-center text-xs text-slate-500">No recent alert rows.</div> : null}</div>
        </div>
      </section>

      <details className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <summary className="cursor-pointer font-bold text-slate-950">Additional audit tables</summary>
        <div className="mt-4 grid gap-4 lg:grid-cols-3"><Metric label="Calibration rows" value={String(betBacktest.length)} /><Metric label="Alert audit groups" value={String(lineAlertBacktest.length)} /><Metric label="Movement history" value={String(lineMovementHistory.length)} /></div>
        <div className="mt-4 overflow-x-auto"><table className="min-w-[720px] w-full text-xs"><thead className="text-left text-[10px] uppercase tracking-wide text-slate-500"><tr><th className="py-2">Date</th><th className="py-2">Game</th><th className="py-2 text-right">Open</th><th className="py-2 text-right">Close</th><th className="py-2">Moved toward</th><th className="py-2">Result</th><th className="py-2">Hit</th></tr></thead><tbody>{lineMovementHistory.slice(0, 50).map((row) => <tr key={`${row.matchupId}-${row.gameDate}`} className="border-t border-slate-100"><td className="py-2">{row.gameDate}</td><td className="py-2 font-semibold">{row.matchup}</td><td className="py-2 text-right">{fmtPct(row.openProb)}</td><td className="py-2 text-right">{fmtPct(row.closeProb)}</td><td className="py-2">{row.movedToward ?? "flat"}</td><td className="py-2">{row.score ?? row.winner ?? "pending"}</td><td className="py-2">{row.movedSideWon == null ? "-" : row.movedSideWon ? "Yes" : "No"}</td></tr>)}</tbody></table></div>
      </details>

      <div className="sr-only">{actionabilityEvidence.length} actionability evidence rows loaded.</div>
    </div>
  );
}

export default function MlbVegasClient(props: Props) {
  const { queryDate, evaluatedAt, decisions, trustDecisions } = props;
  const router = useRouter();
  const [isRefreshing, startRefresh] = useTransition();
  const availableBookKeys = useMemo(() => Array.from(new Set(decisions.map((decision) => decision.bookKey).filter((key): key is string => Boolean(key)))).sort((a, b) => (MLB_BOOK_LABELS[a] ?? a).localeCompare(MLB_BOOK_LABELS[b] ?? b)), [decisions]);
  const defaultBooks = useMemo(() => {
    const preferred = availableBookKeys.filter((key) => (MLB_DEFAULT_BOOKS as readonly string[]).includes(key));
    return preferred.length > 0 ? preferred : availableBookKeys;
  }, [availableBookKeys]);
  const [selectedBooks, setSelectedBooks] = useState<string[]>(defaultBooks);
  const [view, setView] = useState<MainView>("board");
  const [marketFilter, setMarketFilter] = useState<MarketFilter>("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [selectedDecisionId, setSelectedDecisionId] = useState<string | null>(null);
  const [priceWatches, setPriceWatches] = useState<string[]>([]);
  const [parlayLegs, setParlayLegs] = useState<string[]>([]);
  const [nowIso, setNowIso] = useState(evaluatedAt);

  useEffect(() => {
    const id = window.setTimeout(() => {
      const savedBooks = window.localStorage.getItem(MY_BOOKS_STORAGE);
      if (savedBooks) {
        try {
          const parsed = JSON.parse(savedBooks) as unknown;
          if (Array.isArray(parsed)) {
            const valid = parsed.filter((key): key is string => typeof key === "string" && availableBookKeys.includes(key));
            if (valid.length > 0) setSelectedBooks(valid);
          }
        } catch { /* keep server-selected defaults */ }
      }
      const savedWatches = window.localStorage.getItem(PRICE_WATCH_STORAGE);
      if (savedWatches) {
        try {
          const parsed = JSON.parse(savedWatches) as unknown;
          if (Array.isArray(parsed)) setPriceWatches(parsed.filter((key): key is string => typeof key === "string"));
        } catch { /* ignore malformed device preference */ }
      }
    }, 0);
    return () => window.clearTimeout(id);
  }, [availableBookKeys]);

  useEffect(() => {
    const id = window.setInterval(() => setNowIso(new Date().toISOString()), 30_000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    if (!selectedDecisionId) return;
    const onKey = (event: KeyboardEvent) => { if (event.key === "Escape") setSelectedDecisionId(null); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selectedDecisionId]);

  const selectedDecisions = useMemo(() => {
    const candidates = decisions.filter((decision) => decision.bookKey == null || selectedBooks.includes(decision.bookKey));
    const groups = new Map<string, MlbMarketDecision[]>();
    for (const decision of candidates) {
      const key = decisionGroupKey(decision);
      groups.set(key, [...(groups.get(key) ?? []), decision]);
    }
    return Array.from(groups.values()).map((rows) => [...rows].sort(compareDecisionQuality)[0]).sort((a, b) => {
      const timeA = Date.parse(a.commenceTime ?? "") || Infinity;
      const timeB = Date.parse(b.commenceTime ?? "") || Infinity;
      return STATUS_ORDER[a.primaryStatus] - STATUS_ORDER[b.primaryStatus] || timeA - timeB || a.matchup.localeCompare(b.matchup);
    });
  }, [decisions, selectedBooks]);

  const filteredDecisions = selectedDecisions.filter((decision) => (marketFilter === "all" || decision.market === marketFilter) && (statusFilter === "all" || decision.primaryStatus === statusFilter));
  const counts = Object.fromEntries((Object.keys(STATUS_META) as MlbPrimaryStatus[]).map((status) => [status, selectedDecisions.filter((decision) => decision.primaryStatus === status).length])) as Record<MlbPrimaryStatus, number>;
  const queue = filteredDecisions.filter((decision) => decision.primaryStatus === "take_now" || decision.primaryStatus === "watch");
  const selectedDecision = decisions.find((decision) => decision.decisionId === selectedDecisionId) ?? null;
  const priceShop = selectedDecision ? decisions.filter((decision) => decision.matchupId === selectedDecision.matchupId && decision.market === selectedDecision.market).sort(compareDecisionQuality) : [];

  const saveBooks = (next: string[]) => {
    setSelectedBooks(next);
    window.localStorage.setItem(MY_BOOKS_STORAGE, JSON.stringify(next));
  };
  const toggleWatch = (decision: MlbMarketDecision) => {
    const key = priceWatchKey(decision);
    setPriceWatches((current) => {
      const next = current.includes(key) ? current.filter((item) => item !== key) : [...current, key];
      window.localStorage.setItem(PRICE_WATCH_STORAGE, JSON.stringify(next));
      return next;
    });
  };
  const navigateDate = (date: string) => router.push(`/vegas?sport=mlb&date=${date}`);
  const reload = () => startRefresh(() => router.refresh());
  const lastQuote = decisions.map((decision) => Date.parse(decision.oddsCapturedAt ?? "")).filter(Number.isFinite).sort((a, b) => b - a)[0];
  const predictionCount = new Set(decisions.map((decision) => decision.predictionSnapshotId).filter(Boolean)).size;
  const observedCount = new Set(decisions.map((decision) => decision.observedOddsSnapshotId).filter(Boolean)).size;

  return (
    <div className="mx-auto max-w-7xl space-y-5 p-4 sm:p-6">
      <header className="rounded-2xl border border-slate-200 bg-gradient-to-br from-slate-950 via-slate-900 to-blue-950 p-5 text-white shadow-lg sm:p-6">
        <div className="flex flex-wrap items-start justify-between gap-5">
          <div>
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.18em] text-blue-200"><Target className="h-4 w-4" aria-hidden="true" /> Vegas Analysis - MLB</div>
            <h1 className="mt-2 text-2xl font-black tracking-tight sm:text-3xl">MLB Bet Decisions</h1>
            <p className="mt-2 max-w-2xl text-sm text-slate-300">Should I take this exact market, at this exact sportsbook price, right now?</p>
          </div>
          <div className="rounded-xl border border-white/15 bg-white/5 px-4 py-3 text-right">
            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Eastern time</div>
            <div className="mt-1 text-sm font-bold tabular-nums">{fmtEtClock(nowIso)}</div>
          </div>
        </div>

        <div className="mt-5 flex flex-wrap items-center gap-2">
          <button type="button" onClick={() => navigateDate(shiftDate(queryDate, -1))} className="inline-flex h-11 w-11 items-center justify-center rounded-lg border border-white/20 bg-white/10 hover:bg-white/15" aria-label="Previous date"><ChevronLeft className="h-5 w-5" /></button>
          <label className="flex min-h-11 items-center gap-2 rounded-lg border border-white/20 bg-white/10 px-3 text-xs font-semibold"><CalendarDays className="h-4 w-4" aria-hidden="true" /><input type="date" value={queryDate} onChange={(event) => navigateDate(event.target.value)} className="bg-transparent text-sm font-semibold text-white [color-scheme:dark]" /></label>
          <button type="button" onClick={() => navigateDate(shiftDate(queryDate, 1))} className="inline-flex h-11 w-11 items-center justify-center rounded-lg border border-white/20 bg-white/10 hover:bg-white/15" aria-label="Next date"><ChevronRight className="h-5 w-5" /></button>

          <details className="relative">
            <summary className="flex min-h-11 cursor-pointer list-none items-center gap-2 rounded-lg border border-white/20 bg-white/10 px-3 text-xs font-semibold hover:bg-white/15"><SlidersHorizontal className="h-4 w-4" aria-hidden="true" /> My Books ({selectedBooks.length})</summary>
            <div className="absolute left-0 z-30 mt-2 w-64 rounded-xl border border-slate-200 bg-white p-3 text-slate-900 shadow-xl">
              <div className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-slate-500">Use only books I can access</div>
              <div className="space-y-1">
                {availableBookKeys.map((key) => <label key={key} className="flex min-h-10 items-center gap-2 rounded-lg px-2 text-sm hover:bg-slate-50"><input type="checkbox" checked={selectedBooks.includes(key)} onChange={(event) => saveBooks(event.target.checked ? [...selectedBooks, key] : selectedBooks.filter((book) => book !== key))} />{MLB_BOOK_LABELS[key] ?? key}</label>)}
              </div>
            </div>
          </details>

          <button type="button" onClick={reload} disabled={isRefreshing} className="ml-auto inline-flex min-h-11 items-center gap-2 rounded-lg bg-blue-500 px-4 py-2 text-xs font-bold text-white hover:bg-blue-400 disabled:opacity-60"><RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} aria-hidden="true" />{isRefreshing ? "Reloading" : "Reload pipeline output"}</button>
        </div>

        <div className="mt-4 grid grid-cols-2 gap-2 text-[10px] sm:grid-cols-5">
          {[
            ["Schedule", decisions.length > 0, `${new Set(decisions.map((d) => d.matchupId)).size} games`],
            ["Quotes", observedCount > 0, `${observedCount} snapshots`],
            ["Context", predictionCount > 0, `${predictionCount} snapshots`],
            ["Prediction", predictionCount > 0, predictionCount > 0 ? "loaded" : "missing"],
            ["Decisions", true, fmtEt(evaluatedAt)],
          ].map(([label, ok, detail]) => <div key={String(label)} className={`rounded-lg border px-3 py-2 ${ok ? "border-emerald-400/30 bg-emerald-400/10" : "border-red-400/30 bg-red-400/10"}`}><div className="flex items-center gap-1.5 font-bold uppercase tracking-wide">{ok ? <CheckCircle2 className="h-3 w-3 text-emerald-300" /> : <XCircle className="h-3 w-3 text-red-300" />}{label}</div><div className="mt-0.5 truncate text-slate-300">{detail}</div></div>)}
        </div>
        <div className="mt-2 text-[10px] text-slate-400">Last observed quote: {lastQuote ? fmtEt(new Date(lastQuote).toISOString(), true) : "none"}</div>
      </header>

      <nav className="grid grid-cols-3 rounded-xl border border-slate-200 bg-white p-1 shadow-sm" aria-label="MLB Vegas views">
        {(["board", "parlay", "evidence"] as MainView[]).map((item) => <button key={item} type="button" onClick={() => setView(item)} className={`min-h-11 rounded-lg px-3 py-2 text-sm font-bold capitalize ${view === item ? "bg-slate-950 text-white" : "text-slate-600 hover:bg-slate-100"}`}>{item === "board" ? "Today's Decisions" : item}</button>)}
      </nav>

      {view === "board" ? (
        <>
          <section className="grid gap-3 md:grid-cols-2">
            {(["moneyline", "total"] as MlbDecisionMarket[]).map((market) => {
              const trust = trustDecisions.find((decision) => decision.market === market);
              const enabled = selectedDecisions.some((decision) => decision.market === market && decision.primaryStatus === "take_now");
              const open = (trust ? trust.total - trust.passed : 0) + (trust?.canonicalHorizon ? 0 : 1);
              return <button type="button" key={market} onClick={() => setView("evidence")} className={`flex items-center justify-between gap-3 rounded-xl border p-4 text-left shadow-sm ${enabled ? "border-emerald-300 bg-emerald-50" : "border-amber-300 bg-amber-50"}`}><div><div className="text-xs font-bold uppercase tracking-wide text-slate-500">{market}</div><div className="mt-1 font-bold text-slate-950">{enabled ? "TAKE enabled" : `TAKE disabled - ${open} gates open`}</div><div className="mt-1 text-xs text-slate-600">{trust?.summary ?? "Trust evidence unavailable"}</div></div><ChevronRight className="h-5 w-5 text-slate-400" aria-hidden="true" /></button>;
            })}
          </section>

          <section className="grid grid-cols-2 gap-2 sm:grid-cols-5">
            {(Object.keys(STATUS_META) as MlbPrimaryStatus[]).map((status) => <button key={status} type="button" onClick={() => setStatusFilter(statusFilter === status ? "all" : status)} className={`rounded-xl border p-3 text-left shadow-sm ${statusFilter === status ? STATUS_META[status].chip : "border-slate-200 bg-white text-slate-800"}`}><div className="text-[10px] font-bold uppercase tracking-wide">{STATUS_META[status].label}</div><div className="mt-1 text-2xl font-black tabular-nums">{counts[status]}</div></button>)}
          </section>

          <section className="flex flex-wrap items-center gap-2 rounded-xl border border-slate-200 bg-white p-3 shadow-sm">
            <span className="mr-1 text-xs font-bold text-slate-500">Market</span>
            {(["all", "moneyline", "total"] as MarketFilter[]).map((market) => <button key={market} type="button" onClick={() => setMarketFilter(market)} className={`min-h-10 rounded-lg px-3 py-2 text-xs font-bold capitalize ${marketFilter === market ? "bg-blue-700 text-white" : "bg-slate-100 text-slate-700"}`}>{market === "all" ? "All markets" : market}</button>)}
            {statusFilter !== "all" ? <button type="button" onClick={() => setStatusFilter("all")} className="ml-auto min-h-10 rounded-lg border border-slate-300 px-3 py-2 text-xs font-semibold text-slate-700">Clear answer filter</button> : null}
          </section>

          {decisions.length === 0 ? <section className="rounded-xl border border-dashed border-slate-300 bg-white px-6 py-16 text-center"><CalendarDays className="mx-auto h-8 w-8 text-slate-300" /><h2 className="mt-3 font-bold text-slate-900">No MLB games scheduled for this date</h2><p className="mt-1 text-sm text-slate-500">Choose another date or reload after the schedule pipeline runs.</p></section> : selectedBooks.length === 0 ? <section className="rounded-xl border border-amber-300 bg-amber-50 p-5 text-sm text-amber-950"><strong>Select at least one sportsbook in My Books.</strong> Decisions are price-specific and cannot be chosen without an accessible book.</section> : null}

          {decisions.length > 0 && selectedBooks.length > 0 ? (
            <>
              <section>
                <div className="mb-3 flex flex-wrap items-end justify-between gap-3"><div><h2 className="text-lg font-bold text-slate-950">Priority queue</h2><p className="mt-0.5 text-sm text-slate-600">TAKE NOW first, then WATCH. Passes and blockers remain in the complete slate table.</p></div><div className="text-xs text-slate-500">{priceWatches.length} target{priceWatches.length === 1 ? "" : "s"} tracked on this device</div></div>
                {queue.length > 0 ? <div className="space-y-3">{queue.map((decision) => <DecisionCard key={decision.decisionId} decision={decision} nowIso={nowIso} tracked={priceWatches.includes(priceWatchKey(decision))} onDetails={() => setSelectedDecisionId(decision.decisionId)} onTrack={() => toggleWatch(decision)} onAddParlay={() => { setParlayLegs((current) => current.includes(decision.decisionId) ? current : [...current, decision.decisionId]); setView("parlay"); }} />)}</div> : <div className="rounded-xl border border-slate-300 bg-slate-50 p-5"><div className="flex items-start gap-3"><CircleSlash2 className="mt-0.5 h-5 w-5 text-slate-500" aria-hidden="true" /><div><h3 className="font-bold text-slate-950">No bets qualify now</h3><p className="mt-1 text-sm text-slate-600">{counts.blocked > 0 ? `${counts.blocked} markets are blocked by exact data, calibration, stability, or identity requirements.` : `${counts.pass} markets fail the current price rule.`} Open the slate table for the exact reason and next action.</p></div></div></div>}
              </section>

              <section>
                <div className="mb-3"><h2 className="text-lg font-bold text-slate-950">Full slate</h2><p className="mt-0.5 text-sm text-slate-600">One best accessible-book decision per game and market. Open any row for every configured book.</p></div>
                <DecisionTable decisions={filteredDecisions} nowIso={nowIso} onDetails={(decision) => setSelectedDecisionId(decision.decisionId)} />
              </section>
            </>
          ) : null}
        </>
      ) : null}

      {view === "parlay" ? <ParlayView decisions={decisions} selectedBooks={selectedBooks} legs={parlayLegs} setLegs={setParlayLegs} /> : null}
      {view === "evidence" ? <EvidenceView {...props} /> : null}

      {selectedDecision ? <DecisionDrawer decision={selectedDecision} priceShop={priceShop} tracked={priceWatches.includes(priceWatchKey(selectedDecision))} onClose={() => setSelectedDecisionId(null)} onTrack={() => toggleWatch(selectedDecision)} /> : null}
    </div>
  );
}
