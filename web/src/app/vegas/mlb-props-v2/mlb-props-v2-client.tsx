"use client";

import { useMemo, useState } from "react";
import {
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  Clock,
  FlaskConical,
  Landmark,
  Target,
} from "lucide-react";
import type {
  LineAlertRow,
  LineAlertBacktestRow,
  MlbPropProgramRow,
} from "@/db/queries";
import { disclosure, verdict as auditVerdict, multiplicityNote } from "@/lib/alert-audit-policy";
import {
  ENROLLED_DETECTOR_VERSION,
  LIVE_ARM_MIN_EV_PCT,
  PLANNING_CLV_RANGE_PCT,
  applyFilters,
  bookLabel,
  controlArm,
  formatAmerican,
  formatCountdown,
  liveArmHistory,
  liveBoard,
  marketLabel,
  toPropPlay,
  urgency,
  type PropPlay,
} from "@/lib/mlb-prop-board";

// Frozen registration constants, mirrored from model/mlb_prop_program.py. They
// are displayed, never recomputed: ceil((2.80 x 4.12 / 1.0)^2) = 134.
const FLOOR_N_EFF = 134;
const FLOOR_DISTINCT_DATES = 25;
const FLOOR_SETTLED = 30;
const VERDICT_DATE = "2026-10-04";
const DAY14_GATE_DATE = "2026-09-01";
const CONTROL_BASELINE_PP = -0.13;
const CONCENTRATION_DISCLOSE = 0.4;

/* ── small shared pieces ─────────────────────────────────────────────────── */

function Band({
  icon,
  title,
  subtitle,
  children,
}: {
  icon: React.ReactNode;
  title: string;
  subtitle?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-xl border bg-white shadow-sm">
      <header className="flex items-start gap-2.5 border-b px-4 py-3">
        <span className="mt-0.5 text-gray-400">{icon}</span>
        <div className="min-w-0">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-gray-700">
            {title}
          </h2>
          {subtitle && <p className="mt-0.5 text-xs leading-relaxed text-gray-500">{subtitle}</p>}
        </div>
      </header>
      <div className="p-4">{children}</div>
    </section>
  );
}

function Collapsible({
  title,
  count,
  tone = "neutral",
  children,
}: {
  title: string;
  count: number;
  tone?: "neutral" | "control";
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className={`rounded-xl border ${tone === "control" ? "border-dashed bg-gray-50/60" : "bg-white"} shadow-sm`}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 px-4 py-3 text-left"
      >
        {open ? <ChevronDown className="h-4 w-4 text-gray-400" /> : <ChevronRight className="h-4 w-4 text-gray-400" />}
        <span className="text-sm font-semibold uppercase tracking-wide text-gray-600">{title}</span>
        <span className="rounded-full bg-gray-100 px-2 py-0.5 text-[11px] font-medium tabular-nums text-gray-500">
          {count}
        </span>
      </button>
      {open && <div className="border-t px-4 py-4">{children}</div>}
    </div>
  );
}

/** Progress toward a frozen floor. Never renders past 100% — a floor that is
 *  met is met; an overflowing bar would imply surplus evidence that the
 *  conjunctive gate does not grant. */
function FloorMeter({
  label,
  value,
  floor,
  hint,
}: {
  label: string;
  value: number;
  floor: number;
  hint: string;
}) {
  const pctRaw = floor > 0 ? (value / floor) * 100 : 0;
  const pct = Math.min(100, Math.round(pctRaw));
  const met = value >= floor;
  return (
    <div title={hint}>
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[11px] font-medium uppercase tracking-wide text-gray-500">{label}</span>
        <span className="text-xs tabular-nums text-gray-600">
          <span className="font-semibold text-gray-900">{value}</span>
          <span className="text-gray-400"> / {floor}</span>
        </span>
      </div>
      <div className="mt-1.5 h-1.5 overflow-hidden rounded-full bg-gray-100">
        {/* Amber, not green, even at 100%: reaching a sample floor is a
            precondition for a verdict, not a passed validation gate. */}
        <div
          className={`h-full rounded-full ${met ? "bg-amber-400" : "bg-slate-300"}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <p className="mt-1 text-[11px] text-gray-400">
        {met ? "floor met" : `${floor - value} more needed`}
      </p>
    </div>
  );
}

/* ── band 1: program status ──────────────────────────────────────────────── */

function StatusBand({ program }: { program: MlbPropProgramRow }) {
  const topBook = program.execBooks[0];
  const settledBooks = program.execBooks.reduce((s, b) => s + b.n, 0);
  const topShare = topBook && settledBooks > 0 ? topBook.n / settledBooks : null;
  const concentrated = topShare != null && topShare > CONCENTRATION_DISCLOSE;

  const floorsMet =
    program.liveSettled >= FLOOR_N_EFF &&
    program.liveSettled >= FLOOR_SETTLED &&
    program.liveSettledDates >= FLOOR_DISTINCT_DATES;

  return (
    <Band
      icon={<FlaskConical className="h-4 w-4" />}
      title="Measurement program — mlb-prop-program-v1"
      subtitle={
        <>
          One pre-registered question: <em>does the live arm produce capturable CLV at all?</em>{" "}
          Registered 2026-08-15, test family of 1, no efficacy stopping. Verdict is computed once, on{" "}
          <span className="font-medium text-gray-700">{VERDICT_DATE}</span>.
        </>
      }
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="rounded-full bg-slate-800 px-3 py-1 text-xs font-semibold text-white">
          {floorsMet ? "FLOORS MET — awaiting verdict date" : "ACCRUING — no verdict permitted"}
        </span>
        <span className="text-xs text-gray-500">
          Nothing on this page is a recommendation to bet.
        </span>
      </div>

      <div className="mt-4 grid gap-5 sm:grid-cols-3">
        <FloorMeter
          label="Settled (power floor)"
          value={program.liveSettled}
          floor={FLOOR_N_EFF}
          hint={`Graded won/lost alerts from the enrolled detector. Floor 134 = ceil((2.80 x 4.12 / 1.0)^2) — 80% power to detect the pre-registered 1.0pp MDE at a planning SD of 4.12pp. The separate ${FLOOR_SETTLED}-alert disclosure floor is currently ${program.liveSettled >= FLOOR_SETTLED ? "met" : "not met"}.`}
        />
        <FloorMeter
          label="Distinct dates (clusters)"
          value={program.liveSettledDates}
          floor={FLOOR_DISTINCT_DATES}
          hint="The CI is a date-clustered bootstrap: it resamples DATES, not observations, because alerts on one slate share a pitcher, park, weather and umpire. This floor cannot be bought with volume — 10 dates gives a meaningless interval at any n."
        />
        <div>
          <div className="text-[11px] font-medium uppercase tracking-wide text-gray-500">
            Plan against
          </div>
          <div className="mt-1 text-lg font-semibold tabular-nums text-gray-900">
            {PLANNING_CLV_RANGE_PCT[0]}–{PLANNING_CLV_RANGE_PCT[1]}%
          </div>
          <p
            className="mt-1 text-[11px] leading-relaxed text-gray-400"
            title="v1 measured +1.29% CLV over n=73, 95% CI [+0.43%, +2.32%]. It degrades gracefully under trimming — drop the top 3 and it is +0.87%, top 5 +0.62%, top 10 +0.27% [+0.08%, +0.54%], still excluding zero at n=63. Real, small, broadly distributed. The headline is not the planning number."
          >
            CLV per bet, trimmed — not the 1.29% headline
          </p>
        </div>
      </div>

      <div className="mt-4 grid gap-3 border-t pt-3 text-xs text-gray-600 sm:grid-cols-2">
        <div>
          <span className="font-medium text-gray-700">Control arm (instrument check):</span>{" "}
          {program.controlSettled} settled of {program.controlTotal}. Baseline{" "}
          <span className="tabular-nums">{CONTROL_BASELINE_PP.toFixed(2)}pp</span>. Both arms share
          pipeline, grading, latency and voids, so drift here is an{" "}
          <span className="font-medium">instrument alarm</span> — a cadence or close-definition
          problem — never a finding.
        </div>
        <div>
          <span className="font-medium text-gray-700">Execution books (settled):</span>{" "}
          {program.execBooks.length === 0 ? (
            <span className="text-gray-400">none yet</span>
          ) : (
            program.execBooks
              .map((b) => `${bookLabel(b.book)} ${b.n}`)
              .join(" · ")
          )}
          {concentrated && (
            <span className="mt-1 flex items-start gap-1 text-amber-700">
              <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
              <span>
                {bookLabel(topBook!.book)} carries {Math.round(topShare! * 100)}% — past the 40% C1
                bar, so any result reads as a <strong>single-book finding</strong>, not a market
                edge.
              </span>
            </span>
          )}
        </div>
      </div>

      <p className="mt-3 border-t pt-3 text-[11px] leading-relaxed text-gray-400">
        Counts only. The CLV point estimate and its date-clustered interval are frozen at
        registration and computed in one place —{" "}
        <code className="rounded bg-gray-100 px-1 py-0.5 text-gray-600">
          python -m model.mlb_prop_program
        </code>{" "}
        — not recomputed here, so the two cannot drift apart. Blinded variance gate:{" "}
        {DAY14_GATE_DATE}.
      </p>
    </Band>
  );
}

/* ── band 2: the live board ──────────────────────────────────────────────── */

const URGENCY_CLS: Record<string, string> = {
  // Orange encodes TIME PRESSURE only. It is deliberately not the amber used
  // for "unproven signal" and never green, which is reserved for a passed gate.
  soon: "bg-orange-50 text-orange-700 ring-1 ring-orange-200",
  today: "bg-slate-100 text-slate-600",
  later: "bg-gray-50 text-gray-500",
};

function PlayCard({ p }: { p: PropPlay }) {
  const execIsDk = p.execBook === "draftkings" || p.execBook == null;
  return (
    <article className="rounded-xl border bg-white p-4 shadow-sm transition hover:border-gray-300">
      <div className="flex items-center justify-between gap-2">
        <span
          className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-semibold ${URGENCY_CLS[urgency(p.minutesToFirstPitch)]}`}
        >
          <Clock className="h-3 w-3" />
          {formatCountdown(p.minutesToFirstPitch)}
        </span>
        <span className="truncate text-[11px] text-gray-400" title={p.matchup}>
          {p.matchup}
        </span>
      </div>

      {/* The proposition, as the headline. On the old page this was a text
          string inside a 10px chip while "Away @ Home" got its own column. */}
      <div className="mt-3">
        <div className="text-base font-semibold leading-tight text-gray-900">{p.player}</div>
        <div className="mt-0.5 text-sm text-gray-600">
          <span className="font-medium text-gray-800">
            {p.side} {p.line}
          </span>{" "}
          {p.marketLabel}
        </div>
      </div>

      {/* The action: where the bet actually goes. v3 selects at DraftKings and
          executes at the best same-line price across six books — the old chip
          printed the SELECTION price, which is deliberately not where you bet. */}
      <div className="mt-3 rounded-lg bg-slate-50 px-3 py-2.5">
        <div className="text-[10px] font-semibold uppercase tracking-wide text-gray-500">
          Best same-line price
        </div>
        <div className="mt-0.5 flex items-baseline gap-2">
          <span className="text-lg font-bold tabular-nums text-gray-900">
            {formatAmerican(p.execOdds ?? p.dkOdds)}
          </span>
          <span className="text-sm font-medium text-gray-700">
            {bookLabel(p.execBook ?? "draftkings")}
          </span>
        </div>
        <div className="mt-1 text-[11px] text-gray-500">
          {execIsDk ? (
            <>Selection book is also the best price.</>
          ) : (
            <>
              <span className="font-medium text-gray-700">
                +{p.execGainPct?.toFixed(1) ?? "?"}%
              </span>{" "}
              better than DraftKings {formatAmerican(p.dkOdds)} — line-shopping gain, no prediction
              involved.
            </>
          )}
        </div>
      </div>

      <dl className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-[11px]">
        <div>
          <dt className="text-gray-400">EV vs Pinnacle fair</dt>
          <dd
            className="font-semibold tabular-nums text-amber-700"
            title={`DraftKings' price is ${p.evPct?.toFixed(2) ?? "?"}% above Pinnacle's vig-free fair value at the same line (${p.line}). Selection threshold is ${LIVE_ARM_MIN_EV_PCT}%. No model involved — this is a price comparison, not a projection.`}
          >
            +{p.evPct?.toFixed(1) ?? "?"}%
          </dd>
        </div>
        <div>
          <dt className="text-gray-400">Books clearing {LIVE_ARM_MIN_EV_PCT}%</dt>
          <dd
            className="tabular-nums text-gray-700"
            title="At ~7% two-way hold, two books clearing +3% on the same side would require near-zero margin, so 1 is the arithmetically expected result. It is not evidence of market-wide staleness."
          >
            {p.booksQualifying ?? "—"} of 6
          </dd>
        </div>
      </dl>

      <div className="mt-3 flex flex-wrap gap-1.5 border-t pt-2.5">
        {p.enrolled ? (
          <span
            className="rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-medium text-slate-600"
            title={`Enrolled in mlb-prop-program-v1 under detector ${ENROLLED_DETECTOR_VERSION}.`}
          >
            enrolled
          </span>
        ) : (
          <span
            className="rounded-full bg-gray-100 px-2 py-0.5 text-[10px] font-medium text-gray-500"
            title={`Detector version ${p.detectorVersion ?? "unstamped"} is not the enrolled cohort (${ENROLLED_DETECTOR_VERSION}). A changed trigger is not poolable with the enrolled observations.`}
          >
            not enrolled
          </span>
        )}
        {!p.anchoredMarket && (
          <span
            className="rounded-full bg-gray-100 px-2 py-0.5 text-[10px] font-medium text-gray-500"
            title="Outside the four anchored markets in the pooled cell. Pinnacle's same-line coverage on this market did not clear the census, so its reference is less reliable."
          >
            outside anchored set
          </span>
        )}
      </div>
    </article>
  );
}

function LiveBoardBand({
  plays,
  startedCount,
  unknownCount,
}: {
  plays: PropPlay[];
  startedCount: number;
  unknownCount: number;
}) {
  const [market, setMarket] = useState("all");
  const [minEvPct, setMinEvPct] = useState(0);

  const markets = useMemo(() => {
    const s = new Set(plays.map((p) => p.market).filter(Boolean));
    return [...s].sort();
  }, [plays]);

  const shown = useMemo(
    () => applyFilters(plays, { market, minEvPct }),
    [plays, market, minEvPct],
  );

  return (
    <Band
      icon={<Target className="h-4 w-4" />}
      title="Live board — first pitch not yet passed"
      subtitle={
        <>
          The live arm only (<code className="text-gray-600">dk_prop_value</code>): DraftKings&rsquo;
          price beats Pinnacle&rsquo;s vig-free fair value by ≥{LIVE_ARM_MIN_EV_PCT}% at the{" "}
          <em>same line</em>. Soonest first pitch first — never sorted by claimed edge, because a
          board ranked by EV reads as a ranked recommendation.
        </>
      }
    >
      {plays.length > 0 && (
        <div className="mb-4 flex flex-wrap items-center gap-3 text-xs">
          <label className="flex items-center gap-1.5">
            <span className="text-gray-500">Market</span>
            <select
              value={market}
              onChange={(e) => setMarket(e.target.value)}
              className="rounded border bg-white px-2 py-1 text-xs"
            >
              <option value="all">All ({plays.length})</option>
              {markets.map((m) => (
                <option key={m} value={m}>
                  {marketLabel(m)} ({plays.filter((p) => p.market === m).length})
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1.5">
            <span className="text-gray-500">Min EV</span>
            <select
              value={minEvPct}
              onChange={(e) => setMinEvPct(Number(e.target.value))}
              className="rounded border bg-white px-2 py-1 text-xs"
            >
              <option value={0}>any</option>
              <option value={4}>≥ 4%</option>
              <option value={5}>≥ 5%</option>
              <option value={7}>≥ 7%</option>
            </select>
          </label>
          <span className="text-gray-400">
            {shown.length} of {plays.length} shown
          </span>
        </div>
      )}

      {plays.length === 0 ? (
        <div className="rounded-lg border border-dashed bg-gray-50 px-4 py-6 text-center">
          <p className="text-sm font-medium text-gray-600">No live plays right now.</p>
          <p className="mx-auto mt-1 max-w-md text-xs leading-relaxed text-gray-500">
            {startedCount > 0 || unknownCount > 0 ? (
              <>
                {startedCount} live-arm alert{startedCount === 1 ? "" : "s"} on games that have
                already started
                {unknownCount > 0 && <> and {unknownCount} with no recorded start time</>} — see the
                history section below. The scanner runs after each prop capture (3×/day).
              </>
            ) : (
              <>The scanner runs after each prop capture (3×/day).</>
            )}
          </p>
        </div>
      ) : shown.length === 0 ? (
        <div className="rounded-lg border border-dashed bg-gray-50 px-4 py-6 text-center text-sm text-gray-500">
          No live plays match these filters.
        </div>
      ) : (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {shown.map((p) => (
            <PlayCard key={p.key} p={p} />
          ))}
        </div>
      )}
    </Band>
  );
}

/* ── band 3: history + control arm ───────────────────────────────────────── */

function HistoryTable({ plays, control }: { plays: PropPlay[]; control?: boolean }) {
  if (plays.length === 0) {
    return <p className="text-xs text-gray-500">Nothing recorded yet.</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-xs">
        <thead>
          <tr className="border-b text-left text-gray-500">
            <th className="py-1.5 pr-3 font-medium">Fired (UTC)</th>
            <th className="py-1.5 pr-3 font-medium">Proposition</th>
            <th className="py-1.5 pr-3 font-medium">Game</th>
            <th className="py-1.5 pr-3 text-right font-medium">
              {control ? "DK / Pin line" : "Price"}
            </th>
            <th className="py-1.5 pr-3 text-right font-medium">CLV</th>
            <th className="py-1.5 text-right font-medium">Result</th>
          </tr>
        </thead>
        <tbody>
          {plays.map((p) => (
            <tr key={p.key} className="border-b border-gray-50">
              <td className="whitespace-nowrap py-1.5 pr-3 text-gray-500">
                {p.createdAt.slice(5, 16)}
              </td>
              <td className="py-1.5 pr-3">
                <span className="font-medium text-gray-800">{p.player}</span>{" "}
                <span className="text-gray-600">
                  {p.side} {p.line} {p.marketLabel}
                </span>
              </td>
              <td className="py-1.5 pr-3 text-gray-500">{p.matchup}</td>
              <td className="whitespace-nowrap py-1.5 pr-3 text-right tabular-nums text-gray-700">
                {control ? (
                  <span title="DK's line versus Pinnacle's. These are DIFFERENT propositions, so no price edge is computed.">
                    {p.line} vs {p.referenceLine}
                  </span>
                ) : (
                  <>
                    {formatAmerican(p.execOdds ?? p.dkOdds)}{" "}
                    <span className="text-gray-400">{bookLabel(p.execBook ?? "draftkings")}</span>
                  </>
                )}
              </td>
              {/* Emerald/red here reads a SIGN on measured data, matching the
                  existing panel so the two pages stay comparable. No status or
                  verdict chip on this page is ever green. */}
              <td
                className={`py-1.5 pr-3 text-right tabular-nums ${
                  p.clvPp == null ? "text-gray-400" : p.clvPp > 0 ? "text-emerald-600" : "text-red-500"
                }`}
              >
                {p.clvPp != null ? `${p.clvPp >= 0 ? "+" : ""}${p.clvPp.toFixed(1)}pp` : "open"}
              </td>
              <td
                className={`py-1.5 text-right ${
                  p.outcome === "won"
                    ? "text-emerald-600"
                    : p.outcome === "lost"
                      ? "text-red-500"
                      : "text-gray-400"
                }`}
              >
                {p.outcome ?? (p.lifecycle === "started" ? "in play" : "—")}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ── band 4: audit ───────────────────────────────────────────────────────── */

function AuditBand({ backtest }: { backtest: LineAlertBacktestRow[] }) {
  const pp = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}pp`;
  const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
  const label = (t: string) =>
    t === "dk_prop_value" ? "Live arm (DK prop value)" : t === "prop_line_gap" ? "Control arm (line gap)" : t;
  const note = multiplicityNote(backtest.length);

  return (
    <Band
      icon={<Landmark className="h-4 w-4" />}
      title="Running audit"
      subtitle="Every alert is an immutable ledger row graded on CLV (did the market keep moving toward the flagged side) and outcome. Verdicts are withheld below the disclosure floor."
    >
      {note && (
        <p className="mb-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-[11px] leading-relaxed text-amber-800">
          {note}
        </p>
      )}
      {backtest.length === 0 ? (
        <p className="text-xs text-gray-500">No graded alerts yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-xs">
            <thead>
              <tr className="border-b text-gray-500">
                <th className="py-1.5 text-left font-medium">Arm</th>
                <th className="py-1.5 text-right font-medium">n</th>
                <th className="py-1.5 text-right font-medium">Avg CLV</th>
                <th className="py-1.5 text-right font-medium" title="Wins-losses-pushes of the flagged side">
                  W-L-P
                </th>
                <th className="py-1.5 text-right font-medium">Win rate</th>
                <th className="py-1.5 text-right font-medium">Implied</th>
                <th className="py-1.5 text-right font-medium">ROI @ frozen price</th>
                <th className="py-1.5 text-right font-medium">Verdict</th>
              </tr>
            </thead>
            <tbody>
              {backtest.map((b) => {
                // The shared panel called verdict() (which honours the floor)
                // but printed win rate and ROI unconditionally, so a rate could
                // render off a handful of settled alerts — the exact inference
                // alert-audit-policy exists to withhold. Here every DERIVED
                // rate goes through disclosure(); raw counts stay visible.
                const d = disclosure(b);
                const v = auditVerdict(b);
                const withheld = (
                  <span className="cursor-help text-gray-400" title={d.reason}>
                    {d.lockLabel}
                  </span>
                );
                return (
                  <tr key={b.alertType} className="border-b border-gray-50">
                    <td className="py-1.5 font-medium text-gray-800">{label(b.alertType)}</td>
                    <td className="py-1.5 text-right tabular-nums text-gray-500">{b.n}</td>
                    <td
                      className={`py-1.5 text-right tabular-nums ${
                        !d.disclosable
                          ? "text-gray-400"
                          : (b.avgClvPp ?? 0) > 0
                            ? "text-emerald-600"
                            : "text-red-500"
                      }`}
                    >
                      {d.disclosable && b.avgClvPp != null ? pp(b.avgClvPp) : withheld}
                    </td>
                    <td className="py-1.5 text-right tabular-nums text-gray-600">
                      {b.nOutcomes > 0
                        ? `${b.wins}-${b.losses}${b.pushes > 0 ? `-${b.pushes}` : ""}`
                        : "—"}
                    </td>
                    <td className="py-1.5 text-right tabular-nums">
                      {d.disclosable ? pct(b.winRate) : withheld}
                    </td>
                    <td className="py-1.5 text-right tabular-nums text-gray-400">
                      {d.disclosable ? pct(b.impliedRate) : withheld}
                    </td>
                    <td
                      className={`py-1.5 text-right tabular-nums ${
                        !d.disclosable || b.dkUnits == null
                          ? "text-gray-400"
                          : b.dkUnits < 0
                            ? "text-red-500"
                            : "text-gray-900 font-medium"
                      }`}
                      title={
                        b.nExecBooks > 1
                          ? `Mixed-book: ${b.nExecBooks} execution books contributed. Not a single-book ROI.`
                          : undefined
                      }
                    >
                      {!d.disclosable
                        ? withheld
                        : b.dkUnits != null && b.nFrozenPrice > 0
                          ? `${b.dkUnits >= 0 ? "+" : ""}${b.dkUnits.toFixed(1)}u (${(
                              (b.dkUnits / b.nFrozenPrice) *
                              100
                            ).toFixed(0)}%, n=${b.nFrozenPrice}${
                              b.nExecBooks > 1 ? `, ${b.nExecBooks} books` : ""
                            })`
                          : "—"}
                    </td>
                    <td className="py-1.5 text-right">
                      <span
                        title={v.tip}
                        className={`cursor-help rounded-full px-2 py-0.5 text-[10px] font-semibold ${v.cls}`}
                      >
                        {v.label}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Band>
  );
}

/* ── page ────────────────────────────────────────────────────────────────── */

export default function MlbPropsV2Client({
  alerts,
  backtest,
  program,
  evaluatedAt,
}: {
  alerts: LineAlertRow[];
  backtest: LineAlertBacktestRow[];
  program: MlbPropProgramRow;
  evaluatedAt: string;
}) {
  const plays = useMemo(() => {
    const now = new Date(evaluatedAt);
    return alerts.map((a) => toPropPlay(a, now));
  }, [alerts, evaluatedAt]);

  const board = useMemo(() => liveBoard(plays), [plays]);
  const history = useMemo(() => liveArmHistory(plays), [plays]);
  const control = useMemo(() => controlArm(plays), [plays]);
  const startedCount = history.filter((p) => p.lifecycle === "started").length;
  const unknownCount = history.filter((p) => p.lifecycle === "unknown").length;

  return (
    <div className="mx-auto max-w-6xl space-y-4 p-6">
      <header>
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h1 className="text-2xl font-bold tracking-tight text-gray-900">MLB Prop Board</h1>
          <a href="/vegas/mlb-props" className="text-xs text-gray-500 underline hover:text-gray-700">
            compare with the original page →
          </a>
        </div>
        <p className="mt-1 max-w-3xl text-sm leading-relaxed text-gray-600">
          Pitcher strikeouts, outs, hits allowed, earned runs and batter total bases — DraftKings
          versus Pinnacle. Game-line alerts stay on the{" "}
          <a href="/vegas?sport=mlb" className="underline">
            MLB Vegas board
          </a>
          .
        </p>
        <p className="mt-2 text-[11px] text-gray-400">
          Evaluated {evaluatedAt.slice(0, 16).replace("T", " ")} UTC · live/started split is computed
          against this one clock
          {program.lastAlertAt && <> · last alert {program.lastAlertAt.slice(5, 16)}</>}
        </p>
      </header>

      <StatusBand program={program} />
      <LiveBoardBand plays={board} startedCount={startedCount} unknownCount={unknownCount} />

      <Collapsible title="Live arm — started & settled" count={history.length}>
        <HistoryTable plays={history} />
      </Collapsible>

      <Collapsible title="Control arm — measurement only, do not bet" count={control.length} tone="control">
        <p className="mb-3 rounded border border-gray-300 bg-white px-3 py-2 text-[11px] leading-relaxed text-gray-600">
          <strong className="text-gray-800">
            <code>prop_line_gap</code> is a control, not a play.
          </strong>{" "}
          Demoted 2026-08-15 on n=439 settled: same-book same-line CLV −0.13%, 95% CI [−0.25%,
          −0.04%] — entirely below zero. Its trigger is a <em>line</em> disagreement, which says the
          two books differ but not <em>which</em> is stale, so direction is a coin flip. It is frozen
          at its v1 trigger so it stays comparable to those 439 observations, and it keeps running
          because it costs nothing and cancels systematic error the live arm shares. On the original
          page these rows sit in the same list as the live arm, separated by chip colour alone —
          and they are the more numerous of the two.
        </p>
        <HistoryTable plays={control} control />
      </Collapsible>

      <AuditBand backtest={backtest} />

      <footer className="pt-2 text-[11px] leading-relaxed text-gray-400">
        Economics, stated plainly: at $100/bet and roughly 650 bets a year, the CLV-implied range is
        about $700–$3,700 a year. The instrumentation to <em>prove</em> it plausibly costs more
        effort than the edge returns. Judge any decision to keep building against that number, not
        against a point estimate.
      </footer>
    </div>
  );
}
