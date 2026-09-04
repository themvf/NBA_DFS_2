"use client";

import {
  Activity,
  AlertTriangle,
  CalendarDays,
  ChevronLeft,
  ChevronRight,
  RefreshCw,
  ShieldCheck,
  Target,
  TrendingDown,
  TrendingUp,
} from "lucide-react";
import Link from "next/link";
import { useEffect, useMemo, useTransition } from "react";
import { useRouter } from "next/navigation";
import type {
  LineAlertBacktestRow,
  LineAlertRow,
  LineMovementHistoryRow,
  NflHealthIssue,
  NflVegasBoardRow,
  DetectorHealthRow,
} from "@/db/queries";
import {
  MIN_SETTLED_FOR_CI,
  disclosure,
  multiplicityNote,
  verdict,
} from "@/lib/alert-audit-policy";
import DetectorHealthPanel from "../vegas/detector-health-panel";

type Props = {
  queryDate: string;
  evaluatedAt: string;
  matchups: NflVegasBoardRow[];
  lineAlerts: LineAlertRow[];
  lineAlertBacktest: LineAlertBacktestRow[];
  lineMovementHistory: LineMovementHistoryRow[];
  health: NflHealthIssue[];
  detectorHealth: DetectorHealthRow[];
};

function pct(value: number | null): string {
  return value == null || !Number.isFinite(value) ? "—" : `${(value * 100).toFixed(1)}%`;
}

function signed(value: number | null, suffix = ""): string {
  return value == null || !Number.isFinite(value) ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(1)}${suffix}`;
}

function moneyline(value: number | null): string {
  return value == null ? "—" : value > 0 ? `+${value}` : String(value);
}

function shiftDate(date: string, days: number): string {
  const parsed = new Date(`${date}T12:00:00Z`);
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return parsed.toISOString().slice(0, 10);
}

function fmtEt(value: string | null): string {
  if (!value) return "—";
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return "—";
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(parsed);
}

function alertTone(type: string): string {
  if (type === "pinnacle_polymarket_delta") return "border-fuchsia-200 bg-fuchsia-100 text-fuchsia-900";
  if (type === "pinnacle_divergence") return "border-violet-200 bg-violet-100 text-violet-900";
  if (type === "steam") return "border-orange-200 bg-orange-100 text-orange-900";
  if (type === "walking") return "border-blue-200 bg-blue-100 text-blue-900";
  return "border-emerald-200 bg-emerald-100 text-emerald-900";
}

function SummaryCard({ label, value, detail, tone }: { label: string; value: number; detail: string; tone: string }) {
  return <div className={`rounded-xl border p-4 shadow-sm ${tone}`}><div className="text-[10px] font-bold uppercase tracking-wide">{label}</div><div className="mt-1 text-2xl font-black">{value}</div><div className="mt-1 text-xs">{detail}</div></div>;
}

function MovementSparkline({ trail }: { trail: NflVegasBoardRow["trail"] }) {
  const values = trail.flatMap((point) => point.homeProb == null ? [] : [point.homeProb]);
  if (values.length < 2) return <span className="text-slate-400">—</span>;
  const width = 84;
  const height = 24;
  const min = Math.min(...values) - 0.002;
  const max = Math.max(...values) + 0.002;
  const span = Math.max(max - min, 0.004);
  const points = values.map((value, index) => {
    const x = index * width / (values.length - 1);
    const y = height - ((value - min) / span) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return <svg role="img" aria-label="Home win probability movement" viewBox={`0 0 ${width} ${height}`} className="h-6 w-20 text-emerald-700"><polyline points={points} fill="none" stroke="currentColor" strokeWidth="2" vectorEffect="non-scaling-stroke" /></svg>;
}

export default function NflVegasClient({ queryDate, evaluatedAt, matchups, lineAlerts, lineAlertBacktest, lineMovementHistory, health, detectorHealth }: Props) {
  const router = useRouter();
  const [isRefreshing, startRefresh] = useTransition();
  const navigateDate = (date: string) => router.push(`/nfl?date=${date}`);
  useEffect(() => {
    const id = window.setInterval(() => {
      if (document.visibilityState === "visible") startRefresh(() => router.refresh());
    }, 120_000);
    return () => window.clearInterval(id);
  }, [router]);
  const alertsByMatchup = useMemo(() => {
    const grouped = new Map<number, LineAlertRow[]>();
    for (const alert of lineAlerts) grouped.set(alert.matchupId, [...(grouped.get(alert.matchupId) ?? []), alert]);
    return grouped;
  }, [lineAlerts]);
  const capturedGames = matchups.filter((row) => row.captures >= 2).length;
  const notableMoves = matchups.filter((row) => Math.abs(row.movementPp ?? 0) >= 1).length;
  const matchupIds = new Set(matchups.map((row) => row.matchupId));
  const sharpGames = new Set(lineAlerts.filter((alert) => matchupIds.has(alert.matchupId)).map((alert) => alert.matchupId)).size;

  return (
    <div className="mx-auto max-w-7xl space-y-5 p-4 sm:p-6">
      <header className="rounded-2xl border border-slate-200 bg-gradient-to-br from-slate-950 via-slate-900 to-emerald-950 p-5 text-white shadow-lg sm:p-6">
        <div className="flex flex-wrap items-start justify-between gap-5">
          <div>
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200"><Target className="h-4 w-4" /> Vegas Analysis — NFL</div>
            <h1 className="mt-2 text-2xl font-black tracking-tight sm:text-3xl">NFL Line Movement</h1>
            <p className="mt-2 max-w-3xl text-sm text-slate-300">Track moneyline movement, spreads, totals, and sharp-market alerts from the NFL odds ledger.</p>
          </div>
          <div className="rounded-xl border border-white/15 bg-white/5 px-4 py-3 text-right"><div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Page evaluated</div><div className="mt-1 text-sm font-bold tabular-nums">{fmtEt(evaluatedAt)}</div></div>
        </div>
        <div className="mt-5 flex flex-wrap items-center gap-2">
          <button type="button" onClick={() => navigateDate(shiftDate(queryDate, -1))} className="inline-flex h-11 w-11 items-center justify-center rounded-lg border border-white/20 bg-white/10 hover:bg-white/15" aria-label="Previous date"><ChevronLeft className="h-5 w-5" /></button>
          <label className="flex min-h-11 items-center gap-2 rounded-lg border border-white/20 bg-white/10 px-3 text-xs font-semibold"><CalendarDays className="h-4 w-4" /><input type="date" value={queryDate} onChange={(event) => navigateDate(event.target.value)} className="bg-transparent text-sm font-semibold text-white [color-scheme:dark]" /></label>
          <button type="button" onClick={() => navigateDate(shiftDate(queryDate, 1))} className="inline-flex h-11 w-11 items-center justify-center rounded-lg border border-white/20 bg-white/10 hover:bg-white/15" aria-label="Next date"><ChevronRight className="h-5 w-5" /></button>
          <Link href="/dfs/nfl/review" className="ml-auto inline-flex min-h-11 items-center gap-2 rounded-lg border border-white/20 bg-white/10 px-4 py-2 text-xs font-bold text-white hover:bg-white/15">Weekly player review</Link>
          <Link href="/dfs/nfl" className="inline-flex min-h-11 items-center gap-2 rounded-lg border border-white/20 bg-white/10 px-4 py-2 text-xs font-bold text-white hover:bg-white/15">NFL DFS workspace</Link>
          <button type="button" onClick={() => startRefresh(() => router.refresh())} disabled={isRefreshing} className="inline-flex min-h-11 items-center gap-2 rounded-lg bg-emerald-500 px-4 py-2 text-xs font-bold text-white hover:bg-emerald-400 disabled:opacity-60"><RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />{isRefreshing ? "Refreshing" : "Refresh page"}</button>
        </div>
      </header>

      <section className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <SummaryCard label="Games" value={matchups.length} detail="On the selected date" tone="border-slate-200 bg-white text-slate-950" />
        <SummaryCard label="Movement ready" value={capturedGames} detail="Two or more captures" tone="border-blue-200 bg-blue-50 text-blue-950" />
        <SummaryCard label="Notable moves" value={notableMoves} detail="At least 1 percentage point" tone="border-amber-200 bg-amber-50 text-amber-950" />
        <SummaryCard label="Sharp signals" value={sharpGames} detail="Tracked alert games" tone="border-violet-200 bg-violet-50 text-violet-950" />
      </section>

      {health.length > 0 ? <section className={`rounded-xl border p-4 shadow-sm ${health.some((issue) => issue.severity === "error") ? "border-red-200 bg-red-50 text-red-950" : "border-amber-200 bg-amber-50 text-amber-950"}`}><div className="flex items-start gap-3"><AlertTriangle className="mt-0.5 h-5 w-5 shrink-0" /><div><h2 className="font-bold">NFL pipeline health needs attention</h2><ul className="mt-2 space-y-1 text-sm">{health.map((issue) => <li key={`${issue.matchupId}-${issue.code}`}><strong>{issue.matchup}:</strong> {issue.detail}</li>)}</ul></div></div></section> : null}

      {matchups.length > 0 && capturedGames === 0 ? <section className="rounded-xl border border-amber-200 bg-amber-50 p-5 text-amber-950 shadow-sm"><div className="flex items-start gap-3"><AlertTriangle className="mt-0.5 h-5 w-5 shrink-0" /><div><h2 className="font-bold">NFL games are scheduled but have no odds captures</h2><p className="mt-1 text-sm text-amber-900">The schedule is loaded for {queryDate}; the capture workflow has not written a usable pregame snapshot yet.</p></div></div></section> : null}

      <section>
        <div className="mb-3"><h2 className="text-lg font-bold text-slate-950">NFL movement board</h2><p className="mt-1 text-sm text-slate-600">Opening and current values are vig-free home win probabilities. Pin−Poly is Pinnacle minus Polymarket on the home team; positive values mean Pinnacle is more bullish on the home side.</p></div>
        <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-sm">
          <table className="min-w-[1200px] w-full text-xs">
            <thead className="bg-slate-50 text-left text-[10px] uppercase tracking-wide text-slate-500"><tr><th className="px-3 py-3">Game</th><th className="px-3 py-3">Open → current</th><th className="px-3 py-3 text-right">Movement</th><th className="px-3 py-3 text-right">Pin vs Poly</th><th className="px-3 py-3 text-right">Home spread</th><th className="px-3 py-3 text-right">Total</th><th className="px-3 py-3 text-right">Moneylines</th><th className="px-3 py-3">Sharp signals</th><th className="px-3 py-3">Updated</th></tr></thead>
            <tbody>{matchups.map((row) => {
              const move = row.movementPp ?? 0;
              const alerts = alertsByMatchup.get(row.matchupId) ?? [];
              const MoveIcon = move > 0 ? TrendingUp : move < 0 ? TrendingDown : Activity;
              return <tr key={row.matchupId} className="border-t border-slate-100 hover:bg-slate-50/80"><td className="px-3 py-3"><div className="flex items-center gap-2 font-bold text-slate-950">{row.awayTeam} @ {row.homeTeam}{row.seasonType === "preseason" ? <span className="rounded-full border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-[9px] uppercase text-amber-800">Preseason</span> : null}</div><div className="mt-0.5 text-[10px] text-slate-500">{fmtEt(row.commenceTime)} · {row.captures} capture{row.captures === 1 ? "" : "s"}</div></td><td className="whitespace-nowrap px-3 py-3 tabular-nums">{pct(row.openHomeProb)} → {pct(row.currentHomeProb)}</td><td className={`px-3 py-3 text-right font-black tabular-nums ${move > 0 ? "text-emerald-700" : move < 0 ? "text-red-700" : "text-slate-500"}`}><div className="flex items-center justify-end gap-2"><MovementSparkline trail={row.trail} /><span className="inline-flex items-center gap-1"><MoveIcon className="h-4 w-4" />{signed(row.movementPp, "pp")}</span></div></td><td className="whitespace-nowrap px-3 py-3 text-right tabular-nums" title="Vig-free home win probabilities"><div>Pin {pct(row.pinnacleHomeProb)} · Poly {pct(row.polymarketHomeProb)}</div><div className={`mt-0.5 font-black ${(row.pinnaclePolymarketDeltaPp ?? 0) > 0 ? "text-violet-700" : (row.pinnaclePolymarketDeltaPp ?? 0) < 0 ? "text-fuchsia-700" : "text-slate-400"}`}>Δ {signed(row.pinnaclePolymarketDeltaPp, "pp")}</div></td><td className="px-3 py-3 text-right font-semibold tabular-nums"><div>{signed(row.openHomeSpread)} → {signed(row.homeSpread)}</div><div className="mt-0.5 text-[10px] text-slate-500">Δ {signed(row.spreadMove)}</div></td><td className="px-3 py-3 text-right font-semibold tabular-nums"><div>{row.openTotal?.toFixed(1) ?? "—"} → {row.vegasTotal?.toFixed(1) ?? "—"}</div><div className="mt-0.5 text-[10px] text-slate-500">Δ {signed(row.totalMove)}</div></td><td className="px-3 py-3 text-right tabular-nums"><div>A {moneyline(row.awayMl)}</div><div>H {moneyline(row.homeMl)}</div></td><td className="max-w-[240px] px-3 py-3"><div className="flex flex-wrap gap-1">{alerts.map((alert) => <span key={`${alert.alertType}-${alert.side}`} className={`rounded-full border px-2 py-1 text-[10px] font-bold capitalize ${alertTone(alert.alertType)}`}>{alert.alertType.replaceAll("_", " ")} → {alert.side}</span>)}{alerts.length === 0 ? <span className="text-slate-400">None</span> : null}</div></td><td className="whitespace-nowrap px-3 py-3 text-slate-600">{fmtEt(row.latestCapturedAt)}</td></tr>;
            })}</tbody>
          </table>
          {matchups.length === 0 ? <div className="px-4 py-14 text-center text-sm text-slate-500">No NFL games are available for this date.</div> : null}
        </div>
      </section>

      <section>
        <div className="mb-3 flex items-end justify-between gap-3"><div><h2 className="text-lg font-bold text-slate-950">Sharp-signal accrual</h2><p className="mt-1 text-sm text-slate-600">NFL alerts use the same frozen-at-breach audit ledger as the MLB page, and the same {MIN_SETTLED_FOR_CI}-alert disclosure floor.</p></div><ShieldCheck className="h-5 w-5 text-slate-400" /></div>
        {multiplicityNote(lineAlertBacktest.length) ? <p className="mb-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-[11px] font-medium text-amber-900">{multiplicityNote(lineAlertBacktest.length)}</p> : null}
        {/* Rates, CLV and beat-close are WITHHELD below the floor — rendered as a
            lock, never as a greyed number. A greyed percentage is still a
            percentage; the eye reads it and it gets quoted back later. Raw
            W-L-P is always shown: "2-6" is an observation, "25.0%" is an
            inference the sample cannot support. Fixed sort order — sorting by
            performance is a false-discovery machine, it mechanically puts the
            luckiest detector on top and frames it as a ranking. */}
        <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-sm"><table className="min-w-[760px] w-full text-xs"><thead className="bg-slate-50 text-left text-[10px] uppercase tracking-wide text-slate-500"><tr><th className="px-3 py-3">Signal</th><th className="px-3 py-3 text-right">Fired</th><th className="px-3 py-3 text-right">Graded</th><th className="px-3 py-3 text-right">Priced</th><th className="px-3 py-3 text-right">W-L-P</th><th className="px-3 py-3 text-right">Avg CLV</th><th className="px-3 py-3 text-right">Beat close</th><th className="px-3 py-3 text-right">Win rate</th><th className="px-3 py-3 text-right">Status</th></tr></thead><tbody>{lineAlertBacktest.map((row) => { const isLine = row.alertType.startsWith("spread_") || row.alertType.startsWith("total_"); const d = disclosure(row); const v = verdict(row); const lock = <span className="cursor-help text-slate-400" title={d.reason}>🔒 {d.lockLabel}</span>; return <tr key={row.alertType} className="border-t border-slate-100"><td className="px-3 py-3 font-semibold capitalize">{row.alertType.replaceAll("_", " ")}</td><td className="px-3 py-3 text-right">{row.n}</td><td className="px-3 py-3 text-right">{row.nClv}</td><td className="px-3 py-3 text-right" title="Alerts carrying a frozen executable price. Price freezing shipped 2026-08-15; earlier alerts are structurally unpriced.">{row.nFrozenPrice}{row.nExecBooks > 1 ? <span className="text-slate-400"> · {row.nExecBooks} books</span> : null}</td><td className="px-3 py-3 text-right tabular-nums">{row.wins}-{row.losses}-{row.pushes}</td><td className="px-3 py-3 text-right tabular-nums">{d.disclosable ? signed(row.avgClvPp, isLine ? " pts" : "pp") : lock}</td><td className="px-3 py-3 text-right tabular-nums">{d.disclosable ? pct(row.beatClose) : lock}</td><td className="px-3 py-3 text-right tabular-nums">{d.disclosable ? pct(row.winRate) : lock}</td><td className="px-3 py-3 text-right"><span className={`cursor-help rounded-full px-2 py-0.5 text-[10px] font-semibold ${v.cls}`} title={v.tip}>{v.label}</span></td></tr>; })}</tbody></table>{lineAlertBacktest.length === 0 ? <div className="px-4 py-10 text-center text-sm text-slate-500">No settled NFL alert rows yet.</div> : null}</div>
      </section>

      <DetectorHealthPanel health={detectorHealth} />

      <details className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <summary className="cursor-pointer font-bold text-slate-950">Recent NFL open-to-close results ({lineMovementHistory.length})</summary>
        <div className="mt-4 overflow-x-auto"><table className="min-w-[760px] w-full text-xs"><thead className="text-left text-[10px] uppercase tracking-wide text-slate-500"><tr><th className="py-2">Date</th><th className="py-2">Game</th><th className="py-2 text-right">Open</th><th className="py-2 text-right">Close</th><th className="py-2">Moved toward</th><th className="py-2">Score</th><th className="py-2">Moved side won</th></tr></thead><tbody>{lineMovementHistory.map((row) => <tr key={`${row.matchupId}-${row.gameDate}`} className="border-t border-slate-100"><td className="py-2">{row.gameDate}</td><td className="py-2 font-semibold">{row.matchup}</td><td className="py-2 text-right">{pct(row.openProb)}</td><td className="py-2 text-right">{pct(row.closeProb)}</td><td className="py-2 capitalize">{row.movedToward ?? "quiet"}</td><td className="py-2">{row.score ?? "pending"}</td><td className="py-2">{row.movedSideWon == null ? "—" : row.movedSideWon ? "Yes" : "No"}</td></tr>)}</tbody></table>{lineMovementHistory.length === 0 ? <div className="py-8 text-center text-sm text-slate-500">No completed NFL movement history yet.</div> : null}</div>
      </details>
    </div>
  );
}
