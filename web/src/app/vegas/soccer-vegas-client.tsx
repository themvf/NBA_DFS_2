"use client";

import { useState, useMemo } from "react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import type {
  SoccerVegasMatchupRow,
  SoccerBetRow,
  SoccerBacktestTypeRow,
  SoccerFirstScorerRow,
  SoccerMatchGoalRow,
  SoccerPlayerStatsRow,
  SoccerFirstScorerTierRow,
  SoccerFirstScorerNearMissRow,
  SoccerTopPickRow,
  SoccerClvRow,
  SoccerCalibCutRow,
  SoccerClvTrendRow,
  SoccerSettlementIssue,
} from "@/db/queries";

const fmtMl = (ml: number | null) => (ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml));
const fmtPct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(0)}%`);
const fmtPct1 = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
const fmtGoals = (v: number | null) => (v == null ? "—" : v.toFixed(2));

const EDGE_PP = 0.05;
const TOTAL_EDGE = 0.2;

function fmtKickoff(commenceTime: string | null): string {
  if (!commenceTime) return "";
  const d = new Date(commenceTime);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

function fmtDayHeading(gameDate: string): string {
  const d = new Date(`${gameDate}T00:00:00`);
  if (Number.isNaN(d.getTime())) return gameDate;
  return d.toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric" });
}

// Bet date for the ledger: prefer the real kickoff timestamp formatted in the
// viewer's LOCAL timezone (a 01:00 UTC game is the prior evening in the US), and
// only fall back to the bare UTC-derived game_date when no timestamp exists.
function fmtBetDate(eventCommence: string | null, gameDate: string | null): string {
  if (eventCommence) {
    const d = new Date(eventCommence);
    if (!Number.isNaN(d.getTime())) {
      return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
    }
  }
  return gameDate
    ? new Date(`${gameDate}T00:00:00`).toLocaleDateString(undefined, { month: "short", day: "numeric" })
    : "?";
}

// Local-timezone YYYY-MM-DD for a fixture, so games group under the day they
// actually fall on for the viewer (a 01:00 UTC kickoff is the prior US evening),
// not the UTC-derived game_date. Falls back to game_date when no timestamp.
function localDateKey(commenceTime: string | null, gameDate: string): string {
  if (commenceTime) {
    const d = new Date(commenceTime);
    if (!Number.isNaN(d.getTime())) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    }
  }
  return gameDate;
}

const fmtSignedPp = (v: number) => `${v >= 0 ? "+" : ""}${(v * 100).toFixed(0)}%`;
const fmtSignedGoals = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(2)}`;
const fmtRoi = (v: number) => `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`;

const BET_TYPE_LABEL: Record<string, string> = {
  moneyline: "Moneyline",
  total: "Over/Under",
  draw_no_bet: "Win (DNB)",
  first_scorer: "First Scorer",
  outright_winner: "Outright Winner",
  group_winner: "Group Winner",
};

function Stars({ n }: { n: number }) {
  return (
    <span className="tabular-nums tracking-tight text-amber-400" title={`${n} of 5 stars`}>
      {"★".repeat(n)}
      <span className="text-muted-foreground/40">{"★".repeat(5 - n)}</span>
    </span>
  );
}

function StatusPill({ status }: { status: string }) {
  const cls =
    status === "won"
      ? "bg-emerald-500/15 text-emerald-400"
      : status === "lost"
        ? "bg-rose-500/15 text-rose-400"
        : status === "void"
          ? "bg-sky-500/15 text-sky-400"
          : "bg-muted text-muted-foreground";
  return <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium uppercase ${cls}`}>{status}</span>;
}

function ProbBar({ home, draw, away }: { home: number | null; draw: number | null; away: number | null }) {
  if (home == null || draw == null || away == null) {
    return <div className="h-1.5 w-full rounded bg-muted" />;
  }
  return (
    <div className="flex h-1.5 w-full overflow-hidden rounded">
      <div style={{ width: `${home * 100}%` }} className="bg-blue-500" />
      <div style={{ width: `${draw * 100}%` }} className="bg-gray-500" />
      <div style={{ width: `${away * 100}%` }} className="bg-rose-500" />
    </div>
  );
}

function bestEdge(m: SoccerVegasMatchupRow): { label: string; edge: number } | null {
  const cands: { label: string; edge: number }[] = [];
  if (m.ourProbHome != null && m.homeWinProb != null)
    cands.push({ label: m.homeAbbrev ?? m.homeTeam, edge: m.ourProbHome - m.homeWinProb });
  if (m.ourProbDraw != null && m.drawProb != null)
    cands.push({ label: "Draw", edge: m.ourProbDraw - m.drawProb });
  if (m.ourProbAway != null && m.awayWinProb != null)
    cands.push({ label: m.awayAbbrev ?? m.awayTeam, edge: m.ourProbAway - m.awayWinProb });
  if (cands.length === 0) return null;
  return cands.reduce((best, c) => (c.edge > best.edge ? c : best));
}

// ── Stat card ─────────────────────────────────────────────────────────────────
function StatCard({ label, value, sub, color }: {
  label: string; value: string; sub?: string; color?: string;
}) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={`mt-1 text-2xl font-bold tabular-nums ${color ?? ""}`}>{value}</div>
      {sub && <div className="mt-0.5 text-xs text-muted-foreground">{sub}</div>}
    </div>
  );
}

// ── Per-type KPI card ─────────────────────────────────────────────────────────
const BET_TYPE_ICON: Record<string, string> = {
  moneyline: "🎯",
  total: "📊",
  draw_no_bet: "2️⃣",
  first_scorer: "🥅",
  outright_winner: "🏆",
  group_winner: "🗂️",
  all: "📋",
};

// Deterministic per-type verdict for the backtest panel. Derived from the type's
// 3★+ market-bet tiers (the bets we'd actually place). `total` is HARDCODED to a
// no-edge verdict regardless of its small settled sample — a lucky positive ROI
// must never re-create the false impression the walk-forward disproved
// (spec §8 decision 5; memory soccer-totals-no-edge).
type Verdict = { tone: "good" | "warn" | "muted"; text: string };
function backtestVerdict(betType: string, rows: SoccerBacktestTypeRow[]): Verdict {
  if (betType === "total")
    return { tone: "warn", text: "No out-of-sample edge — capped at 2★ (walk-forward)" };
  const placed = rows.filter((r) => r.stars >= 3 && r.marketBets > 0);
  const n = placed.reduce((s, r) => s + r.marketBets, 0);
  if (n === 0) return { tone: "muted", text: "No graded plays at 3★+ yet" };
  // Aggregate ROI + realized-vs-expected across the placeable tiers.
  const wExp = placed.reduce((s, r) => s + r.expectedWinRate * r.n, 0);
  const wReal = placed.reduce((s, r) => s + r.realizedWinRate * r.n, 0);
  const totN = placed.reduce((s, r) => s + r.n, 0);
  const exp = totN > 0 ? wExp / totN : 0;
  const real = totN > 0 ? wReal / totN : 0;
  const roiVals = placed.filter((r) => r.roi != null);
  const roi = roiVals.length
    ? roiVals.reduce((s, r) => s + (r.roi as number) * r.marketBets, 0) / n
    : null;
  const positive = roi != null && roi > 0 && real >= exp;
  if (n < 10) return { tone: "muted", text: `Insufficient sample (n=${n})` };
  if (positive && n >= 20) return { tone: "good", text: `Edge holds (n=${n})` };
  if (positive) return { tone: "warn", text: `Leaning positive — small sample (n=${n})` };
  return { tone: "warn", text: `Underperforming (n=${n})` };
}

function BetTypeCard({ betType, won, lost, voided, sumExpected, nExpected, marketBets, profit }: {
  betType: string; won: number; lost: number; voided: number;
  sumExpected: number; nExpected: number; marketBets: number; profit: number;
}) {
  const nonVoid = won + lost;
  const wr = nonVoid > 0 ? won / nonVoid : null;
  const expectedWr = nExpected > 0 ? sumExpected / nExpected : null;
  const beating = wr != null && expectedWr != null && wr >= expectedWr;
  const roi = marketBets > 0 ? profit / marketBets : null;
  const total = won + lost + voided;

  return (
    <div className="rounded-lg border bg-card p-4 flex flex-col gap-2">
      {/* Header */}
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
          {BET_TYPE_ICON[betType] ?? "📌"} {BET_TYPE_LABEL[betType] ?? betType}
        </span>
        {roi != null && (
          <span className={`text-[10px] font-semibold rounded px-1.5 py-0.5 tabular-nums ${roi >= 0 ? "bg-emerald-500/15 text-emerald-400" : "bg-rose-500/15 text-rose-400"}`}>
            ROI {fmtRoi(roi)}
          </span>
        )}
      </div>

      {/* Win rate — big number */}
      <div className="flex items-end gap-2">
        <span className={`text-3xl font-bold tabular-nums leading-none ${wr != null && beating ? "text-emerald-400" : wr != null ? "text-rose-400" : "text-muted-foreground"}`}>
          {wr != null ? fmtPct1(wr) : "—"}
        </span>
        {expectedWr != null && (
          <span className="text-xs text-muted-foreground mb-0.5">
            exp {fmtPct1(expectedWr)}
          </span>
        )}
      </div>

      {/* W / L / push pills */}
      <div className="flex items-center gap-1.5 text-xs">
        <span className="rounded bg-emerald-500/15 px-1.5 py-0.5 font-medium text-emerald-400 tabular-nums">{won}W</span>
        <span className="rounded bg-rose-500/15 px-1.5 py-0.5 font-medium text-rose-400 tabular-nums">{lost}L</span>
        {voided > 0 && (
          <span className="rounded bg-muted px-1.5 py-0.5 text-muted-foreground tabular-nums">{voided} push</span>
        )}
        <span className="ml-auto text-muted-foreground">{total} games</span>
      </div>

      {/* Calibration badge */}
      {wr != null && expectedWr != null && nonVoid >= 3 && (
        <div className={`text-[10px] ${beating ? "text-emerald-400" : "text-rose-400"}`}>
          {beating
            ? `+${fmtPct1(wr - expectedWr)} above expected ✓`
            : `${fmtPct1(wr - expectedWr)} below expected`}
        </div>
      )}
      {nonVoid < 3 && (
        <div className="text-[10px] text-muted-foreground">need more data</div>
      )}
    </div>
  );
}

// ── Sortable column header ─────────────────────────────────────────────────────
type SortDir = "asc" | "desc";
function SortTh({
  col, label, sort, onSort, align = "center",
}: {
  col: string; label: string; sort: { col: string; dir: SortDir };
  onSort: (col: string) => void; align?: "left" | "center";
}) {
  const active = sort.col === col;
  return (
    <th
      onClick={() => onSort(col)}
      className={`cursor-pointer select-none px-3 py-2 text-${align} font-medium hover:text-foreground ${active ? "text-foreground" : "text-muted-foreground"}`}
    >
      {label}{active ? (sort.dir === "desc" ? " ↓" : " ↑") : ""}
    </th>
  );
}

// ── P&L chart ─────────────────────────────────────────────────────────────────
function PnlChart({ bets }: { bets: SoccerBetRow[] }) {
  const points = useMemo(() => {
    const settled = bets
      .filter((b) => b.status !== "pending" && b.status !== "void" && b.marketDecimal != null)
      .sort((a, b) => (a.gameDate ?? "").localeCompare(b.gameDate ?? ""));

    let cumPnl = 0;
    const pts: { label: string; pnl: number; bet: string }[] = [];
    for (const b of settled) {
      const pnl = b.status === "won" ? b.marketDecimal! - 1 : -1;
      cumPnl += pnl;
      const date = fmtBetDate(b.eventCommence, b.gameDate);
      pts.push({ label: date, pnl: Math.round(cumPnl * 100) / 100, bet: b.selectionLabel });
    }
    return pts;
  }, [bets]);

  if (points.length < 2) return null;

  const final = points[points.length - 1]?.pnl ?? 0;
  const peak = Math.max(...points.map((p) => p.pnl), 0);
  const trough = Math.min(...points.map((p) => p.pnl), 0);
  const positive = final >= 0;

  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Cumulative P&amp;L (units)
        </h3>
        <span className={`text-sm font-bold tabular-nums ${positive ? "text-emerald-400" : "text-rose-400"}`}>
          {final >= 0 ? "+" : ""}{final.toFixed(2)}u
        </span>
      </div>
      <div className="rounded-lg border bg-card p-3">
        <ResponsiveContainer width="100%" height={160}>
          <AreaChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={positive ? "#10b981" : "#f43f5e"} stopOpacity={0.25} />
                <stop offset="95%" stopColor={positive ? "#10b981" : "#f43f5e"} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => `${v > 0 ? "+" : ""}${v}`}
              domain={[Math.floor(trough - 0.5), Math.ceil(peak + 0.5)]}
              width={36}
            />
            <Tooltip
              contentStyle={{
                background: "hsl(var(--card))",
                border: "1px solid hsl(var(--border))",
                borderRadius: 6,
                fontSize: 11,
              }}
              formatter={(value) => {
                const v = typeof value === "number" ? value : 0;
                return [`${v >= 0 ? "+" : ""}${v.toFixed(2)}u`, "Cumul. P&L"];
              }}
              labelFormatter={(label, payload) => {
                const bet = payload?.[0]?.payload?.bet ?? "";
                return `${label}${bet ? ` · ${bet}` : ""}`;
              }}
            />
            <ReferenceLine y={0} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.4} strokeDasharray="4 4" />
            <Area
              type="monotone"
              dataKey="pnl"
              stroke={positive ? "#10b981" : "#f43f5e"}
              strokeWidth={2}
              fill="url(#pnlGrad)"
              dot={false}
              activeDot={{ r: 4, strokeWidth: 0 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
      <p className="mt-1 text-[11px] text-muted-foreground">
        1 unit staked per game (best-rated side only — excludes no-market group winner bets and pushes).
        Peak: {peak >= 0 ? "+" : ""}{peak.toFixed(2)}u · Trough: {trough.toFixed(2)}u · {points.length} settled bets.
      </p>
    </div>
  );
}

// ── Results panel ─────────────────────────────────────────────────────────────
/**
 * Closing Line Value — did the market move toward our side between when we rated
 * the bet and kickoff? The sharpest small-sample read on real edge: positive CLV
 * means we beat the close. The "rated" (3★+) row is what matters — it's the bets
 * we'd actually place.
 */
function ClvPanel({ clv }: { clv: SoccerClvRow[] }) {
  if (clv.length === 0) return null;
  const byKey = (t: string, tier: string) => clv.find((r) => r.betType === t && r.tier === tier);
  const markets: { type: string; label: string }[] = [
    { type: "moneyline", label: "Moneyline" },
    { type: "total", label: "Totals (O/U)" },
  ];
  const fmtClv = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(2)}pp`;
  const fmtPct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(0)}%`);

  return (
    <div className="rounded-lg border bg-card p-4">
      <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-1">
        Closing Line Value
      </h3>
      <p className="text-xs text-muted-foreground mb-3">
        Did the market move <strong className="text-foreground">toward our side</strong> between
        our rating and kickoff? Positive CLV = we beat the close = real edge — visible on far fewer
        bets than win/loss ROI needs. The <strong className="text-foreground">rated (3★+)</strong>
        row is the one that matters: it&rsquo;s the bets we&rsquo;d actually place.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-muted-foreground">
              <th className="py-1 text-left">Market</th>
              <th className="py-1 text-left">Tier</th>
              <th className="py-1 text-right">Bets</th>
              <th className="py-1 text-right">Avg CLV</th>
              <th className="py-1 text-right">Beat close</th>
            </tr>
          </thead>
          <tbody>
            {markets.flatMap((m) =>
              ["all", "rated"].map((tier) => {
                const r = byKey(m.type, tier);
                if (!r || r.n === 0) return null;
                const good = r.avgClvPp >= 0;
                return (
                  <tr key={`${m.type}-${tier}`} className={`border-b last:border-0 ${tier === "rated" ? "bg-accent/30 font-medium" : ""}`}>
                    <td className="py-1.5">{tier === "all" ? m.label : ""}</td>
                    <td className="py-1.5 text-muted-foreground">{tier === "rated" ? "3★+ (placed)" : "all"}</td>
                    <td className="py-1.5 text-right text-muted-foreground">{r.n}</td>
                    <td className={`py-1.5 text-right tabular-nums ${good ? "text-emerald-500" : "text-rose-500"}`}>
                      {fmtClv(r.avgClvPp)}
                    </td>
                    <td className={`py-1.5 text-right tabular-nums ${(r.beatRate ?? 0) >= 0.5 ? "text-emerald-500" : "text-rose-500"}`}>
                      {fmtPct(r.beatRate)}
                    </td>
                  </tr>
                );
              }),
            )}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-[11px] text-muted-foreground">
        Beat-close &lt; 50% or negative CLV on the rated row means the market is moving against our
        confident bets — a warning that those edges aren&rsquo;t real, independent of the win/loss
        sample. Watch this as model changes land.
      </p>
    </div>
  );
}

/**
 * Model diagnostics — calibration (realized − expected win%) sliced by the cuts
 * that matter, plus the rated-CLV trend. Makes systematic bias self-serve: the
 * Side row is exactly how the home/away bias surfaces without running a script.
 */
function DiagnosticsPanel({ cuts, clvTrend }: { cuts: SoccerCalibCutRow[]; clvTrend: SoccerClvTrendRow[] }) {
  if (cuts.length === 0) return null;
  const dims = Array.from(new Set(cuts.map((c) => c.dimension)));
  const pct = (v: number) => `${(v * 100).toFixed(0)}%`;
  const trend = clvTrend.filter((t) => t.n > 0).map((t) => ({
    date: new Date(`${t.date}T00:00:00`).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
    clv: Math.round(t.avgClvPp * 100) / 100,
    n: t.n,
  }));

  return (
    <div className="space-y-4">
      <div className="rounded-lg border bg-card p-4">
        <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-1">
          Calibration by cut
        </h3>
        <p className="text-xs text-muted-foreground mb-3">
          Realized minus expected win% on settled bets, sliced by the levers we can act on. A
          persistent <strong className="text-foreground">gap</strong> in one bucket is a systematic
          bias to fix — e.g. the <strong className="text-foreground">Side</strong> rows are how the
          home/away miss shows up. Each row is a group mean, so it stays honest on a small sample;
          mind low <em>n</em>.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="border-b text-muted-foreground">
                <th className="py-1 text-left">Cut</th>
                <th className="py-1 text-left">Bucket</th>
                <th className="py-1 text-right">n</th>
                <th className="py-1 text-right">Exp</th>
                <th className="py-1 text-right">Real</th>
                <th className="py-1 text-right">Gap</th>
              </tr>
            </thead>
            <tbody>
              {dims.flatMap((dim) =>
                cuts.filter((c) => c.dimension === dim).map((c, i) => {
                  const gap = c.realized - c.expected;
                  const big = Math.abs(gap) >= 0.05 && c.n >= 15;
                  return (
                    <tr key={`${dim}-${c.bucket}`} className="border-b last:border-0">
                      <td className="py-1.5 text-muted-foreground">{i === 0 ? dim : ""}</td>
                      <td className="py-1.5">{c.bucket}</td>
                      <td className="py-1.5 text-right text-muted-foreground">{c.n}</td>
                      <td className="py-1.5 text-right tabular-nums text-muted-foreground">{pct(c.expected)}</td>
                      <td className="py-1.5 text-right tabular-nums">{pct(c.realized)}</td>
                      <td className={`py-1.5 text-right tabular-nums font-medium ${big ? (gap > 0 ? "text-emerald-500" : "text-rose-500") : "text-muted-foreground"}`}>
                        {gap >= 0 ? "+" : ""}{(gap * 100).toFixed(0)}pp
                      </td>
                    </tr>
                  );
                }),
              )}
            </tbody>
          </table>
        </div>
      </div>

      {trend.length > 1 && (
        <div className="rounded-lg border bg-card p-4">
          <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-1">
            Rated CLV trend
          </h3>
          <p className="text-xs text-muted-foreground mb-3">
            Closing-line value of our 3★+ bets by game date. This is the &ldquo;is it working?&rdquo;
            view — as model changes land, watch the line climb toward and above zero.
          </p>
          <ResponsiveContainer width="100%" height={160}>
            <AreaChart data={trend} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
              <defs>
                <linearGradient id="clvGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.4} />
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 10 }} width={40} tickFormatter={(v) => `${v}pp`} />
              <ReferenceLine y={0} stroke="#9ca3af" strokeDasharray="3 3" />
              <Tooltip
                formatter={(value) => {
                  const v = typeof value === "number" ? value : 0;
                  return [`${v >= 0 ? "+" : ""}${v.toFixed(2)}pp`, "Avg CLV"];
                }}
              />
              <Area type="monotone" dataKey="clv" stroke="#10b981" fill="url(#clvGrad)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

function ResultsPanel({
  bets,
  backtest,
  clv,
  calibCuts,
  clvTrend,
  fscorerTiers,
  fscorerNearMisses,
  topPickAccuracy,
  settlementHealth,
}: {
  bets: SoccerBetRow[];
  backtest: SoccerBacktestTypeRow[];
  clv: SoccerClvRow[];
  calibCuts: SoccerCalibCutRow[];
  clvTrend: SoccerClvTrendRow[];
  fscorerTiers: SoccerFirstScorerTierRow[];
  fscorerNearMisses: SoccerFirstScorerNearMissRow[];
  topPickAccuracy: SoccerTopPickRow[];
  settlementHealth: SoccerSettlementIssue[];
}) {
  // ── Settled bet table state ──────────────────────────────────────────────────
  const [tType, setTType] = useState("all");
  const [tStatus, setTStatus] = useState("all");
  const [tMinStars, setTMinStars] = useState(1);
  const [tSearch, setTSearch] = useState("");
  // Backtest panel: which bet type's calibration to show. Default 'moneyline' —
  // the market that carries edge — so the panel opens on the meaningful view
  // rather than the blended 'all' rollup (spec §8 decision 2).
  const [btType, setBtType] = useState("moneyline");
  const [sort, setSort] = useState<{ col: string; dir: SortDir }>({ col: "gameDate", dir: "desc" });

  function toggleSort(col: string) {
    setSort((s) => s.col === col ? { col, dir: s.dir === "desc" ? "asc" : "desc" } : { col, dir: "desc" });
  }

  const searchLower = tSearch.toLowerCase();
  const filteredBets = bets.filter(
    (b) =>
      (tType === "all" || b.betType === tType) &&
      (tStatus === "all" || b.status === tStatus) &&
      b.stars >= tMinStars &&
      (tSearch === "" ||
        b.selectionLabel.toLowerCase().includes(searchLower) ||
        (b.fixture ?? "").toLowerCase().includes(searchLower)),
  ).sort((a, b) => {
    let av: number | string, bv: number | string;
    if (sort.col === "stars")       { av = a.stars;              bv = b.stars; }
    else if (sort.col === "ev")     { av = a.ev ?? -99;          bv = b.ev ?? -99; }
    else if (sort.col === "ourProb"){ av = a.ourProb;            bv = b.ourProb; }
    else if (sort.col === "edge")   { av = a.edge ?? -99;        bv = b.edge ?? -99; }
    else if (sort.col === "gameDate") { av = a.gameDate ?? "";   bv = b.gameDate ?? ""; }
    else if (sort.col === "status") {
      const order: Record<string, number> = { won: 0, lost: 1, void: 2 };
      av = order[a.status] ?? 3; bv = order[b.status] ?? 3;
    }
    else { av = a.gameDate ?? ""; bv = b.gameDate ?? ""; }
    if (typeof av === "string" && typeof bv === "string")
      return sort.dir === "desc" ? bv.localeCompare(av) : av.localeCompare(bv);
    return sort.dir === "desc" ? (bv as number) - (av as number) : (av as number) - (bv as number);
  });

  // ── Best-per-game: for multi-side markets (moneyline: 3 sides, total: 2 sides)
  // pick the highest-rated side per (betType, scope) so the KPI cards reflect
  // "if we bet the best side of each game, how did we do?" not all sides at once.
  //
  // first_scorer is special: it has 15+ mutually-exclusive selections per game.
  // Ranking those by stars/EV always surfaces the biggest-edge LONGSHOT (e.g. our
  // 8% vs the market's 2.6% — a ~38/1 shot) which by construction almost never
  // wins, making the panel read 0% and misrepresenting model accuracy. For this
  // market the meaningful "best pick" is the model's FAVORITE (highest our_prob),
  // i.e. who we actually think is most likely to score first.
  const bestPerGame = useMemo(() => {
    const map = new Map<string, SoccerBetRow>();
    for (const b of bets) {
      if (b.status === "pending") continue;
      const key = `${b.betType}::${b.scope}`;
      const cur = map.get(key);
      const better =
        !cur ||
        (b.betType === "first_scorer"
          ? b.ourProb > cur.ourProb
          : b.stars > cur.stars || (b.stars === cur.stars && (b.ev ?? -99) > (cur.ev ?? -99)));
      if (better) map.set(key, b);
    }
    return Array.from(map.values());
  }, [bets]);

  // ── Aggregate stats — one bet per game per type, filtered by tMinStars ────────
  const starFilteredBest = bestPerGame.filter((b) => b.stars >= tMinStars);

  const byTypeMap = new Map<string, { won: number; lost: number; voided: number; sumExpected: number; nExpected: number; marketBets: number; profit: number }>();
  for (const b of starFilteredBest) {
    const entry = byTypeMap.get(b.betType) ?? { won: 0, lost: 0, voided: 0, sumExpected: 0, nExpected: 0, marketBets: 0, profit: 0 };
    if (b.status === "won") entry.won++;
    else if (b.status === "lost") entry.lost++;
    else if (b.status === "void") entry.voided++;
    if (b.status !== "void") { entry.sumExpected += b.ourProb; entry.nExpected++; }
    if (b.marketDecimal != null && b.status !== "void") {
      entry.marketBets++;
      entry.profit += b.status === "won" ? b.marketDecimal - 1 : -1;
    }
    byTypeMap.set(b.betType, entry);
  }
  const byTypeRows = Array.from(byTypeMap.entries())
    .map(([betType, e]) => ({ betType, ...e }))
    .sort((a, b) => a.betType.localeCompare(b.betType));

  const totalWon = starFilteredBest.filter((b) => b.status === "won").length;
  const totalLost = starFilteredBest.filter((b) => b.status === "lost").length;
  const totalVoid = starFilteredBest.filter((b) => b.status === "void").length;
  const totalSettled = totalWon + totalLost + totalVoid;
  const totalMarket = byTypeRows.reduce((s, r) => s + r.marketBets, 0);
  const totalProfit = byTypeRows.reduce((s, r) => s + r.profit, 0);
  const overallWinRate = (totalWon + totalLost) > 0 ? totalWon / (totalWon + totalLost) : null;
  const overallRoi = totalMarket > 0 ? totalProfit / totalMarket : null;

  if (totalSettled === 0 && bets.length === 0) {
    return (
      <section className="space-y-3">
        <h2 className="text-lg font-bold">📈 Results &amp; Analytics</h2>
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No settled bets yet. Results populate as games finish and scores are confirmed. Check
          back after group-stage matches complete.
        </div>
      </section>
    );
  }

  return (
    <section className="space-y-5">
      <h2 className="text-lg font-bold">📈 Results &amp; Analytics</h2>

      {/* Settlement health alert — only renders when something is wrong, so it's a
          real alert, not noise. Catches the silent-rot failure mode (stuck/wrong
          first-scorer settlements) that hid the wrong-void bug for weeks. */}
      {settlementHealth.length > 0 && (
        <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 p-4 text-sm">
          <div className="font-semibold text-amber-700 dark:text-amber-400">
            ⚠ Settlement needs attention ({settlementHealth.length})
          </div>
          <ul className="mt-2 space-y-1 text-muted-foreground">
            {settlementHealth.map((h) => (
              <li key={`${h.kind}-${h.gameId}`}>
                {h.kind === "badVoid" ? (
                  <>
                    <span className="font-medium text-foreground">{h.fixture} ({h.score})</span>{" "}
                    — voided “No goals” but the game had goals; reopen &amp; re-settle ({h.nBets} bets).
                  </>
                ) : (
                  <>
                    <span className="font-medium text-foreground">{h.fixture} ({h.score})</span>{" "}
                    — {h.nBets} first-scorer bets stuck pending (no timeline from feed). Settle:{" "}
                    <code className="rounded bg-muted px-1 py-0.5 text-xs">
                      python -m ingest.soccer_results --first-scorer {h.gameId} &quot;&lt;player&gt;&quot;
                    </code>
                  </>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Overall summary strip */}
      <div className="flex flex-wrap items-center gap-4 rounded-lg border bg-card px-4 py-3 text-sm">
        <div>
          <span className="text-muted-foreground text-xs">Games bet</span>
          <span className="ml-1.5 font-bold tabular-nums">{totalSettled}</span>
          <span className="ml-1 text-xs text-muted-foreground">({totalWon}W · {totalLost}L · {totalVoid} push)</span>
        </div>
        <div className="h-4 w-px bg-border hidden sm:block" />
        <div>
          <span className="text-muted-foreground text-xs">Win rate</span>
          <span className={`ml-1.5 font-bold tabular-nums ${overallWinRate != null && overallWinRate >= 0.5 ? "text-emerald-400" : "text-rose-400"}`}>
            {overallWinRate != null ? fmtPct1(overallWinRate) : "—"}
          </span>
        </div>
        <div className="h-4 w-px bg-border hidden sm:block" />
        <div>
          <span className="text-muted-foreground text-xs">Overall ROI</span>
          <span className={`ml-1.5 font-bold tabular-nums ${overallRoi != null && overallRoi >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
            {overallRoi != null ? fmtRoi(overallRoi) : "—"}
          </span>
          <span className="ml-1 text-xs text-muted-foreground">({totalMarket} bets)</span>
        </div>
        <div className="h-4 w-px bg-border hidden sm:block" />
        <div>
          <span className="text-muted-foreground text-xs">Star tiers</span>
          <span className="ml-1.5 font-bold tabular-nums">
            {backtest.filter((r) => r.betType === "all").length}
          </span>
        </div>
      </div>

      {/* Per-type KPI cards */}
      {byTypeRows.length > 0 && (
        <div>
          <h3 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            By bet type{tMinStars > 1 ? ` — ${tMinStars}★+ only` : ""}
          </h3>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
            {byTypeRows.map((r) => (
              <BetTypeCard key={r.betType} {...r} />
            ))}
          </div>
          <p className="mt-1.5 text-[11px] text-muted-foreground">
            1 bet per game — best-rated side only (moneyline: home/draw/away; O/U: over/under).
            Win % excludes pushes. ROI = profit per unit staked at market odds.
          </p>
        </div>
      )}

      {/* Moneyline by side breakdown */}
      {(() => {
        const mlBets = bestPerGame.filter((b) => b.betType === "moneyline" && b.status !== "void" && b.side != null);
        if (mlBets.length === 0) return null;
        const totalGames = mlBets.length;
        const sides = [
          { key: "home", label: "Home", emoji: "🏠" },
          { key: "draw", label: "Draw", emoji: "🤝" },
          { key: "away", label: "Away", emoji: "✈️" },
        ];
        const sideStats = sides.map(({ key, label, emoji }) => {
          const rows = mlBets.filter((b) => b.side === key);
          const won = rows.filter((b) => b.status === "won").length;
          const lost = rows.filter((b) => b.status === "lost").length;
          const n = won + lost;
          const wr = n > 0 ? won / n : null;
          const expWr = n > 0 ? rows.filter(b => b.status !== "void").reduce((s, b) => s + b.ourProb, 0) / n : null;
          const marketRows = rows.filter((b) => b.marketDecimal != null);
          const profit = marketRows.reduce((s, b) => s + (b.status === "won" ? b.marketDecimal! - 1 : -1), 0);
          const roi = marketRows.length > 0 ? profit / marketRows.length : null;
          return { key, label, emoji, n, won, lost, wr, expWr, roi, pct: totalGames > 0 ? n / totalGames : 0 };
        });
        const dominant = sideStats.find((r) => r.pct >= 0.8);
        return (
          <div>
            <div className="mb-2 flex items-center gap-3">
              <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
                Moneyline — breakdown by side
              </h3>
              {dominant && (
                <span className="rounded bg-amber-500/15 px-2 py-0.5 text-[10px] font-medium text-amber-400">
                  Model leaned {dominant.label} in {dominant.n}/{totalGames} games
                </span>
              )}
            </div>
            <div className="overflow-x-auto rounded-lg border bg-card">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-[10px] uppercase text-muted-foreground">
                    <th className="px-3 py-2 text-left font-medium">Side</th>
                    <th className="px-2 py-2 text-center font-medium">Picked</th>
                    <th className="px-2 py-2 text-center font-medium">W</th>
                    <th className="px-2 py-2 text-center font-medium">L</th>
                    <th className="px-2 py-2 text-center font-medium">Win %</th>
                    <th className="px-2 py-2 text-center font-medium">Exp %</th>
                    <th className="px-2 py-2 text-center font-medium">ROI</th>
                  </tr>
                </thead>
                <tbody>
                  {sideStats.map((r) => (
                    <tr key={r.key} className={`border-b last:border-0 hover:bg-accent/40 ${r.n === 0 ? "opacity-40" : ""}`}>
                      <td className="px-3 py-2 font-medium">{r.emoji} {r.label}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {r.n > 0 ? <>{r.n} <span className="text-[10px]">({fmtPct(r.pct)})</span></> : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-emerald-400 font-medium">{r.n > 0 ? r.won : "—"}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-rose-400">{r.n > 0 ? r.lost : "—"}</td>
                      <td className={`px-2 py-2 text-center tabular-nums font-medium ${r.wr != null ? (r.wr >= (r.expWr ?? 0) ? "text-emerald-400" : "text-rose-400") : "text-muted-foreground"}`}>
                        {r.wr != null ? fmtPct1(r.wr) : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {r.expWr != null ? fmtPct1(r.expWr) : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        {r.roi != null ? (
                          <span className={r.roi >= 0 ? "text-emerald-400" : "text-rose-400"}>{fmtRoi(r.roi)}</span>
                        ) : <span className="text-muted-foreground">—</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="mt-1 text-[11px] text-muted-foreground">
              Best-rated side per game. Win % green = beating our own expected %. Faded rows = model never picked this side.
            </p>
          </div>
        );
      })()}

      {/* Over/Under by side breakdown */}
      {(() => {
        const ouBets = bestPerGame.filter((b) => b.betType === "total" && b.status !== "void");
        if (ouBets.length === 0) return null;
        const totalGames = ouBets.length;
        const sides = [
          { key: "over",  label: "Over",  emoji: "📈" },
          { key: "under", label: "Under", emoji: "📉" },
        ];
        const sideStats = sides.map(({ key, label, emoji }) => {
          const rows = ouBets.filter((b) => b.selectionLabel.toLowerCase().startsWith(key));
          const won = rows.filter((b) => b.status === "won").length;
          const lost = rows.filter((b) => b.status === "lost").length;
          const n = won + lost;
          const wr = n > 0 ? won / n : null;
          const expWr = n > 0 ? rows.filter(b => b.status !== "void").reduce((s, b) => s + b.ourProb, 0) / n : null;
          const marketRows = rows.filter((b) => b.marketDecimal != null);
          const profit = marketRows.reduce((s, b) => s + (b.status === "won" ? b.marketDecimal! - 1 : -1), 0);
          const roi = marketRows.length > 0 ? profit / marketRows.length : null;
          return { key, label, emoji, n, won, lost, wr, expWr, roi, pct: totalGames > 0 ? n / totalGames : 0 };
        });
        const dominant = sideStats.find((r) => r.pct >= 0.8);
        return (
          <div>
            <div className="mb-2 flex items-center gap-3">
              <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
                Over/Under — breakdown by side
              </h3>
              {dominant && (
                <span className="rounded bg-amber-500/15 px-2 py-0.5 text-[10px] font-medium text-amber-400">
                  Model leaned {dominant.label} in {dominant.n}/{totalGames} games
                </span>
              )}
            </div>
            <div className="overflow-x-auto rounded-lg border bg-card">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-[10px] uppercase text-muted-foreground">
                    <th className="px-3 py-2 text-left font-medium">Side</th>
                    <th className="px-2 py-2 text-center font-medium">Picked</th>
                    <th className="px-2 py-2 text-center font-medium">W</th>
                    <th className="px-2 py-2 text-center font-medium">L</th>
                    <th className="px-2 py-2 text-center font-medium">Win %</th>
                    <th className="px-2 py-2 text-center font-medium">Exp %</th>
                    <th className="px-2 py-2 text-center font-medium">ROI</th>
                  </tr>
                </thead>
                <tbody>
                  {sideStats.map((r) => (
                    <tr key={r.key} className={`border-b last:border-0 hover:bg-accent/40 ${r.n === 0 ? "opacity-40" : ""}`}>
                      <td className="px-3 py-2 font-medium">{r.emoji} {r.label}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {r.n > 0 ? <>{r.n} <span className="text-[10px]">({fmtPct(r.pct)})</span></> : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-emerald-400 font-medium">{r.n > 0 ? r.won : "—"}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-rose-400">{r.n > 0 ? r.lost : "—"}</td>
                      <td className={`px-2 py-2 text-center tabular-nums font-medium ${r.wr != null ? (r.wr >= (r.expWr ?? 0) ? "text-emerald-400" : "text-rose-400") : "text-muted-foreground"}`}>
                        {r.wr != null ? fmtPct1(r.wr) : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {r.expWr != null ? fmtPct1(r.expWr) : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        {r.roi != null ? (
                          <span className={r.roi >= 0 ? "text-emerald-400" : "text-rose-400"}>{fmtRoi(r.roi)}</span>
                        ) : <span className="text-muted-foreground">—</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="mt-1 text-[11px] text-muted-foreground">
              Best-rated side per game. Win % green = beating our own expected %. Faded rows = model never picked this side.
            </p>
          </div>
        );
      })()}

      {/* P&L chart — same best-per-game pool as the KPI cards */}
      <PnlChart bets={bestPerGame} />

      {/* Individual settled bets — filterable + sortable */}
      <div>
        <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
          <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            Settled bets ({filteredBets.length}{filteredBets.length !== bets.length ? ` of ${bets.length}` : ""})
          </h3>
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <input
              type="text"
              placeholder="Search team / selection…"
              value={tSearch}
              onChange={(e) => setTSearch(e.target.value)}
              className="rounded border bg-background px-2 py-1 text-xs placeholder:text-muted-foreground w-44"
            />
            <label className="flex items-center gap-1">
              <span className="text-muted-foreground">Type</span>
              <select value={tType} onChange={(e) => setTType(e.target.value)}
                className="rounded border bg-background px-1.5 py-1">
                <option value="all">All</option>
                <option value="moneyline">Moneyline</option>
                <option value="total">Over/Under</option>
                <option value="draw_no_bet">Win (DNB)</option>
                <option value="outright_winner">Outright</option>
                <option value="group_winner">Group Winner</option>
                <option value="first_scorer">First Scorer</option>
              </select>
            </label>
            <label className="flex items-center gap-1">
              <span className="text-muted-foreground">Result</span>
              <select value={tStatus} onChange={(e) => setTStatus(e.target.value)}
                className="rounded border bg-background px-1.5 py-1">
                <option value="all">All</option>
                <option value="won">Won</option>
                <option value="lost">Lost</option>
                <option value="void">Push</option>
              </select>
            </label>
            <label className="flex items-center gap-1">
              <span className="text-muted-foreground">Min ★</span>
              <select value={tMinStars} onChange={(e) => setTMinStars(Number(e.target.value))}
                className="rounded border bg-background px-1.5 py-1">
                {[1, 2, 3, 4, 5].map((s) => <option key={s} value={s}>{s}★+</option>)}
              </select>
            </label>
          </div>
        </div>

        {filteredBets.length === 0 ? (
          <div className="rounded-lg border bg-card p-4 text-sm text-muted-foreground">
            {bets.length === 0
              ? "No settled bets yet — check back after group-stage matches complete."
              : "No bets match these filters."}
          </div>
        ) : (
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[820px] text-sm">
              <thead>
                <tr className="border-b text-xs">
                  <SortTh col="gameDate" label="Date" sort={sort} onSort={toggleSort} align="left" />
                  <SortTh col="stars" label="Rating" sort={sort} onSort={toggleSort} align="left" />
                  <th className="px-3 py-2 text-left text-xs font-medium text-muted-foreground">Type</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-muted-foreground">Selection</th>
                  <SortTh col="ourProb" label="Our %" sort={sort} onSort={toggleSort} />
                  <th className="px-3 py-2 text-center text-xs font-medium text-muted-foreground">Mkt %</th>
                  <th className="px-3 py-2 text-center text-xs font-medium text-muted-foreground">Odds</th>
                  <SortTh col="ev" label="EV" sort={sort} onSort={toggleSort} />
                  <SortTh col="edge" label="Edge" sort={sort} onSort={toggleSort} />
                  <th className="px-3 py-2 text-center text-xs font-medium text-muted-foreground">Score</th>
                  <SortTh col="status" label="Result" sort={sort} onSort={toggleSort} />
                </tr>
              </thead>
              <tbody>
                {filteredBets.map((b) => (
                  <tr key={b.id} className="border-b last:border-0 hover:bg-accent/40">
                    <td className="px-3 py-2 text-xs text-muted-foreground whitespace-nowrap">
                      {b.eventCommence || b.gameDate ? fmtBetDate(b.eventCommence, b.gameDate) : "—"}
                    </td>
                    <td className="px-3 py-2"><Stars n={b.stars} /></td>
                    <td className="px-3 py-2 text-xs text-muted-foreground whitespace-nowrap">
                      {BET_TYPE_LABEL[b.betType] ?? b.betType}
                    </td>
                    <td className="px-3 py-2">
                      <div className="font-medium leading-tight">{b.selectionLabel}</div>
                      {b.fixture && (
                        <div className="text-[10px] text-muted-foreground">{b.fixture}</div>
                      )}
                    </td>
                    <td className="px-3 py-2 text-center tabular-nums">{fmtPct(b.ourProb)}</td>
                    <td className="px-3 py-2 text-center tabular-nums text-muted-foreground">
                      {b.marketProb != null ? fmtPct(b.marketProb) : "—"}
                    </td>
                    <td className="px-3 py-2 text-center tabular-nums">{fmtMl(b.marketOdds)}</td>
                    <td className="px-3 py-2 text-center tabular-nums">
                      {b.ev != null ? (
                        <span className={b.ev > 0 ? "text-emerald-400" : "text-muted-foreground"}>
                          {fmtSignedPp(b.ev)}
                        </span>
                      ) : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-3 py-2 text-center tabular-nums">
                      {b.edge != null ? (
                        <span className={b.edge > 0 ? "text-emerald-400" : "text-rose-400"}>
                          {fmtSignedPp(b.edge)}
                        </span>
                      ) : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-3 py-2 text-center text-xs text-muted-foreground whitespace-nowrap">
                      {b.resultDetail ?? "—"}
                    </td>
                    <td className="px-3 py-2 text-center">
                      <StatusPill status={b.status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Closing line value — leading indicator of real edge */}
      <ClvPanel clv={clv} />

      {/* Model diagnostics — calibration by cut + rated CLV trend */}
      <DiagnosticsPanel cuts={calibCuts} clvTrend={clvTrend} />

      {/* By star rating */}
      {backtest.length > 0 && (() => {
        // Bet types present in the settled data (exclude the 'all' rollup from the
        // selector — it's reachable via its own pill). Ordered: markets we care
        // about first, then the rollup.
        const present = Array.from(new Set(backtest.map((r) => r.betType)));
        const ORDER = ["moneyline", "draw_no_bet", "total", "first_scorer",
                       "outright_winner", "group_winner", "all"];
        const types = ORDER.filter((t) => present.includes(t));
        // Fall back to 'all' if the default (moneyline) has no settled rows yet.
        const activeType = types.includes(btType) ? btType : (types[0] ?? "all");
        const rows = backtest.filter((r) => r.betType === activeType)
          .sort((a, b) => b.stars - a.stars);
        const verdict = backtestVerdict(activeType, rows);
        const versions = Array.from(new Set(rows.flatMap((r) => r.modelVersions)))
          .map((v) => v.replace(/^(gameline-|firstscorer-|futures-)/, ""));
        const vTone = verdict.tone === "good" ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
          : verdict.tone === "warn" ? "border-amber-500/40 bg-amber-500/10 text-amber-300"
          : "border-border bg-muted/30 text-muted-foreground";
        return (
        <div>
          <h3 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            Results by star rating
          </h3>
          <div className="mb-2 flex flex-wrap gap-1.5">
            {types.map((t) => (
              <button
                key={t}
                onClick={() => setBtType(t)}
                className={`rounded-full border px-2.5 py-1 text-xs font-medium transition ${
                  t === activeType ? "border-foreground bg-foreground text-background"
                    : "border-border bg-background text-muted-foreground hover:text-foreground"
                }`}
              >
                {BET_TYPE_ICON[t] ?? "📌"} {t === "all" ? "All (rollup)" : (BET_TYPE_LABEL[t] ?? t)}
              </button>
            ))}
          </div>
          <div className={`mb-2 flex items-center justify-between gap-2 rounded-lg border px-3 py-1.5 text-xs font-medium ${vTone}`}>
            <span>{verdict.text}</span>
            {versions.length > 0 && (
              <span className="font-normal opacity-70">model: {versions.join(", ")}</span>
            )}
          </div>
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[500px] text-sm">
              <thead>
                <tr className="border-b text-xs text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Tier</th>
                  <th className="px-2 py-2 text-center font-medium">Settled</th>
                  <th className="px-2 py-2 text-center font-medium">Won</th>
                  <th className="px-2 py-2 text-center font-medium">Win %</th>
                  <th className="px-2 py-2 text-center font-medium">Expected</th>
                  <th className="px-2 py-2 text-center font-medium">ROI</th>
                  <th className="px-2 py-2 text-left font-medium">Calibration</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const beat = r.realizedWinRate >= r.expectedWinRate;
                  const calibGap = r.realizedWinRate - r.expectedWinRate;
                  return (
                    <tr key={r.stars} className="border-b last:border-0">
                      <td className="px-3 py-2"><Stars n={r.stars} /></td>
                      <td className="px-2 py-2 text-center tabular-nums">{r.n}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-emerald-400">
                        {Math.round(r.realizedWinRate * r.n)}
                      </td>
                      <td className={`px-2 py-2 text-center tabular-nums font-medium ${beat ? "text-emerald-400" : "text-rose-400"}`}>
                        {fmtPct1(r.realizedWinRate)}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {fmtPct1(r.expectedWinRate)}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        {r.roi != null ? (
                          <span className={r.roi >= 0 ? "text-emerald-400" : "text-rose-400"}>
                            {fmtRoi(r.roi)}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="px-2 py-2 text-xs">
                        {beat ? (
                          <span className="text-emerald-400">
                            +{fmtPct1(calibGap)} above expected ✓
                          </span>
                        ) : (
                          <span className="text-rose-400">
                            {fmtPct1(calibGap)} below expected
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="mt-1 text-[11px] text-muted-foreground">
            ROI is the profitability metric; win% vs expected is the calibration check. Voids excluded.
          </p>
        </div>
        );
      })()}

      <FirstScorerAnalysis
        tiers={fscorerTiers}
        nearMisses={fscorerNearMisses}
        topPicks={topPickAccuracy}
      />
    </section>
  );
}

// ── First scorer analytics ────────────────────────────────────────────────────
function FirstScorerAnalysis({
  tiers,
  nearMisses,
  topPicks,
}: {
  tiers: SoccerFirstScorerTierRow[];
  nearMisses: SoccerFirstScorerNearMissRow[];
  topPicks: SoccerTopPickRow[];
}) {
  const totalSettled = tiers.reduce((s, r) => s + r.n, 0);
  if (totalSettled === 0) return null;

  // Top-pick summary stats
  const completedGames = topPicks.filter((r) => r.actualFirstScorer != null);
  const topPickHits = completedGames.filter((r) => r.topPickWasFirst).length;
  const topPickScored = completedGames.filter((r) => r.topPickScored).length;
  const hitRate = completedGames.length > 0 ? topPickHits / completedGames.length : null;
  const scoredRate = completedGames.length > 0 ? topPickScored / completedGames.length : null;

  // Near-miss summary
  const scoredButNotFirst = nearMisses.filter((r) => r.scoredInMatch).length;
  const nearMissRate = nearMisses.length > 0 ? scoredButNotFirst / nearMisses.length : null;

  return (
    <div className="space-y-4 border-t pt-4">
      <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
        First Scorer — Model Diagnostics
      </h3>

      {/* Summary stat cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard
          label="Top pick hit rate"
          value={hitRate != null ? fmtPct1(hitRate) : "—"}
          sub={`${topPickHits}/${completedGames.length} games`}
          color={hitRate != null && hitRate > 0.12 ? "text-emerald-400" : "text-muted-foreground"}
        />
        <StatCard
          label="Top pick scored (any)"
          value={scoredRate != null ? fmtPct1(scoredRate) : "—"}
          sub="scored but may not be 1st"
          color={scoredRate != null && scoredRate > 0.25 ? "text-emerald-400" : "text-muted-foreground"}
        />
        <StatCard
          label="Near-miss rate"
          value={nearMissRate != null ? fmtPct1(nearMissRate) : "—"}
          sub={`${scoredButNotFirst}/${nearMisses.length} losses`}
          color="text-amber-400"
        />
        <StatCard
          label="Settled bets (1★)"
          value={String(totalSettled)}
          sub="excl. voids (no scorer)"
        />
      </div>

      {/* Indicator 1: Tier calibration */}
      <div>
        <h4 className="mb-2 text-xs font-semibold text-muted-foreground uppercase tracking-wide">
          1. Probability tier calibration
        </h4>
        <div className="overflow-x-auto rounded-lg border bg-card">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-[10px] uppercase text-muted-foreground">
                <th className="px-3 py-2 text-left font-medium">Prob bucket</th>
                <th className="px-2 py-2 text-center font-medium">Bets</th>
                <th className="px-2 py-2 text-center font-medium">Wins</th>
                <th className="px-2 py-2 text-center font-medium">Avg our %</th>
                <th className="px-2 py-2 text-center font-medium">Actual %</th>
                <th className="px-3 py-2 text-left font-medium">Calibration</th>
              </tr>
            </thead>
            <tbody>
              {tiers.map((r) => {
                const delta = r.realizedRate - r.avgOurProb;
                const calibrated = Math.abs(delta) < 0.03;
                return (
                  <tr key={r.tier} className="border-b last:border-0 hover:bg-accent/40">
                    <td className="px-3 py-2 text-xs font-medium">{r.tier}</td>
                    <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">{r.n}</td>
                    <td className="px-2 py-2 text-center tabular-nums text-emerald-400">{r.wins}</td>
                    <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">{fmtPct1(r.avgOurProb)}</td>
                    <td className={`px-2 py-2 text-center tabular-nums font-medium ${r.realizedRate >= r.avgOurProb ? "text-emerald-400" : "text-rose-400"}`}>
                      {fmtPct1(r.realizedRate)}
                    </td>
                    <td className="px-3 py-2 text-xs">
                      {r.n < 5 ? (
                        <span className="text-muted-foreground">need more data</span>
                      ) : calibrated ? (
                        <span className="text-emerald-400">well calibrated ✓</span>
                      ) : delta > 0 ? (
                        <span className="text-emerald-400">+{fmtPct1(delta)} above expected</span>
                      ) : (
                        <span className="text-rose-400">{fmtPct1(delta)} below expected</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <p className="mt-1 text-[11px] text-muted-foreground">
          Actual % = fraction of bets in this bucket where the player scored first.
          A calibrated model has Avg our % ≈ Actual %. Needs ~20+ bets per tier for significance.
        </p>
      </div>

      {/* Indicator 2: Near-misses */}
      {nearMisses.length > 0 && (
        <div>
          <h4 className="mb-2 text-xs font-semibold text-muted-foreground uppercase tracking-wide">
            2. Scored but not first (near-misses)
          </h4>
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-[10px] uppercase text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Player</th>
                  <th className="px-3 py-2 text-left font-medium">Game</th>
                  <th className="px-2 py-2 text-center font-medium">Our %</th>
                  <th className="px-2 py-2 text-center font-medium">Odds</th>
                  <th className="px-2 py-2 text-center font-medium">Scored?</th>
                  <th className="px-2 py-2 text-left font-medium">Minute(s)</th>
                </tr>
              </thead>
              <tbody>
                {nearMisses.filter((r) => r.scoredInMatch).slice(0, 15).map((r, i) => (
                  <tr key={i} className="border-b last:border-0 hover:bg-accent/40 bg-amber-500/5">
                    <td className="px-3 py-2 font-medium text-amber-400">{r.playerName}</td>
                    <td className="px-3 py-2 text-xs text-muted-foreground">{r.fixture}</td>
                    <td className="px-2 py-2 text-center tabular-nums">{fmtPct1(r.ourProb)}</td>
                    <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">{fmtMl(r.marketOdds)}</td>
                    <td className="px-2 py-2 text-center">
                      <span className="text-amber-400 font-medium">⚽ ×{r.goalCount}</span>
                    </td>
                    <td className="px-2 py-2 text-xs text-muted-foreground">
                      {r.goalMinutes.length > 0 ? r.goalMinutes.map((m) => `${m}'`).join(", ") : "—"}
                    </td>
                  </tr>
                ))}
                {nearMisses.filter((r) => !r.scoredInMatch).slice(0, 5).map((r, i) => (
                  <tr key={`miss-${i}`} className="border-b last:border-0 hover:bg-accent/40 opacity-50">
                    <td className="px-3 py-2 text-xs">{r.playerName}</td>
                    <td className="px-3 py-2 text-xs text-muted-foreground">{r.fixture}</td>
                    <td className="px-2 py-2 text-center tabular-nums">{fmtPct1(r.ourProb)}</td>
                    <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">{fmtMl(r.marketOdds)}</td>
                    <td className="px-2 py-2 text-center text-muted-foreground text-xs">—</td>
                    <td className="px-2 py-2 text-xs text-muted-foreground">—</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mt-1 text-[11px] text-muted-foreground">
            Amber rows = player scored in the match but wasn&apos;t first. Faded rows = player didn&apos;t score at all.
            Near-misses show directional accuracy: the model identified the right scorer, just wrong timing.
            Near-miss rate: <strong className="text-foreground">{nearMissRate != null ? fmtPct1(nearMissRate) : "—"}</strong> of lost bets.
          </p>
        </div>
      )}

      {/* Indicator 3: Top-pick accuracy per game */}
      {completedGames.length > 0 && (
        <div>
          <h4 className="mb-2 text-xs font-semibold text-muted-foreground uppercase tracking-wide">
            3. Top pick vs actual first scorer (per game)
          </h4>
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-[10px] uppercase text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Game</th>
                  <th className="px-3 py-2 text-left font-medium">Our top pick</th>
                  <th className="px-2 py-2 text-center font-medium">Our %</th>
                  <th className="px-3 py-2 text-left font-medium">Actual first scorer</th>
                  <th className="px-2 py-2 text-center font-medium">Hit?</th>
                </tr>
              </thead>
              <tbody>
                {completedGames.map((r, i) => (
                  <tr
                    key={i}
                    className={`border-b last:border-0 hover:bg-accent/40 ${r.topPickWasFirst ? "bg-emerald-500/10" : r.topPickScored ? "bg-amber-500/5" : ""}`}
                  >
                    <td className="px-3 py-2 text-xs text-muted-foreground">{r.fixture}</td>
                    <td className="px-3 py-2 font-medium text-sm">
                      {r.topPick}
                      {r.topPickScored && !r.topPickWasFirst && (
                        <span className="ml-1.5 text-[10px] text-amber-400">scored later</span>
                      )}
                    </td>
                    <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">{fmtPct1(r.topPickProb)}</td>
                    <td className="px-3 py-2 text-sm">
                      {r.actualFirstScorer ?? <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-2 py-2 text-center">
                      {r.topPickWasFirst ? (
                        <span className="text-emerald-400 font-bold">✓</span>
                      ) : (
                        <span className="text-rose-400 text-xs">✗</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mt-1 text-[11px] text-muted-foreground">
            Top pick = player with highest <em>our %</em> per game (model&apos;s #1 choice, not necessarily the bet value pick).
            Hit rate: <strong className="text-foreground">{hitRate != null ? fmtPct1(hitRate) : "—"}</strong> ({topPickHits}/{completedGames.length}).
            Amber = top pick scored but wasn&apos;t first (near-miss). Baseline for random pick ≈ 1/22 = ~4.5%.
          </p>
        </div>
      )}
    </div>
  );
}

// ── Bets panel ────────────────────────────────────────────────────────────────
function BetsPanel({ bets }: { bets: SoccerBetRow[] }) {
  const [minStars, setMinStars] = useState(4);
  const [betType, setBetType] = useState<string>("all");
  const filtered = bets.filter(
    (b) => b.stars >= minStars && (betType === "all" || b.betType === betType),
  );

  return (
    <section className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-lg font-bold">⭐ Star-Rated Bets</h2>
        <div className="flex items-center gap-3 text-xs">
          <label className="flex items-center gap-1">
            <span className="text-muted-foreground">Min stars</span>
            <select
              value={minStars}
              onChange={(e) => setMinStars(Number(e.target.value))}
              className="rounded border bg-background px-1.5 py-1"
            >
              {[1, 2, 3, 4, 5].map((s) => (
                <option key={s} value={s}>{s}★+</option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1">
            <span className="text-muted-foreground">Type</span>
            <select
              value={betType}
              onChange={(e) => setBetType(e.target.value)}
              className="rounded border bg-background px-1.5 py-1"
            >
              <option value="all">All</option>
              <option value="moneyline">Moneyline</option>
              <option value="total">Over/Under</option>
              <option value="draw_no_bet">Win (DNB)</option>
              <option value="outright_winner">Outright</option>
              <option value="group_winner">Group winner</option>
              <option value="first_scorer">First scorer</option>
            </select>
          </label>
        </div>
      </div>

      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        Every recommendation is logged to an auditable ledger with the model version and frozen
        inputs, and <strong className="text-foreground">locks at kickoff</strong> so the backtest
        uses the number we committed to. Stars combine EV (vs the offered price) and edge (vs the
        vig-free market). 1★ = avoid/fade. Note: first-scorer markets carry huge vig, so the model
        rates almost all of them 1★ — that &ldquo;don&rsquo;t bet&rdquo; signal is itself the value.
      </p>

      {filtered.length === 0 ? (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No bets at {minStars}★+ for this filter. Lower the threshold, or run{" "}
          <code className="rounded bg-muted px-1 py-0.5">model.soccer_futures</code> /{" "}
          <code className="rounded bg-muted px-1 py-0.5">model.soccer_first_scorer</code>.
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border bg-card">
          <table className="w-full min-w-[720px] text-sm">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="px-3 py-2 text-left font-medium">Rating</th>
                <th className="px-2 py-2 text-left font-medium">Type</th>
                <th className="px-2 py-2 text-left font-medium">Selection</th>
                <th className="px-2 py-2 text-center font-medium">Our %</th>
                <th className="px-2 py-2 text-center font-medium">Mkt %</th>
                <th className="px-2 py-2 text-center font-medium">Odds</th>
                <th className="px-2 py-2 text-center font-medium">EV</th>
                <th className="px-2 py-2 text-center font-medium">Result</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((b) => (
                <tr key={b.id} className="border-b last:border-0 hover:bg-accent/40">
                  <td className="px-3 py-2"><Stars n={b.stars} /></td>
                  <td className="px-2 py-2 text-xs text-muted-foreground">
                    {BET_TYPE_LABEL[b.betType] ?? b.betType}
                    {b.scope.startsWith("Group ") && <span className="ml-1">({b.scope})</span>}
                  </td>
                  <td className="px-2 py-2">
                    <div className="font-medium">{b.selectionLabel}</div>
                    {b.fixture && <div className="text-[10px] text-muted-foreground">{b.fixture}</div>}
                  </td>
                  <td className="px-2 py-2 text-center tabular-nums">{fmtPct(b.ourProb)}</td>
                  <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                    {b.marketProb != null ? fmtPct(b.marketProb) : "—"}
                  </td>
                  <td className="px-2 py-2 text-center tabular-nums">{fmtMl(b.marketOdds)}</td>
                  <td className="px-2 py-2 text-center tabular-nums">
                    {b.ev != null ? (
                      <span className={b.ev > 0 ? "text-emerald-400" : "text-muted-foreground"}>
                        {fmtSignedPp(b.ev)}
                      </span>
                    ) : (
                      <span className="text-muted-foreground" title="no market line">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-center"><StatusPill status={b.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

// ── First scorer panel ────────────────────────────────────────────────────────
// Normalize a name for fuzzy matching (strip accents, lowercase, alphanumeric only).
function normName(s: string) {
  return s.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function FirstScorerPanel({ rows, matchGoals }: { rows: SoccerFirstScorerRow[]; matchGoals: SoccerMatchGoalRow[] }) {
  // Index goals by gameId for fast lookup.
  const goalsByGame = new Map<string, SoccerMatchGoalRow[]>();
  for (const g of matchGoals) {
    const list = goalsByGame.get(g.gameId) ?? [];
    list.push(g);
    goalsByGame.set(g.gameId, list);
  }

  const byGame = new Map<string, SoccerFirstScorerRow[]>();
  for (const r of rows) {
    const list = byGame.get(r.gameId) ?? [];
    list.push(r);
    byGame.set(r.gameId, list);
  }
  const games = Array.from(byGame.keys());
  const anyBet = rows.some((r) => r.stars >= 3);

  return (
    <section className="space-y-3">
      <h2 className="text-lg font-bold">🥅 First Goal Scorer</h2>
      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        Top candidates per upcoming match: <strong className="text-foreground">Our %</strong> (stat-driven
        Poisson model — historical xG/90 from World Cup 2018 + 2022 + Euro 2020 scaled to this
        match&rsquo;s predicted total) vs the best market price and its vig-free probability.{" "}
        {!anyBet && (
          <>
            First-scorer markets carry a huge built-in margin (~300–500% combined overround), so the
            model rates essentially all of them <strong className="text-foreground">1★ (don&rsquo;t
            bet)</strong> — that fade verdict is the value here. A 4–5★ would appear only if a soft
            price beat our number.
          </>
        )}
      </p>

      {games.length === 0 ? (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No first-scorer markets posted yet for upcoming games. Books list these ~1–2 days out.
        </div>
      ) : (
        <div className="grid gap-3 md:grid-cols-2">
          {games.map((gid) => {
            const list = byGame.get(gid) ?? [];
            const fixture = list[0]?.fixture ?? "Match";
            const goals = goalsByGame.get(gid) ?? [];
            const hasResult = goals.length > 0;
            const firstGoal = goals.find((g) => g.isFirstGoal);

            // Build goal lookup: normalized player name → { goalCount, isFirst, minute }
            const goalMap = new Map<string, SoccerMatchGoalRow>();
            for (const g of goals) goalMap.set(normName(g.playerName), g);

            return (
              <div key={gid} className="overflow-hidden rounded-lg border bg-card">
                <div className="flex items-center justify-between border-b px-3 py-2">
                  <span className="text-sm font-medium">{fixture}</span>
                  {hasResult && firstGoal && (
                    <span className="text-[10px] text-emerald-400 font-medium">
                      1st goal: {firstGoal.playerName} {firstGoal.firstGoalMinute != null ? `${firstGoal.firstGoalMinute}'` : ""}
                    </span>
                  )}
                </div>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-[10px] uppercase text-muted-foreground">
                      <th className="px-3 py-1.5 text-left font-medium">Player</th>
                      <th className="px-2 py-1.5 text-center font-medium">Our %</th>
                      <th className="px-2 py-1.5 text-center font-medium">Best Odds</th>
                      <th className="px-2 py-1.5 text-center font-medium">Mkt %</th>
                      <th className="px-2 py-1.5 text-center font-medium">Rating</th>
                      {hasResult && <th className="px-2 py-1.5 text-center font-medium">Goals</th>}
                    </tr>
                  </thead>
                  <tbody>
                    {list.map((r) => {
                      const g = goalMap.get(normName(r.player));
                      const scored = g != null && g.goalCount > 0;
                      const scoredFirst = g?.isFirstGoal ?? false;
                      return (
                        <tr
                          key={r.player}
                          className={`border-b last:border-0 hover:bg-accent/40 ${scoredFirst ? "bg-emerald-500/10" : ""}`}
                        >
                          <td className="px-3 py-1.5 font-medium">
                            {r.player}
                            {scoredFirst && (
                              <span className="ml-1.5 text-[10px] font-semibold text-emerald-400">1st</span>
                            )}
                          </td>
                          <td className="px-2 py-1.5 text-center tabular-nums">{fmtPct(r.ourProb)}</td>
                          <td className="px-2 py-1.5 text-center tabular-nums">{fmtMl(r.marketOdds)}</td>
                          <td className="px-2 py-1.5 text-center tabular-nums text-muted-foreground">
                            {r.marketProb != null ? fmtPct(r.marketProb) : "—"}
                          </td>
                          <td className="px-2 py-1.5 text-center"><Stars n={r.stars} /></td>
                          {hasResult && (
                            <td className="px-2 py-1.5 text-center tabular-nums">
                              {scored ? (
                                <span className={`font-semibold ${scoredFirst ? "text-emerald-400" : "text-foreground"}`}>
                                  {"⚽".repeat(g!.goalCount)}
                                </span>
                              ) : (
                                <span className="text-muted-foreground text-xs">—</span>
                              )}
                            </td>
                          )}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                {hasResult && goals.length > 0 && (
                  <div className="border-t px-3 py-1.5 text-[10px] text-muted-foreground">
                    All scorers:{" "}
                    {goals.map((g, i) => (
                      <span key={i} className={g.isFirstGoal ? "text-emerald-400 font-medium" : ""}>
                        {g.playerName}{g.goalCount > 1 ? ` ×${g.goalCount}` : ""}
                        {i < goals.length - 1 ? ", " : ""}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

// ── Fixtures panel ────────────────────────────────────────────────────────────
function FixturesPanel({ matchups, queryDate }: { matchups: SoccerVegasMatchupRow[]; queryDate: string | null }) {
  const byDate = new Map<string, SoccerVegasMatchupRow[]>();
  for (const m of matchups) {
    const key = localDateKey(m.commenceTime, m.gameDate);
    const list = byDate.get(key) ?? [];
    list.push(m);
    byDate.set(key, list);
  }
  const days = Array.from(byDate.keys()).sort();
  const hasModel = matchups.some((m) => m.ourTotalPred != null);

  return (
    <section className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-lg font-bold">📅 Fixtures — Our Model vs Vegas</h2>
        <span className="text-sm text-muted-foreground">
          {matchups.length} {queryDate ? `fixtures on ${queryDate}` : "upcoming fixtures"}
        </span>
      </div>

      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        <strong className="text-foreground">Our model</strong> is a bivariate-Poisson goal model
        driven by Elo + attack/defense ratings from 49k historical internationals, anchored to the
        market. The <strong className="text-foreground">Edge</strong> column flags where we most
        disagree with Vegas — positive = we rate that outcome higher than the books do. Win-prob
        bars: <span className="text-blue-500">home</span> / <span className="text-gray-400">draw</span>{" "}
        / <span className="text-rose-500">away</span>.
        {!hasModel && (
          <span className="mt-1 block text-amber-500">
            Model numbers not yet computed — run{" "}
            <code className="rounded bg-muted px-1 py-0.5">python -m model.soccer_predictions</code>.
          </span>
        )}
      </p>

      {matchups.length === 0 && (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No fixtures found. Run{" "}
          <code className="rounded bg-muted px-1 py-0.5">python -m ingest.soccer_schedule</code>{" "}
          to load the latest World Cup schedule, odds, and model predictions.
        </div>
      )}

      {days.map((day) => (
        <div key={day} className="space-y-2">
          <h2 className="text-sm font-semibold text-muted-foreground">{fmtDayHeading(day)}</h2>
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[880px] text-sm">
              <thead>
                <tr className="border-b text-xs text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Match</th>
                  <th className="px-2 py-2 text-center font-medium">Time</th>
                  <th className="px-2 py-2 text-center font-medium">Moneyline (H/D/A)</th>
                  <th className="px-2 py-2 text-center font-medium">O/U<br />V → Model</th>
                  <th className="px-2 py-2 text-center font-medium">Vegas Win %</th>
                  <th className="px-2 py-2 text-center font-medium">Pinnacle %</th>
                  <th className="px-2 py-2 text-center font-medium">Our Win %</th>
                  <th className="px-2 py-2 text-center font-medium">Edge</th>
                </tr>
              </thead>
              <tbody>
                {(byDate.get(day) ?? []).map((m) => {
                  const finished = m.homeScore != null && m.awayScore != null;
                  const totalDelta =
                    m.ourTotalPred != null && m.vegasTotal != null
                      ? m.ourTotalPred - m.vegasTotal
                      : null;
                  const edge = bestEdge(m);
                  const edgeHot = edge != null && edge.edge >= EDGE_PP;
                  return (
                    <tr key={m.matchupId} className="border-b last:border-0 align-top hover:bg-accent/40">
                      <td className="px-3 py-2">
                        <div className="font-medium">
                          {m.homeTeam} <span className="text-muted-foreground">v</span> {m.awayTeam}
                        </div>
                        {finished && (
                          <div className="text-xs text-emerald-500">
                            Final {m.homeScore}–{m.awayScore}
                          </div>
                        )}
                        {!finished && m.motivation && (
                          <div
                            className="mt-0.5 inline-block rounded bg-amber-100 px-1.5 py-0.5 text-[10px] font-medium text-amber-700"
                            title="Matchday-3 game state — our number is adjusted for rotation/dead-rubber risk the Elo model is otherwise blind to."
                          >
                            ⚑ {m.motivation}
                          </div>
                        )}
                      </td>
                      <td className="px-2 py-2 text-center text-xs text-muted-foreground">
                        {fmtKickoff(m.commenceTime)}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-xs">
                        {fmtMl(m.homeMl)} / <span className="text-muted-foreground">{fmtMl(m.drawMl)}</span> /{" "}
                        {fmtMl(m.awayMl)}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        <div className="font-medium">{fmtGoals(m.vegasTotal)}</div>
                        {m.ourTotalPred != null && (
                          <div className="text-xs">
                            {fmtGoals(m.ourTotalPred)}
                            {totalDelta != null && Math.abs(totalDelta) >= TOTAL_EDGE && (
                              <span className={totalDelta > 0 ? "ml-1 text-emerald-500" : "ml-1 text-sky-400"}>
                                ({totalDelta > 0 ? "O" : "U"} {fmtSignedGoals(totalDelta)})
                              </span>
                            )}
                          </div>
                        )}
                      </td>
                      <td className="px-2 py-2">
                        <div className="flex flex-col items-center gap-1">
                          <ProbBar home={m.homeWinProb} draw={m.drawProb} away={m.awayWinProb} />
                          <div className="text-[10px] tabular-nums text-muted-foreground">
                            {fmtPct(m.homeWinProb)}/{fmtPct(m.drawProb)}/{fmtPct(m.awayWinProb)}
                          </div>
                        </div>
                      </td>
                      <td className="px-2 py-2">
                        {m.pinnacleProbHome != null ? (
                          <div className="flex flex-col items-center gap-1">
                            <ProbBar home={m.pinnacleProbHome} draw={m.pinnacleProbDraw} away={m.pinnacleProbAway} />
                            <div className="text-[10px] tabular-nums text-purple-400">
                              {fmtPct(m.pinnacleProbHome)}/{fmtPct(m.pinnacleProbDraw)}/{fmtPct(m.pinnacleProbAway)}
                            </div>
                          </div>
                        ) : (
                          <div className="text-center text-xs text-muted-foreground">—</div>
                        )}
                      </td>
                      <td className="px-2 py-2">
                        {m.ourProbHome != null ? (
                          <div className="flex flex-col items-center gap-1">
                            <ProbBar home={m.ourProbHome} draw={m.ourProbDraw} away={m.ourProbAway} />
                            <div className="text-[10px] tabular-nums text-muted-foreground">
                              {fmtPct(m.ourProbHome)}/{fmtPct(m.ourProbDraw)}/{fmtPct(m.ourProbAway)}
                            </div>
                          </div>
                        ) : (
                          <div className="text-center text-xs text-muted-foreground">—</div>
                        )}
                      </td>
                      <td className="px-2 py-2 text-center">
                        {edge ? (
                          <span
                            className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium tabular-nums ${
                              edgeHot
                                ? "bg-emerald-500/15 text-emerald-400"
                                : "text-muted-foreground"
                            }`}
                          >
                            {edge.label} {fmtSignedPp(edge.edge)}
                          </span>
                        ) : (
                          <span className="text-xs text-muted-foreground">—</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </section>
  );
}

// ── Scorers panel ─────────────────────────────────────────────────────────────
function ScorersPanel({ rows }: { rows: SoccerPlayerStatsRow[] }) {
  if (rows.length === 0) {
    return (
      <section className="space-y-3">
        <h2 className="text-lg font-bold">⚽ Tournament Scorers</h2>
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No goals recorded yet. Data updates after each match via TheSportsDB.
        </div>
      </section>
    );
  }

  const maxGoals = rows[0]?.goals ?? 1;

  return (
    <section className="space-y-4">
      <h2 className="text-lg font-bold">⚽ Tournament Scorers &amp; Assists</h2>

      <div className="overflow-hidden rounded-lg border bg-card">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-[10px] uppercase text-muted-foreground">
              <th className="w-8 px-3 py-2 text-center font-medium">#</th>
              <th className="px-3 py-2 text-left font-medium">Player</th>
              <th className="px-3 py-2 text-left font-medium text-muted-foreground">Team</th>
              <th className="px-2 py-2 text-center font-medium">G</th>
              <th className="px-2 py-2 text-center font-medium">A</th>
              <th className="px-2 py-2 text-center font-medium">G+A</th>
              <th className="px-2 py-2 text-center font-medium hidden sm:table-cell">1st</th>
              <th className="px-3 py-2 text-left font-medium hidden md:table-cell">Goals</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const ga = r.goals + r.assists;
              return (
                <tr
                  key={r.playerName}
                  className={`border-b last:border-0 hover:bg-accent/40 ${r.goals === maxGoals && maxGoals > 0 ? "bg-amber-500/5" : ""}`}
                >
                  <td className="px-3 py-2 text-center text-xs text-muted-foreground">{i + 1}</td>
                  <td className="px-3 py-2 font-medium">
                    {r.playerName}
                    {r.goals === maxGoals && maxGoals > 0 && (
                      <span className="ml-1.5 text-[10px] text-amber-400 font-semibold">🥇</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-muted-foreground text-xs">{r.playerTeam ?? "—"}</td>
                  <td className="px-2 py-2 text-center font-bold tabular-nums">
                    {r.goals > 0 ? r.goals : <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                    {r.assists > 0 ? r.assists : <span className="opacity-40">—</span>}
                  </td>
                  <td className="px-2 py-2 text-center tabular-nums font-medium">{ga}</td>
                  <td className="px-2 py-2 text-center hidden sm:table-cell">
                    {r.firstGoals > 0 ? (
                      <span className="text-emerald-400 text-xs font-semibold">{r.firstGoals}</span>
                    ) : (
                      <span className="text-muted-foreground opacity-40">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground hidden md:table-cell">
                    {"⚽".repeat(r.goals)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="text-[11px] text-muted-foreground">
        G = goals, A = assists, G+A = goal contributions, 1st = first goals scored.
        Assists from TheSportsDB — penalties credited with no assist. Data lags 1–6h post-match.
      </p>
    </section>
  );
}

// ── Tab nav ───────────────────────────────────────────────────────────────────
type Tab = "bets" | "first_scorer" | "scorers" | "results" | "fixtures";

const TABS: { id: Tab; label: string }[] = [
  { id: "bets", label: "⭐ Bets" },
  { id: "first_scorer", label: "🥅 First Scorer" },
  { id: "scorers", label: "⚽ Scorers" },
  { id: "results", label: "📈 Results" },
  { id: "fixtures", label: "📅 Fixtures" },
];

// ── Root component ────────────────────────────────────────────────────────────
export default function SoccerVegasClient({
  matchups,
  bets,
  settledBets,
  backtest,
  clv,
  calibCuts,
  clvTrend,
  firstScorers,
  matchGoals,
  playerStats,
  fscorerTiers,
  fscorerNearMisses,
  topPickAccuracy,
  settlementHealth,
  queryDate,
}: {
  matchups: SoccerVegasMatchupRow[];
  bets: SoccerBetRow[];
  settledBets: SoccerBetRow[];
  backtest: SoccerBacktestTypeRow[];
  clv: SoccerClvRow[];
  calibCuts: SoccerCalibCutRow[];
  clvTrend: SoccerClvTrendRow[];
  firstScorers: SoccerFirstScorerRow[];
  matchGoals: SoccerMatchGoalRow[];
  playerStats: SoccerPlayerStatsRow[];
  fscorerTiers: SoccerFirstScorerTierRow[];
  fscorerNearMisses: SoccerFirstScorerNearMissRow[];
  topPickAccuracy: SoccerTopPickRow[];
  settlementHealth: SoccerSettlementIssue[];
  queryDate: string | null;
}) {
  const [tab, setTab] = useState<Tab>("bets");

  const totalWon = settledBets.filter((b) => b.status === "won").length;
  const totalLost = settledBets.filter((b) => b.status === "lost").length;
  const totalVoid = settledBets.filter((b) => b.status === "void").length;
  const totalSettled = settledBets.length;
  const pendingCount = bets.filter((b) => b.status === "pending").length;

  return (
    <div className="mx-auto max-w-6xl space-y-4 p-6">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h1 className="text-xl font-bold">⚽ World Cup 2026 — Our Model vs Vegas</h1>
        <div className="flex items-center gap-3 text-xs text-muted-foreground">
          {pendingCount > 0 && <span>{pendingCount} pending bets</span>}
          {totalSettled > 0 && (
            <span className={totalWon / (totalWon + totalLost || 1) >= 0.5 ? "text-emerald-400" : "text-rose-400"}>
              {totalWon}W / {totalLost}L / {totalVoid} push ({fmtPct(totalWon / Math.max(1, totalWon + totalLost))} win)
            </span>
          )}
        </div>
      </div>

      {/* Tab bar — pill nav on desktop, select dropdown on mobile */}
      <div>
        {/* Mobile: dropdown */}
        <div className="sm:hidden">
          <select
            value={tab}
            onChange={(e) => setTab(e.target.value as Tab)}
            className="w-full rounded-lg border bg-card px-3 py-2 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-primary"
          >
            {TABS.map((t) => (
              <option key={t.id} value={t.id}>{t.label}</option>
            ))}
          </select>
        </div>
        {/* Desktop: pill nav */}
        <div className="hidden sm:flex gap-1 border-b overflow-x-auto">
          {TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`whitespace-nowrap px-4 py-2 text-sm font-medium transition-colors ${
                tab === t.id
                  ? "border-b-2 border-primary text-foreground"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tab content */}
      {tab === "bets" && <BetsPanel bets={bets} />}
      {tab === "first_scorer" && <FirstScorerPanel rows={firstScorers} matchGoals={matchGoals} />}
      {tab === "scorers" && <ScorersPanel rows={playerStats} />}
      {tab === "results" && (
        <ResultsPanel
          bets={settledBets}
          backtest={backtest}
          clv={clv}
          calibCuts={calibCuts}
          clvTrend={clvTrend}
          fscorerTiers={fscorerTiers}
          fscorerNearMisses={fscorerNearMisses}
          topPickAccuracy={topPickAccuracy}
          settlementHealth={settlementHealth}
        />
      )}
      {tab === "fixtures" && <FixturesPanel matchups={matchups} queryDate={queryDate} />}
    </div>
  );
}
