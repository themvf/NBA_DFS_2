"use client";

import { useState } from "react";
import type {
  SoccerVegasMatchupRow,
  SoccerBetRow,
  SoccerBacktestRow,
  SoccerFirstScorerRow,
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

const fmtSignedPp = (v: number) => `${v >= 0 ? "+" : ""}${(v * 100).toFixed(0)}%`;
const fmtSignedGoals = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(2)}`;
const fmtRoi = (v: number) => `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`;

const BET_TYPE_LABEL: Record<string, string> = {
  moneyline: "Moneyline",
  total: "Over/Under",
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

// ── Results panel ─────────────────────────────────────────────────────────────
function ResultsPanel({
  bets,
  backtest,
}: {
  bets: SoccerBetRow[];
  backtest: SoccerBacktestRow[];
}) {
  // ── Settled bet table state ──────────────────────────────────────────────────
  const [tType, setTType] = useState("all");
  const [tStatus, setTStatus] = useState("all");
  const [tMinStars, setTMinStars] = useState(1);
  const [tSearch, setTSearch] = useState("");
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

  // ── Aggregate stats — computed from bets filtered by tMinStars ───────────────
  const starFilteredBets = bets.filter((b) => b.stars >= tMinStars);

  // Group by bet type for the aggregate table
  const byTypeMap = new Map<string, { won: number; lost: number; voided: number; sumExpected: number; nExpected: number; marketBets: number; profit: number }>();
  for (const b of starFilteredBets) {
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

  const totalWon = starFilteredBets.filter((b) => b.status === "won").length;
  const totalLost = starFilteredBets.filter((b) => b.status === "lost").length;
  const totalVoid = starFilteredBets.filter((b) => b.status === "void").length;
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

      {/* Summary cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard
          label="Settled bets"
          value={String(totalSettled)}
          sub={`${totalWon}W · ${totalLost}L · ${totalVoid} push`}
        />
        <StatCard
          label="Overall win rate"
          value={overallWinRate != null ? fmtPct1(overallWinRate) : "—"}
          sub={`${totalWon + totalLost} non-void`}
          color={overallWinRate != null && overallWinRate >= 0.5 ? "text-emerald-400" : "text-rose-400"}
        />
        <StatCard
          label="ROI (market bets)"
          value={overallRoi != null ? fmtRoi(overallRoi) : "—"}
          sub={`${totalMarket} bets with odds`}
          color={overallRoi != null && overallRoi >= 0 ? "text-emerald-400" : "text-rose-400"}
        />
        <StatCard
          label="Star tiers tracked"
          value={String(backtest.length)}
          sub={backtest.length > 0 ? `${backtest[0]?.stars}★ top tier` : "none yet"}
        />
      </div>

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
                      {b.gameDate
                        ? new Date(`${b.gameDate}T00:00:00`).toLocaleDateString(undefined, { month: "short", day: "numeric" })
                        : "—"}
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

      {/* By bet type — reacts to tMinStars filter */}
      <div>
        <h3 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Results by bet type{tMinStars > 1 ? ` — ${tMinStars}★+ only` : ""}
        </h3>
        {byTypeRows.length === 0 ? (
          <div className="rounded-lg border bg-card p-4 text-sm text-muted-foreground">
            No settled bets at {tMinStars}★+.
          </div>
        ) : (
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[600px] text-sm">
              <thead>
                <tr className="border-b text-xs text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Type</th>
                  <th className="px-2 py-2 text-center font-medium">Won</th>
                  <th className="px-2 py-2 text-center font-medium">Lost</th>
                  <th className="px-2 py-2 text-center font-medium">Push</th>
                  <th className="px-2 py-2 text-center font-medium">Win %</th>
                  <th className="px-2 py-2 text-center font-medium">Expected</th>
                  <th className="px-2 py-2 text-center font-medium">ROI</th>
                </tr>
              </thead>
              <tbody>
                {byTypeRows.map((r) => {
                  const nonVoid = r.won + r.lost;
                  const wr = nonVoid > 0 ? r.won / nonVoid : null;
                  const expectedWr = r.nExpected > 0 ? r.sumExpected / r.nExpected : null;
                  const beat = wr != null && expectedWr != null && wr >= expectedWr;
                  const roi = r.marketBets > 0 ? r.profit / r.marketBets : null;
                  return (
                    <tr key={r.betType} className="border-b last:border-0 hover:bg-accent/40">
                      <td className="px-3 py-2 font-medium">
                        {BET_TYPE_LABEL[r.betType] ?? r.betType}
                      </td>
                      <td className="px-2 py-2 text-center">
                        <span className="font-medium text-emerald-400">{r.won}</span>
                      </td>
                      <td className="px-2 py-2 text-center">
                        <span className="text-rose-400">{r.lost}</span>
                      </td>
                      <td className="px-2 py-2 text-center text-muted-foreground">{r.voided}</td>
                      <td className={`px-2 py-2 text-center font-medium tabular-nums ${beat ? "text-emerald-400" : wr != null ? "text-rose-400" : "text-muted-foreground"}`}>
                        {wr != null ? fmtPct1(wr) : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {expectedWr != null ? fmtPct1(expectedWr) : "—"}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        {roi != null ? (
                          <span className={roi >= 0 ? "text-emerald-400" : "text-rose-400"}>
                            {fmtRoi(roi)}
                          </span>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <p className="mt-1 text-[11px] text-muted-foreground">
          Win % excludes pushes. ROI = profit per unit staked (−100% = all lost, +100% = doubled).
          Green = beating our own expected win rate. Use the Min ★ filter above to drill into a star tier.
        </p>
      </div>

      {/* By star rating */}
      {backtest.length > 0 && (
        <div>
          <h3 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            Results by star rating
          </h3>
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
                {backtest.map((r) => {
                  const beat = r.realizedWinRate >= r.expectedWinRate;
                  const brier = r.realizedWinRate - r.expectedWinRate;
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
                            +{fmtPct1(brier)} above expected ✓
                          </span>
                        ) : (
                          <span className="text-rose-400">
                            {fmtPct1(brier)} below expected
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
            A trustworthy 4–5★ tier has Realized ≥ Expected and positive ROI. The first-scorer
            pool dominates settled count but are all 1★; moneyline/totals carry the meaningful
            signal at higher tiers.
          </p>
        </div>
      )}
    </section>
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
function FirstScorerPanel({ rows }: { rows: SoccerFirstScorerRow[] }) {
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
            return (
              <div key={gid} className="overflow-hidden rounded-lg border bg-card">
                <div className="border-b px-3 py-2 text-sm font-medium">{fixture}</div>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-[10px] uppercase text-muted-foreground">
                      <th className="px-3 py-1.5 text-left font-medium">Player</th>
                      <th className="px-2 py-1.5 text-center font-medium">Our %</th>
                      <th className="px-2 py-1.5 text-center font-medium">Best Odds</th>
                      <th className="px-2 py-1.5 text-center font-medium">Mkt %</th>
                      <th className="px-2 py-1.5 text-center font-medium">Rating</th>
                    </tr>
                  </thead>
                  <tbody>
                    {list.map((r) => (
                      <tr key={r.player} className="border-b last:border-0 hover:bg-accent/40">
                        <td className="px-3 py-1.5">{r.player}</td>
                        <td className="px-2 py-1.5 text-center tabular-nums">{fmtPct(r.ourProb)}</td>
                        <td className="px-2 py-1.5 text-center tabular-nums">{fmtMl(r.marketOdds)}</td>
                        <td className="px-2 py-1.5 text-center tabular-nums text-muted-foreground">
                          {r.marketProb != null ? fmtPct(r.marketProb) : "—"}
                        </td>
                        <td className="px-2 py-1.5 text-center"><Stars n={r.stars} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
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
    const list = byDate.get(m.gameDate) ?? [];
    list.push(m);
    byDate.set(m.gameDate, list);
  }
  const days = Array.from(byDate.keys());
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

// ── Tab nav ───────────────────────────────────────────────────────────────────
type Tab = "bets" | "first_scorer" | "results" | "fixtures";

const TABS: { id: Tab; label: string }[] = [
  { id: "bets", label: "⭐ Bets" },
  { id: "first_scorer", label: "🥅 First Scorer" },
  { id: "results", label: "📈 Results" },
  { id: "fixtures", label: "📅 Fixtures" },
];

// ── Root component ────────────────────────────────────────────────────────────
export default function SoccerVegasClient({
  matchups,
  bets,
  settledBets,
  backtest,
  firstScorers,
  queryDate,
}: {
  matchups: SoccerVegasMatchupRow[];
  bets: SoccerBetRow[];
  settledBets: SoccerBetRow[];
  backtest: SoccerBacktestRow[];
  firstScorers: SoccerFirstScorerRow[];
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

      {/* Tab bar */}
      <div className="flex gap-1 border-b">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium transition-colors ${
              tab === t.id
                ? "border-b-2 border-primary text-foreground"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === "bets" && <BetsPanel bets={bets} />}
      {tab === "first_scorer" && <FirstScorerPanel rows={firstScorers} />}
      {tab === "results" && <ResultsPanel bets={settledBets} backtest={backtest} />}
      {tab === "fixtures" && <FixturesPanel matchups={matchups} queryDate={queryDate} />}
    </div>
  );
}
