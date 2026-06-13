"use client";

import { useState } from "react";
import type { SoccerVegasMatchupRow, SoccerBetRow, SoccerBacktestRow } from "@/db/queries";

const fmtMl = (ml: number | null) => (ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml));
const fmtPct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(0)}%`);
const fmtGoals = (v: number | null) => (v == null ? "—" : v.toFixed(2));

// Edge threshold (probability points) above which we highlight a disagreement.
const EDGE_PP = 0.05;
const TOTAL_EDGE = 0.2; // goals

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
        : "bg-muted text-muted-foreground";
  return <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium uppercase ${cls}`}>{status}</span>;
}

// Three-way probability bar (home / draw / away).
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

// Largest model-vs-market probability disagreement → the lean we'd bet.
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
                <th className="px-2 py-2 text-center font-medium">Status</th>
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

function BacktestPanel({ backtest }: { backtest: SoccerBacktestRow[] }) {
  const totalSettled = backtest.reduce((s, r) => s + r.n, 0);
  return (
    <section className="space-y-3">
      <h2 className="text-lg font-bold">📊 Star-Rating Backtest</h2>
      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        The accountability check: among <strong className="text-foreground">settled</strong> bets,
        does each star tier win at the rate we claimed? <strong className="text-foreground">Expected</strong>{" "}
        is our average model probability; <strong className="text-foreground">Realized</strong> is the
        actual win rate. A trustworthy 4–5★ tier has Realized ≥ Expected and positive ROI.
      </p>
      {totalSettled === 0 ? (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No settled bets yet — the World Cup hasn&rsquo;t produced results. Group/outright settle
          from final standings; first-scorer via{" "}
          <code className="rounded bg-muted px-1 py-0.5">ingest.soccer_results</code>. Check back as
          games complete.
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border bg-card">
          <table className="w-full min-w-[520px] text-sm">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="px-3 py-2 text-left font-medium">Tier</th>
                <th className="px-2 py-2 text-center font-medium">Bets</th>
                <th className="px-2 py-2 text-center font-medium">Expected</th>
                <th className="px-2 py-2 text-center font-medium">Realized</th>
                <th className="px-2 py-2 text-center font-medium">ROI</th>
              </tr>
            </thead>
            <tbody>
              {backtest.map((r) => {
                const beat = r.realizedWinRate >= r.expectedWinRate;
                return (
                  <tr key={r.stars} className="border-b last:border-0">
                    <td className="px-3 py-2"><Stars n={r.stars} /></td>
                    <td className="px-2 py-2 text-center tabular-nums">{r.n}</td>
                    <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                      {fmtPct(r.expectedWinRate)}
                    </td>
                    <td className={`px-2 py-2 text-center tabular-nums ${beat ? "text-emerald-400" : "text-rose-400"}`}>
                      {fmtPct(r.realizedWinRate)}
                    </td>
                    <td className="px-2 py-2 text-center tabular-nums">
                      {r.roi != null ? (
                        <span className={r.roi >= 0 ? "text-emerald-400" : "text-rose-400"}>
                          {fmtSignedPp(r.roi)}
                        </span>
                      ) : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default function SoccerVegasClient({
  matchups,
  bets,
  backtest,
  queryDate,
}: {
  matchups: SoccerVegasMatchupRow[];
  bets: SoccerBetRow[];
  backtest: SoccerBacktestRow[];
  queryDate: string | null;
}) {
  const byDate = new Map<string, SoccerVegasMatchupRow[]>();
  for (const m of matchups) {
    const list = byDate.get(m.gameDate) ?? [];
    list.push(m);
    byDate.set(m.gameDate, list);
  }
  const days = Array.from(byDate.keys());
  const hasModel = matchups.some((m) => m.ourTotalPred != null);

  return (
    <div className="mx-auto max-w-6xl space-y-6 p-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h1 className="text-xl font-bold">⚽ World Cup 2026 — Our Model vs Vegas</h1>
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

      <BetsPanel bets={bets} />
      <BacktestPanel backtest={backtest} />

      <h2 className="border-t pt-5 text-lg font-bold">📅 Fixtures — Our Model vs Vegas</h2>
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
            <table className="w-full min-w-[760px] text-sm">
              <thead>
                <tr className="border-b text-xs text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Match</th>
                  <th className="px-2 py-2 text-center font-medium">Time</th>
                  <th className="px-2 py-2 text-center font-medium">Moneyline (H/D/A)</th>
                  <th className="px-2 py-2 text-center font-medium">O/U<br />V → Model</th>
                  <th className="px-2 py-2 text-center font-medium">Vegas Win %</th>
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
    </div>
  );
}
