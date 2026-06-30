"use client";

import { useMemo, useState } from "react";
import type { TennisMatchRow, TennisBetRow } from "@/db/queries";

const fmtMl = (ml: number | null) => (ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml));
const fmtPct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(0)}%`);
const fmtSignedPp = (v: number | null) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(0)}%`);

function Stars({ n }: { n: number }) {
  return (
    <span className="tabular-nums tracking-tight text-amber-400" title={`${n} of 5 stars`}>
      {"★".repeat(n)}
      <span className="text-muted-foreground/40">{"★".repeat(5 - n)}</span>
    </span>
  );
}

function fmtTime(iso: string | null): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

function fmtDayHeading(date: string): string {
  const d = new Date(`${date}T00:00:00`);
  if (Number.isNaN(d.getTime())) return date;
  return d.toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric" });
}

// Win-probability split bar (favorite shaded). 2-way — no draw.
function ProbBar({ home, away }: { home: number | null; away: number | null }) {
  if (home == null || away == null) return <div className="h-1.5 w-full rounded bg-muted" />;
  return (
    <div className="flex h-1.5 w-full overflow-hidden rounded">
      <div style={{ width: `${home * 100}%` }} className="bg-blue-500" />
      <div style={{ width: `${away * 100}%` }} className="bg-rose-500" />
    </div>
  );
}

export default function TennisVegasClient({
  matchups,
  bets,
  queryDate,
}: {
  matchups: TennisMatchRow[];
  bets: TennisBetRow[];
  queryDate: string | null;
}) {
  const [tour, setTour] = useState<"all" | "ATP" | "WTA">("all");

  const filtered = useMemo(
    () => (tour === "all" ? matchups : matchups.filter((m) => m.tour === tour)),
    [matchups, tour],
  );

  // Group by match_date for day headings.
  const byDay = useMemo(() => {
    const map = new Map<string, TennisMatchRow[]>();
    for (const m of filtered) {
      const arr = map.get(m.matchDate) ?? [];
      arr.push(m);
      map.set(m.matchDate, arr);
    }
    return Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  }, [filtered]);

  const atpCount = matchups.filter((m) => m.tour === "ATP").length;
  const wtaCount = matchups.filter((m) => m.tour === "WTA").length;

  return (
    <div className="space-y-6 p-6 max-w-5xl mx-auto">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h1 className="text-xl font-bold">Vegas Analysis — WIMBLEDON 🎾</h1>
        <span className="text-xs text-muted-foreground">
          {matchups.length} matches · consensus across books · {queryDate ?? "upcoming"}
        </span>
      </div>

      <p className="text-sm text-muted-foreground">
        Live consensus odds from The Odds API across all books, vig removed in probability space.
        Our grass-Elo model (market-anchored) rates ATP moneylines 1–5★ — value = where we disagree
        with the market. WTA is odds-only until a women&rsquo;s ratings source is wired.
      </p>

      {/* Top rated bets — the model's recommendations, best first */}
      {bets.length > 0 && (() => {
        const top = bets.filter((b) => b.stars >= 3 && (b.edge ?? 0) > 0).slice(0, 12);
        if (top.length === 0) return null;
        return (
          <div>
            <h2 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
              Rated ATP moneyline plays (3★+ value) · {top.length}
            </h2>
            <div className="overflow-x-auto rounded-lg border bg-card">
              <table className="w-full min-w-[680px] text-sm">
                <thead>
                  <tr className="border-b text-[10px] uppercase text-muted-foreground">
                    <th className="px-3 py-2 text-left font-medium">Rating</th>
                    <th className="px-3 py-2 text-left font-medium">Pick</th>
                    <th className="px-3 py-2 text-left font-medium">Match</th>
                    <th className="px-2 py-2 text-center font-medium">Our %</th>
                    <th className="px-2 py-2 text-center font-medium">Mkt %</th>
                    <th className="px-2 py-2 text-center font-medium">Edge</th>
                    <th className="px-2 py-2 text-center font-medium">Odds</th>
                    <th className="px-2 py-2 text-center font-medium">EV</th>
                  </tr>
                </thead>
                <tbody>
                  {top.map((b) => (
                    <tr key={b.id} className="border-b last:border-0 hover:bg-accent/40">
                      <td className="px-3 py-2"><Stars n={b.stars} /></td>
                      <td className="px-3 py-2 font-medium">{b.selectionLabel}</td>
                      <td className="px-3 py-2 text-xs text-muted-foreground">{b.fixture}</td>
                      <td className="px-2 py-2 text-center tabular-nums">{fmtPct(b.ourProb)}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">{fmtPct(b.marketProb)}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-emerald-400">{fmtSignedPp(b.edge)}</td>
                      <td className="px-2 py-2 text-center tabular-nums">{fmtMl(b.marketOdds)}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-emerald-400">{fmtSignedPp(b.ev)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="mt-1 text-[11px] text-muted-foreground">
              Edge = our model prob − vig-free market prob. EV = expected ROI per unit at the offered price.
              Calibration (do these win at the rate we claim?) lands with Kaggle settlement.
            </p>
          </div>
        );
      })()}

      {/* Tour filter */}
      <div className="flex gap-1.5">
        {([
          ["all", `All (${matchups.length})`],
          ["ATP", `ATP ${atpCount}`],
          ["WTA", `WTA ${wtaCount}`],
        ] as const).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTour(key as "all" | "ATP" | "WTA")}
            className={`rounded-full border px-3 py-1 text-xs font-medium transition ${
              tour === key
                ? "border-foreground bg-foreground text-background"
                : "border-border bg-background text-muted-foreground hover:text-foreground"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {byDay.length === 0 && (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No matches in the feed. Run <code className="rounded bg-muted px-1 py-0.5">python -m ingest.tennis_schedule</code> to seed odds.
        </div>
      )}

      {byDay.map(([day, matches]) => (
        <div key={day} className="space-y-2">
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            {fmtDayHeading(day)} · {matches.length}
          </h2>
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[760px] text-sm">
              <thead>
                <tr className="border-b text-[10px] uppercase text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Time</th>
                  <th className="px-2 py-2 text-left font-medium">Tour</th>
                  <th className="px-3 py-2 text-left font-medium">Match</th>
                  <th className="px-2 py-2 text-center font-medium">Moneyline</th>
                  <th className="px-2 py-2 text-center font-medium">Mkt win prob</th>
                  <th className="px-2 py-2 text-center font-medium">Our prob (edge)</th>
                  <th className="px-2 py-2 text-center font-medium">Total games</th>
                  <th className="px-2 py-2 text-center font-medium">Handicap</th>
                  <th className="px-2 py-2 text-center font-medium">Books</th>
                </tr>
              </thead>
              <tbody>
                {matches.map((m) => {
                  const homeFav = (m.homeWinProb ?? 0) >= (m.awayWinProb ?? 0);
                  return (
                    <tr key={m.id} className="border-b last:border-0 hover:bg-accent/40 align-top">
                      <td className="px-3 py-2 text-xs text-muted-foreground whitespace-nowrap">
                        {fmtTime(m.commenceTime)}
                      </td>
                      <td className="px-2 py-2 text-xs text-muted-foreground">{m.tour}</td>
                      <td className="px-3 py-2">
                        <div className={`leading-tight ${homeFav ? "font-semibold" : ""}`}>
                          {m.homePlayer}
                        </div>
                        <div className={`leading-tight ${!homeFav ? "font-semibold" : ""}`}>
                          {m.awayPlayer}
                        </div>
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums whitespace-nowrap">
                        <div>{fmtMl(m.homeMl)}</div>
                        <div className="text-muted-foreground">{fmtMl(m.awayMl)}</div>
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        <div className="mb-1 text-xs">
                          {fmtPct(m.homeWinProb)} / {fmtPct(m.awayWinProb)}
                        </div>
                        <ProbBar home={m.homeWinProb} away={m.awayWinProb} />
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        {m.ourProbHome != null ? (() => {
                          const edgeH = m.homeWinProb != null ? m.ourProbHome! - m.homeWinProb : null;
                          // Only a meaningful Elo signal (>1pp either side) is highlighted;
                          // market-fallback rows show our≈market and stay muted.
                          const hasEdge = edgeH != null && Math.abs(edgeH) > 0.01;
                          return (
                            <>
                              <div className="text-xs">
                                {fmtPct(m.ourProbHome)} / {fmtPct(m.ourProbAway)}
                              </div>
                              {hasEdge && (
                                <div className={`text-[10px] ${edgeH! > 0 ? "text-emerald-400" : "text-rose-400"}`}>
                                  {edgeH! > 0 ? "▲" : "▼"} {fmtSignedPp(edgeH)} home
                                </div>
                              )}
                            </>
                          );
                        })() : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        {m.totalGamesLine != null ? (
                          <>
                            <div>{m.totalGamesLine}</div>
                            <div className="text-[10px] text-muted-foreground">
                              O {fmtMl(m.overOdds)} / U {fmtMl(m.underOdds)}
                            </div>
                          </>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">
                        {m.setHandicap != null ? (
                          <>
                            <div>{m.setHandicap > 0 ? `+${m.setHandicap}` : m.setHandicap}</div>
                            <div className="text-[10px] text-muted-foreground">
                              {fmtMl(m.handicapHomeOdds)} / {fmtMl(m.handicapAwayOdds)}
                            </div>
                          </>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {m.nBooks ?? "—"}
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
