"use client";

import type { SoccerVegasMatchupRow } from "@/db/queries";

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

export default function SoccerVegasClient({
  matchups,
  queryDate,
}: {
  matchups: SoccerVegasMatchupRow[];
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
