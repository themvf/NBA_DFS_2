"use client";

import type { SoccerVegasMatchupRow } from "@/db/queries";

const fmtMl = (ml: number | null) => (ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml));
const fmtPct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(0)}%`);
const fmtGoals = (v: number | null) => (v == null ? "—" : v.toFixed(2));

function fmtKickoff(commenceTime: string | null): string {
  if (!commenceTime) return "";
  const d = new Date(commenceTime);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

function fmtDayHeading(gameDate: string): string {
  const d = new Date(`${gameDate}T00:00:00`);
  if (Number.isNaN(d.getTime())) return gameDate;
  return d.toLocaleDateString(undefined, {
    weekday: "long",
    month: "long",
    day: "numeric",
  });
}

// Three-way win-probability bar (home / draw / away).
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

export default function SoccerVegasClient({
  matchups,
  queryDate,
}: {
  matchups: SoccerVegasMatchupRow[];
  queryDate: string | null;
}) {
  // Group fixtures by calendar date (already ordered by kickoff time).
  const byDate = new Map<string, SoccerVegasMatchupRow[]>();
  for (const m of matchups) {
    const list = byDate.get(m.gameDate) ?? [];
    list.push(m);
    byDate.set(m.gameDate, list);
  }
  const days = Array.from(byDate.keys());

  return (
    <div className="mx-auto max-w-5xl space-y-6 p-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h1 className="text-xl font-bold">⚽ World Cup 2026 — Vegas Lines</h1>
        <span className="text-sm text-muted-foreground">
          {matchups.length} {queryDate ? `fixtures on ${queryDate}` : "upcoming fixtures"}
        </span>
      </div>

      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        3-way moneylines, goal totals, and implied goals per side — consensus
        across US/UK/EU books via The Odds API. Lines refresh when{" "}
        <code className="rounded bg-muted px-1 py-0.5">ingest.soccer_schedule</code>{" "}
        is run. Win-probability bar: <span className="text-blue-500">home</span> /{" "}
        <span className="text-gray-400">draw</span> /{" "}
        <span className="text-rose-500">away</span>.
      </p>

      {matchups.length === 0 && (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No fixtures found. Run{" "}
          <code className="rounded bg-muted px-1 py-0.5">python -m ingest.soccer_schedule</code>{" "}
          to load the latest World Cup schedule and odds.
        </div>
      )}

      {days.map((day) => (
        <div key={day} className="space-y-2">
          <h2 className="text-sm font-semibold text-muted-foreground">{fmtDayHeading(day)}</h2>
          <div className="overflow-hidden rounded-lg border bg-card">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-xs text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Match</th>
                  <th className="px-2 py-2 text-center font-medium">Time</th>
                  <th className="px-2 py-2 text-center font-medium">Home</th>
                  <th className="px-2 py-2 text-center font-medium">Draw</th>
                  <th className="px-2 py-2 text-center font-medium">Away</th>
                  <th className="px-2 py-2 text-center font-medium">Win %</th>
                  <th className="px-2 py-2 text-center font-medium">O/U</th>
                  <th className="px-2 py-2 text-center font-medium">Impl. Goals</th>
                </tr>
              </thead>
              <tbody>
                {(byDate.get(day) ?? []).map((m) => {
                  const finished = m.homeScore != null && m.awayScore != null;
                  return (
                    <tr key={m.matchupId} className="border-b last:border-0 hover:bg-accent/40">
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
                      <td className="px-2 py-2 text-center tabular-nums">{fmtMl(m.homeMl)}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                        {fmtMl(m.drawMl)}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">{fmtMl(m.awayMl)}</td>
                      <td className="px-2 py-2">
                        <div className="flex flex-col items-center gap-1">
                          <ProbBar home={m.homeWinProb} draw={m.drawProb} away={m.awayWinProb} />
                          <div className="text-[10px] tabular-nums text-muted-foreground">
                            {fmtPct(m.homeWinProb)} / {fmtPct(m.drawProb)} / {fmtPct(m.awayWinProb)}
                          </div>
                        </div>
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums font-medium">
                        {fmtGoals(m.vegasTotal)}
                      </td>
                      <td className="px-2 py-2 text-center text-xs tabular-nums text-muted-foreground">
                        {fmtGoals(m.homeImplied)} – {fmtGoals(m.awayImplied)}
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
