export const dynamic = "force-dynamic";

import Link from "next/link";
import type { Metadata } from "next";
import { easternDateString } from "@/lib/eastern-date";
import { buildBoard } from "@/lib/nba-top-scorers";
import {
  getNbaPointsPropDates,
  getNbaPointsProps,
  getNbaPulledLines,
  getNbaSeasonStats,
  getNbaTeams,
} from "@/db/nba-top-scorers";
import { TopScorersClient } from "./top-scorers-client";

export const metadata: Metadata = {
  title: "NBA Top Scorers",
  description: "Who the sportsbooks expect to score the most points tonight.",
};

// Last completed season: the comparison column, until this season has enough games.
const LAST_SEASON = "2025-26";

// The app-wide `* { border-color: var(--border) }` rule beats border utilities.
const DARK_BORDER = { "--border": "#1e293b" } as React.CSSProperties;

function stamp(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(value));
}

function longDate(d: string) {
  return new Date(`${d}T12:00:00`).toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric" });
}

export default async function TopScorersPage({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const params = await searchParams;
  const today = easternDateString();
  const dates = await getNbaPointsPropDates();
  const requested = params.date && /^\d{4}-\d{2}-\d{2}$/.test(params.date) ? params.date : null;
  const date = requested ?? (dates.includes(today) ? today : dates.find((d) => d > today) ?? dates[0] ?? today);

  const [rows, pulled, lastSeason, teams] = await Promise.all([
    getNbaPointsProps(date),
    getNbaPulledLines(date),
    getNbaSeasonStats(LAST_SEASON),
    getNbaTeams(),
  ]);
  const { scorers, games } = buildBoard(rows, lastSeason, teams);
  const lastCapture = games.reduce<string | null>((m, g) => (m == null || g.capturedAt > m ? g.capturedAt : m), null);
  const bookCount = new Set(scorers.flatMap((s) => s.books.map((b) => b.book))).size;

  return (
    <main className="mx-auto max-w-7xl space-y-4 px-4 py-6">
      <header style={DARK_BORDER} className="rounded-xl border border-slate-800 bg-[#0f1726] px-5 py-4 text-slate-200">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="font-mono text-xs text-slate-400">NBA · Player points · Market lines</div>
            <h1 className="mt-1 text-2xl font-bold tracking-tight text-white">Top Scorers · {longDate(date)}</h1>
            <div className="mt-1 font-mono text-sm text-slate-400">
              <span className="text-emerald-400">{games.length} games</span> · {scorers.length} players · {bookCount} books
              {lastCapture ? ` · lines as of ${stamp(lastCapture)}` : ""}
            </div>
          </div>
          <span className="rounded-full border border-emerald-500/60! px-3 py-1 font-mono text-xs text-emerald-300">
            Market number · unvalidated
          </span>
        </div>
        {dates.length > 0 && (
          <nav className="mt-3 flex flex-wrap gap-1.5 font-mono text-xs" aria-label="Dates">
            {[...dates].sort().map((d) => (
              <Link
                key={d}
                href={`/nba/top-scorers?date=${d}`}
                className={`rounded px-2.5 py-1 ${d === date ? "bg-sky-500 text-white" : "border border-slate-700 text-slate-300 hover:bg-slate-800"}`}
              >
                {d.slice(5)}
              </Link>
            ))}
          </nav>
        )}
      </header>

      {scorers.length === 0 ? (
        <section style={DARK_BORDER} className="rounded-xl border border-slate-800 bg-[#0f1726] p-6 text-sm text-slate-300">
          <p className="font-medium text-white">No points lines stored for {longDate(date)}.</p>
          <p className="mt-1 text-slate-400">
            Lines come from{" "}
            <code className="text-slate-200">python -m ingest.nba_points_props --date {date} --apply --max-credits 20</code>.
            Books usually post NBA player props the morning of the game, so an early capture can come back empty.
          </p>
        </section>
      ) : (
        <TopScorersClient scorers={scorers} games={games} pulled={pulled} lastSeason={LAST_SEASON} />
      )}

      <section className="space-y-1 text-xs text-muted-foreground">
        <p>
          Line is the points line most books quote. A points line is the books&apos; median outcome, so a player scores above
          it about half the time; P(over) is that chance with the vig removed, from the books quoting that line. Low – High is
          the spread of lines across books, not a projected floor or ceiling.
        </p>
        <p>
          This is the market&apos;s number, not a model of ours, and it has not been checked against results here. Position,
          team and {LAST_SEASON} PPG come from last season&apos;s stats and can be stale for a player who changed teams.
        </p>
      </section>
    </main>
  );
}
