import Link from "next/link";
import { getNflRollingProjectionBoard } from "@/db/nfl-rolling-projections";
import { formatKickoff, parseFilters } from "@/lib/nfl-dfs/rolling-projection-view";
import ProjectionsExplorer from "./projections-explorer";
import RollingRefresh from "./rolling-refresh";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL Rolling Projections", description: "Salary-independent NFL player projections for every position, with each game's implied team totals." };

function Notice({ children }: { children: React.ReactNode }) {
  return <p role="status" className="rounded-xl border border-amber-500/40 bg-amber-500/10 p-4 text-sm">{children}</p>;
}

export default async function NflRollingProjectionsPage({ searchParams }: {
  searchParams: Promise<Record<string, string | undefined>>;
}) {
  const params = await searchParams;
  const board = await getNflRollingProjectionBoard(params.week);
  const teams = board.games.flatMap(game => [game.home, game.away]);
  const week = board.run ? `${board.run.season}-${board.run.week}` : "";
  return (
    <main className="mx-auto max-w-7xl space-y-5 p-5">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">NFL · Player research</p>
          <h1 className="mt-1 text-3xl font-bold tracking-tight">Rolling projections</h1>
          <p className="mt-2 max-w-3xl text-sm text-muted-foreground">
            DraftKings-scoring forecasts for every position from the scheduled model run, with no salary slate needed.
            Bars show each player&apos;s floor-to-ceiling range on one shared scale.
          </p>
        </div>
        <div className="flex gap-2">
          <RollingRefresh />
          <Link href="/dfs/nfl" className="rounded-lg border bg-card px-3 py-2 text-sm font-medium hover:bg-muted">Optimizer</Link>
        </div>
      </header>

      {!board.run ? (
        <section className="rounded-xl border bg-card p-6">
          <h2 className="font-semibold">No projection run available</h2>
          <p className="mt-2 text-sm text-muted-foreground">The scheduled projection pipeline has not saved a regular-season run yet.</p>
        </section>
      ) : <>
        <dl className="grid gap-3 rounded-xl border bg-card p-4 text-sm shadow-xs sm:grid-cols-4">
          <div><dt className="text-xs text-muted-foreground">Week</dt><dd className="font-semibold">{board.run.season} · Week {board.run.week}</dd></div>
          <div><dt className="text-xs text-muted-foreground">Projection as of</dt><dd className="font-semibold">{formatKickoff(board.run.asOf)}</dd></div>
          <div><dt className="text-xs text-muted-foreground">Games not yet started</dt><dd className="font-semibold">{board.games.length}</dd></div>
          <div><dt className="text-xs text-muted-foreground">Model</dt><dd className="font-semibold">{board.run.modelVersion}</dd></div>
        </dl>
        {board.games.length === 0 && <Notice>Every game in this saved week has started. The next week appears when the scheduled pipeline saves its projection run.</Notice>}
        {board.contextError && <Notice>{board.contextError}</Notice>}
        {board.games.length > 0 && (
          <ProjectionsExplorer key={board.run.runId} board={board} week={week} initial={parseFilters(params, teams)} />
        )}
        <footer className="space-y-1 text-xs text-muted-foreground">
          <p>{board.salaryAsOf
            ? `Salaries are DraftKings Classic, from the player pool captured ${formatKickoff(board.salaryAsOf)}. Classic has no kickers, so kickers show no salary.`
            : "No DraftKings Classic player pool has been captured for this week yet, so salaries are blank."}</p>
          {board.omittedPlayers > 0 && <p>{board.omittedPlayers} saved projection rows are hidden because their game started or the saved team and opponent did not match the schedule.</p>}
          <p>Projections are frozen at the run time. Implied points, lines and the quarterback passing context refresh when you open the page, press Refresh, or every five minutes while it is open; they describe the matchup and never change a projection.</p>
          <p>Players with too few NFL games for their own projection get the position average, which has run several points high for them, so they are hidden unless you include them. Defenses show the opponent&apos;s implied points.</p>
        </footer>
      </>}
    </main>
  );
}
