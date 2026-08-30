export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  getLatestRankingSet,
  getProjectionScatter,
  getWeeklyFantasyPoints,
} from "@/db/queries-fantasy-football";
import ProjectionChart from "./projection-chart";
import WeeklyTable from "./weekly-table";

const SCORING_OPTIONS = ["STD", "HALF", "PPR"];
/** Weekly actuals come from the most recent completed season. */
const WEEKLY_SEASON = 2025;

export default async function ProjectionsPage({
  searchParams,
}: {
  searchParams: Promise<{ scoring?: string }>;
}) {
  const params = await searchParams;
  const requested = String(params.scoring || "PPR").toUpperCase();
  const scoring = SCORING_OPTIONS.includes(requested) ? requested : "PPR";
  const set = await getLatestRankingSet(scoring);
  const [rows, weekly] = set
    ? await Promise.all([
        getProjectionScatter(set.id),
        getWeeklyFantasyPoints(set.id, WEEKLY_SEASON, "QB"),
      ])
    : [[], []];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-bold uppercase tracking-widest text-emerald-700">
            Last season against next season
          </p>
          <h1 className="text-3xl font-black">Projection Scatter</h1>
          <p className="max-w-3xl text-sm text-muted-foreground">
            Where each player scored in 2025 against where our board puts them in 2026. Above the
            dashed parity line the model expects growth; below it, decline. Rookies, and anyone else
            without a 2025 sample, sit in the strip at the left rather than at a fake zero.
          </p>
        </div>
        <div className="flex gap-2">
          {SCORING_OPTIONS.map((value) => (
            <Link
              key={value}
              href={`/fantasy-football/projections?scoring=${value}`}
              className={`rounded-lg px-3 py-2 text-sm font-bold ${
                value === scoring ? "bg-slate-900 text-white" : "border hover:bg-muted"
              }`}
            >
              {value}
            </Link>
          ))}
        </div>
      </div>

      {!set ? (
        <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6">
          Run the <code>Refresh Fantasy Football Draft Data</code> GitHub workflow to populate this
          page.
        </div>
      ) : (
        <>
          <p className="text-xs text-muted-foreground">
            {set.name} · {rows.length} projected players ·{" "}
            {new Date(set.createdAt).toLocaleString()}
          </p>
          <ProjectionChart rows={rows} scoring={scoring} modelVersion={set.modelVersion ?? null} />
          {weekly.length > 0 ? (
            <WeeklyTable rows={weekly} season={WEEKLY_SEASON} scoring={scoring} position="QB" />
          ) : (
            <div className="rounded-2xl border border-amber-300 bg-amber-50 p-5 text-sm">
              No {WEEKLY_SEASON} weekly stat lines are stored yet. Run{" "}
              <code>python -m ingest.ff_backfill_week_stats --season {WEEKLY_SEASON}</code>, or wait
              for the next scheduled Fantasy Football refresh.
            </div>
          )}
        </>
      )}
    </div>
  );
}
