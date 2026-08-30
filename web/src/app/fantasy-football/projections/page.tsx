export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  getLatestRankingSet,
  getProjectionScatter,
  getWeeklyFantasyPoints,
  getWeeklyUpside,
} from "@/db/queries-fantasy-football";
import { BASELINE_GAMES, WEEKLY_SEASONS } from "@/lib/fantasy-football/projection-scatter";
import ProjectionsClient from "./projections-client";

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
  const [rows, weekly, upsideRecent, upsideAll] = set
    ? await Promise.all([
        getProjectionScatter(set.id),
        getWeeklyFantasyPoints(set.id, WEEKLY_SEASON),
        getWeeklyUpside(set.id, [WEEKLY_SEASON]),
        getWeeklyUpside(set.id, WEEKLY_SEASONS),
      ])
    : [[], [], [], []];

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
        <div className="flex flex-wrap items-center gap-2">
          <a
            href="#weekly"
            className="rounded-lg border px-3 py-2 text-sm font-bold hover:bg-muted"
          >
            Week by week &darr;
          </a>
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
          {weekly.length === 0 && (
            <div className="rounded-2xl border border-amber-300 bg-amber-50 p-5 text-sm">
              No {WEEKLY_SEASON} weekly stat lines are stored yet, so the week-by-week grid is
              empty. Run{" "}
              <code>python -m ingest.ff_backfill_week_stats --season {WEEKLY_SEASON}</code>, or wait
              for the next scheduled Fantasy Football refresh.
            </div>
          )}
          <ProjectionsClient
            rows={rows}
            weekly={weekly}
            weeklySeason={WEEKLY_SEASON}
            upsideRecent={upsideRecent}
            upsideAll={upsideAll}
            scoring={scoring}
          />
          <div className="space-y-2 rounded-2xl border bg-card p-4 text-xs text-muted-foreground">
            <p className="text-[10px] font-bold uppercase tracking-widest">How to read this</p>
            <p>
              <strong className="text-foreground">Per game is the fair comparison.</strong> The 2026
              projection is a {BASELINE_GAMES}-active-game baseline, so on the season-total view
              anyone who missed time in 2025 shows an enormous gain that is really just health.
              Switch views and watch the injured players move.
            </p>
            <p>
              <strong className="text-foreground">Scoring is {scoring}.</strong> 2025 actuals are
              resolved in the same format as the projection, so the two axes are always comparable.
              Kicker points use Yahoo distance tiers and DEF points use Yahoo defensive scoring; both
              are identical across STD, HALF and PPR.
            </p>
            <p>
              <strong className="text-foreground">Source.</strong> 2025 actuals from nflverse season
              and weekly features; 2026 numbers from the live{" "}
              <code className="rounded bg-muted px-1 py-0.5">
                {set.modelVersion ?? "independent"}
              </code>{" "}
              board. ADP is Fantasy Football Calculator 12-team {scoring} &mdash; comparison only, it
              never moves the projection.
            </p>
          </div>
        </>
      )}
    </div>
  );
}
