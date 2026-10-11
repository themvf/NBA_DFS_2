export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  getLatestRankingSet,
  getProjectionScatter,
  getWeeklyCompleteThroughWeek,
  getWeeklyFantasyPoints,
  getWeeklyUpside,
} from "@/db/queries-fantasy-football";
import { BASELINE_GAMES, weeklyUpsideSeasons } from "@/lib/fantasy-football/projection-scatter";
import { currentNflSeason } from "@/lib/nfl/season";
import ProjectionsClient from "./projections-client";

const SCORING_OPTIONS = ["STD", "HALF", "PPR"];

export default async function ProjectionsPage({
  searchParams,
}: {
  searchParams: Promise<{ scoring?: string }>;
}) {
  const params = await searchParams;
  const requested = String(params.scoring || "PPR").toUpperCase();
  const scoring = SCORING_OPTIONS.includes(requested) ? requested : "PPR";
  const set = await getLatestRankingSet(scoring);
  // Seasons come from the board the page is rendering, never a literal year:
  // the grid used to be pinned to 2025 and went on showing last season while
  // the season in progress was ingested daily and never displayed.
  // The header renders before the board is checked, so an empty database must
  // not print season 0. currentNflSeason() is the same helper every other
  // page defaults through.
  const currentSeason = set?.season ?? currentNflSeason();
  const priorSeason = currentSeason - 1;
  const upsideSeasons = weeklyUpsideSeasons(currentSeason);
  // Both weekly seasons are fetched up front so the toggle is instant, the
  // same reason the upside panel pre-fetches both of its windows.
  const [rows, weeklyCurrent, weeklyPrior, upsideRecent, upsideAll, currentThrough, priorThrough] =
    set
      ? await Promise.all([
          getProjectionScatter(set.id),
          getWeeklyFantasyPoints(set.id, currentSeason),
          getWeeklyFantasyPoints(set.id, priorSeason),
          getWeeklyUpside(set.id, [priorSeason]),
          getWeeklyUpside(set.id, upsideSeasons),
          getWeeklyCompleteThroughWeek(currentSeason),
          getWeeklyCompleteThroughWeek(priorSeason),
        ])
      : [[], [], [], [], [], null, null];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-bold uppercase tracking-widest text-emerald-700">
            Last season against this season
          </p>
          <h1 className="text-3xl font-black">Projection Scatter</h1>
          <p className="max-w-3xl text-sm text-muted-foreground">
            Where each player scored in {priorSeason} against where our board puts them in{" "}
            {currentSeason}. Above the dashed parity line the model expects growth; below it,
            decline. Rookies, and anyone else without a {priorSeason} sample, sit in the strip at
            the left rather than at a fake zero.
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
          {weeklyCurrent.length === 0 && weeklyPrior.length === 0 && (
            <div className="rounded-2xl border border-amber-300 bg-amber-50 p-5 text-sm">
              No weekly stat lines are stored for {currentSeason} or {priorSeason}, so the
              week-by-week grid is empty. Run{" "}
              <code>python -m ingest.ff_backfill_week_stats --season {priorSeason}</code>, or wait
              for the next scheduled Fantasy Football refresh.
            </div>
          )}
          <ProjectionsClient
            rows={rows}
            weeklyCurrent={weeklyCurrent}
            weeklyPrior={weeklyPrior}
            currentSeason={currentSeason}
            priorSeason={priorSeason}
            currentThroughWeek={currentThrough}
            priorThroughWeek={priorThrough}
            upsideSeasons={upsideSeasons}
            upsideRecent={upsideRecent}
            upsideAll={upsideAll}
            scoring={scoring}
          />
          <div className="space-y-2 rounded-2xl border bg-card p-4 text-xs text-muted-foreground">
            <p className="text-[10px] font-bold uppercase tracking-widest">How to read this</p>
            <p>
              <strong className="text-foreground">Per game is the fair comparison.</strong> The{" "}
              {currentSeason} projection is a {BASELINE_GAMES}-active-game baseline, so on the
              season-total view anyone who missed time in {priorSeason} shows an enormous gain that
              is really just health.
              Switch views and watch the injured players move.
            </p>
            <p>
              <strong className="text-foreground">Scoring is {scoring}.</strong> {priorSeason}{" "}
              actuals are resolved in the same format as the projection, so the two axes are always
              comparable.
              Kicker points use Yahoo distance tiers and DEF points use Yahoo defensive scoring; both
              are identical across STD, HALF and PPR.
            </p>
            <p>
              <strong className="text-foreground">Source.</strong> {priorSeason} actuals from
              nflverse season and weekly features; {currentSeason} numbers from the live{" "}
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
