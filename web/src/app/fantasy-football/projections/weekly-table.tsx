"use client";

/**
 * Week-by-week fantasy points, one row per player.
 *
 * Columns are grouped Season (Weeks 1-14) and Playoff (Weeks 15-17), matching
 * how this app splits fantasy weeks everywhere else. A week the player did not
 * record a stat line renders as 0 -- but a bye is tinted differently from a
 * played week, because "the schedule gave him nothing to do" and "he played
 * and produced nothing" are not the same fact about a player.
 */

import { useMemo, useState } from "react";
import type { WeeklyPointsRow } from "@/db/queries-fantasy-football";
import {
  PLAYOFF_WEEKS,
  SEASON_WEEKS,
  weekHeat,
} from "@/lib/fantasy-football/projection-scatter";

type Sort = "season" | "playoff" | "total" | "avg";

const ALL_WEEKS = [...SEASON_WEEKS, ...PLAYOFF_WEEKS];
const fmt = (n: number) => (Number.isInteger(n) ? String(n) : n.toFixed(1));

export default function WeeklyTable({
  rows,
  season,
  scoring,
  position,
}: {
  rows: WeeklyPointsRow[];
  season: number;
  scoring: string;
  position: string;
}) {
  const [draftedOnly, setDraftedOnly] = useState(true);
  const [sort, setSort] = useState<Sort>("total");

  const prepared = useMemo(() => {
    const scoped = draftedOnly ? rows.filter((r) => r.adp != null) : rows;
    return scoped
      .map((row) => {
        const points = (week: number) => row.weeks[String(week)] ?? 0;
        const played = ALL_WEEKS.filter((w) => row.weeks[String(w)] != null).length;
        const seasonTotal = SEASON_WEEKS.reduce((sum, w) => sum + points(w), 0);
        const playoffTotal = PLAYOFF_WEEKS.reduce((sum, w) => sum + points(w), 0);
        // Week 18 is not a fantasy week and gets no column of its own, but it
        // is shown in the totals so a row reconciles against the season total
        // on the chart above instead of silently disagreeing with it.
        const week18 = points(18);
        return {
          row,
          points,
          played,
          seasonTotal,
          playoffTotal,
          week18,
          total: seasonTotal + playoffTotal,
          avg: played ? (seasonTotal + playoffTotal) / played : 0,
        };
      })
      .sort((a, b) =>
        sort === "season"
          ? b.seasonTotal - a.seasonTotal
          : sort === "playoff"
            ? b.playoffTotal - a.playoffTotal
            : sort === "avg"
              ? b.avg - a.avg
              : b.total - a.total,
      );
  }, [rows, draftedOnly, sort]);

  const sortButton = (key: Sort, label: string) => (
    <button
      type="button"
      onClick={() => setSort(key)}
      aria-pressed={sort === key}
      className={`rounded-md px-2 py-1 text-xs ${
        sort === key ? "bg-slate-900 font-bold text-white" : "border hover:bg-muted"
      }`}
    >
      {label}
    </button>
  );

  return (
    <section className="space-y-3 rounded-2xl border bg-card p-4">
      <div className="flex flex-wrap items-end gap-x-6 gap-y-3">
        <div>
          <h2 className="font-bold">
            {position} week by week &mdash; {season} actual
          </h2>
          <p className="text-xs text-muted-foreground">
            {scoring} points per week. Season is Weeks 1&ndash;14; Playoff is Weeks 15&ndash;17, the
            three fantasy tournament rounds.
          </p>
        </div>
        <div className="ml-auto flex flex-wrap items-center gap-3">
          <label className="flex cursor-pointer items-center gap-2 text-xs">
            <input
              type="checkbox"
              checked={draftedOnly}
              onChange={(event) => setDraftedOnly(event.target.checked)}
              className="size-3.5 accent-slate-900"
            />
            Drafted only
          </label>
          <span className="flex items-center gap-1.5">
            <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
              Sort
            </span>
            {sortButton("total", "Total")}
            {sortButton("season", "Season")}
            {sortButton("playoff", "Playoff")}
            {sortButton("avg", "Avg")}
          </span>
        </div>
      </div>

      <div className="max-h-[560px] overflow-auto rounded-lg border">
        <table className="border-separate border-spacing-0 text-xs tabular-nums">
          <thead>
            <tr>
              <th
                rowSpan={2}
                className="sticky left-0 top-0 z-30 min-w-[170px] border-b border-r bg-card p-2 text-left text-[10px] uppercase tracking-wider text-muted-foreground"
              >
                {position}
              </th>
              <th
                colSpan={SEASON_WEEKS.length}
                className="sticky top-0 z-20 border-b border-r bg-card p-1.5 text-center text-[10px] font-bold uppercase tracking-widest text-muted-foreground"
              >
                Season weeks
              </th>
              <th
                colSpan={PLAYOFF_WEEKS.length}
                className="sticky top-0 z-20 border-b border-r bg-amber-50 p-1.5 text-center text-[10px] font-bold uppercase tracking-widest text-amber-900"
              >
                Playoff weeks
              </th>
              <th
                colSpan={5}
                className="sticky top-0 z-20 border-b bg-card p-1.5 text-center text-[10px] font-bold uppercase tracking-widest text-muted-foreground"
              >
                Totals
              </th>
            </tr>
            <tr>
              {SEASON_WEEKS.map((week) => (
                <th
                  key={week}
                  className="sticky top-[26px] z-20 w-9 border-b bg-card p-1 text-center text-[10px] font-semibold text-muted-foreground"
                >
                  {week}
                </th>
              ))}
              {PLAYOFF_WEEKS.map((week) => (
                <th
                  key={week}
                  className="sticky top-[26px] z-20 w-9 border-b bg-amber-50 p-1 text-center text-[10px] font-semibold text-amber-900"
                >
                  {week}
                </th>
              ))}
              {["Szn", "Post", "Tot", "W18", "Avg"].map((label) => (
                <th
                  key={label}
                  className="sticky top-[26px] z-20 w-12 border-b border-l bg-card p-1 text-center text-[10px] font-semibold text-muted-foreground"
                >
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {prepared.map(({ row, points, played, seasonTotal, playoffTotal, total, week18, avg }) => (
              <tr key={row.playerId} className="group">
                <th
                  scope="row"
                  className="sticky left-0 z-10 border-b border-r bg-card p-2 text-left font-semibold group-hover:bg-muted"
                >
                  {row.name}
                  <span className="ml-1.5 font-normal text-[10px] uppercase tracking-wide text-muted-foreground">
                    {row.team}
                    {row.adp == null && " · undrafted"}
                  </span>
                </th>
                {ALL_WEEKS.map((week) => {
                  const value = points(week);
                  const didPlay = row.weeks[String(week)] != null;
                  const isBye = row.seasonByeWeek === week;
                  const heat = didPlay ? weekHeat(value) : null;
                  const lastSeasonWeek = week === SEASON_WEEKS[SEASON_WEEKS.length - 1];
                  const lastPlayoffWeek = week === PLAYOFF_WEEKS[PLAYOFF_WEEKS.length - 1];
                  return (
                    <td
                      key={week}
                      title={
                        isBye
                          ? `Week ${week}: bye`
                          : didPlay
                            ? `Week ${week}: ${fmt(value)}`
                            : `Week ${week}: did not play`
                      }
                      className={`border-b p-1 text-center ${
                        lastSeasonWeek || lastPlayoffWeek ? "border-r" : ""
                      } ${!didPlay ? (isBye ? "bg-muted/70 text-muted-foreground/70" : "text-muted-foreground/60") : ""}`}
                      style={heat ? { background: heat.bg, color: heat.fg } : undefined}
                    >
                      {didPlay ? fmt(value) : 0}
                    </td>
                  );
                })}
                <td className="border-b border-l p-1 text-center font-semibold">{fmt(seasonTotal)}</td>
                <td className="border-b p-1 text-center font-semibold">{fmt(playoffTotal)}</td>
                <td className="border-b p-1 text-center font-bold">{fmt(total)}</td>
                <td
                  className="border-b p-1 text-center text-muted-foreground/70"
                  title="Week 18 -- not scored by standard fantasy formats, shown so the row adds up to the season total"
                >
                  {fmt(week18)}
                </td>
                <td className="border-b p-1 text-center text-muted-foreground">
                  {played ? avg.toFixed(1) : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          Fewer
          {[0.01, 10, 15, 20, 25, 30].map((min) => {
            const heat = weekHeat(min);
            return (
              <span
                key={min}
                className="inline-block size-3.5 rounded-sm border"
                style={heat ? { background: heat.bg } : undefined}
              />
            );
          })}
          More points
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block size-3.5 rounded-sm border bg-muted/70" />
          Bye week
        </span>
        <span>
          A week with no stat line shows 0. Bye weeks are tinted so a scheduled zero stays
          distinguishable from a played one. &ldquo;Tot&rdquo; is Weeks 1&ndash;17, the weeks fantasy
          actually scores; Week 18 gets no column of its own but is carried in{" "}
          <strong className="text-foreground">W18</strong> so Szn + Post + W18 reconciles with the
          season total on the chart above.
        </span>
      </div>
    </section>
  );
}
