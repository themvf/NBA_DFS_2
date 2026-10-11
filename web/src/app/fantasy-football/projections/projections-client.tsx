"use client";

/**
 * Owns the position selection for the whole page.
 *
 * The chart and the weekly grid answer the same question about the same
 * position, so they share one set of tabs rather than each carrying its own --
 * two tab strips saying "QB" on the same screen is a way to get them out of
 * sync, not a convenience.
 */

import { useState } from "react";
import type {
  ProjectionScatterRow,
  WeeklyPointsRow,
  WeeklyUpsideRow,
} from "@/db/queries-fantasy-football";
import {
  POSITION_COLORS,
  VIEW_LABELS,
  VIEW_ORDER,
  VIEW_POSITIONS,
  type ScatterView,
} from "@/lib/fantasy-football/projection-scatter";
import ProjectionChart from "./projection-chart";
import UpsideTable from "./upside-table";
import WeeklyTable from "./weekly-table";

export default function ProjectionsClient({
  rows,
  weeklyCurrent,
  weeklyPrior,
  currentSeason,
  priorSeason,
  currentThroughWeek,
  priorThroughWeek,
  upsideSeasons,
  upsideRecent,
  upsideAll,
  scoring,
}: {
  rows: ProjectionScatterRow[];
  weeklyCurrent: WeeklyPointsRow[];
  weeklyPrior: WeeklyPointsRow[];
  currentSeason: number;
  priorSeason: number;
  currentThroughWeek: number | null;
  priorThroughWeek: number | null;
  upsideSeasons: number[];
  upsideRecent: WeeklyUpsideRow[];
  upsideAll: WeeklyUpsideRow[];
  scoring: string;
}) {
  const [view, setView] = useState<ScatterView>("QB");
  // The season in progress is what an in-season reader wants first. Before
  // week 1 has been played there is nothing in it, so the opening choice
  // falls back to the completed season rather than an empty grid.
  const [weeklyChoice, setWeeklyChoice] = useState<"current" | "prior">(
    weeklyCurrent.length > 0 ? "current" : "prior",
  );
  const weekly = weeklyChoice === "current" ? weeklyCurrent : weeklyPrior;
  const weeklySeason = weeklyChoice === "current" ? currentSeason : priorSeason;
  const weeklyThroughWeek = weeklyChoice === "current" ? currentThroughWeek : priorThroughWeek;
  // Both season windows are fetched up front so the toggle is instant; a
  // 3-season window is the better base for a variance estimate, but the most
  // recent season is what people reach for first.
  const [seasonMode, setSeasonMode] = useState<"recent" | "all">("recent");

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-2">
        {VIEW_ORDER.map((key) => {
          const active = key === view;
          const accent = key === "FLEX" ? null : POSITION_COLORS[VIEW_POSITIONS[key][0]];
          return (
            <button
              key={key}
              type="button"
              onClick={() => setView(key)}
              aria-pressed={active}
              className={`rounded-lg border px-4 py-2 text-sm font-bold transition ${
                active ? "border-slate-900 bg-slate-900 text-white" : "hover:bg-muted"
              }`}
            >
              <span className="flex items-center gap-2">
                {accent && (
                  <span
                    className="inline-block size-2.5 rounded-full"
                    style={{ background: accent.light }}
                  />
                )}
                {VIEW_LABELS[key]}
              </span>
            </button>
          );
        })}
      </div>

      <ProjectionChart rows={rows} view={view} />

      <div id="weekly" className="scroll-mt-20 space-y-2">
        <div className="flex items-center justify-end gap-2">
          <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
            Weekly season
          </span>
          <div className="flex overflow-hidden rounded-lg border">
            {(
              [
                ["current", currentSeason],
                ["prior", priorSeason],
              ] as const
            ).map(([value, year]) => (
              <button
                key={value}
                type="button"
                aria-pressed={weeklyChoice === value}
                onClick={() => setWeeklyChoice(value)}
                className={`px-2.5 py-1 text-xs ${
                  weeklyChoice === value ? "bg-slate-900 font-bold text-white" : "hover:bg-muted"
                }`}
              >
                {year}
              </button>
            ))}
          </div>
        </div>
        <WeeklyTable
          rows={weekly}
          season={weeklySeason}
          throughWeek={weeklyThroughWeek}
          scoring={scoring}
          view={view}
        />
      </div>

      <div id="upside" className="scroll-mt-20">
        <UpsideTable
          rows={seasonMode === "all" ? upsideAll : upsideRecent}
          view={view}
          seasons={seasonMode === "all" ? upsideSeasons : [upsideSeasons[0]]}
          seasonMode={seasonMode}
          onSeasonModeChange={setSeasonMode}
        />
      </div>
    </div>
  );
}
