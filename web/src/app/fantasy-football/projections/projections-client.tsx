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
  WEEKLY_SEASONS,
  type ScatterView,
} from "@/lib/fantasy-football/projection-scatter";
import ProjectionChart from "./projection-chart";
import UpsideTable from "./upside-table";
import WeeklyTable from "./weekly-table";

export default function ProjectionsClient({
  rows,
  weekly,
  weeklySeason,
  upsideRecent,
  upsideAll,
  scoring,
}: {
  rows: ProjectionScatterRow[];
  weekly: WeeklyPointsRow[];
  weeklySeason: number;
  upsideRecent: WeeklyUpsideRow[];
  upsideAll: WeeklyUpsideRow[];
  scoring: string;
}) {
  const [view, setView] = useState<ScatterView>("QB");
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

      <div id="weekly" className="scroll-mt-20">
        <WeeklyTable rows={weekly} season={weeklySeason} scoring={scoring} view={view} />
      </div>

      <div id="upside" className="scroll-mt-20">
        <UpsideTable
          rows={seasonMode === "all" ? upsideAll : upsideRecent}
          view={view}
          seasons={seasonMode === "all" ? WEEKLY_SEASONS : [WEEKLY_SEASONS[0]]}
          seasonMode={seasonMode}
          onSeasonModeChange={setSeasonMode}
        />
      </div>
    </div>
  );
}
