"use client";

import { ChevronDown, ChevronUp, ChevronsUpDown } from "lucide-react";
import type { PoolSort, PoolSortKey } from "@/lib/nfl-dfs/player-pool-sort";

/**
 * The pool table's header row. Column order here is the single source of truth
 * for the body's cell order -- if a column moves, it moves in both places.
 *
 * `Control` and `Target %` hold inputs rather than data, so they are not
 * sortable and render as plain headers.
 */
const COLUMNS: readonly { label: string; key?: PoolSortKey; align: "left" | "right" | "center"; pad?: boolean; hint?: string }[] = [
  { label: "Control", align: "left", pad: true },
  { label: "Exposure %", align: "center", pad: true },
  { label: "Player", key: "name", align: "left", pad: true },
  { label: "Pos", key: "position", align: "left" },
  { label: "Team", key: "team", align: "left" },
  { label: "Salary", key: "salary", align: "right" },
  {
    label: "Value", key: "value", align: "right",
    hint: "Sort by projected points per $1,000. Players whose projection is a position prior, and players ruled out, sort to the bottom in both directions — their multiple is not a value verdict.",
  },
  { label: "Our", key: "ourProj", align: "right", hint: "Sort by our projection. Players with no model projection sort to the bottom, ordered by DK average." },
  { label: "Player P10", key: "floorFpts", align: "right" },
  { label: "Player P90", key: "ceilingFpts", align: "right" },
  { label: "DK Avg", key: "avgFptsDk", align: "right" },
  { label: "FantasyPros", key: "fantasyprosProj", align: "right" },
  { label: "LineStar", key: "linestarProj", align: "right" },
  { label: "Own", key: "linestarOwnPct", align: "right", pad: true },
];

const JUSTIFY = { left: "justify-start", right: "justify-end", center: "justify-center" } as const;
const TEXT = { left: "text-left", right: "text-right", center: "text-center" } as const;

export function PoolTableHeader({ sort, onSort }: { sort: PoolSort; onSort: (key: PoolSortKey) => void }) {
  return (
    <thead className="sticky top-0 z-10 bg-slate-100 text-left text-[10px] uppercase text-slate-500">
      <tr>
        {COLUMNS.map((column) => {
          const active = column.key !== undefined && sort.key === column.key;
          const ariaSort = active ? (sort.direction === "asc" ? "ascending" : "descending") : undefined;
          return (
            <th
              key={column.label}
              scope="col"
              aria-sort={column.key ? (ariaSort ?? "none") : undefined}
              className={`${column.pad ? "p-3" : ""} ${TEXT[column.align]} ${active ? "text-slate-900" : ""}`}
            >
              {column.key === undefined ? column.label : (
                <button
                  type="button"
                  onClick={() => onSort(column.key as PoolSortKey)}
                  title={column.hint ?? `Sort by ${column.label}. Blanks stay at the bottom.`}
                  className={`inline-flex w-full items-center gap-1 uppercase hover:text-blue-700 ${JUSTIFY[column.align]} ${active ? "font-bold text-slate-900" : ""}`}
                >
                  {column.align === "right" ? null : <span>{column.label}</span>}
                  {/* The icon sits on the inside edge so the label stays flush
                      with the numbers it heads. */}
                  {active
                    ? (sort.direction === "asc"
                        ? <ChevronUp className="h-3 w-3 shrink-0 text-blue-700" aria-hidden />
                        : <ChevronDown className="h-3 w-3 shrink-0 text-blue-700" aria-hidden />)
                    : <ChevronsUpDown className="h-3 w-3 shrink-0 text-slate-300" aria-hidden />}
                  {column.align === "right" ? <span>{column.label}</span> : null}
                </button>
              )}
            </th>
          );
        })}
      </tr>
    </thead>
  );
}
