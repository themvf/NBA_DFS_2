"use client";

import { useState } from "react";
import { niceTicks } from "@/lib/charts";
import { formatSalaryShort, isPositionEstimate, type RollingPlayer } from "@/lib/nfl-dfs/rolling-projection-view";
import { useElementWidth } from "@/lib/use-element-width";
import { ExpandButton, PlayerTooltip, PositionLegend } from "./chart-parts";
import { positionStyle } from "./range-bar";

const ROW_HEIGHT = 24;
const MARGIN = { top: 18, right: 62, bottom: 26, left: 156 };
const NAME_CHARS = 20;

const label = (name: string) => name.length > NAME_CHARS ? `${name.slice(0, NAME_CHARS - 1)}…` : name;

/** The first rows of the current list, in its current order, on one points axis. */
export default function RangeChart({ players, rowCount, expanded, onToggleExpand }: {
  players: readonly RollingPlayer[]; rowCount: number; expanded: boolean; onToggleExpand: () => void;
}) {
  const [wrapRef, width] = useElementWidth<HTMLDivElement>(560);
  const [hover, setHover] = useState<number | null>(null);
  const rows = players.filter(player => player.projected != null).slice(0, rowCount);
  const lo = Math.min(0, ...rows.map(player => player.floor ?? 0));
  const hi = Math.max(1, ...rows.map(player => player.ceiling ?? player.projected ?? 0));
  const ticks = niceTicks(0, hi, 5);
  const d0 = lo, d1 = ticks[ticks.length - 1];
  const x0 = MARGIN.left, x1 = width - MARGIN.right;
  const sx = (value: number) => x0 + ((value - d0) / (d1 - d0)) * (x1 - x0);
  const height = MARGIN.top + rows.length * ROW_HEIGHT + MARGIN.bottom;
  const rowY = (index: number) => MARGIN.top + index * ROW_HEIGHT + ROW_HEIGHT / 2;
  const hovered = hover == null ? null : rows[hover];

  return (
    <figure className="min-w-0 rounded-xl border bg-card p-4 shadow-xs">
      <figcaption className="flex flex-wrap items-baseline justify-between gap-2">
        <span className="text-sm font-semibold">Floor to ceiling, top {rows.length}</span>
        <span className="flex items-center gap-3 text-xs text-muted-foreground">
          Bar: floor to ceiling · tick: median · dot: projection
          <ExpandButton expanded={expanded} onToggle={onToggleExpand} />
        </span>
      </figcaption>
      <div className="mt-2"><PositionLegend players={rows} /></div>
      <div ref={wrapRef} className="relative mt-2 w-full" onMouseLeave={() => setHover(null)}>
        <svg width={width} height={height} role="img"
          aria-label={`DraftKings points range for the top ${rows.length} players in the current list`}>
          {ticks.map(tick => (
            <g key={tick}>
              <line x1={sx(tick)} x2={sx(tick)} y1={MARGIN.top} y2={height - MARGIN.bottom}
                className={tick === 0 ? "stroke-muted-foreground/40" : "stroke-border"} strokeWidth={1} />
              <text x={sx(tick)} y={height - 8} textAnchor="middle" className="fill-muted-foreground text-[10px] tabular-nums">{tick}</text>
            </g>
          ))}
          <text x={width - 4} y={10} textAnchor="end" className="fill-muted-foreground text-[10px]">Salary</text>
          {rows.map((player, index) => {
            const y = rowY(index);
            const active = hover === index;
            return (
              <g key={player.id} style={positionStyle(player.position)} onMouseEnter={() => setHover(index)}>
                <rect x={0} y={y - ROW_HEIGHT / 2} width={width} height={ROW_HEIGHT}
                  className={active ? "fill-muted" : "fill-transparent"} />
                <text x={MARGIN.left - 10} y={y} dy="0.35em" textAnchor="end"
                  className={`text-[11px] ${active ? "fill-foreground font-semibold" : "fill-foreground"}`}>
                  {label(player.name)}
                </text>
                {player.floor != null && player.ceiling != null && (
                  <rect x={sx(player.floor)} y={y - 4} height={8} rx={4}
                    width={Math.max(2, sx(player.ceiling) - sx(player.floor))}
                    className={`fill-[var(--pos)]/30 dark:fill-[var(--pos-dark)]/35 ${isPositionEstimate(player) ? "stroke-[var(--pos)] dark:stroke-[var(--pos-dark)]" : ""}`}
                    strokeDasharray={isPositionEstimate(player) ? "3 2" : undefined} />
                )}
                {player.median != null && (
                  <line x1={sx(player.median)} x2={sx(player.median)} y1={y - 7} y2={y + 7}
                    className="stroke-foreground/60" strokeWidth={2} strokeLinecap="round" />
                )}
                <text x={width - 4} y={y} dy="0.35em" textAnchor="end"
                  className={`text-[11px] tabular-nums ${player.salary == null ? "fill-muted-foreground" : "fill-foreground"}`}>
                  {formatSalaryShort(player.salary)}
                </text>
                <circle cx={sx(player.projected!)} cy={y} r={5}
                  className="fill-[var(--pos)] stroke-card dark:fill-[var(--pos-dark)]" strokeWidth={2} />
              </g>
            );
          })}
        </svg>
        {hovered && <PlayerTooltip player={hovered} x={sx(hovered.projected!)} y={rowY(hover!)} width={width} />}
      </div>
      <p className="mt-1 text-[11px] text-muted-foreground">DK points and DraftKings Classic salary. Ordered by the sort above; change filters or sort to redraw.</p>
    </figure>
  );
}
