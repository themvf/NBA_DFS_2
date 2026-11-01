"use client";

import { useState, type MouseEvent } from "react";
import { nearestPoint, niceTicks } from "@/lib/charts";
import { isPositionEstimate, type RollingPlayer } from "@/lib/nfl-dfs/rolling-projection-view";
import { useElementWidth } from "@/lib/use-element-width";
import { PlayerTooltip, PositionLegend } from "./chart-parts";
import { positionStyle } from "./range-bar";

const HEIGHT = 380;
const MARGIN = { top: 12, right: 18, bottom: 40, left: 44 };
const LABELS = 6;
const LABEL_GAP = 14;
const HOVER_RADIUS = 18;

/** Projection against ceiling: points high above the dashed 2x line have the most room to run. */
export default function UpsideScatter({ players, onSelect }: {
  players: readonly RollingPlayer[]; onSelect: (player: RollingPlayer) => void;
}) {
  const [wrapRef, width] = useElementWidth<HTMLDivElement>(560);
  const [hover, setHover] = useState<number | null>(null);
  const points = players.filter(player => player.projected != null && player.ceiling != null);
  const xTicks = niceTicks(0, Math.max(1, ...points.map(player => player.projected!)), 5);
  const yTicks = niceTicks(0, Math.max(1, ...points.map(player => player.ceiling!)), 5);
  const xd1 = xTicks[xTicks.length - 1], yd1 = yTicks[yTicks.length - 1];
  const x0 = MARGIN.left, x1 = width - MARGIN.right, y0 = HEIGHT - MARGIN.bottom, y1 = MARGIN.top;
  const sx = (value: number) => x0 + (value / xd1) * (x1 - x0);
  const sy = (value: number) => y0 - (value / yd1) * (y0 - y1);
  const placed = points.map(player => ({ player, cx: sx(player.projected!), cy: sy(player.ceiling!) }));
  const guideEnd = Math.min(xd1, yd1 / 2);

  const labelled: typeof placed = [];
  for (const point of [...placed].sort((a, b) => b.player.ceiling! - a.player.ceiling!)) {
    if (labelled.length === LABELS) break;
    if (labelled.every(other => Math.abs(other.cy - point.cy) >= LABEL_GAP || Math.abs(other.cx - point.cx) > 120)) labelled.push(point);
  }

  const locate = (event: MouseEvent<SVGSVGElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    return nearestPoint(placed, event.clientX - box.left, event.clientY - box.top, HOVER_RADIUS);
  };
  const hovered = hover == null ? null : placed[hover];

  return (
    <figure className="min-w-0 rounded-xl border bg-card p-4 shadow-xs">
      <figcaption className="flex flex-wrap items-baseline justify-between gap-2">
        <span className="text-sm font-semibold">Projection vs ceiling</span>
        <span className="text-xs text-muted-foreground">{points.length} players · click a dot to find him</span>
      </figcaption>
      <div className="mt-2"><PositionLegend players={points} /></div>
      <div ref={wrapRef} className="relative mt-2 w-full">
        <svg width={width} height={HEIGHT} role="img" className={hover == null ? undefined : "cursor-pointer"}
          aria-label={`Projected DK points against ceiling for ${points.length} players`}
          onMouseMove={event => setHover(locate(event))} onMouseLeave={() => setHover(null)}
          onClick={event => { const index = locate(event); if (index != null) onSelect(placed[index].player); }}>
          {yTicks.map(tick => (
            <g key={`y${tick}`}>
              <line x1={x0} x2={x1} y1={sy(tick)} y2={sy(tick)} className="stroke-border" strokeWidth={1} />
              <text x={x0 - 6} y={sy(tick)} dy="0.35em" textAnchor="end" className="fill-muted-foreground text-[10px] tabular-nums">{tick}</text>
            </g>
          ))}
          {xTicks.map(tick => (
            <text key={`x${tick}`} x={sx(tick)} y={y0 + 16} textAnchor="middle" className="fill-muted-foreground text-[10px] tabular-nums">{tick}</text>
          ))}
          <text x={(x0 + x1) / 2} y={HEIGHT - 6} textAnchor="middle" className="fill-muted-foreground text-[11px]">Projection (DK pts)</text>
          <text x={12} y={(y0 + y1) / 2} textAnchor="middle" transform={`rotate(-90 12 ${(y0 + y1) / 2})`}
            className="fill-muted-foreground text-[11px]">Ceiling (DK pts)</text>
          <line x1={sx(0)} y1={sy(0)} x2={sx(guideEnd)} y2={sy(guideEnd * 2)}
            className="stroke-muted-foreground/60" strokeWidth={1.5} strokeDasharray="5 4" />
          {placed.map(({ player, cx, cy }, index) => (
            <circle key={player.id} cx={cx} cy={cy} r={hover === index ? 6 : 4.5} style={positionStyle(player.position)}
              className={isPositionEstimate(player)
                ? "fill-card stroke-[var(--pos)] dark:stroke-[var(--pos-dark)]"
                : "fill-[var(--pos)] stroke-card dark:fill-[var(--pos-dark)]"}
              strokeWidth={1.5} />
          ))}
          {labelled.map(({ player, cx, cy }) => (
            <text key={`label${player.id}`} x={cx + 8} y={cy} dy="0.35em" textAnchor={cx > x1 - 110 ? "end" : "start"}
              dx={cx > x1 - 110 ? -16 : 0} className="pointer-events-none fill-foreground text-[11px]">
              {player.name}
            </text>
          ))}
        </svg>
        {hovered && <PlayerTooltip player={hovered.player} x={hovered.cx} y={hovered.cy} width={width} />}
      </div>
      <p className="mt-1 text-[11px] text-muted-foreground">Ceiling is the 90th percentile. Dashed line: ceiling twice the projection; dots above it have the most room to run. Hollow dots are position estimates.</p>
    </figure>
  );
}
