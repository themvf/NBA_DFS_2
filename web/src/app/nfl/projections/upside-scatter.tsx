"use client";

import { useState, type MouseEvent } from "react";
import { nearestPoint, niceTicks } from "@/lib/charts";
import { isPositionEstimate, pointsPerThousand, type RollingPlayer } from "@/lib/nfl-dfs/rolling-projection-view";
import { useElementWidth } from "@/lib/use-element-width";
import { ExpandButton, PlayerTooltip, PositionLegend } from "./chart-parts";
import { positionStyle } from "./range-bar";

const MARGIN = { top: 12, right: 22, bottom: 40, left: 48 };
const LABELS = 6;
const LABEL_GAP = 14;
const HOVER_RADIUS = 18;

type Axis = { label: string; value: (player: RollingPlayer) => number | null; tick: (value: number) => string; fromZero: boolean };
type Mode = {
  name: string; title: string; x: Axis; y: Axis;
  guides: { slope: number; label: string }[];
  rank: (player: RollingPlayer) => number | null;
  missing: string; note: string;
};

const points = (value: number) => String(value);
const MODES = {
  ceiling: {
    name: "Ceiling", title: "Projection vs ceiling",
    x: { label: "Projection (DK pts)", value: player => player.projected, tick: points, fromZero: true },
    y: { label: "Ceiling (DK pts)", value: player => player.ceiling, tick: points, fromZero: true },
    guides: [{ slope: 2, label: "2x" }],
    rank: player => player.ceiling,
    missing: "without a range",
    note: "Ceiling is the 90th percentile. Dashed line: ceiling twice the projection; dots above it have the most room to run.",
  },
  salary: {
    name: "Salary", title: "Projection vs salary",
    x: { label: "DraftKings Classic salary", value: player => player.salary, tick: value => `$${value / 1000}K`, fromZero: false },
    y: { label: "Projection (DK pts)", value: player => player.projected, tick: points, fromZero: true },
    guides: [{ slope: 0.002, label: "2x" }, { slope: 0.003, label: "3x" }],
    rank: pointsPerThousand,
    missing: "without a Classic salary",
    note: "Dashed lines: 2x and 3x points per $1,000 of salary; dots above them are cheap for their projection. Labels are the best points per $1K.",
  },
} satisfies Record<string, Mode>;
type ModeKey = keyof typeof MODES;

export default function UpsideScatter({ players, onSelect, height, expanded, onToggleExpand }: {
  players: readonly RollingPlayer[]; onSelect: (player: RollingPlayer) => void;
  height: number; expanded: boolean; onToggleExpand: () => void;
}) {
  const [wrapRef, width] = useElementWidth<HTMLDivElement>(560);
  const [modeKey, setModeKey] = useState<ModeKey>("ceiling");
  const [hover, setHover] = useState<number | null>(null);
  const mode: Mode = MODES[modeKey];
  const shown = players.filter(player => mode.x.value(player) != null && mode.y.value(player) != null);
  const xs = shown.map(player => mode.x.value(player)!), ys = shown.map(player => mode.y.value(player)!);
  const xTicks = niceTicks(mode.x.fromZero ? 0 : Math.min(...xs), Math.max(1, ...xs), 5);
  const yTicks = niceTicks(mode.y.fromZero ? 0 : Math.min(...ys), Math.max(1, ...ys), 5);
  const xd0 = xTicks[0], xd1 = xTicks[xTicks.length - 1], yd0 = yTicks[0], yd1 = yTicks[yTicks.length - 1];
  const x0 = MARGIN.left, x1 = width - MARGIN.right, y0 = height - MARGIN.bottom, y1 = MARGIN.top;
  const sx = (value: number) => x0 + ((value - xd0) / (xd1 - xd0)) * (x1 - x0);
  const sy = (value: number) => y0 - ((value - yd0) / (yd1 - yd0)) * (y0 - y1);
  const placed = shown.map(player => ({ player, cx: sx(mode.x.value(player)!), cy: sy(mode.y.value(player)!) }));
  const missing = players.length - shown.length;

  const labelled: typeof placed = [];
  for (const point of [...placed].sort((a, b) => (mode.rank(b.player) ?? -Infinity) - (mode.rank(a.player) ?? -Infinity))) {
    if (labelled.length === LABELS) break;
    if (labelled.every(other => Math.abs(other.cy - point.cy) >= LABEL_GAP || Math.abs(other.cx - point.cx) > 120)) labelled.push(point);
  }

  const locate = (event: MouseEvent<SVGSVGElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    return nearestPoint(placed, event.clientX - box.left, event.clientY - box.top, HOVER_RADIUS);
  };
  const choose = (key: ModeKey) => { setModeKey(key); setHover(null); };
  const hovered = hover == null ? null : placed[hover];

  return (
    <figure className="min-w-0 rounded-xl border bg-card p-4 shadow-xs">
      <figcaption className="flex flex-wrap items-center justify-between gap-2">
        <span className="text-sm font-semibold">{mode.title}</span>
        <span className="flex items-center gap-3 text-xs">
          <span role="group" aria-label="Chart" className="inline-flex rounded-md border p-0.5">
            {(Object.keys(MODES) as ModeKey[]).map(key => (
              <button key={key} type="button" aria-pressed={modeKey === key} onClick={() => choose(key)}
                className={`rounded px-2 py-0.5 font-medium focus-visible:outline-2 focus-visible:outline-ring ${modeKey === key ? "bg-foreground text-background" : "text-muted-foreground hover:text-foreground"}`}>
                {MODES[key].name}
              </button>
            ))}
          </span>
          <ExpandButton expanded={expanded} onToggle={onToggleExpand} />
        </span>
      </figcaption>
      <div className="mt-2 flex flex-wrap items-center justify-between gap-2">
        <PositionLegend players={shown} />
        <span className="text-xs text-muted-foreground">
          {shown.length} players{missing > 0 ? ` · ${missing} ${mode.missing}` : ""} · click a dot to find him
        </span>
      </div>
      <div ref={wrapRef} className="relative mt-2 w-full">
        <svg width={width} height={height} role="img" className={hover == null ? undefined : "cursor-pointer"}
          aria-label={`${mode.y.label} against ${mode.x.label} for ${shown.length} players`}
          onMouseMove={event => setHover(locate(event))} onMouseLeave={() => setHover(null)}
          onClick={event => { const index = locate(event); if (index != null) onSelect(placed[index].player); }}>
          {yTicks.map(tick => (
            <g key={`y${tick}`}>
              <line x1={x0} x2={x1} y1={sy(tick)} y2={sy(tick)} className="stroke-border" strokeWidth={1} />
              <text x={x0 - 6} y={sy(tick)} dy="0.35em" textAnchor="end" className="fill-muted-foreground text-[10px] tabular-nums">{mode.y.tick(tick)}</text>
            </g>
          ))}
          {xTicks.map(tick => (
            <text key={`x${tick}`} x={sx(tick)} y={y0 + 16} textAnchor="middle" className="fill-muted-foreground text-[10px] tabular-nums">{mode.x.tick(tick)}</text>
          ))}
          <text x={(x0 + x1) / 2} y={height - 6} textAnchor="middle" className="fill-muted-foreground text-[11px]">{mode.x.label}</text>
          <text x={12} y={(y0 + y1) / 2} textAnchor="middle" transform={`rotate(-90 12 ${(y0 + y1) / 2})`}
            className="fill-muted-foreground text-[11px]">{mode.y.label}</text>
          {mode.guides.map(({ slope, label }) => {
            const start = Math.max(xd0, yd0 / slope), end = Math.min(xd1, yd1 / slope);
            if (end <= start) return null;
            return (
              <g key={label}>
                <line x1={sx(start)} y1={sy(start * slope)} x2={sx(end)} y2={sy(end * slope)}
                  className="stroke-muted-foreground/60" strokeWidth={1.5} strokeDasharray="5 4" />
                <text x={sx(end) + 4} y={sy(end * slope)} dy="0.35em" className="fill-muted-foreground text-[10px]">{label}</text>
              </g>
            );
          })}
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
      <p className="mt-1 text-[11px] text-muted-foreground">{mode.note} Hollow dots are position estimates.</p>
    </figure>
  );
}
