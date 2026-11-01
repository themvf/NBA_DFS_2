import {
  boomThreshold, formatNumber, formatPercent, isPositionEstimate, POSITIONS, type RollingPlayer,
} from "@/lib/nfl-dfs/rolling-projection-view";
import { PositionChip } from "./projection-card";
import { positionStyle } from "./range-bar";

export function PositionLegend({ players }: { players: readonly RollingPlayer[] }) {
  const present = POSITIONS.filter(position => players.some(player => player.position === position));
  if (present.length < 2) return null;
  return (
    <ul aria-label="Positions" className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
      {present.map(position => (
        <li key={position} style={positionStyle(position)} className="inline-flex items-center gap-1.5">
          <span aria-hidden className="size-2 rounded-full bg-[var(--pos)] dark:bg-[var(--pos-dark)]" />
          {position}
        </li>
      ))}
    </ul>
  );
}

/** Placed at (x, y) in the chart's pixel space, flipped to stay inside the chart width. */
export function PlayerTooltip({ player, x, y, width }: { player: RollingPlayer; x: number; y: number; width: number }) {
  const threshold = boomThreshold(player.position);
  const flip = x > width - 200;
  return (
    <div role="tooltip" style={{ left: flip ? undefined : x + 12, right: flip ? width - x + 12 : undefined, top: y }}
      className="pointer-events-none absolute z-20 w-48 -translate-y-1/2 rounded-lg border bg-popover p-2.5 text-xs text-popover-foreground shadow-md">
      <div className="flex items-center gap-1.5">
        <PositionChip position={player.position} />
        <span className="truncate font-semibold">{player.name}</span>
      </div>
      <p className="mt-0.5 text-muted-foreground">{player.team} vs {player.opponent}</p>
      <dl className="mt-2 grid grid-cols-2 gap-x-2 gap-y-1 tabular-nums">
        <dt className="text-muted-foreground">Projection</dt><dd className="text-right font-semibold">{formatNumber(player.projected)}</dd>
        <dt className="text-muted-foreground">Floor</dt><dd className="text-right">{formatNumber(player.floor)}</dd>
        <dt className="text-muted-foreground">Median</dt><dd className="text-right">{formatNumber(player.median)}</dd>
        <dt className="text-muted-foreground">Ceiling</dt><dd className="text-right">{formatNumber(player.ceiling)}</dd>
        <dt className="text-muted-foreground">{threshold == null ? "Boom" : `Boom ${threshold}+`}</dt>
        <dd className="text-right">{formatPercent(player.boomRate)}</dd>
      </dl>
      {isPositionEstimate(player) && <p className="mt-2 text-muted-foreground">Position estimate: too few games for his own.</p>}
    </div>
  );
}
