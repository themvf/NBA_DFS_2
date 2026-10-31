import type { CSSProperties } from "react";
import { POSITION_COLORS } from "@/lib/fantasy-football/projection-scatter";
import { formatNumber, type RollingPlayer } from "@/lib/nfl-dfs/rolling-projection-view";

export function positionStyle(position: string): CSSProperties {
  const colors = POSITION_COLORS[position] ?? POSITION_COLORS.RB;
  return { "--pos": colors.light, "--pos-dark": colors.dark } as CSSProperties;
}

const share = (value: number, max: number) => `${Math.min(100, Math.max(0, (value / max) * 100))}%`;

/** Floor to ceiling as a band, median as a tick, the projection as a dot, all on one shared scale. */
export default function RangeBar({ player, max, compact = false, estimate = false }: {
  player: RollingPlayer; max: number; compact?: boolean; estimate?: boolean;
}) {
  const { floor, median, ceiling, projected } = player;
  const label = `Floor ${formatNumber(floor)}, median ${formatNumber(median)}, ceiling ${formatNumber(ceiling)}, projection ${formatNumber(projected)} DK points`;
  return (
    <div role="img" aria-label={label} style={positionStyle(player.position)}
      className={`relative w-full rounded-full bg-muted ${compact ? "h-2" : "h-3"}`}>
      {floor != null && ceiling != null && (
        <div className={`absolute inset-y-0 rounded-full bg-[var(--pos)]/25 dark:bg-[var(--pos-dark)]/30 ${estimate ? "border border-dashed border-[var(--pos)] dark:border-[var(--pos-dark)]" : ""}`}
          style={{ left: share(floor, max), width: `calc(${share(ceiling, max)} - ${share(floor, max)})` }} />
      )}
      {median != null && (
        <div className="absolute -inset-y-0.5 w-0.5 -translate-x-1/2 rounded bg-foreground/50" style={{ left: share(median, max) }} />
      )}
      {projected != null && (
        <div className={`absolute top-1/2 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-card bg-[var(--pos)] dark:bg-[var(--pos-dark)] ${compact ? "size-2.5" : "size-3.5"}`}
          style={{ left: share(projected, max) }} />
      )}
    </div>
  );
}

export function RangeScale({ max }: { max: number }) {
  const step = max > 40 ? 10 : 5;
  const ticks = Array.from({ length: Math.floor(max / step) + 1 }, (_, index) => index * step);
  return (
    <div aria-hidden className="relative h-4 w-full text-[10px] tabular-nums text-muted-foreground">
      {ticks.map((tick, index) => (
        <span key={tick} style={{ left: share(tick, max) }}
          className={`absolute ${index === 0 ? "" : index === ticks.length - 1 && tick === max ? "-translate-x-full" : "-translate-x-1/2"}`}>
          {tick}
        </span>
      ))}
    </div>
  );
}
