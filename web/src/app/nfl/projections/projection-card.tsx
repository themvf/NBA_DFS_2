import {
  boomThreshold, formatKickoff, formatNumber, formatPercent, formatSalary, formatSigned, formatValue,
  isPositionEstimate, isOut, pointsPerThousand, relevantImplied,
  statLine, type RollingPlayer, type TeamEnvironment,
} from "@/lib/nfl-dfs/rolling-projection-view";
import RangeBar, { positionStyle } from "./range-bar";

export function PositionChip({ position }: { position: string }) {
  return (
    <span style={positionStyle(position)}
      className="inline-flex min-w-9 justify-center rounded-md bg-[var(--pos)]/15 px-1.5 py-0.5 text-[11px] font-semibold text-[var(--pos)] dark:bg-[var(--pos-dark)]/20 dark:text-[var(--pos-dark)]">
      {position}
    </span>
  );
}

export function StatusBadges({ player }: { player: RollingPlayer }) {
  return <>
    {isOut(player) && <span className="rounded-md bg-destructive/10 px-1.5 py-0.5 text-[11px] font-semibold text-destructive">Out</span>}
    {isPositionEstimate(player) && (
      <span title="Too few NFL games for a projection of his own. This is the position average, which has run several points high for these players."
        className="rounded-md border border-dashed border-muted-foreground/50 px-1.5 py-0.5 text-[11px] text-muted-foreground">
        Position estimate
      </span>
    )}
  </>;
}

function Fact({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="min-w-0" title={hint}>
      <dt className="truncate text-[11px] text-muted-foreground">{label}</dt>
      <dd className="font-semibold tabular-nums">{value}</dd>
    </div>
  );
}

function QbMatchupDetail({ player }: { player: RollingPlayer }) {
  const context = player.teamContext;
  if (!context) return null;
  return (
    <details className="group mt-3 rounded-lg bg-muted/60 px-3 py-2 text-xs">
      <summary className="cursor-pointer select-none font-medium text-foreground/80 marker:text-muted-foreground">Passing game context</summary>
      <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-2">
        <Fact label="Close-game dropbacks" value={formatPercent(context.closeDropbackRate)} hint={`${context.closePlays} plays`} />
        <Fact label="Opp. pass EPA effect" value={formatSigned(context.opponentAdjustedEpa, 3)}
          hint="Positive: opponents passed more efficiently against this defense than in their other games." />
        <Fact label="Dropbacks trailing 8+" value={formatPercent(context.trailingDropbackRate)} hint={`${context.trailingPlays} plays`} />
        <Fact label="Dropbacks leading 8+" value={formatPercent(context.leadingDropbackRate)} hint={`${context.leadingPlays} plays`} />
        <Fact label="Opp. TD drives allowed"
          value={context.opponentCompetitiveDrives ? `${context.opponentTouchdownDrives}/${context.opponentCompetitiveDrives}` : "—"}
          hint="Touchdowns per competitive drive; kneels and clock-expired drives excluded." />
      </dl>
      <p className="mt-2 text-muted-foreground">
        {context.status === "ready" ? "Descriptive only; it does not change the projection." : context.reason ?? "Matchup sample incomplete."}
      </p>
    </details>
  );
}

export default function ProjectionCard({ player, environment, max, rank }: {
  player: RollingPlayer; environment: TeamEnvironment | undefined; max: number; rank: number;
}) {
  const estimate = isPositionEstimate(player);
  const implied = relevantImplied(player.position, environment);
  const threshold = boomThreshold(player.position);
  const stats = statLine(player);
  return (
    <article className={`flex flex-col rounded-xl border bg-card p-4 text-card-foreground shadow-xs ${estimate ? "border-dashed" : ""} ${isOut(player) ? "opacity-60" : ""}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="text-xs tabular-nums text-muted-foreground">{rank}</span>
            <PositionChip position={player.position} />
            <StatusBadges player={player} />
          </div>
          <h3 className="mt-1 truncate text-base font-semibold" title={player.name}>{player.name}</h3>
          <p className="text-xs text-muted-foreground">{player.team} vs {player.opponent} · {formatKickoff(player.kickoff)}</p>
        </div>
        <div className="shrink-0 text-right">
          <div className="text-2xl font-bold tabular-nums leading-none">{formatNumber(player.projected)}</div>
          <div className="mt-1 text-[11px] text-muted-foreground">DK pts</div>
          <div className="mt-1 text-[11px] tabular-nums" title="DraftKings Classic salary and projected points per $1,000">
            {player.salary == null
              ? <span className="text-muted-foreground">No Classic salary</span>
              : <><span className="font-medium">{formatSalary(player.salary)}</span> <span className="text-muted-foreground">· {formatValue(pointsPerThousand(player))}</span></>}
          </div>
        </div>
      </div>
      <div className="mt-4">
        <RangeBar player={player} max={max} estimate={estimate} />
        <div className="mt-1.5 flex justify-between text-[11px] tabular-nums text-muted-foreground">
          <span>Floor {formatNumber(player.floor)}</span>
          <span>Median {formatNumber(player.median)}</span>
          <span>Ceiling {formatNumber(player.ceiling)}</span>
        </div>
      </div>
      {stats.length > 0 && (
        <ul className="mt-3 flex flex-wrap gap-1.5 text-xs">
          {stats.map(item => (
            <li key={item.label} className="rounded-md bg-muted px-2 py-0.5 tabular-nums">
              <span className="font-semibold">{item.value.toFixed(item.digits)}</span> <span className="text-muted-foreground">{item.label}</span>
            </li>
          ))}
        </ul>
      )}
      <div className="min-h-3 flex-1" />
      <dl className="grid grid-cols-4 gap-2 border-t pt-3 text-sm">
        <Fact label={implied.label} value={formatNumber(implied.value)} />
        <Fact label={threshold == null ? "Boom" : `Boom ${threshold}+`} value={formatPercent(player.boomRate)}
          hint={threshold == null ? undefined : `Chance of ${threshold}+ DK points`} />
        <Fact label="Spread" value={formatSigned(environment?.spread ?? null)} />
        <Fact label="Games" value={String(player.historyGames)} hint="Games of history behind the projection" />
      </dl>
      {player.position === "QB" && <QbMatchupDetail player={player} />}
    </article>
  );
}
