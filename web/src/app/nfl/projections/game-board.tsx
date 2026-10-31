import { formatKickoff, formatNumber, formatSigned, type RollingGame } from "@/lib/nfl-dfs/rolling-projection-view";

function TeamRow({ team, implied, max, selected, onSelect }: {
  team: string; implied: number | null; max: number; selected: boolean; onSelect: (team: string) => void;
}) {
  return (
    <button type="button" onClick={() => onSelect(team)} aria-pressed={selected}
      className={`grid w-full grid-cols-[2.5rem_1fr_2.25rem] items-center gap-1.5 rounded-md px-1 py-0.5 text-left text-sm transition-colors hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring ${selected ? "bg-muted ring-1 ring-foreground/30" : ""}`}>
      <span className="font-semibold">{team}</span>
      <span className="h-2 rounded-full bg-muted">
        {implied != null && <span className="block h-2 rounded-full bg-foreground/70" style={{ width: `${Math.min(100, (implied / max) * 100)}%` }} />}
      </span>
      <span className="text-right tabular-nums">{formatNumber(implied)}</span>
    </button>
  );
}

/** Each team's implied points from the latest pregame line; click a team to filter the players below. */
export default function GameBoard({ games, selectedTeam, onSelectTeam }: {
  games: readonly RollingGame[]; selectedTeam: string | null; onSelectTeam: (team: string) => void;
}) {
  const max = Math.max(30, ...games.flatMap(game => [game.homeImplied ?? 0, game.awayImplied ?? 0]));
  const priced = games.filter(game => game.total != null).length;
  return (
    <section aria-labelledby="game-board-title" className="rounded-xl border bg-card p-4 shadow-xs">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 id="game-board-title" className="text-base font-semibold">Game environment</h2>
        <p className="text-xs text-muted-foreground">
          Implied team points from the pregame spread and total{priced < games.length ? ` · ${games.length - priced} without a line yet` : ""}
        </p>
      </div>
      <div className="-mx-1 mt-3 flex gap-2 overflow-x-auto px-1 pb-1 sm:mx-0 sm:grid sm:grid-cols-3 sm:overflow-visible sm:px-0 sm:pb-0 lg:grid-cols-5 xl:grid-cols-7">
        {games.map(game => (
          <div key={game.gameId} className="w-44 shrink-0 rounded-lg border p-1.5 sm:w-auto"
            title={game.homeSpread == null ? undefined : `${game.home} ${formatSigned(game.homeSpread)}`}>
            <div className="mb-0.5 flex justify-between gap-2 px-1 text-[11px] text-muted-foreground">
              <span className="truncate">{formatKickoff(game.kickoff)}</span>
              <span className="shrink-0 tabular-nums">{game.total == null ? "No line" : `O/U ${formatNumber(game.total)}`}</span>
            </div>
            <TeamRow team={game.away} implied={game.awayImplied} max={max} selected={selectedTeam === game.away} onSelect={onSelectTeam} />
            <TeamRow team={game.home} implied={game.homeImplied} max={max} selected={selectedTeam === game.home} onSelect={onSelectTeam} />
          </div>
        ))}
      </div>
    </section>
  );
}
