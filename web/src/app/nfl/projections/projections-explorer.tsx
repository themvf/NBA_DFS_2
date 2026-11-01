"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  environmentByTeam, filterPlayers, filterSearch, hiddenCounts, positionCounts, POSITION_FILTERS, rangeMax,
  SORT_KEYS, SORT_LABELS, sortPlayers, type ExplorerState, type PositionFilter, type RollingProjectionBoard, type SortKey,
} from "@/lib/nfl-dfs/rolling-projection-view";
import GameBoard from "./game-board";
import ProjectionCard from "./projection-card";
import ProjectionTable from "./projection-table";
import RangeChart from "./range-chart";
import UpsideScatter from "./upside-scatter";
import { positionStyle } from "./range-bar";

const PAGE_SIZE = 36;

function PositionTabs({ value, counts, onChange }: {
  value: PositionFilter; counts: Record<PositionFilter, number>; onChange: (value: PositionFilter) => void;
}) {
  return (
    <div role="tablist" aria-label="Position" className="flex flex-wrap gap-1">
      {POSITION_FILTERS.map(filter => {
        const active = filter === value;
        const single = filter !== "ALL" && filter !== "FLEX";
        return (
          <button key={filter} type="button" role="tab" aria-selected={active} onClick={() => onChange(filter)}
            style={single ? positionStyle(filter) : undefined}
            className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-sm transition-colors focus-visible:outline-2 focus-visible:outline-ring ${active ? "border-foreground bg-foreground text-background" : "hover:bg-muted"}`}>
            {single && <span aria-hidden className="size-2 rounded-full bg-[var(--pos)] dark:bg-[var(--pos-dark)]" />}
            <span className="font-medium">{filter}</span>
            <span className={`tabular-nums text-xs ${active ? "text-background/70" : "text-muted-foreground"}`}>{counts[filter]}</span>
          </button>
        );
      })}
    </div>
  );
}

function Toggle({ checked, onChange, children }: { checked: boolean; onChange: (checked: boolean) => void; children: React.ReactNode }) {
  return (
    <label className="inline-flex cursor-pointer items-center gap-2 text-sm text-muted-foreground">
      <input type="checkbox" checked={checked} onChange={event => onChange(event.target.checked)} className="size-4 accent-foreground" />
      {children}
    </label>
  );
}

export default function ProjectionsExplorer({ board, initial, week }: {
  board: RollingProjectionBoard; initial: ExplorerState; week: string;
}) {
  const router = useRouter();
  const [state, setState] = useState(initial);
  const [visible, setVisible] = useState(PAGE_SIZE);
  const update = (patch: Partial<ExplorerState>) => { setState(current => ({ ...current, ...patch })); setVisible(PAGE_SIZE); };

  useEffect(() => {
    window.history.replaceState(null, "", `${window.location.pathname}${filterSearch(state, week)}`);
  }, [state, week]);

  const teams = useMemo(() => board.games.flatMap(game => [game.away, game.home]).sort(), [board.games]);
  const environments = useMemo(() => environmentByTeam(board.games), [board.games]);
  const counts = useMemo(() => positionCounts(board.players, state), [board.players, state]);
  const hidden = useMemo(() => hiddenCounts(board.players, state), [board.players, state]);
  const players = useMemo(() => sortPlayers(filterPlayers(board.players, state), state.sort), [board.players, state]);
  const max = useMemo(() => rangeMax(players), [players]);
  const shown = state.view === "cards" ? players.slice(0, visible) : players;

  return (
    <div className="space-y-5">
      <GameBoard games={board.games} selectedTeam={state.team}
        onSelectTeam={team => update({ team: state.team === team ? null : team })} />

      <section aria-label="Filters" className="z-10 -mx-1 md:sticky md:top-[57px] space-y-3 rounded-xl border bg-card/95 p-3 shadow-xs backdrop-blur">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <PositionTabs value={state.position} counts={counts} onChange={position => update({ position })} />
          <div role="group" aria-label="View" className="inline-flex rounded-lg border p-0.5 text-sm">
            {(["cards", "table"] as const).map(view => (
              <button key={view} type="button" aria-pressed={state.view === view} onClick={() => update({ view })}
                className={`rounded-md px-3 py-1 capitalize focus-visible:outline-2 focus-visible:outline-ring ${state.view === view ? "bg-foreground text-background" : "text-muted-foreground hover:text-foreground"}`}>
                {view}
              </button>
            ))}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <input type="search" value={state.query} onChange={event => update({ query: event.target.value })}
            placeholder="Search player" aria-label="Search player"
            className="h-9 w-48 rounded-lg border bg-background px-3 text-sm placeholder:text-muted-foreground focus-visible:outline-2 focus-visible:outline-ring" />
          <label className="flex items-center gap-2 text-sm text-muted-foreground">Team
            <select value={state.team ?? ""} onChange={event => update({ team: event.target.value || null })}
              className="h-9 rounded-lg border bg-background px-2 text-sm text-foreground">
              <option value="">All teams</option>
              {teams.map(team => <option key={team} value={team}>{team}</option>)}
            </select>
          </label>
          <label className="flex items-center gap-2 text-sm text-muted-foreground">Sort
            <select value={state.sort} onChange={event => update({ sort: event.target.value as SortKey })}
              className="h-9 rounded-lg border bg-background px-2 text-sm text-foreground">
              {SORT_KEYS.map(key => <option key={key} value={key}>{SORT_LABELS[key]}</option>)}
            </select>
          </label>
          <Toggle checked={state.includeEstimates} onChange={includeEstimates => update({ includeEstimates })}>
            Include {hidden.estimates} position estimates
          </Toggle>
          <Toggle checked={state.includeOut} onChange={includeOut => update({ includeOut })}>
            Include {hidden.out} ruled out
          </Toggle>
          <div className="ml-auto flex items-center gap-3 text-xs text-muted-foreground">
            <label className="flex items-center gap-2">Week
              <select value={week} onChange={event => router.push(`/nfl/projections${filterSearch(state, event.target.value)}`)}
                className="h-8 rounded-lg border bg-background px-2 text-sm text-foreground">
                {board.options.map(run => <option key={run.runId} value={`${run.season}-${run.week}`}>{run.season} · Week {run.week}</option>)}
              </select>
            </label>
            <span aria-live="polite" className="tabular-nums">{players.length} players</span>
          </div>
        </div>
      </section>

      {players.length > 0 && (
        <div className="grid items-start gap-3 lg:grid-cols-2">
          <RangeChart players={players} />
          <UpsideScatter players={players} onSelect={player => update({ query: player.name })} />
        </div>
      )}

      {players.length === 0 ? (
        <div className="rounded-xl border border-dashed p-8 text-center text-sm text-muted-foreground">
          No players match these filters.{" "}
          <button type="button" className="font-medium text-foreground underline underline-offset-4"
            onClick={() => update({ position: "ALL", team: null, query: "" })}>Clear filters</button>
        </div>
      ) : state.view === "table" ? (
        <ProjectionTable players={shown} environments={environments} max={max} sort={state.sort}
          onSort={sort => update({ sort })} />
      ) : (
        <>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {shown.map((player, index) => (
              <ProjectionCard key={player.id} player={player} environment={environments.get(player.team)} max={max} rank={index + 1} />
            ))}
          </div>
          {visible < players.length && (
            <div className="flex justify-center">
              <button type="button" onClick={() => setVisible(count => count + PAGE_SIZE)}
                className="rounded-lg border bg-card px-4 py-2 text-sm font-medium hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring">
                Show {Math.min(PAGE_SIZE, players.length - visible)} more of {players.length - visible}
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
