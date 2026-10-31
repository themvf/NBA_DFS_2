import {
  formatNumber, formatPercent, isPositionEstimate, isOut, relevantImplied, statLine,
  type RollingPlayer, type SortKey, type TeamEnvironment,
} from "@/lib/nfl-dfs/rolling-projection-view";
import { PositionChip, StatusBadges } from "./projection-card";
import RangeBar, { RangeScale } from "./range-bar";

function SortHeader({ label, sortKey, sort, onSort, align = "right" }: {
  label: string; sortKey: SortKey; sort: SortKey; onSort: (key: SortKey) => void; align?: "left" | "right";
}) {
  const active = sort === sortKey;
  return (
    <th scope="col" aria-sort={active ? (sortKey === "name" ? "ascending" : "descending") : undefined}
      className={`p-2 font-medium ${align === "right" ? "text-right" : "text-left"}`}>
      <button type="button" onClick={() => onSort(sortKey)}
        className={`rounded px-1 hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring ${active ? "text-foreground underline underline-offset-4" : ""}`}>
        {label}
      </button>
    </th>
  );
}

export default function ProjectionTable({ players, environments, max, sort, onSort }: {
  players: readonly RollingPlayer[]; environments: Map<string, TeamEnvironment>; max: number;
  sort: SortKey; onSort: (key: SortKey) => void;
}) {
  return (
    <div className="overflow-x-auto rounded-xl border bg-card shadow-xs">
      <table className="w-full min-w-[960px] text-left text-sm">
        <thead>
          <tr className="border-b text-xs text-muted-foreground">
            <th scope="col" className="w-10 p-2 text-right font-medium">#</th>
            <SortHeader label="Player" sortKey="name" sort={sort} onSort={onSort} align="left" />
            <th scope="col" className="p-2 font-medium">Matchup</th>
            <th scope="col" className="w-48 p-2 font-medium">Range<RangeScale max={max} /></th>
            <SortHeader label="Proj" sortKey="projected" sort={sort} onSort={onSort} />
            <SortHeader label="Floor" sortKey="floor" sort={sort} onSort={onSort} />
            <th scope="col" className="p-2 text-right font-medium">Median</th>
            <SortHeader label="Ceiling" sortKey="ceiling" sort={sort} onSort={onSort} />
            <SortHeader label="Boom" sortKey="boom" sort={sort} onSort={onSort} />
            <th scope="col" className="p-2 text-right font-medium" title="Team implied points; for a defense, the opponent's">Implied</th>
            <th scope="col" className="p-2 font-medium">Stat line</th>
            <th scope="col" className="p-2 text-right font-medium">Games</th>
          </tr>
        </thead>
        <tbody>
          {players.map((player, index) => (
            <tr key={player.id} className={`border-b last:border-0 hover:bg-muted/50 ${isOut(player) ? "opacity-60" : ""}`}>
              <td className="p-2 text-right tabular-nums text-muted-foreground">{index + 1}</td>
              <td className="p-2">
                <div className="flex items-center gap-2">
                  <PositionChip position={player.position} />
                  <span className="font-medium">{player.name}</span>
                  <StatusBadges player={player} />
                </div>
              </td>
              <td className="whitespace-nowrap p-2 text-muted-foreground">{player.team} vs {player.opponent}</td>
              <td className="p-2"><RangeBar player={player} max={max} compact estimate={isPositionEstimate(player)} /></td>
              <td className="p-2 text-right font-semibold tabular-nums">{formatNumber(player.projected)}</td>
              <td className="p-2 text-right tabular-nums">{formatNumber(player.floor)}</td>
              <td className="p-2 text-right tabular-nums">{formatNumber(player.median)}</td>
              <td className="p-2 text-right tabular-nums">{formatNumber(player.ceiling)}</td>
              <td className="p-2 text-right tabular-nums">{formatPercent(player.boomRate)}</td>
              <td className="p-2 text-right tabular-nums">{formatNumber(relevantImplied(player.position, environments.get(player.team)).value)}</td>
              <td className="whitespace-nowrap p-2 text-xs text-muted-foreground tabular-nums">
                {statLine(player).map(item => `${item.value.toFixed(item.digits)} ${item.label}`).join(" · ") || "—"}
              </td>
              <td className="p-2 text-right tabular-nums">{player.historyGames}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
