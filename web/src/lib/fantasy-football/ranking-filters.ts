import type { FantasyRankingRow } from "@/db/queries-fantasy-football";

export type FantasyRankingFilters = {
  name: string;
  position: string;
  team: string;
};

export function filterFantasyRankings(
  rankings: FantasyRankingRow[],
  filters: FantasyRankingFilters,
): FantasyRankingRow[] {
  const name = filters.name.trim().toLocaleLowerCase();
  return rankings.filter((player) => (
    (!name || player.name.toLocaleLowerCase().includes(name))
    && (!filters.position || player.position === filters.position)
    && (!filters.team || player.team === filters.team)
  ));
}
