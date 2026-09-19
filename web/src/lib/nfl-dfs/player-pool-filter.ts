/**
 * Position filtering for the DFS Lab player pool.
 *
 * Pure: no React, no database — so the FLEX rule can be tested directly
 * rather than inferred from a dropdown.
 *
 * FLEX is *eligibility*, not a position a player has. It draws its member
 * positions from `NFL_FLEX_POSITIONS` in the salary parser rather than
 * re-listing them, so the pool filter and the optimizer's own slot rule
 * (`lineups.ts`) can never drift apart.
 *
 * Showdown caveat, deliberately not papered over: on a DK Showdown slate
 * every rostered player fills a FLEX slot, including K and DST. This filter
 * keeps the Classic meaning (RB/WR/TE) in both formats, which is why the UI
 * labels the option "FLEX (RB/WR/TE)" rather than a bare "FLEX" — the label
 * states the rule so the Showdown reader is never misled about DK's slots.
 */
import { NFL_FLEX_POSITIONS } from "./dk-salary-csv";

/** Dropdown options, in display order. */
export const POOL_POSITION_FILTERS = ["ALL", "QB", "RB", "WR", "TE", "FLEX", "K", "DST"] as const;
export type PoolPositionFilter = (typeof POOL_POSITION_FILTERS)[number];

export const POOL_FILTER_LABELS: Record<PoolPositionFilter, string> = {
  ALL: "All positions",
  QB: "QB",
  RB: "RB",
  WR: "WR",
  TE: "TE",
  FLEX: `FLEX (${NFL_FLEX_POSITIONS.join("/")})`,
  K: "K",
  DST: "DST",
};

export function isPoolPositionFilter(value: string): value is PoolPositionFilter {
  return (POOL_POSITION_FILTERS as readonly string[]).includes(value);
}

/** Does this player's position survive the chosen filter? */
export function matchesPoolPosition(position: string, filter: PoolPositionFilter): boolean {
  if (filter === "ALL") return true;
  if (filter === "FLEX") return (NFL_FLEX_POSITIONS as readonly string[]).includes(position);
  return position === filter;
}

/**
 * How many players each option would show, for the dropdown's counts.
 * Counted over the *unsearched* pool so the numbers describe the slate, not
 * the current text query — a count that moved with every keystroke would be
 * read as a filter result rather than as slate composition.
 */
export function poolPositionCounts(
  players: readonly { position: string }[],
): Record<PoolPositionFilter, number> {
  const counts = Object.fromEntries(
    POOL_POSITION_FILTERS.map((f) => [f, 0]),
  ) as Record<PoolPositionFilter, number>;
  for (const player of players) {
    for (const filter of POOL_POSITION_FILTERS) {
      if (matchesPoolPosition(player.position, filter)) counts[filter] += 1;
    }
  }
  return counts;
}
