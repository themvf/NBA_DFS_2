import "server-only";

import current from "@/data/game-leaders.json";
import index from "@/data/game-leaders-weeks/index.json";

export type GameLeadersPage = typeof current;
export type GameLeadersWeek = { key: string; season: number; week: number; decision_at: string; games: number };

/** Every archived week, newest first. The current page is always listed, even before its first archive. */
export function gameLeadersWeeks(): GameLeadersWeek[] {
  const weeks = index.weeks as GameLeadersWeek[];
  const key = weekKey(current.season, current.week);
  return weeks.some((w) => w.key === key) ? weeks
    : [{ key, season: current.season, week: current.week, decision_at: current.decision_at, games: current.games.length }, ...weeks];
}

export const weekKey = (season: number, week: number) => `${season}-w${String(week).padStart(2, "0")}`;

/**
 * The saved Game Leaders page for a week (`2026-w07`), or the current one when the key is
 * missing or unknown. A past week is the last version published before its games, read
 * from web/src/data/game-leaders-weeks/.
 */
export async function loadGameLeadersPage(key?: string): Promise<{ page: GameLeadersPage; key: string }> {
  const currentKey = weekKey(current.season, current.week);
  const known = gameLeadersWeeks().find((w) => w.key === key);
  if (!known || known.key === currentKey) return { page: current, key: currentKey };
  const page = (await import(`@/data/game-leaders-weeks/${known.key}.json`)).default as GameLeadersPage;
  return { page, key: known.key };
}
