/**
 * When to rebuild the production NFL DFS projection snapshot.
 *
 * Vercel calls `/api/cron/nfl-projections` at :05 and :35 as a reliable clock;
 * this decides which of those calls dispatch `refresh_nfl_dfs_projections.yml`.
 * GitHub's own schedule for that workflow skipped its 21:35 UTC slot on
 * 2026-09-26, leaving Sunday's slate on a 1:22 PM projection. Roster evidence
 * captured later than a projection's decision time is refused, so every player
 * on the slate showed availability UNKNOWN and export blocked.
 *
 * Times in UTC:
 *   13:35 daily   the morning pass, ahead of Sunday's early games (09:35 ET)
 *   21:35 daily   Thursday night, Saturday evening news, Sunday night games
 *   16:05 Sunday  after inactives start landing, before the 1:00 PM ET lock
 *   19:05 Sunday  before the 4:05 / 4:25 PM ET games
 * A build takes about four minutes, so each pass finishes before its lock.
 */
export const NFL_PROJECTION_SLOTS_UTC: ReadonlyArray<{ days: readonly number[] | "daily"; hour: number; minute: number }> = [
  { days: "daily", hour: 13, minute: 35 },
  { days: "daily", hour: 21, minute: 35 },
  { days: [0], hour: 16, minute: 5 },
  { days: [0], hour: 19, minute: 5 },
];

/** True when `now` falls in the same half-hour as a scheduled slot. */
export function nflProjectionDispatchDue(now: Date): boolean {
  const day = now.getUTCDay();
  const hour = now.getUTCHours();
  const half = now.getUTCMinutes() < 30 ? 0 : 30;
  return NFL_PROJECTION_SLOTS_UTC.some((slot) =>
    (slot.days === "daily" || slot.days.includes(day)) && slot.hour === hour && (slot.minute < 30 ? 0 : 30) === half);
}
