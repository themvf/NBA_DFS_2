/**
 * When a ghost freeze runs (nfl-ghost-factorial-v1, amended 2026-10-25). Pure,
 * so the test script can import it without a database connection.
 *
 * The freeze waits for the pregame prop capture (dispatched 60-75 minutes
 * before each kickoff group, so after inactives at ~90): lines captured within
 * FRESH_LINES_MINUTES of the first kickoff count as fresh. Until 2026-10-25 the
 * freeze ran at the first tick in its window (~T-105), and all four freezes so
 * far used the day-before lines and pre-inactive availability.
 */
export const FRESH_LINES_MINUTES = 80;
/** Past this many minutes before kickoff the freeze stops waiting and records stale lines. */
export const FRESH_LINES_DEADLINE_MINUTES = 40;

export type FreezeTiming = "wait" | "freeze" | "freeze_stale";

/** Whether a slate in its freeze window should freeze now, keep waiting for fresh lines, or freeze without them. */
export function freezeTiming(minutesToKickoff: number, hasFreshLines: boolean): FreezeTiming {
  if (hasFreshLines) return "freeze";
  return minutesToKickoff > FRESH_LINES_DEADLINE_MINUTES ? "wait" : "freeze_stale";
}
