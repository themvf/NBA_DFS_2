/**
 * Where a slate is in its week, and what the workspace should say about it.
 *
 * Every slate goes through the same four moments -- get the salary file in,
 * build, review and export, then learn from the result -- and the user wants
 * something different at each. The page used to be organized by kind of tool
 * instead, with no idea which moment it was in: on the morning after the
 * Thursday ATL@GB game it still led with "a newer projection run is
 * available", and the results upload sat 38 panels deep in a research tab.
 *
 * This module is the page's sense of time. It is pure so the rules are
 * testable: `test:nfl-workspace-stage`.
 */

export type WorkspaceStage = "slate" | "build" | "review" | "results";

export const WORKSPACE_STAGES: readonly WorkspaceStage[] = ["slate", "build", "review", "results"];

export const STAGE_LABELS: Record<WorkspaceStage, string> = {
  slate: "Slate",
  build: "Build",
  review: "Review and export",
  results: "Results",
};

export interface StageInputs {
  hasSlate: boolean;
  lineupCount: number;
  /** ISO time of the slate's first kickoff, when known. */
  firstKickoff: string | null;
  now: number;
}

/** True once the slate's first game has kicked off: lineups are locked. */
export function isLocked(firstKickoff: string | null, now: number): boolean {
  if (!firstKickoff) return false;
  const start = Date.parse(firstKickoff);
  return Number.isFinite(start) && now >= start;
}

/**
 * The step the workspace should open on. Before a slate exists, the Slate
 * step; once its games have started nothing can be built or entered, so
 * Results; otherwise Review when lineups exist, Build when they don't.
 */
export function recommendedStage(input: StageInputs): WorkspaceStage {
  if (!input.hasSlate) return "slate";
  if (isLocked(input.firstKickoff, input.now)) return "results";
  return input.lineupCount > 0 ? "review" : "build";
}

export type StatusTone = "danger" | "warning" | "info";

export interface StatusItem {
  id: string;
  tone: StatusTone;
  text: string;
  /** Only meaningful before lock -- dropped once the games have started. */
  preLockOnly?: boolean;
  action?: { label: string; target: WorkspaceStage | "refresh-projections" };
}

const TONE_RANK: Record<StatusTone, number> = { danger: 0, warning: 1, info: 2 };

/**
 * Collapse the page's notices into one ordered list: most urgent first, stable
 * within a tone, and without anything that stopped mattering at kickoff (a
 * newer projection run, a stale live status). The page shows the first and
 * folds the rest behind "N more", instead of stacking every notice as if each
 * were the most important.
 */
export function prioritizeStatus(items: readonly StatusItem[], locked: boolean): StatusItem[] {
  return items
    .map((item, index) => ({ item, index }))
    .filter(({ item }) => !(locked && item.preLockOnly))
    .sort((a, b) => TONE_RANK[a.item.tone] - TONE_RANK[b.item.tone] || a.index - b.index)
    .map(({ item }) => item);
}

/**
 * First kickoff read from DraftKings' Game Info column, as a fallback when the
 * schedule has not been joined: "ATL@GB 09/24/2026 08:15PM ET". "ET" is
 * America/New_York, so the offset is -4 in daylight time and -5 otherwise;
 * it is resolved for that date rather than assumed.
 */
export function parseDkGameInfoKickoff(gameInfo: string | null | undefined): string | null {
  const m = String(gameInfo ?? "").match(/(\d{2})\/(\d{2})\/(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET/i);
  if (!m) return null;
  const [, mm, dd, yyyy, hh, mi, ampm] = m;
  let hour = Number(hh) % 12;
  if (ampm.toUpperCase() === "PM") hour += 12;
  // Treat the wall-clock time as UTC, then shift by New York's offset that day.
  const asUtc = Date.UTC(Number(yyyy), Number(mm) - 1, Number(dd), hour, Number(mi));
  const offsetMinutes = newYorkOffsetMinutes(new Date(asUtc));
  return new Date(asUtc - offsetMinutes * 60_000).toISOString();
}

function newYorkOffsetMinutes(at: Date): number {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", hourCycle: "h23",
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
  }).formatToParts(at);
  const get = (type: string) => Number(parts.find((p) => p.type === type)?.value);
  const wall = Date.UTC(get("year"), get("month") - 1, get("day"), get("hour") % 24, get("minute"));
  return Math.round((wall - at.getTime()) / 60_000);
}
