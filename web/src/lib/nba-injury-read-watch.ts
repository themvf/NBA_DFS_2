// Did an official NBA injury report read that was due arrive? (nba-injury-read-watch-v1)
// Pure. The reads are the dispatcher's own (`nbaInjuryRefreshDue`: 6:37 and 6:52 PM Eastern,
// every tick in the 50 minutes before the night's first tip, one tick before each later tip),
// so the page expects exactly what the dispatcher fires. A failed read leaves the older report
// on the contest builder; this is what lets the page say so.

import { NO_CONTEXT, nbaInjuryRefreshDue } from "./cron-dispatch";

/** The dispatcher's ticks, minutes past each hour. */
const TICK_MINUTES = [7, 22, 37, 52] as const;
/** A dispatched read starts, downloads and stores in a minute or two; give it this long. */
export const INJURY_READ_GRACE_MIN = 10;
/** A read from the tick before the newest due one still counts: one missed tick is not an outage. */
export const INJURY_READ_SLACK_MIN = 20;
/** How far back a due read is looked for. */
const LOOKBACK_MIN = 180;
/** The dispatcher sees tips from the last eight hours and the next three (route.ts `nbaTips`). */
const TIPS_BACK_MS = 8 * 3_600_000, TIPS_AHEAD_MS = 3 * 3_600_000;

export type MissedInjuryRead = {
  /** The newest tick at which a read was due and had time to finish. */
  dueAt: string;
  /** The newest report read the page has, or null when none was read for the date. */
  lastReadAt: string | null;
};

/** The newest dispatcher tick at or before `at`, to the second. */
function tickAtOrBefore(at: Date): Date {
  const t = new Date(at);
  t.setUTCSeconds(0, 0);
  for (let i = 0; i < 16; i++) {
    if ((TICK_MINUTES as readonly number[]).includes(t.getUTCMinutes())) return t;
    t.setUTCMinutes(t.getUTCMinutes() - 1);
  }
  return t;
}

/**
 * Null when the newest due read arrived (or none was due in the last three hours). Otherwise
 * the due tick and the newest read on hand. `tips` are the date's tip times from the stored
 * lines; `lastReadAt` is when the report on the page was fetched.
 */
export function missedInjuryRead(now: Date, tips: Date[], lastReadAt: Date | null): MissedInjuryRead | null {
  const newest = now.getTime() - INJURY_READ_GRACE_MIN * 60_000, oldest = now.getTime() - LOOKBACK_MIN * 60_000;
  for (let t = tickAtOrBefore(new Date(newest)); t.getTime() >= oldest; t = tickAtOrBefore(new Date(t.getTime() - 60_000))) {
    const seen = tips.filter((tip) => tip.getTime() > t.getTime() - TIPS_BACK_MS && tip.getTime() <= t.getTime() + TIPS_AHEAD_MS);
    if (!nbaInjuryRefreshDue(t, { ...NO_CONTEXT, nbaTips: seen })) continue;
    if (lastReadAt && lastReadAt.getTime() >= t.getTime() - INJURY_READ_SLACK_MIN * 60_000) return null;
    return { dueAt: t.toISOString(), lastReadAt: lastReadAt ? lastReadAt.toISOString() : null };
  }
  return null;
}

/** The league's page listing every report of the season the date falls in (season starts in July). */
export function nbaInjuryReportIndexUrl(date: string): string {
  const year = Number(date.slice(0, 4)), month = Number(date.slice(5, 7));
  const start = month >= 7 ? year : year - 1;
  return `https://official.nba.com/nba-injury-report-${start}-${String(start + 1).slice(-2)}-season/`;
}
