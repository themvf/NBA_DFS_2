// Fixed-time snapshot columns on the MLB "Today's movement board"
// (web/src/app/vegas/mlb-vegas-client.tsx). Shows the vig-free home
// probability at specific America/New_York wall-clock times throughout the
// day, matched to the nearest real capture within ±20 minutes.
//
// This logic is deliberately timezone-independent of wherever it runs.
// mlb-vegas-client.tsx is a Client Component ("use client"), so it executes
// in the VIEWER's browser - `new Date(someAmbiguousString)` there parses
// using the viewer's own local timezone, not a fixed one. An earlier version
// of this file assumed the ambient timezone was UTC (so a bare date-time
// string would parse as UTC) and then manually added a hardcoded +4h EDT
// offset on top. For any Eastern-timezone viewer that double-applied the
// offset, silently shifting every checkpoint 4 hours late (confirmed
// 2026-08-06: the "10am" column was actually matching captures near 2pm).
// The fix below never parses an ambiguous local string - it uses
// Intl.DateTimeFormat.formatToParts exclusively, so the result is identical
// no matter what timezone the code happens to execute in.

export const MLB_SNAPSHOT_TIMES_ET = [
  // 10am, not 9am: the capture cron (.github/workflows/capture_odds_history.yml)
  // deliberately starts at 10 AM ET (covers the earliest MLB first pitch,
  // ~11:35 AM ET, with margin) - a 9am checkpoint could never populate.
  { label: "10am", hour: 10, minute: 0 },
  { label: "1:10p", hour: 13, minute: 10 },
  { label: "6:20p", hour: 18, minute: 20 },
  { label: "6:50p", hour: 18, minute: 50 },
  { label: "7:30p", hour: 19, minute: 30 },
  { label: "9:20p", hour: 21, minute: 20 },
] as const;

export const MLB_SNAPSHOT_MAX_DISTANCE_MS = 20 * 60_000; // ±20 minutes

/**
 * How far a timezone's wall-clock reading is from the true UTC instant, at
 * approximately `instantMs`. Uses formatToParts only - never re-parses a
 * locale-formatted string with `new Date()`, which is what reintroduces
 * ambient-timezone dependence.
 */
export function zoneOffsetMsAt(instantMs: number, timeZone: string): number {
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hourCycle: "h23",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
  const parts = Object.fromEntries(dtf.formatToParts(new Date(instantMs)).map((p) => [p.type, p.value]));
  const asIfUtc = Date.UTC(
    Number(parts.year), Number(parts.month) - 1, Number(parts.day),
    Number(parts.hour), Number(parts.minute), Number(parts.second),
  );
  return asIfUtc - instantMs;
}

/** Converts a wall-clock time in `timeZone` (e.g. "1:10 PM America/New_York") to its true UTC epoch ms. Correct across DST. */
export function zonedWallClockToUtcMs(dateStr: string, hour: number, minute: number, timeZone: string): number {
  const [y, m, d] = dateStr.split("-").map(Number);
  const utcGuess = Date.UTC(y, m - 1, d, hour, minute, 0);
  const offset = zoneOffsetMsAt(utcGuess, timeZone);
  return utcGuess - offset;
}

/** Finds the trail point closest to the given ET wall-clock target, within ±20 minutes; null if none. */
export function getMlbSnapshotAtTime(
  trail: Array<{ capturedAt: string; homeProb: number }>,
  gameDate: string,
  targetHour: number,
  targetMinute: number,
): number | null {
  if (!trail || trail.length === 0) return null;
  const targetUtcMs = zonedWallClockToUtcMs(gameDate, targetHour, targetMinute, "America/New_York");

  let closest: { prob: number; distance: number } | null = null;
  for (const point of trail) {
    const capMs = Date.parse(point.capturedAt);
    if (!Number.isFinite(capMs)) continue;
    const dist = Math.abs(capMs - targetUtcMs);
    if (dist <= MLB_SNAPSHOT_MAX_DISTANCE_MS && (closest == null || dist < closest.distance)) {
      closest = { prob: point.homeProb, distance: dist };
    }
  }
  return closest?.prob ?? null;
}
