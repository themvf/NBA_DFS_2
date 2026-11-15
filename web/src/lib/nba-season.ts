/**
 * NBA season strings ("2026-27") from dates. The season turns over in August,
 * so 2026-10-24 is 2026-27 and 2026-04-10 is 2025-26.
 *
 * Mirrors `nba_season_for` in config.py. A hard-coded season string fed the
 * 2026-27 slates last season's final ten games (April tank-rotation minutes),
 * which projected deep-bench players at 20-30 DK points.
 */

/** Season of a YYYY-MM-DD date (or Date, read as its UTC calendar day). */
export function nbaSeasonFor(day: string | Date): string {
  const iso = typeof day === "string" ? day : day.toISOString();
  const year = Number(iso.slice(0, 4));
  const month = Number(iso.slice(5, 7));
  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    throw new Error(`nbaSeasonFor: not a date: ${String(day)}`);
  }
  const start = month >= 8 ? year : year - 1;
  return `${start}-${String((start + 1) % 100).padStart(2, "0")}`;
}

/** Today's season, by the Eastern calendar date. */
export function currentNbaSeason(now: Date = new Date()): string {
  const eastern = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(now);
  return nbaSeasonFor(eastern);
}
