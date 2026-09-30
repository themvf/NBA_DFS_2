/**
 * "Today" for a US sports slate is the Eastern-time calendar date, never the
 * UTC one. `new Date().toISOString().slice(0, 10)` rolls over at 8pm ET (7pm
 * during standard time), so a page that used it as its default date showed
 * TOMORROW's slate for the whole evening window in which tonight's games are
 * actually played. Every page default and every "today" comparison should
 * go through here instead.
 */
export function easternDateString(at: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(at);
  const value = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${value.year}-${value.month}-${value.day}`;
}

/** Eastern-time calendar year, for sports whose season sits inside one year (MLB). */
export function easternYear(at: Date = new Date()): number {
  return Number(easternDateString(at).slice(0, 4));
}
