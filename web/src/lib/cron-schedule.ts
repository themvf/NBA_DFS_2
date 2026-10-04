/**
 * Minimal 5-field cron evaluation (UTC), for the /health checklist's "next
 * run" and "overdue" columns. Supports `*`, `a`, `a-b`, `a,b`, `*\/n`,
 * `a-b/n`, `a/n`; day-of-week 0-6 with 7 = Sunday. When both day-of-month and
 * day-of-week are restricted, either may match (standard cron semantics).
 *
 * Every field is validated: a value outside its range, a reversed range, a
 * missing step or a name (`MON`, `JAN`) throws instead of producing a cron
 * that silently never matches (`60 9 * * *` used to parse to an empty minute
 * set, so the job it described could never be overdue).
 */

type Field = Set<number>;

function parseField(spec: string, min: number, max: number): Field {
  const out = new Set<number>();
  if (!spec) throw new Error(`bad cron field "${spec}"`);
  for (const part of spec.split(",")) {
    const pieces = part.split("/");
    if (pieces.length > 2) throw new Error(`bad cron field "${spec}"`);
    const [range, stepText] = pieces;
    const step = pieces.length === 2 ? Number(stepText) : 1;
    let lo: number, hi: number;
    if (range === "*") { lo = min; hi = max; }
    else if (range.includes("-")) { [lo, hi] = range.split("-").map(Number); }
    else { lo = Number(range); hi = pieces.length === 2 ? max : lo; }
    // Day-of-week accepts 7 for Sunday; nothing else may leave its range.
    const top = max === 6 && min === 0 ? 7 : max;
    if (![lo, hi, step].every(Number.isInteger) || step <= 0 || lo < min || hi > top || lo > hi || range === "" || stepText === "") {
      throw new Error(`bad cron field "${spec}"`);
    }
    for (let v = lo; v <= hi; v += step) out.add(v === 7 && max === 6 ? 0 : v);
  }
  return out;
}

export interface CronSpec { minute: Field; hour: Field; dom: Field; month: Field; dow: Field; domAny: boolean; dowAny: boolean }

export function parseCron(expr: string): CronSpec {
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5) throw new Error(`cron "${expr}" must have 5 fields`);
  const [m, h, dom, mon, dow] = parts;
  return { minute: parseField(m, 0, 59), hour: parseField(h, 0, 23), dom: parseField(dom, 1, 31), month: parseField(mon, 1, 12),
    dow: parseField(dow, 0, 6), domAny: dom === "*", dowAny: dow === "*" };
}

function dayMatches(spec: CronSpec, t: Date): boolean {
  if (!spec.month.has(t.getUTCMonth() + 1)) return false;
  const domOk = spec.dom.has(t.getUTCDate()), dowOk = spec.dow.has(t.getUTCDay());
  if (spec.domAny && spec.dowAny) return true;
  if (spec.domAny) return dowOk;
  if (spec.dowAny) return domOk;
  return domOk || dowOk;
}

export function cronMatches(spec: CronSpec, t: Date): boolean {
  return spec.minute.has(t.getUTCMinutes()) && spec.hour.has(t.getUTCHours()) && dayMatches(spec, t);
}

/** Fire times of any of `crons` strictly after `after`, up to `limit`, within `horizonMinutes`. */
export function cronTimes(crons: string[], after: Date, horizonMinutes = 8 * 1440, limit = 400): Date[] {
  const specs = crons.map(parseCron);
  const out: Date[] = [];
  const start = Math.floor(after.getTime() / 60_000) + 1;
  for (let m = start; m <= start + horizonMinutes && out.length < limit; m++) {
    const t = new Date(m * 60_000);
    if (specs.some((s) => cronMatches(s, t))) out.push(t);
  }
  return out;
}

const DAY_MS = 86400_000;

/**
 * One spec's extreme fire time on the day starting at `dayStart`: with hours
 * and minutes sorted descending, the newest time <= `limit`; sorted
 * ascending, the oldest time >= `limit`. Null when the day has none.
 */
function onDay(spec: CronSpec, hours: number[], minutes: number[], dayStart: number, limit: number, newest: boolean): number | null {
  if (!dayMatches(spec, new Date(dayStart))) return null;
  for (const h of hours) for (const m of minutes) {
    const t = dayStart + h * 3600_000 + m * 60_000;
    if (newest ? t <= limit : t >= limit) return t;
  }
  return null;
}

const ordered = (f: Field, desc: boolean) => [...f].sort((a, b) => (desc ? b - a : a - b));

/**
 * The newest fire time at or before `at`, looking back up to `lookbackDays`
 * (default 400: a yearly cron is found). Steps by day, then by each spec's own
 * hours and minutes, so a monthly cron costs a few hundred checks rather than
 * a minute-by-minute scan. Null when no fire time falls in the window.
 */
export function lastCronTime(crons: string[], at: Date, lookbackDays = 400): Date | null {
  const specs = crons.map(parseCron).map((spec) => ({ spec, hours: ordered(spec.hour, true), minutes: ordered(spec.minute, true) }));
  const limit = at.getTime();
  const day0 = Math.floor(limit / DAY_MS);
  for (let d = day0; d >= day0 - lookbackDays; d--) {
    let best: number | null = null;
    for (const { spec, hours, minutes } of specs) {
      const t = onDay(spec, hours, minutes, d * DAY_MS, limit, true);
      if (t != null && (best == null || t > best)) best = t;
    }
    if (best != null) return new Date(best);
  }
  return null;
}

/** The first fire time strictly after `after`, looking ahead up to `horizonDays` (default 400). Null when none. */
export function nextCronTime(crons: string[], after: Date, horizonDays = 400): Date | null {
  const specs = crons.map(parseCron).map((spec) => ({ spec, hours: ordered(spec.hour, false), minutes: ordered(spec.minute, false) }));
  const limit = (Math.floor(after.getTime() / 60_000) + 1) * 60_000; // the next whole minute
  const day0 = Math.floor(limit / DAY_MS);
  for (let d = day0; d <= day0 + horizonDays; d++) {
    let best: number | null = null;
    for (const { spec, hours, minutes } of specs) {
      const t = onDay(spec, hours, minutes, d * DAY_MS, limit, false);
      if (t != null && (best == null || t < best)) best = t;
    }
    if (best != null) return new Date(best);
  }
  return null;
}

/** Longest gap between consecutive times (plus the gap from `from` to the first), in ms; null with no times. */
export function longestGapMs(times: Date[], from: Date): number | null {
  if (!times.length) return null;
  let gap = times[0].getTime() - from.getTime(), prev = times[0].getTime();
  for (const t of times.slice(1)) { gap = Math.max(gap, t.getTime() - prev); prev = t.getTime(); }
  return gap;
}
