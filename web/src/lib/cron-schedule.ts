/**
 * Minimal 5-field cron evaluation (UTC), for the /health checklist's "next
 * run" and "overdue" columns. Supports `*`, `a`, `a-b`, `a,b`, `*\/n`,
 * `a-b/n`; day-of-week 0-6 with 7 = Sunday. When both day-of-month and
 * day-of-week are restricted, either may match (standard cron semantics).
 */

type Field = Set<number>;

function parseField(spec: string, min: number, max: number): Field {
  const out = new Set<number>();
  for (const part of spec.split(",")) {
    const [range, stepText] = part.split("/");
    const step = stepText ? Number(stepText) : 1;
    let lo: number, hi: number;
    if (range === "*") { lo = min; hi = max; }
    else if (range.includes("-")) { [lo, hi] = range.split("-").map(Number); }
    else { lo = Number(range); hi = stepText ? max : lo; }
    if (![lo, hi, step].every(Number.isFinite) || step <= 0) throw new Error(`bad cron field "${spec}"`);
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

export function cronMatches(spec: CronSpec, t: Date): boolean {
  if (!spec.minute.has(t.getUTCMinutes()) || !spec.hour.has(t.getUTCHours()) || !spec.month.has(t.getUTCMonth() + 1)) return false;
  const domOk = spec.dom.has(t.getUTCDate()), dowOk = spec.dow.has(t.getUTCDay());
  if (spec.domAny && spec.dowAny) return true;
  if (spec.domAny) return dowOk;
  if (spec.dowAny) return domOk;
  return domOk || dowOk;
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

/** Longest gap between consecutive times (plus the gap from `from` to the first), in ms; null with no times. */
export function longestGapMs(times: Date[], from: Date): number | null {
  if (!times.length) return null;
  let gap = times[0].getTime() - from.getTime(), prev = times[0].getTime();
  for (const t of times.slice(1)) { gap = Math.max(gap, t.getTime() - prev); prev = t.getTime(); }
  return gap;
}
