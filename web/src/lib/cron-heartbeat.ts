/**
 * Heartbeats for the Vercel cron routes.
 *
 * Vercel is the clock for everything scheduled here (the GitHub dispatcher,
 * the DraftKings pool capture, closing lines, the NFL Slate Check), and its
 * logs are not visible from the app or from GitHub. A route that started
 * returning 500s, or stopped being called at all, would be silent: the jobs
 * it drives would just stop. Each route records its last run and outcome in
 * one row, and /health shows a route as failing, late or never run.
 */
import { sql } from "drizzle-orm";

// Loaded on first use so the pure status logic can be imported (and tested)
// without a database connection string.
const database = async () => (await import("@/db")).db;

export interface CronRouteSpec {
  label: string;
  everyMinutes: number;
  /** Where it runs: a Vercel cron route (default) or a GitHub job that records a heartbeat. */
  source?: "vercel" | "github";
  /** Overrides the default lateness allowance (three intervals plus five minutes). */
  lateAfterMinutes?: number;
}

/** Every cron route in vercel.json (and the GitHub jobs that heartbeat), with its schedule's longest gap in minutes. */
export const CRON_ROUTES: Record<string, CronRouteSpec> = {
  "dispatch": { label: "GitHub job dispatcher", everyMinutes: 15 },
  "nfl-slate-check": { label: "NFL Slate Check (scheduled)", everyMinutes: 30 },
  "nfl-pool-capture": { label: "NFL DraftKings pool capture", everyMinutes: 1 },
  "event-closing-lines": { label: "Closing-line capture trigger", everyMinutes: 1 },
  "health-check": { label: "Health checklist (this page)", everyMinutes: 30 },
  "daily-failure-sweep": { label: "Daily failure sweep (email)", everyMinutes: 1440, source: "github", lateAfterMinutes: 26 * 60 },
};

export interface CronHeartbeat {
  route: string;
  lastRunAt: string;
  lastOk: boolean;
  lastDetail: string | null;
  lastOkAt: string | null;
  lastErrorAt: string | null;
  lastError: string | null;
}

export type CronState = "ok" | "failing" | "late" | "never";

export interface CronStatus { route: string; label: string; state: CronState; heartbeat: CronHeartbeat | null; text: string; nextRunAt: string | null }

/** Late once the gap exceeds three schedule intervals plus five minutes (Vercel fires within the minute). */
export function lateAfterMs(spec: CronRouteSpec): number {
  return (spec.lateAfterMinutes ?? spec.everyMinutes * 3 + 5) * 60_000;
}

/** Pure: each registered route's state from its heartbeat. */
export function cronStatuses(heartbeats: CronHeartbeat[], now: number): CronStatus[] {
  const byRoute = new Map(heartbeats.map((h) => [h.route, h]));
  return Object.entries(CRON_ROUTES).map(([route, spec]) => {
    const hb = byRoute.get(route) ?? null;
    if (!hb) return { route, label: spec.label, state: "never" as const, heartbeat: null, text: "No run recorded yet.", nextRunAt: null };
    const age = now - Date.parse(hb.lastRunAt);
    const nextRunAt = new Date(Date.parse(hb.lastRunAt) + spec.everyMinutes * 60_000).toISOString();
    if (!Number.isFinite(age) || age > lateAfterMs(spec)) {
      const every = spec.everyMinutes >= 120 ? `${Math.round(spec.everyMinutes / 60)} h` : `${spec.everyMinutes} min`;
      const ran = age >= 7200_000 ? `${(age / 3600_000).toFixed(1)} h` : `${Math.round(age / 60_000)} min`;
      return { route, label: spec.label, state: "late" as const, heartbeat: hb, text: `Last ran ${ran} ago; it should run every ${every}.`, nextRunAt };
    }
    if (!hb.lastOk) return { route, label: spec.label, state: "failing" as const, heartbeat: hb, text: hb.lastError ?? hb.lastDetail ?? "Last run failed.", nextRunAt };
    return { route, label: spec.label, state: "ok" as const, heartbeat: hb, text: hb.lastDetail ?? "Last run succeeded.", nextRunAt };
  });
}

let ensured: Promise<void> | null = null;
function ensureTable(): Promise<void> {
  ensured ??= database().then((db) => db.execute(sql`CREATE TABLE IF NOT EXISTS cron_heartbeats (
      route TEXT PRIMARY KEY,
      last_run_at TIMESTAMPTZ NOT NULL,
      last_ok BOOLEAN NOT NULL,
      last_detail TEXT,
      last_ok_at TIMESTAMPTZ,
      last_error_at TIMESTAMPTZ,
      last_error TEXT
    )`)).then(() => undefined).catch((error) => { ensured = null; throw error; });
  return ensured;
}

/** Record one run. Never throws: a heartbeat failure must not break the route, so it is logged. */
export async function recordCronRun(route: string, ok: boolean, detail: string): Promise<void> {
  const text = detail.slice(0, 500);
  try {
    await ensureTable();
    const db = await database();
    await db.execute(sql`INSERT INTO cron_heartbeats (route, last_run_at, last_ok, last_detail, last_ok_at, last_error_at, last_error)
      VALUES (${route}, NOW(), ${ok}, ${text}, ${ok ? sql`NOW()` : sql`NULL`}, ${ok ? sql`NULL` : sql`NOW()`}, ${ok ? null : text})
      ON CONFLICT (route) DO UPDATE SET last_run_at = NOW(), last_ok = EXCLUDED.last_ok, last_detail = EXCLUDED.last_detail,
        last_ok_at = COALESCE(EXCLUDED.last_ok_at, cron_heartbeats.last_ok_at),
        last_error_at = COALESCE(EXCLUDED.last_error_at, cron_heartbeats.last_error_at),
        last_error = COALESCE(EXCLUDED.last_error, cron_heartbeats.last_error)`);
  } catch (error) {
    console.error(`cron heartbeat: could not record ${route}`, error);
  }
}

/**
 * Observations: facts one process learns in passing that the checklist needs
 * from another. The dispatch token's expiry is a response header only the PAT
 * receives (the daily sweep runs with the Actions token and never sees it);
 * the deployed commit is an env var only Vercel has. They are stored as
 * heartbeat rows under an `obs:` route key, which CRON_ROUTES never lists, so
 * they are not rendered as clocks; `lastRunAt` is when the value was observed.
 */
export const OBSERVATION_PREFIX = "obs:";

export async function recordObservation(key: string, value: string): Promise<void> {
  return recordCronRun(OBSERVATION_PREFIX + key, true, value);
}

/** Pure: the stored observation for `key`, or null when none has been recorded. */
export function observation(heartbeats: CronHeartbeat[], key: string): { value: string; observedAt: string } | null {
  const hb = heartbeats.find((h) => h.route === OBSERVATION_PREFIX + key);
  return hb?.lastDetail ? { value: hb.lastDetail, observedAt: hb.lastRunAt } : null;
}

export async function readCronHeartbeats(): Promise<CronHeartbeat[]> {
  await ensureTable();
  const db = await database();
  const rows = await db.execute(sql`SELECT route, last_run_at, last_ok, last_detail, last_ok_at, last_error_at, last_error FROM cron_heartbeats`);
  const iso = (v: unknown) => v == null ? null : new Date(String(v)).toISOString();
  return rows.rows.map((r) => ({ route: String(r.route), lastRunAt: iso(r.last_run_at)!, lastOk: Boolean(r.last_ok),
    lastDetail: r.last_detail == null ? null : String(r.last_detail), lastOkAt: iso(r.last_ok_at), lastErrorAt: iso(r.last_error_at),
    lastError: r.last_error == null ? null : String(r.last_error) }));
}

/**
 * Wrap a cron handler: record whether it succeeded (HTTP < 400) with a short
 * summary of its JSON body, or the error it threw. 401s are not runs (a caller
 * without the secret), so they are not recorded.
 */
export async function withHeartbeat(route: string, handler: () => Promise<Response>): Promise<Response> {
  let response: Response;
  try {
    response = await handler();
  } catch (error) {
    await recordCronRun(route, false, `threw: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
  if (response.status === 401) return response;
  let detail = `HTTP ${response.status}`;
  try { detail = `HTTP ${response.status} ${JSON.stringify(await response.clone().json())}`; } catch { /* non-JSON body: status alone is the detail */ }
  await recordCronRun(route, response.status < 400, detail);
  return response;
}
