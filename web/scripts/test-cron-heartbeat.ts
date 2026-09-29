/**
 * Cron heartbeats: every route in vercel.json is registered; a route that has
 * never run, stopped running, or last failed is reported as such.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { CRON_ROUTES, cronStatuses, lateAfterMs, type CronHeartbeat } from "../src/lib/cron-heartbeat";

// Every cron path in vercel.json has a heartbeat entry, and every entry is a real cron.
const vercel = JSON.parse(readFileSync(path.resolve(__dirname, "..", "vercel.json"), "utf8")) as { crons: { path: string }[] };
const cronRoutes = vercel.crons.map((c) => c.path.replace(/^\/api\/cron\//, "")).sort();
const vercelRoutes = Object.entries(CRON_ROUTES).filter(([, s]) => (s.source ?? "vercel") === "vercel").map(([k]) => k).sort();
assert.deepEqual(vercelRoutes, cronRoutes, "CRON_ROUTES must list exactly the crons in vercel.json");
// Each route file wraps its handler.
for (const route of cronRoutes) {
  const src = readFileSync(path.resolve(__dirname, "..", "src", "app", "api", "cron", route, "route.ts"), "utf8");
  assert.match(src, new RegExp(`withHeartbeat\\("${route}"`), `${route} records a heartbeat`);
}

const now = Date.parse("2026-09-29T12:30:00Z");
const hb = (route: string, minutesAgo: number, ok: boolean, over: Partial<CronHeartbeat> = {}): CronHeartbeat => ({
  route, lastRunAt: new Date(now - minutesAgo * 60_000).toISOString(), lastOk: ok, lastDetail: ok ? "HTTP 200" : null,
  lastOkAt: ok ? new Date(now - minutesAgo * 60_000).toISOString() : null, lastErrorAt: ok ? null : new Date(now).toISOString(),
  lastError: ok ? null : "HTTP 500 {\"error\":\"boom\"}", ...over });

const statuses = cronStatuses([
  hb("dispatch", 10, true),
  hb("nfl-slate-check", 200, true),             // should run every 30 min: late
  hb("nfl-pool-capture", 1, false),             // ran a minute ago and failed
], now);
const s = (route: string) => statuses.find((x) => x.route === route)!;
assert.equal(s("dispatch").state, "ok");
assert.equal(s("nfl-slate-check").state, "late");
assert.match(s("nfl-slate-check").text, /Last ran 3.3 h ago; it should run every 30 min/);
assert.equal(s("nfl-pool-capture").state, "failing");
assert.match(s("nfl-pool-capture").text, /boom/);
assert.equal(s("event-closing-lines").state, "never", "no heartbeat is never, not ok");
assert.equal(lateAfterMs(CRON_ROUTES["dispatch"]), 50 * 60_000);
assert.equal(lateAfterMs(CRON_ROUTES["daily-failure-sweep"]), 26 * 3600_000, "the daily sweep is late after 26 h, not 3 days");
assert.equal(s("dispatch").nextRunAt, new Date(now - 10 * 60_000 + 15 * 60_000).toISOString());

console.log("Cron heartbeat: every vercel.json cron is registered and wrapped; never / late / failing / ok are distinguished.");
