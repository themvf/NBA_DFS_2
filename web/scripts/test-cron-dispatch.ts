/**
 * Cron dispatch bridge: the job table names real workflows, each job's cadence
 * matches the schedule it replaced, vercel.json points at routes that exist,
 * and a failed GitHub call is reported rather than thrown.
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { DISPATCH_JOBS, dispatchWorkflow, dueJobs } from "../src/lib/cron-dispatch";

const root = path.resolve(__dirname, "..");
const repo = path.resolve(root, "..");

// Every job dispatches a workflow file that exists in the repo.
for (const job of DISPATCH_JOBS) {
  assert.ok(existsSync(path.join(repo, ".github", "workflows", job.workflow)), `${job.key}: ${job.workflow} is missing`);
}
assert.equal(new Set(DISPATCH_JOBS.map((j) => j.key)).size, DISPATCH_JOBS.length, "job keys are unique");

// Every cron path in vercel.json has a route.
const vercel = JSON.parse(readFileSync(path.join(root, "vercel.json"), "utf8")) as { crons: Array<{ path: string; schedule: string }> };
for (const cron of vercel.crons) {
  assert.ok(existsSync(path.join(root, "src", "app", cron.path, "route.ts")), `${cron.path} has no route`);
}
const tick = vercel.crons.find((c) => c.path === "/api/cron/dispatch");
assert.equal(tick?.schedule, "7,37 * * * *", "the bridge ticks at :07 and :37, every hour");

const keys = (iso: string) => dueJobs(new Date(iso)).map((j) => j.key).sort();

// MLB: the former `7,37 14-23,0-3 * * *` window, and nothing in the gap.
assert.ok(keys("2026-09-30T14:07:00Z").includes("mlb-odds-capture"));
assert.ok(keys("2026-09-30T03:37:00Z").includes("mlb-odds-capture"));
assert.ok(!keys("2026-09-30T09:07:00Z").includes("mlb-odds-capture"), "MLB is quiet 04:00-13:59 UTC");

// NFL availability context: hourly on the :07 tick in season, never on :37, never in July.
assert.ok(keys("2026-09-30T09:07:00Z").includes("nfl-availability-context"));
assert.ok(!keys("2026-09-30T09:37:00Z").includes("nfl-availability-context"));
assert.ok(!keys("2026-07-15T09:07:00Z").includes("nfl-availability-context"), "no NFL availability capture in July");
let contextTicks = 0;
for (let h = 0; h < 24; h += 1) for (const m of ["07", "37"]) if (keys(`2026-10-04T${String(h).padStart(2, "0")}:${m}:00Z`).includes("nfl-availability-context")) contextTicks += 1;
assert.equal(contextTicks, 24, "24 dispatches a day; the job's own gate thins them to hourly/2-hourly");

// The projection and DK-pool jobs delegate to their own tested cadences.
assert.ok(keys("2026-09-27T16:07:00Z").includes("nfl-projections"), "Sunday 16:05 UTC pass lands on the :07 tick");
assert.ok(!keys("2026-09-30T16:07:00Z").includes("nfl-projections"), "no 16:05 pass on a Wednesday");
assert.ok(keys("2026-09-27T18:07:00Z").includes("nfl-dk-pool"), "Sunday game window polls every tick");

// Post-week review + upside grade: Tuesday and Wednesday 10:07 UTC only, once each.
let postweekTicks = 0;
for (let d = 27; d <= 30; d += 1) for (let h = 0; h < 24; h += 1) for (const m of ["07", "37"]) {
  if (keys(`2026-09-${d}T${String(h).padStart(2, "0")}:${m}:00Z`).includes("nfl-dfs-postweek")) postweekTicks += 1;
}
assert.equal(postweekTicks, 2, "one dispatch Tuesday, one Wednesday, across Sun 27 - Wed 30 Sep");
assert.ok(keys("2026-09-29T10:07:00Z").includes("nfl-dfs-postweek"), "Tuesday 10:07 UTC");
assert.ok(keys("2026-09-30T10:07:00Z").includes("nfl-dfs-postweek"), "Wednesday 10:07 UTC");
assert.ok(!keys("2026-09-29T10:37:00Z").includes("nfl-dfs-postweek"), "not the :37 tick");

(async () => {
  // A GitHub failure is an outcome, not an exception; a 204 is success.
  const fake = (status: number) => (async () => new Response(status === 204 ? null : "nope", { status })) as unknown as typeof fetch;
  const job = DISPATCH_JOBS[0];
  assert.deepEqual(await dispatchWorkflow(job, "t", fake(204)), { key: job.key, workflow: job.workflow, ok: true, status: 204 });
  const failed = await dispatchWorkflow(job, "t", fake(422));
  assert.equal(failed.ok, false); assert.equal(failed.status, 422); assert.equal(failed.detail, "nope");
  const threw = await dispatchWorkflow(job, "t", (async () => { throw new Error("offline"); }) as unknown as typeof fetch);
  assert.equal(threw.ok, false); assert.equal(threw.detail, "offline");
})().then(() => {
  console.log(`Cron dispatch: ${DISPATCH_JOBS.length} bridged workflows, one :07/:37 tick, cadences match the schedules they replaced.`);
}).catch((e) => { console.error(e); process.exit(1); });
