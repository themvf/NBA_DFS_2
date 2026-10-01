/**
 * Cron dispatch bridge: the job table names real workflows, each job's cadence
 * matches the schedule it replaced, vercel.json points at routes that exist,
 * and a failed GitHub call is reported rather than thrown.
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { DISPATCH_JOBS, dispatchWorkflow, dueJobs, type DispatchContext } from "../src/lib/cron-dispatch";

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
assert.equal(tick?.schedule, "7,22,37,52 * * * *", "the bridge ticks every 15 minutes");

const keys = (iso: string, context?: DispatchContext) => dueJobs(new Date(iso), context).map((j) => j.key).sort();
const TICKS = ["07", "22", "37", "52"];
const hh = (h: number) => String(h).padStart(2, "0");

// MLB: the former `7,37 14-23,0-3 * * *` window, and nothing in the gap.
assert.ok(keys("2026-09-30T14:07:00Z").includes("mlb-odds-capture"));
assert.ok(keys("2026-09-30T03:37:00Z").includes("mlb-odds-capture"));
assert.ok(!keys("2026-09-30T09:07:00Z").includes("mlb-odds-capture"), "MLB is quiet 04:00-13:59 UTC");
// The quarter-hour ticks never spend Odds API credits: still 28 captures a day.
assert.ok(!keys("2026-09-30T14:22:00Z").includes("mlb-odds-capture"), "no MLB capture on :22");
let mlbTicks = 0;
for (let h = 0; h < 24; h += 1) for (const m of TICKS) if (keys(`2026-09-30T${hh(h)}:${m}:00Z`).includes("mlb-odds-capture")) mlbTicks += 1;
assert.equal(mlbTicks, 28, "28 MLB captures a day, as before the 15-minute tick");

// NFL availability context: hourly on the :07 tick in season, never on :37, never in July.
assert.ok(keys("2026-09-30T09:07:00Z").includes("nfl-availability-context"));
assert.ok(!keys("2026-09-30T09:37:00Z").includes("nfl-availability-context"));
assert.ok(!keys("2026-07-15T09:07:00Z").includes("nfl-availability-context"), "no NFL availability capture in July");
let contextTicks = 0;
for (let h = 0; h < 24; h += 1) for (const m of TICKS) if (keys(`2026-10-04T${hh(h)}:${m}:00Z`).includes("nfl-availability-context")) contextTicks += 1;
assert.equal(contextTicks, 24, "24 dispatches a day away from kickoffs; the job's own gate thins them to hourly/2-hourly");

// The projection and DK-pool jobs delegate to their own tested cadences.
assert.ok(keys("2026-09-27T16:07:00Z").includes("nfl-projections"), "Sunday 16:05 UTC pass lands on the :07 tick");
assert.ok(!keys("2026-09-30T16:07:00Z").includes("nfl-projections"), "no 16:05 pass on a Wednesday");
assert.ok(!keys("2026-09-27T16:22:00Z").includes("nfl-projections"), "the 16:05 pass fires once, not again on :22");
assert.ok(keys("2026-09-27T18:07:00Z").includes("nfl-dk-pool"), "Sunday game window polls every half hour");
assert.ok(!keys("2026-09-27T18:22:00Z").includes("nfl-dk-pool"), "without a kickoff in reach, not on the quarter-hour");

// Near kickoff: injuries/depth and DraftKings statuses every 15 minutes for the
// two hours before a kickoff, then back to the normal cadence once it passes.
const early: DispatchContext = { nflKickoffs: [new Date("2026-10-04T17:00:00Z")] };
for (const iso of ["2026-10-04T15:07:00Z", "2026-10-04T15:22:00Z", "2026-10-04T16:37:00Z", "2026-10-04T16:52:00Z"]) {
  assert.ok(keys(iso, early).includes("nfl-availability-context"), `availability due at ${iso}`);
  assert.ok(keys(iso, early).includes("nfl-dk-pool"), `DK statuses due at ${iso}`);
}
assert.ok(!keys("2026-10-04T14:52:00Z", early).includes("nfl-availability-context"), "2h08m out is not near");
assert.ok(!keys("2026-10-04T17:22:00Z", early).includes("nfl-availability-context"), "after kickoff, no quarter-hour capture");
assert.ok(!keys("2026-10-04T16:22:00Z", early).includes("mlb-odds-capture"), "a kickoff never adds MLB captures");
assert.ok(!keys("2026-10-04T16:22:00Z", { nflKickoffs: null }).includes("nfl-availability-context"), "unreadable schedule: half-hour cadence only");
let nearTicks = 0;
for (let h = 0; h < 24; h += 1) for (const m of TICKS) if (keys(`2026-10-04T${hh(h)}:${m}:00Z`, early).includes("nfl-availability-context")) nearTicks += 1;
assert.equal(nearTicks, 24 + 6, "the two hours before a kickoff add six quarter-hour dispatches (the :07 ticks were already due)");

// NFL availability runs in January (weeks 17-18) and February (playoffs), not in July.
assert.ok(keys("2027-01-05T09:07:00Z").includes("nfl-availability-context"), "week 17 is in January");
assert.ok(keys("2027-02-07T09:07:00Z").includes("nfl-availability-context"), "the Super Bowl is in February");

// Health: freshness readings every 3 h; the failure sweep once a day at 11:07 UTC; pbp Mon 12:07 / Tue 13:07.
let healthTicks = 0, sweepTicks = 0;
for (let h = 0; h < 24; h += 1) for (const m of TICKS) {
  const k = keys(`2026-09-30T${hh(h)}:${m}:00Z`);
  if (k.includes("pipeline-health")) healthTicks += 1;
  if (k.includes("daily-failure-sweep")) sweepTicks += 1;
}
assert.equal(healthTicks, 8, "Pipeline Health every 3 hours");
assert.equal(sweepTicks, 1, "one failure sweep a day");
assert.ok(keys("2026-09-30T11:07:00Z").includes("daily-failure-sweep"));
assert.ok(keys("2026-09-28T12:07:00Z").includes("nfl-pbp-archetypes"), "Monday 12:07 UTC");
assert.ok(keys("2026-09-29T13:07:00Z").includes("nfl-pbp-archetypes"), "Tuesday 13:07 UTC, after Monday night is published");
assert.ok(!keys("2026-09-29T09:07:00Z").includes("nfl-pbp-archetypes"), "not 09:07, before Monday night is published");
assert.ok(!keys("2026-09-29T09:22:00Z").includes("nfl-pbp-archetypes"), "once, not on the quarter-hour");

// MLB terminal settlement: hourly in March-November, never in the winter.
let settleTicks = 0;
for (let h = 0; h < 24; h += 1) for (const m of TICKS) if (keys(`2026-09-30T${hh(h)}:${m}:00Z`).includes("mlb-terminal-settlement")) settleTicks += 1;
assert.equal(settleTicks, 24, "hourly");
assert.ok(keys("2026-11-30T20:07:00Z").includes("mlb-terminal-settlement"), "November (World Series fallout)");
assert.ok(!keys("2026-12-15T20:07:00Z").includes("mlb-terminal-settlement"), "no MLB in December");
assert.ok(!keys("2027-02-15T20:07:00Z").includes("mlb-terminal-settlement"), "no MLB in February");

// Post-week review + upside grade: Tuesday 14:07 (after the 13:07 pbp relabel DST scoring reads) and Wednesday 10:07 UTC only, once each.
let postweekTicks = 0;
for (let d = 27; d <= 30; d += 1) for (let h = 0; h < 24; h += 1) for (const m of TICKS) {
  if (keys(`2026-09-${d}T${hh(h)}:${m}:00Z`).includes("nfl-dfs-postweek")) postweekTicks += 1;
}
assert.equal(postweekTicks, 2, "one dispatch Tuesday, one Wednesday, across Sun 27 - Wed 30 Sep");
assert.ok(keys("2026-09-29T14:07:00Z").includes("nfl-dfs-postweek"), "Tuesday 14:07 UTC, after Monday night's stats land (~11:34)");
assert.ok(!keys("2026-09-29T13:07:00Z").includes("nfl-dfs-postweek"), "not alongside the pbp relabel it depends on");
assert.ok(!keys("2026-09-29T10:07:00Z").includes("nfl-dfs-postweek"), "not Tuesday 10:07, before they land");
assert.ok(keys("2026-09-30T10:07:00Z").includes("nfl-dfs-postweek"), "Wednesday 10:07 UTC");
assert.ok(!keys("2026-09-29T10:37:00Z").includes("nfl-dfs-postweek"), "not the :37 tick");

// CFB terminal: every tick August-January, with the slot that matches the
// cron line it replaced, so the full CFBD schedule refresh stays six-hourly.
{
  const cfb = (iso: string) => dueJobs(new Date(iso)).find((j) => j.key === "cfb-terminal");
  const slots: Record<string, number> = {};
  for (let h = 0; h < 24; h += 1) for (const m of TICKS) {
    const job = cfb(`2026-10-03T${hh(h)}:${m}:00Z`);
    assert.ok(job, `CFB terminal dispatches at ${hh(h)}:${m}`);
    assert.equal(job!.inputs?.force_schedule, "false", "never the manual full-refresh default");
    assert.equal(job!.inputs?.capture_now, "false", "never a paid capture");
    slots[job!.inputs!.slot] = (slots[job!.inputs!.slot] ?? 0) + 1;
  }
  assert.deepEqual(slots, { schedule: 4, scores: 20, events: 72 }, "same mix as the former three cron lines");
  assert.equal(cfb("2026-10-03T06:07:00Z")!.inputs!.slot, "schedule");
  assert.equal(cfb("2026-10-03T07:07:00Z")!.inputs!.slot, "scores");
  assert.equal(cfb("2026-10-03T07:22:00Z")!.inputs!.slot, "events");
  assert.ok(cfb("2027-01-10T12:07:00Z"), "bowl season in January");
  assert.equal(cfb("2026-06-15T12:07:00Z"), undefined, "off season");
  // The table entry itself carries no stale static inputs.
  assert.equal(DISPATCH_JOBS.find((j) => j.key === "cfb-terminal")!.inputs, undefined);
}

(async () => {
  // A GitHub failure is an outcome, not an exception; a 204 is success.
  const fake = (status: number) => (async () => new Response(status === 204 ? null : "nope", { status })) as unknown as typeof fetch;
  const job = DISPATCH_JOBS[0];
  assert.deepEqual(await dispatchWorkflow(job, "t", fake(204)), { key: job.key, workflow: job.workflow, ok: true, status: 204 });
  const failed = await dispatchWorkflow(job, "t", fake(422));
  assert.equal(failed.ok, false); assert.equal(failed.status, 422); assert.equal(failed.detail, "nope");
  const threw = await dispatchWorkflow(job, "t", (async () => { throw new Error("offline"); }) as unknown as typeof fetch);
  assert.equal(threw.ok, false); assert.equal(threw.detail, "offline");
  // With return_run_details GitHub answers 200 and names the run it created.
  let sentBody = "";
  const withRun = (async (_url: string, init: RequestInit) => { sentBody = String(init.body);
    return new Response(JSON.stringify({ workflow_run_id: 42, html_url: "https://github.com/x/runs/42" }), { status: 200 }); }) as unknown as typeof fetch;
  const created = await dispatchWorkflow(job, "t", withRun, { returnRunDetails: true });
  assert.equal(created.ok, true); assert.equal(created.runId, 42); assert.equal(created.htmlUrl, "https://github.com/x/runs/42");
  assert.equal(JSON.parse(sentBody).return_run_details, true);
})().then(() => {
  console.log(`Cron dispatch: ${DISPATCH_JOBS.length} bridged workflows on a 15-minute tick; half-hour cadences unchanged, NFL availability every 15 minutes before a kickoff.`);
}).catch((e) => { console.error(e); process.exit(1); });
