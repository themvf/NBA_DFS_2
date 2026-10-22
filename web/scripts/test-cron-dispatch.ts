/**
 * Cron dispatch bridge: the job table names real workflows, each job's cadence
 * matches the schedule it replaced, vercel.json points at routes that exist,
 * and a failed GitHub call is reported rather than thrown.
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { DISPATCH_JOBS, dispatchWorkflow, dueJobs, parseTokenExpiry, type DispatchContext } from "../src/lib/cron-dispatch";

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

// Post-week review + upside grade: Monday 13:07 (after the 12:07 pbp relabel) and Tuesday 14:07 UTC (after the 13:07 relabel) only, once each.
let postweekTicks = 0;
for (let d = 27; d <= 30; d += 1) for (let h = 0; h < 24; h += 1) for (const m of TICKS) {
  if (keys(`2026-09-${d}T${hh(h)}:${m}:00Z`).includes("nfl-dfs-postweek")) postweekTicks += 1;
}
assert.equal(postweekTicks, 2, "one dispatch Monday, one Tuesday, across Sun 27 - Wed 30 Sep");
assert.ok(keys("2026-09-28T13:07:00Z").includes("nfl-dfs-postweek"), "Monday 13:07 UTC, Sunday's games are published");
assert.ok(!keys("2026-09-28T12:07:00Z").includes("nfl-dfs-postweek"), "not alongside the Monday pbp relabel it depends on");
assert.ok(keys("2026-09-29T14:07:00Z").includes("nfl-dfs-postweek"), "Tuesday 14:07 UTC, after Monday night's stats land (~11:34)");
assert.ok(!keys("2026-09-29T13:07:00Z").includes("nfl-dfs-postweek"), "not alongside the Tuesday pbp relabel it depends on");
assert.ok(!keys("2026-09-30T10:07:00Z").includes("nfl-dfs-postweek"), "no Wednesday pass any more");
assert.ok(!keys("2026-09-28T13:37:00Z").includes("nfl-dfs-postweek"), "not the :37 tick");

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
  // The token's expiry header (a fine-grained PAT's) rides on the outcome so the route can
  // surface it; a token without one, or a header that does not parse, adds nothing.
  assert.equal(parseTokenExpiry("2026-11-15 12:00:00 UTC"), "2026-11-15T12:00:00.000Z");
  assert.equal(parseTokenExpiry(null), null);
  assert.equal(parseTokenExpiry("never"), null);
  const expiring = (async () => new Response(null, { status: 204, headers: { "github-authentication-token-expiration": "2026-11-15 12:00:00 UTC" } })) as unknown as typeof fetch;
  assert.equal((await dispatchWorkflow(job, "t", expiring)).tokenExpiresAt, "2026-11-15T12:00:00.000Z");
  assert.equal((await dispatchWorkflow(job, "t", fake(204))).tokenExpiresAt, undefined, "no header, no field");
  assert.equal((await dispatchWorkflow(job, "t", (async () => new Response("nope", { status: 422, headers: { "github-authentication-token-expiration": "2026-11-15 12:00:00 UTC" } })) as unknown as typeof fetch)).tokenExpiresAt,
    "2026-11-15T12:00:00.000Z", "a failed dispatch still reports the expiry");
})().then(() => {
  // Upload-triggered opponent captures: dispatched only when the database says a request is waiting.
assert.ok(keys("2026-10-03T15:22:00Z", { nflKickoffs: [], nflDefensiveCapturesPending: true }).includes("nfl-defensive-capture"));
assert.ok(!keys("2026-10-03T15:22:00Z", { nflKickoffs: [], nflDefensiveCapturesPending: false }).includes("nfl-defensive-capture"));
assert.ok(!keys("2026-10-03T15:22:00Z", { nflKickoffs: [], nflDefensiveCapturesPending: null }).includes("nfl-defensive-capture"), "unreadable: no blind dispatch");
assert.ok(!keys("2026-10-03T15:22:00Z").includes("nfl-defensive-capture"), "no context, no dispatch");

// CFB pattern study: board every half hour August-January, grade once a day at 10:07 UTC.
{
  let boardTicks = 0, gradeTicks = 0;
  for (let h = 0; h < 24; h += 1) for (const m of TICKS) {
    const k = keys(`2026-10-10T${hh(h)}:${m}:00Z`);
    if (k.includes("cfb-pattern-board")) boardTicks += 1;
    if (k.includes("cfb-pattern-grade")) gradeTicks += 1;
  }
  assert.equal(boardTicks, 48, "CFB pattern board every half hour (:07 and :37)");
  assert.equal(gradeTicks, 1, "CFB pattern grade once a day");
  assert.ok(keys("2026-10-10T10:07:00Z").includes("cfb-pattern-grade"), "grade at 10:07 UTC, after the overnight finals");
  assert.ok(!keys("2026-10-10T16:22:00Z").includes("cfb-pattern-board"), "no board on the quarter-hour ticks");
  assert.ok(keys("2027-01-02T16:07:00Z").includes("cfb-pattern-board"), "bowls in January");
  assert.ok(!keys("2026-06-13T16:07:00Z").includes("cfb-pattern-board"), "no board in June");
  assert.ok(!keys("2026-06-13T10:07:00Z").includes("cfb-pattern-grade"), "no grade in June");
  const board = dueJobs(new Date("2026-10-10T16:07:00Z")).find((j) => j.key === "cfb-pattern-board");
  const grade = dueJobs(new Date("2026-10-10T10:07:00Z")).find((j) => j.key === "cfb-pattern-grade");
  assert.equal(board?.inputs?.job, "board", "board dispatch names its job");
  assert.equal(grade?.inputs?.job, "grade", "grade dispatch names its job");
  assert.equal(board?.workflow, "cfb_pattern_watch.yml");
}
// NFL props: exactly one capture tick per kickoff time, on the quarter-hour ticks 60-75 minutes ahead.
for (const kickoff of ["2026-10-11T17:00:00Z", "2026-10-11T20:05:00Z", "2026-10-11T20:25:00Z", "2026-10-12T00:20:00Z", "2026-10-11T13:30:00Z"]) {
  const k = new Date(kickoff);
  const context: DispatchContext = { nflKickoffs: [k] };
  let fired = 0;
  for (let h = 0; h < 4; h++) for (const m of TICKS) {
    const tick = new Date(k.getTime() - (4 - h) * 3600_000);
    tick.setUTCMinutes(Number(m), 0, 0);
    const lead = k.getTime() - tick.getTime();
    if (lead > 0 && lead <= 2 * 3600_000 && keys(tick.toISOString(), context).includes("nfl-prop-capture")) fired++;
  }
  assert.equal(fired, 1, `${kickoff}: one prop capture`);
}
assert.ok(keys("2026-10-11T15:52:00Z", { nflKickoffs: [new Date("2026-10-11T17:00:00Z")] }).includes("nfl-prop-capture"), "68 minutes out");
assert.ok(!keys("2026-10-11T15:22:00Z", { nflKickoffs: [new Date("2026-10-11T17:00:00Z")] }).includes("nfl-prop-capture"), "98 minutes out is too early now");
assert.ok(!keys("2026-10-11T15:52:00Z", { nflKickoffs: [] }).includes("nfl-prop-capture"), "no game near, no paid capture");
assert.ok(!keys("2026-10-11T15:52:00Z", { nflKickoffs: null }).includes("nfl-prop-capture"), "unreadable schedule: no blind paid capture");
const propInputs = DISPATCH_JOBS.find((j) => j.key === "nfl-prop-capture")?.inputs;
assert.deepEqual([propInputs?.within_hours, propInputs?.min_hours], ["1.25", "1"], "only the group kicking off 60-75 minutes out, never an unbounded capture");
// Day before: the 22:07 UTC tick with an NFL game on tomorrow's Eastern date.
const thu = new Date("2026-10-16T00:15:00Z"); // Thursday 8:15 PM ET
assert.ok(keys("2026-10-14T22:07:00Z", { nflKickoffs: [thu] }).includes("nfl-prop-capture-day-ahead"), "Wednesday evening for Thursday's game");
assert.ok(!keys("2026-10-15T22:07:00Z", { nflKickoffs: [thu] }).includes("nfl-prop-capture-day-ahead"), "not on the game's own day");
assert.ok(!keys("2026-10-14T21:52:00Z", { nflKickoffs: [thu] }).includes("nfl-prop-capture-day-ahead"), "only the 22:07 tick");
assert.ok(keys("2026-10-11T22:07:00Z", { nflKickoffs: [new Date("2026-10-13T00:15:00Z")] }).includes("nfl-prop-capture-day-ahead"), "Sunday evening for Monday night");
assert.ok(!keys("2026-10-14T22:07:00Z", { nflKickoffs: null }).includes("nfl-prop-capture-day-ahead"), "unreadable schedule: no blind paid capture");
assert.equal(DISPATCH_JOBS.find((j) => j.key === "nfl-prop-capture-day-ahead")?.inputs?.day_ahead, "true");
// Game Leaders: Wednesday's weekly build, then once per kickoff group 150-165 minutes out.
assert.ok(keys("2026-10-14T14:07:00Z").includes("nfl-game-leaders"), "Wednesday 14:07 UTC weekly build");
assert.ok(!keys("2026-10-14T14:22:00Z").includes("nfl-game-leaders"), "the weekly build fires once");
const sunEarly = { nflKickoffs: [new Date("2026-10-18T17:00:00Z")] };
const leaderTicks = Array.from({ length: 96 }, (_, i) => new Date(Date.parse("2026-10-18T00:07:00Z") + i * 900_000))
  .filter((at) => dueJobs(at, sunEarly).some((j) => j.key === "nfl-game-leaders")).map((at) => at.toISOString());
assert.deepEqual(leaderTicks, ["2026-10-18T14:22:00.000Z"], "one republish per kickoff group, 2h38m before the 1 PM ET games");
assert.ok(keys("2026-10-15T21:37:00Z", { nflKickoffs: [thu] }).includes("nfl-game-leaders"), "Thursday night: 2h38m before kickoff");
assert.ok(!keys("2026-10-18T14:22:00Z", { nflKickoffs: null }).includes("nfl-game-leaders"), "unreadable schedule: weekly build only");

// Expert coverage: exactly four passes per kickoff group, at T-48h, T-24h, T-6h and T-90m.
const tnf = { nflKickoffs: [new Date("2026-10-23T00:15:00Z")] };
const expertTicks = Array.from({ length: 4 * 96 }, (_, i) => new Date(Date.parse("2026-10-20T00:07:00Z") + i * 900_000))
  .filter((at) => dueJobs(at, tnf).some((j) => j.key === "nfl-expert-coverage")).map((at) => at.toISOString());
assert.deepEqual(expertTicks, ["2026-10-21T00:22:00.000Z", "2026-10-22T00:22:00.000Z", "2026-10-22T18:22:00.000Z",
  "2026-10-22T22:52:00.000Z"], "T-48h, T-24h, T-6h and T-90m before Thursday night's 00:15 UTC kickoff");
const sunGroups = { nflKickoffs: [new Date("2026-10-25T17:00:00Z"), new Date("2026-10-25T20:25:00Z")] };
assert.ok(keys("2026-10-25T15:37:00Z", sunGroups).includes("nfl-expert-coverage"), "T-90m of the 1 PM ET group");
assert.ok(keys("2026-10-25T19:07:00Z", sunGroups).includes("nfl-expert-coverage"), "T-90m of the 4:25 PM ET group");
assert.ok(!keys("2026-10-25T16:22:00Z", sunGroups).includes("nfl-expert-coverage"), "38 minutes out is no pass");
assert.ok(!keys("2026-10-21T00:07:00Z", { nflKickoffs: null }).includes("nfl-expert-coverage"), "unreadable schedule: no blind pass");

// NBA points props: 15:07 and 22:07 UTC, October-June, nothing else; no kickoff context needed.
assert.ok(keys("2026-10-21T15:07:00Z").includes("nba-points-props"), "11:07 AM ET morning read");
assert.ok(keys("2026-10-21T22:07:00Z").includes("nba-points-props"), "6:07 PM ET closing read");
assert.ok(keys("2026-10-21T22:07:00Z", { nflKickoffs: null }).includes("nba-points-props"), "no schedule read needed");
assert.ok(!keys("2026-10-21T15:22:00Z").includes("nba-points-props"), "the :07 tick only");
assert.ok(!keys("2026-10-21T15:37:00Z").includes("nba-points-props"), "not every half hour: each run spends credits");
assert.ok(!keys("2026-08-21T15:07:00Z").includes("nba-points-props"), "off-season: nothing to capture");
assert.ok(keys("2027-06-10T22:07:00Z").includes("nba-points-props"), "Finals in June");
let nbaTicks = 0;
for (let h = 0; h < 24; h += 1) for (const m of TICKS) if (keys(`2026-10-21T${hh(h)}:${m}:00Z`).includes("nba-points-props")) nbaTicks += 1;
assert.equal(nbaTicks, 2, "exactly two paid NBA captures a day");
assert.equal(DISPATCH_JOBS.find((j) => j.key === "nba-points-props")?.inputs?.max_credits, "40");

console.log(`Cron dispatch: ${DISPATCH_JOBS.length} bridged workflows on a 15-minute tick; half-hour cadences unchanged, NFL availability every 15 minutes before a kickoff.`);
}).catch((e) => { console.error(e); process.exit(1); });
