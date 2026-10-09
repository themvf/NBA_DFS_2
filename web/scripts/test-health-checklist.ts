/**
 * The /health checklist and the daily failure sweep:
 * - cron parsing and next/overdue times;
 * - a job is FAIL when its last run failed, when it is overdue against its own
 *   schedule, when it never ran although scheduled, or when a workflow_run
 *   follower did not follow; a disabled job and a manual job are INFO;
 * - one unreadable workflow is its own row; an unreadable source is a FAIL row;
 * - datasets, clocks, slates and the availability monitor map to PASS/FAIL;
 * - the sweep opens, comments (once a day unless something new), updates and
 *   closes the tracking issue correctly.
 */
import assert from "node:assert/strict";
import { cronTimes, lastCronTime, nextCronTime, parseCron, cronMatches } from "../src/lib/cron-schedule";
import { buildChecklist, type ChecklistInputs, type ManifestWorkflow } from "../src/lib/health-checklist";
import { planSweep, problemsFromChecklist, parseState } from "../src/lib/failure-sweep";
import type { WorkflowRunLite } from "../src/lib/workflow-health";

// --- cron ---
const every15 = cronTimes(["3,18,33,48 * * * *"], new Date("2026-09-29T12:40:00Z"), 60);
assert.deepEqual(every15.map((d) => d.toISOString().slice(11, 16)), ["12:48", "13:03", "13:18", "13:33"]);
assert.equal(cronTimes(["0 12 * * 1"], new Date("2026-09-29T12:00:00Z"), 8 * 1440)[0].toISOString(), "2026-10-05T12:00:00.000Z", "next Monday noon");
assert.equal(cronTimes(["17 */6 * 1,2,9-12 *"], new Date("2026-12-31T23:00:00Z"), 120)[0].toISOString(), "2027-01-01T00:17:00.000Z", "January is included");
assert.ok(cronMatches(parseCron("0 9 * * 7"), new Date("2026-09-27T09:00:00Z")), "7 means Sunday");
assert.throws(() => parseCron("* * *"), /5 fields/);
// Steps, ranges with steps, start/step, and 7 inside a range.
assert.deepEqual([...parseCron("0 9 1-10/3 * *").dom], [1, 4, 7, 10]);
assert.deepEqual([...parseCron("5/10 * * * *").minute], [5, 15, 25, 35, 45, 55]);
assert.deepEqual([...parseCron("*/15 * * * *").minute], [0, 15, 30, 45]);
assert.deepEqual([...parseCron("0 9 * * 5-7").dow], [5, 6, 0]);
// Day-of-month and day-of-week both restricted: either may match (standard cron).
const either = parseCron("0 9 1-10 * 2");
assert.ok(cronMatches(either, new Date("2026-09-15T09:00:00Z")), "Tuesday the 15th matches on the weekday");
assert.ok(cronMatches(either, new Date("2026-09-03T09:00:00Z")), "Thursday the 3rd matches on the day of month");
assert.ok(!cronMatches(either, new Date("2026-09-16T09:00:00Z")), "Wednesday the 16th matches neither");
// A field that can never fire throws; it used to parse to an empty set and describe a job that could never be overdue.
for (const bad of ["60 9 * * *", "0 24 * * *", "0 9 0 * *", "0 9 32 * *", "0 9 * 13 *", "0 9 * * 8", "0 9 10-1 * *", "*/ * * * *", "1/0 * * * *", "0 9 * * MON", "0 9 * JAN *", "1.5 * * * *"]) {
  assert.throws(() => parseCron(bad), /bad cron field/, `${bad} must throw`);
}
// Exact last/next fire times beyond the 8-day scan: monthly, seasonal, and dense crons.
const probeAt = new Date("2026-09-29T12:43:00Z");
assert.equal(lastCronTime(["0 6 1 * *"], probeAt)!.toISOString(), "2026-09-01T06:00:00.000Z", "monthly: last fire 28 days back");
assert.equal(nextCronTime(["0 6 1 * *"], probeAt)!.toISOString(), "2026-10-01T06:00:00.000Z", "monthly: next fire beyond 8 days");
assert.equal(lastCronTime(["0 6 1 * *"], probeAt, 20), null, "nothing inside a 20-day lookback");
assert.equal(lastCronTime(["17 */6 * 1,2,9-12 *"], new Date("2026-03-15T00:00:00Z"))!.toISOString(), "2026-02-28T18:17:00.000Z", "seasonal: last slot of February");
assert.equal(nextCronTime(["17 */6 * 1,2,9-12 *"], new Date("2026-03-15T00:00:00Z"))!.toISOString(), "2026-09-01T00:17:00.000Z", "seasonal: first slot of September");
assert.equal(lastCronTime(["*/5 * * * *"], probeAt)!.toISOString(), "2026-09-29T12:40:00.000Z");
assert.equal(nextCronTime(["*/5 * * * *"], probeAt)!.toISOString(), "2026-09-29T12:45:00.000Z");
assert.equal(lastCronTime(["*/5 * * * *"], new Date("2026-09-29T12:40:00Z"))!.toISOString(), "2026-09-29T12:40:00.000Z", "at or before");
assert.equal(lastCronTime(["7 */6 * * *", "37 * * * *"], probeAt)!.toISOString(), "2026-09-29T12:37:00.000Z", "newest across two specs on one day");
assert.equal(nextCronTime(["0 12 * * 1", "0 14 * * 2"], new Date("2026-12-31T23:59:00Z"))!.toISOString(), "2027-01-04T12:00:00.000Z", "year boundary");

// --- checklist ---
const now = new Date("2026-09-29T12:40:00Z");
let id = 0;
const run = (file: string, createdAt: string, conclusion: string | null, name = file): WorkflowRunLite =>
  ({ id: ++id, workflowId: 1, name, workflow: file, event: "schedule", status: conclusion ? "completed" : "in_progress", conclusion, createdAt, url: `https://gh/run/${id}` });
const wf = (file: string, over: Partial<ManifestWorkflow> = {}): ManifestWorkflow =>
  ({ file, name: file.replace(".yml", ""), crons: [], dispatch: true, afterWorkflows: [], push: false, pullRequest: false, ...over });

const base = (over: Partial<ChecklistInputs> = {}): ChecklistInputs => ({
  now, nextCheckAt: new Date(now.getTime() + 30 * 60_000),
  manifest: [
    wf("hourly.yml", { crons: ["5 * * * *"] }),                 // ran 12:05: pass
    wf("broken.yml", { crons: ["0 */3 * * *"] }),               // last run failed
    wf("dropped.yml"),                                          // the dispatcher started it at 09:07 today; it never ran
    wf("late.yml", { crons: ["0 9 * * *"] }),                   // GitHub daily 09:00, not run yet today: late, not missed
    wf("stopped.yml", { crons: ["0 9 * * *"] }),                // GitHub daily, last ran two days ago: missed
    wf("follower.yml", { afterWorkflows: ["hourly"] }),         // did not follow hourly's success
    wf("manual.yml"),                                           // manual, succeeded
    wf("oldmanual.yml"),                                        // manual, failed a month ago
    wf("disabled.yml", { crons: ["0 * * * *"] }),
    wf("new.yml", { crons: ["0 12 * * *"] }),                   // not on GitHub yet (404)
    wf("never.yml", { crons: ["0 * * * *"] }),                  // scheduled, no runs at all
  ],
  dispatchTimes: { "dropped.yml": { past: [new Date("2026-09-28T09:07:00Z"), new Date("2026-09-29T09:07:00Z")], future: [new Date("2026-09-30T09:07:00Z")] } },
  runs: {
    "hourly.yml": [run("hourly.yml", "2026-09-29T11:05:00Z", "success", "hourly"), run("hourly.yml", "2026-09-29T12:05:00Z", "success", "hourly")],
    "broken.yml": [run("broken.yml", "2026-09-29T09:00:00Z", "failure"), run("broken.yml", "2026-09-29T12:00:00Z", "failure"), run("broken.yml", "2026-09-29T06:00:00Z", "success")],
    "dropped.yml": [run("dropped.yml", "2026-09-28T09:08:00Z", "success")],
    "late.yml": [run("late.yml", "2026-09-28T11:40:00Z", "success")],
    "stopped.yml": [run("stopped.yml", "2026-09-27T10:02:00Z", "success")],
    "follower.yml": [run("follower.yml", "2026-09-29T09:00:00Z", "success")],
    "manual.yml": [run("manual.yml", "2026-09-20T10:00:00Z", "success")],
    "oldmanual.yml": [run("oldmanual.yml", "2026-08-20T10:00:00Z", "failure")],
    "disabled.yml": [run("disabled.yml", "2026-09-01T10:00:00Z", "success")],
    "never.yml": [],
  },
  workflowStates: { "disabled.yml": "disabled_manually" },
  workflowErrors: { "new.yml": "GitHub answered 404 for /workflows/new.yml/runs" },
  githubError: null,
  datasets: [
    { key: "nfl_dfs_projections", label: "NFL DFS projections", status: "fresh", lastRowAt: "2026-09-29T12:10:00Z", ageHours: 0.5, maxAgeHours: 36, ownerWorkflow: "hourly.yml", checkedAt: "2026-09-29T12:00:00Z" },
    { key: "mlb_props", label: "MLB player-prop odds", status: "stale", lastRowAt: "2026-08-23T00:00:00Z", ageHours: 880, maxAgeHours: 24, ownerWorkflow: "broken.yml", checkedAt: "2026-09-29T12:00:00Z" },
    { key: "soccer", label: "Soccer", status: "dormant", lastRowAt: null, ageHours: null, maxAgeHours: 24, ownerWorkflow: "disabled.yml", checkedAt: "2026-09-29T12:00:00Z", note: "between tournaments" },
  ],
  datasetsError: null, datasetCadenceHours: 3,
  heartbeats: [{ route: "dispatch", lastRunAt: "2026-09-29T12:37:00Z", lastOk: true, lastDetail: "HTTP 200", lastOkAt: "2026-09-29T12:37:00Z", lastErrorAt: null, lastError: null }],
  heartbeatsError: null,
  slates: [{ signature: "sig-thu", headline: "Showdown · SEA @ ARI: 2 things need you", needs: 2, checkedAt: "2026-09-29T12:12:00Z" }],
  slatesError: null,
  availabilityOps: { status: "critical", evaluatedAt: "2026-09-29T12:11:00Z", week: 4, alerts: ["critical Latest Sleeper capture is 7.0 hours old."] },
  availabilityOpsError: null,
  ...over,
});

const items = buildChecklist(base());
const get = (key: string) => { const i = items.find((x) => x.key === key); assert.ok(i, `missing ${key}`); return i!; };
assert.equal(get("workflow:hourly.yml").status, "pass");
assert.match(get("workflow:hourly.yml").nextEventAt!, /2026-09-29T13:05/);
assert.equal(get("workflow:broken.yml").status, "fail");
assert.match(get("workflow:broken.yml").detail, /2 runs in a row failed.*last success/);
assert.equal(get("workflow:dropped.yml").status, "fail");
assert.match(get("workflow:dropped.yml").detail, /^Overdue: the dispatcher was due to start it Sep 29, 5:07 AM ET \(allowing 2 h\), but it has not run since/);
// GitHub's scheduler is best effort: a daily job 3.7 h late is late, not missed.
assert.equal(get("workflow:late.yml").status, "pass", "GitHub slot inside its 12 h grace");
// But a GitHub schedule that has stopped firing is caught once a slot is 12 h old.
assert.equal(get("workflow:stopped.yml").status, "fail");
assert.match(get("workflow:stopped.yml").detail, /^Overdue: GitHub's scheduler was due to start it Sep 28, 5:00 AM ET \(allowing 12 h\), but it has not run since Sep 27/);
assert.equal(get("workflow:follower.yml").status, "fail");
assert.match(get("workflow:follower.yml").detail, /Did not run after hourly succeeded/);
assert.equal(get("workflow:follower.yml").nextEventNote, "after hourly");
assert.equal(get("workflow:manual.yml").status, "pass");
assert.equal(get("workflow:manual.yml").nextEventNote, "manual");
assert.equal(get("workflow:oldmanual.yml").status, "info", "an old manual failure is shown, not emailed");
assert.equal(get("workflow:disabled.yml").status, "info");
assert.match(get("workflow:disabled.yml").detail, /disabled manually/);
assert.equal(get("workflow:new.yml").status, "info");
assert.match(get("workflow:new.yml").detail, /Not on GitHub's default branch yet/);
assert.equal(get("workflow:never.yml").status, "fail");
assert.match(get("workflow:never.yml").detail, /^Scheduled by GitHub's scheduler .*allowing 12 h\) but no run found\.$/);
assert.equal(get("data:nfl_dfs_projections").status, "pass");
assert.equal(get("data:nfl_dfs_projections").group, "NFL DFS");
assert.equal(get("data:nfl_dfs_projections").nextEventAt, get("workflow:hourly.yml").nextEventAt, "next write = owner job's next run");
assert.equal(get("data:mlb_props").status, "fail");
assert.equal(get("data:soccer").status, "info");
assert.equal(get("checklist:freshness-monitor").status, "pass");
assert.equal(get("clock:dispatch").status, "pass");
assert.equal(get("clock:health-check").status, "fail", "a clock with no heartbeat is never-run, a FAIL");
assert.equal(get("slate:sig-thu").status, "fail");
assert.equal(get("nfl:availability-monitor").status, "fail");
assert.match(get("nfl:availability-monitor").detail, /Week 4: critical \(critical Latest Sleeper capture is 7.0 hours old\.\)/);
assert.equal(items[0].status, "fail", "failures sort first");
for (const i of items) { assert.ok(i.lastCheckedAt, `${i.key} has a last-checked time`); }

// A running job keeps the previous verdict; the freshness monitor itself going quiet is a FAIL.
const quiet = buildChecklist(base({ datasets: base().datasets!.map((d) => ({ ...d, checkedAt: "2026-09-29T02:00:00Z" })) }));
assert.equal(quiet.find((i) => i.key === "checklist:freshness-monitor")!.status, "fail");

// The monitor is overdue one hour past its cadence, and its row says the data rows are that old.
const at = (iso: string) => base().datasets!.map((d) => ({ ...d, checkedAt: iso }));
assert.equal(buildChecklist(base({ datasets: at("2026-09-29T09:10:00Z") })).find((i) => i.key === "checklist:freshness-monitor")!.status, "pass", "3.5 h old reading, 3 h cadence");
const lagging = buildChecklist(base({ datasets: at("2026-09-29T08:10:00Z") })).find((i) => i.key === "checklist:freshness-monitor")!;
assert.equal(lagging.status, "fail", "4.5 h old reading is overdue");
assert.match(lagging.detail, /^Overdue: last reading 4\.5 h ago/);
// With the job in the manifest, its next run comes from the job's schedule, and data rows check again then.
const withMonitor = buildChecklist(base({ manifest: [...base().manifest, wf("pipeline_health.yml", { crons: ["7 */3 * * *"] })],
  runs: { ...base().runs!, "pipeline_health.yml": [run("pipeline_health.yml", "2026-09-29T12:07:00Z", "success")] } }));
assert.match(withMonitor.find((i) => i.key === "checklist:freshness-monitor")!.nextEventAt!, /2026-09-29T15:07/);
assert.match(withMonitor.find((i) => i.key === "data:mlb_props")!.nextCheckAt!, /2026-09-29T15:07/);

// Data rows state what the reading saw, as of the reading, never an age that reads as "now".
assert.equal(get("data:nfl_dfs_projections").detail, "Newest row was 30 min old at the Sep 29, 8:00 AM ET reading; budget 36 h; written by hourly.yml.");
assert.equal(get("data:mlb_props").detail, "Newest row was 36.7 days old at the Sep 29, 8:00 AM ET reading, 36.7x its 24 h budget; written by broken.yml.");
// A row that is not due gives the checker's reason, not a generic "out of season".
const pending = buildChecklist(base({ datasets: [{ key: "nfl_context_variant_freeze", label: "NFL context variant freeze", status: "dormant", lastRowAt: null, ageHours: null,
  maxAgeHours: 168, ownerWorkflow: "hourly.yml", checkedAt: "2026-09-29T12:00:00Z", note: "Current study pin; zero context rows by Saturday 21:35 UTC fails.",
  detail: "week 4: pending; 0 eligible player-weeks; deadline 2026-10-03T21:35:00+00:00; missing started games []" }] }));
const freeze = pending.find((i) => i.key === "data:nfl_context_variant_freeze")!;
assert.equal(freeze.status, "info");
assert.match(freeze.detail, /^Not due now: week 4: pending/);
assert.doesNotMatch(freeze.detail, /season/i);
// A deliberately paused feed says so, with how to resume, and is not emailed.
const pausedRow = buildChecklist(base({ datasets: [{ key: "mlb_props", label: "MLB player-prop odds", status: "dormant", lastRowAt: "2026-08-23T13:47:00Z", ageHours: null,
  maxAgeHours: 24, ownerWorkflow: "refresh_mlb_vegas.yml", checkedAt: "2026-09-29T12:00:00Z", detail: "paused on the schedule: 'Capture MLB player-prop odds' in refresh_mlb_vegas.yml runs only when `inputs.run_props == true`; last write 37.0d ago (2026-08-23)" }] }))
  .find((i) => i.key === "data:mlb_props")!;
assert.equal(pausedRow.status, "info");
assert.equal(pausedRow.detail, "Paused on purpose: 'Capture MLB player-prop odds' in refresh_mlb_vegas.yml runs only when `inputs.run_props == true`; last write 37.0d ago (2026-08-23).");
assert.equal(pausedRow.lastEventAt, "2026-08-23T13:47:00Z", "the last write is still shown");

// A check with its own explanation and no timestamp (the freeze check failing) shows that explanation.
const broke = buildChecklist(base({ datasets: [{ key: "nfl_context_variant_freeze", label: "x", status: "empty", lastRowAt: null, ageHours: null, maxAgeHours: 168,
  ownerWorkflow: "hourly.yml", checkedAt: "2026-09-29T12:00:00Z", detail: "context freeze check failed: KeyError" }] })).find((i) => i.key === "data:nfl_context_variant_freeze")!;
assert.equal(broke.status, "fail");
assert.match(broke.detail, /^Context freeze check failed: KeyError \(at the Sep 29, 8:00 AM ET reading\)\.$/);
// The availability monitor's next run is its workflow's.
const avail = buildChecklist(base({ manifest: [...base().manifest, wf("refresh_nfl_availability_context.yml", { crons: ["17 */6 * 1,2,9-12 *"] })],
  runs: { ...base().runs!, "refresh_nfl_availability_context.yml": [run("refresh_nfl_availability_context.yml", "2026-09-29T12:17:00Z", "success")] } }));
assert.match(avail.find((i) => i.key === "nfl:availability-monitor")!.nextEventAt!, /2026-09-29T18:17/);

// Unreadable sources are FAIL rows, never gaps.
const blind = buildChecklist(base({ runs: null, githubError: "GitHub answered 401", datasets: null, datasetsError: "db down", heartbeats: null, heartbeatsError: "db down", slates: null, slatesError: "db down", availabilityOps: null, availabilityOpsError: "db down" }));
for (const key of ["checklist:github", "checklist:datasets", "checklist:heartbeats", "checklist:slates", "checklist:availability-ops"]) {
  assert.equal(blind.find((i) => i.key === key)?.status, "fail", `${key} is a FAIL row when unreadable`);
}
assert.match(blind.find((i) => i.key === "checklist:github")!.detail, /Could not be checked: GitHub answered 401/);

// A job on a dense GitHub cron (*/5) that stopped two days ago. The old 8-day scan was
// capped at 400 fire times, so the newest slot it could judge against was 6.6 days old
// and the job read PASS until it had been dead for a week.
const only = (manifest: ManifestWorkflow[], runs: Record<string, WorkflowRunLite[]>, over: Partial<ChecklistInputs> = {}) =>
  buildChecklist(base({ manifest, runs, dispatchTimes: {}, workflowStates: {}, workflowErrors: {}, ...over }));
const dense = only([wf("five.yml", { crons: ["*/5 * * * *"] })], { "five.yml": [run("five.yml", "2026-09-27T12:00:00Z", "success")] }).find((i) => i.key === "workflow:five.yml")!;
assert.equal(dense.status, "fail");
assert.match(dense.detail, /^Overdue: GitHub's scheduler was due to start it Sep 28, 8:40 PM ET \(allowing 12 h\), but it has not run since Sep 27/);
// A monthly cron whose slot was four weeks ago and whose last run was two months ago: an
// 8-day window used to let this age out into "Last run succeeded Aug 1"; its next run
// is also more than 8 days away and must still be shown.
const monthly = only([wf("monthly.yml", { crons: ["0 6 1 * *"] })], { "monthly.yml": [run("monthly.yml", "2026-08-01T06:01:00Z", "success")] }).find((i) => i.key === "workflow:monthly.yml")!;
assert.equal(monthly.status, "fail");
assert.match(monthly.detail, /^Overdue: GitHub's scheduler was due to start it Sep 1, 2:00 AM ET \(allowing 12 h\), but it has not run since Aug 1/);
assert.equal(monthly.nextEventAt, "2026-10-01T06:00:00.000Z");
// The same monthly job that ran on time is fine, and a seasonal job out of season is not overdue.
assert.equal(only([wf("monthly.yml", { crons: ["0 6 1 * *"] })], { "monthly.yml": [run("monthly.yml", "2026-09-01T06:01:00Z", "success")] }).find((i) => i.key === "workflow:monthly.yml")!.status, "pass");
const offSeason = only([wf("adp.yml", { crons: ["7 0,12 * 7-9 *"] })], { "adp.yml": [run("adp.yml", "2026-09-30T12:07:30Z", "success")] }, { now: new Date("2026-11-15T12:00:00Z"), nextCheckAt: new Date("2026-11-15T12:30:00Z") }).find((i) => i.key === "workflow:adp.yml")!;
assert.equal(offSeason.status, "pass");
assert.equal(offSeason.nextEventAt, "2027-07-01T00:07:00.000Z", "next season's first slot, not a blank");

// One workflow the checklist cannot judge (a cron the parser rejects) is its own FAIL row; it
// used to throw out of buildChecklist and take every other row down with it.
const partial = only([wf("bad.yml", { crons: ["0 9 * * MON"] }), wf("good.yml", { crons: ["0 * * * *"] })], { "bad.yml": [], "good.yml": [run("good.yml", "2026-09-29T12:00:00Z", "success")] });
assert.equal(partial.find((i) => i.key === "workflow:bad.yml")!.status, "fail");
assert.match(partial.find((i) => i.key === "workflow:bad.yml")!.detail, /^Could not be checked: the checklist could not judge this workflow \(bad cron field "MON"\)/);
assert.equal(partial.find((i) => i.key === "workflow:good.yml")!.status, "pass", "the other workflows are still judged");
// A scheduled job whose recent runs were all cancelled has no finished run to vouch for it; this
// crashed the whole checklist ("Cannot read properties of null") instead of being a FAIL row.
const cancelledOnly = only([wf("c.yml", { crons: ["0 * * * *"] })], { "c.yml": [run("c.yml", "2026-09-29T12:00:00Z", "cancelled"), run("c.yml", "2026-09-29T11:00:00Z", "cancelled")] }).find((i) => i.key === "workflow:c.yml")!;
assert.equal(cancelledOnly.status, "fail");
assert.match(cancelledOnly.detail, /^None of its last 2 runs finished \(newest was cancelled/);

// Cancelled runs did no work. An hourly job whose last success was three days ago and whose
// eight runs since were all cancelled read "Last run succeeded Sep 26"; three in a row is a FAIL,
// and even one is named rather than skipped over.
const cancelledRuns = (n: number) => Array.from({ length: n }, (_, k) => run("h.yml", new Date(now.getTime() - (k + 1) * 3600_000).toISOString(), "cancelled"));
const cancelledStreak = only([wf("h.yml", { crons: ["0 * * * *"] })], { "h.yml": [...cancelledRuns(8), run("h.yml", "2026-09-26T12:00:00Z", "success")] }).find((i) => i.key === "workflow:h.yml")!;
assert.equal(cancelledStreak.status, "fail");
assert.match(cancelledStreak.detail, /^Its last 8 runs were cancelled before finishing \(latest Sep 29, 7:40 AM ET\); last success Sep 26/);
const cancelledOnce = only([wf("h.yml", { crons: ["0 * * * *"] })], { "h.yml": [...cancelledRuns(1), run("h.yml", "2026-09-29T10:00:00Z", "success")] }).find((i) => i.key === "workflow:h.yml")!;
assert.equal(cancelledOnce.status, "pass", "one cancelled run is forgiven");
assert.match(cancelledOnce.detail, /^Last completed run succeeded Sep 29, 6:00 AM ET; its newest run \(latest Sep 29, 7:40 AM ET\) was cancelled before finishing\./);

// --- sweep ---
const problems = problemsFromChecklist(items);
assert.ok(problems.every((p) => items.find((i) => i.key === p.key)?.status === "fail"));
assert.equal(problems.find((p) => p.key === "slate:sig-thu")?.url, "https://nbadfs.vercel.app/dfs/nfl", "relative links become absolute in email");
const t1 = "2026-09-29T11:07:00Z";
const open = planSweep(null, problems, t1, "themvf");
assert.equal(open.kind, "open");
if (open.kind !== "open") throw new Error("unreachable");
assert.match(open.comment, /^@themvf daily failure sweep/);
assert.match(open.comment, /\*\*New \(\d+\)\*\*/);
const state = parseState(open.body)!;
assert.equal(Object.keys(state.firstSeen).length, problems.length);
assert.equal(state.lastCommentDay, "2026-09-29");
// Second run the same day, nothing new: update the body without emailing again.
assert.equal(planSweep(open.body, problems, "2026-09-29T12:41:00Z", "themvf").kind, "update");
// Next day, same problems: comment again (a daily reminder), marking them still failing with first-seen times.
const next = planSweep(open.body, problems, "2026-09-30T11:07:00Z", "themvf");
assert.equal(next.kind, "comment");
if (next.kind !== "comment") throw new Error("unreachable");
assert.match(next.comment, /\*\*Still failing/);
assert.match(next.comment, /first seen/);
// Same day but a new problem appears: comment immediately.
const extra = [...problems, { key: "workflow:x.yml", group: "Scheduled jobs" as const, title: "X", detail: "failed", url: null }];
assert.equal(planSweep(open.body, extra, "2026-09-29T12:41:00Z", "themvf").kind, "comment");
// One problem resolves: comment lists it as resolved.
const fewer = planSweep(open.body, problems.slice(1), "2026-09-29T12:41:00Z", "themvf");
assert.equal(fewer.kind, "comment");
if (fewer.kind === "comment") assert.match(fewer.comment, /\*\*Resolved \(1\)\*\*/);
// All clear: close. No problems and no issue: nothing.
assert.equal(planSweep(open.body, [], "2026-09-30T11:07:00Z", "themvf").kind, "close");
assert.equal(planSweep(null, [], "2026-09-30T11:07:00Z", "themvf").kind, "none");

console.log(`Health checklist: ${items.length} synthetic items classified; unreadable sources become FAIL rows; the sweep opens, reminds daily, updates quietly, and closes.`);
