/**
 * "Update data": each state reads as one plain sentence, a failed or stuck job
 * is never reported as success, the button closes at kickoff, a second press
 * follows the running update, and a queued run replaced by a scheduled one is
 * followed rather than reported as cancelled.
 */
import assert from "node:assert/strict";
import {
  DATA_UPDATE_JOBS, DATA_UPDATE_STALE_MS, dataUpdateBlockedReason, describeDataAsOf, describeDataUpdate, jobOpen, replacementRun, runningUpdate, toJobStatus,
  type DataUpdate, type DataUpdateJob,
} from "../src/lib/nfl-dfs/data-update";

const t0 = Date.parse("2026-10-04T15:30:00Z");
const job = (key: DataUpdateJob["key"], over: Partial<DataUpdateJob> = {}): DataUpdateJob => {
  const spec = DATA_UPDATE_JOBS.find((j) => j.key === key)!;
  return { key, label: spec.label, workflow: spec.workflow, runId: 1, htmlUrl: "https://github.com/run/1", status: "queued",
    conclusion: null, note: null, dispatchedAt: new Date(t0).toISOString(), completedAt: null, ...over };
};
const update = (jobs: DataUpdateJob[], over: Partial<DataUpdate> = {}): DataUpdate =>
  ({ id: "u1", uploadId: null, requestedAt: new Date(t0).toISOString(), finishedAt: null, jobs, ...over });

// The jobs are the ones the schedule runs.
assert.deepEqual(DATA_UPDATE_JOBS.map((j) => j.workflow), ["refresh_nfl_availability_context.yml", "refresh_nfl_dk_pool.yml"]);
assert.equal(DATA_UPDATE_JOBS[0].inputs?.force, "true", "the injury capture runs even between its scheduled slots");

// Running.
const running = describeDataUpdate(update([job("availability", { status: "in_progress" }), job("dk_status", { status: "completed", conclusion: "success" })]), t0 + 60_000);
assert.equal(running.state, "running");
assert.equal(running.headline, "Updating data: 1 of 2 jobs still running.");
assert.match(running.lines[0].text, /^Running; usually 2-10 min\.$/);
assert.equal(running.lines[1].text, "Done.");

// Success.
const done = describeDataUpdate(update([job("availability", { status: "completed", conclusion: "success" }), job("dk_status", { status: "completed", conclusion: "success" })]), t0 + 600_000);
assert.equal(done.state, "succeeded"); assert.equal(done.headline, "Data updated.");

// A failure is never folded into success.
const failed = describeDataUpdate(update([job("availability", { status: "completed", conclusion: "failure" }), job("dk_status", { status: "completed", conclusion: "success" })]), t0 + 600_000);
assert.equal(failed.state, "failed");
assert.equal(failed.headline, "Injuries, depth charts and projections failed. The slate keeps the last good data.");
const refused = describeDataUpdate(update([job("availability", { status: "dispatch_failed", runId: null, note: "GitHub answered 401." }), job("dk_status", { status: "completed", conclusion: "success" })]), t0);
assert.equal(refused.state, "failed");
assert.match(refused.lines[0].text, /^Could not start: GitHub answered 401\.$/);

// A dispatch GitHub accepted without naming its run is neither failed nor done:
// it has its own state and is never reported as "Data updated" (2026-09-29 audit).
const untracked = describeDataUpdate(update([job("availability", { status: "untracked", runId: null }), job("dk_status", { status: "completed", conclusion: "success" })]), t0);
assert.equal(untracked.state, "untracked");
assert.notEqual(untracked.headline, "Data updated.");
assert.match(untracked.headline, /^Injuries, depth charts and projections started, but can't be followed from here, so the new data may not be in yet\. Check back in a few minutes\.$/);
assert.equal(untracked.lines[0].state, "untracked");
assert.match(untracked.lines[0].text, /^Started, but GitHub didn't say which run it is, so its progress can't be followed here; it usually takes 2-10 min\. Check back in a few minutes\.$/);
assert.equal(untracked.lines[1].state, "done");
// Rows saved before the fix (completed + conclusion "untracked") read the same.
const legacy = describeDataUpdate(update([job("availability", { status: "completed", conclusion: "untracked", runId: null }), job("dk_status", { status: "completed", conclusion: "success" })]), t0);
assert.equal(legacy.state, "untracked");
// While a followable job still runs, the update is running; an untracked job never holds it open.
assert.equal(describeDataUpdate(update([job("availability", { status: "untracked", runId: null }), job("dk_status", { status: "in_progress" })]), t0 + 60_000).state, "running");
assert.equal(jobOpen(job("availability", { status: "untracked", runId: null })), false);
assert.equal(jobOpen(job("availability", { status: "queued" })), true);
// A failure still outranks an untracked job.
assert.equal(describeDataUpdate(update([job("availability", { status: "untracked", runId: null }), job("dk_status", { status: "completed", conclusion: "failure" })]), t0).state, "failed");

// Stuck: still open after 30 minutes.
const stuck = describeDataUpdate(update([job("availability", { status: "queued" })]), t0 + DATA_UPDATE_STALE_MS + 1);
assert.equal(stuck.state, "stuck");
assert.match(stuck.headline, /much longer than usual/);

// The button: closed at kickoff, and without the token.
assert.equal(dataUpdateBlockedReason({ firstKickoff: "2026-10-04T17:00:00Z", now: t0, tokenConfigured: true }), null);
assert.match(dataUpdateBlockedReason({ firstKickoff: "2026-10-04T15:00:00Z", now: t0, tokenConfigured: true })!, /Games have started/);
assert.match(dataUpdateBlockedReason({ firstKickoff: null, now: t0, tokenConfigured: false })!, /only the live site has/);

// A second press follows the running update; a finished or abandoned one does not block.
const open = update([job("availability")]);
assert.equal(runningUpdate(open, t0 + 60_000)?.id, "u1");
assert.equal(runningUpdate({ ...open, finishedAt: new Date(t0).toISOString() }, t0 + 60_000), null);
assert.equal(runningUpdate(open, t0 + DATA_UPDATE_STALE_MS + 1), null);
assert.equal(runningUpdate(null, t0), null);

// A queued run GitHub replaced with a newer scheduled one: follow the newer.
const runs = [
  { id: 10, conclusion: "cancelled", createdAt: "2026-10-04T15:30:01Z" },
  { id: 11, conclusion: null, createdAt: "2026-10-04T15:37:02Z" },
];
assert.equal(replacementRun(10, runs)?.id, 11);
assert.equal(replacementRun(10, [runs[0]]), null, "nothing replaced it: stay cancelled");
assert.equal(replacementRun(10, [{ id: 9, conclusion: "success", createdAt: "2026-10-04T15:00:00Z" }, runs[0]]), null, "an older run is not a replacement");

// Run status reduction.
assert.equal(toJobStatus("waiting"), "queued"); assert.equal(toJobStatus("in_progress"), "in_progress"); assert.equal(toJobStatus("completed"), "completed");

// Data as of.
assert.equal(describeDataAsOf({ roster: "2026-10-04T15:07:00Z", dkStatuses: null, projections: "2026-10-04T15:12:00Z" }),
  "Depth charts Sun 11:07 AM ET · DraftKings statuses not checked · Projections Sun 11:12 AM ET");

console.log("Data update: running, done, failed, refused, stuck and started-but-unfollowable each read plainly; an unfollowable job is never reported as done; closed at kickoff; a replaced run is followed.");
