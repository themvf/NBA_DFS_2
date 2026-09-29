/**
 * Failing-job detection: a job counts as failing only when its latest finished,
 * non-pull-request run failed; a replaced (cancelled) run is not a failure; a
 * recovered job drops off; streaks and "failing since" are right; and a GitHub
 * read failure is thrown, never turned into "all clear".
 */
import assert from "node:assert/strict";
import { failingWorkflows, readFailingWorkflows, toRunLite, type WorkflowRunLite } from "../src/lib/workflow-health";

let id = 0;
const run = (workflowId: number, createdAt: string, conclusion: string | null, over: Partial<WorkflowRunLite> = {}): WorkflowRunLite =>
  ({ id: ++id, workflowId, name: `wf${workflowId}`, workflow: `wf${workflowId}.yml`, event: "schedule", status: conclusion ? "completed" : "in_progress",
    conclusion, createdAt, url: `https://github.com/x/runs/${id}`, ...over });
const byWf = (runs: WorkflowRunLite[]) => { const m = new Map<number, WorkflowRunLite[]>(); for (const r of runs) m.set(r.workflowId, [...(m.get(r.workflowId) ?? []), r]); return m; };

// 1: failed three times after a success. 2: failed then recovered. 3: latest run
// was cancelled (replaced) after a failure: the failure before it still counts.
// 4: only a pull-request run failed. 5: timed out. 6: in progress after a failure.
const result = failingWorkflows(byWf([
  run(1, "2026-09-29T01:00:00Z", "success"), run(1, "2026-09-29T02:00:00Z", "failure"), run(1, "2026-09-29T03:00:00Z", "failure"), run(1, "2026-09-29T04:00:00Z", "failure"),
  run(2, "2026-09-29T01:00:00Z", "failure"), run(2, "2026-09-29T02:00:00Z", "success"),
  run(3, "2026-09-29T01:00:00Z", "failure"), run(3, "2026-09-29T02:00:00Z", "cancelled"),
  run(4, "2026-09-29T01:00:00Z", "success"), run(4, "2026-09-29T02:00:00Z", "failure", { event: "pull_request" }),
  run(5, "2026-09-29T05:00:00Z", "timed_out"),
  run(6, "2026-09-29T01:00:00Z", "failure"), run(6, "2026-09-29T02:00:00Z", null),
]));
const w = (n: number) => result.find((f) => f.workflow === `wf${n}.yml`);
assert.equal(w(1)?.streak, 3); assert.equal(w(1)?.streakCapped, false);
assert.equal(w(1)?.failingSince, "2026-09-29T02:00:00Z"); assert.equal(w(1)?.lastSuccessAt, "2026-09-29T01:00:00Z");
assert.equal(w(2), undefined, "a recovered job is not failing");
assert.equal(w(3)?.streak, 1, "a replaced run does not hide the failure before it");
assert.equal(w(4), undefined, "pull-request runs never count");
assert.equal(w(5)?.streakCapped, true, "every run read failed: the streak may be longer");
assert.equal(w(6)?.failedAt, "2026-09-29T01:00:00Z", "a run still in progress does not clear the last failure");
assert.equal(result[0].workflow, "wf5.yml", "newest failure first");

// The workflow file name comes from GitHub's path field.
assert.equal(toRunLite({ id: 1, workflow_id: 2, path: ".github/workflows/refresh_tennis.yml", status: "completed", conclusion: "failure" }).workflow, "refresh_tennis.yml");

(async () => {
  // A GitHub error is an exception for the caller to report, never an empty list.
  const denied = (async () => new Response("no", { status: 403 })) as unknown as typeof fetch;
  await assert.rejects(readFailingWorkflows("t", { fetchImpl: denied }), /GitHub answered 403/);
  // End to end with a fake GitHub: one failing scheduled job, one PR-only failure.
  const fake = (async (url: string) => {
    if (url.includes("/actions/runs?status=failure")) return Response.json({ workflow_runs: [
      { id: 1, workflow_id: 7, path: ".github/workflows/a.yml", name: "A", event: "schedule", status: "completed", conclusion: "failure", created_at: "2026-09-29T03:00:00Z", html_url: "u1" },
      { id: 2, workflow_id: 8, path: ".github/workflows/tests.yml", name: "Tests", event: "pull_request", status: "completed", conclusion: "failure", created_at: "2026-09-29T03:00:00Z", html_url: "u2" }] });
    if (url.includes("/actions/runs?status=")) return Response.json({ workflow_runs: [] });
    if (url.includes("/workflows/7/runs")) return Response.json({ workflow_runs: [
      { id: 1, workflow_id: 7, path: ".github/workflows/a.yml", name: "A", event: "schedule", status: "completed", conclusion: "failure", created_at: "2026-09-29T03:00:00Z", html_url: "u1" },
      { id: 0, workflow_id: 7, path: ".github/workflows/a.yml", name: "A", event: "schedule", status: "completed", conclusion: "success", created_at: "2026-09-29T02:00:00Z", html_url: "u0" }] });
    throw new Error(`unexpected ${url}`);
  }) as unknown as typeof fetch;
  // A GitHub that never answers is reported as a timeout, not as "nothing failing".
  const hang = ((_u: string, init: RequestInit) => new Promise((_r, reject) => init.signal?.addEventListener("abort", () =>
    reject(Object.assign(new Error("aborted"), { name: "TimeoutError" }))))) as unknown as typeof fetch;
  // (A ref'd timer keeps Node alive while the unref'd abort timer runs.)
  const keepAlive = setTimeout(() => {}, 10_000);
  await assert.rejects(readFailingWorkflows("t", { fetchImpl: hang, timeoutMs: 50 }), /did not answer within 0.05s/);
  clearTimeout(keepAlive);
  const live = await readFailingWorkflows("t", { fetchImpl: fake });
  assert.deepEqual(live.map((f) => f.workflow), ["a.yml"], "the PR-only failure is not read further");
})().then(() => console.log("Workflow health: failing, recovered, replaced, PR-only, timed-out and in-progress runs classified; GitHub errors surface."))
  .catch((e) => { console.error(e); process.exit(1); });
