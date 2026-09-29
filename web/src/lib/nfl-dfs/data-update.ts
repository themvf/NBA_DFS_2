/**
 * "Update data": pull the latest injuries, depth charts, projections and
 * DraftKings statuses before building, instead of waiting for the schedule.
 *
 * The button dispatches the same GitHub workflows the schedule runs
 * (src/lib/cron-dispatch.ts), follows the exact runs GitHub reports back, and
 * says in plain words what each is doing. The data is global, not per slate,
 * so one update serves every slate; a second press while one is running
 * follows the running one instead of starting another.
 *
 * Pure: the server reads GitHub and the database, this decides what to say.
 */

export type DataUpdateJobKey = "availability" | "dk_status";

export interface DataUpdateJobSpec {
  key: DataUpdateJobKey;
  label: string;
  workflow: string;
  inputs?: Record<string, string>;
  /** Typical wall-clock minutes, from recent runs; shown while it runs. */
  typicalMinutes: string;
}

export const DATA_UPDATE_JOBS: readonly DataUpdateJobSpec[] = [
  {
    key: "availability",
    label: "Injuries, depth charts and projections",
    workflow: "refresh_nfl_availability_context.yml",
    // Capture even between the job's own scheduled slots.
    inputs: { force: "true" },
    typicalMinutes: "2-10",
  },
  {
    key: "dk_status",
    label: "DraftKings player statuses",
    workflow: "refresh_nfl_dk_pool.yml",
    typicalMinutes: "1",
  },
];

export type DataUpdateJobStatus = "dispatch_failed" | "queued" | "in_progress" | "completed";

export interface DataUpdateJob {
  key: DataUpdateJobKey;
  label: string;
  workflow: string;
  runId: number | null;
  htmlUrl: string | null;
  status: DataUpdateJobStatus;
  /** GitHub's conclusion once completed: success, failure, cancelled, ... */
  conclusion: string | null;
  /** Why the dispatch failed, or that a newer run replaced this one. */
  note: string | null;
  dispatchedAt: string;
  completedAt: string | null;
}

export interface DataUpdate {
  id: string;
  uploadId: string | null;
  requestedAt: string;
  finishedAt: string | null;
  jobs: DataUpdateJob[];
}

/** A run older than this that has not finished is treated as stuck. */
export const DATA_UPDATE_STALE_MS = 30 * 60 * 1000;

export type DataUpdateState = "running" | "succeeded" | "failed" | "stuck";

export interface DataUpdateView {
  state: DataUpdateState;
  headline: string;
  lines: { key: DataUpdateJobKey; label: string; state: "waiting" | "running" | "done" | "failed"; text: string; href: string | null }[];
}

const jobState = (job: DataUpdateJob): DataUpdateView["lines"][number]["state"] => {
  if (job.status === "dispatch_failed") return "failed";
  if (job.status === "completed") return job.conclusion === "success" || job.conclusion === "untracked" ? "done" : "failed";
  return job.status === "in_progress" ? "running" : "waiting";
};

/** GitHub run status, reduced to the three we display. */
export function toJobStatus(status: string): Exclude<DataUpdateJobStatus, "dispatch_failed"> {
  if (status === "completed") return "completed";
  if (status === "in_progress") return "in_progress";
  return "queued"; // queued, requested, waiting, pending
}

export function describeDataUpdate(update: DataUpdate, now: number): DataUpdateView {
  const specs = new Map(DATA_UPDATE_JOBS.map((j) => [j.key, j]));
  const lines = update.jobs.map((job) => {
    const state = jobState(job);
    const typical = specs.get(job.key)?.typicalMinutes;
    const text = state === "done" ? (job.conclusion === "untracked" ? (job.note ?? "Started.") : "Done.")
      : state === "failed" ? (job.status === "dispatch_failed" ? `Could not start: ${job.note ?? "GitHub refused the request."}`
        : `Failed (${job.conclusion ?? "unknown"}). The slate keeps the last good data.`)
      : state === "running" ? `Running${typical ? `; usually ${typical} min` : ""}.${job.note ? ` ${job.note}` : ""}`
      : `Waiting to start${job.note ? `. ${job.note}` : "; another run of this job may be finishing first."}`;
    return { key: job.key, label: job.label, state, text, href: job.htmlUrl };
  });
  const failed = lines.filter((l) => l.state === "failed");
  const open = lines.filter((l) => l.state === "waiting" || l.state === "running");
  if (!open.length) {
    return failed.length
      ? { state: "failed", headline: `${failed.map((l) => l.label).join(" and ")} failed. The slate keeps the last good data.`, lines }
      : { state: "succeeded", headline: "Data updated.", lines };
  }
  if (now - Date.parse(update.requestedAt) > DATA_UPDATE_STALE_MS) {
    return { state: "stuck", headline: "This update is taking much longer than usual. Open the run to see why, or start a new update.", lines };
  }
  return { state: "running", headline: `Updating data: ${open.length} of ${lines.length} job${lines.length === 1 ? "" : "s"} still running.`, lines };
}

/** Whether a new update may start now; null when it may, otherwise why not. */
export function dataUpdateBlockedReason(input: { firstKickoff: string | null; now: number; tokenConfigured: boolean }): string | null {
  if (input.firstKickoff && Date.parse(input.firstKickoff) <= input.now) return "Games have started, so there is nothing left to update for this slate.";
  if (!input.tokenConfigured) return "Updating data needs the GitHub dispatch token, which only the live site has.";
  return null;
}

/** The running update to follow instead of starting another, if any. */
export function runningUpdate(latest: DataUpdate | null, now: number): DataUpdate | null {
  if (!latest || latest.finishedAt) return null;
  return now - Date.parse(latest.requestedAt) <= DATA_UPDATE_STALE_MS ? latest : null;
}

/**
 * Pick the run to follow after ours was cancelled: GitHub keeps one pending
 * run per concurrency group, so a scheduled dispatch arriving while ours
 * waited replaces it. That newer run does the same work, so follow it.
 */
export function replacementRun<T extends { id: number; conclusion: string | null; createdAt: string }>(
  cancelledRunId: number, runs: readonly T[]): T | null {
  const cancelled = runs.find((r) => r.id === cancelledRunId);
  const after = runs.filter((r) => r.id !== cancelledRunId && r.conclusion !== "cancelled"
    && (!cancelled || r.createdAt >= cancelled.createdAt));
  return after.length ? after[after.length - 1] : null;
}

export interface DataAsOf {
  roster: string | null;
  dkStatuses: string | null;
  projections: string | null;
}

const clock = (iso: string | null) => {
  if (!iso) return null;
  const t = new Date(iso);
  return Number.isFinite(t.getTime())
    ? t.toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit" }) + " ET"
    : null;
};

/** "Depth charts Sun 11:30 AM ET · DraftKings statuses ... · Projections ..." */
export function describeDataAsOf(asOf: DataAsOf): string {
  return [
    `Depth charts ${clock(asOf.roster) ?? "unknown"}`,
    `DraftKings statuses ${clock(asOf.dkStatuses) ?? "not checked"}`,
    `Projections ${clock(asOf.projections) ?? "unknown"}`,
  ].join(" · ");
}
