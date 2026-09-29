/**
 * Server side of "Update data" (see lib/nfl-dfs/data-update). Called only
 * through ./safe-actions, so a failure reaches the page as a real message.
 *
 * Starting an update dispatches the scheduled workflows now and records the
 * exact runs GitHub reports back. Reading one follows those runs; when GitHub
 * cancels a queued run because a scheduled dispatch replaced it, the newer run
 * is followed instead, since it does the same work.
 */
import { randomUUID } from "node:crypto";
import { eq, sql } from "drizzle-orm";
import { db } from "@/db";
import { ensureNflDfsTables } from "@/db/ensure-schema";
import { nflDfsSlatePlayers, nflDfsSlateUploads } from "@/db/schema";
import { dispatchWorkflow, listDispatchedRuns, readWorkflowRun } from "@/lib/cron-dispatch";
import {
  DATA_UPDATE_JOBS, dataUpdateBlockedReason, describeDataUpdate, replacementRun, runningUpdate, toJobStatus,
  type DataUpdate, type DataUpdateJob, type DataUpdateView,
} from "@/lib/nfl-dfs/data-update";
import { parseDkGameInfoKickoff } from "@/lib/nfl-dfs/workspace-stage";

export interface NflDataUpdateResult {
  update: DataUpdate | null;
  view: DataUpdateView | null;
  /** Null when a new update may start; otherwise why not. */
  blockedReason: string | null;
}

const token = () => process.env.GITHUB_DISPATCH_TOKEN || null;

function rowToUpdate(row: Record<string, unknown>): DataUpdate {
  return {
    id: String(row.id),
    uploadId: row.upload_id == null ? null : String(row.upload_id),
    requestedAt: new Date(String(row.requested_at)).toISOString(),
    finishedAt: row.finished_at == null ? null : new Date(String(row.finished_at)).toISOString(),
    jobs: row.jobs as DataUpdateJob[],
  };
}

async function latestUpdate(): Promise<DataUpdate | null> {
  const rows = await db.execute(sql`SELECT id, upload_id, requested_at, finished_at, jobs FROM nfl_dfs_data_updates
    ORDER BY requested_at DESC LIMIT 1`);
  return rows.rows[0] ? rowToUpdate(rows.rows[0] as Record<string, unknown>) : null;
}

async function slateFirstKickoff(uploadId: string): Promise<string | null> {
  const rows = await db.select({ gameInfo: nflDfsSlatePlayers.gameInfo }).from(nflDfsSlatePlayers)
    .where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  const times = rows.map((r) => Date.parse(parseDkGameInfoKickoff(r.gameInfo) ?? "")).filter(Number.isFinite);
  return times.length ? new Date(Math.min(...times)).toISOString() : null;
}

/** Bring an unfinished update's jobs up to date with GitHub, and save it. */
async function follow(update: DataUpdate, githubToken: string): Promise<DataUpdate> {
  if (update.finishedAt) return update;
  const jobs: DataUpdateJob[] = [];
  for (const job of update.jobs) {
    if (job.status === "completed" || job.status === "dispatch_failed" || job.runId == null) { jobs.push(job); continue; }
    try {
      const run = await readWorkflowRun(job.runId, githubToken);
      let next: DataUpdateJob = { ...job, htmlUrl: run.htmlUrl, status: toJobStatus(run.status), conclusion: run.conclusion,
        completedAt: run.status === "completed" ? run.updatedAt : null };
      if (run.status === "completed" && run.conclusion === "cancelled") {
        const replacement = replacementRun(run.id, await listDispatchedRuns(job.workflow, new Date(job.dispatchedAt), githubToken));
        if (replacement) {
          next = { ...next, runId: replacement.id, htmlUrl: replacement.htmlUrl, status: toJobStatus(replacement.status),
            conclusion: replacement.conclusion, completedAt: replacement.status === "completed" ? replacement.updatedAt : null,
            note: "A scheduled run of the same job replaced this one; following it." };
        }
      }
      jobs.push(next);
    } catch (error) {
      // A GitHub read failure is not a job failure: keep the last known state.
      console.error(`NFL data update: could not read run ${job.runId}`, error);
      jobs.push(job);
    }
  }
  const finished = jobs.every((j) => j.status === "completed" || j.status === "dispatch_failed");
  const next: DataUpdate = { ...update, jobs, finishedAt: finished ? new Date().toISOString() : null };
  await db.execute(sql`UPDATE nfl_dfs_data_updates SET jobs=${JSON.stringify(next.jobs)}::jsonb,
    finished_at=${next.finishedAt}::timestamptz WHERE id=${next.id}::uuid`);
  return next;
}

/** The latest update (running or finished), followed up to now. */
export async function readNflDataUpdate(uploadId?: string | null): Promise<NflDataUpdateResult> {
  await ensureNflDfsTables();
  const githubToken = token();
  let update = await latestUpdate();
  if (update && !update.finishedAt && githubToken) update = await follow(update, githubToken);
  const now = Date.now();
  const firstKickoff = uploadId && /^[0-9a-f-]{36}$/.test(uploadId) ? await slateFirstKickoff(uploadId) : null;
  return {
    update,
    view: update ? describeDataUpdate(update, now) : null,
    blockedReason: dataUpdateBlockedReason({ firstKickoff, now, tokenConfigured: Boolean(githubToken) }),
  };
}

/** Start an update now, or follow the one already running. */
export async function startNflDataUpdate(uploadId: string): Promise<NflDataUpdateResult> {
  if (!/^[0-9a-f-]{36}$/.test(uploadId)) throw new Error("Invalid saved slate.");
  await ensureNflDfsTables();
  const [upload] = await db.select({ id: nflDfsSlateUploads.uploadId }).from(nflDfsSlateUploads)
    .where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1);
  if (!upload) throw new Error("Saved salary slate not found.");
  const githubToken = token();
  const blocked = dataUpdateBlockedReason({ firstKickoff: await slateFirstKickoff(uploadId), now: Date.now(), tokenConfigured: Boolean(githubToken) });
  if (blocked) throw new Error(blocked);
  return beginNflDataUpdate(uploadId, githubToken!);
}

/**
 * Dispatch the update's workflows, or follow the update already running.
 * Checks nothing about the slate: `startNflDataUpdate` is the only caller the
 * page reaches (this module is not a server action), and it checks first.
 */
export async function beginNflDataUpdate(uploadId: string, githubToken: string): Promise<NflDataUpdateResult> {
  await ensureNflDfsTables();
  const now = Date.now();
  const running = runningUpdate(await latestUpdate(), now);
  if (running) {
    const followed = await follow(running, githubToken);
    return { update: followed, view: describeDataUpdate(followed, Date.now()), blockedReason: null };
  }

  const jobs: DataUpdateJob[] = [];
  for (const spec of DATA_UPDATE_JOBS) {
    const dispatchedAt = new Date().toISOString();
    const outcome = await dispatchWorkflow({ key: spec.key, workflow: spec.workflow, inputs: spec.inputs }, githubToken, fetch, { returnRunDetails: true });
    jobs.push({
      key: spec.key, label: spec.label, workflow: spec.workflow,
      runId: outcome.runId ?? null, htmlUrl: outcome.htmlUrl ?? null,
      status: !outcome.ok ? "dispatch_failed" : "queued", conclusion: null,
      note: !outcome.ok ? `GitHub answered ${outcome.status || "no response"}${outcome.detail ? `: ${outcome.detail.slice(0, 160)}` : ""}.`
        : outcome.runId == null ? "Started, but GitHub did not name the run; check the Actions page." : null,
      dispatchedAt, completedAt: null,
    });
  }
  // A dispatch GitHub accepted without naming the run cannot be followed; it
  // counts as finished here so the update does not wait on it forever.
  const tracked = jobs.map((j) => j.status === "queued" && j.runId == null ? { ...j, status: "completed" as const, conclusion: "untracked" } : j);
  const update: DataUpdate = {
    id: randomUUID(), uploadId, requestedAt: new Date(now).toISOString(),
    finishedAt: tracked.every((j) => j.status !== "queued") ? new Date().toISOString() : null, jobs: tracked,
  };
  await db.execute(sql`INSERT INTO nfl_dfs_data_updates (id, upload_id, requested_at, finished_at, jobs)
    VALUES (${update.id}::uuid, ${uploadId}::uuid, ${update.requestedAt}::timestamptz, ${update.finishedAt}::timestamptz, ${JSON.stringify(update.jobs)}::jsonb)`);
  return { update, view: describeDataUpdate(update, Date.now()), blockedReason: null };
}
