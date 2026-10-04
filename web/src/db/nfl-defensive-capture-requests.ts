import 'server-only';
import { sql } from 'drizzle-orm';
import { db } from '@/db';
import { dispatchWorkflow } from '@/lib/cron-dispatch';
import type { CaptureRequestRow } from '@/lib/nfl-dfs/defensive-capture-status';

/**
 * Opponent-capture requests for a salary upload (table owned jointly with
 * ingest/nfl_defensive_capture_requests.py; created by ensureNflDfsTables).
 * Spec: docs/nfl-dfs-freshness-capture-on-upload-spec.md.
 */
export const DEFENSIVE_CAPTURE_PROFILES = ['pfr-efficiency', 'allowed-rushing-volume'] as const;
export const DEFENSIVE_CAPTURE_WORKFLOW = 'capture_nfl_defensive_on_upload.yml';

export async function readDefensiveCaptureRequests(uploadId: string, projectionRunId: string): Promise<CaptureRequestRow[]> {
  const rows = await db.execute(sql`SELECT profile, state, captured_players, last_error, dispatch_error, worker_run_url, updated_at
    FROM nfl_dfs_defensive_capture_requests WHERE upload_id=${uploadId}::uuid AND projection_run_id=${projectionRunId}::uuid`);
  return rows.rows.map((r) => ({
    profile: String(r.profile) as CaptureRequestRow['profile'], state: String(r.state) as CaptureRequestRow['state'],
    capturedPlayers: r.captured_players == null ? null : Number(r.captured_players),
    lastError: r.last_error == null ? null : String(r.last_error), dispatchError: r.dispatch_error == null ? null : String(r.dispatch_error),
    workerRunUrl: r.worker_run_url == null ? null : String(r.worker_run_url),
    updatedAt: r.updated_at == null ? null : new Date(String(r.updated_at)).toISOString(),
  }));
}

/** Insert one pending request per profile; existing ones are left alone. Returns how many were new. */
async function enqueue(uploadId: string, projectionRunId: string): Promise<number> {
  // One statement, so both profiles land together or neither does.
  const rows = await db.execute(sql`INSERT INTO nfl_dfs_defensive_capture_requests (upload_id, projection_run_id, profile)
    VALUES (${uploadId}::uuid, ${projectionRunId}::uuid, 'pfr-efficiency'), (${uploadId}::uuid, ${projectionRunId}::uuid, 'allowed-rushing-volume')
    ON CONFLICT (upload_id, projection_run_id, profile) DO NOTHING RETURNING request_id`);
  return rows.rows.length;
}

/** Put failed requests back in the queue (a manual retry). Returns how many. */
async function requeueFailed(uploadId: string, projectionRunId: string): Promise<number> {
  const rows = await db.execute(sql`UPDATE nfl_dfs_defensive_capture_requests
    SET state='pending', attempts=0, last_error=NULL, dispatch_error=NULL, lease_until=NULL, finished_at=NULL, updated_at=NOW()
    WHERE upload_id=${uploadId}::uuid AND projection_run_id=${projectionRunId}::uuid AND state='failed' RETURNING request_id`);
  return rows.rows.length;
}

async function recordDispatch(uploadId: string, projectionRunId: string, error: string | null): Promise<void> {
  await db.execute(sql`UPDATE nfl_dfs_defensive_capture_requests
    SET dispatched_at=NOW(), dispatch_error=${error}, updated_at=NOW()
    WHERE upload_id=${uploadId}::uuid AND projection_run_id=${projectionRunId}::uuid AND state='pending'`);
}

export interface CaptureRequestOutcome { requested: number; dispatched: boolean; error: string | null }

/**
 * Request captures for an upload and start the worker. Never throws: a salary
 * upload must not fail because the capture could not be queued or started.
 * A queued request whose dispatch failed is retried by the 15-minute cron.
 */
export async function requestDefensiveCaptures(uploadId: string, projectionRunId: string,
  options: { retry?: boolean } = {}): Promise<CaptureRequestOutcome> {
  let requested = 0;
  try {
    requested = options.retry ? await requeueFailed(uploadId, projectionRunId) + await enqueue(uploadId, projectionRunId)
      : await enqueue(uploadId, projectionRunId);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`NFL opponent capture: could not queue upload ${uploadId}`, error);
    return { requested: 0, dispatched: false, error: `couldn't queue the capture: ${message}` };
  }
  if (!requested) return { requested, dispatched: false, error: null };
  const token = process.env.GITHUB_DISPATCH_TOKEN;
  const outcome = token
    ? await dispatchWorkflow({ key: 'nfl-defensive-capture', workflow: DEFENSIVE_CAPTURE_WORKFLOW, inputs: { upload_id: uploadId } }, token)
    : { ok: false, status: 0, detail: 'GITHUB_DISPATCH_TOKEN is not configured on this deployment' };
  const error = outcome.ok ? null : `dispatch failed (${outcome.status}): ${outcome.detail ?? 'no detail'}`;
  if (error) console.error(`NFL opponent capture: ${error} for upload ${uploadId}`);
  try { await recordDispatch(uploadId, projectionRunId, error); }
  catch (recordError) { console.error('NFL opponent capture: could not record the dispatch', recordError); }
  return { requested, dispatched: outcome.ok, error };
}
