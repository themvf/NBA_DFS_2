import { NextRequest, NextResponse } from "next/server";
import { recordObservation, withHeartbeat } from "@/lib/cron-heartbeat";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { dispatchWorkflow, dueJobs, NEAR_KICKOFF_MS, type DispatchContext } from "@/lib/cron-dispatch";

// The one Vercel Cron -> GitHub Actions bridge. Vercel calls this every 15
// minutes (see vercel.json); src/lib/cron-dispatch.ts decides which workflows
// are due on this tick and this route fires them. The only thing read here is
// the next few NFL kickoffs, so the availability jobs can run every tick in
// the two hours before one; if that read fails the half-hour cadence runs. It replaced three
// single-workflow routes (mlb-odds-capture, nfl-dk-pool, nfl-projections) on
// 2026-09-26 so that adding a bridged workflow is one table entry.
//
// Required Vercel project env vars (Production + Preview):
//   CRON_SECRET            - Vercel sends `Authorization: Bearer <CRON_SECRET>`
//   GITHUB_DISPATCH_TOKEN  - fine-grained PAT, this repo only, Actions: read/write

export const dynamic = "force-dynamic";
export const maxDuration = 30;

async function handle(request: NextRequest) {
  // Distinguish "secret missing from this deployment" (a build/config problem
  // to surface in logs) from "wrong caller" (a rejection). Env vars are baked
  // in at build time, so a newly added variable needs a redeploy.
  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    console.error("cron dispatch: CRON_SECRET is not readable by this deployment; set it for Production and redeploy.");
    return NextResponse.json({ ok: false, error: "CRON_SECRET is not configured in this deployment" }, { status: 500 });
  }
  const authHeader = request.headers.get("authorization");
  if (authHeader !== `Bearer ${cronSecret}`) {
    // Never log either value, only whether a header arrived.
    console.error(`cron dispatch: rejected request (authorization header ${authHeader ? "present but non-matching" : "missing"})`);
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  const now = new Date();
  const context = await dispatchContext(now);
  // A fallback that only logs is invisible: the response (and so the heartbeat
  // detail on /health) says when this tick ran without the kickoff schedule.
  const warnings = context.nflKickoffs === null ? ["NFL kickoffs could not be read; near-kickoff quarter-hour ticks were skipped on this tick"] : [];
  const warned = warnings.length ? { warnings } : {};
  const jobs = dueJobs(now, context);
  if (!jobs.length) return NextResponse.json({ ok: true, at: now.toISOString(), ...warned, dispatched: [], skipped: "no job due on this tick" });

  const token = process.env.GITHUB_DISPATCH_TOKEN;
  if (!token) {
    console.error("cron dispatch: GITHUB_DISPATCH_TOKEN is not configured");
    return NextResponse.json({ ok: false, error: "GITHUB_DISPATCH_TOKEN is not configured", due: jobs.map((j) => j.key) }, { status: 500 });
  }

  // Sequential: GitHub rate-limits dispatches, and four calls take under a second.
  const results = [];
  for (const job of jobs) {
    const outcome = await dispatchWorkflow(job, token);
    if (!outcome.ok) console.error(`cron dispatch: ${job.key} (${job.workflow}) failed (${outcome.status}): ${outcome.detail ?? ""}`);
    results.push(outcome);
  }
  // The token's expiry rides on every GitHub response. It goes into the JSON
  // ahead of the (long) outcome list so it survives the heartbeat's 500-char
  // detail, and is recorded as an observation the checklist reads from any
  // process (the daily sweep never holds this token).
  const tokenExpiresAt = results.find((r) => r.tokenExpiresAt)?.tokenExpiresAt ?? null;
  if (tokenExpiresAt) await recordObservation("github-dispatch-token", tokenExpiresAt);
  const failed = results.filter((r) => !r.ok);
  return NextResponse.json({ ok: failed.length === 0, at: now.toISOString(), tokenExpiresAt, ...warned, dispatched: results }, { status: failed.length ? 502 : 200 });
}

async function dispatchContext(now: Date): Promise<DispatchContext> {
  return { nflKickoffs: await nflKickoffs(now), nflDefensiveCapturesPending: await nflDefensiveCapturesPending(now) };
}

async function nflKickoffs(now: Date): Promise<Date[] | null> {
  try {
    const rows = await db.execute(sql`SELECT kickoff FROM nfl_season_games
      WHERE kickoff > ${now.toISOString()}::timestamptz AND kickoff <= ${new Date(now.getTime() + NEAR_KICKOFF_MS).toISOString()}::timestamptz`);
    return rows.rows.map((row) => new Date(String(row.kickoff))).filter((d) => Number.isFinite(d.getTime()));
  } catch (error) {
    console.error("cron dispatch: could not read NFL kickoffs; near-kickoff ticks skipped", error);
    return null;
  }
}

/**
 * Whether an opponent-capture request needs a worker: pending for longer than
 * the upload's own dispatch needs to start (10 minutes), or holding an expired
 * lease. A missing table (before the first upload creates it) reads as false.
 */
async function nflDefensiveCapturesPending(now: Date): Promise<boolean | null> {
  try {
    const grace = new Date(now.getTime() - 10 * 60_000).toISOString();
    const rows = await db.execute(sql`SELECT EXISTS (SELECT 1 FROM nfl_dfs_defensive_capture_requests
      WHERE attempts < 3 AND ((state = 'pending' AND COALESCE(dispatched_at, requested_at) < ${grace}::timestamptz)
        OR (state = 'running' AND lease_until < ${now.toISOString()}::timestamptz))) AS due`);
    return rows.rows[0]?.due === true;
  } catch (error) {
    if (/does not exist/i.test(error instanceof Error ? error.message : "")) return false;
    console.error("cron dispatch: could not read opponent-capture requests; retry skipped this tick", error);
    return null;
  }
}

// Every run records its outcome for /health (lib/cron-heartbeat): Vercel's own
// logs are not visible from here, so a failing or stopped cron would be silent.
export async function GET(request: NextRequest) {
  return withHeartbeat("dispatch", () => handle(request));
}
