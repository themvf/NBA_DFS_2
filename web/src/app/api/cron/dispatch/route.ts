import { NextRequest, NextResponse } from "next/server";
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

export async function GET(request: NextRequest) {
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
  const jobs = dueJobs(now, await dispatchContext(now));
  if (!jobs.length) return NextResponse.json({ ok: true, at: now.toISOString(), dispatched: [], skipped: "no job due on this tick" });

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
  const failed = results.filter((r) => !r.ok);
  return NextResponse.json({ ok: failed.length === 0, at: now.toISOString(), dispatched: results }, { status: failed.length ? 502 : 200 });
}

async function dispatchContext(now: Date): Promise<DispatchContext> {
  try {
    const rows = await db.execute(sql`SELECT kickoff FROM nfl_season_games
      WHERE kickoff > ${now.toISOString()}::timestamptz AND kickoff <= ${new Date(now.getTime() + NEAR_KICKOFF_MS).toISOString()}::timestamptz`);
    return { nflKickoffs: rows.rows.map((row) => new Date(String(row.kickoff))).filter((d) => Number.isFinite(d.getTime())) };
  } catch (error) {
    console.error("cron dispatch: could not read NFL kickoffs; near-kickoff ticks skipped", error);
    return { nflKickoffs: null };
  }
}
