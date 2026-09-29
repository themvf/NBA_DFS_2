import { NextRequest, NextResponse } from "next/server";
import { runScheduledSlateChecks } from "@/app/dfs/nfl/actions";

// The scheduled Slate Check (C2 in docs/nfl-dfs-reliability-program.md): every
// half hour, run the NFL DFS Slate Check on each upcoming saved slate and
// record it, so the slate picker shows what needs attention before a slate is
// opened. Reads only; the one write is the recorded check.
//
// Vercel sends `Authorization: Bearer <CRON_SECRET>` (same secret as
// /api/cron/dispatch).

export const dynamic = "force-dynamic";
export const maxDuration = 300;

export async function GET(request: NextRequest) {
  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    console.error("nfl slate check: CRON_SECRET is not readable by this deployment");
    return NextResponse.json({ ok: false, error: "CRON_SECRET is not configured in this deployment" }, { status: 500 });
  }
  if (request.headers.get("authorization") !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }
  const startedAt = new Date();
  const results = await runScheduledSlateChecks(startedAt.getTime());
  const failed = results.filter((r) => r.error);
  for (const r of failed) console.error(`nfl slate check: ${r.uploadId} failed: ${r.error}`);
  return NextResponse.json({ ok: failed.length === 0, at: startedAt.toISOString(), slates: results }, { status: failed.length ? 500 : 200 });
}
