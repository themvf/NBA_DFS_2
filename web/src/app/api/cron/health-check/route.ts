import { NextRequest, NextResponse } from "next/server";
import { recordObservation, withHeartbeat } from "@/lib/cron-heartbeat";
import { collectHealth, OBS_DEPLOYED_COMMIT, storeHealth } from "@/lib/health-collector";

// The /health checklist, every 30 minutes (vercel.json): evaluate every
// workflow, dataset, clock, upcoming NFL slate and the NFL availability
// monitor, and store the result the page reads. The daily failure sweep runs
// the same collector and emails the FAIL rows.

export const dynamic = "force-dynamic";
export const maxDuration = 120;

async function handle(request: NextRequest) {
  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) return NextResponse.json({ ok: false, error: "CRON_SECRET is not configured in this deployment" }, { status: 500 });
  if (request.headers.get("authorization") !== `Bearer ${cronSecret}`) return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  const now = new Date();
  // Only the deployment knows which commit it runs; record it for every checker.
  const deployed = process.env.VERCEL_GIT_COMMIT_SHA;
  if (deployed) await recordObservation(OBS_DEPLOYED_COMMIT, deployed);
  const items = await collectHealth({ githubToken: process.env.GITHUB_DISPATCH_TOKEN || null, now });
  await storeHealth(items, now);
  const fail = items.filter((i) => i.status === "fail");
  // The run itself succeeded even when it found failures; they live in the checklist.
  return NextResponse.json({ ok: true, at: now.toISOString(), items: items.length, fail: fail.length, failing: fail.map((i) => i.key) });
}

// Every run records its outcome for /health (lib/cron-heartbeat): Vercel's own
// logs are not visible from here, so a failing or stopped cron would be silent.
export async function GET(request: NextRequest) {
  return withHeartbeat("health-check", () => handle(request));
}
