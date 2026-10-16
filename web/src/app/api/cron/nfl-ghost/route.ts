import { NextRequest, NextResponse } from "next/server";
import { withHeartbeat } from "@/lib/cron-heartbeat";
import { runGhostTick } from "@/lib/nfl-dfs/ghost/service";

/**
 * Ghost-lineup study tick (docs/nfl-ghost-factorial-study.md), every five
 * minutes: freeze any gradable slate entering its window, then build every
 * missing arm of every open freeze. Does nothing, cheaply, outside a window.
 * The response carries counts and ids only.
 */
export const dynamic = "force-dynamic";
export const maxDuration = 300;

async function handle(request: NextRequest) {
  const secret = process.env.CRON_SECRET;
  if (!secret) return NextResponse.json({ error: "Cron authentication is not configured" }, { status: 500 });
  if (request.headers.get("authorization") !== `Bearer ${secret}`) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  const result = await runGhostTick(secret);
  return NextResponse.json(result, { status: result.failed ? 500 : 200 });
}

export async function GET(request: NextRequest) {
  return withHeartbeat("nfl-ghost", () => handle(request));
}
