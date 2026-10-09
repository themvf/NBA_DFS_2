import { NextRequest, NextResponse } from "next/server";
import { dispatchNflPbpRefresh } from "@/lib/nfl/pbp-dispatch";

export const dynamic = "force-dynamic";

// Scripted/curl entry point for triggering the NFL PBP ingest, gated by the
// same CRON_SECRET pattern the other privileged routes use. The /nfl/pbp
// button does NOT go through here -- it uses a server action so no secret
// ever reaches the browser. Both share dispatchNflPbpRefresh(), which asks
// GitHub Actions to run the Python single-writer in `stale` mode.

export async function POST(request: NextRequest) {
  if (!process.env.CRON_SECRET) {
    return NextResponse.json(
      { error: "CRON_SECRET is not configured on this deployment" },
      { status: 500 },
    );
  }
  if (request.headers.get("authorization") !== `Bearer ${process.env.CRON_SECRET}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const result = await dispatchNflPbpRefresh();
  if (!result.ok) {
    // 502 for an upstream GitHub failure, 500 for our own misconfiguration.
    return NextResponse.json(result, { status: result.status ? 502 : 500 });
  }
  return NextResponse.json(result);
}
