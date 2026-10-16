import { NextRequest, NextResponse } from "next/server";
import { buildAndSaveArm } from "@/lib/nfl-dfs/ghost/service";

/** One ghost arm, built and saved in its own function call (fanned out by /api/cron/nfl-ghost). */
export const dynamic = "force-dynamic";
export const maxDuration = 300;

export async function POST(request: NextRequest) {
  const secret = process.env.CRON_SECRET;
  if (!secret) return NextResponse.json({ error: "Cron authentication is not configured" }, { status: 500 });
  if (request.headers.get("authorization") !== `Bearer ${secret}`) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  const body = await request.json().catch(() => null) as { freezeId?: unknown; arm?: unknown } | null;
  const freezeId = typeof body?.freezeId === "string" && /^[0-9a-f-]{36}$/.test(body.freezeId) ? body.freezeId : null;
  const arm = Number.isInteger(body?.arm) ? Number(body!.arm) : null;
  if (!freezeId || arm == null || arm < 0) return NextResponse.json({ error: "freezeId and arm are required" }, { status: 400 });
  try {
    return NextResponse.json(await buildAndSaveArm(freezeId, arm));
  } catch (error) {
    return NextResponse.json({ status: "error", detail: error instanceof Error ? error.message : String(error) }, { status: 500 });
  }
}
