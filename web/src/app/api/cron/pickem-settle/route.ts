import { NextRequest, NextResponse } from "next/server";
import { withHeartbeat } from "@/lib/cron-heartbeat";
import { pickemLedgerSeasons, settlePickemLedger } from "@/lib/nfl/pickem-settlement";

// Grade every frozen pick'em card from final scores, twice a day (vercel.json).
// The page's "Settle completed weeks" button runs the same settlement; this
// exists because a card graded only on a press stayed "pending" for two weeks.
// Pool finishes (did the entry win?) are still entered by hand, never inferred.

export const dynamic = "force-dynamic";
export const maxDuration = 60;

async function handle(request: NextRequest) {
  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) return NextResponse.json({ ok: false, error: "CRON_SECRET is not configured in this deployment" }, { status: 500 });
  if (request.headers.get("authorization") !== `Bearer ${cronSecret}`) return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  const seasons = await pickemLedgerSeasons();
  const results = [];
  for (const season of seasons) results.push({ season, ...(await settlePickemLedger(season)) });
  return NextResponse.json({ ok: true, seasons: results });
}

export async function GET(request: NextRequest) {
  return withHeartbeat("pickem-settle", () => handle(request));
}
