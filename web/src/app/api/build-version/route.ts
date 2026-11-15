import { NextResponse } from "next/server";
import { nflBuildInfo } from "@/lib/nfl-dfs/build-info";

// The commit the live site is serving right now. An open tab compares it with
// the commit it was loaded from and warns when they differ.
export const dynamic = "force-dynamic";

export function GET() {
  return NextResponse.json({ sha: nflBuildInfo().commitSha }, { headers: { "Cache-Control": "no-store" } });
}
