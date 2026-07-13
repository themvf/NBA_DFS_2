import { NextResponse } from "next/server";

import { getTennisEloDashboard } from "@/db/queries";
import { canPromoteTennisSurfaceElo } from "@/lib/tennis-elo-policy";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const dashboard = await getTennisEloDashboard();
    return NextResponse.json({
      ok: true,
      generatedAt: new Date().toISOString(),
      promotionStatus: canPromoteTennisSurfaceElo(dashboard.gates)
        ? "promoted"
        : "not_promoted",
      ...dashboard,
    });
  } catch (error) {
    console.error("Tennis surface-evidence API failed", error);
    return NextResponse.json(
      { ok: false, error: "Surface rating evidence is temporarily unavailable." },
      { status: 500 },
    );
  }
}
