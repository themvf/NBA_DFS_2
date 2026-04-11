export const dynamic = "force-dynamic";

import { Suspense } from "react";
import type { Sport } from "@/db/queries";
import AnalyticsContent from "./analytics-content";
import PerfectLineupPanel from "./perfect-lineup-panel";

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport: rawSport } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

  return (
    <>
      <Suspense
        fallback={
          <div className="space-y-8 p-6 max-w-5xl mx-auto">
            <div>
              <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
              <p className="text-sm text-gray-500 mt-1">
                Loading {sport.toUpperCase()} analytics…
              </p>
            </div>
            <div className="rounded-lg border bg-card p-6 text-sm text-gray-400">
              Loading accuracy trends, position breakdowns, and leverage calibration…
            </div>
          </div>
        }
      >
        <AnalyticsContent sport={sport} />
      </Suspense>
      <Suspense
        fallback={
          <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4">
            <h2 className="text-sm font-semibold mb-1">Perfect Lineup Structure</h2>
            <p className="text-xs text-gray-400">
              Loading {sport.toUpperCase()} perfect-lineup analytics…
            </p>
          </div>
        }
      >
        <PerfectLineupPanel sport={sport} />
      </Suspense>
    </>
  );
}
