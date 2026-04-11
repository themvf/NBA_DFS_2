export const dynamic = "force-dynamic";

import { Suspense } from "react";
import {
  getCrossSlateAccuracy,
  getPositionAccuracy,
  getSalaryTierAccuracy,
  getLeverageCalibration,
  getOwnershipVsTeamTotal,
} from "@/db/queries";
import type { Sport } from "@/db/queries";
import AnalyticsClient from "./analytics-client";
import PerfectLineupPanel from "./perfect-lineup-panel";

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport: rawSport } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

  const [crossSlate, posAccuracy, salaryTier, leverageCalib, ownVsTotal] = await Promise.all([
    getCrossSlateAccuracy(sport),
    getPositionAccuracy(sport),
    getSalaryTierAccuracy(sport),
    getLeverageCalibration(sport),
    getOwnershipVsTeamTotal(sport),
  ]);

  return (
    <>
      <AnalyticsClient
        crossSlate={crossSlate}
        posAccuracy={posAccuracy}
        salaryTier={salaryTier}
        leverageCalib={leverageCalib}
        ownVsTotal={ownVsTotal}
        sport={sport}
      />
      {sport === "nba" && (
        <Suspense
          fallback={
            <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4">
              <h2 className="text-sm font-semibold mb-1">Perfect Lineup Structure</h2>
              <p className="text-xs text-gray-400">Loading NBA perfect-lineup analytics…</p>
            </div>
          }
        >
          <PerfectLineupPanel />
        </Suspense>
      )}
    </>
  );
}
