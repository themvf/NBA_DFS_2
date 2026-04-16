import {
  getCachedCrossSlateAccuracy,
  getCachedGameTotalModelAccuracy,
  getCachedLeverageCalibration,
  getCachedMlbBattingOrderCalibration,
  getCachedOwnershipVsTeamTotal,
  getCachedPositionAccuracy,
  getCachedProjectionSourceBreakdown,
  getCachedSalaryTierAccuracy,
  getCachedStatLevelAccuracy,
} from "@/db/analytics-cache";
import type { Sport } from "@/db/queries";
import AnalyticsClient from "./analytics-client";

/** Resolve a promise to its value, or null on any error. */
async function safe<T>(p: Promise<T>): Promise<T | null> {
  try {
    return await p;
  } catch {
    return null;
  }
}

export default async function AnalyticsContent({ sport }: { sport: Sport }) {
  const [
    crossSlate,
    posAccuracy,
    salaryTier,
    leverageCalib,
    ownVsTotal,
    battingOrderCalib,
    projSourceBreakdown,
    statLevelAccuracy,
    gameTotalModel,
  ] = await Promise.all([
    safe(getCachedCrossSlateAccuracy(sport)),
    safe(getCachedPositionAccuracy(sport)),
    safe(getCachedSalaryTierAccuracy(sport)),
    safe(getCachedLeverageCalibration(sport)),
    safe(getCachedOwnershipVsTeamTotal(sport)),
    sport === "mlb"
      ? safe(getCachedMlbBattingOrderCalibration())
      : Promise.resolve([]),
    safe(getCachedProjectionSourceBreakdown(sport)),
    safe(getCachedStatLevelAccuracy(sport)),
    sport === "nba"
      ? safe(getCachedGameTotalModelAccuracy())
      : Promise.resolve([]),
  ]);

  return (
    <AnalyticsClient
      crossSlate={crossSlate ?? []}
      posAccuracy={posAccuracy ?? []}
      salaryTier={salaryTier ?? []}
      leverageCalib={leverageCalib ?? []}
      ownVsTotal={ownVsTotal ?? []}
      battingOrderCalib={battingOrderCalib ?? []}
      projSourceBreakdown={projSourceBreakdown ?? []}
      statLevelAccuracy={statLevelAccuracy ?? []}
      gameTotalModel={gameTotalModel ?? []}
      sport={sport}
    />
  );
}
