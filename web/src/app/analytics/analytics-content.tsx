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

/**
 * Run fn(), returning null on any error (including synchronous throws).
 * Using a lambda wrapper — not safe(fn()) — so that synchronous throws
 * inside fn() are caught before they escape this async boundary.
 */
async function safeRun<T>(fn: () => Promise<T>): Promise<T | null> {
  try {
    return await fn();
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
    safeRun(() => getCachedCrossSlateAccuracy(sport)),
    safeRun(() => getCachedPositionAccuracy(sport)),
    safeRun(() => getCachedSalaryTierAccuracy(sport)),
    safeRun(() => getCachedLeverageCalibration(sport)),
    safeRun(() => getCachedOwnershipVsTeamTotal(sport)),
    sport === "mlb"
      ? safeRun(() => getCachedMlbBattingOrderCalibration())
      : Promise.resolve([]),
    safeRun(() => getCachedProjectionSourceBreakdown(sport)),
    safeRun(() => getCachedStatLevelAccuracy(sport)),
    sport === "nba"
      ? safeRun(() => getCachedGameTotalModelAccuracy())
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
