import {
  getCachedCrossSlateAccuracy,
  getCachedGameTotalModelAccuracy,
  getCachedLeverageCalibration,
  getCachedLsOwnershipBiasMatrix,
  getCachedLsOwnershipTeamPositionMatrix,
  getCachedLsProjectionBiasMatrix,
  getCachedMlbBattingOrderCalibration,
  getCachedOurOwnershipBiasMatrix,
  getCachedOwnershipVsTeamTotal,
  getCachedPositionAccuracy,
  getCachedPositionSalaryMatrix,
  getCachedProjectionSourceBreakdown,
  getCachedSalaryTierAccuracy,
  getCachedSlateTypePerformance,
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

export default async function AnalyticsContent({
  sport,
  showHeader = true,
}: {
  sport: Sport;
  showHeader?: boolean;
}) {
  // Run all independent queries in parallel — reduces total DB time from
  // sum(query latencies) to max(query latency), preventing function timeouts
  // on cache miss when Neon wakes from suspend.
  const [
    crossSlate,
    posAccuracy,
    salaryTier,
    positionSalaryMatrix,
    slateTypePerformance,
    leverageCalib,
    ownVsTotal,
    battingOrderCalib,
    projSourceBreakdown,
    statLevelAccuracy,
    gameTotalModel,
    lsProjectionBiasMatrix,
    ourOwnershipBiasMatrix,
    lsOwnershipBiasMatrix,
    lsOwnershipTeamPositionMatrix,
  ] = await Promise.all([
    safeRun(() => getCachedCrossSlateAccuracy(sport)),
    safeRun(() => getCachedPositionAccuracy(sport)),
    safeRun(() => getCachedSalaryTierAccuracy(sport)),
    safeRun(() => getCachedPositionSalaryMatrix(sport)),
    safeRun(() => getCachedSlateTypePerformance(sport)),
    safeRun(() => getCachedLeverageCalibration(sport)),
    safeRun(() => getCachedOwnershipVsTeamTotal(sport)),
    sport === "mlb" ? safeRun(() => getCachedMlbBattingOrderCalibration()) : Promise.resolve(null),
    safeRun(() => getCachedProjectionSourceBreakdown(sport)),
    safeRun(() => getCachedStatLevelAccuracy(sport)),
    sport === "nba" ? safeRun(() => getCachedGameTotalModelAccuracy()) : Promise.resolve(null),
    safeRun(() => getCachedLsProjectionBiasMatrix(sport)),
    safeRun(() => getCachedOurOwnershipBiasMatrix(sport)),
    safeRun(() => getCachedLsOwnershipBiasMatrix(sport)),
    safeRun(() => getCachedLsOwnershipTeamPositionMatrix(sport)),
  ]);

  return (
    <AnalyticsClient
      crossSlate={crossSlate ?? []}
      posAccuracy={posAccuracy ?? []}
      salaryTier={salaryTier ?? []}
      positionSalaryMatrix={positionSalaryMatrix ?? []}
      slateTypePerformance={slateTypePerformance ?? []}
      leverageCalib={leverageCalib ?? []}
      ownVsTotal={ownVsTotal ?? []}
      battingOrderCalib={battingOrderCalib ?? []}
      projSourceBreakdown={projSourceBreakdown ?? []}
      statLevelAccuracy={statLevelAccuracy ?? []}
      gameTotalModel={gameTotalModel ?? []}
      lsProjectionBiasMatrix={lsProjectionBiasMatrix ?? []}
      ourOwnershipBiasMatrix={ourOwnershipBiasMatrix ?? []}
      lsOwnershipBiasMatrix={lsOwnershipBiasMatrix ?? []}
      lsOwnershipTeamPositionMatrix={lsOwnershipTeamPositionMatrix ?? []}
      sport={sport}
      showHeader={showHeader}
    />
  );
}
