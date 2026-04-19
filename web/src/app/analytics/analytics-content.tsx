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
  const crossSlate = await safeRun(() => getCachedCrossSlateAccuracy(sport));
  const posAccuracy = await safeRun(() => getCachedPositionAccuracy(sport));
  const salaryTier = await safeRun(() => getCachedSalaryTierAccuracy(sport));
  const leverageCalib = await safeRun(() => getCachedLeverageCalibration(sport));
  const ownVsTotal = await safeRun(() => getCachedOwnershipVsTeamTotal(sport));
  const battingOrderCalib = sport === "mlb"
    ? await safeRun(() => getCachedMlbBattingOrderCalibration())
    : [];
  const projSourceBreakdown = await safeRun(() => getCachedProjectionSourceBreakdown(sport));
  const statLevelAccuracy = await safeRun(() => getCachedStatLevelAccuracy(sport));
  const gameTotalModel = sport === "nba"
    ? await safeRun(() => getCachedGameTotalModelAccuracy())
    : [];

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
