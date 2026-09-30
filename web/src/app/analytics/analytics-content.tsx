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
  getCachedMlbOurProjTeamPositionMatrix,
  getCachedMlbOurProjTeamSalaryMatrix,
  getCachedMlbOurOwnTeamPositionMatrix,
  getCachedMlbOurOwnTeamSalaryMatrix,
  getCachedMlbLsProjTeamPositionMatrix,
  getCachedMlbLsProjTeamSalaryMatrix,
  getCachedMlbLsOwnTeamPositionMatrix,
  getCachedMlbLsOwnTeamSalaryMatrix,
} from "@/db/analytics-cache";
import type { Sport } from "@/db/queries";
import AnalyticsClient from "./analytics-client";
import { loadSection, sectionErrors, sectionValue, skippedSection } from "./analytics-loads";

export default async function AnalyticsContent({
  sport,
  showHeader = true,
}: {
  sport: Sport;
  showHeader?: boolean;
}) {
  // Run all independent queries in parallel — reduces total DB time from
  // sum(query latencies) to max(query latency), preventing function timeouts
  // on cache miss when Neon wakes from suspend. Each load records its own
  // outcome; a failed section is named on the page, never rendered as empty.
  const mlbOnly = <T,>(label: string, fn: () => Promise<T>, empty: T) =>
    sport === "mlb" ? loadSection(label, fn) : Promise.resolve(skippedSection(label, empty));
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
    mlbOurProjTeamPos,
    mlbOurProjTeamSal,
    mlbOurOwnTeamPos,
    mlbOurOwnTeamSal,
    mlbLsProjTeamPos,
    mlbLsProjTeamSal,
    mlbLsOwnTeamPos,
    mlbLsOwnTeamSal,
  ] = await Promise.all([
    loadSection("Accuracy trend", () => getCachedCrossSlateAccuracy(sport)),
    loadSection("Position breakdown", () => getCachedPositionAccuracy(sport)),
    loadSection("Salary tier", () => getCachedSalaryTierAccuracy(sport)),
    loadSection("Position x salary matrix", () => getCachedPositionSalaryMatrix(sport)),
    loadSection("Slate type performance", () => getCachedSlateTypePerformance(sport)),
    loadSection("Leverage calibration", () => getCachedLeverageCalibration(sport)),
    loadSection("Ownership vs team total", () => getCachedOwnershipVsTeamTotal(sport)),
    mlbOnly("Batting order calibration", () => getCachedMlbBattingOrderCalibration(), []),
    loadSection("Projection source breakdown", () => getCachedProjectionSourceBreakdown(sport)),
    loadSection("Stat-level accuracy", () => getCachedStatLevelAccuracy(sport)),
    sport === "nba"
      ? loadSection("Game total model", () => getCachedGameTotalModelAccuracy())
      : Promise.resolve(skippedSection("Game total model", [])),
    loadSection("LineStar projection bias matrix", () => getCachedLsProjectionBiasMatrix(sport)),
    loadSection("Our ownership bias matrix", () => getCachedOurOwnershipBiasMatrix(sport)),
    loadSection("LineStar ownership bias matrix", () => getCachedLsOwnershipBiasMatrix(sport)),
    loadSection("LineStar ownership team x position", () => getCachedLsOwnershipTeamPositionMatrix(sport)),
    mlbOnly("MLB our projection team x position", () => getCachedMlbOurProjTeamPositionMatrix(), []),
    mlbOnly("MLB our projection team x salary", () => getCachedMlbOurProjTeamSalaryMatrix(), []),
    mlbOnly("MLB our ownership team x position", () => getCachedMlbOurOwnTeamPositionMatrix(), []),
    mlbOnly("MLB our ownership team x salary", () => getCachedMlbOurOwnTeamSalaryMatrix(), []),
    mlbOnly("MLB LineStar projection team x position", () => getCachedMlbLsProjTeamPositionMatrix(), []),
    mlbOnly("MLB LineStar projection team x salary", () => getCachedMlbLsProjTeamSalaryMatrix(), []),
    mlbOnly("MLB LineStar ownership team x position", () => getCachedMlbLsOwnTeamPositionMatrix(), []),
    mlbOnly("MLB LineStar ownership team x salary", () => getCachedMlbLsOwnTeamSalaryMatrix(), []),
  ]);

  const loadErrors = sectionErrors([
    crossSlate, posAccuracy, salaryTier, positionSalaryMatrix, slateTypePerformance, leverageCalib,
    ownVsTotal, battingOrderCalib, projSourceBreakdown, statLevelAccuracy, gameTotalModel,
    lsProjectionBiasMatrix, ourOwnershipBiasMatrix, lsOwnershipBiasMatrix, lsOwnershipTeamPositionMatrix,
    mlbOurProjTeamPos, mlbOurProjTeamSal, mlbOurOwnTeamPos, mlbOurOwnTeamSal,
    mlbLsProjTeamPos, mlbLsProjTeamSal, mlbLsOwnTeamPos, mlbLsOwnTeamSal,
  ]);

  return (
    <AnalyticsClient
      crossSlate={sectionValue(crossSlate, [])}
      crossSlateFailed={!crossSlate.ok}
      posAccuracy={sectionValue(posAccuracy, [])}
      salaryTier={sectionValue(salaryTier, [])}
      positionSalaryMatrix={sectionValue(positionSalaryMatrix, [])}
      slateTypePerformance={sectionValue(slateTypePerformance, [])}
      leverageCalib={sectionValue(leverageCalib, [])}
      ownVsTotal={sectionValue(ownVsTotal, [])}
      battingOrderCalib={sectionValue(battingOrderCalib, [])}
      projSourceBreakdown={sectionValue(projSourceBreakdown, [])}
      statLevelAccuracy={sectionValue(statLevelAccuracy, [])}
      gameTotalModel={sectionValue(gameTotalModel, [])}
      lsProjectionBiasMatrix={sectionValue(lsProjectionBiasMatrix, [])}
      ourOwnershipBiasMatrix={sectionValue(ourOwnershipBiasMatrix, [])}
      lsOwnershipBiasMatrix={sectionValue(lsOwnershipBiasMatrix, [])}
      lsOwnershipTeamPositionMatrix={sectionValue(lsOwnershipTeamPositionMatrix, [])}
      mlbOurProjTeamPos={sectionValue(mlbOurProjTeamPos, [])}
      mlbOurProjTeamSal={sectionValue(mlbOurProjTeamSal, [])}
      mlbOurOwnTeamPos={sectionValue(mlbOurOwnTeamPos, [])}
      mlbOurOwnTeamSal={sectionValue(mlbOurOwnTeamSal, [])}
      mlbLsProjTeamPos={sectionValue(mlbLsProjTeamPos, [])}
      mlbLsProjTeamSal={sectionValue(mlbLsProjTeamSal, [])}
      mlbLsOwnTeamPos={sectionValue(mlbLsOwnTeamPos, [])}
      mlbLsOwnTeamSal={sectionValue(mlbLsOwnTeamSal, [])}
      loadErrors={loadErrors}
      sport={sport}
      showHeader={showHeader}
    />
  );
}
