import {
  getCrossSlateAccuracy,
  getPositionAccuracy,
  getSalaryTierAccuracy,
  getLeverageCalibration,
  getOwnershipVsTeamTotal,
  getMlbBattingOrderCalibration,
  getProjectionSourceBreakdown,
  getStatLevelAccuracy,
  getGameTotalModelAccuracy,
} from "@/db/queries";
import type { Sport } from "@/db/queries";
import AnalyticsClient from "./analytics-client";

export default async function AnalyticsContent({ sport }: { sport: Sport }) {
  const [
    crossSlate, posAccuracy, salaryTier, leverageCalib,
    ownVsTotal, battingOrderCalib, projSourceBreakdown,
    statLevelAccuracy, gameTotalModel,
  ] = await Promise.all([
    getCrossSlateAccuracy(sport),
    getPositionAccuracy(sport),
    getSalaryTierAccuracy(sport),
    getLeverageCalibration(sport),
    getOwnershipVsTeamTotal(sport),
    sport === "mlb" ? getMlbBattingOrderCalibration() : Promise.resolve([]),
    getProjectionSourceBreakdown(sport),
    getStatLevelAccuracy(sport),
    sport === "nba" ? getGameTotalModelAccuracy() : Promise.resolve([]),
  ]);

  return (
    <AnalyticsClient
      crossSlate={crossSlate}
      posAccuracy={posAccuracy}
      salaryTier={salaryTier}
      leverageCalib={leverageCalib}
      ownVsTotal={ownVsTotal}
      battingOrderCalib={battingOrderCalib}
      projSourceBreakdown={projSourceBreakdown}
      statLevelAccuracy={statLevelAccuracy}
      gameTotalModel={gameTotalModel}
      sport={sport}
    />
  );
}
