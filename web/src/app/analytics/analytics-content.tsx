import {
  getCrossSlateAccuracy,
  getPositionAccuracy,
  getSalaryTierAccuracy,
  getLeverageCalibration,
  getOwnershipVsTeamTotal,
} from "@/db/queries";
import type { Sport } from "@/db/queries";
import AnalyticsClient from "./analytics-client";

export default async function AnalyticsContent({ sport }: { sport: Sport }) {
  const [crossSlate, posAccuracy, salaryTier, leverageCalib, ownVsTotal] = await Promise.all([
    getCrossSlateAccuracy(sport),
    getPositionAccuracy(sport),
    getSalaryTierAccuracy(sport),
    getLeverageCalibration(sport),
    getOwnershipVsTeamTotal(sport),
  ]);

  return (
    <AnalyticsClient
      crossSlate={crossSlate}
      posAccuracy={posAccuracy}
      salaryTier={salaryTier}
      leverageCalib={leverageCalib}
      ownVsTotal={ownVsTotal}
      sport={sport}
    />
  );
}
