export const dynamic = "force-dynamic";

import {
  getCrossSlateAccuracy,
  getPositionAccuracy,
  getSalaryTierAccuracy,
  getLeverageCalibration,
} from "@/db/queries";
import AnalyticsClient from "./analytics-client";

export default async function AnalyticsPage() {
  const [crossSlate, posAccuracy, salaryTier, leverageCalib] = await Promise.all([
    getCrossSlateAccuracy(),
    getPositionAccuracy(),
    getSalaryTierAccuracy(),
    getLeverageCalibration(),
  ]);

  return (
    <AnalyticsClient
      crossSlate={crossSlate}
      posAccuracy={posAccuracy}
      salaryTier={salaryTier}
      leverageCalib={leverageCalib}
    />
  );
}
