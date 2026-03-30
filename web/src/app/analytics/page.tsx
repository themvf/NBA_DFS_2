export const dynamic = "force-dynamic";

import {
  getCrossSlateAccuracy,
  getPositionAccuracy,
  getSalaryTierAccuracy,
  getLeverageCalibration,
} from "@/db/queries";
import type { Sport } from "@/db/queries";
import AnalyticsClient from "./analytics-client";

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport: rawSport } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

  const [crossSlate, posAccuracy, salaryTier, leverageCalib] = await Promise.all([
    getCrossSlateAccuracy(sport),
    getPositionAccuracy(sport),
    getSalaryTierAccuracy(sport),
    getLeverageCalibration(sport),
  ]);

  return (
    <AnalyticsClient
      crossSlate={crossSlate}
      posAccuracy={posAccuracy}
      salaryTier={salaryTier}
      leverageCalib={leverageCalib}
      sport={sport}
    />
  );
}
