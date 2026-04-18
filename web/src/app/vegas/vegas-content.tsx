import {
  getVegasMatchups,
  getOuHitRate,
  getTeamTotalAccuracy,
  getSpreadCoverage,
  getMlbVegasMatchups,
  getMlbOuHitRate,
  getMlbTeamTotalAccuracy,
  getMlbRunLineCoverage,
  getMlbVegasCoverageStatus,
  getVegasSummaryStats,
  getBiggestMisses,
  getTeamVegasInsights,
} from "@/db/queries";
import type { Sport } from "@/db/queries";
import VegasClient from "./vegas-client";

export default async function VegasContent({ date, sport = "nba" }: { date?: string; sport?: Sport }) {
  const [sportData, mlbCoverageStatus, vegasSummary, biggestMisses, teamInsights] = await Promise.all([
    Promise.all(
      sport === "mlb"
        ? [getMlbVegasMatchups(date), getMlbOuHitRate(), getMlbTeamTotalAccuracy(), getMlbRunLineCoverage()]
        : [getVegasMatchups(date), getOuHitRate(), getTeamTotalAccuracy(), getSpreadCoverage()],
    ),
    sport === "mlb" ? getMlbVegasCoverageStatus() : Promise.resolve(null),
    getVegasSummaryStats(sport),
    getBiggestMisses(sport, 20),
    getTeamVegasInsights(sport),
  ]);
  const [matchups, ouHitRate, teamTotalAccuracy, spreadCoverage] = sportData;

  return (
    <VegasClient
      matchups={matchups}
      ouHitRate={ouHitRate}
      teamTotalAccuracy={teamTotalAccuracy}
      spreadCoverage={spreadCoverage}
      mlbCoverageStatus={mlbCoverageStatus}
      vegasSummary={vegasSummary}
      biggestMisses={biggestMisses}
      teamInsights={teamInsights}
      queryDate={date ?? new Date().toISOString().slice(0, 10)}
      sport={sport}
    />
  );
}
