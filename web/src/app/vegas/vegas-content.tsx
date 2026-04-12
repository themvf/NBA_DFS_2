import {
  getVegasMatchups,
  getOuHitRate,
  getTeamTotalAccuracy,
  getSpreadCoverage,
} from "@/db/queries";
import VegasClient from "./vegas-client";

export default async function VegasContent({ date }: { date?: string }) {
  const [matchups, ouHitRate, teamTotalAccuracy, spreadCoverage] = await Promise.all([
    getVegasMatchups(date),
    getOuHitRate(),
    getTeamTotalAccuracy(),
    getSpreadCoverage(),
  ]);

  return (
    <VegasClient
      matchups={matchups}
      ouHitRate={ouHitRate}
      teamTotalAccuracy={teamTotalAccuracy}
      spreadCoverage={spreadCoverage}
      queryDate={date ?? new Date().toISOString().slice(0, 10)}
    />
  );
}
