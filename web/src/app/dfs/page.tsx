export const dynamic = "force-dynamic";
export const maxDuration = 60; // allow full 60s for server actions (Vercel Hobby limit)

import { getDkPlayers, getLatestSlateInfo, getDfsAccuracy, getDkLineupComparison, getDkStrategySummary } from "@/db/queries";
import type { Sport } from "@/db/queries";
import DfsClient from "./dfs-client";

export default async function DfsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport: rawSport } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

  const [players, slateInfo, accuracy, comparison, strategySummary] = await Promise.all([
    getDkPlayers(sport),
    getLatestSlateInfo(sport),
    getDfsAccuracy(sport),
    getDkLineupComparison(sport),
    getDkStrategySummary(sport),
  ]);

  return (
    <DfsClient
      players={players}
      slateDate={slateInfo?.slateDate ?? null}
      accuracy={accuracy}
      comparison={comparison}
      strategySummary={strategySummary}
      sport={sport}
    />
  );
}
