export const dynamic = "force-dynamic";

import { getDkPlayers, getLatestSlateInfo, getDfsAccuracy, getDkLineupComparison, getDkStrategySummary } from "@/db/queries";
import DfsClient from "./dfs-client";

export default async function DfsPage() {
  const [players, slateInfo, accuracy, comparison, strategySummary] = await Promise.all([
    getDkPlayers(),
    getLatestSlateInfo(),
    getDfsAccuracy(),
    getDkLineupComparison(),
    getDkStrategySummary(),
  ]);

  return (
    <DfsClient
      players={players}
      slateDate={slateInfo?.slateDate ?? null}
      accuracy={accuracy}
      comparison={comparison}
      strategySummary={strategySummary}
    />
  );
}
