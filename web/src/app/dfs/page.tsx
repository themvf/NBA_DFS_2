export const dynamic = "force-dynamic";
export const maxDuration = 60; // allow full 60s for server actions (Vercel Hobby limit)

import { Suspense } from "react";
import { getDfsPagePlayers, getLatestMlbPitcherSignals, getLatestSlateInfo, getMlbGameEnvironmentCards } from "@/db/queries";
import type { MlbGameEnvironmentCard, MlbPitcherSlateSignal, Sport } from "@/db/queries";
import DfsClient from "./dfs-client";
import DfsSecondaryPanels from "./dfs-secondary-panels";

function DfsSecondaryPanelsFallback() {
  return (
    <div className="space-y-6">
      {[0, 1].map((idx) => (
        <div key={idx} className="rounded-lg border bg-card p-4 animate-pulse">
          <div className="h-4 w-56 rounded bg-gray-200" />
          <div className="mt-4 grid gap-3 md:grid-cols-3">
            <div className="h-20 rounded border bg-gray-100" />
            <div className="h-20 rounded border bg-gray-100" />
            <div className="h-20 rounded border bg-gray-100" />
          </div>
        </div>
      ))}
    </div>
  );
}

export default async function DfsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport: rawSport } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

  const slateInfo = await getLatestSlateInfo(sport);
  const slateDate = slateInfo?.slateDate ?? null;

  const [players, mlbPitcherSignals, mlbGameCards] = await Promise.all([
    getDfsPagePlayers(sport),
    sport === "mlb"
      ? getLatestMlbPitcherSignals()
      : Promise.resolve([] as MlbPitcherSlateSignal[]),
    sport === "mlb"
      ? getMlbGameEnvironmentCards(slateDate)
      : Promise.resolve([] as MlbGameEnvironmentCard[]),
  ]);

  return (
    <div className="space-y-6">
      <DfsClient
        players={players}
        slateDate={slateDate}
        mlbPitcherSignals={mlbPitcherSignals}
        mlbGameCards={mlbGameCards}
        sport={sport}
      />
      <Suspense fallback={<DfsSecondaryPanelsFallback />}>
        <DfsSecondaryPanels sport={sport} />
      </Suspense>
    </div>
  );
}
