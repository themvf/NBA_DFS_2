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

  let players: Awaited<ReturnType<typeof getDfsPagePlayers>> = [];
  let mlbPitcherSignals: MlbPitcherSlateSignal[] = [];
  let mlbGameCards: MlbGameEnvironmentCard[] = [];
  try {
    players = await getDfsPagePlayers(sport);
  } catch (e) {
    console.error("[DfsPage] getDfsPagePlayers error:", e);
    throw e;
  }
  if (sport === "mlb") {
    try {
      mlbPitcherSignals = await getLatestMlbPitcherSignals();
    } catch (e) {
      console.error("[DfsPage] getLatestMlbPitcherSignals error:", e);
      throw e;
    }
    try {
      mlbGameCards = await getMlbGameEnvironmentCards(slateDate);
    } catch (e) {
      console.error("[DfsPage] getMlbGameEnvironmentCards error:", e);
      throw e;
    }
  }
  const slateKey = `${sport}:${slateDate ?? "none"}:${players[0]?.slateId ?? "none"}`;

  return (
    <div className="space-y-6">
      <DfsClient
        key={slateKey}
        players={players}
        slateDate={slateDate}
        mlbPitcherSignals={mlbPitcherSignals}
        mlbGameCards={mlbGameCards}
        sport={sport}
      />
      <Suspense key={`secondary:${slateKey}`} fallback={<DfsSecondaryPanelsFallback />}>
        <DfsSecondaryPanels sport={sport} />
      </Suspense>
    </div>
  );
}
