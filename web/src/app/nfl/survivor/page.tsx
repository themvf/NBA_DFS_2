export const dynamic = "force-dynamic";

import { getNflSurvivorGrid, getSurvivorLedger, getSurvivorPools } from "@/db/queries";
import SurvivorClient from "./survivor-client";
import { resolveNflSeason } from "@/lib/nfl/season";

export default async function SurvivorPage({
  searchParams,
}: {
  searchParams: Promise<{ season?: string }>;
}) {
  const { season } = await searchParams;
  const target = resolveNflSeason(season);

  const [grid, pools, ledger] = await Promise.all([
    getNflSurvivorGrid(target),
    getSurvivorPools(target),
    getSurvivorLedger(target),
  ]);

  return (
    <SurvivorClient
      grid={grid}
      pools={pools}
      ledger={ledger}
      loadedAt={new Date().toISOString()}
    />
  );
}
