export const dynamic = "force-dynamic";

import { getNflSurvivorGrid, getSurvivorLedger, getSurvivorPools } from "@/db/queries";
import SurvivorClient from "./survivor-client";

export default async function SurvivorPage({
  searchParams,
}: {
  searchParams: Promise<{ season?: string }>;
}) {
  const { season } = await searchParams;
  const parsed = Number(season);
  const target = Number.isFinite(parsed) && parsed > 2000 ? parsed : 2026;

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
