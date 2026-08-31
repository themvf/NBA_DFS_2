export const dynamic = "force-dynamic";

import { getNflSurvivorGrid } from "@/db/queries";
import SurvivorClient from "./survivor-client";

export default async function SurvivorPage({
  searchParams,
}: {
  searchParams: Promise<{ season?: string }>;
}) {
  const { season } = await searchParams;
  const parsed = Number(season);
  const grid = await getNflSurvivorGrid(Number.isFinite(parsed) && parsed > 2000 ? parsed : 2026);
  return <SurvivorClient grid={grid} loadedAt={new Date().toISOString()} />;
}
