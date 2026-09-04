"use server";
import { getNflPlayerReviewHistory } from "@/db/nfl-dfs-report-card";
import { VARIANT_LABELS, type ReportVariant } from "@/lib/nfl-dfs/report-card";

export async function loadPlayerHistory(season: number, playerId: number, variant: ReportVariant) {
  if (!Number.isInteger(season) || season < 2000 || season > 2099 || !Number.isSafeInteger(playerId) || playerId <= 0 || !Object.hasOwn(VARIANT_LABELS, variant)) {
    throw new Error("Invalid player-history selection");
  }
  return getNflPlayerReviewHistory(season, playerId, variant);
}
