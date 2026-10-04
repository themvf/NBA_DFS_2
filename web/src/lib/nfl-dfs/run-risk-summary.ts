import type { NflGeneratedLineup } from "@/app/dfs/nfl/nfl-optimizer";

export type RunRiskSummary = {
  sourceFamilies: string[];
  dstOpponentLineups: number[];
};

function sourceFamily(source: string): string {
  return source === "our" || source === "our_fallback" || source === "defensive" ? "historical" : source;
}

/** Inspect the saved roster itself; older runs may lack a complete pool audit. */
export function summarizeRunRisks(lineups: readonly NflGeneratedLineup[]): RunRiskSummary {
  const sourceFamilies = new Set<string>();
  const dstOpponentLineups: number[] = [];
  for (const lineup of lineups) {
    for (const slot of lineup.slots) sourceFamilies.add(sourceFamily(slot.projectionSource));
    const hasConflict = lineup.slots.some(({ player: dst }) =>
      dst.position === "DST" && Boolean(dst.opponent) && lineup.slots.some(({ player: offense }) =>
        offense.position !== "DST" && offense.position !== "K" && offense.team === dst.opponent));
    if (hasConflict) dstOpponentLineups.push(lineup.lineupNumber);
  }
  return { sourceFamilies: [...sourceFamilies].sort(), dstOpponentLineups };
}
