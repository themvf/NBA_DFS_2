import type { NflGeneratedLineup, NflProjectionSource } from "@/app/dfs/nfl/nfl-optimizer";

export type NflLineupInsight = {
  lineupNumber: number;
  projectionRank: number;
  floorRank: number;
  ceilingRank: number;
  salaryUsedPct: number;
  valuePerThousand: number;
  uniqueFromPrevious: number | null;
  sourceCounts: Record<string, number>;
  gameCounts: { game: string; players: number; projection: number }[];
  reasons: { label: string; detail: string; tone: "ceiling" | "floor" | "correlation" | "value" | "diversity" | "source" }[];
};

function rank(lineups: NflGeneratedLineup[], value: (lineup: NflGeneratedLineup) => number, target: NflGeneratedLineup): number {
  return [...lineups].sort((a, b) => value(b) - value(a)).findIndex((lineup) => lineup.lineupNumber === target.lineupNumber) + 1;
}

export function lineupOverlap(a: NflGeneratedLineup, b: NflGeneratedLineup): number {
  const ids = new Set(a.playerIds);
  return b.playerIds.filter((id) => ids.has(id)).length;
}

export function buildLineupInsight(
  lineup: NflGeneratedLineup,
  lineups: NflGeneratedLineup[],
  mode: "cash" | "gpp",
  selectedSource: NflProjectionSource,
): NflLineupInsight {
  const projectionRank = rank(lineups, (entry) => entry.projectedFpts, lineup);
  const floorRank = rank(lineups, (entry) => entry.floorFpts, lineup);
  const ceilingRank = rank(lineups, (entry) => entry.ceilingFpts, lineup);
  const sourceCounts: Record<string, number> = {};
  const games = new Map<string, { players: number; projection: number }>();
  lineup.slots.forEach((entry) => {
    sourceCounts[entry.projectionSource] = (sourceCounts[entry.projectionSource] ?? 0) + 1;
    const game = entry.player.gameKey ?? "Unmapped game";
    const existing = games.get(game) ?? { players: 0, projection: 0 };
    existing.players += 1;
    existing.projection += entry.projection;
    games.set(game, existing);
  });
  const previous = lineups.find((entry) => entry.lineupNumber === lineup.lineupNumber - 1) ?? null;
  const uniqueFromPrevious = previous ? lineup.playerIds.length - lineupOverlap(lineup, previous) : null;
  const reasons: NflLineupInsight["reasons"] = [];
  if (mode === "gpp") reasons.push({ label: `Ceiling rank #${ceilingRank}`, detail: `${lineup.ceilingFpts.toFixed(1)} simulated ceiling points drive the tournament objective.`, tone: "ceiling" });
  else reasons.push({ label: `Floor rank #${floorRank}`, detail: `${lineup.floorFpts.toFixed(1)} floor points drive the cash objective.`, tone: "floor" });
  const stack = lineup.stackSummary;
  if (stack.quarterback && stack.passCatchers.length) reasons.push({ label: `${stack.passCatchers.length}-player QB stack`, detail: `${stack.quarterback} is paired with ${stack.passCatchers.join(" and ")}${stack.bringBack ? ` plus bring-back ${stack.bringBack}` : ""}.`, tone: "correlation" });
  reasons.push({ label: `${(lineup.totalSalary / 500).toFixed(1)}% cap used`, detail: `${(lineup.projectedFpts / (lineup.totalSalary / 1000)).toFixed(2)} projected points per $1K.`, tone: "value" });
  if (uniqueFromPrevious != null) reasons.push({ label: `${uniqueFromPrevious} unique vs prior`, detail: `Lineup ${lineup.lineupNumber} changes ${uniqueFromPrevious} roster spots from lineup ${lineup.lineupNumber - 1}.`, tone: "diversity" });
  const direct = sourceCounts[selectedSource] ?? 0;
  const fallback = sourceCounts.dk_avg_fallback ?? 0;
  reasons.push({ label: `${direct}/${lineup.slots.length} direct-source`, detail: fallback ? `${fallback} roster spot${fallback === 1 ? "" : "s"} use the disclosed DK Avg fallback.` : "Every roster spot uses the selected projection source.", tone: "source" });
  return {
    lineupNumber: lineup.lineupNumber, projectionRank, floorRank, ceilingRank,
    salaryUsedPct: lineup.totalSalary / 500, valuePerThousand: lineup.projectedFpts / (lineup.totalSalary / 1000),
    uniqueFromPrevious, sourceCounts,
    gameCounts: [...games].map(([game, values]) => ({ game, ...values })).sort((a, b) => b.players - a.players || b.projection - a.projection),
    reasons,
  };
}

export function averagePairwiseUnique(lineups: NflGeneratedLineup[]): number {
  if (lineups.length < 2) return lineups[0]?.playerIds.length ?? 0;
  let pairs = 0, unique = 0;
  for (let i = 0; i < lineups.length; i++) for (let j = i + 1; j < lineups.length; j++) {
    unique += lineups[i].playerIds.length - lineupOverlap(lineups[i], lineups[j]);
    pairs++;
  }
  return unique / pairs;
}
