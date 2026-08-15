import type { RosterConfig } from "./league-config";

export type RankingSignal = {
  playerId: number;
  position: string;
  ourRank: number | null;
  ecr: number | null;
  adp: number | null;
  projectedPoints: number | null;
  tier: number | null;
  confidence: number | null;
  availabilityProbability?: number | null;
};

export type Recommendation = RankingSignal & { score: number; adpDelta: number; explanation: string[] };

const DEFAULT_ROSTER: RosterConfig = { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6 };
const FLEX_POSITIONS = new Set(["RB", "WR", "TE"]);

function getNeed(position: string, counts: Record<string, number>, roster: RosterConfig) {
  const directNeed = Math.max(0, (roster[position as keyof RosterConfig] ?? 0) - (counts[position] ?? 0));
  if (directNeed > 0) return { value: directNeed, label: `${position} starter need` };
  if (!FLEX_POSITIONS.has(position)) return { value: 0, label: `${position} depth` };

  const eligibleDrafted = ["RB", "WR", "TE"].reduce((total, eligible) => total + (counts[eligible] ?? 0), 0);
  const directEligibleSlots = roster.RB + roster.WR + roster.TE;
  const flexNeed = Math.max(0, roster.FLEX - Math.max(0, eligibleDrafted - directEligibleSlots));
  return flexNeed > 0 ? { value: flexNeed, label: "FLEX need" } : { value: 0, label: `${position} depth` };
}

export function recommendPlayers(
  players: RankingSignal[],
  rosterPositions: string[],
  picksUntilNextTurn: number,
  rosterConfig: RosterConfig = DEFAULT_ROSTER,
): Recommendation[] {
  const counts = rosterPositions.reduce<Record<string, number>>((acc, position) => {
    acc[position] = (acc[position] ?? 0) + 1;
    return acc;
  }, {});

  return players.map((player) => {
    const baseline = player.ourRank ?? player.ecr ?? 999;
    const adpDelta = player.adp === null ? 0 : player.adp - baseline;
    const need = getNeed(player.position, counts, rosterConfig);
    const hasOdds = player.availabilityProbability !== null && player.availabilityProbability !== undefined;
    const urgency = hasOdds ? 1 - (player.availabilityProbability as number)
      : player.adp !== null && player.adp <= baseline + picksUntilNextTurn ? 1 : 0;
    const score = 120 - baseline + Math.min(20, adpDelta) * 0.55 + need.value * 8 + urgency * 5 + (player.confidence ?? 0.5) * 4;
    const explanation = [
      `Our rank ${baseline}`,
      player.adp !== null ? `${adpDelta >= 0 ? "+" : ""}${adpDelta.toFixed(1)} vs ADP` : "ADP unavailable",
      hasOdds ? `${Math.round((player.availabilityProbability as number) * 100)}% available at your pick` : "availability odds unavailable",
      need.label,
      player.tier !== null ? `Tier ${player.tier}` : "tier unavailable",
    ];
    return { ...player, score, adpDelta, explanation };
  }).sort((a, b) => b.score - a.score || (a.ourRank ?? 999) - (b.ourRank ?? 999));
}
