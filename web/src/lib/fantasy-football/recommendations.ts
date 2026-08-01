export type RankingSignal = {
  playerId: number;
  position: string;
  ourRank: number | null;
  ecr: number | null;
  adp: number | null;
  projectedPoints: number | null;
  tier: number | null;
  confidence: number | null;
};

export type Recommendation = RankingSignal & {
  score: number;
  adpDelta: number;
  explanation: string[];
};

const POSITION_NEED: Record<string, number> = { QB: 1, RB: 2, WR: 2, TE: 1, K: 1, DST: 1 };

export function recommendPlayers(
  players: RankingSignal[],
  rosterPositions: string[],
  picksUntilNextTurn: number,
): Recommendation[] {
  const counts = rosterPositions.reduce<Record<string, number>>((acc, position) => {
    acc[position] = (acc[position] ?? 0) + 1;
    return acc;
  }, {});
  return players
    .map((player) => {
      const baseline = player.ourRank ?? player.ecr ?? 999;
      const adpDelta = player.adp === null ? 0 : player.adp - baseline;
      const need = Math.max(0, (POSITION_NEED[player.position] ?? 0) - (counts[player.position] ?? 0));
      const urgency = player.adp !== null && player.adp <= baseline + picksUntilNextTurn ? 1 : 0;
      const score = 120 - baseline + Math.min(20, adpDelta) * 0.55 + need * 8 + urgency * 5 + (player.confidence ?? 0.5) * 4;
      const explanation = [
        `Our rank ${baseline}`,
        player.adp !== null ? `${adpDelta >= 0 ? "+" : ""}${adpDelta.toFixed(1)} vs ADP` : "ADP unavailable",
        need ? `fills ${player.position} need` : `${player.position} depth`,
        player.tier !== null ? `Tier ${player.tier}` : "tier unavailable",
      ];
      return { ...player, score, adpDelta, explanation };
    })
    .sort((a, b) => b.score - a.score || (a.ourRank ?? 999) - (b.ourRank ?? 999));
}
