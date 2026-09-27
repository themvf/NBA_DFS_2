import type { PickemSlate } from "@/db/queries";
import type { PickemEvidence } from "./pickem-evidence";
import { timestamp } from "./pickem-evidence";

export type PickemDefensiveMode = "approved" | "experimental";

export type PickemDefensiveComparison = {
  gameId: number;
  baselineHome: number;
  baselinePlusDefenseHome: number;
  applied: boolean;
};

/** Always provide a paired number for the same current game and quote. */
export function comparePickemDefensiveForecasts(
  slate: PickemSlate,
  evidence: PickemEvidence,
  asOf: string,
): { slate: PickemSlate; comparisons: Record<number, PickemDefensiveComparison>; appliedGameIds: number[] } {
  const selected = selectPickemDefensiveForecasts(slate, evidence, "experimental", asOf);
  const applied = new Set(selected.appliedGameIds);
  const comparisons = Object.fromEntries(slate.games.map((game, index) => [game.gameId, {
    gameId: game.gameId,
    baselineHome: game.pHome,
    baselinePlusDefenseHome: selected.slate.games[index].pHome,
    applied: applied.has(game.gameId),
  }]));
  return { slate: selected.slate, comparisons, appliedGameIds: selected.appliedGameIds };
}

/** The frozen market-residual study is a separate pick'em model, not a conversion of DFS points. */
export function selectPickemDefensiveForecasts(
  slate: PickemSlate,
  evidence: PickemEvidence,
  mode: PickemDefensiveMode,
  asOf: string,
): { slate: PickemSlate; appliedGameIds: number[] } {
  if (mode === "approved") return { slate, appliedGameIds: [] };
  const at = timestamp(asOf);
  const appliedGameIds: number[] = [];
  const games = slate.games.map(game => {
    const comparison = evidence.games[game.gameId]?.matchup;
    const quote = evidence.games[game.gameId]?.latest;
    const cutoff = timestamp(comparison?.input.decisionCutoff);
    const kickoff = timestamp(game.kickoff);
    const captured = timestamp(quote?.capturedAt);
    const frozen = timestamp(comparison?.input.baseline.marketCapturedAt);
    const maximumAge = kickoff - at <= 86400000 ? 7200000 : 86400000;
    if (game.completed || game.provenance !== "market_ml_novig" ||
        comparison?.status !== "shadow" || !comparison.candidate || !quote ||
        !Number.isFinite(at) || !Number.isFinite(cutoff) || !Number.isFinite(kickoff) ||
        !Number.isFinite(captured) || at >= kickoff || cutoff > at ||
        captured !== frozen || captured > at || at - captured > maximumAge ||
        quote.pHome == null || !Number.isFinite(quote.pHome) ||
        Math.abs(quote.pHome - game.pHome) > 1e-10 ||
        Math.abs(comparison.input.baseline.homeConditional - game.pHome) > 1e-10 ||
        comparison.input.baseline.tie !== game.pTie ||
        !Number.isFinite(comparison.candidate.homeConditional) ||
        comparison.candidate.homeConditional <= 0 || comparison.candidate.homeConditional >= 1) return game;
    appliedGameIds.push(game.gameId);
    return { ...game, pHome: comparison.candidate.homeConditional,
      provenance: "experimental_defensive_matchup", computedAt: comparison.input.decisionCutoff };
  });
  return { slate: { ...slate, games }, appliedGameIds };
}
