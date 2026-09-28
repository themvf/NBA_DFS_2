import type { PickemSlate } from "@/db/queries";
import type { PickemEvidence } from "./pickem-evidence";
import { timestamp } from "./pickem-evidence";
import { matchupResidual, type MatchupForecast } from "./pickem-matchup";

export type PickemDefensiveMode = "approved" | "experimental";

export type PickemDefensiveComparison = {
  gameId: number;
  baselineHome: number;
  baselinePlusDefenseHome: number;
  applied: boolean;
  reason?: string;
  historical?: boolean;
};

/** Always provide a paired number for the same current game and quote. */
export function comparePickemDefensiveForecasts(
  slate: PickemSlate,
  evidence: PickemEvidence,
  asOf: string,
): { slate: PickemSlate; comparisons: Record<number, PickemDefensiveComparison>; appliedGameIds: number[] } {
  const selected = selectPickemDefensiveForecasts(slate, evidence, "experimental", asOf);
  const applied = new Set(selected.appliedGameIds);
  const comparisons = Object.fromEntries(slate.games.map((game, index) => {
    const frozen = evidence.games[game.gameId]?.matchup;
    const historical = timestamp(game.kickoff) <= timestamp(asOf);
    // A saved pregame pair remains visible after kickoff. It is never used to
    // create a new decision for an already-started game.
    const validFrozen = historical && frozen?.candidate && ["shadow", "qualified"].includes(frozen.status)
      && timestamp(frozen.input.decisionCutoff) < timestamp(game.kickoff);
    return [game.gameId, {
      gameId: game.gameId,
      baselineHome: validFrozen ? frozen.input.baseline.homeConditional : game.pHome,
      baselinePlusDefenseHome: validFrozen ? frozen.candidate!.homeConditional : selected.slate.games[index].pHome,
      applied: validFrozen ? true : applied.has(game.gameId),
      ...(historical ? { historical: true } : {}),
      ...(selected.reasons[game.gameId] ? { reason: validFrozen ? "Saved pregame forecast" : selected.reasons[game.gameId] } : {}),
    }];
  }));
  return { slate: selected.slate, comparisons, appliedGameIds: selected.appliedGameIds };
}

/** The frozen market-residual study is a separate pick'em model, not a conversion of DFS points. */
export function selectPickemDefensiveForecasts(
  slate: PickemSlate,
  evidence: PickemEvidence,
  mode: PickemDefensiveMode,
  asOf: string,
): { slate: PickemSlate; appliedGameIds: number[]; reasons: Record<number, string>; forecasts: Record<number, MatchupForecast> } {
  const reasons: Record<number, string> = {}, forecasts: Record<number, MatchupForecast> = {};
  if (mode === "approved") return { slate, appliedGameIds: [], reasons, forecasts };
  const at = timestamp(asOf);
  const appliedGameIds: number[] = [];
  const games = slate.games.map(game => {
    const comparison = evidence.games[game.gameId]?.matchup;
    const quote = evidence.games[game.gameId]?.latest;
    const cutoff = timestamp(comparison?.input.decisionCutoff);
    const kickoff = timestamp(game.kickoff);
    const captured = timestamp(quote?.capturedAt);
    const maximumAge = kickoff - at <= 86400000 ? 7200000 : 86400000;
    const fail = (reason: string) => { reasons[game.gameId] = reason; return game; };
    if (![at, kickoff].every(Number.isFinite)) return fail("Game time is missing or invalid");
    if (game.completed || at >= kickoff) return fail("Game started; new pregame decisions are locked");
    if (!comparison || !comparison.input.model) return fail("Opponent forecast has not been generated");
    if (!Number.isFinite(cutoff) || cutoff > at || cutoff >= kickoff) return fail("Opponent evidence has an invalid observation time");
    if (!quote || quote.pHome == null || !Number.isFinite(quote.pHome) || quote.pHome <= 0 || quote.pHome >= 1)
      return fail("Two-sided moneyline odds are missing");
    if (!Number.isFinite(captured) || captured > at || captured >= kickoff || at - captured > maximumAge)
      return fail("Odds need a fresh capture");
    if (game.provenance !== "market_ml_novig" || Math.abs(quote.pHome - game.pHome) > 1e-10)
      return fail("Baseline does not match the current moneyline odds");
    // Recompute against the current quote using the frozen opponent inputs.
    // This also recovers a forecast whose original odds were stale. Feature
    // availability and model chronology are checked again by matchupResidual.
    const current = matchupResidual({ ...comparison.input, decisionCutoff: asOf,
      baseline: { ...comparison.input.baseline, homeConditional: game.pHome,
        tie: game.pTie, marketCapturedAt: quote.capturedAt } });
    if (current.status !== "shadow" || !current.candidate)
      return fail([...comparison.reasons.filter(r => !/Retrospective development fit|Under evaluation|active forecast/i.test(r)),
        ...current.reasons].join("; ") || "Opponent history is insufficient");
    current.forecastId = comparison.forecastId;
    forecasts[game.gameId] = current;
    appliedGameIds.push(game.gameId);
    // pHome is conditional on no tie. Apply that residual while retaining
    // the slate's tie assumption; the research model's tie mass is separate.
    return { ...game, pHome: current.candidate.homeConditional,
      provenance: "experimental_defensive_matchup", computedAt: asOf };
  });
  return { slate: { ...slate, games }, appliedGameIds, reasons, forecasts };
}
