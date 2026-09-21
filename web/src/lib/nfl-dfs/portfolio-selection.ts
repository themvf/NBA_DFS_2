/**
 * Phase 7 (spec §14): correlated scenario scoring and portfolio selection.
 *
 * Promotes the Scenario Lab machinery into a selection engine. Every candidate
 * is scored on the SAME (common random) selection scenarios so differences are
 * attributable to lineup construction, not sampling noise. The portfolio is
 * chosen by MARGINAL contribution to a portfolio success event (at least one
 * lineup exceeds a threshold in a scenario), so two lineups that win the same
 * scenarios contribute less together than two covering different game states.
 * The chosen portfolio is then re-scored on an INDEPENDENT evaluation bank.
 *
 * It never describes a sum of marginal player quantiles as a lineup quantile
 * (P7-AC5). Metrics that require a contest field are omitted, not guessed.
 */
import type { NflDkSlate } from "./dk-salary-csv";
import type { NflLineup } from "./lineups";
import { validateNflLineup } from "./lineups";
import { scoreNflLineupDraws, summarizeNflDraws, type PreparedNflScenarios } from "./scenarios";

export const NFL_PORTFOLIO_SELECTOR_VERSION = "nfl-portfolio-marginal-v1";

export type PortfolioObjective =
  | "max_prob_any_top_threshold"   // maximize P(at least one lineup exceeds the score threshold)
  | "max_mean_best_lineup";        // maximize expected best-lineup score across scenarios

export interface CandidateScore {
  key: string;
  lineup: NflLineup;
  /** Per-scenario lineup scores on the selection bank (common random scenarios). */
  selectionDraws: number[];
  /** Distribution summary on the selection bank. */
  selection: ReturnType<typeof summarizeNflDraws>;
}

export interface PortfolioSelectionResult {
  selectorVersion: string;
  objective: PortfolioObjective;
  target: number;
  /** Selected lineups in pick order with their marginal contribution when added. */
  selected: Array<{ key: string; lineup: NflLineup; marginalContribution: number }>;
  /** Portfolio success probability on the SELECTION bank. */
  selectionSuccessProbability: number;
  /** Portfolio success probability on the INDEPENDENT evaluation bank (P7-AC4). */
  evaluationSuccessProbability: number;
  /** Per-candidate selection-bank distribution summaries (P7-AC5-safe: lineup-level, joint). */
  candidates: Array<{ key: string; selection: ReturnType<typeof summarizeNflDraws> }>;
  limitations: string[];
}

/** Score every candidate on the shared selection bank (common random scenarios). */
export function scoreCandidates(slate: NflDkSlate, candidates: NflLineup[], selection: PreparedNflScenarios, target: number): CandidateScore[] {
  const iid = selection.metadata.sampling === "iid" && selection.dependence === "supplied-joint";
  return candidates.map((lineup) => {
    const { key } = validateNflLineup(slate, lineup);
    const draws = scoreNflLineupDraws(slate, lineup, selection);
    return { key, lineup, selectionDraws: draws, selection: summarizeNflDraws(draws, selection.weights, target, iid) };
  });
}

/** P(at least one lineup in `chosen` exceeds `target`) under weighted scenarios. */
function anySuccessProbability(chosen: number[][], weights: number[], target: number): number {
  if (!chosen.length) return 0;
  let prob = 0;
  for (let s = 0; s < weights.length; s++) {
    if (chosen.some((draws) => draws[s] >= target)) prob += weights[s];
  }
  return Math.min(1, Math.max(0, prob));
}

/**
 * Greedily select up to `count` lineups maximizing marginal contribution to the
 * portfolio success event, respecting a max pairwise overlap. Deterministic:
 * ties break by canonical key.
 */
export function selectPortfolio(
  slate: NflDkSlate,
  candidates: NflLineup[],
  selection: PreparedNflScenarios,
  evaluation: PreparedNflScenarios,
  options: { count: number; target: number; objective?: PortfolioObjective; maxPairwiseOverlap?: number },
): PortfolioSelectionResult {
  // Provenance: selection and evaluation banks MUST be independent (P7-AC4).
  if (selection.metadata.runId === evaluation.metadata.runId || selection.metadata.streamId === evaluation.metadata.streamId || selection.metadata.seed === evaluation.metadata.seed) {
    throw new Error("Selection and evaluation banks must be separate runs, streams and seeds.");
  }
  const selectionIds = new Set(selection.scenarioIds);
  if (evaluation.scenarioIds.some((id) => selectionIds.has(id))) throw new Error("Selection and evaluation scenario IDs overlap.");

  const objective = options.objective ?? "max_prob_any_top_threshold";
  const scored = scoreCandidates(slate, candidates, selection, options.target);
  const overlapCap = options.maxPairwiseOverlap ?? 6;

  const chosen: Array<{ key: string; lineup: NflLineup; draws: number[]; marginalContribution: number }> = [];
  const remaining = new Map(scored.map((c) => [c.key, c]));

  while (chosen.length < options.count && remaining.size) {
    const currentProb = anySuccessProbability(chosen.map((c) => c.draws), selection.weights, options.target);
    let best: { key: string; gain: number } | null = null;
    for (const cand of remaining.values()) {
      // Respect the overlap cap against every already-chosen lineup.
      const playerIds = cand.lineup.map((e) => e.playerId);
      const violatesOverlap = chosen.some((c) => c.lineup.filter((e) => playerIds.includes(e.playerId)).length > overlapCap);
      if (violatesOverlap) continue;
      const withCand = anySuccessProbability([...chosen.map((c) => c.draws), cand.selectionDraws], selection.weights, options.target);
      const gain = objective === "max_prob_any_top_threshold" ? withCand - currentProb : cand.selection.mean;
      if (!best || gain > best.gain || (gain === best.gain && cand.key < best.key)) best = { key: cand.key, gain };
    }
    if (!best) break;
    const cand = remaining.get(best.key)!;
    chosen.push({ key: cand.key, lineup: cand.lineup, draws: cand.selectionDraws, marginalContribution: best.gain });
    remaining.delete(best.key);
  }

  const selectionSuccess = anySuccessProbability(chosen.map((c) => c.draws), selection.weights, options.target);
  // Independent evaluation of the SELECTED portfolio on unseen draws (P7-AC4).
  const evalIid = evaluation.metadata.sampling === "iid" && evaluation.dependence === "supplied-joint";
  const evalDraws = chosen.map((c) => scoreNflLineupDraws(slate, c.lineup, evaluation));
  void evalIid;
  const evaluationSuccess = anySuccessProbability(evalDraws, evaluation.weights, options.target);

  return {
    selectorVersion: NFL_PORTFOLIO_SELECTOR_VERSION,
    objective, target: options.target,
    selected: chosen.map((c) => ({ key: c.key, lineup: c.lineup, marginalContribution: c.marginalContribution })),
    selectionSuccessProbability: selectionSuccess,
    evaluationSuccessProbability: evaluationSuccess,
    candidates: scored.map((c) => ({ key: c.key, selection: c.selection })),
    limitations: [
      "Portfolio success = P(at least one lineup exceeds the score threshold). Not a contest finish or ROI.",
      "Top-percent, first-place and payout metrics require a validated contest field and are omitted here, not estimated.",
      "Scenario coherence/calibration depends on the supplied banks; this engine validates identity, provenance and independence.",
      "Common random scenarios make candidate differences attributable to construction, not sampling; evaluation uses disjoint draws.",
    ],
  };
}
