import "server-only";
import type { NflDkSlate } from "@/lib/nfl-dfs/dk-salary-csv";
import { evaluateNflMatchupPortfolio, type NflContestConfig, type NflContestField, type NflPortfolioRules } from "@/lib/nfl-dfs/matchup-contest";
import { validateNflLineup, type NflLineup, type NflSlot } from "@/lib/nfl-dfs/lineups";
import type { NflGeneratedLineup, NflOptimizerResult, NflOptimizerSettings } from "./nfl-optimizer";

export const toScenarioLineup = (lineup: NflGeneratedLineup): NflLineup => lineup.slots.map((entry) => ({
  slot: entry.slot.replace(/\d+$/, "") as NflSlot, playerId: entry.player.dkPlayerId,
}));

/** Optional research capability: invalid/missing banks leave saved production output intact. */
export function evaluateNflOptimizerShadowIfAvailable(input: Parameters<typeof evaluateNflOptimizerShadow>[0]) {
  try {
    return { status: "evaluated" as const, baseline: input.baseline, report: evaluateNflOptimizerShadow(input), reason: null };
  } catch (error) {
    return { status: "unavailable" as const, baseline: input.baseline, report: null,
      reason: error instanceof Error ? error.message : "Scenario inputs are unavailable." };
  }
}

/** Evaluate existing optimizer candidates; never mutate generated or saved entries. */
export function evaluateNflOptimizerShadow(input: {
  slate: NflDkSlate; baseline: NflOptimizerResult; candidates: NflOptimizerResult;
  settings: NflOptimizerSettings; contest: NflContestConfig;
  selection: unknown; evaluation: unknown; field?: NflContestField; target: number;
}) {
  if (input.baseline.lineups.length !== input.settings.nLineups || input.contest.entryCount !== input.settings.nLineups) throw new Error("A complete equal-count baseline is required.");
  const source = new Map([...input.baseline.lineups, ...input.candidates.lineups].map((lineup) => [validateNflLineup(input.slate, toScenarioLineup(lineup)).key, lineup]));
  const exposureCounts: NonNullable<NflPortfolioRules["exposureCounts"]> = [];
  for (const row of input.baseline.exposureReport ?? []) {
    exposureCounts.push({ playerId: row.dkPlayerId, min: row.overallMin, max: row.overallMax });
    if (input.slate.format === "showdown") exposureCounts.push(
      { playerId: row.dkPlayerId, slot: "CPT", min: row.captainMin, max: row.captainMax },
      { playerId: row.dkPlayerId, slot: "FLEX", min: row.flexMin, max: row.flexMax });
  }
  const rules: NflPortfolioRules = {
    maxPairwiseOverlap: input.settings.maxPairwiseOverlap ?? (input.slate.format === "classic" ? 9 : 6) - input.settings.minUnique,
    lockedPlayerIds: input.settings.lockedPlayerIds, excludedPlayerIds: input.settings.excludedPlayerIds, exposureCounts,
  };
  const report = evaluateNflMatchupPortfolio({ slate: input.slate, baseline: input.baseline.lineups.map(toScenarioLineup),
    candidates: [...source.values()].map(toScenarioLineup), contest: input.contest, selection: input.selection, evaluation: input.evaluation,
    field: input.field, target: input.target, rules, constructionObjective: "max_mean_best_lineup" });
  const selectedGenerated = report.selected.map((lineup) => source.get(validateNflLineup(input.slate, lineup).key)!);
  // Reranking changes portfolio quotas even though every individual candidate is legal.
  // Keep these final constraints explicit; do not export an accidentally relaxed portfolio.
  const constraintProblems: string[] = [];
  if (input.settings.archetypeQuotas?.length) {
    for (const quota of input.settings.archetypeQuotas) {
      const baselineCount = input.baseline.lineups.filter((lineup) => lineup.archetype?.id === quota.archetypeId).length;
      if (selectedGenerated.filter((lineup) => lineup.archetype?.id === quota.archetypeId).length !== baselineCount) constraintProblems.push(`Archetype quota changed: ${quota.archetypeId}`);
    }
  }
  if (input.settings.salaryPolicy?.salaryLeftBands?.length) {
    for (const band of input.settings.salaryPolicy.salaryLeftBands) {
      const count = (rows: NflGeneratedLineup[]) => rows.filter((lineup) => 50000 - lineup.totalSalary >= band.min && 50000 - lineup.totalSalary <= band.max).length;
      if (count(selectedGenerated) !== count(input.baseline.lineups)) constraintProblems.push(`Salary-left allocation changed: ${band.min}-${band.max}`);
    }
  }
  return { ...report, status: constraintProblems.length ? "incomplete_search" as const : report.status,
    challenger: constraintProblems.length ? null : report.challenger,
    constraintProblems, selectedGenerated, exportAuthorized: false,
    baselineWarnings: input.baseline.warnings, candidateWarnings: input.candidates.warnings };
}
