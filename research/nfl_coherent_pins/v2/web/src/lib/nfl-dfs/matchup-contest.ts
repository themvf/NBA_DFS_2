/** Shadow-only matchup/contest evaluator. Production generation is unchanged. */
import type { NflDkSlate } from "./dk-salary-csv";
import { validateNflLineup, type NflLineup } from "./lineups";
import { selectPortfolio, type PortfolioObjective } from "./portfolio-selection";
import { prepareNflScenarios, scoreNflLineupDraws, summarizeNflDraws, type PreparedNflScenarios } from "./scenarios";
import { prepareNflMarginalScores, type NflMarginalScoreBank } from "./marginal-scenario-bank";

export const NFL_MATCHUP_CONTEST_VERSION = "nfl-matchup-contest-shadow-v1";
export type NflContestMode = "single_entry" | "three_entry" | "multi_entry";
export type NflContestConfig = {
  id: string;
  platform: "draftkings";
  scoringVersion: string;
  slateId: string;
  format: "classic" | "showdown";
  mode: NflContestMode;
  entryCount: number;
  maxEntriesPerUser: number;
  fieldSize: number | null;
  entryFee: number | null;
  /** Gross prizes indexed by finishing rank minus one; omitted ranks pay zero. */
  payouts: number[] | null;
  tieRule: "split_occupied_prizes";
  decisionAt: string;
  lockAt: string;
  lateSwap: boolean;
  ownershipCapability: "missing" | "heuristic_uncalibrated" | "validated";
};
export type NflContestField = {
  modelVersion: string;
  snapshotId: string;
  capturedAt: string;
  qualificationId: string;
  /** A validated field snapshot for this contest cohort, not marginal ownership. */
  qualification: "validated";
  entries: Array<{ lineup: NflLineup; multiplicity: number }>;
};
export type NflPortfolioRules = {
  maxPairwiseOverlap: number;
  lockedPlayerIds?: number[];
  excludedPlayerIds?: number[];
  exposureCounts?: Array<{ playerId: number; slot?: "CPT" | "FLEX"; min: number; max: number }>;
};

function time(value: string, label: string) {
  if (!/(Z|[+-]\d\d:\d\d)$/.test(value) || !Number.isFinite(Date.parse(value))) throw new Error(`${label} needs an explicit timezone.`);
  return Date.parse(value);
}
export function validateNflContestConfig(config: NflContestConfig, slate: NflDkSlate) {
  if (!config.id || !config.slateId || !config.scoringVersion || config.platform !== "draftkings" || config.format !== slate.format) throw new Error("Contest identity/scoring/format is invalid.");
  if (!["single_entry", "three_entry", "multi_entry"].includes(config.mode)) throw new Error("Unknown contest mode.");
  if (!Number.isSafeInteger(config.entryCount) || config.entryCount < 1 || config.entryCount > 150
    || !Number.isSafeInteger(config.maxEntriesPerUser) || config.maxEntriesPerUser < config.entryCount) throw new Error("Invalid contest entry limits.");
  if (config.mode === "single_entry" && (config.entryCount !== 1 || config.maxEntriesPerUser !== 1)) throw new Error("Single-entry requires one allowed entry.");
  if (config.mode === "three_entry" && config.maxEntriesPerUser !== 3) throw new Error("Three-entry requires the three-entry contest limit.");
  if (config.fieldSize !== null && (!Number.isSafeInteger(config.fieldSize) || config.fieldSize < config.entryCount)) throw new Error("Invalid contest field size.");
  if (config.entryFee !== null && (!Number.isFinite(config.entryFee) || config.entryFee < 0)) throw new Error("Invalid entry fee.");
  if (config.payouts !== null && (!config.payouts.length || config.payouts.some((value, i) => !Number.isFinite(value) || value < 0 || (i > 0 && value > config.payouts![i - 1]))
    || config.fieldSize === null || config.payouts.length > config.fieldSize)) throw new Error("Invalid gross payout table.");
  if (config.tieRule !== "split_occupied_prizes") throw new Error("Unsupported prize tie rule.");
  if (time(config.decisionAt, "Decision") >= time(config.lockAt, "Lock")) throw new Error("New contest recommendations must precede lock.");
  if (!["missing", "heuristic_uncalibrated", "validated"].includes(config.ownershipCapability)) throw new Error("Unknown ownership capability.");
}

function validateBanks(slate: NflDkSlate, selectionInput: unknown, evaluationInput: unknown, config: NflContestConfig) {
  const prepare = (value: unknown) => value && typeof value === "object" && "schemaVersion" in value && value.schemaVersion === "nfl-marginal-score-bank-v1"
    ? prepareNflMarginalScores(slate, value as NflMarginalScoreBank) : prepareNflScenarios(slate, value);
  const selection = prepare(selectionInput);
  const evaluation = prepare(evaluationInput);
  if (selection.dependence !== evaluation.dependence) throw new Error("Scenario dependence mode differs between streams.");
  for (const key of ["snapshotId", "modelVersion", "source", "decisionAt", "inputsCapturedAt"] as const) {
    if (selection.metadata[key] !== evaluation.metadata[key]) throw new Error(`Scenario ${key} mismatch.`);
  }
  if (selection.metadata.decisionAt !== config.decisionAt) throw new Error("Contest and scenario decision cutoffs differ.");
  if (selection.metadata.source !== "model") throw new Error("A live matchup comparison requires model-source banks.");
  if (selection.metadata.runId === evaluation.metadata.runId || selection.metadata.streamId === evaluation.metadata.streamId || selection.metadata.seed === evaluation.metadata.seed) throw new Error("Separate selection/evaluation streams required.");
  const ids = new Set(selection.scenarioIds);
  if (evaluation.scenarioIds.some((id) => ids.has(id))) throw new Error("Selection/evaluation draws overlap.");
  return { selection, evaluation };
}

function fieldScores(slate: NflDkSlate, field: NflContestField, config: NflContestConfig, bank: PreparedNflScenarios) {
  if (field.qualification !== "validated" || !field.qualificationId || !field.modelVersion || !field.snapshotId) throw new Error("Field model qualification is missing.");
  if (time(field.capturedAt, "Field capture") > time(config.decisionAt, "Decision")) throw new Error("Field information arrived after the cutoff.");
  if (config.fieldSize === null) throw new Error("Field size required.");
  let count = 0;
  const rows = field.entries.map((entry) => {
    if (!Number.isSafeInteger(entry.multiplicity) || entry.multiplicity < 1) throw new Error("Field multiplicity must be a positive integer.");
    count += entry.multiplicity;
    return { key: validateNflLineup(slate, entry.lineup).key, multiplicity: entry.multiplicity, draws: scoreNflLineupDraws(slate, entry.lineup, bank) };
  });
  if (count !== config.fieldSize - config.entryCount) throw new Error("Field rivals must exactly equal total field minus this portfolio's entries.");
  return rows;
}

/** Counts all competing own entries, all rival duplicates, and occupied prize ranks. */
export function scoreNflContestOutcome(ownScores: number[], rivalScores: Array<{ score: number; multiplicity: number }>, payouts: number[], fieldSize: number) {
  if (!ownScores.length || ownScores.some((score) => !Number.isFinite(score)) || rivalScores.some((row) => !Number.isFinite(row.score) || !Number.isSafeInteger(row.multiplicity) || row.multiplicity < 1)) throw new Error("Invalid contest outcomes.");
  if (ownScores.length + rivalScores.reduce((sum, row) => sum + row.multiplicity, 0) !== fieldSize) throw new Error("Outcome field size mismatch.");
  const entries = ownScores.map((score) => {
    const higher = ownScores.filter((other) => other > score).length + rivalScores.reduce((sum, row) => sum + (row.score > score ? row.multiplicity : 0), 0);
    const tied = ownScores.filter((other) => other === score).length + rivalScores.reduce((sum, row) => sum + (row.score === score ? row.multiplicity : 0), 0);
    let occupiedPrizes = 0;
    for (let rank = higher; rank < Math.min(higher + tied, payouts.length); rank++) occupiedPrizes += payouts[rank];
    return { rank: higher + 1, tied, grossPayout: occupiedPrizes / tied };
  });
  return { entries, grossPayout: entries.reduce((sum, row) => sum + row.grossPayout, 0),
    firstOrFirstTie: entries.some((row) => row.rank === 1),
    topOnePercentOrBoundaryTie: entries.some((row) => row.rank <= Math.max(1, Math.ceil(fieldSize * .01))) };
}

function portfolioValid(lineups: NflLineup[], rules: NflPortfolioRules, final: boolean) {
  for (const lineup of lineups) {
    if (rules.lockedPlayerIds?.some((id) => !lineup.some((p) => p.playerId === id))) return false;
    if (rules.excludedPlayerIds?.some((id) => lineup.some((p) => p.playerId === id))) return false;
  }
  for (let i = 0; i < lineups.length; i++) for (let j = i + 1; j < lineups.length; j++) {
    const ids = new Set(lineups[i].map((row) => row.playerId));
    if (lineups[j].filter((row) => ids.has(row.playerId)).length > rules.maxPairwiseOverlap) return false;
  }
  return !(rules.exposureCounts ?? []).some((rule) => {
    const count = lineups.reduce((sum, lineup) => sum + Number(lineup.some((row) => row.playerId === rule.playerId && (!rule.slot || (rule.slot === "CPT" ? row.slot === "CPT" : row.slot !== "CPT")))), 0);
    return count > rule.max || (final && count < rule.min);
  });
}

export function evaluateNflMatchupPortfolio(input: {
  slate: NflDkSlate; candidates: NflLineup[]; baseline: NflLineup[];
  selection: unknown; evaluation: unknown; contest: NflContestConfig;
  field?: NflContestField; rules: NflPortfolioRules; target: number;
  constructionObjective?: PortfolioObjective;
  contestObjective?: "expected_net_payout" | "probability_any_top_one_percent";
}) {
  const { slate, contest, rules } = input;
  validateNflContestConfig(contest, slate);
  if (!Number.isFinite(input.target)) throw new Error("Construction target is required.");
  if (!Number.isSafeInteger(rules.maxPairwiseOverlap) || rules.maxPairwiseOverlap < 0 || rules.maxPairwiseOverlap > (slate.format === "classic" ? 9 : 6)) throw new Error("Invalid overlap limit.");
  for (const rule of rules.exposureCounts ?? []) if (!Number.isSafeInteger(rule.min) || !Number.isSafeInteger(rule.max) || rule.min < 0 || rule.max < rule.min || rule.max > contest.entryCount) throw new Error("Invalid exposure counts.");
  if (!input.candidates.length || input.baseline.length !== contest.entryCount) throw new Error("Candidates and equal-entry-count baseline required.");
  const keys = input.candidates.map((lineup) => validateNflLineup(slate, lineup).key);
  if (new Set(keys).size !== keys.length) throw new Error("Duplicate canonical candidates.");
  input.baseline.forEach((lineup) => validateNflLineup(slate, lineup));
  if (new Set(input.baseline.map((lineup) => validateNflLineup(slate, lineup).key)).size !== input.baseline.length) throw new Error("Duplicate baseline lineup.");
  if (!portfolioValid(input.baseline, rules, true)) throw new Error("Baseline violates the frozen portfolio rules.");
  const { selection, evaluation } = validateBanks(slate, input.selection, input.evaluation, contest);
  const independentFallback = selection.dependence === "independent-ablation";
  const hasContest = !independentFallback && !!input.field && contest.fieldSize !== null && contest.entryFee !== null && contest.payouts !== null;
  const selectionField = hasContest ? fieldScores(slate, input.field!, contest, selection) : null;
  const evaluationField = hasContest ? fieldScores(slate, input.field!, contest, evaluation) : null;
  const summarize = (lineups: NflLineup[], bank: PreparedNflScenarios, field: ReturnType<typeof fieldScores> | null) => {
    const draws = lineups.map((lineup) => scoreNflLineupDraws(slate, lineup, bank));
    const best = bank.weights.map((_, i) => Math.max(...draws.map((row) => row[i])));
    const construction = summarizeNflDraws(best, bank.weights, input.target, bank.metadata.sampling === "iid" && bank.dependence === "supplied-joint");
    if (!field) return { construction, contest: null };
    let expectedGrossPayout = 0, probabilityAnyFirstOrTie = 0, probabilityAnyTopOnePercentOrTie = 0;
    bank.weights.forEach((weight, i) => {
      const outcome = scoreNflContestOutcome(draws.map((row) => row[i]), field.map((row) => ({ score: row.draws[i], multiplicity: row.multiplicity })), contest.payouts!, contest.fieldSize!);
      expectedGrossPayout += weight * outcome.grossPayout;
      probabilityAnyFirstOrTie += weight * Number(outcome.firstOrFirstTie);
      probabilityAnyTopOnePercentOrTie += weight * Number(outcome.topOnePercentOrBoundaryTie);
    });
    const cost = contest.entryFee! * contest.entryCount;
    return { construction, contest: { expectedGrossPayout, entryCost: cost, expectedNetPayout: expectedGrossPayout - cost,
      probabilityAnyFirstOrTie, probabilityAnyTopOnePercentOrTie,
      meanRivalDuplicates: lineups.map((lineup) => {
        const key = validateNflLineup(slate, lineup).key;
        return { key, countInFrozenField: field.filter((row) => row.key === key).reduce((sum, row) => sum + row.multiplicity, 0) };
      }) } };
  };
  let selected: NflLineup[] = [];
  if (!hasContest && !rules.exposureCounts?.length && !rules.lockedPlayerIds?.length && !rules.excludedPlayerIds?.length) {
    selected = selectPortfolio(slate, input.candidates, selection, evaluation, {
      count: contest.entryCount, target: input.target, maxPairwiseOverlap: rules.maxPairwiseOverlap,
      objective: input.constructionObjective ?? "max_mean_best_lineup",
    }).selected.map((row) => row.lineup);
  } else {
    // Complete portfolio scoring accounts for our entries competing for the same prizes.
    // Until the portfolio is full, vacant own slots use a losing sentinel in ranking.
    const remaining = new Map(input.candidates.map((lineup, i) => [keys[i], lineup]));
    const candidateDraws = new Map([...remaining].map(([key, lineup]) => [key, scoreNflLineupDraws(slate, lineup, selection)]));
    while (selected.length < contest.entryCount && remaining.size) {
      let best: { key: string; value: number } | null = null;
      for (const [key, lineup] of remaining) {
        const proposed = [...selected, lineup];
        if (!portfolioValid(proposed, rules, proposed.length === contest.entryCount)) continue;
        const proposedDraws = proposed.map((row) => candidateDraws.get(validateNflLineup(slate, row).key)!);
        let value = 0;
        selection.weights.forEach((weight, i) => {
          const scores = proposedDraws.map((row) => row[i]);
          if (selectionField) {
            while (scores.length < contest.entryCount) scores.push(-1e9);
            const outcome = scoreNflContestOutcome(scores, selectionField.map((row) => ({ score: row.draws[i], multiplicity: row.multiplicity })), contest.payouts!, contest.fieldSize!);
            value += weight * (input.contestObjective === "probability_any_top_one_percent" ? Number(outcome.topOnePercentOrBoundaryTie) : outcome.entries.slice(0, proposed.length).reduce((sum, row) => sum + row.grossPayout, 0));
          } else {
            const maximum = Math.max(...scores);
            value += weight * (input.constructionObjective === "max_prob_any_top_threshold" ? Number(maximum >= input.target) : maximum);
          }
        });
        if (!best || value > best.value || (value === best.value && key < best.key)) best = { key, value };
      }
      if (!best) break;
      selected.push(remaining.get(best.key)!); remaining.delete(best.key);
    }
  }
  const complete = selected.length === contest.entryCount && portfolioValid(selected, rules, true);
  return { version: NFL_MATCHUP_CONTEST_VERSION, status: complete ? "shadow_comparison" as const : "incomplete_search" as const,
    capability: hasContest ? "validated_field_conditional" as const : "construction_only" as const,
    dependence: selection.dependence,
    mode: contest.mode, contest, rules, productionChanged: false,
    objective: hasContest ? input.contestObjective ?? "expected_net_payout" : input.constructionObjective ?? "max_mean_best_lineup",
    selected, requestedEntries: contest.entryCount,
    baseline: summarize(input.baseline, evaluation, evaluationField),
    challenger: complete ? summarize(selected, evaluation, evaluationField) : null,
    selectionManifest: selection.metadata, evaluationManifest: evaluation.metadata,
    fieldManifest: hasContest ? { modelVersion: input.field!.modelVersion, snapshotId: input.field!.snapshotId, qualificationId: input.field!.qualificationId, capturedAt: input.field!.capturedAt } : null,
    limitations: ["Shadow recommendation; production entries and projection defaults are unchanged.",
      "Search is bounded by the supplied candidate set; greedy selection is not a proof of optimum or infeasibility.",
      "Outcome estimates depend on supplied model calibration. Field metrics, when available, condition on this frozen field; field-model uncertainty is not simulation noise.",
      ...(independentFallback ? ["Independent player marginal fallback: lineup distributions omit teammate/opponent dependence and are diagnostic only. This is not a coherent game scenario bank; no field-relative claims are permitted."] : []),
      ...(!hasContest ? ["No complete validated field and payout package: contest-win, ROI and duplicate-count claims are unavailable."] : []),
      ...(!complete ? ["Requested count or exposure minima not fulfilled; keep the baseline portfolio."] : [])] };
}
