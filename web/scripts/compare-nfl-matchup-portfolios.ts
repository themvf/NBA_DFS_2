/** Saved-input shadow comparison. No database writes, exports, or contest entry. */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { createHash } from "node:crypto";
import { optimizeNflLineups, type NflOptimizerSettings, type NflOptimizerPlayer } from "../src/app/dfs/nfl/nfl-optimizer";
import { evaluateNflOptimizerShadow } from "../src/app/dfs/nfl/nfl-optimizer-shadow";
import { evaluateNflMatchupPortfolio, type NflContestConfig } from "../src/lib/nfl-dfs/matchup-contest";
import { toScenarioLineup } from "../src/app/dfs/nfl/nfl-optimizer-shadow";

const [sourcePath, comparisonPath, outputPath] = process.argv.slice(2);
if (!sourcePath || !comparisonPath || !outputPath) throw new Error("Supply marginal-input.json, slate-comparison.json, output.json");
const sourceText = readFileSync(resolve(sourcePath), "utf8");
const input = JSON.parse(sourceText);
const comparisonText = readFileSync(resolve(comparisonPath), "utf8");
const comparison = JSON.parse(comparisonText);
if (String(input.audit.productionRunId) !== String(comparison.baseline_run_id)) throw new Error("Frozen baseline run differs between banks and matchup comparison.");
const shadowById = new Map<number, any>(comparison.players.map((row: any) => [Number(row.dk_player_id), row.shadow]));
const oldPlayers: NflOptimizerPlayer[] = input.optimizerPlayers;
const coherentMode = input.status === "coherent_research_unqualified";
if (coherentMode && input.manifest?.sources?.comparison_file_sha256 !== createHash("sha256").update(comparisonText).digest("hex")) {
  throw new Error("Coherent bank and player comparison have different frozen input digests.");
}
const coherentMarginals = new Map<number, any>((input.diagnostics?.[0]?.player_marginals ?? []).map((row: any) => [Number(row.dkPlayerId), row]));
const newPlayers = oldPlayers.map((player) => {
  const marginal = coherentMarginals.get(player.dkPlayerId);
  if (coherentMode && marginal) return { ...player, ourProj: marginal.researchMean, floorFpts: marginal.researchP10,
    ceilingFpts: marginal.researchP90, boomRate: null };
  const shadow = shadowById.get(player.dkPlayerId);
  return shadow?.status === "under_evaluation" ? { ...player, ourProj: shadow.candidate.mean,
    floorFpts: shadow.candidate.p10, ceilingFpts: shadow.candidate.p90, boomRate: shadow.candidate.boom } : player;
});
const kickoffTimes = comparison.players.map((row: any) => Date.parse(row.kickoff)).filter(Number.isFinite);
if (!kickoffTimes.length) throw new Error("Actual kickoff data required.");
const lockAt = new Date(Math.min(...kickoffTimes)).toISOString();
const records = [];
let cachedBaseline: ReturnType<typeof optimizeNflLineups> | null = null;
let cachedCandidates: ReturnType<typeof optimizeNflLineups> | null = null;
for (const [mode, count, max] of [["single_entry", 1, 1], ["three_entry", 3, 3], ["multi_entry", 20, 150]] as const) {
  const settings: NflOptimizerSettings = {
    format: input.slate.format, mode: "gpp", projectionSource: "our", allowDkFallback: false,
    nLineups: count, minSalary: 49000, maxExposure: 1, minUnique: 2,
    stackPassCatchers: 1, bringBack: true, randomness: 0, requireObservedHistory: true,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
  };
  cachedBaseline ??= optimizeNflLineups(oldPlayers, { ...settings, nLineups: 20 });
  cachedCandidates ??= optimizeNflLineups(newPlayers, { ...settings, nLineups: 24 });
  // These explicit settings have no exposure minima or caps below 100%; the
  // deterministic prefix is the same legacy construction policy at each count.
  const baseline = { ...cachedBaseline, lineups: cachedBaseline.lineups.slice(0, count), exposureReport: undefined };
  const candidates = cachedCandidates;
  const contest: NflContestConfig = {
    id: `parameterized-${mode}`, platform: "draftkings", scoringVersion: "nfl-dk-scenario-v1", slateId: input.audit.uploadId,
    format: input.slate.format, mode, entryCount: count, maxEntriesPerUser: max, fieldSize: null, entryFee: null, payouts: null,
    tieRule: "split_occupied_prizes", decisionAt: input.selection.decisionAt ?? input.selection.metadata?.decisionAt, lockAt, lateSwap: true, ownershipCapability: "missing",
  };
  const oldEvaluation = evaluateNflMatchupPortfolio({ slate: input.slate,
    baseline: baseline.lineups.map(toScenarioLineup), candidates: baseline.lineups.map(toScenarioLineup),
    contest, selection: input.selection, evaluation: input.evaluation, target: 180,
    rules: { maxPairwiseOverlap: input.slate.format === "classic" ? 7 : 4 }, constructionObjective: "max_mean_best_lineup" });
  const shadow = evaluateNflOptimizerShadow({ slate: input.slate, baseline, candidates, settings, contest,
    selection: input.challengerSelection ?? input.selection, evaluation: input.challengerEvaluation ?? input.evaluation, target: 180 });
  records.push({ mode, settings, baselineGenerated: baseline.lineups, baselinePortfolioUnderBaselineModel: coherentMode ? null : oldEvaluation.baseline,
    baselinePortfolioUnderShadowModel: shadow.baseline, shadowComparison: shadow });
  console.log(JSON.stringify({ mode, baselineEntries: baseline.lineups.length, candidateEntries: candidates.lineups.length,
    status: shadow.status, oldExpectedBest: oldEvaluation.baseline.construction.mean,
    oldPortfolioWithShadowForecast: shadow.baseline.construction.mean,
    selectedExpectedBest: shadow.challenger?.construction.mean ?? null }));
}
const report = { version: "nfl-saved-slate-portfolio-comparison-v1", createdAt: new Date().toISOString(),
  uploadId: input.audit.uploadId, sourceDigest: createHash("sha256").update(sourceText).digest("hex"),
  comparisonDigest: createHash("sha256").update(comparisonText).digest("hex"), inputAudit: input.audit,
  status: coherentMode ? "coherent_research_construction_only" : "construction_only_independent_diagnostic", productionChanged: false,
  evaluationModel: input.evaluation.modelVersion ?? input.evaluation.metadata.modelVersion,
  scenarioManifest: coherentMode ? input.manifest : null,
  assumptions: ["Separate illustrative 1-, 3-, and 20-entry comparisons; actual contest size, fees, payouts and field are not supplied.",
    "Fixed explicit construction settings: $49,000 minimum salary, QB+one pass-catcher, one bring-back, two different players, no randomness, no individual exposure cap beyond one per lineup.",
    coherentMode ? "Both current and reselected portfolios are evaluated under the same separately registered coherent research model. Its changed player/DST marginals have not passed forward qualification; these are not production forecast estimates or contest-win/ROI claims."
      : "Forecast models are independently sampled across players. These are construction sensitivity comparisons, not coherent lineup ceilings, calibrated win probabilities or ROI.",
    "All rows use the audited own-history subset; current-source replay is not a reconstruction of past executable decisions.",
    "The reselected portfolio changes both the forecast inputs and the selection policy. Its gain cannot be attributed to PFR; the separate fixed-candidate-pool attribution report isolates these construction sensitivities.",
    "No entry or export is authorized by these shadow results. Pre-lock availability and complete QA remain required."], records };
writeFileSync(resolve(outputPath), JSON.stringify(report, null, 2));
const lines = ["# Today's DFS construction sensitivity comparison", "", "Shadow research only. No production projection, lineup or contest entry was changed.", "",
  coherentMode ? "The existing portfolio and a reselected portfolio are evaluated under the same coherent research model on independent evaluation draws. This model changes player and DST distributions and has not been qualified as a production forecast. No contest field or payout model is assumed."
    : "The current PFR shadow forecasts and the existing optimizer were compared on the same saved salary slate. Independent player samples omit football correlations, so these results do **not** establish realistic stack ceilings, tournament win probabilities or a profitable strategy.", "",
  `Audited pool: ${input.audit.auditedPlayers} of ${input.audit.inputPlayers} salary rows. Excluded players retain explicit reasons in the JSON.`, "",
  coherentMode ? "| Illustrative entry mode | Current portfolio / research model | Reselected portfolio / research model | Status |"
    : "| Illustrative entry mode | Old portfolio / old forecasts | Old portfolio / PFR shadow | Reselected portfolio / PFR shadow | Status |",
  coherentMode ? "|---|---:|---:|---|" : "|---|---:|---:|---:|---|"];
for (const row of records) lines.push(`| ${row.mode} (${row.settings.nLineups}) | ${coherentMode ? "" : `${row.baselinePortfolioUnderBaselineModel!.construction.mean.toFixed(2)} | `}${row.baselinePortfolioUnderShadowModel.construction.mean.toFixed(2)} | ${row.shadowComparison.challenger?.construction.mean.toFixed(2) ?? "Unavailable"} | ${row.shadowComparison.status} |`);
lines.push("", "Values are simulated average **best score among that mode's entries**, using evaluation draws that were not used to select the challenger. They are neither expected tournament winnings nor individual-player projection gains. Compare within a row only: more entries naturally provide more chances.", "", ...report.assumptions.map((text) => `- ${text}`), "");
writeFileSync(resolve(outputPath).replace(/\.json$/, ".md"), lines.join("\n"));
