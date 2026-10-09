/** Hold the legal candidate pool fixed to separate forecast and selection effects. */
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { evaluateNflOptimizerShadow, toScenarioLineup } from "../src/app/dfs/nfl/nfl-optimizer-shadow";
import { validateNflLineup } from "../src/lib/nfl-dfs/lineups";
const [bankPath, reportPath, outputPath] = process.argv.slice(2);
const banks = JSON.parse(readFileSync(bankPath, "utf8"));
const originalText = readFileSync(reportPath, "utf8"), original = JSON.parse(originalText);
if (banks.audit.productionRunId !== original.inputAudit.productionRunId) throw new Error("Baseline mismatch");
const pool = new Map();
for (const row of original.records) for (const lineup of [...row.baselineGenerated, ...row.shadowComparison.selectedGenerated]) {
  pool.set(validateNflLineup(banks.slate, toScenarioLineup(lineup)).key, lineup);
}
const records = original.records.map((row: any) => {
  const count = row.settings.nLineups;
  const contest = { id: `attribution-${row.mode}`, platform: "draftkings" as const, scoringVersion: "nfl-dk-scenario-v1", slateId: original.uploadId,
    format: banks.slate.format, mode: row.mode, entryCount: count, maxEntriesPerUser: count === 20 ? 150 : count,
    fieldSize: null, entryFee: null, payouts: null, tieRule: "split_occupied_prizes" as const,
    decisionAt: banks.selection.metadata.decisionAt, lockAt: row.shadowComparison.contest.lockAt, lateSwap: true, ownershipCapability: "missing" as const };
  const sourceCoverage = { requested: 0, direct: 0, fallback: 0, excluded: 0 };
  const base = { lineups: row.baselineGenerated, warnings: [], sourceCoverage };
  const candidates = { lineups: [...pool.values()], warnings: [], sourceCoverage };
  const common = { slate: banks.slate, baseline: base, candidates, settings: row.settings, contest, target: 180 };
  const policy = evaluateNflOptimizerShadow({ ...common, selection: banks.selection, evaluation: banks.evaluation });
  const modelAndPolicy = evaluateNflOptimizerShadow({ ...common, selection: banks.challengerSelection, evaluation: banks.challengerEvaluation });
  return { mode: row.mode, count, candidatePool: pool.size,
    originalUnderBaseline: policy.baseline.construction.mean,
    originalUnderPfr: modelAndPolicy.baseline.construction.mean,
    policyOnlyExpectedBest: policy.challenger?.construction.mean ?? null,
    policyAndPfrExpectedBest: modelAndPolicy.challenger?.construction.mean ?? null,
    baselinePolicyLineups: policy.selected, pfrPolicyLineups: modelAndPolicy.selected,
    status: policy.status === "shadow_comparison" && modelAndPolicy.status === "shadow_comparison" ? "construction_only" : "withheld" };
});
writeFileSync(outputPath, JSON.stringify({ version: "nfl-fixed-pool-attribution-v1", productionChanged: false,
  baselineRunId: banks.audit.productionRunId, originalReportDigest: createHash("sha256").update(originalText).digest("hex"),
  limitation: "A fixed union of previously generated candidates isolates selection/forecast sensitivity within this inspected candidate set. Independent player draws omit football dependence. This is neither causal proof of PFR improvement nor tournament profit/win evidence.", records }, null, 2));
writeFileSync(outputPath.replace(/\.json$/, ".md"), ["# Separate forecast and selection effects", "",
  `All arms below use the same ${pool.size} legal candidate lineups, the same optimizer constraints and independent evaluation draws. This inspected union differs from the earlier 24-new-candidate comparison. Values are simulated mean best portfolio scores, not prize or win estimates.`, "",
  "| Entries | Original / baseline | Original / PFR | Reselection / baseline | Reselection / PFR |", "|---|---:|---:|---:|---:|",
  ...records.map((r: any) => `| ${r.count} | ${r.originalUnderBaseline.toFixed(2)} | ${r.originalUnderPfr.toFixed(2)} | ${r.policyOnlyExpectedBest?.toFixed(2) ?? "Unavailable"} | ${r.policyAndPfrExpectedBest?.toFixed(2) ?? "Unavailable"} |`), "",
  "The large change comes mainly from a different selection objective. The additional PFR forecast effect is small and can be negative. Independent player draws omit game dependence; this comparison does not establish improved accuracy or profitability.", ""].join("\n"));
console.log(JSON.stringify(records.map(({ baselinePolicyLineups, pfrPolicyLineups, ...rest }: any) => rest)));
