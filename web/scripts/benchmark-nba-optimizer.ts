import * as optimizerModule from "../src/app/dfs/optimizer";
import * as fixtureModule from "./nba-optimizer-fixtures";
import type { OptimizerSettings } from "../src/app/dfs/optimizer";

const { optimizeLineupsWithDebug } = optimizerModule;
const { assertValidLineups, createSyntheticNbaPool, formatBenchmark } = fixtureModule;

const pool = createSyntheticNbaPool({ gameCount: 8, valuePlayersPerTeam: 3 });
const settings: OptimizerSettings = {
  mode: "gpp",
  nLineups: 20,
  minStack: 2,
  teamStackCount: 1,
  maxExposure: 0.6,
  bringBackEnabled: true,
  bringBackSize: 1,
};

const started = Date.now();
const { lineups, debug } = optimizeLineupsWithDebug(pool, settings);
const wallMs = Date.now() - started;

assertValidLineups(lineups, pool, settings, {
  salaryFloor: debug.effectiveSettings.salaryFloor ?? 49_000,
});

const durations = debug.lineupSummaries.map((lineup) => lineup.durationMs);
const stats = formatBenchmark(durations);

console.log("NBA optimizer benchmark");
console.log(`Pool size: ${pool.length}`);
console.log(`Requested lineups: ${settings.nLineups}`);
console.log(`Built lineups: ${lineups.length}`);
console.log(`Termination: ${debug.terminationReason}`);
console.log(`Wall time: ${wallMs}ms`);
console.log(`Debug total: ${debug.totalMs}ms`);
console.log(`Pruned candidates: ${debug.heuristic?.prunedCandidateCount ?? 0}`);
console.log(`Template count: ${debug.heuristic?.templateCount ?? 0}`);
console.log(`Templates tried: ${debug.heuristic?.templatesTried ?? 0}`);
console.log(`Repair attempts: ${debug.heuristic?.repairAttempts ?? 0}`);
console.log(`Lineup duration min/avg/max/p95: ${stats.min}/${stats.avg}/${stats.max}/${stats.p95}ms`);

if (wallMs > 30_000) {
  console.error(`Benchmark exceeded 30s target: ${wallMs}ms`);
  process.exit(1);
}
