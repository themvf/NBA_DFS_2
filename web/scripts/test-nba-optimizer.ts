import * as optimizerModule from "../src/app/dfs/optimizer";
import * as fixtureModule from "./nba-optimizer-fixtures";
import type { OptimizerSettings } from "../src/app/dfs/optimizer";

const { optimizeLineupsWithDebug } = optimizerModule;
const { assertValidLineups, buildRequiredTeamStack, createSyntheticNbaPool, findPlayerId } = fixtureModule;

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

type TestCase = {
  name: string;
  run: () => void;
};

function runSingleStackBringBack() {
  const pool = createSyntheticNbaPool({ gameCount: 8, valuePlayersPerTeam: 3 });
  const settings: OptimizerSettings = {
    mode: "gpp",
    nLineups: 4,
    minStack: 2,
    teamStackCount: 1,
    maxExposure: 0.75,
    bringBackEnabled: true,
    bringBackSize: 1,
  };

  const { lineups, debug } = optimizeLineupsWithDebug(pool, settings);
  assert(lineups.length === settings.nLineups, `expected ${settings.nLineups} lineups, got ${lineups.length}`);
  assert(debug.terminationReason === "completed", `unexpected termination: ${debug.terminationReason}`);
  assertValidLineups(lineups, pool, settings, {
    salaryFloor: debug.effectiveSettings.salaryFloor ?? 49_000,
  });
}

function runDoubleStackBringBack() {
  const pool = createSyntheticNbaPool({ gameCount: 4, valuePlayersPerTeam: 3 });
  const settings: OptimizerSettings = {
    mode: "gpp",
    nLineups: 4,
    minStack: 3,
    teamStackCount: 2,
    maxExposure: 0.8,
    bringBackEnabled: true,
    bringBackSize: 1,
  };

  const { lineups, debug } = optimizeLineupsWithDebug(pool, settings);
  assert(lineups.length === settings.nLineups, `expected ${settings.nLineups} lineups, got ${lineups.length}`);
  assert(debug.terminationReason === "completed", `unexpected termination: ${debug.terminationReason}`);
  assertValidLineups(lineups, pool, settings, {
    salaryFloor: debug.effectiveSettings.salaryFloor ?? 49_000,
  });
}

function runLockBlockTeamRules() {
  const pool = createSyntheticNbaPool({ gameCount: 8, valuePlayersPerTeam: 3 });
  const lockedPlayerId = findPlayerId(pool, "T01", "Primary PG");
  const blockedPlayerId = findPlayerId(pool, "T02", "Scoring SG");
  const requiredTeamId = 3;
  const settings: OptimizerSettings = {
    mode: "gpp",
    nLineups: 4,
    minStack: 2,
    teamStackCount: 1,
    maxExposure: 0.8,
    bringBackEnabled: false,
    bringBackSize: 1,
    playerLocks: [lockedPlayerId],
    playerBlocks: [blockedPlayerId],
    blockedTeamIds: [16],
    requiredTeamStacks: [buildRequiredTeamStack(requiredTeamId, 2)],
  };

  const { lineups, debug } = optimizeLineupsWithDebug(pool, settings);
  assert(lineups.length === settings.nLineups, `expected ${settings.nLineups} lineups, got ${lineups.length}`);
  assert(debug.terminationReason === "completed", `unexpected termination: ${debug.terminationReason}`);
  assertValidLineups(lineups, pool, settings, {
    salaryFloor: debug.effectiveSettings.salaryFloor ?? 49_000,
  });
}

function runExposureExhaustion() {
  const pool = createSyntheticNbaPool({ gameCount: 3, valuePlayersPerTeam: 1 })
    .filter((player) => player.salary >= 4300 || player.teamId === 1 || player.teamId === 2);
  const settings: OptimizerSettings = {
    mode: "gpp",
    nLineups: 20,
    minStack: 2,
    teamStackCount: 1,
    maxExposure: 0.25,
    bringBackEnabled: true,
    bringBackSize: 1,
  };

  const { lineups, debug } = optimizeLineupsWithDebug(pool, settings);
  assert(lineups.length > 0, "expected at least one lineup before exposure exhaustion");
  assert(lineups.length < settings.nLineups, "expected partial generation under exposure exhaustion");
  assert(debug.terminationReason === "lineup_failed", `unexpected termination: ${debug.terminationReason}`);
  assertValidLineups(lineups, pool, {
    ...settings,
    nLineups: settings.nLineups,
  }, {
    salaryFloor: debug.effectiveSettings.salaryFloor ?? 49_000,
  });
}

const testCases: TestCase[] = [
  { name: "single-stack-with-bringback", run: runSingleStackBringBack },
  { name: "double-stack-with-bringback", run: runDoubleStackBringBack },
  { name: "locks-blocks-required-team-stack", run: runLockBlockTeamRules },
  { name: "partial-generation-under-exposure", run: runExposureExhaustion },
];

for (const testCase of testCases) {
  const started = Date.now();
  testCase.run();
  const duration = Date.now() - started;
  console.log(`PASS ${testCase.name} (${duration}ms)`);
}

console.log(`PASS ${testCases.length} NBA optimizer fixture tests`);
