import assert from "node:assert/strict";

import {
  getCurrentEligibleNflContext,
  getCurrentNflAvailabilityCoverage,
  getPinnedNflContext,
} from "../src/db/nfl-context";

const targetId = process.argv[2];
if (!targetId) throw new Error("usage: verify-nfl-availability-phase2.ts <game-id>");

const consumers = [
  ["nfl_availability_vercel", "availability_display"],
  ["nfl_availability_dfs", "projection_audit"],
  ["nfl_availability_market", "market_research"],
  ["nfl_availability_props", "prop_research"],
] as const;

async function main() {
  const requestedAsOf = new Date();
  const coverage = await getCurrentNflAvailabilityCoverage(targetId, requestedAsOf);
  assert.ok(coverage.players.length > 0, "expected player availability contexts");
  assert.ok(coverage.quarterbacks.length > 0, "expected team QB contexts");
  const selected = coverage.players[0];
  const identities: string[] = [];
  for (const [consumerId, useCase] of consumers) {
    const current = await getCurrentEligibleNflContext({
      definitionId: selected.measurement.definitionId,
      subjectId: selected.measurement.subject.id,
      targetId,
      consumerId,
      useCase,
      cohort: "all",
      usage: "descriptive",
      requestedAsOf,
    });
    const pinned = await getPinnedNflContext({
      snapshotId: current.manifest.contextSnapshotId,
      consumerId,
      useCase,
      cohort: "all",
      usage: "descriptive",
      requestedAsOf,
    });
    assert.deepEqual(pinned.measurement, current.measurement);
    identities.push(current.manifest.contextSnapshotId);
  }
  assert.equal(new Set(identities).size, 1, "all consumers must resolve one saved snapshot");
  console.log(JSON.stringify({
    targetId,
    playerContexts: coverage.players.length,
    teamQbContexts: coverage.quarterbacks.length,
    stateCounts: coverage.stateCounts,
    sharedSnapshotId: identities[0],
    consumers: consumers.map(([consumerId]) => consumerId),
    pinnedReplay: "passed",
  }, null, 2));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
