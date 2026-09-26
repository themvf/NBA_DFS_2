import assert from "node:assert/strict";

import {
  getCurrentEligibleNflContext,
  getPinnedNflContext,
} from "../src/db/nfl-context";

const targetId = process.argv[2] ?? "2026_02_NYG_LA";
const requestedAsOf = new Date();

async function main() {
  const current = await Promise.all(
    ["NYG", "LA"].map(subjectId =>
      getCurrentEligibleNflContext({
        definitionId: "neutral_offensive_snap_interval_seconds@v1",
        subjectId,
        targetId,
        consumerId: "nfl_pbp_explorer",
        useCase: "game_explanation",
        cohort: "all_teams",
        usage: "descriptive",
        requestedAsOf,
      }),
    ),
  );
  assert.equal(current.length, 2);
  for (const value of current) {
    assert.equal(value.manifest.eligibility.approved, true);
    assert.equal(value.manifest.eligibility.usage, "descriptive");
    assert.equal(value.measurement.targetId, targetId);
    assert.ok(value.measurement.measurement.denominator! > 0);
    const replay = await getPinnedNflContext({
      snapshotId: value.manifest.contextSnapshotId,
      consumerId: "nfl_pbp_explorer",
      useCase: "game_explanation",
      cohort: "all_teams",
      usage: "descriptive",
      requestedAsOf,
    });
    assert.deepEqual(replay.measurement, value.measurement);
  }

  await assert.rejects(
    getCurrentEligibleNflContext({
      definitionId: "neutral_offensive_snap_interval_seconds@v1",
      subjectId: "NYG",
      targetId,
      consumerId: "nfl_dfs_optimizer",
      useCase: "lineup_selection",
      cohort: "classic",
      usage: "decision",
      requestedAsOf,
    }),
    /Unqualified NFL context dependency/,
  );

  console.log(
    JSON.stringify(
      {
        targetId,
        snapshots: current.map(value => ({
          team: value.measurement.subject.id,
          value: value.measurement.measurement.value,
          denominator: value.measurement.measurement.denominator,
          snapshotId: value.manifest.contextSnapshotId,
          factReleaseId: value.manifest.factReleaseId,
          policyVersion: value.manifest.policyVersion,
        })),
        pinnedReplay: "passed",
        unauthorizedDecisionRead: "rejected",
      },
      null,
      2,
    ),
  );
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
