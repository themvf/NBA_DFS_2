/**
 * Availability coverage, and the export gate that reads it.
 *
 * The 2026 week-2 Sunday classic slate came back with all 670 players UNKNOWN
 * and none fresh, and the workspace built 20 lineups without that reaching the
 * screen the button was on.
 */
import assert from "node:assert/strict";
import {
  availabilityCoverage,
  RESOLVED_BLIND_THRESHOLD,
  RESOLVED_THIN_THRESHOLD,
  FRESH_THIN_THRESHOLD,
  type CoveragePlayer,
} from "../src/lib/nfl-dfs/availability-coverage";
import { runNflPreExportQa } from "../src/lib/nfl-dfs/pre-export-qa";

const p = (status: string | null, fresh: boolean | null = true, isOut = false): CoveragePlayer =>
  ({ isOut, availability: status === null ? null : { status, fresh } });

const many = (n: number, make: (i: number) => CoveragePlayer) => Array.from({ length: n }, (_, i) => make(i));

function main() {
  // --- The real slate: every player unknown, nothing fresh ------------------
  const blind = availabilityCoverage(many(670, () => p("UNKNOWN", false)));
  assert.equal(blind.considered, 670);
  assert.equal(blind.resolved, 0, "UNKNOWN is the absence of a status, not a status");
  assert.equal(blind.state, "blind");
  assert.match(blind.headline, /do not know who is playing/i);
  assert.equal(blind.metric, "0/670");

  // Absent and empty availability read the same way as UNKNOWN.
  assert.equal(availabilityCoverage(many(10, () => p(null))).state, "blind");
  assert.equal(availabilityCoverage(many(10, () => p(""))).state, "blind");
  assert.equal(availabilityCoverage([{}, {}, {}]).state, "blind");

  // --- A healthy slate ------------------------------------------------------
  const good = availabilityCoverage(many(50, () => p("ACTIVE", true)));
  assert.equal(good.state, "adequate");
  assert.equal(good.resolved, 50);
  assert.equal(good.fresh, 50);
  assert.equal(good.metric, "50/50", "no fresh suffix when everything resolved is fresh");

  // --- Resolved but stale is thin, not adequate ------------------------------
  const stale = availabilityCoverage(many(50, () => p("ACTIVE", false)));
  assert.equal(stale.resolved, 50);
  assert.equal(stale.fresh, 0);
  assert.equal(stale.state, "thin", "last week's roster is coverage of the wrong week");
  assert.equal(stale.metric, "50/50 · 0 fresh");

  // --- Freshness is only counted where a status exists ----------------------
  // "Fresh evidence of nothing" is not coverage: we fetched and got nothing.
  const freshNothing = availabilityCoverage(many(40, () => p("UNKNOWN", true)));
  assert.equal(freshNothing.fresh, 0);
  assert.equal(freshNothing.state, "blind");

  // --- Ruled-out players leave the denominator ------------------------------
  // Otherwise a slate looks better covered the more players are missing.
  const withOuts = availabilityCoverage([
    ...many(10, () => p("ACTIVE", true)),
    ...many(90, () => p("UNKNOWN", false, true)),
  ]);
  assert.equal(withOuts.considered, 10);
  assert.equal(withOuts.ruledOut, 90);
  assert.equal(withOuts.state, "adequate", "the 90 are settled, not unknown");

  // --- Thresholds are the stated boundaries ---------------------------------
  const atBlind = availabilityCoverage([
    ...many(25, () => p("ACTIVE", true)), ...many(75, () => p("UNKNOWN", false)),
  ]);
  assert.equal(atBlind.resolvedShare, RESOLVED_BLIND_THRESHOLD);
  assert.equal(atBlind.state, "thin", "at the threshold, not below it");
  const belowBlind = availabilityCoverage([
    ...many(24, () => p("ACTIVE", true)), ...many(76, () => p("UNKNOWN", false)),
  ]);
  assert.equal(belowBlind.state, "blind");

  const atThin = availabilityCoverage([
    ...many(75, () => p("ACTIVE", true)), ...many(25, () => p("UNKNOWN", false)),
  ]);
  assert.equal(atThin.resolvedShare, RESOLVED_THIN_THRESHOLD);
  assert.equal(atThin.state, "adequate");

  // Fresh share has its own boundary, independent of resolved share.
  const halfFresh = availabilityCoverage([
    ...many(50, () => p("ACTIVE", true)), ...many(50, () => p("ACTIVE", false)),
  ]);
  assert.equal(halfFresh.freshShare, FRESH_THIN_THRESHOLD);
  assert.equal(halfFresh.state, "adequate", "at the threshold");
  const underFresh = availabilityCoverage([
    ...many(49, () => p("ACTIVE", true)), ...many(51, () => p("ACTIVE", false)),
  ]);
  assert.equal(underFresh.state, "thin", "fully resolved but mostly stale is still thin");

  // --- An empty slate is blind, not adequate --------------------------------
  const empty = availabilityCoverage([]);
  assert.equal(empty.state, "blind");
  assert.equal(empty.considered, 0);

  // --- The export gate ------------------------------------------------------
  const lineup = {
    lineupNumber: 1, playerIds: [1, 2, 3, 4, 5, 6], totalSalary: 49000,
    slots: [1, 2, 3, 4, 5, 6].map((id) => ({ slot: "FLEX", playerId: id })),
  };
  const qa = (cov?: Parameters<typeof runNflPreExportQa>[0]["availabilityCoverage"]) =>
    runNflPreExportQa({ format: "showdown", requestedLineups: 1, lineups: [lineup], availabilityCoverage: cov });

  const blocked = qa({ state: "blind", resolved: 0, considered: 670, fresh: 0 });
  const check = blocked.checks.find((c) => c.id === "availability_coverage")!;
  assert.equal(check.severity, "blocker");
  assert.equal(blocked.decision, "blocked");
  assert.ok(blocked.openBlockers.includes("availability_coverage"));
  assert.match(check.detail, /0 of 670/);

  // Overridable: exporting a blind slate is a legitimate CHOICE, so long as it
  // is one. The failure was that nothing asked.
  assert.equal(check.overridable, true);
  const overridden = runNflPreExportQa(
    { format: "showdown", requestedLineups: 1, lineups: [lineup],
      availabilityCoverage: { state: "blind", resolved: 0, considered: 670, fresh: 0 } },
    [{ checkId: "availability_coverage", reason: "known", user: "u", at: "now", rulesetVersion: "v", runId: "r" }],
  );
  assert.equal(overridden.openBlockers.length, 0);

  assert.equal(qa({ state: "thin", resolved: 40, considered: 100, fresh: 10 })
    .checks.find((c) => c.id === "availability_coverage")!.severity, "warning");
  assert.equal(qa({ state: "adequate", resolved: 100, considered: 100, fresh: 100 })
    .checks.some((c) => c.id === "availability_coverage"), false, "nothing to say when covered");
  // Not supplied is not the same as blind, and must not block a legacy caller.
  assert.equal(qa(undefined).checks.some((c) => c.id === "availability_coverage"), false);

  console.log("Availability coverage:");
  console.log("  - UNKNOWN/absent/empty all read as no status, and 670 of them is 'blind'");
  console.log("  - resolved and fresh are counted separately; they fail separately");
  console.log("  - ruled-out players leave the denominator");
  console.log("  - a blind slate blocks export, and the block is overridable on purpose");
}

main();
