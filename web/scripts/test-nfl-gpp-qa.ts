/**
 * Phase 6 tests (spec §13): pre-export portfolio QA.
 * Covers P6-AC1..P6-AC5 plus severity/override units.
 */
import assert from "node:assert/strict";
import { currentPoolForQa, currentUnavailableReason, nflOverlapCap, runNflPreExportQa, NFL_QA_RULESET_VERSION, type QaInput, type QaOverride } from "../src/lib/nfl-dfs/pre-export-qa";

function legalLineup(n: number, ids: number[]): QaInput["lineups"][number] {
  return { lineupNumber: n, playerIds: ids, totalSalary: 49000, slots: ids.map((id, i) => ({ slot: i === 0 ? "CPT" : `FLEX${i}`, playerId: id })), archetype: { id: "standard_ceiling", label: "Standard ceiling", fadedPlayerIds: [], beneficiariesSatisfied: [] } };
}

function baseInput(over: Partial<QaInput> = {}): QaInput {
  return {
    format: "showdown", requestedLineups: 2,
    lineups: [legalLineup(1, [1, 2, 3, 4, 5, 6]), legalLineup(2, [1, 2, 3, 4, 5, 7])],
    eligibility: [], ...over,
  };
}

function main() {
  // --- A clean portfolio is Ready ---
  const clean = runNflPreExportQa(baseInput());
  assert.equal(clean.decision, "ready");
  assert.equal(clean.rulesetVersion, NFL_QA_RULESET_VERSION);
  assert.equal(clean.openBlockers.length, 0);

  // --- P6-AC1: an unapproved $400 player in a lineup blocks export ---
  const puntInput = baseInput({
    eligibility: [{ dkPlayerId: 6, name: "$400 body", eligible: false, reasonCode: "ABSOLUTE_SALARY_BLOCK", overridden: false }],
  });
  const puntQa = runNflPreExportQa(puntInput);
  assert.equal(puntQa.decision, "blocked");
  assert.ok(puntQa.openBlockers.includes("no_unapproved_punt"));

  // Overriding the punt check clears it (it is overridable).
  const override: QaOverride[] = [{ checkId: "no_unapproved_punt", reason: "verified returner role", user: "t", at: "2026-09-20T00:00:00Z", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }];
  const puntOverridden = runNflPreExportQa(puntInput, override);
  assert.ok(!puntOverridden.openBlockers.includes("no_unapproved_punt"), "override clears the punt blocker");

  // --- Inactive is a blocker that is NEVER overridable ---
  const inactiveInput = baseInput({ eligibility: [{ dkPlayerId: 6, name: "OUT guy", eligible: false, reasonCode: "INACTIVE", overridden: false }] });
  const inactiveQa = runNflPreExportQa(inactiveInput, [{ checkId: "no_inactive", reason: "nope", user: "t", at: "x", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }]);
  assert.ok(inactiveQa.openBlockers.includes("no_inactive"), "inactive cannot be overridden");

  // --- P6-AC2: exposure/archetype shortfalls name the exact unsatisfied constraint ---
  const exposureQa = runNflPreExportQa(baseInput({ exposureReport: [{ dkPlayerId: 3, name: "Player 3", binding: "captain min missed (0/2)" }] }));
  assert.ok(exposureQa.openBlockers.includes("exposure_ranges"));
  const exposureCheck = exposureQa.checks.find((c) => c.id === "exposure_ranges")!;
  assert.ok(/Player 3/.test(exposureCheck.detail) && /captain min missed/.test(exposureCheck.detail));

  const quotaQa = runNflPreExportQa(baseInput({ archetypePlan: [{ archetypeId: "single_chalk_fade", label: "Single-chalk fade", requested: 2, realized: 1 }] }));
  const quotaCheck = quotaQa.checks.find((c) => c.id === "archetype_quotas")!;
  assert.ok(/Single-chalk fade 1\/2/.test(quotaCheck.detail));

  // --- Fade without a beneficiary is blocked ---
  const fadeInput = baseInput();
  fadeInput.lineups[0].archetype = { id: "single_chalk_fade", label: "Single-chalk fade", fadedPlayerIds: [9], beneficiariesSatisfied: [] };
  const fadeQa = runNflPreExportQa(fadeInput);
  assert.ok(fadeQa.openBlockers.includes("fade_beneficiary"));

  // --- Illegal roster is a non-overridable blocker ---
  const illegal = baseInput();
  illegal.lineups[0].totalSalary = 51000;
  const illegalQa = runNflPreExportQa(illegal, [{ checkId: "legal_roster", reason: "x", user: "t", at: "x", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }]);
  assert.ok(illegalQa.openBlockers.includes("legal_roster"), "illegal roster never overridable");

  // --- Ownership: invalid + leverage on is a blocker; unavailable is info ---
  const invalidLev = runNflPreExportQa(baseInput({ ownership: { capability: "validated", errors: ["bad captain total"], features: { leverage: true } } }));
  assert.ok(invalidLev.openBlockers.includes("ownership_leverage_valid"));
  const projOnly = runNflPreExportQa(baseInput({ ownership: { capability: "unavailable", errors: [], features: { leverage: false } } }));
  assert.ok(projOnly.checks.some((c) => c.id === "ownership_unavailable" && c.severity === "info"));

  // --- P6-AC3: changing inputs re-runs QA (pure fn: different input -> different report) ---
  const before = runNflPreExportQa(baseInput());
  const after = runNflPreExportQa(baseInput({ exposureReport: [{ dkPlayerId: 1, name: "P1", binding: "overall min missed (0/1)" }] }));
  assert.notEqual(before.decision, after.decision, "a settings change yields a different QA decision");

  // --- P6-AC4: the report is fully reconstructable from persisted inputs (deterministic) ---
  const a = runNflPreExportQa(puntInput, override);
  const b = runNflPreExportQa(puntInput, override);
  assert.deepEqual(a, b, "QA is deterministic and reconstructable");

  // --- P6-AC5: projection-only cannot claim leverage (info check present, no leverage blocker) ---
  assert.ok(!projOnly.openBlockers.includes("ownership_leverage_valid"));

  // Warning-only portfolio is "ready_with_warnings".
  const warnOnly = runNflPreExportQa(baseInput({ salaryBandReport: [{ band: { min: 0, max: 400 }, withinPlan: false, count: 5, minCount: 0, maxCount: 1 }] }));
  assert.equal(warnOnly.decision, "ready_with_warnings");

  // --- Export requires the live site's code (2026-09-27 lineups came from a stale local checkout) ---
  const local = runNflPreExportQa(baseInput({ build: { commitSha: "local-uncommitted" } }));
  assert.equal(local.decision, "blocked");
  assert.ok(local.openBlockers.includes("live_build"));
  assert.equal(local.checks.find((c) => c.id === "live_build")!.overridable, false, "a local build cannot be overridden into export");
  const stillLocal = runNflPreExportQa(baseInput({ build: { commitSha: "local-uncommitted" } }),
    [{ checkId: "live_build", reason: "trust me", user: "t", at: "2026-09-29T00:00:00Z", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }]);
  assert.ok(stillLocal.openBlockers.includes("live_build"), "an override does not clear it");
  assert.equal(runNflPreExportQa(baseInput({ build: { commitSha: null } })).decision, "blocked", "no recorded version is not the live site");
  assert.equal(runNflPreExportQa(baseInput({ build: { commitSha: "aad997934e7dabffd111312c3fff43226c2edca5" } })).decision, "ready");
  assert.equal(runNflPreExportQa(baseInput()).checks.some((c) => c.id === "live_build"), false, "not supplied is not checked");

  const note = (id: string, r: ReturnType<typeof runNflPreExportQa>) => r.checks.find((c) => c.id === id);
  const overrideOf = (checkId: string): QaOverride[] => [{ checkId, reason: "entering fewer on purpose", user: "t", at: "2026-09-29T00:00:00Z", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }];

  // --- A player ruled out AFTER the build blocks export (2026-09-29 audit: eligibility was generation-time only) ---
  const pool = (over: Record<number, string | null> = {}) => ({ players: [1, 2, 3, 4, 5, 6, 7].map((id) => ({ dkPlayerId: id, name: `Player ${id}`, unavailable: over[id] ?? null })) });
  const nowOut = runNflPreExportQa(baseInput({ currentPool: pool({ 3: "DraftKings lists him OUT" }) }));
  assert.ok(nowOut.openBlockers.includes("current_pool"));
  assert.match(note("current_pool", nowOut)!.detail, /^Player 3 \(DraftKings lists him OUT\) is in lineups 1, 2\. Build again/);
  assert.ok(runNflPreExportQa(baseInput({ currentPool: pool({ 3: "DraftKings lists him OUT" }) }), overrideOf("current_pool")).openBlockers.includes("current_pool"), "a player who is out cannot be overridden into export");
  const gone = runNflPreExportQa(baseInput({ currentPool: { players: pool().players.filter((p) => p.dkPlayerId !== 7) } }));
  assert.match(note("current_pool", gone)!.detail, /Player 7 \(no longer on this slate's player pool\) is in lineup 2/);
  assert.equal(note("current_pool", runNflPreExportQa(baseInput({ currentPool: pool() })))!.passed, true);
  assert.equal(runNflPreExportQa(baseInput()).checks.some((c) => c.id === "current_pool"), false, "not supplied is not checked");
  const unread = runNflPreExportQa(baseInput({ currentPool: null }));
  assert.equal(unread.decision, "ready_with_warnings", "an unreadable pool is said, not passed");
  assert.equal(currentUnavailableReason({ dkPlayerId: 1, name: "a", isOut: true, dkStatus: "ir" }), "DraftKings lists him IR");
  assert.equal(currentUnavailableReason({ dkPlayerId: 1, name: "a", isOut: true, availability: { blockedReason: "Listed QB2; starter workload not supported" } }), "Listed QB2; starter workload not supported");
  assert.equal(currentUnavailableReason({ dkPlayerId: 1, name: "a", isOut: false, projectionStatus: "out" }), "ruled out by our injury feed");
  assert.equal(currentUnavailableReason({ dkPlayerId: 1, name: "a", isOut: false, projectionStatus: "historical" }), null);
  assert.equal(currentPoolForQa([{ dkPlayerId: 9, name: "Nine", isOut: false }]).players[0].unavailable, null);
  // A QB the depth chart now lists as a backup: a judgement the chart can get wrong, so overridable with a reason.
  const backupPool = currentPoolForQa([1, 2, 3, 4, 5, 6, 7].map((id) => id === 3
    ? { dkPlayerId: id, name: "Backup QB", isOut: true, ruledOut: false, availability: { blockedReason: "Listed QB2; starter workload not supported" } }
    : { dkPlayerId: id, name: `Player ${id}`, isOut: false }));
  assert.equal(backupPool.players[2].roleOnly, true);
  const backupQa = runNflPreExportQa(baseInput({ currentPool: backupPool }));
  assert.ok(backupQa.openBlockers.includes("current_pool_role"));
  assert.equal(note("current_pool", backupQa)!.passed, true, "a backup is not 'out'");
  assert.match(note("current_pool_role", backupQa)!.detail, /^Backup QB \(Listed QB2; starter workload not supported\) is in lineups 1, 2\. Confirm the starter/);
  assert.ok(!runNflPreExportQa(baseInput({ currentPool: backupPool }), overrideOf("current_pool_role")).openBlockers.includes("current_pool_role"));
  // An injured QB is never "role only", even though his block text is a role block elsewhere.
  assert.equal(currentPoolForQa([{ dkPlayerId: 3, name: "Hurt", isOut: true, ruledOut: true, availability: { blockedReason: "Unavailable: OUT" } }]).players[0].roleOnly, false);

  // --- Missing run evidence reads as "can't be checked", never as passed (40 of 51 saved runs) ---
  const noEvidence = runNflPreExportQa(baseInput({ eligibility: null, exposureReport: null }));
  assert.equal(noEvidence.decision, "ready_with_warnings");
  assert.equal(note("no_inactive", noEvidence)!.passed, false);
  assert.match(note("no_inactive", noEvidence)!.detail, /can't be checked/);
  assert.equal(note("exposure_ranges", noEvidence)!.passed, false);
  assert.equal(runNflPreExportQa(baseInput({ eligibility: undefined })).checks.some((c) => c.id === "no_inactive"), false);

  // --- A partial run blocks until the shortfall is a stated choice ---
  const partial = runNflPreExportQa(baseInput({ requestedLineups: 20 }));
  assert.ok(partial.openBlockers.includes("lineup_count"));
  assert.match(note("lineup_count", partial)!.detail, /Only 2 of the 20 lineups you asked for were built/);
  assert.ok(!runNflPreExportQa(baseInput({ requestedLineups: 20 }), overrideOf("lineup_count")).openBlockers.includes("lineup_count"), "overridable with a reason");
  assert.equal(note("lineup_count", clean)!.passed, true);

  // --- Entry file: too few rows can't export; spare rows are a choice ---
  const tooFew = runNflPreExportQa(baseInput({ entryRows: 1 }));
  assert.ok(tooFew.openBlockers.includes("entry_rows"));
  assert.equal(note("entry_rows", tooFew)!.overridable, false);
  const spare = runNflPreExportQa(baseInput({ entryRows: 21 }));
  assert.match(note("entry_rows", spare)!.detail, /19 entries would keep whatever lineup DraftKings already has/);
  assert.ok(!runNflPreExportQa(baseInput({ entryRows: 21 }), overrideOf("entry_rows")).openBlockers.includes("entry_rows"));
  assert.equal(note("entry_rows", runNflPreExportQa(baseInput({ entryRows: 2 })))!.passed, true);
  assert.equal(clean.checks.some((c) => c.id === "entry_rows"), false, "no file chosen yet, nothing to check");

  // --- Archetype quotas: omitted when no plan, "can't be checked" when unrecorded ---
  assert.equal(clean.checks.some((c) => c.id === "archetype_quotas"), false, "a plan that was never used is not shown as met");
  assert.equal(note("archetype_quotas", runNflPreExportQa(baseInput({ archetypePlan: null })))!.severity, "warning");

  // --- Overlap is measured from the lineups, not taken on trust ---
  const dupes = baseInput({ lineups: [legalLineup(1, [1, 2, 3, 4, 5, 6]), legalLineup(2, [1, 2, 3, 4, 5, 6])] });
  assert.ok(runNflPreExportQa(dupes).openBlockers.includes("overlap"), "exact duplicates block even without a recorded overlap");
  assert.ok(runNflPreExportQa({ ...baseInput(), overlapCap: nflOverlapCap("showdown", 2) }).openBlockers.includes("overlap"), "5 shared > the min-unique-2 cap of 4");
  assert.equal(nflOverlapCap("classic", 2), 7); assert.equal(nflOverlapCap("showdown", 1), 5); assert.equal(nflOverlapCap("classic", 1, 6), 6);

  // --- Projection age reaches QA ---
  const aged = runNflPreExportQa(baseInput({ projectionStale: true, projectionAgeHours: 14.2 }));
  assert.equal(aged.decision, "ready_with_warnings");
  assert.match(note("projection_freshness", aged)!.detail, /14 hours old/);
  assert.ok(runNflPreExportQa(baseInput({ projectionStale: true, projectionAgeHours: 50 })).openBlockers.includes("projection_freshness"));
  assert.match(note("projection_freshness", runNflPreExportQa(baseInput({ projectionAgeHours: 3 })))!.detail, /built 3 hours ago/);

  console.log("NFL GPP Phase 6 (pre-export QA): P6-AC1..AC5, severity/override units, current-pool, partial-run, entry-file, missing-evidence and overlap checks passed.");
}

main();
