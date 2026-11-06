import assert from "node:assert/strict";
import { applyTeamKickerContext, identifyTeamKickersByElimination, kickerRoleBlockedReason, presentPinnedGameAvailability,
  resolveAvailability, KICKER_BY_ELIMINATION_ROLE, type RosterEvidence } from "../src/lib/nfl-dfs/availability";

// nfl-kicker-by-elimination-v1: Jake Moody, BAL, 2026 week 9. Loop on IR, Moody signed that week with no depth number.
const now = Date.parse("2026-11-06T00:38:00Z");
const ev = (sleeper: Record<string, unknown>, fetchedAt = "2026-11-06T00:30:00Z"): RosterEvidence =>
  ({ team: "BAL", position: "K", fetchedAt, sleeper: { team: "BAL", position: "K", status: "Active", ...sleeper } });
const avail = (name: string, sleeper: Record<string, unknown>, fetchedAt?: string) =>
  ({ team: "BAL", position: "K", name, availability: resolveAvailability(ev(sleeper, fetchedAt), "BAL", "K", now) });

const moody = avail("Jake Moody", {});
const loop = avail("Tyler Loop", { status: "IR", depth_chart_order: 1 });
assert.match(moody.availability.blockedReason!, /Kicking role unresolved/, "before the rule Moody is blocked on role alone");
assert.match(loop.availability.blockedReason!, /Unavailable: IR/);

const little = { team: "JAX", position: "K", name: "Cam Little", availability: resolveAvailability({ ...ev({ depth_chart_order: 1 }), team: "JAX", sleeper: { team: "JAX", position: "K", status: "Active", depth_chart_order: 1 } }, "JAX", "K", now) };
const elim = identifyTeamKickersByElimination([moody, loop, little]);
assert.deepEqual([...elim.keys()], ["BAL"], "only the team with an eliminated kicker resolves");
assert.equal(elim.get("BAL")!.name, "Jake Moody");
assert.deepEqual(elim.get("BAL")!.unavailable, ["Tyler Loop (IR)"]);

const resolved = applyTeamKickerContext(moody.availability, "K", "Jake Moody", elim.get("BAL"));
assert.equal(resolved.role, KICKER_BY_ELIMINATION_ROLE);
assert.equal(resolved.blockedReason, null);
assert.equal(resolved.roleBlockedReason, null);
assert.equal(resolved.chartRole, undefined, "chartRole stays unset so kickerRoleBlockedReason reads the new role");
assert.match(resolved.warnings!.at(-1)!, /Kicker by elimination: Tyler Loop \(IR\) unavailable/);
assert.equal(kickerRoleBlockedReason({ position: "K", availability: resolved }), null, "the optimizer and export QA accept the eliminated kicker");
assert.match(kickerRoleBlockedReason({ position: "K", availability: moody.availability })!, /Kicking role unresolved/);

// The eliminated teammate keeps his health block.
assert.match(applyTeamKickerContext(loop.availability, "K", "Tyler Loop", elim.get("BAL")).blockedReason!, /Unavailable: IR/);
// Another team's kicker, or a non-kicker, is untouched.
assert.equal(applyTeamKickerContext(moody.availability, "K", "Jake Moody", undefined).blockedReason, moody.availability.blockedReason);
assert.equal(applyTeamKickerContext(resolveAvailability(undefined, "BAL", "WR", now), "WR", "Zay Flowers", elim.get("BAL")).blockedReason, null);

// A Questionable regular is healthy, so two healthy kickers resolve nothing.
const loopQ = avail("Tyler Loop", { injury_status: "Questionable", depth_chart_order: 1 });
assert.equal(identifyTeamKickersByElimination([moody, loopQ]).size, 0, "a questionable K1 is not eliminated");
// Two unresolved healthy kickers resolve nothing.
assert.equal(identifyTeamKickersByElimination([moody, avail("Someone Else", {})]).size, 0);
// A lone kicker with nobody eliminated resolves nothing (elimination needs someone to eliminate).
assert.equal(identifyTeamKickersByElimination([moody]).size, 0);
// Stale evidence on the candidate resolves nothing: blocks apply, clearances do not.
const staleMoody = avail("Jake Moody", {}, "2026-10-20T00:30:00Z");
assert.equal(staleMoody.availability.fresh, false);
assert.equal(identifyTeamKickersByElimination([staleMoody, loop]).size, 0, "stale candidate evidence cannot clear him");
// A listed K2 behind an IR K1 is the kicker by elimination too.
const k2 = avail("Jake Moody", { depth_chart_order: 2 });
assert.match(k2.availability.blockedReason!, /listed K2/);
assert.equal(identifyTeamKickersByElimination([k2, loop]).get("BAL")?.name, "Jake Moody");
// An already-listed K1 needs no elimination.
assert.equal(identifyTeamKickersByElimination([avail("Cam Little", { depth_chart_order: 1 }), loop]).size, 0);

// The pinned-decision presenter path: the health decision is ACTIVE and the role block came from the chart.
const active = { version: "player-game-availability-v1", state: "EXPECTED_ACTIVE", projection_status: "EXPECTED_ACTIVE", source: "sleeper",
  observation_id: 12, source_snapshot_id: 21, available_at: "2026-11-06T00:30:00Z", as_of_at: "2026-11-06T00:38:00Z",
  kickoff: "2026-11-06T01:15:00Z", reason: "Qualified status.", qualifying_observation_ids: [12], display_only_observation_ids: [] };
const pinnedMoody = { ...moody, availability: presentPinnedGameAvailability(active, moody.availability.role, moody.availability.roleBlockedReason) };
const pinnedLoop = { ...loop, availability: presentPinnedGameAvailability({ ...active, state: "OUT_CONFIRMED", projection_status: "IR" }, loop.availability.role, loop.availability.roleBlockedReason) };
assert.match(pinnedMoody.availability.blockedReason!, /Kicking role unresolved/);
const pinnedElim = identifyTeamKickersByElimination([pinnedMoody, pinnedLoop]);
assert.equal(pinnedElim.get("BAL")?.name, "Jake Moody");
assert.equal(applyTeamKickerContext(pinnedMoody.availability, "K", "Jake Moody", pinnedElim.get("BAL")).blockedReason, null);
assert.match(applyTeamKickerContext(pinnedLoop.availability, "K", "Tyler Loop", pinnedElim.get("BAL")).blockedReason!, /Unavailable: IR/);

console.log("NFL kicker by elimination: Moody case, questionable regular, two unresolved, lone kicker, stale candidate, listed K2, pinned path passed");
