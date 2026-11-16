/**
 * Backups on the rolling projections page (/nfl/projections): the optimizer's
 * roster rules applied to the scheduled run, plus `nfl-off-depth-chart-v1`.
 * Pure; no database.
 */
import assert from "node:assert/strict";
import { offDepthChartReason, type RosterEvidence } from "../src/lib/nfl-dfs/availability";
import { resolveProjectionRoles, type RoleInput } from "../src/lib/nfl-dfs/projection-roles";

const now = Date.parse("2026-11-15T22:00:00Z");
const kickoff = "2026-11-16T01:20:00.000Z";
const fresh = "2026-11-15T20:00:00.000Z";
const stale = "2026-11-08T20:00:00.000Z";

let nextId = 1;
const roster = new Map<number, RosterEvidence>();
function player(name: string, team: string, position: string,
  sleeper: { depth?: number | null; status?: string; injury?: string; team?: string } | null, fetchedAt = fresh): RoleInput {
  const id = nextId++;
  if (sleeper) roster.set(id, { team, position, fetchedAt, sleeper: {
    team: sleeper.team ?? team, position, status: sleeper.status ?? "Active", injury_status: sleeper.injury ?? null,
    ...(sleeper.depth === undefined ? {} : { depth_chart_order: sleeper.depth }) } });
  return { key: id, ffPlayerId: id, name, team, position, kickoff };
}

const burrow = player("Joe Burrow", "CIN", "QB", { depth: 1 });
const flacco = player("Joe Flacco", "CIN", "QB", { depth: 2 });
const herbert = player("Justin Herbert", "LAC", "QB", { depth: 1 });
const browning = player("Jake Browning", "LAC", "QB", { depth: null });
const chase = player("Ja'Marr Chase", "CIN", "WR", { depth: 1 });
const perine = player("Samaje Perine", "CIN", "RB", { depth: 2 });
const deepTe = player("Cam Grandy", "CIN", "TE", { depth: 4 });
const perry = player("A.T. Perry", "PIT", "WR", { depth: null });
const staleUnlisted = player("Chris Moore", "BAL", "WR", { depth: null }, stale);
const traded = player("Audric Estime", "MIN", "RB", { depth: null, team: "NO" });
const unknown = player("No Capture", "PIT", "TE", null);
const released = player("Released Receiver", "PIT", "WR", { depth: null, team: "" });
const releasedLongAgo = player("Released Back", "BAL", "RB", { depth: null, team: "" }, stale);
const mcconkey = player("Ladd McConkey", "LAC", "WR", { depth: 1, injury: "Out" });
const inactive = player("Sincere Brown", "LAC", "WR", { depth: null, status: "Inactive", injury: "Questionable" });
const boswell = player("Chris Boswell", "PIT", "K", { depth: 1 });
const marjan = player("Laith Marjan", "PIT", "K", { depth: null });
const steelers = player("Pittsburgh Steelers", "PIT", "DST", null);

const roles = resolveProjectionRoles([burrow, flacco, herbert, browning, chase, perine, deepTe, perry, staleUnlisted,
  traded, unknown, released, releasedLongAgo, mcconkey, inactive, boswell, marjan, steelers], roster, now, 10);
const role = (input: RoleInput) => roles.get(input.key)!;

// --- Quarterbacks: the starter plays; a listed QB2 and an unranked QB behind a listed QB1 are backups ---
assert.deepEqual(role(burrow), { depth: "QB1", unavailableReason: null, backupReason: null });
assert.equal(role(flacco).depth, "QB2");
assert.match(role(flacco).backupReason ?? "", /^Listed QB2/);
assert.match(role(browning).backupReason ?? "", /Justin Herbert is listed QB1/, "an unranked QB behind a listed QB1 is a backup");
assert.equal(role(herbert).backupReason, null);

// --- RB/WR/TE: any listed rank plays; only a missing rank is a backup ---
assert.equal(role(chase).backupReason, null);
assert.deepEqual(role(perine), { depth: "RB2", unavailableReason: null, backupReason: null });
assert.equal(role(deepTe).backupReason, null, "a TE4 still plays sometimes, so a deep rank is not a block");
assert.equal(role(perry).backupReason, "Not on the depth chart");
assert.equal(role(staleUnlisted).backupReason, "Not on the depth chart (roster captured 2026-11-08)", "stale evidence still blocks, and says so");
assert.equal(role(traded).backupReason, null, "a capture for another team is no evidence about this one");
assert.equal(role(released).backupReason, "On no team's roster in Sleeper", "no team at all is evidence");
assert.equal(role(releasedLongAgo).backupReason, "On no team's roster in Sleeper (roster captured 2026-11-08)");
assert.equal(role(unknown).backupReason, null, "no capture is no evidence");

// --- A status newer than the run marks him out, never a backup ---
assert.deepEqual(role(mcconkey), { depth: "WR1", unavailableReason: "Unavailable: OUT", backupReason: null });
assert.equal(role(inactive).unavailableReason, "Unavailable: INACTIVE");
assert.equal(role(inactive).backupReason, null);

// --- Kickers need K1; defenses carry no depth rule ---
assert.equal(role(boswell).backupReason, null);
assert.match(role(marjan).backupReason ?? "", /K1/);
assert.deepEqual(role(steelers), { depth: null, unavailableReason: null, backupReason: null });

// --- The rule itself ---
assert.equal(offDepthChartReason({ depthOrder: null, fresh: true, capturedAt: fresh }, "QB"), null, "quarterbacks have their own rule");
assert.equal(offDepthChartReason({ depthOrder: undefined, fresh: true, capturedAt: fresh }, "WR"), null);
assert.equal(offDepthChartReason({ depthOrder: 6, fresh: true, capturedAt: fresh }, "WR"), null);

console.log("nfl projection roles: all assertions passed");
