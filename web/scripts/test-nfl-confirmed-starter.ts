/**
 * A confirmed starting QB unlocks the existing promotion when the depth chart
 * has already moved the injured starter down (PHI@CHI, 2026-09-28).
 */
import assert from "node:assert/strict";
import { applyConfirmedStartingQbs, confirmStarterAvailability, sanitizeConfirmedStartingQbs } from "../src/lib/nfl-dfs/confirmed-starter";
import type { Availability } from "../src/lib/nfl-dfs/availability";
import { redistributeOutOpportunity, type RedistributionRow } from "../src/lib/nfl-dfs/opportunity-redistribution";

const qb = (key: number, name: string, depthOrder: number | null, attempts: number, extra: Partial<RedistributionRow> = {}): RedistributionRow => ({
  key, name, position: "QB", team: "CHI", isOut: false, depthOrder, canDonate: true, historyGames: 12,
  statMeans: { attempts, passing_yards: attempts * 6.5, passing_tds: attempts * 0.04, passing_interceptions: attempts * 0.02 },
  ourProj: attempts * 0.36, floorFpts: null, ceilingFpts: null, ...extra,
});
// As the feed had it: Williams ruled out and moved to QB3, the pipeline refused the transfer.
const rows = [
  qb(1, "Caleb Williams", 3, 32.1, { isOut: true, canDonate: false, historyGames: 34 }),
  qb(2, "Case Keenum", 1, 21.0, { historyGames: 2 }),
  qb(3, "Tyson Bagent", 2, 9.7),
];

assert.equal(redistributeOutOpportunity(rows).applied.length, 0, "the reshuffled depth chart blocks the promotion on its own");

const confirmed = applyConfirmedStartingQbs(rows, { CHI: 3 });
assert.deepEqual(confirmed.report.applied, [{ team: "CHI", starter: "Tyson Bagent", donor: "Caleb Williams" }]);
const promoted = redistributeOutOpportunity(confirmed.rows).applied;
assert.equal(promoted.length, 1);
assert.equal(promoted[0].name, "Tyson Bagent", "the confirmed starter gets the job, not the depth chart's QB1");
assert.ok(Math.abs(promoted[0].statMeans.attempts - 32.1) < 1e-9, "he takes over the starter's attempts, replacing his own");

assert.equal(redistributeOutOpportunity(applyConfirmedStartingQbs(rows, { CHI: 2 }).rows).applied[0].name, "Case Keenum");
assert.equal(applyConfirmedStartingQbs(rows, { CHI: 1 }).report.rejected.length, 1, "a ruled-out player cannot be the starter");
assert.equal(applyConfirmedStartingQbs(rows, { CHI: 99 }).report.rejected.length, 1);
const healthy = rows.map((row) => ({ ...row, isOut: false }));
assert.equal(applyConfirmedStartingQbs(healthy, { CHI: 3 }).report.rejected.length, 1, "no ruled-out QB, no workload to move");
assert.deepEqual(applyConfirmedStartingQbs(rows, {}).rows, rows, "without a confirmation nothing changes");

assert.deepEqual(sanitizeConfirmedStartingQbs({ CHI: 3, "x; drop": 4, PHI: -1, NE: 1.5 }), { CHI: 3 });
assert.deepEqual(sanitizeConfirmedStartingQbs(null), {});

// Blocked backups are isOut too, but only an injured QB's work may move.
const withBlockedBackup = rows.map((row) => row.key === 2 ? { ...row, isOut: true, statMeans: { ...row.statMeans, attempts: 40 } } : row);
const onlyInjured = applyConfirmedStartingQbs(withBlockedBackup, { CHI: 3 }, (row) => row.key === 1);
assert.equal(onlyInjured.report.applied[0].donor, "Caleb Williams", "a benched QB1 is not the donor even with more attempts");

// Availability: the confirmed starter is cleared from a depth-chart block, the chart's QB1 is blocked,
// and an injured player is never cleared.
const avail = (role: string, blockedReason: string | null, status = "EXPECTED_ACTIVE"): Availability =>
  ({ role, status, source: "test", capturedAt: null, blockedReason, fresh: true });
const name = () => "Tyson Bagent";
const player = (dkPlayerId: number, platformOut = false) => ({ dkPlayerId, team: "CHI", position: "QB", platformOut });
const bagent = confirmStarterAvailability(avail("Backup · QB2", "Listed QB2; starter workload not supported"), player(3), { CHI: 3 }, name);
assert.equal(bagent.blockedReason, null); assert.ok(bagent.role.startsWith("Expected starter"));
const keenum = confirmStarterAvailability(avail("Expected starter · QB1", null), player(2), { CHI: 3 }, name);
assert.match(keenum.blockedReason ?? "", /Tyson Bagent is the confirmed starter/);
const williams = avail("QB role unresolved", "Unavailable: OUT", "OUT");
assert.equal(confirmStarterAvailability(williams, player(1), { CHI: 1 }, name), williams, "a ruled-out QB is never cleared");
assert.equal(confirmStarterAvailability(avail("Backup · QB2", "x"), player(3, true), { CHI: 3 }, name).blockedReason, "x", "DraftKings OUT is never cleared");
const unrelated = avail("Expected starter · QB1", null);
assert.equal(confirmStarterAvailability(unrelated, { ...player(9), team: "PHI" }, { CHI: 3 }, name), unrelated, "other teams are untouched");

console.log("Confirmed starting QB: promotes the named starter from the ruled-out QB's workload.");
