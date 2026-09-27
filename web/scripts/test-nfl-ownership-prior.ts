/**
 * NFL ownership prior: budgets sum, caps hold, out players get zero, status
 * and value move ownership the right way, showdown splits captain/flex, and
 * the source can never be rated validated.
 */
import assert from "node:assert/strict";
import { allocateBudget, CLASSIC_MAX_PCT, ownershipScore, projectOwnershipPrior, type OwnershipPriorPlayer } from "../src/lib/nfl-dfs/ownership-prior";
import { assessOwnership } from "../src/lib/nfl-dfs/ownership-capability";

let id = 0;
const mk = (position: string, salary: number, projection: number | null, extra: Partial<OwnershipPriorPlayer> = {}): OwnershipPriorPlayer =>
  ({ dkPlayerId: ++id, position, salary, projection, dkAvg: projection, isOut: false, dkStatus: "", ...extra });

// A small but complete Classic pool.
const pool: OwnershipPriorPlayer[] = [
  mk("QB", 8000, 30), mk("QB", 6500, 27), mk("QB", 5500, 16), mk("QB", 4000, 12), mk("QB", 4000, 0),
  mk("RB", 7700, 27), mk("RB", 8800, 25), mk("RB", 5400, 14), mk("RB", 4400, 5), mk("RB", 6600, 19, { isOut: true }),
  mk("RB", 6800, 15), mk("RB", 6400, 14), mk("RB", 5200, 13), mk("RB", 6200, 13), mk("RB", 4600, 8), mk("RB", 4000, 4),
  mk("WR", 8600, 27), mk("WR", 7900, 27), mk("WR", 4500, 16), mk("WR", 5500, 14, { dkStatus: "Q" }), mk("WR", 5500, 14), mk("WR", 3000, 4),
  mk("WR", 7200, 23), mk("WR", 6400, 16), mk("WR", 6100, 14.7), mk("WR", 5400, 13), mk("WR", 4700, 8), mk("WR", 3800, 6),
  mk("TE", 4800, 15.7), mk("TE", 6700, 17.6), mk("TE", 2500, 3),
  mk("DST", 3100, 11), mk("DST", 2700, 9), mk("DST", 3000, 8.9),
];
const classic = projectOwnershipPrior(pool, "classic");
const sum = classic.players.reduce((t, p) => t + p.ownPct, 0);
assert.ok(Math.abs(sum - 900) < 0.5, `classic ownership sums to 900, got ${sum.toFixed(1)}`);
assert.ok(classic.players.every((p) => p.ownPct <= CLASSIC_MAX_PCT + 1e-9), "no player over the cap");
const by = new Map(classic.players.map((p) => [p.dkPlayerId, p.ownPct]));
assert.equal(by.get(pool[9].dkPlayerId), 0, "an OUT player draws nothing");
assert.equal(classic.unallocatedPct, 0, "a realistic pool absorbs its whole budget");
assert.equal(by.get(pool[4].dkPlayerId), 0, "a zero-projection player draws nothing");
assert.ok(by.get(pool[19].dkPlayerId)! < by.get(pool[20].dkPlayerId)!, "Q drafts less than the same player healthy");
assert.ok(by.get(pool[1].dkPlayerId)! > by.get(pool[2].dkPlayerId)!, "more points and more value drafts more");
assert.ok(by.get(pool[0].dkPlayerId)! > 30, "the top QB is chalk");
assert.deepEqual(projectOwnershipPrior(pool, "classic"), classic, "deterministic");

// A pool too thin to hold its budget under the cap reports the shortfall rather than losing it.
const thin = projectOwnershipPrior([mk("RB", 7000, 20), mk("RB", 6000, 18)], "classic");
assert.ok(thin.unallocatedPct > 0 && Math.abs(thin.players.reduce((t, p) => t + p.ownPct, 0) + thin.unallocatedPct - 900) < 0.5, "shortfall is accounted for");

// Cap redistribution: one runaway score is held at the cap and the rest is re-shared.
const alloc = allocateBudget(new Map([[1, 1000], [2, 1], [3, 1]]), 100, 60);
assert.equal(alloc.get(1), 60); assert.ok(Math.abs(alloc.get(2)! - 20) < 1e-9 && Math.abs(alloc.get(3)! - 20) < 1e-9);
assert.equal(allocateBudget(new Map([[1, 0], [2, 0]]), 100, 60).get(1), 0, "no scores, no ownership, no NaN");

// DK average carries a player we have no projection for; nothing carries a player with neither.
assert.ok(ownershipScore(mk("WR", 5000, null, { dkAvg: 12 })) > 0);
assert.equal(ownershipScore(mk("WR", 5000, null, { dkAvg: null })), 0);

// Showdown: captain 100, flex 500, captain more concentrated than flex.
const sd = pool.slice(0, 12).map((p) => ({ ...p, captainSalary: Math.round(p.salary * 1.5) }));
const showdown = projectOwnershipPrior(sd, "showdown");
const cap = showdown.players.reduce((t, p) => t + (p.captainPct ?? 0), 0), flex = showdown.players.reduce((t, p) => t + (p.flexPct ?? 0), 0);
assert.ok(Math.abs(cap - 100) < 0.5 && Math.abs(flex - 500) < 0.5, `showdown sums: captain ${cap.toFixed(1)} flex ${flex.toFixed(1)}`);
const topCap = Math.max(...showdown.players.map((p) => p.captainPct ?? 0)), topFlex = Math.max(...showdown.players.map((p) => p.flexPct ?? 0));
assert.ok(topCap / 100 > topFlex / 500, "captain share is more concentrated than flex share");
assert.ok(showdown.players.every((p) => Math.abs(p.ownPct - ((p.captainPct ?? 0) + (p.flexPct ?? 0))) < 0.011));

// The prior declares itself heuristic: capability never reaches "validated".
const assessment = assessOwnership(
  pool.filter((p) => !p.isOut).map((p) => ({ playerId: p.dkPlayerId, medianProjection: p.projection })),
  classic.players.map((p) => ({ playerId: p.dkPlayerId, flexPct: p.ownPct / 100, captainPct: null, source: classic.version, asOf: "2026-09-27T00:00:00Z" })),
  { heuristic: true, optIntoHeuristic: true, format: "classic" },
);
assert.equal(assessment.capability, "heuristic_uncalibrated");
assert.equal(assessment.features.duplicationModel, false, "validated-only features stay off");
console.log("NFL ownership prior: sums to 900 (100 + 500 showdown), capped, deterministic, never validated by declaration.");
