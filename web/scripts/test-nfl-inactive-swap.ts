/** Inactive-player swap (nfl-inactive-swap-v1) and the no-lines exposure QA warning (fix #4, fix #10). */
import assert from "node:assert/strict";
import type { NflGeneratedLineup, NflLineupSlot, NflOptimizerPlayer } from "../src/app/dfs/nfl/nfl-optimizer";
import { swapInactivePlayers, swapScore, type SwapCandidate, type SwapRules } from "../src/lib/nfl-dfs/inactive-swap";
import { noLinesExposureCheck, withNoLinesExposure, type QaInput, type QaReport } from "../src/lib/nfl-dfs/pre-export-qa";
import { assertShowdownLineup } from "../src/lib/nfl-dfs/showdown-legality";

type Pos = NflOptimizerPlayer["position"];
function player(id: number, name: string, position: Pos, team: string, salary: number, proj: number, extra: Partial<NflOptimizerPlayer> = {}): NflOptimizerPlayer {
  const opponent = team === "AAA" ? "BBB" : team === "BBB" ? "AAA" : team === "CCC" ? "DDD" : "CCC";
  const gameKey = team === "AAA" || team === "BBB" ? "AAA@BBB" : "CCC@DDD";
  return { id, dkPlayerId: id, captainDkPlayerId: id + 1000, name, position, team, opponent, gameKey, salary, captainSalary: Math.round(salary * 1.5),
    rosterPositions: ["CPT", "FLEX"], isOut: false, projectionStatus: "historical", ourProj: proj, floorFpts: proj * 0.5, ceilingFpts: proj * 2,
    boomRate: null, avgFptsDk: null, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null, ...extra } as NflOptimizerPlayer;
}
const cand = (p: NflOptimizerPlayer, flags: Partial<SwapCandidate> = {}): SwapCandidate =>
  ({ player: p, projection: p.ourProj!, source: "our", floor: p.floorFpts!, ceiling: p.ceilingFpts!, flexEligible: true, captainEligible: true, ...flags });

function showdownLineup(n: number, captain: NflOptimizerPlayer, flex: NflOptimizerPlayer[]): NflGeneratedLineup {
  const slots: NflLineupSlot[] = [
    { slot: "CPT", player: captain, salary: captain.captainSalary!, multiplier: 1.5, projection: captain.ourProj! * 1.5, projectionSource: "our" },
    ...flex.map((p, i) => ({ slot: `FLEX${i + 1}`, player: p, salary: p.salary, multiplier: 1, projection: p.ourProj!, projectionSource: "our" as const })),
  ];
  return { lineupNumber: n, slots, playerIds: slots.map((s) => s.player.dkPlayerId), totalSalary: slots.reduce((s, x) => s + x.salary, 0),
    projectedFpts: slots.reduce((s, x) => s + x.projection, 0), floorFpts: 50, ceilingFpts: 150, projectedOwnership: null,
    stackSummary: { quarterback: null, passCatchers: [], bringBack: null } };
}

// ---------------- Showdown ----------------
const qbA = player(1, "QB A", "QB", "AAA", 11000, 20);
const wr1 = player(2, "Amon-Ra St. Brown", "WR", "AAA", 10000, 18);
const rbB = player(3, "RB B", "RB", "BBB", 9000, 15);
const wrB = player(4, "WR B", "WR", "BBB", 8000, 12);
const teA = player(5, "TE A", "TE", "AAA", 5000, 8);
const kB = player(6, "K B", "K", "BBB", 4000, 7);
const wr2 = player(7, "Jameson Williams", "WR", "AAA", 7000, 14);
const wr3 = player(8, "Sam LaPorta", "TE", "AAA", 6000, 10);
const rich = player(9, "Too Rich", "WR", "AAA", 14000, 30);
const dstA = player(10, "DST A", "DST", "AAA", 3000, 6);
const pool = [qbA, wr1, rbB, wrB, teA, kB, wr2, wr3, rich, dstA];

const sdRules: SwapRules = { format: "showdown", mode: "cash", minSalary: 0, overlapCap: 5, maxLineupsFor: () => 10 };
const L1 = showdownLineup(1, qbA, [wr1, rbB, wrB, teA, kB]);
const L2 = showdownLineup(2, rbB, [qbA, wrB, teA, kB, dstA]);
const L3 = showdownLineup(3, wr1, [qbA, wrB, teA, kB, dstA]);
const out = new Map([[wr1.dkPlayerId, "DraftKings lists him OUT"]]);
const candidates = pool.map((p) => cand(p, p === kB ? { captainEligible: false } : {}));
const res = swapInactivePlayers({ lineups: [L3, L1, L2], outReasons: out, candidates, rules: sdRules });

assert.deepEqual(res.lineups.map((l) => l.lineupNumber), [1, 2, 3], "lineup numbers kept, in order");
assert.equal(res.lineups[1], L2, "an untouched lineup is returned as it was");
assert.equal(res.unresolved.length, 0);
for (const l of res.lineups) {
  assert.ok(!l.playerIds.includes(wr1.dkPlayerId), `lineup ${l.lineupNumber} no longer holds the out player`);
  assert.ok(l.totalSalary <= 50000, "salary cap");
  assertShowdownLineup(l);
}
// Lineup 1: the Captain stays; FLEX1 is refilled; Too Rich ($14k) would bust the cap ($11k x1.5 + 9+8+5+4 = 42.5k, room 7.5k).
const r1 = res.lineups[0];
assert.equal(r1.slots[0].player, qbA, "the Captain is kept in the Captain slot");
assert.deepEqual(r1.slots.slice(2).map((s) => s.player), [rbB, wrB, teA, kB], "other FLEX players untouched, in their slots");
assert.equal(r1.slots[1].slot, "FLEX1");
assert.equal(r1.slots[1].player.name, "Jameson Williams", "best projection that fits the cap");
// Lineup 3: the out player was the Captain; the new Captain is captain-eligible, at Captain salary.
const r3 = res.lineups[2];
assert.equal(r3.slots[0].slot, "CPT");
assert.notEqual(r3.slots[0].player, kB, "a player ruled ineligible as Captain cannot be the Captain");
assert.equal(r3.slots[0].salary, Math.round(r3.slots[0].player.salary * 1.5));
assert.equal(r3.slots[0].multiplier, 1.5);
assert.deepEqual(r3.slots.slice(1).map((s) => s.player), [qbA, wrB, teA, kB, dstA]);
assert.ok(Math.abs(r3.projectedFpts - r3.slots.reduce((s, x) => s + x.projection, 0)) < 1e-9);
assert.equal(res.swaps.length, 2);
assert.match(res.summary[0], /^2 lineups had Amon-Ra St\. Brown \(DraftKings lists him OUT\); replaced with /);
assert.match(res.summary.at(-1)!, /lineup numbers are kept/);

// Overlap cap: lineup 1 already shares 4 players with lineup 4; Jameson would make it 5, past a cap of 4, so it takes the next.
const L4 = showdownLineup(4, qbA, [wr2, rbB, wrB, teA, dstA]);
const tight = swapInactivePlayers({ lineups: [L1, L4], outReasons: out, candidates, rules: { ...sdRules, overlapCap: 4 } });
assert.notEqual(tight.lineups[0].slots[1].player.name, "Jameson Williams", "overlap cap rejects a near-copy of another lineup");
assert.ok(tight.lineups[0].playerIds.filter((id) => L4.playerIds.includes(id)).length <= 4);

// Captain positions: a WR-only captain rule.
const wrCap = swapInactivePlayers({ lineups: [L3], outReasons: out, candidates, rules: { ...sdRules, captainPositions: ["WR"] } });
assert.equal(wrCap.lineups[0].slots[0].player.position, "WR");

// GPP scores by capped ceiling: a 2x-ceiling 10-pointer beats a flat 11-pointer.
assert.ok(swapScore({ projection: 10, ceiling: 20 }, 1, { mode: "gpp" }) > swapScore({ projection: 11, ceiling: 11 }, 1, { mode: "gpp" }));
assert.equal(swapScore({ projection: 10, ceiling: 40 }, 1.5, { mode: "gpp", maxCeilingMultiple: 2.5 }), 37.5);
assert.equal(swapScore({ projection: 10, ceiling: 40 }, 1, { mode: "cash" }), 10);

// No legal replacement: the lineup stays as it was and is reported.
const none = swapInactivePlayers({ lineups: [L1], outReasons: out, candidates: [cand(rich)], rules: sdRules });
assert.deepEqual(none.unresolved, [1]);
assert.equal(none.lineups[0], L1);
assert.match(none.summary.join(" "), /no legal replacement/);
assert.match(none.summary.join(" "), /Lineup 1 still holds an out player/);

// Salary floor is soft: relaxed only when nothing fits above it, and said.
const floor = swapInactivePlayers({ lineups: [L1], outReasons: out, candidates: [cand(teA), cand(dstA)], rules: { ...sdRules, minSalary: 49_500 } });
assert.equal(floor.unresolved.length, 0);
assert.equal(floor.lineups[0].slots[1].player, dstA);
assert.match(floor.summary.join(" "), /below the \$49,500 salary floor/);

// Exposure is soft too, and only after the floor.
const capped = swapInactivePlayers({ lineups: [L1], outReasons: out, candidates: [cand(wr2)], rules: { ...sdRules, maxLineupsFor: () => 0 } });
assert.equal(capped.lineups[0].slots[1].player, wr2);
assert.match(capped.summary.join(" "), /exposure limit/);

// Nothing out: nothing changed.
const clean = swapInactivePlayers({ lineups: [L2], outReasons: new Map(), candidates, rules: sdRules });
assert.equal(clean.lineups[0], L2);
assert.deepEqual(clean.summary, ["No lineup holds a player who is out now. Nothing was changed."]);

// ---------------- Classic ----------------
const c = (id: number, name: string, pos: Pos, team: string, salary: number, proj: number) => player(id, name, pos, team, salary, proj, { captainDkPlayerId: null, captainSalary: null, rosterPositions: [pos] });
const cQb = c(101, "QB C", "QB", "CCC", 7000, 20), cRb1 = c(102, "RB1", "RB", "CCC", 7000, 16), cRb2 = c(103, "RB2", "RB", "DDD", 6000, 14);
const cWr1 = c(104, "WR1", "WR", "CCC", 7000, 16), cWr2 = c(105, "WR out", "WR", "AAA", 6000, 13), cWr3 = c(106, "WR3", "WR", "DDD", 5000, 11);
const cTe = c(107, "TE C", "TE", "CCC", 4000, 8), cFlex = c(108, "FLEX RB", "RB", "AAA", 4000, 9), cDst = c(109, "DST C", "DST", "CCC", 3000, 7);
const subRb = c(110, "Big RB", "RB", "AAA", 6000, 19), subWr = c(111, "Sub WR", "WR", "BBB", 6000, 12);
const classicSlots: [string, NflOptimizerPlayer][] = [["QB", cQb], ["RB1", cRb1], ["RB2", cRb2], ["WR1", cWr1], ["WR2", cWr2], ["WR3", cWr3], ["TE", cTe], ["FLEX", cFlex], ["DST", cDst]];
const slots: NflLineupSlot[] = classicSlots.map(([slot, p]) => ({ slot, player: p, salary: p.salary, multiplier: 1, projection: p.ourProj!, projectionSource: "our" }));
const CL: NflGeneratedLineup = { lineupNumber: 7, slots, playerIds: slots.map((s) => s.player.dkPlayerId), totalSalary: slots.reduce((s, x) => s + x.salary, 0),
  projectedFpts: slots.reduce((s, x) => s + x.projection, 0), floorFpts: 60, ceilingFpts: 200, projectedOwnership: 100, stackSummary: { quarterback: "QB C", passCatchers: ["WR1", "TE C"], bringBack: "RB2" } };
const cr = swapInactivePlayers({ lineups: [CL], outReasons: new Map([[cWr2.dkPlayerId, "ruled out"]]), candidates: [cand(subRb), cand(subWr)],
  rules: { format: "classic", mode: "cash", minSalary: 0, overlapCap: 8, maxLineupsFor: () => 5 } });
assert.equal(cr.lineups[0].lineupNumber, 7);
assert.equal(cr.lineups[0].slots[4].slot, "WR2");
assert.equal(cr.lineups[0].slots[4].player, subWr, "a WR slot takes a WR, even when a higher-projected RB is available");
assert.equal(cr.lineups[0].floorFpts, 60 - cWr2.floorFpts! + subWr.floorFpts!, "range totals move by the swapped player only");
assert.deepEqual(cr.lineups[0].stackSummary, { quarterback: "QB C", passCatchers: ["WR1", "TE C"], bringBack: "RB2" });
assert.match(cr.summary[0], /^1 lineup had WR out \(ruled out\); replaced with Sub WR\.$/);

// ---------------- No-lines exposure warning ----------------
type QaLineup = QaInput["lineups"][number];
const anchored = (p: NflOptimizerPlayer, mode: "full" | "td_only" | null) => ({ ...p, marketAnchor: mode ? { mode } : null }) as unknown as NflOptimizerPlayer;
const qaLineup = (n: number, ps: NflOptimizerPlayer[]): QaLineup => ({ lineupNumber: n, playerIds: ps.map((p) => p.dkPlayerId), totalSalary: 0,
  slots: ps.map((p, i) => ({ slot: i ? `FLEX${i}` : "CPT", playerId: p.dkPlayerId, salary: p.salary, player: p })) });
const noLines = anchored(wr2, null), tdOnly = anchored(wrB, "td_only"), full = anchored(qbA, "full"), prior = anchored({ ...rbB, historyGames: 0 }, null);
const kicker = anchored(kB, null);
const qaLineups = [
  qaLineup(1, [full, noLines, tdOnly, kicker]), qaLineup(2, [full, noLines, kicker]), qaLineup(3, [full, tdOnly, prior, kicker]),
  qaLineup(4, [full, kicker]), qaLineup(5, [full, kicker]), qaLineup(6, [full, kicker]), qaLineup(7, [full, kicker]), qaLineup(8, [full, kicker]),
  qaLineup(9, [full, kicker]), qaLineup(10, [full, kicker]),
];
const check = noLinesExposureCheck(qaLineups);
assert.equal(check.severity, "warning");
assert.equal(check.passed, false);
assert.deepEqual(check.affected, ["Jameson Williams", "WR B"], "20%+ only (RB B is in 1 of 10), QB/RB/WR/TE only (no kicker), full lines ignored");
assert.match(check.detail, /Jameson Williams \(WR\) is in 2 of 10 lineups \(20%\), projected 14\.0 from our model only, no betting lines/);
assert.match(check.detail, /WR B \(WR\) is in 2 of 10 lineups \(20%\), projected 12\.0 from the books' touchdown line only, our model for the rest/);
assert.match(noLinesExposureCheck(qaLineups, 0.1).detail, /RB B \(RB\) is in 1 of 10 lineups \(10%\), projected 15\.0 from our model only, from his position's average/);
assert.equal(noLinesExposureCheck(qaLineups.slice(3)).passed, true);

const ready: QaReport = { rulesetVersion: "x", decision: "ready", checks: [], counts: { blocker: 0, warning: 0, info: 0 }, openBlockers: [] };
const warned = withNoLinesExposure(ready, qaLineups);
assert.equal(warned.decision, "ready_with_warnings");
assert.equal(warned.counts.warning, 1);
assert.equal(withNoLinesExposure(warned, qaLineups), warned, "added once");
const blocked: QaReport = { ...ready, decision: "blocked", openBlockers: ["current_pool"], counts: { blocker: 1, warning: 0, info: 0 },
  checks: [{ id: "current_pool", title: "t", severity: "blocker", passed: false, detail: "", overridable: true, affected: [] }] };
assert.equal(withNoLinesExposure(blocked, qaLineups).decision, "blocked");
assert.equal(withNoLinesExposure(ready, qaLineups.slice(3)).decision, "ready");

console.log("nfl inactive swap + no-lines exposure: all assertions passed");
