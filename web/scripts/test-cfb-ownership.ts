/**
 * CFB field-ownership prior (cfb-ownership-prior-v1): budgets, caps, the
 * blend, kickers, statuses and the grade, on the real 2026-09-25 Classic and
 * 2026-10-31 Showdown salary files. Pinned values catch silent drift in the
 * NFL v5 functions this prior borrows.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { parseCfbSalaryCsv } from "../src/lib/cfb-dfs/salary-csv";
import {
  CFB_CLASSIC_BUDGETS, cfbFieldPoints, gradeCell, gradeCfbOwnership, projectCfbOwnership, type CfbOwnershipInput,
} from "../src/lib/cfb-dfs/ownership-prior";

// Recorded 2026-11-01 from this build; change only with a new prior version.
const PINNED = { smith: [84.72, 13.57, 71.15], topClassic: [44252564, 60] };
const fixture = (name: string) => readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8");
const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);
const inputs = (players: ReturnType<typeof parseCfbSalaryCsv>["players"]): CfbOwnershipInput[] => players.map((p) => ({
  dkId: p.dkId, position: p.position, salary: p.salary, captainSalary: p.captainSalary ?? null,
  proj: p.position === "K" ? 0 : p.dkAvg, manual: null, dkAvg: p.dkAvg, status: p.status.toUpperCase() === "OUT" ? "O" : p.status,
}));

// The blend: half ours, half DK average; a manual number replaces ours; kickers have no "ours".
assert.equal(cfbFieldPoints({ position: "WR", proj: 10, manual: null, dkAvg: 20 }), 15);
assert.equal(cfbFieldPoints({ position: "WR", proj: 10, manual: 30, dkAvg: 20 }), 25);
assert.equal(cfbFieldPoints({ position: "WR", proj: null, manual: null, dkAvg: 20 }), 20);
assert.equal(cfbFieldPoints({ position: "K", proj: 0, manual: null, dkAvg: 8.6 }), 8.6);
assert.equal(cfbFieldPoints({ position: "K", proj: 0, manual: 10, dkAvg: 8 }), 9);
assert.equal(cfbFieldPoints({ position: "RB", proj: null, manual: null, dkAvg: null }), null);

// Classic, real 2026-09-25 file: budgets per position, 60% cap, 800 in total.
const classic = parseCfbSalaryCsv(fixture("cfb-dk-salaries-2026-09-25.csv"));
const ci = inputs(classic.players);
const c = projectCfbOwnership(ci, "classic");
assert.equal(c.budgetPct, 800);
assert.ok(c.unallocatedPct < 0.01, `unallocated ${c.unallocatedPct}`);
for (const [pos, budget] of Object.entries(CFB_CLASSIC_BUDGETS)) {
  const ids = new Set(ci.filter((p) => p.position === pos).map((p) => p.dkId));
  assert.ok(Math.abs(sum(c.players.filter((p) => ids.has(p.dkId)).map((p) => p.ownPct)) - budget) < 0.5, `${pos} budget`);
}
assert.ok(c.players.every((p) => p.ownPct <= 60 + 1e-9 && p.captainPct == null));

// Status: OUT is 0; D is cut to a quarter of the same player's score.
const star = [...ci].sort((a, b) => b.dkAvg! - a.dkAvg!)[0];
const out = projectCfbOwnership(ci.map((p) => (p.dkId === star.dkId ? { ...p, status: "O" } : p)), "classic");
assert.equal(out.players.find((p) => p.dkId === star.dkId)!.ownPct, 0);
const doubtful = projectCfbOwnership(ci.map((p) => (p.dkId === star.dkId ? { ...p, status: "D" } : p)), "classic");
assert.ok(doubtful.players.find((p) => p.dkId === star.dkId)!.ownPct < c.players.find((p) => p.dkId === star.dkId)!.ownPct);

// Showdown, real 2026-10-31 OSU @ USC file: UTIL 500, Captain 100, caps 90 / 50 / 95.
const showdown = parseCfbSalaryCsv(fixture("cfb-dk-showdown-salaries-2026-10-31-osu-usc.csv"), "showdown");
const si = inputs(showdown.players);
const s = projectCfbOwnership(si, "showdown");
assert.equal(s.budgetPct, 600);
assert.ok(Math.abs(sum(s.players.map((p) => p.utilPct!)) - 500) < 0.5, "UTIL sums to 500");
assert.ok(Math.abs(sum(s.players.map((p) => p.captainPct!)) - 100) < 0.5, "Captain sums to 100");
assert.ok(s.players.every((p) => p.utilPct! <= 90 + 1e-9 && p.captainPct! <= 50 + 1e-9 && p.ownPct <= 95 + 1e-9));
// OUT players (Sayin and others) draw nothing.
for (const p of si.filter((x) => x.status === "O")) assert.equal(s.players.find((r) => r.dkId === p.dkId)!.ownPct, 0);
// Captain shares by position: NFL v5's with TE and DST absent, renormalised.
const cptBy = (pos: string) => sum(s.players.filter((p) => si.find((x) => x.dkId === p.dkId)!.position === pos).map((p) => p.captainPct!));
const expected = { QB: 22.82, RB: 28.82, WR: 31.92, K: 1.8 }, total = sum(Object.values(expected));
for (const [pos, share] of Object.entries(expected)) assert.ok(Math.abs(cptBy(pos) - (share / total) * 100) < 0.05, `${pos} captain share`);
// Kickers draft on their DK average with no projection.
const hawkins = si.find((p) => p.position === "K" && p.dkAvg === 8.6)!;
assert.ok(s.players.find((p) => p.dkId === hawkins.dkId)!.ownPct > 0);

// Pinned outputs on the real files (catch drift in the borrowed NFL v5 code).
const smith = s.players.find((p) => p.dkId === 44711795)!;
assert.deepEqual([smith.ownPct, smith.captainPct, smith.utilPct], PINNED.smith, JSON.stringify([smith.ownPct, smith.captainPct, smith.utilPct]));
const topClassic = [...c.players].sort((a, b) => b.ownPct - a.ownPct)[0];
assert.deepEqual([topClassic.dkId, topClassic.ownPct], PINNED.topClassic, JSON.stringify([topClassic.dkId, topClassic.ownPct]));

// Grading: Spearman on ranks, MAE, bias = estimate minus actual; a player nobody drafted counts as 0%.
const g = gradeCell([[10, 20], [20, 30], [30, 40]])!;
assert.deepEqual(g, { n: 3, spearman: 1, mae: 10, bias: -10 });
assert.equal(gradeCell([[1, 2]]), null);
const graded = gradeCfbOwnership("showdown", [
  { key: "a", ownPct: 50, captainPct: 10, utilPct: 40 }, { key: "b", ownPct: 20, captainPct: 5, utilPct: 15 }, { key: "c", ownPct: 5, captainPct: 0, utilPct: 5 },
], new Map<string, { total: number; bySlot: Record<string, number> }>([["a", { total: 60, bySlot: { CPT: 20, UTIL: 40 } }], ["b", { total: 10, bySlot: { UTIL: 10 } }]]));
assert.deepEqual(graded.total, { n: 3, spearman: 1, mae: 8.33, bias: 1.67 });
assert.deepEqual(graded.captain, { n: 3, spearman: 0.866, mae: 5, bias: -1.67 }); // ties share ranks

console.log("CFB ownership: blend, Classic budgets and caps, Showdown 500/100 with Captain shares, statuses, kickers, pinned outputs and grading hold.");
