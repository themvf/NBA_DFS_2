import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  SIT_INJURY_VERSION, SIT_VERSION, WTA, baselineOrder, buildPortfolio, candidateTrios, contestPool, correlationMatrix, dnpProb, fieldOwnership, normCdf,
  optimizeContest, recalibrateSurvival, rng, simulatePoints, sitProb, sitProbInjury, survivalKnots, type ContestPlayer,
} from "../src/lib/nba-contest-optimizer";

// Written by `python -m research.nba_wta_trios fixture`; the Python test fails if it is stale.
const fx = JSON.parse(readFileSync(fileURLToPath(new URL("../../tests/fixtures/nba_wta_fixture.json", import.meta.url)), "utf8"));
const close = (a: number, b: number, tol = 1e-9, what = "") => assert.ok(Math.abs(a - b) <= tol, `${what}: ${a} vs ${b}`);

// Constants match the frozen Python rules.
for (const [k, v] of Object.entries(fx.constants)) {
  assert.deepEqual((WTA as Record<string, unknown>)[k], v, `constant ${k}`);
}

// Survival knots, case by case.
for (const c of fx.knots) {
  const k = survivalKnots(c.curve as [number, number][]);
  if (c.s == null) { assert.equal(k, null); continue; }
  assert.ok(k);
  assert.equal(k.s.length, c.s.length);
  c.s.forEach((v: number, i: number) => close(k.s[i], v, 1e-12, "knot s"));
  c.x.forEach((v: number, i: number) => close(k.x[i], v, 1e-12, "knot x"));
}

for (const [b, p] of Object.entries(fx.dnp)) assert.equal(dnpProb(Number(b)), p);

// Sitting chance by books and lead time (nba-points-sit-lead-v1) matches the Python, case by case.
assert.equal(SIT_VERSION, fx.sit.version);
assert.deepEqual([...WTA.SIT_LEAD_HOURS], fx.sit.leadHours);
assert.deepEqual(WTA.SIT_BY_BOOKS_LEAD.map(([upper, rates]) => [upper, [...rates]]), fx.sit.byBooksLead);
for (const [books, lead, p] of fx.sit.cases as [number, number | null, number][]) assert.equal(sitProb(books, lead), p, `sit ${books} books, lead ${lead}`);
assert.equal(sitProb(6, undefined), dnpProb(6));

// The injury-report rule (nba-points-sit-injury-v1) matches the Python, case by case; null = left out.
const fi = fx.sitInjury;
assert.equal(SIT_INJURY_VERSION, fi.version);
assert.deepEqual([[...WTA.INJURY_EXCLUDED], WTA.INJURY_QUESTIONABLE, WTA.INJURY_CLEARED, [...WTA.INJURY_CLEAR_STATUSES], WTA.INJURY_FRESH_HOURS],
  [fi.excluded, fi.questionable, fi.cleared, fi.clearStatuses, fi.freshHours]);
for (const [status, books, lead, rep, sub, want] of fi.cases as [string | null, number, number | null, number | null, boolean, number | null][]) {
  assert.equal(sitProbInjury(status, books, lead, rep, sub), want, `injury ${status} ${books} books lead ${lead} report ${rep} filed ${sub}`);
}

// The top-end correction (nba-points-sim-recal-v1) matches the Python.
fx.recal.u.forEach((u: number, i: number) => close(recalibrateSurvival(u), fx.recal.out[i], 1e-12, `recal ${u}`));

// Correlations.
const spec = fx.correlation.spec as [string, string | null][];
const corr = correlationMatrix(spec.map(([e, t]) => ({ eventId: e, team: t })));
fx.correlation.matrix.forEach((row: number[], i: number) => row.forEach((v, j) => close(corr[i][j], v, 1e-12, "corr")));

// Trio order and baseline order.
assert.deepEqual(candidateTrios(spec.length), fx.trios);
const bo = fx.baselineOrder;
const bpool = bo.lines.map((line: number, i: number) => ({ line, pOver: bo.pOver[i], key: bo.keys[i] }));
assert.deepEqual(baselineOrder(bpool, candidateTrios(bpool.length), 5), bo.order);

// Prize-share selection on a fixed matrix: greedy and ordered.
const P = fx.portfolio;
const sims = P.totals.length, nT = P.totals[0].length;
const cm = new Float64Array(sims * nT);
for (let s = 0; s < sims; s++) for (let k = 0; k < nT; k++) cm[k * sims + s] = P.totals[s][k];
const check = (got: ReturnType<typeof buildPortfolio>, want: typeof P.greedy) => {
  assert.equal(got.length, want.length);
  got.forEach((g, i) => {
    assert.equal(g.trio, want[i].trio, `pick ${i}`);
    close(g.marginalShare, want[i].marginalShare, 1e-12, "marginal");
    close(g.portfolioShare, want[i].portfolioShare, 1e-12, "portfolio");
    close(g.standaloneShare, want[i].standaloneShare, 1e-12, "standalone");
  });
};
check(buildPortfolio(cm, sims, nT, P.fmax, P.fcount, 5), P.greedy);
check(buildPortfolio(cm, sims, nT, P.fmax, P.fcount, 4, [3, 0, 7, 1]), P.ordered);

// Simulation: not pinned to numpy's draws, so check what it must get right.
close(normCdf(0), 0.5, 1e-7, "cdf"); close(normCdf(1.96), 0.975, 1e-4, "cdf");
const player = (key: string, line: number, books = 6, eventId = "e1", team: string | null = "A"): ContestPlayer => ({
  key, player: key.toUpperCase(), eventId, team, line, pOver: 0.5, books,
  curve: [{ point: line - 8.5, p: 0.9 }, { point: line, p: 0.5 }, { point: line + 7.5, p: 0.15 }, { point: line + 12.5, p: 0.05 }],
});
const pts = simulatePoints([player("a", 20), player("b", 12, 1)], 20000, rng(1));
let zeroB = 0;
const a: number[] = [];
for (let s = 0; s < 20000; s++) { a.push(pts[s * 2]); if (pts[s * 2 + 1] === 0) zeroB++; assert.equal(pts[s * 2], Math.round(pts[s * 2])); }
a.sort((x, y) => x - y);
assert.ok(Math.abs(a[10000] - 20) <= 1, `median ${a[10000]}`);
assert.ok(zeroB / 20000 > 0.32 && zeroB / 20000 < 0.38, `one-book sit rate ${zeroB / 20000}`);

// The pool drops a name quoted in two games and keeps the 20 biggest lines.
const many = Array.from({ length: 24 }, (_, i) => player(`p${String(i).padStart(2, "0")}`, 30 - i * 0.5, 6, `e${i % 4}`, i % 2 ? "A" : "B"));
const dup = [...many, { ...many[0], eventId: "e9" }];
const pool = contestPool(dup);
assert.equal(pool.length, 20);
assert.ok(!pool.some((p) => p.key === "p00"));

// End to end: deterministic, distinct entries, baseline = biggest line sums.
const r1 = optimizeContest(many, { sims: 1500, entries: 6 });
const r2 = optimizeContest(many, { sims: 1500, entries: 6 });
assert.deepEqual(r1, r2);
assert.equal(new Set(r1.entries.map((e) => [...e.players].sort().join("|"))).size, 6);
assert.deepEqual(r1.baseline[0].players, ["p00", "p01", "p02"]);
assert.ok(r1.entries.every((e) => e.standaloneShare >= 0 && e.standaloneShare <= 1));
assert.throws(() => optimizeContest(many.slice(0, 2)), /at least three/);

// A given sitting chance is the one simulated.
// (A curve can reach 0 on its own, so the sitting share is read against a run where nobody sits.)
const zeroShare = (sitting: number[]) => {
  const out = simulatePoints([player("a", 20), player("b", 12)], 40000, rng(2), false, sitting);
  let za = 0, zb = 0;
  for (let s = 0; s < 40000; s++) { if (out[s * 2] === 0) za++; if (out[s * 2 + 1] === 0) zb++; }
  return [za / 40000, zb / 40000];
};
const [noneA, noneB] = zeroShare([0, 0]), [sitA, sitB] = zeroShare([0, 0.1]);
close(sitA, noneA, 0.004, "a player given no sitting chance");
assert.ok(sitB - noneB > 0.085 && sitB - noneB < 0.11, `a 10% sitting chance added ${sitB - noneB}`);

// Without lead times the book-count rates apply and the run says so; with them, the lead-time rates.
assert.equal(r1.sitVersion, null);
assert.deepEqual(r1.sit, r1.pool.map((p) => dnpProb(p.books)));
const timed = many.map((p, i) => ({ ...p, leadHours: i % 4 === 0 ? 0.5 : 3.5 }));
const r3 = optimizeContest(timed, { sims: 1500, entries: 6 });
assert.equal(r3.sitVersion, SIT_VERSION);
assert.deepEqual(r3.sit, r3.pool.map((p) => sitProb(p.books, p.leadHours)));
assert.ok(r3.sit.includes(0.002) && r3.sit.includes(0.021));
assert.equal(optimizeContest(timed, { sims: 1500, entries: 6, leadSit: false }).sitVersion, null);
// One player without a lead time: the whole pool falls back, never a mix of two measurements.
assert.equal(optimizeContest(timed.map((p, i) => (i === 3 ? { ...p, leadHours: null } : p)), { sims: 1500, entries: 6 }).sitVersion, null);

// Modeled field share: the three biggest lines carry the chalk block, shares fall with the line, three picks each.
const own = fieldOwnership([34.5, 33.5, 29.5, 26.5, 25.5, 22.5, 20.5, 19.5], rng(5), 6000);
assert.ok(own.every((v) => v >= 0 && v <= 1));
assert.ok(own[0] > own[2] && own[2] > own[5] && own[5] > own[7], `ownership order ${own}`);
assert.ok(own[0] > 0.9, `top line share ${own[0]}`);
close(own.reduce((x, y) => x + y, 0), 3, 1e-9, "three picks per entry");
assert.equal(r3.fieldOwnership.length, r3.pool.length);

console.log("test-nba-contest-optimizer: all assertions passed");
