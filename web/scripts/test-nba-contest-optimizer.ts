import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  WTA, baselineOrder, buildPortfolio, candidateTrios, contestPool, correlationMatrix, dnpProb, normCdf,
  optimizeContest, recalibrateSurvival, rng, simulatePoints, survivalKnots, type ContestPlayer,
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

console.log("test-nba-contest-optimizer: all assertions passed");
