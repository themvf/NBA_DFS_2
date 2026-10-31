import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { buildDistribution, exceedanceAt, nonIncreasing, powerExponent, quantileAt, type PointsDistribution } from "../src/lib/nba-points-ladder";
import { bookLines, consensus, nameKey, teamKey, type BookLine } from "../src/lib/nba-top-scorers";

// Normal(25, 7) survival function via an erf approximation (Abramowitz-Stegun 7.1.26).
function erf(x: number) {
  const s = Math.sign(x); x = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * x);
  const y = 1 - ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
  return s * y;
}
const S = (x: number) => 0.5 * (1 - erf((x - 25) / (7 * Math.SQRT2)));
const toAmerican = (p: number) => Math.round(p >= 0.5 ? (-100 * p) / (1 - p) : (100 * (1 - p)) / p);

// One book: a main line at 24.5 with 4.5% overround, and an over-only ladder
// whose raw implied chances carry the same overround.
const VIG = 1.045;
const mainOver = S(24.5) * VIG;
const mainUnder = (1 - S(24.5)) * VIG;
const ladder = [9.5, 14.5, 19.5, 29.5, 34.5, 39.5, 44.5].map((pt) => ({ point: pt, side: "over", price: toAmerican(Math.min(0.99, S(pt) * VIG)) }));
const dist = buildDistribution(
  { draftkings: { line: 24.5, over: toAmerican(mainOver), under: toAmerican(mainUnder) } },
  { draftkings: { ladder } },
)!;
assert.ok(dist, "distribution built");
assert.equal(dist.ladderBooks, 1);
// True P10 = 25 - 1.2816*7 = 16.03, P90 = 33.97; linear interpolation between rungs is close.
assert.ok(Math.abs(dist.floor! - 16.03) < 0.6, `floor ${dist.floor}`);
assert.ok(Math.abs(dist.ceiling! - 33.97) < 0.6, `ceiling ${dist.ceiling}`);
assert.ok(Math.abs(dist.atLeast[30]! - S(29.5)) < 0.01, `30+ ${dist.atLeast[30]}`);
assert.ok(Math.abs(dist.atLeast[20]! - S(19.5)) < 0.01, `20+ ${dist.atLeast[20]}`);
// 45+ is past the top rung (44.5 is "45+"), 50+ is not on the ladder: no guess.
assert.ok(dist.atLeast[40] != null);
assert.equal(exceedanceAt(dist.curve, 60), null);

// The curve is non-increasing even when books disagree out of order.
const pooled = nonIncreasing([
  { point: 10, p: 0.9, books: 1 },
  { point: 20, p: 0.6, books: 1 },
  { point: 25, p: 0.65, books: 1 },
  { point: 30, p: 0.3, books: 1 },
]);
for (let i = 1; i < pooled.length; i++) assert.ok(pooled[i].p <= pooled[i - 1].p);
assert.ok(Math.abs(pooled[1].p - 0.625) < 1e-9);

// A ladder that never reaches 90% has no floor rather than an extrapolated one.
assert.equal(quantileAt([{ point: 20, p: 0.8, books: 1 }, { point: 30, p: 0.2, books: 1 }], 0.9), null);

// Rungs from a book with no main two-sided line cannot be de-vigged: skipped, and
// with no usable ladder at all there is no distribution.
const none = buildDistribution({}, { fanduel: { ladder } });
assert.equal(none, null);

// A rung quoted on both sides is de-vigged as a pair, no main line needed for it.
const paired = buildDistribution(
  { draftkings: { line: 24.5, over: -110, under: -110 } },
  { fanduel: { ladder: [
    { point: 19.5, side: "over", price: -300 }, { point: 19.5, side: "under", price: 240 },
    { point: 29.5, side: "over", price: 200 }, { point: 29.5, side: "under", price: -250 },
  ] } },
)!;
assert.equal(paired.skippedRungs, 0);
assert.ok(paired.atLeast[20]! > 0.7 && paired.atLeast[30]! < 0.35);

// The shared fixture pins model/nba_points_ladder.py (the nightly grader's port)
// to this implementation: write-nba-points-ladder-fixture.ts regenerates it,
// tests/test_nba_points_ladder.py asserts the Python side.
type FixtureCase = {
  name: string;
  books: Record<string, never>;
  ladders: Record<string, never>;
  expected: { bookLines: BookLine[]; consensus: unknown; distribution: PointsDistribution | null; distributionPower: PointsDistribution | null };
};
const fixture = JSON.parse(readFileSync(resolve(__dirname, "../../tests/fixtures/nba_points_ladder_fixture.json"), "utf8")) as {
  nameKeys: Record<string, string>;
  teamKeys: Record<string, string>;
  cases: FixtureCase[];
  powerCases: FixtureCase[];
  powerExponents: { over: number; under: number; k: number | null }[];
};
for (const [name, key] of Object.entries(fixture.nameKeys)) assert.equal(nameKey(name), key, name);
for (const [name, key] of Object.entries(fixture.teamKeys)) assert.equal(teamKey(name), key, name);
assert.equal(fixture.cases.length, 6, "fixture cases");
assert.equal(fixture.powerCases.length, 4, "power fixture cases");
for (const c of [...fixture.cases, ...fixture.powerCases]) {
  const lines = bookLines(c.books);
  assert.deepEqual(lines, c.expected.bookLines, `${c.name}: book lines`);
  assert.deepEqual(consensus(lines), c.expected.consensus, `${c.name}: consensus`);
  const main = Object.fromEntries(lines.map((l) => [l.book, { line: l.line, over: l.over, under: l.under }]));
  assert.deepEqual(buildDistribution(main, c.ladders), c.expected.distribution, `${c.name}: distribution`);
  // The default reader is v1; "proportional" spelled out is the same reader.
  assert.deepEqual(buildDistribution(main, c.ladders, { devig: "proportional" }), c.expected.distribution, `${c.name}: proportional`);
  assert.deepEqual(buildDistribution(main, c.ladders, { devig: "power" }), c.expected.distributionPower, `${c.name}: power`);
}

// Power de-vig (nba-points-ladder-v2 candidate): k is about 1.072 at -110/-110,
// a k outside [0.25, 4] is null, and only one-sided rungs change.
const imp = (a: number) => (a < 0 ? -a / (-a + 100) : 100 / (a + 100));
for (const row of fixture.powerExponents) assert.deepEqual(powerExponent(imp(row.over), imp(row.under)), row.k, `k ${row.over}/${row.under}`);
assert.ok(Math.abs(powerExponent(imp(-110), imp(-110))! - 1.072) < 5e-4);
assert.equal(powerExponent(imp(-600), imp(-600)), null);
const pairedPower = buildDistribution(
  { draftkings: { line: 24.5, over: -110, under: -110 } },
  { fanduel: { ladder: [
    { point: 19.5, side: "over", price: -300 }, { point: 19.5, side: "under", price: 240 },
    { point: 29.5, side: "over", price: 200 }, { point: 29.5, side: "under", price: -250 },
  ] } },
  { devig: "power" },
);
assert.deepEqual(pairedPower, paired, "two-sided rungs are identical under power");

console.log("test-nba-points-ladder: all assertions passed");
