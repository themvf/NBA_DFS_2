/**
 * Simulated captain odds: the fitted distributions reproduce each player's
 * projection and tails, and the odds respond to spread, not just the mean.
 */
import assert from "node:assert/strict";
import { fitQuantileShape, quantile, recommendFromSimulation, simulateCaptainOdds, type SimulationCandidate } from "../src/lib/nfl-dfs/captain-simulation";
import { nflRandom } from "../src/lib/nfl-dfs/random";

// ── The quantile curve keeps the projection as the mean and hits P10/median/P90 ──
function sample(mean: number, p10: number, median: number, p90: number, n = 200_000) {
  const shape = fitQuantileShape(mean, p10, median, p90);
  const random = nflRandom(7);
  const values = Array.from({ length: n }, () => quantile(shape, random())).sort((a, b) => a - b);
  return { shape, mean: values.reduce((a, b) => a + b, 0) / n, p10: values[Math.floor(0.1 * n)],
    median: values[Math.floor(0.5 * n)], p90: values[Math.floor(0.9 * n)] };
}
// Real ATL@GB rows: Kraft (lumpy), Bijan, Love, Watson.
for (const [mean, p10, median, p90] of [[12.2474, 3.5669, 9.68, 37.3736], [20.7855, 6.9121, 21.07, 39.6841],
  [16.5275, 3.8522, 16.43, 26.8353], [17.38, 3.77, 14.59, 36.76]]) {
  const s = sample(mean, p10, median, p90);
  assert.ok(s.shape.meanMatched, `mean reachable for ${mean}`);
  assert.ok(Math.abs(s.mean - mean) < 0.1, `mean ${s.mean} vs ${mean}`);
  for (const [got, want] of [[s.p10, p10], [s.median, median], [s.p90, p90]]) assert.ok(Math.abs(got - want) < 0.2, `quantile ${got} vs ${want}`);
}
assert.equal(fitQuantileShape(60, 3, 9, 37).meanMatched, false, "an unreachable projection is flagged, not faked");

const p = (id: number, name: string, team: string, ourProj: number, floorFpts: number | null, ceilingFpts: number | null,
  extra: Partial<SimulationCandidate> = {}): SimulationCandidate =>
  ({ dkPlayerId: id, name, position: "WR", team, opponent: team === "A" ? "B" : "A", ourProj, floorFpts, ceilingFpts,
     medianFpts: floorFpts == null || ceilingFpts == null ? null : Math.min(ceilingFpts, Math.max(floorFpts, ourProj * 0.9)),
     isOut: false, captainDkPlayerId: 1000 + id, ...extra });

// ── Odds ──
const sim = simulateCaptainOdds([
  p(1, "Star", "A", 20, 7, 40), p(2, "Steady", "B", 14, 10, 18), p(3, "Boom", "B", 12, 3, 37),
  p(4, "Hurt", "A", 18, 5, 35, { availabilityStatus: "Q" }), p(5, "Gone", "A", 15, 4, 30, { isOut: true }),
  p(6, "NoTails", "B", 9, null, null),
], { draws: 4000 });
const odds = Object.fromEntries(sim.odds.map((o) => [o.name, o.topPct]));
assert.equal(Math.round(sim.odds.reduce((a, o) => a + o.topPct, 0)), 100, "exactly one top scorer per draw");
assert.ok(odds.Star > odds.Steady && odds.Star > odds.Boom, "the strongest distribution leads");
const shifted = simulateCaptainOdds([p(1, "Star", "A", 20, 7, 40), p(2, "Steady", "B", 14, 10, 18), p(3, "Boom", "B", 18, 9, 43)], { draws: 4000 });
assert.ok(shifted.odds.find((o) => o.name === "Boom")!.topPct > odds.Boom, "moving a player's whole distribution up raises his odds");
assert.equal(odds.Hurt, undefined, "Questionable players are not captain candidates");
assert.equal(odds.Gone, undefined);
assert.deepEqual(sim.missingTails, ["NoTails"], "no invented spread: missing tails are listed, not simulated");
assert.deepEqual(simulateCaptainOdds([p(1, "Star", "A", 20, 7, 40), p(3, "Boom", "B", 12, 3, 37)], { draws: 500 }).odds,
  simulateCaptainOdds([p(1, "Star", "A", 20, 7, 40), p(3, "Boom", "B", 12, 3, 37)], { draws: 500 }).odds, "seeded: same inputs, same odds");

// ── Ranges ──
const rec = recommendFromSimulation(sim);
assert.equal(rec.basis, "simulation");
assert.ok(rec.rows.every((r) => r.min <= r.sharePct && r.sharePct <= r.max), "the odds sit inside their range");
assert.ok(rec.rows.reduce((a, r) => a + r.min, 0) <= 100, "minimums never exceed 100%");
assert.ok(rec.rows.reduce((a, r) => a + r.max, 0) >= 100, "maximums can fill every lineup");
assert.deepEqual(Object.keys(rec.targets).sort(), rec.rows.map((r) => String(r.dkPlayerId)).sort());
assert.deepEqual(recommendFromSimulation(simulateCaptainOdds([])).rows, []);

console.log("Captain simulation: fits keep mean and tails; odds follow spread; ranges are feasible.");
