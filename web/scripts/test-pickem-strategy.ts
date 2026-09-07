/**
 * Tests for the pick'em / confidence pool engine.
 *
 * The load-bearing tests are the two brute-force equivalence checks:
 *
 *   1. On a slate small enough to enumerate every entry, the claimed
 *      EV-optimal entry must actually be the best of all n! * 2^n of them.
 *      Everything on the page prices its deviations against that baseline, so
 *      if the baseline is wrong every number downstream is wrong.
 *   2. The closed-form prize share must match a direct Monte Carlo over
 *      explicitly drawn rivals. That formula is what lets a 5,000-entry pool
 *      be evaluated as cheaply as a 20-entry one, and it is the single
 *      easiest place to be quietly wrong.
 *
 * Run: npm run test:pickem
 */

import {
  cheapDifferentiation,
  evOptimalEntry,
  evaluateEntry,
  expectedPoints,
  fieldHomeShare,
  flipCost,
  makeRng,
  optimizeEntry,
  simulateWorld,
  swapCost,
  type Entry,
  type PickemGame,
  type PoolFormat,
} from "../src/lib/nfl/pickem-strategy";
import { DEFAULT_FIELD, PICKEM_IS_VALIDATED, defaultObjective, poolAdvisory } from "../src/lib/nfl/pickem-policy";

let passed = 0;
let failed = 0;

function check(name: string, condition: boolean, detail?: string) {
  if (condition) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    console.error(`  FAIL  ${name}${detail ? ` -- ${detail}` : ""}`);
  }
}

function close(a: number, b: number, tol: number) {
  return Math.abs(a - b) < tol;
}

function game(i: number, pHome: number, fieldHomePct: number | null = null): PickemGame {
  return {
    gameId: i,
    week: 1,
    homeAbbrev: `H${i}`,
    awayAbbrev: `A${i}`,
    pHome,
    provenance: "market_ml_novig",
    kickoff: null,
    completed: false,
    homeWon: null,
    fieldHomePct,
  };
}

// ---------------------------------------------------------------------------
console.log("\nEV optimum -- brute force equivalence");
// ---------------------------------------------------------------------------

function permutations(n: number): number[][] {
  if (n === 0) return [[]];
  const out: number[][] = [];
  const base = Array.from({ length: n }, (_, i) => i + 1);
  const walk = (rest: number[], acc: number[]) => {
    if (rest.length === 0) {
      out.push([...acc]);
      return;
    }
    for (let i = 0; i < rest.length; i += 1) {
      walk([...rest.slice(0, i), ...rest.slice(i + 1)], [...acc, rest[i]]);
    }
  };
  walk(base, []);
  return out;
}

{
  // 5 games -> 120 orderings x 32 side combinations = 3,840 entries. Exhaustive.
  const games = [game(0, 0.82), game(1, 0.41), game(2, 0.55), game(3, 0.68), game(4, 0.5)];
  const n = games.length;
  let bestEv = -Infinity;
  let bestEntry: Entry | null = null;
  for (const perm of permutations(n)) {
    for (let mask = 0; mask < 1 << n; mask += 1) {
      const entry: Entry = {
        pickHome: Array.from({ length: n }, (_, i) => Boolean(mask & (1 << i))),
        confidence: perm,
      };
      const ev = expectedPoints(games, entry);
      if (ev > bestEv) {
        bestEv = ev;
        bestEntry = entry;
      }
    }
  }
  const claimed = evOptimalEntry(games, "confidence");
  const claimedEv = expectedPoints(games, claimed);
  check(
    "evOptimalEntry matches exhaustive search over all n! * 2^n entries",
    close(claimedEv, bestEv, 1e-9),
    `claimed ${claimedEv.toFixed(6)} vs brute force ${bestEv.toFixed(6)}`,
  );
  // Game 4 is an exact 50/50, where both sides are optimal and the brute force
  // may legitimately return either. Asserting a unique optimum there would be
  // asserting something untrue about the problem, so it is excluded.
  check(
    "brute-force optimum takes every favourite on the games that have one",
    bestEntry !== null &&
      games.every((g, i) => g.pHome === 0.5 || bestEntry!.pickHome[i] === g.pHome > 0.5),
  );
  check(
    "the 50/50 game is a genuine tie -- both sides reach the same optimum",
    (() => {
      const a = evOptimalEntry(games, "confidence");
      const b: Entry = { pickHome: [...a.pickHome], confidence: [...a.confidence] };
      b.pickHome[4] = !b.pickHome[4];
      return close(expectedPoints(games, a), expectedPoints(games, b), 1e-12);
    })(),
  );
  check(
    "a 50/50 game is assigned the lowest confidence",
    claimed.confidence[4] === 1,
    `got ${claimed.confidence[4]}`,
  );
}

{
  // Straight pick'em: every weight is 1, so only sides matter.
  const games = [game(0, 0.82), game(1, 0.41), game(2, 0.55)];
  const entry = evOptimalEntry(games, "straight");
  check("straight pick'em uses unit weights", entry.confidence.every((c) => c === 1));
  check(
    "straight pick'em still takes favourites",
    entry.pickHome[0] === true && entry.pickHome[1] === false && entry.pickHome[2] === true,
  );
}

// ---------------------------------------------------------------------------
console.log("\nExact deviation costs");
// ---------------------------------------------------------------------------

{
  const games = [game(0, 0.8), game(1, 0.55), game(2, 0.52)];
  const entry = evOptimalEntry(games, "confidence");
  for (const [i, j] of [[0, 1], [0, 2], [1, 2]] as const) {
    const swapped: Entry = {
      pickHome: [...entry.pickHome],
      confidence: [...entry.confidence],
    };
    const t = swapped.confidence[i];
    swapped.confidence[i] = swapped.confidence[j];
    swapped.confidence[j] = t;
    const actual = expectedPoints(games, entry) - expectedPoints(games, swapped);
    check(
      `swapCost(${i},${j}) equals the realised EV drop`,
      close(swapCost(games, entry, i, j), actual, 1e-12),
      `formula ${swapCost(games, entry, i, j).toFixed(9)} vs actual ${actual.toFixed(9)}`,
    );
  }
  for (let i = 0; i < games.length; i += 1) {
    const flipped: Entry = { pickHome: [...entry.pickHome], confidence: [...entry.confidence] };
    flipped.pickHome[i] = !flipped.pickHome[i];
    const actual = expectedPoints(games, entry) - expectedPoints(games, flipped);
    check(
      `flipCost(${i}) equals the realised EV drop`,
      close(flipCost(games, entry, i), actual, 1e-12),
    );
  }
  check("every swap away from the EV optimum costs something", swapCost(games, entry, 0, 2) > 0);
  check(
    "deviating on the flat pair is far cheaper than on the top game",
    swapCost(games, entry, 1, 2) < swapCost(games, entry, 0, 2) / 5,
    `${swapCost(games, entry, 1, 2).toFixed(4)} vs ${swapCost(games, entry, 0, 2).toFixed(4)}`,
  );
}

// ---------------------------------------------------------------------------
console.log("\nClosed-form prize share vs direct rival simulation");
// ---------------------------------------------------------------------------

{
  // Score the same entry two ways: through the closed form, and by explicitly
  // drawing every rival. They must agree.
  const games = [game(0, 0.75), game(1, 0.62), game(2, 0.58), game(3, 0.51), game(4, 0.45)];
  const format: PoolFormat = "confidence";
  const rivals = 24;
  const entry = evOptimalEntry(games, format);

  const world = simulateWorld(games, format, DEFAULT_FIELD, {
    sims: 20000,
    poolEntries: rivals + 1,
    sampleOpponents: 400,
    seed: 7,
  });
  const viaFormula = evaluateEntry(games, entry, world).prizeShare;

  // Direct: draw `rivals` explicit opponents per sim and count wins/ties.
  const rng = makeRng(99);
  const n = games.length;
  const shares = games.map((g) => fieldHomeShare(g, DEFAULT_FIELD).share);
  const sims = 40000;
  let total = 0;
  for (let s = 0; s < sims; s += 1) {
    const outcome = games.map((g) => rng() < g.pHome);
    let mine = 0;
    for (let g = 0; g < n; g += 1) if (entry.pickHome[g] === outcome[g]) mine += entry.confidence[g];
    let tied = 0;
    let beaten = false;
    for (let k = 0; k < rivals && !beaten; k += 1) {
      const perceived = games.map((g) => Math.abs(g.pHome - 0.5) + 0.35 * (rng() - 0.5));
      const order = perceived.map((p, i) => ({ p, i })).sort((a, b) => b.p - a.p);
      const weight = new Array<number>(n);
      order.forEach((row, r) => { weight[row.i] = n - r; });
      let score = 0;
      for (let g = 0; g < n; g += 1) if ((rng() < shares[g]) === outcome[g]) score += weight[g];
      if (score > mine) beaten = true;
      else if (score === mine) tied += 1;
    }
    if (!beaten) total += 1 / (1 + tied);
  }
  const viaDirect = total / sims;
  check(
    "closed-form prize share agrees with an explicit rival simulation",
    close(viaFormula, viaDirect, 0.02),
    `formula ${viaFormula.toFixed(4)} vs direct ${viaDirect.toFixed(4)}`,
  );
  check("prize share is a probability", viaFormula > 0 && viaFormula < 1);
}

{
  // A one-entry pool has no rivals: you always win.
  const games = [game(0, 0.6), game(1, 0.55)];
  const world = simulateWorld(games, "confidence", DEFAULT_FIELD, {
    sims: 200,
    poolEntries: 1,
    seed: 3,
  });
  const ev = evaluateEntry(games, evOptimalEntry(games, "confidence"), world);
  check("a pool of one always wins", close(ev.prizeShare, 1, 1e-12));
}

// ---------------------------------------------------------------------------
console.log("\nPool size drives how much deviation is worth");
// ---------------------------------------------------------------------------

const slate: PickemGame[] = [
  game(0, 0.88), game(1, 0.79), game(2, 0.72), game(3, 0.66),
  game(4, 0.61), game(5, 0.58), game(6, 0.56), game(7, 0.54),
  game(8, 0.52), game(9, 0.51), game(10, 0.47), game(11, 0.44),
  game(12, 0.39), game(13, 0.31),
];

{
  const runs: Array<{ entries: number; devs: number; evCost: number; gain: number }> = [];
  for (const entries of [8, 50, 400, 3000]) {
    const world = simulateWorld(slate, "confidence", DEFAULT_FIELD, {
      sims: 3000,
      poolEntries: entries,
      seed: 11,
    });
    const plan = optimizeEntry(slate, "confidence", world, { maxDeviations: 4 });
    runs.push({
      entries,
      devs: plan.deviations.length,
      evCost: plan.baselineEval.expectedPoints - plan.recommendedEval.expectedPoints,
      gain: plan.recommendedEval.prizeShare - plan.baselineEval.prizeShare,
    });
  }
  for (const r of runs) {
    console.log(
      `        ${String(r.entries).padStart(5)} entries: ${r.devs} deviation(s), ` +
      `EV cost ${r.evCost.toFixed(2)} pts, prize-share gain ${(r.gain * 100).toFixed(3)}pp`,
    );
  }
  check(
    "the optimizer never reports a negative prize-share gain",
    runs.every((r) => r.gain >= 0),
  );
  check(
    "deviation is never free -- every accepted move costs expected points",
    runs.every((r) => r.devs === 0 || r.evCost > 0),
  );
  check(
    "a large pool tolerates at least as much EV cost as a tiny one",
    runs[runs.length - 1].evCost >= runs[0].evCost - 1e-9,
    `8 entries paid ${runs[0].evCost.toFixed(3)}, 3000 paid ${runs[runs.length - 1].evCost.toFixed(3)}`,
  );
}

{
  // Common random numbers: the same world must score the same entry identically.
  const world = simulateWorld(slate, "confidence", DEFAULT_FIELD, {
    sims: 1500, poolEntries: 100, seed: 5,
  });
  const entry = evOptimalEntry(slate, "confidence");
  const a = evaluateEntry(slate, entry, world);
  const b = evaluateEntry(slate, entry, world);
  check("evaluation against a frozen world is deterministic", a.prizeShare === b.prizeShare);

  const w1 = simulateWorld(slate, "confidence", DEFAULT_FIELD, { sims: 1500, poolEntries: 100, seed: 5 });
  const w2 = simulateWorld(slate, "confidence", DEFAULT_FIELD, { sims: 1500, poolEntries: 100, seed: 5 });
  check(
    "the same seed rebuilds the same world",
    evaluateEntry(slate, entry, w1).prizeShare === evaluateEntry(slate, entry, w2).prizeShare,
  );
}

{
  const world = simulateWorld(slate, "confidence", DEFAULT_FIELD, {
    sims: 2000, poolEntries: 500, seed: 21,
  });
  const plan = optimizeEntry(slate, "confidence", world);
  check(
    "the recommended entry is still a valid permutation of 1..n",
    (() => {
      const sorted = [...plan.recommended.confidence].sort((a, b) => a - b);
      return sorted.every((c, i) => c === i + 1);
    })(),
  );
  check(
    "every reported deviation carries a positive, exact EV price",
    plan.deviations.every((d) => d.evCost > 0 && Number.isFinite(d.evCost)),
  );
  check(
    "the baseline the plan prices against is the EV optimum",
    close(
      plan.baselineEval.expectedPoints,
      expectedPoints(slate, evOptimalEntry(slate, "confidence")),
      1e-12,
    ),
  );
}

// ---------------------------------------------------------------------------
console.log("\nField model");
// ---------------------------------------------------------------------------

{
  const observed = game(0, 0.7, 0.42);
  const modeled = game(1, 0.7);
  check(
    "an observed pick share is used verbatim, not blended with the model",
    fieldHomeShare(observed, DEFAULT_FIELD).share === 0.42,
  );
  check("observed vs modeled is reported, not hidden", fieldHomeShare(observed, DEFAULT_FIELD).source === "observed");
  check("modeled field is labelled modeled", fieldHomeShare(modeled, DEFAULT_FIELD).source === "modeled");
  check(
    "the modeled field over-backs the favourite relative to the market",
    fieldHomeShare(modeled, DEFAULT_FIELD).share > 0.7,
  );
  check(
    "bias 1.0 makes the field a mirror of the market",
    close(fieldHomeShare(modeled, { favoriteBias: 1, skillSigma: 0 }).share, 0.7, 1e-9),
  );
  check(
    "the modeled field is symmetric about a coin flip",
    close(fieldHomeShare(game(2, 0.5), DEFAULT_FIELD).share, 0.5, 1e-9),
  );
}

// ---------------------------------------------------------------------------
console.log("\nCheap differentiation");
// ---------------------------------------------------------------------------

{
  const entry = evOptimalEntry(slate, "confidence");
  const cheap = cheapDifferentiation(slate, entry, 5);
  check("cheap differentiation returns candidates", cheap.length === 5);
  check(
    "the cheapest swaps come from the flat middle, not the top of the slate",
    cheap.every((c) => c.probabilityGap < 0.1),
    cheap.map((c) => c.probabilityGap.toFixed(3)).join(", "),
  );
  check(
    "every listed swap moves confidence a meaningful distance",
    cheap.every((c) => c.confidenceGap >= 2),
  );
  const topGameInvolved = cheap.some((c) => c.i === 0 || c.j === 0);
  check("the most-certain game is not offered as cheap differentiation", !topGameInvolved);
}

// ---------------------------------------------------------------------------
console.log("\nPolicy honesty");
// ---------------------------------------------------------------------------

{
  check("nothing on this page claims validation", PICKEM_IS_VALIDATED === false);
  check("the default objective is the proved one, not the modeled one", defaultObjective() === "ev");
  check(
    "a tiny pool is advised toward the EV-optimal entry",
    poolAdvisory(6, "confidence").includes("EV-optimal"),
  );
  check(
    "an unset pool size asks for one rather than assuming",
    poolAdvisory(null, "confidence").includes("pool size"),
  );
  check(
    "a large pool is warned that the field model is doing the work",
    poolAdvisory(2000, "confidence").includes("field model"),
  );
}

console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);
