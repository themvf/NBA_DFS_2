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

  // The brute force below draws every rival independently, so it contains no
  // chalk block. The field is pinned to match; the chalk path gets its own
  // brute-force check immediately after.
  const noChalk = { ...DEFAULT_FIELD, chalkFraction: 0 };
  const world = simulateWorld(games, format, noChalk, {
    sims: 20000,
    poolEntries: rivals + 1,
    sampleOpponents: 400,
    seed: 7,
  });
  const viaFormula = evaluateEntry(games, entry, world).prizeShare;

  // Direct: draw `rivals` explicit opponents per sim and count wins/ties.
  const rng = makeRng(99);
  const n = games.length;
  const shares = games.map((g) => fieldHomeShare(g, noChalk).share);
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
  // Chalk rivals against brute force. This is the path that decides whether
  // deviating is worth anything, so it gets the same treatment as the original
  // closed form rather than being trusted because it type-checks.
  const games = [game(0, 0.78), game(1, 0.64), game(2, 0.57), game(3, 0.52)];
  const format: PoolFormat = "straight";
  const poolEntries = 21;
  const field = { favoriteBias: 1.3, skillSigma: 0.35, chalkFraction: 0.4 };
  const entry: Entry = { pickHome: [true, true, true, false], confidence: [1, 1, 1, 1] };

  const world = simulateWorld(games, format, field, {
    sims: 30000, poolEntries, sampleOpponents: 400, seed: 21,
  });
  const viaFormula = evaluateEntry(games, entry, world).prizeShare;

  const chalkEntry = evOptimalEntry(games, format);
  const chalkRivals = Math.round((poolEntries - 1) * field.chalkFraction);
  const noisyRivals = poolEntries - 1 - chalkRivals;
  const shares = games.map((g) => fieldHomeShare(g, field).share);
  const rng = makeRng(4242);
  const sims = 120000;
  let total = 0;
  for (let s = 0; s < sims; s += 1) {
    const outcome = games.map((g) => rng() < g.pHome);
    const score = (e: Entry) =>
      e.pickHome.reduce((a, ph, i) => a + (ph === outcome[i] ? e.confidence[i] : 0), 0);
    const mine = score(entry);
    const chalkScore = score(chalkEntry);
    let best = -1;
    let ties = 0;
    if (chalkRivals > 0) { best = chalkScore; ties = chalkRivals; }
    for (let k = 0; k < noisyRivals; k += 1) {
      let sc = 0;
      for (let i = 0; i < games.length; i += 1) {
        if ((rng() < shares[i]) === outcome[i]) sc += 1;
      }
      if (sc > best) { best = sc; ties = 1; }
      else if (sc === best) ties += 1;
    }
    if (mine > best) total += 1;
    else if (mine === best) total += 1 / (1 + ties);
  }
  const viaDirect = total / sims;
  check(
    "chalk-rival prize share agrees with an explicit brute force",
    close(viaFormula, viaDirect, 0.02),
    `formula ${viaFormula.toFixed(4)} vs direct ${viaDirect.toFixed(4)}`,
  );
  check(
    "chalk rivals are counted as a tie block, not sampled",
    world.chalkRivals === chalkRivals && world.noisyRivals === noisyRivals,
    `${world.chalkRivals}/${world.noisyRivals} vs ${chalkRivals}/${noisyRivals}`,
  );
  check("no NaN leaks out of the chalk path", Number.isFinite(viaFormula));
}

{
  // Chalk rivals make being identical to them expensive: the same slate, the
  // same entry, evaluated against a field with and without a chalk block.
  const games = [game(0, 0.80), game(1, 0.66), game(2, 0.58), game(3, 0.54), game(4, 0.51)];
  const opts = { sims: 8000, poolEntries: 60, sampleOpponents: 250, seed: 909 };
  const chalk = evOptimalEntry(games, "straight");
  const flipped: Entry = { pickHome: [...chalk.pickHome], confidence: [...chalk.confidence] };
  flipped.pickHome[4] = !flipped.pickHome[4];

  const bare = simulateWorld(games, "straight", { favoriteBias: 1.3, skillSigma: 0.35, chalkFraction: 0 }, opts);
  const withChalk = simulateWorld(games, "straight", { favoriteBias: 1.3, skillSigma: 0.35, chalkFraction: 0.4 }, opts);

  const chalkBare = evaluateEntry(games, chalk, bare).prizeShare;
  const chalkVs = evaluateEntry(games, chalk, withChalk).prizeShare;
  const flipBare = evaluateEntry(games, flipped, bare).prizeShare;
  const flipVs = evaluateEntry(games, flipped, withChalk).prizeShare;

  check(
    "a chalk block makes the chalk card much worse",
    chalkVs < chalkBare,
    `${(chalkVs * 100).toFixed(2)}% vs ${(chalkBare * 100).toFixed(2)}%`,
  );
  check(
    "a chalk block makes flipping relatively better",
    flipVs / chalkVs > flipBare / chalkBare,
    `ratio ${(flipVs / chalkVs).toFixed(2)} vs ${(flipBare / chalkBare).toFixed(2)}`,
  );
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
console.log("\nParity sawtooth and search lookahead");
// ---------------------------------------------------------------------------

{
  // With a chalk block in the field, an EVEN number of flips can land level
  // with the whole block and split the prize, so k=2 scores worse than k=1 and
  // k=3. A purely greedy climb stops at one flip and never reaches three; the
  // two-move lookahead exists to get past that. The sawtooth itself is
  // emergent from the field model, not a coded rule.
  const probs = [0.88, 0.79, 0.72, 0.66, 0.61, 0.58, 0.565, 0.555, 0.545, 0.535, 0.525, 0.515, 0.508, 0.504];
  const slate2 = probs.map((p, i) => game(i, p));
  const field = { favoriteBias: 1.3, skillSigma: 0.35, chalkFraction: 0.25 };
  const world = simulateWorld(slate2, "straight", field, {
    sims: 6000, poolEntries: 50, sampleOpponents: 250, seed: 11,
  });
  const chalk = evOptimalEntry(slate2, "straight");
  const order = slate2
    .map((_, i) => i)
    .sort(
      (a, b) =>
        Math.max(slate2[a].pHome, 1 - slate2[a].pHome) -
        Math.max(slate2[b].pHome, 1 - slate2[b].pHome),
    );
  const kShare = (k: number) => {
    const e: Entry = { pickHome: [...chalk.pickHome], confidence: [...chalk.confidence] };
    for (let j = 0; j < k; j += 1) e.pickHome[order[j]] = !e.pickHome[order[j]];
    return evaluateEntry(slate2, e, world).prizeShare;
  };
  const s0 = kShare(0);
  const s1 = kShare(1);
  const s2 = kShare(2);
  const s3 = kShare(3);

  check(
    "a chalk block makes zero flips far worse than one",
    s0 < s1 / 2,
    `${(s0 * 100).toFixed(2)}% vs ${(s1 * 100).toFixed(2)}%`,
  );
  check(
    "two flips score worse than one -- parity effect is emergent, not coded",
    s2 < s1,
    `k=2 ${(s2 * 100).toFixed(2)}% vs k=1 ${(s1 * 100).toFixed(2)}%`,
  );
  check("three flips beat both", s3 > s1 && s3 > s2, `k=3 ${(s3 * 100).toFixed(2)}%`);

  const plan = optimizeEntry(slate2, "straight", world, { maxDeviations: 4 });
  check(
    "the optimizer crosses the sawtooth instead of stopping at one flip",
    plan.deviations.length >= 3,
    `chose ${plan.deviations.length}`,
  );
  check(
    "and lands at least as good as the best fixed-k card",
    plan.recommendedEval.prizeShare >= s3 - 1e-9,
    `${(plan.recommendedEval.prizeShare * 100).toFixed(2)}% vs ${(s3 * 100).toFixed(2)}%`,
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
    close(fieldHomeShare(modeled, { favoriteBias: 1, skillSigma: 0, chalkFraction: 0 }).share, 0.7, 1e-9),
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
