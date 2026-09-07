/**
 * Pick'em / confidence pool strategy engine.
 *
 * The whole page rests on one distinction, so it is stated here rather than in
 * the UI: maximizing your EXPECTED SCORE and maximizing your PROBABILITY OF
 * WINNING THE POOL are different problems with different answers, and only the
 * second one pays.
 *
 * Two results below are PROVED, not simulated, and are marked as such wherever
 * they surface:
 *
 *   1. EV-max is trivial. Expected points are sum(c_g * p_g) over a permutation
 *      of confidence weights c. By the rearrangement inequality that sum is
 *      maximized by pairing the largest weight with the largest probability --
 *      i.e. "sort by win probability". No search, no simulation. This is also
 *      exactly why EV-max is a weak pool strategy: every opponent who sorts
 *      correctly submits a near-identical entry, so a pool of EV-maximizers is
 *      decided by whose coin-flips landed, not by whose analysis was better.
 *
 *   2. The cost of a deviation is exact and closed-form. Swapping the
 *      confidence weights of games i and j costs
 *          dEV = (c_i - c_j) * (p_i - p_j)
 *      expected points. Flipping game g to the other side costs
 *          dEV = c_g * (2 * p_g - 1).
 *      Neither needs a simulation, so the price of every contrarian move on
 *      this page is a computed number rather than an impression.
 *
 * Everything about whether a given deviation is WORTH its cost is simulated,
 * and simulation rests on a model of the field that this repo has never
 * measured against a real pick'em pool. See pickem-policy.ts for what that
 * means for how the output is labelled.
 *
 * Literature this follows: Clair & Letscher, "Optimal Strategies for Sports
 * Betting Pools", Operations Research 55(6), 2007 -- the result that pool
 * strategy must model opponent behaviour and that contrarian value grows with
 * pool size. Kaplan & Garstka, "March Madness and the Office Pool", Management
 * Science 47(3), 2001 -- expected-score vs probability-of-winning objectives.
 * Metrick (1996) and Niemi/Carlin/Alexander (2008) on contrarian sizing.
 */

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

export type PickemGame = {
  gameId: number;
  week: number;
  homeAbbrev: string;
  awayAbbrev: string;
  /** Our probability that the HOME team wins. Ties are folded in by the caller. */
  pHome: number;
  provenance: string;
  kickoff: string | null;
  completed: boolean;
  /** True/false once played, null before. */
  homeWon: boolean | null;
  /**
   * Observed share of the field taking HOME, when the pool publishes it.
   * Null means we fall back to the modeled field -- and the two are never
   * blended or rendered alike.
   */
  fieldHomePct: number | null;
};

/** One entry: which side, and how many confidence points, per game. */
export type Entry = {
  /** pickHome[i] for games[i]. */
  pickHome: boolean[];
  /** confidence[i] for games[i]; a permutation of 1..n in a confidence pool. */
  confidence: number[];
};

export type PoolFormat = "confidence" | "straight";

export type FieldModel = {
  /**
   * How much harder the field backs favourites than the market does, on the
   * logit scale: q = sigmoid(bias * logit(p)). 1.0 makes the field a mirror of
   * our own probabilities; above 1.0 makes it over-back favourites, which is
   * the documented public tendency. This is a STATED PRIOR, not a measurement.
   */
  favoriteBias: number;
  /**
   * Spread of opponent skill, in logits. 0 means every opponent ranks games in
   * exactly our order; larger values scatter their confidence assignments.
   */
  skillSigma: number;
};

// ---------------------------------------------------------------------------
// Small numerics
// ---------------------------------------------------------------------------

const EPS = 1e-9;

export function logit(p: number): number {
  const c = Math.min(Math.max(p, 1e-6), 1 - 1e-6);
  return Math.log(c / (1 - c));
}

export function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-x));
}

/** Deterministic PRNG so every run of the page is reproducible from a seed. */
export function makeRng(seed: number): () => number {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13; s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5; s >>>= 0;
    return s / 4294967296;
  };
}

function gauss(rng: () => number): number {
  // Box-Muller. One value per call is fine at these sample sizes.
  const u = Math.max(rng(), 1e-12);
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * rng());
}

// ---------------------------------------------------------------------------
// The field
// ---------------------------------------------------------------------------

/**
 * Share of the field expected to take HOME.
 *
 * Uses the pool's own published share when there is one; otherwise the modeled
 * favourite-bias curve. The caller is told which it got, because a modeled
 * field and an observed field are not the same evidence.
 */
export function fieldHomeShare(game: PickemGame, model: FieldModel): {
  share: number;
  source: "observed" | "modeled";
} {
  if (game.fieldHomePct != null && Number.isFinite(game.fieldHomePct)) {
    return { share: Math.min(Math.max(game.fieldHomePct, 0.01), 0.99), source: "observed" };
  }
  return { share: sigmoid(model.favoriteBias * logit(game.pHome)), source: "modeled" };
}

// ---------------------------------------------------------------------------
// Expected points -- exact, no simulation
// ---------------------------------------------------------------------------

/** P(this entry's pick on game i is correct). */
export function pickProbability(game: PickemGame, pickHome: boolean): number {
  return pickHome ? game.pHome : 1 - game.pHome;
}

export function expectedPoints(games: PickemGame[], entry: Entry): number {
  let total = 0;
  for (let i = 0; i < games.length; i += 1) {
    total += entry.confidence[i] * pickProbability(games[i], entry.pickHome[i]);
  }
  return total;
}

/**
 * The EV-maximizing entry: favourite on every game, confidence descending in
 * win probability. Optimal by the rearrangement inequality -- proved, not
 * searched. In a straight pick'em every weight is 1 and only the sides matter.
 */
export function evOptimalEntry(games: PickemGame[], format: PoolFormat): Entry {
  const n = games.length;
  const pickHome = games.map((g) => g.pHome >= 0.5);
  const confidence = new Array<number>(n).fill(1);
  if (format === "confidence") {
    const order = games
      .map((g, i) => ({ i, edge: Math.max(g.pHome, 1 - g.pHome) }))
      .sort((a, b) => b.edge - a.edge);
    order.forEach((row, rank) => {
      confidence[row.i] = n - rank;
    });
  }
  return { pickHome, confidence };
}

/** Exact expected-points cost of transposing the confidence on games i and j. */
export function swapCost(games: PickemGame[], entry: Entry, i: number, j: number): number {
  const pi = pickProbability(games[i], entry.pickHome[i]);
  const pj = pickProbability(games[j], entry.pickHome[j]);
  return (entry.confidence[i] - entry.confidence[j]) * (pi - pj);
}

/** Exact expected-points cost of flipping game i to the other side. */
export function flipCost(games: PickemGame[], entry: Entry, i: number): number {
  const p = pickProbability(games[i], entry.pickHome[i]);
  return entry.confidence[i] * (2 * p - 1);
}

// ---------------------------------------------------------------------------
// Simulated world
// ---------------------------------------------------------------------------

/**
 * One frozen world: a set of game outcomes, and the opponent score
 * distribution under each of them.
 *
 * Built ONCE and reused for every candidate entry, so candidates are compared
 * under identical draws. Without common random numbers the local search below
 * would be climbing Monte Carlo noise -- the same paired-trial discipline the
 * survivor field study used.
 */
export type SimulatedWorld = {
  sims: number;
  opponents: number;
  /** poolEntries - 1, may exceed `opponents`; the tail is handled analytically. */
  rivalCount: number;
  /** outcomes[s][g] === true when HOME won in sim s. */
  outcomes: boolean[][];
  /** Sorted opponent scores for sim s. */
  oppScores: Float64Array[];
  fieldSources: Array<"observed" | "modeled">;
};

export function simulateWorld(
  games: PickemGame[],
  format: PoolFormat,
  model: FieldModel,
  options: { sims: number; poolEntries: number; sampleOpponents?: number; seed?: number },
): SimulatedWorld {
  const n = games.length;
  const sims = Math.max(1, options.sims);
  const rivalCount = Math.max(0, options.poolEntries - 1);
  const opponents = Math.max(1, Math.min(rivalCount || 1, options.sampleOpponents ?? 240));
  const rng = makeRng(options.seed ?? 12345);

  const shares = games.map((g) => fieldHomeShare(g, model));
  const fieldSources = shares.map((s) => s.source);

  const outcomes: boolean[][] = new Array(sims);
  const oppScores: Float64Array[] = new Array(sims);

  // Opponent confidence orderings are redrawn per opponent per sim: two people
  // with the same picks but different rank orders are genuinely different
  // entries, and collapsing that understates how spread the field's scores are.
  const perceived = new Array<number>(n);
  const idx = new Array<number>(n);

  for (let s = 0; s < sims; s += 1) {
    const outcome = new Array<boolean>(n);
    for (let g = 0; g < n; g += 1) outcome[g] = rng() < games[g].pHome;
    outcomes[s] = outcome;

    const scores = new Float64Array(opponents);
    for (let k = 0; k < opponents; k += 1) {
      for (let g = 0; g < n; g += 1) {
        const noisy = sigmoid(logit(games[g].pHome) + model.skillSigma * gauss(rng));
        perceived[g] = Math.abs(noisy - 0.5);
        idx[g] = g;
      }
      let weight: number[];
      if (format === "confidence") {
        idx.sort((a, b) => perceived[b] - perceived[a]);
        weight = new Array<number>(n);
        for (let r = 0; r < n; r += 1) weight[idx[r]] = n - r;
      } else {
        weight = new Array<number>(n).fill(1);
      }
      let score = 0;
      for (let g = 0; g < n; g += 1) {
        const tookHome = rng() < shares[g].share;
        if (tookHome === outcome[g]) score += weight[g];
      }
      scores[k] = score;
    }
    scores.sort();
    oppScores[s] = scores;
  }

  return { sims, opponents, rivalCount, outcomes, oppScores, fieldSources };
}

// ---------------------------------------------------------------------------
// Scoring an entry against a frozen world
// ---------------------------------------------------------------------------

/**
 * Expected share of a winner-take-all prize, splitting ties.
 *
 * Given the sampled opponent distribution we know, for one rival, the
 * probabilities of scoring below us (y), level with us (x), and above us. With
 * R independent rivals the expected share is
 *
 *     E[ 1{none above} / (1 + #tied) ]
 *   = sum_{j=0..R} C(R,j) x^j y^(R-j) / (1 + j)
 *   = ( (x+y)^(R+1) - y^(R+1) ) / ( (R+1) * x )
 *
 * which is exact and closed-form, so the number of rivals never has to be
 * simulated -- only one rival's score distribution does. That is what lets a
 * 5,000-entry pool cost the same to evaluate as a 20-entry one.
 *
 * The approximation that remains is real and is not hidden: x and y are
 * plug-in estimates from a finite opponent sample, and raising a noisy y to a
 * large power R amplifies its error. The estimator stays monotone in our own
 * score, which is what the local search needs, but a headline win probability
 * in a very large pool should be read as an ordering, not a calibrated number.
 */
function prizeShare(x: number, y: number, rivals: number): number {
  if (rivals <= 0) return 1;
  if (x < EPS) return Math.pow(y, rivals);
  const R = rivals;
  return (Math.pow(x + y, R + 1) - Math.pow(y, R + 1)) / ((R + 1) * x);
}

export type EntryEvaluation = {
  expectedPoints: number;
  /** Expected share of a winner-take-all prize. The objective. */
  prizeShare: number;
  /** P(no opponent scores strictly higher). Ties included. */
  pAtLeastTied: number;
  meanScore: number;
  scoreStdDev: number;
};

export function evaluateEntry(
  games: PickemGame[],
  entry: Entry,
  world: SimulatedWorld,
): EntryEvaluation {
  const n = games.length;
  let shareTotal = 0;
  let tiedTotal = 0;
  let scoreTotal = 0;
  let scoreSq = 0;

  for (let s = 0; s < world.sims; s += 1) {
    const outcome = world.outcomes[s];
    let score = 0;
    for (let g = 0; g < n; g += 1) {
      if (entry.pickHome[g] === outcome[g]) score += entry.confidence[g];
    }
    scoreTotal += score;
    scoreSq += score * score;

    const opp = world.oppScores[s];
    // opp is sorted: binary search the block equal to `score`.
    const lo = lowerBound(opp, score);
    const hi = upperBound(opp, score);
    const y = lo / world.opponents;
    const x = (hi - lo) / world.opponents;
    shareTotal += prizeShare(x, y, world.rivalCount);
    tiedTotal += Math.pow(Math.min(1, x + y), world.rivalCount);
  }

  const mean = scoreTotal / world.sims;
  return {
    expectedPoints: expectedPoints(games, entry),
    prizeShare: shareTotal / world.sims,
    pAtLeastTied: tiedTotal / world.sims,
    meanScore: mean,
    scoreStdDev: Math.sqrt(Math.max(0, scoreSq / world.sims - mean * mean)),
  };
}

function lowerBound(arr: Float64Array, target: number): number {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] < target - EPS) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function upperBound(arr: Float64Array, target: number): number {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] <= target + EPS) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

// ---------------------------------------------------------------------------
// Optimization
// ---------------------------------------------------------------------------

export type Deviation = {
  kind: "flip" | "swap";
  /** Game indices involved. `flip` uses only `i`. */
  i: number;
  j: number | null;
  /** Expected points given up. Exact, closed-form. */
  evCost: number;
  /** Gain in expected prize share, paired against the same simulated world. */
  shareGain: number;
  description: string;
};

export type OptimizedPlan = {
  baseline: Entry;
  baselineEval: EntryEvaluation;
  recommended: Entry;
  recommendedEval: EntryEvaluation;
  deviations: Deviation[];
  /** Search actually ran to a local optimum rather than hitting the cap. */
  converged: boolean;
};

/**
 * Hill-climb from the EV-optimal entry toward maximum prize share.
 *
 * Deliberately a local search from the EV optimum rather than a global one.
 * The space is n! * 2^n, but the EV optimum is provably the best entry when the
 * pool has one participant, so it is the right starting point, and every step
 * away from it is recorded with the exact price it cost. An entry arrived at by
 * annealing from nowhere would be unauditable -- the point of this tool is that
 * each deviation can be explained and priced individually.
 */
export function optimizeEntry(
  games: PickemGame[],
  format: PoolFormat,
  world: SimulatedWorld,
  options: { maxDeviations?: number; minShareGain?: number; maxPasses?: number } = {},
): OptimizedPlan {
  const maxDeviations = options.maxDeviations ?? 4;
  const minShareGain = options.minShareGain ?? 1e-4;
  const maxPasses = options.maxPasses ?? 6;
  const n = games.length;

  const baseline = evOptimalEntry(games, format);
  const baselineEval = evaluateEntry(games, baseline, world);

  let current: Entry = { pickHome: [...baseline.pickHome], confidence: [...baseline.confidence] };
  let currentEval = baselineEval;
  const deviations: Deviation[] = [];
  let converged = false;

  for (let pass = 0; pass < maxPasses; pass += 1) {
    if (deviations.length >= maxDeviations) break;

    let best: { entry: Entry; ev: EntryEvaluation; dev: Deviation } | null = null;

    const consider = (entry: Entry, dev: Omit<Deviation, "shareGain">) => {
      const ev = evaluateEntry(games, entry, world);
      const gain = ev.prizeShare - currentEval.prizeShare;
      if (gain <= minShareGain) return;
      if (best !== null && gain <= best.dev.shareGain) return;
      best = { entry, ev, dev: { ...dev, shareGain: gain } };
    };

    for (let i = 0; i < n; i += 1) {
      const flipped: Entry = { pickHome: [...current.pickHome], confidence: [...current.confidence] };
      flipped.pickHome[i] = !flipped.pickHome[i];
      const cost = flipCost(games, current, i);
      const from = current.pickHome[i] ? games[i].homeAbbrev : games[i].awayAbbrev;
      const to = current.pickHome[i] ? games[i].awayAbbrev : games[i].homeAbbrev;
      consider(flipped, {
        kind: "flip",
        i,
        j: null,
        evCost: cost,
        description: `Take ${to} over ${from}`,
      });

      if (format !== "confidence") continue;
      for (let j = i + 1; j < n; j += 1) {
        const swapped: Entry = { pickHome: [...current.pickHome], confidence: [...current.confidence] };
        const tmp = swapped.confidence[i];
        swapped.confidence[i] = swapped.confidence[j];
        swapped.confidence[j] = tmp;
        consider(swapped, {
          kind: "swap",
          i,
          j,
          evCost: swapCost(games, current, i, j),
          description:
            `Swap confidence ${current.confidence[i]} and ${current.confidence[j]} ` +
            `(${games[i].awayAbbrev}@${games[i].homeAbbrev} / ${games[j].awayAbbrev}@${games[j].homeAbbrev})`,
        });
      }
    }

    if (best === null) {
      converged = true;
      break;
    }
    const chosen = best as { entry: Entry; ev: EntryEvaluation; dev: Deviation };
    current = chosen.entry;
    currentEval = chosen.ev;
    deviations.push(chosen.dev);
  }

  return {
    baseline,
    baselineEval,
    recommended: current,
    recommendedEval: currentEval,
    deviations,
    converged,
  };
}

// ---------------------------------------------------------------------------
// Cheap differentiation -- exact, no simulation
// ---------------------------------------------------------------------------

export type CheapSwap = {
  i: number;
  j: number;
  evCost: number;
  confidenceGap: number;
  probabilityGap: number;
  label: string;
};

/**
 * Confidence transpositions that buy the most separation from the field per
 * expected point surrendered.
 *
 * The reason this list is usually not empty: a real NFL slate has a flat middle
 * where six or seven games sit within a few points of a coin flip, yet the
 * confidence ranks assigned to them span five or six weights. Transposing
 * inside that block costs (c_i - c_j)(p_i - p_j) -- a large weight gap times a
 * tiny probability gap -- so it is nearly free in expected points while moving
 * your score well away from everyone who sorted the same slate the same way.
 * Deviating on the top game costs an order of magnitude more for the same
 * separation.
 */
export function cheapDifferentiation(
  games: PickemGame[],
  entry: Entry,
  limit = 6,
): CheapSwap[] {
  const out: CheapSwap[] = [];
  for (let i = 0; i < games.length; i += 1) {
    for (let j = i + 1; j < games.length; j += 1) {
      const cost = Math.abs(swapCost(games, entry, i, j));
      const confidenceGap = Math.abs(entry.confidence[i] - entry.confidence[j]);
      if (confidenceGap < 2) continue;
      const pi = pickProbability(games[i], entry.pickHome[i]);
      const pj = pickProbability(games[j], entry.pickHome[j]);
      out.push({
        i,
        j,
        evCost: cost,
        confidenceGap,
        probabilityGap: Math.abs(pi - pj),
        label:
          `${games[i].awayAbbrev}@${games[i].homeAbbrev} (${entry.confidence[i]}) ` +
          `<-> ${games[j].awayAbbrev}@${games[j].homeAbbrev} (${entry.confidence[j]})`,
      });
    }
  }
  // Separation bought per expected point paid.
  out.sort((a, b) => b.confidenceGap / (b.evCost + 0.05) - a.confidenceGap / (a.evCost + 0.05));
  return out.slice(0, limit);
}
