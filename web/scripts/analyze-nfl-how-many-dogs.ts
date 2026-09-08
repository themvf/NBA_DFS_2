/**
 * How many underdogs should a straight pick'em card carry?
 *
 * Not a market study -- there is nothing left to test there after five. This is
 * a decision question with a computable answer: GIVEN that the closing line is
 * right, how many flips maximise the chance of winning the pool?
 *
 * METHODOLOGICAL NOTE, and the reason this file was rewritten once.
 *
 * The first version scored candidate cards against the actual 2025 results.
 * That is backfitting: with outcomes fixed, "the best k" is whichever set of
 * flips happened to hit last season, not the count that is right going in. The
 * tell was a U-shaped result (k=0 better than k=1, k=2 worst of all, k=4 best)
 * -- a genuine cost/benefit tradeoff cannot zigzag like that, it was tracking
 * which specific dogs won.
 *
 * This version integrates over outcome uncertainty instead. Each game's result
 * is drawn from the market's own probability, so the answer is an expectation
 * over seasons that could have happened rather than a report on the one that
 * did. Real 2025 SLATES (the actual distribution of favourite strengths week
 * to week) are still used, because that shape is what determines how cheap the
 * flips on offer are.
 *
 * Two rules are compared:
 *   FIXED COUNT  flip exactly k games, whatever the slate looks like.
 *   THRESHOLD    flip every game whose favourite sits at or below p.
 * The threshold rule is what answers "one dog, or two if they are similar" --
 * it takes two when the slate offers two cheap ones and one when it does not.
 *
 * The field model remains the weak point: rivals take the favourite with
 * probability sigmoid(1.3 * logit(p)). It is a stated prior, not an
 * observation. The headline table uses NO chalk rivals, which the earlier
 * pool study showed is the CONSERVATIVE case -- more chalk in the field makes
 * flipping look better, not worse.
 *
 * Run: npm run analyze:how-many-dogs
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);

const DRAWS = 20000;
const POOL_SIZES = [10, 25, 50, 100, 250, 1000];
const THRESH = [0.5, 0.53, 0.55, 0.58, 0.6, 0.63];

type G = { week: number; pFav: number };

const implied = (a: number) => (a < 0 ? -a / (-a + 100) : 100 / (a + 100));
const logit = (p: number) => Math.log(p / (1 - p));
const sigmoid = (x: number) => 1 / (1 + Math.exp(-x));

function makeRng(seed: number) {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13; s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5; s >>>= 0;
    return s / 4294967296;
  };
}

/**
 * Expected prize share against R independent rivals, given that one rival
 * scores below us with probability y and level with us with probability x.
 *
 *   sum_j C(R,j) x^j y^(R-j) / (1+j)  =  ((x+y)^(R+1) - y^(R+1)) / ((R+1) x)
 *
 * Same identity as pickem-strategy.ts. Exact and O(1), which is what makes a
 * 1000-entry pool cost no more than a 10-entry one.
 */
function prizeShare(x: number, y: number, rivals: number): number {
  if (rivals <= 0) return 1;
  if (x < 1e-12) return Math.pow(y, rivals);
  return (Math.pow(x + y, rivals + 1) - Math.pow(y, rivals + 1)) / ((rivals + 1) * x);
}

/**
 * Expected prize share when `c` rivals are already tied with us (the chalk
 * block) and `R` noisy rivals remain:
 *
 *   sum_j C(R,j) x^j y^(R-j) / (c+1+j)  =  (x+y)^R * E_{j~Bin(R,q)}[1/(c+1+j)]
 *
 * with q = x/(x+y). Reduces to prizeShare() when c = 0. When the expected
 * number of ties is large the sum is replaced by the delta-method value
 * 1/(c+1+Rq); the integrand 1/(c+1+j) is smooth and the relative spread of j
 * is small there, so the approximation is tight exactly where the exact sum
 * would be slowest.
 */
function shareWithTies(x: number, y: number, R: number, c: number): number {
  if (c === 0) return prizeShare(x, y, R);
  if (R <= 0) return 1 / (1 + c);
  if (x < 1e-12) return Math.pow(y, R) / (1 + c);
  const q = x / (x + y);
  const base = Math.pow(x + y, R);
  if (R * q > 30) return base / (c + 1 + R * q);
  let pmf = Math.pow(1 - q, R);
  let sum = pmf / (c + 1);
  let mass = pmf;
  for (let j = 0; j < R && mass < 1 - 1e-12; j += 1) {
    pmf *= ((R - j) / (j + 1)) * (q / (1 - q));
    sum += pmf / (c + 2 + j);
    mass += pmf;
  }
  return base * sum;
}

/**
 * One noisy rival's score distribution GIVEN a drawn outcome vector.
 * Poisson-binomial over the games; 16-game DP, exact.
 */
function rivalDist(games: G[], favWon: boolean[]): number[] {
  let dist = [1];
  for (let i = 0; i < games.length; i += 1) {
    const shareOnFav = sigmoid(1.3 * logit(games[i].pFav));
    const p = favWon[i] ? shareOnFav : 1 - shareOnFav;
    const next = new Array<number>(dist.length + 1).fill(0);
    for (let k = 0; k < dist.length; k += 1) {
      next[k] += dist[k] * (1 - p);
      next[k + 1] += dist[k] * p;
    }
    dist = next;
  }
  return dist;
}

/** Self-check: prizeShare against brute force with explicit rivals. */
function selfCheck(): void {
  const games: G[] = [
    { week: 1, pFav: 0.52 }, { week: 1, pFav: 0.58 }, { week: 1, pFav: 0.65 },
    { week: 1, pFav: 0.74 }, { week: 1, pFav: 0.83 },
  ];
  const favWon = [true, false, true, true, false];
  const dist = rivalDist(games, favWon);
  let worst = 0;
  for (const [pool, myScore] of [[12, 3], [25, 4], [60, 3], [200, 5]] as const) {
    let y = 0;
    for (let k = 0; k < myScore; k += 1) y += dist[k];
    const x = dist[myScore] ?? 0;
    const exact = prizeShare(x, y, pool - 1);

    const shares = games.map((g) => sigmoid(1.3 * logit(g.pFav)));
    const rnd = makeRng(999);
    const TRIALS = 120000;
    let total = 0;
    for (let t = 0; t < TRIALS; t += 1) {
      let best = -1; let ties = 0;
      for (let r = 0; r < pool - 1; r += 1) {
        let sc = 0;
        for (let i = 0; i < games.length; i += 1) {
          if ((rnd() < shares[i]) === favWon[i]) sc += 1;
        }
        if (sc > best) { best = sc; ties = 1; }
        else if (sc === best) ties += 1;
      }
      if (myScore > best) total += 1;
      else if (myScore === best) total += 1 / (1 + ties);
    }
    const sim = total / TRIALS;
    worst = Math.max(worst, Math.abs(exact - sim));
    console.log(
      `    pool=${String(pool).padStart(4)} myScore=${myScore}   ` +
      `exact ${(exact * 100).toFixed(3)}%   sim ${(sim * 100).toFixed(3)}%   ` +
      `diff ${(Math.abs(exact - sim) * 100).toFixed(3)}pp`,
    );
  }
  if (worst > 0.005) {
    console.error(`\n  SELF-CHECK FAILED (${(worst * 100).toFixed(3)}pp). Aborting.`);
    process.exit(1);
  }
  console.log(`    worst error ${(worst * 100).toFixed(3)}pp — formula verified.\n`);
}

async function main() {
  console.log("\n  Verifying the exact prize-share formula against brute force:");
  selfCheck();

  const rows = await sql`
    SELECT week, quoted_home_ml hml, quoted_away_ml aml
    FROM nfl_season_games
    WHERE season = 2025 AND quoted_home_ml IS NOT NULL
    ORDER BY week
  `;
  const byWeek = new Map<number, G[]>();
  for (const r of rows) {
    const ih = implied(Number(r.hml));
    const ia = implied(Number(r.aml));
    const pHome = ih / (ih + ia);
    const g: G = { week: Number(r.week), pFav: Math.max(pHome, 1 - pHome) };
    if (!byWeek.has(g.week)) byWeek.set(g.week, []);
    byWeek.get(g.week)!.push(g);
  }
  for (const list of byWeek.values()) list.sort((a, b) => a.pFav - b.pFav);
  const weeks = [...byWeek.keys()].sort((a, b) => a - b);

  console.log("=".repeat(80));
  console.log("HOW MANY UNDERDOGS? — straight pick'em");
  console.log(`Real 2025 slates (${weeks.length} weeks, ${rows.length} games).`);
  console.log(`Outcomes drawn from the market's own probabilities, ${DRAWS.toLocaleString()} seasons.`);
  console.log("=".repeat(80));

  console.log("\n\nWHAT THE SLATE ACTUALLY OFFERS");
  console.log("-".repeat(80));
  console.log("  Favourite's win probability at the 1st, 2nd, 3rd cheapest games:\n");
  console.log("   wk   cheapest     2nd     3rd    gap 1->2");
  let tiedWeeks = 0;
  for (const w of weeks) {
    const l = byWeek.get(w)!;
    const gap = l[1].pFav - l[0].pFav;
    if (gap < 0.03) tiedWeeks += 1;
    console.log(
      `   ${String(w).padStart(2)}   ${(l[0].pFav * 100).toFixed(1).padStart(7)}%` +
      `${(l[1].pFav * 100).toFixed(1).padStart(8)}%${(l[2].pFav * 100).toFixed(1).padStart(8)}%` +
      `${(gap * 100).toFixed(1).padStart(10)}pp${gap < 0.03 ? "   <- effectively tied" : ""}`,
    );
  }
  console.log(
    `\n  In ${tiedWeeks} of ${weeks.length} weeks the two cheapest games were within 3pp of each\n` +
    `  other. "Are there two similar ones" is the normal case, not the exception.`,
  );

  // ---- shared outcome draws, reused by every strategy (paired comparison) --
  type WeekSim = { dists: number[][]; chalkScores: number[]; favWon: boolean[][] };
  const sims = new Map<number, WeekSim>();
  const rnd = makeRng(20260907);
  for (const w of weeks) {
    const list = byWeek.get(w)!;
    const dists: number[][] = [];
    const chalkScores: number[] = [];
    const favWonAll: boolean[][] = [];
    for (let d = 0; d < DRAWS; d += 1) {
      const favWon = list.map((g) => rnd() < g.pFav);
      dists.push(rivalDist(list, favWon));
      chalkScores.push(favWon.reduce((a, v) => a + (v ? 1 : 0), 0));
      favWonAll.push(favWon);
    }
    sims.set(w, { dists, chalkScores, favWon: favWonAll });
  }

  const evaluate = (pool: number, pick: (list: G[]) => Set<number>): number => {
    let total = 0;
    for (const w of weeks) {
      const list = byWeek.get(w)!;
      const flipped = pick(list);
      const sim = sims.get(w)!;
      let acc = 0;
      for (let d = 0; d < DRAWS; d += 1) {
        const favWon = sim.favWon[d];
        let my = 0;
        for (let i = 0; i < list.length; i += 1) {
          const tookFav = !flipped.has(i);
          if (tookFav === favWon[i]) my += 1;
        }
        const dist = sim.dists[d];
        let y = 0;
        for (let k = 0; k < my && k < dist.length; k += 1) y += dist[k];
        acc += prizeShare(my < dist.length ? dist[my] : 0, y, pool - 1);
      }
      total += acc / DRAWS;
    }
    return total / weeks.length;
  };

  console.log("\n\nFIXED COUNT — flip the k cheapest games");
  console.log("-".repeat(80));
  console.log("  Expected share of a winner-take-all prize, per week.\n");
  console.log("    pool        k=0      k=1      k=2      k=3      k=4    best");
  for (const pool of POOL_SIZES) {
    const vals: number[] = [];
    for (let k = 0; k <= 4; k += 1) {
      vals.push(evaluate(pool, () => new Set(Array.from({ length: k }, (_, i) => i))));
    }
    const best = vals.indexOf(Math.max(...vals));
    console.log(
      `    ${String(pool).padStart(4)}  ` +
      vals.map((v, i) => `${(v * 100).toFixed(2)}%${i === best ? "*" : " "}`.padStart(9)).join("") +
      `${String(best).padStart(8)}`,
    );
  }

  console.log("\n\nTHRESHOLD RULE — flip every game at or below a favourite probability");
  console.log("-".repeat(80));
  console.log(
    "    pool  " + THRESH.map((t) => `<=${(t * 100).toFixed(0)}%`.padStart(9)).join("") +
    "     best  avg flips",
  );
  for (const pool of POOL_SIZES) {
    const vals: number[] = [];
    for (const t of THRESH) {
      vals.push(evaluate(pool, (list) => {
        const s = new Set<number>();
        list.forEach((g, i) => { if (g.pFav <= t) s.add(i); });
        return s;
      }));
    }
    const best = vals.indexOf(Math.max(...vals));
    let flips = 0;
    for (const w of weeks) flips += byWeek.get(w)!.filter((g) => g.pFav <= THRESH[best]).length;
    console.log(
      `    ${String(pool).padStart(4)}  ` +
      vals.map((v, i) => `${(v * 100).toFixed(2)}%${i === best ? "*" : " "}`.padStart(9)).join("") +
      `  <=${(THRESH[best] * 100).toFixed(0)}%${(flips / weeks.length).toFixed(1).padStart(10)}`,
    );
  }

  // -------------------------------------------------------------------------
  // Chalk rivals. The tables above assume NOBODY else submits the all-favourites
  // card, which is not what a real pool looks like -- it is the premise the
  // whole question started from. Chalk rivals all score the same as each other,
  // so when they tie you the prize splits many ways, and that is precisely what
  // makes being identical expensive.
  // -------------------------------------------------------------------------
  console.log("\n\nWITH CHALK RIVALS — the case that actually matches a real pool");
  console.log("-".repeat(80));
  console.log("  A fraction of rivals submit the exact all-favourites card.\n");

  const evaluateChalk = (pool: number, k: number, chalkFraction: number, draws: number): number => {
    const rivals = pool - 1;
    const chalkRivals = Math.round(rivals * chalkFraction);
    const noisy = rivals - chalkRivals;
    let total = 0;
    for (const w of weeks) {
      const list = byWeek.get(w)!;
      const sim = sims.get(w)!;
      let acc = 0;
      for (let d = 0; d < draws; d += 1) {
        const favWon = sim.favWon[d];
        let my = 0;
        for (let i = 0; i < list.length; i += 1) {
          const tookFav = i >= k;
          if (tookFav === favWon[i]) my += 1;
        }
        const chalkScore = sim.chalkScores[d];
        if (chalkRivals > 0 && chalkScore > my) continue; // beaten outright
        const c = chalkRivals > 0 && chalkScore === my ? chalkRivals : 0;
        const dist = sim.dists[d];
        let y = 0;
        for (let j = 0; j < my && j < dist.length; j += 1) y += dist[j];
        const x = my < dist.length ? dist[my] : 0;
        acc += shareWithTies(x, y, noisy, c);
      }
      total += acc / draws;
    }
    return total / weeks.length;
  };

  const CHALK_DRAWS = 4000;
  for (const cf of [0.25, 0.5]) {
    console.log(`  ${(cf * 100).toFixed(0)}% of rivals submit exact chalk:`);
    console.log("    pool        k=0      k=1      k=2      k=3      k=4    best");
    for (const pool of [10, 25, 50, 100, 250, 1000]) {
      const vals: number[] = [];
      for (let k = 0; k <= 4; k += 1) vals.push(evaluateChalk(pool, k, cf, CHALK_DRAWS));
      const best = vals.indexOf(Math.max(...vals));
      console.log(
        `    ${String(pool).padStart(4)}  ` +
        vals.map((v, i) => `${(v * 100).toFixed(2)}%${i === best ? "*" : " "}`.padStart(9)).join("") +
        `${String(best).padStart(8)}`,
      );
    }
    console.log("");
  }

  console.log("\nWHY EVEN NUMBERS OF FLIPS UNDERPERFORM ODD ONES");
  console.log("-".repeat(80));
  console.log("  Not noise -- it is arithmetic. With k flips,");
  console.log("      myScore - chalkScore = 2*(dogs that hit) - k");
  console.log("  which can equal ZERO only when k is even. So an even number of flips");
  console.log("  can land you exactly level with the entire chalk block and split the");
  console.log("  prize with all of them; an odd number makes that impossible. Flipping");
  console.log("  1 or 3 beats flipping 2 for that reason alone.");

  console.log("\n" + "=".repeat(80));
  console.log("Prize share is MODELLED — the field model is a stated prior, not an");
  console.log("observation. Trust the ORDERING of the columns far more than the levels.");
  console.log("The no-chalk-rival tables are the CONSERVATIVE case; the chalk-rival");
  console.log("tables match a real pool, where most entrants do play the favourites.");
  console.log("=".repeat(80));
}

main().catch((e) => { console.error(e); process.exit(1); });
