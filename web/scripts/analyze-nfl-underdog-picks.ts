/**
 * Which underdog should you differentiate on? — NFL 2023-2025.
 *
 * Follow-up to analyze-nfl-pickem-2025.ts, which established that in a straight
 * pick'em pool the all-favourites card loses to the field once the pool is
 * bigger than about 25, so you need one or two dogs. This asks the next
 * question: does anything in the CLOSING LINES tell you which dogs to take?
 *
 * PRE-REGISTRATION. Written and committed before the confirmation season was
 * scored.
 *
 *   Discovery: 2023 + 2024 (544 games). Confirmation: 2025 (272 games).
 *
 *   TWO tests only, both chosen for a stated mechanism rather than by
 *   scanning. Two tests => P(>=1 false positive) ~ 10%, reported with the
 *   result. This is a SECOND study on the same data as the ten-segment screen,
 *   which already spent its own budget; the honest reading is that the
 *   cumulative family is now 12 tests, and a lone survivor here would be
 *   weaker evidence than its own CI suggests.
 *
 *   T1 — TOTAL. Mechanism: if margin variance grew with the scoring
 *   environment, a 3-point favourite in a shootout would be less safe than a
 *   3-point favourite in a rock fight, and a moneyline derived from the spread
 *   alone would misprice one of them. Section 0 measures whether that
 *   mechanism exists at all before the market is tested.
 *
 *   T2 — SPREAD vs MONEYLINE DISAGREEMENT. Mechanism: the spread and the
 *   moneyline are two prices for the same event. Convert the spread to a win
 *   probability with the measured margin SD; where that disagrees with the
 *   de-vigged moneyline, at least one price is stale, and the disagreement
 *   may point at which.
 *
 *   Metric for both: out-of-sample log loss on 2025 of a market-anchored
 *   logistic regression WITH the candidate feature, minus the same model
 *   WITHOUT it. Negative = the feature helped. Coefficients are fit on
 *   2023-24 only and never refit on 2025. Week-clustered bootstrap CI.
 *
 *   Kill criterion: CI includes zero => the feature adds nothing over the
 *   closing moneyline, and the answer to "which dog" is not in the lines.
 *   No re-cutting into sub-bands afterwards; a band found that way would be
 *   the eleventh through twentieth test of a family that has already produced
 *   one sign-flipping false positive.
 *
 *   Honest prior: no edge, and section 0 is likely to explain why before the
 *   tests are run.
 *
 * Run: npm run analyze:underdogs
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);

type Game = {
  season: number;
  week: number;
  spread: number; // + = home favoured
  total: number;
  pHomeMl: number; // de-vigged moneyline
  homeWon: boolean;
  margin: number;
};

function implied(a: number): number {
  return a < 0 ? -a / (-a + 100) : 100 / (a + 100);
}
function logit(p: number): number {
  const c = Math.min(Math.max(p, 1e-6), 1 - 1e-6);
  return Math.log(c / (1 - c));
}
function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-x));
}
/** Normal CDF (Abramowitz-Stegun 7.1.26). */
function normCdf(z: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989423 * Math.exp((-z * z) / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  return z > 0 ? 1 - p : p;
}

// ---------------------------------------------------------------------------
// Logistic regression by IRLS with light ridge (stability, not selection).
// ---------------------------------------------------------------------------
function fitLogistic(X: number[][], y: number[], ridge = 1e-4): number[] {
  const n = X.length;
  const k = X[0].length;
  const w = new Array<number>(k).fill(0);
  for (let iter = 0; iter < 60; iter += 1) {
    const g = new Array<number>(k).fill(0);
    const H: number[][] = Array.from({ length: k }, () => new Array<number>(k).fill(0));
    for (let i = 0; i < n; i += 1) {
      let z = 0;
      for (let j = 0; j < k; j += 1) z += w[j] * X[i][j];
      const p = sigmoid(z);
      const r = y[i] - p;
      const s = Math.max(p * (1 - p), 1e-9);
      for (let j = 0; j < k; j += 1) {
        g[j] += r * X[i][j];
        for (let l = 0; l < k; l += 1) H[j][l] += s * X[i][j] * X[i][l];
      }
    }
    for (let j = 0; j < k; j += 1) {
      g[j] -= ridge * w[j];
      H[j][j] += ridge;
    }
    const step = solve(H, g);
    if (!step) break;
    let maxd = 0;
    for (let j = 0; j < k; j += 1) {
      w[j] += step[j];
      maxd = Math.max(maxd, Math.abs(step[j]));
    }
    if (maxd < 1e-10) break;
  }
  return w;
}

/** Gaussian elimination with partial pivoting. */
function solve(A: number[][], b: number[]): number[] | null {
  const k = b.length;
  const M = A.map((row, i) => [...row, b[i]]);
  for (let c = 0; c < k; c += 1) {
    let piv = c;
    for (let r = c + 1; r < k; r += 1) if (Math.abs(M[r][c]) > Math.abs(M[piv][c])) piv = r;
    if (Math.abs(M[piv][c]) < 1e-12) return null;
    [M[c], M[piv]] = [M[piv], M[c]];
    for (let r = 0; r < k; r += 1) {
      if (r === c) continue;
      const f = M[r][c] / M[c][c];
      for (let j = c; j <= k; j += 1) M[r][j] -= f * M[c][j];
    }
  }
  return M.map((row, i) => row[k] / M[i][i]);
}

function logLoss(w: number[], X: number[][], y: number[]): number[] {
  return X.map((row, i) => {
    let z = 0;
    for (let j = 0; j < row.length; j += 1) z += w[j] * row[j];
    const p = Math.min(Math.max(sigmoid(z), 1e-9), 1 - 1e-9);
    return -(y[i] * Math.log(p) + (1 - y[i]) * Math.log(1 - p));
  });
}

/** Week-clustered bootstrap CI on the mean of (a - b), paired per game. */
function bootstrapDelta(diffs: number[], weeks: string[], iters = 6000, seed = 7717) {
  const byWeek = new Map<string, number[]>();
  weeks.forEach((wk, i) => {
    if (!byWeek.has(wk)) byWeek.set(wk, []);
    byWeek.get(wk)!.push(diffs[i]);
  });
  const clusters = [...byWeek.values()];
  let s = seed >>> 0;
  const rnd = () => {
    s ^= s << 13; s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5; s >>>= 0;
    return s / 4294967296;
  };
  const draws: number[] = [];
  for (let i = 0; i < iters; i += 1) {
    let sum = 0;
    let n = 0;
    for (let c = 0; c < clusters.length; c += 1) {
      const cl = clusters[Math.floor(rnd() * clusters.length)];
      for (const v of cl) { sum += v; n += 1; }
    }
    draws.push(sum / n);
  }
  draws.sort((a, b) => a - b);
  const point = diffs.reduce((a, b) => a + b, 0) / diffs.length;
  return { point, lo: draws[Math.floor(draws.length * 0.025)], hi: draws[Math.floor(draws.length * 0.975)] };
}

async function load(seasons: number[]): Promise<Game[]> {
  const rows = await sql`
    SELECT season, week, quoted_spread_line sp, quoted_total_line tot,
           quoted_home_ml hml, quoted_away_ml aml, home_score hs, away_score as_
    FROM nfl_season_games
    WHERE season = ANY(${sql.unsafe(`ARRAY[${seasons.join(",")}]`)})
      AND quoted_total_line IS NOT NULL AND quoted_home_ml IS NOT NULL
      AND home_score IS NOT NULL AND home_score <> away_score
    ORDER BY season, week
  `;
  return rows.map((r) => {
    const h = implied(Number(r.hml));
    const a = implied(Number(r.aml));
    return {
      season: Number(r.season),
      week: Number(r.week),
      spread: Number(r.sp),
      total: Number(r.tot),
      pHomeMl: h / (h + a),
      homeWon: Number(r.hs) > Number(r.as_),
      margin: Number(r.hs) - Number(r.as_),
    };
  });
}

async function main() {
  const disc = await load([2023, 2024]);
  const conf = await load([2025]);

  console.log("=".repeat(78));
  console.log("WHICH UNDERDOG? — closing-line features, NFL 2023-2025");
  console.log(`Discovery ${disc.length} games | Confirmation 2025 ${conf.length} games`);
  console.log("=".repeat(78));

  // -------------------------------------------------------------------------
  console.log("\n\n0. DOES THE MECHANISM EVEN EXIST?");
  console.log("-".repeat(78));
  console.log("For the total to matter, margin uncertainty has to grow with the");
  console.log("scoring environment. If it does not, a moneyline built from the");
  console.log("spread alone is already right and T1 is dead before it is run.\n");

  const all = [...disc, ...conf];
  const resid = all.map((g) => g.margin - g.spread);
  const mAll = resid.reduce((a, b) => a + b, 0) / resid.length;
  const sdAll = Math.sqrt(resid.reduce((a, r) => a + (r - mAll) ** 2, 0) / resid.length);
  console.log(`  SD(margin - spread) overall = ${sdAll.toFixed(2)} points  (n=${all.length})\n`);
  console.log("  total band     n    SD(margin-spread)   mean actual total");
  for (const [lo, hi] of [[0, 42], [42, 45], [45, 48], [48, 99]] as const) {
    const s = all.filter((g) => g.total >= lo && g.total < hi);
    if (s.length === 0) continue;
    const r = s.map((g) => g.margin - g.spread);
    const m = r.reduce((a, b) => a + b, 0) / r.length;
    const sd = Math.sqrt(r.reduce((a, v) => a + (v - m) ** 2, 0) / r.length);
    const at = s.reduce((a, g) => a + g.margin, 0);
    void at;
    console.log(
      `  ${`${lo}-${hi === 99 ? "+" : hi}`.padEnd(12)}${String(s.length).padStart(5)}` +
      `${sd.toFixed(2).padStart(19)}`,
    );
  }
  const xs = all.map((g) => g.total);
  const ys = all.map((g) => Math.abs(g.margin - g.spread));
  const mx = xs.reduce((a, b) => a + b, 0) / xs.length;
  const my = ys.reduce((a, b) => a + b, 0) / ys.length;
  const corr =
    xs.reduce((a, _, i) => a + (xs[i] - mx) * (ys[i] - my), 0) /
    (Math.sqrt(xs.reduce((a, v) => a + (v - mx) ** 2, 0)) *
      Math.sqrt(ys.reduce((a, v) => a + (v - my) ** 2, 0)));
  console.log(`\n  corr(total, |margin - spread|) = ${corr.toFixed(4)}`);
  console.log("  Near zero means margin uncertainty is flat across the total.");

  // -------------------------------------------------------------------------
  console.log("\n\n1. DESCRIPTIVE GRID — how do underdogs actually do?");
  console.log("-".repeat(78));
  console.log("Underdog win rate minus the de-vigged moneyline's implied rate.");
  console.log("2025 only. Positive = dogs beat their price in that cell.\n");

  const spreadBands: Array<[number, number, string]> = [
    [0, 3.5, "pk to 3"],
    [3.5, 7.5, "3.5 to 7"],
    [7.5, 99, "7.5+"],
  ];
  const totalBands: Array<[number, number, string]> = [
    [0, 43, "< 43"],
    [43, 47, "43-47"],
    [47, 99, "47+"],
  ];
  console.log("  spread \\ total   " + totalBands.map((t) => t[2].padStart(14)).join(""));
  for (const [slo, shi, slab] of spreadBands) {
    let line = `  ${slab.padEnd(16)}`;
    for (const [tlo, thi] of totalBands) {
      const cell = conf.filter(
        (g) => Math.abs(g.spread) >= slo && Math.abs(g.spread) < shi && g.total >= tlo && g.total < thi,
      );
      if (cell.length === 0) { line += "".padStart(14); continue; }
      const dogWins = cell.filter((g) => (g.pHomeMl >= 0.5 ? !g.homeWon : g.homeWon)).length;
      const impliedDog = cell.reduce((a, g) => a + Math.min(g.pHomeMl, 1 - g.pHomeMl), 0) / cell.length;
      const gap = dogWins / cell.length - impliedDog;
      line += `${(gap >= 0 ? "+" : "") + (gap * 100).toFixed(1)}pp n=${cell.length}`.padStart(14);
    }
    console.log(line);
  }
  console.log("\n  Cell sizes are small. This grid is for looking, not concluding —");
  console.log("  the tests below are what decide anything.");

  // -------------------------------------------------------------------------
  console.log("\n\n2. PRE-REGISTERED TESTS");
  console.log("-".repeat(78));
  console.log("Market-anchored logistic fit on 2023-24, scored once on 2025.");
  console.log("Target: home team wins. Baseline feature: logit(de-vigged home ML).\n");

  const meanTotal = disc.reduce((a, g) => a + g.total, 0) / disc.length;

  const spreadProb = (g: Game) => normCdf(g.spread / sdAll);
  const featureSets: Array<{ name: string; build: (g: Game) => number[] }> = [
    { name: "market only (baseline)", build: (g) => [1, logit(g.pHomeMl)] },
    { name: "T1  market + total", build: (g) => [1, logit(g.pHomeMl), (g.total - meanTotal) / 5] },
    {
      name: "T2  market + spread/ML disagreement",
      build: (g) => [1, logit(g.pHomeMl), logit(spreadProb(g)) - logit(g.pHomeMl)],
    },
  ];

  const yDisc = disc.map((g) => (g.homeWon ? 1 : 0));
  const yConf = conf.map((g) => (g.homeWon ? 1 : 0));
  const weeksConf = conf.map((g) => `${g.season}-${g.week}`);

  const losses: Record<string, number[]> = {};
  for (const fs of featureSets) {
    const w = fitLogistic(disc.map(fs.build), yDisc);
    const l = logLoss(w, conf.map(fs.build), yConf);
    losses[fs.name] = l;
    const mean = l.reduce((a, b) => a + b, 0) / l.length;
    console.log(`  ${fs.name.padEnd(38)} 2025 log loss ${mean.toFixed(5)}   coefs [${w.map((v) => v.toFixed(3)).join(", ")}]`);
  }

  const base = losses["market only (baseline)"];
  console.log("");
  for (const name of ["T1  market + total", "T2  market + spread/ML disagreement"]) {
    const diffs = losses[name].map((v, i) => v - base[i]);
    const b = bootstrapDelta(diffs, weeksConf);
    const helped = b.hi < 0;
    const hurt = b.lo > 0;
    console.log(
      `  ${name.padEnd(38)} delta ${b.point >= 0 ? "+" : ""}${b.point.toFixed(5)}  ` +
      `95% CI [${b.lo.toFixed(5)}, ${b.hi.toFixed(5)}]  ` +
      `${helped ? "HELPS" : hurt ? "HURTS" : "no effect (CI includes zero)"}`,
    );
  }
  console.log("\n  Negative delta = lower log loss than market alone = the feature helped.");
  console.log("  Two tests => P(>=1 false positive) ~ 10%. Cumulative family with the");
  console.log("  earlier ten-segment screen is 12 tests, one of which already produced");
  console.log("  a sign-flipping false positive.");

  // -------------------------------------------------------------------------
  console.log("\n\n3. SO HOW DO YOU PICK THE DOG?");
  console.log("-".repeat(78));
  console.log("This part needs no edge and is the part that actually pays.");
  console.log("Ranked by 2025 data: the cheapest flips available per week.\n");

  const byWeek = new Map<number, Game[]>();
  for (const g of conf) {
    if (!byWeek.has(g.week)) byWeek.set(g.week, []);
    byWeek.get(g.week)!.push(g);
  }
  let cheapCount = 0;
  let cheapWins = 0;
  let cheapCost = 0;
  for (const wk of byWeek.keys()) {
    const set = byWeek.get(wk)!;
    // The single cheapest flip that week = the game whose favourite is weakest.
    const sorted = [...set].sort(
      (a, b) => Math.max(a.pHomeMl, 1 - a.pHomeMl) - Math.max(b.pHomeMl, 1 - b.pHomeMl),
    );
    const target = sorted[0];
    const dogWon = target.pHomeMl >= 0.5 ? !target.homeWon : target.homeWon;
    cheapCount += 1;
    if (dogWon) cheapWins += 1;
    cheapCost += 2 * Math.max(target.pHomeMl, 1 - target.pHomeMl) - 1;
  }
  // Expected dog wins over those same games = sum(1 - p_fav) = n/2 - cost/2.
  const expectedDogWins = cheapCount / 2 - cheapCost / 2;
  console.log(
    `  Strategy: every week, flip the ONE game with the weakest favourite.\n`,
  );
  console.log(`    Realised   : ${cheapWins} of ${cheapCount} flips hit`);
  console.log(`    Expected   : ${expectedDogWins.toFixed(1)} (from the frozen closing prices)`);
  console.log(
    `    EV cost    : ${(cheapCost / cheapCount).toFixed(3)} wins per week, ` +
    `${cheapCost.toFixed(1)} across the season`,
  );
  console.log(
    `\n  ${cheapWins} vs ${expectedDogWins.toFixed(1)} is noise at n=18, not an edge — ` +
    `the point is the cost line.`,
  );
  console.log(
    `  One flip a week cost about ${cheapCost.toFixed(0)} win${Math.round(cheapCost) === 1 ? "" : "s"} ` +
    `across a whole season. That is the price of not being identical.`,
  );

  console.log("\n  The tradeoff, if you want a second dog (2025 averages):\n");
  console.log("    dog by spread     n    dog win %   flip costs   field on fav*");
  for (const [lo, hi, lab] of [
    [0, 2.5, "pk to 2"],
    [2.5, 4.5, "2.5 to 4"],
    [4.5, 7.5, "4.5 to 7"],
    [7.5, 99, "7.5+"],
  ] as Array<[number, number, string]>) {
    const cell = conf.filter((g) => Math.abs(g.spread) >= lo && Math.abs(g.spread) < hi);
    if (cell.length === 0) continue;
    const pFav = cell.reduce((a, g) => a + Math.max(g.pHomeMl, 1 - g.pHomeMl), 0) / cell.length;
    // Field share on the favourite under the page's stated-prior field model.
    const share = sigmoid(1.3 * logit(pFav));
    console.log(
      `    ${lab.padEnd(14)}${String(cell.length).padStart(4)}` +
      `${((1 - pFav) * 100).toFixed(1).padStart(11)}%` +
      `${(2 * pFav - 1).toFixed(3).padStart(13)}` +
      `${(share * 100).toFixed(0).padStart(14)}%`,
    );
  }
  console.log("\n    * modelled, not observed — this repo has no pick-share feed.");
  console.log("      It is the one number here that is an assumption.");
  console.log("\n  Bigger dogs separate you from more of the field but cost far more.");
  console.log("  The cheap band buys most of the separation for a fraction of the");
  console.log("  price, which is why the optimizer keeps landing there.");
  console.log("\n  The lesson is not that any of this beats the market — it does not.");
  console.log("  It is that flipping is the cheapest way to stop being identical to");
  console.log("  everyone else, which is what was actually costing you.");

  console.log("\n" + "=".repeat(78));
}

main().catch((e) => { console.error(e); process.exit(1); });
