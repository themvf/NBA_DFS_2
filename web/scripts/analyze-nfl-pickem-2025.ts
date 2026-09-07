/**
 * Straight pick'em edge screen — NFL 2023-2025.
 *
 * The user's pool is straight pick'em: pick the winner, one point a game, no
 * confidence weights. That removes the confidence lever entirely, so the only
 * way to differ from the field is to take a side the field does not. This
 * script asks whether there is ever a reason to.
 *
 * PRE-REGISTRATION. Written and committed before 2025 was examined.
 *
 *   Discovery set: 2023 + 2024 (544 games).
 *   Confirmation set: 2025 (272 games), looked at only after the segment list
 *   below was frozen.
 *
 *   Hypothesis H: the closing moneyline is miscalibrated inside at least one of
 *   the segments listed in SEGMENTS, such that the favourite wins at a rate
 *   reliably different from its de-vigged implied probability.
 *
 *   Primary metric: realised favourite win rate minus mean implied favourite
 *   probability ("gap"), in percentage points, with a week-clustered bootstrap
 *   95% CI. Week-clustered because a pick'em entry lives or dies a week at a
 *   time, so the week is the unit a pool player actually experiences.
 *
 *   Decision rule, fixed in advance:
 *     - a segment ADVANCES from discovery only if its 2023-24 CI excludes zero;
 *     - an advancing segment CONFIRMS only if its 2025 CI also excludes zero
 *       AND points the same way;
 *     - the segment list is FIXED at 10. No segment is added, split, or
 *       re-cut after seeing a result. With 10 tests at nominal 95%,
 *       P(>=1 false positive by chance) is about 40%, which is reported next
 *       to any survivor rather than left for the reader to remember.
 *
 *   Honest prior: no edge. This repo has four independent confirmed negatives
 *   against closing lines (soccer ML/totals/first-scorer, MLB ML/totals,
 *   tennis ML, NFL total_walking), and the NFL closing moneyline is among the
 *   most efficient prices in sport. The expected outcome is that the market is
 *   well calibrated everywhere and the useful output of this script is
 *   section 3, which is structural rather than predictive.
 *
 * Run: npm run analyze:pickem-2025
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);

const DISCOVERY = [2023, 2024];
const CONFIRM = 2025;

type Game = {
  season: number;
  week: number;
  spread: number; // positive = home favoured (nflverse convention)
  pHome: number; // de-vigged
  homeWon: boolean | null; // null on a tie
  divGame: boolean;
  roof: string | null;
  homeRest: number | null;
  awayRest: number | null;
  kickoffHour: number | null; // ET
};

/** American price -> raw implied probability. */
function implied(american: number): number {
  return american < 0 ? -american / (-american + 100) : 100 / (american + 100);
}

// ---------------------------------------------------------------------------
// Segments — FIXED. Ten of them. Nothing is added after results are seen.
// ---------------------------------------------------------------------------
type Segment = { name: string; test: (g: Game) => boolean };

const SEGMENTS: Segment[] = [
  // "Home underdog" and "road favourite" are the SAME set of games, so only one
  // of them appears. The complement (home favourites) is listed instead, which
  // is a genuinely different slice.
  { name: "Road favourites (home dog)", test: (g) => g.pHome < 0.5 },
  { name: "Home favourites", test: (g) => g.pHome >= 0.5 },
  { name: "Divisional games", test: (g) => g.divGame },
  { name: "Big favourites (spread >= 7)", test: (g) => Math.abs(g.spread) >= 7 },
  { name: "Huge favourites (spread >= 10)", test: (g) => Math.abs(g.spread) >= 10 },
  { name: "Near coin flips (spread <= 3)", test: (g) => Math.abs(g.spread) <= 3 },
  { name: "Rest advantage >= 3 days for favourite", test: (g) => favouriteRestEdge(g) >= 3 },
  { name: "Primetime (kickoff 20:00 ET or later)", test: (g) => (g.kickoffHour ?? -1) >= 20 },
  { name: "Indoors (dome or closed roof)", test: (g) => g.roof === "dome" || g.roof === "closed" },
  { name: "Late season (week >= 15)", test: (g) => g.week >= 15 },
];

function favouriteRestEdge(g: Game): number {
  if (g.homeRest == null || g.awayRest == null) return 0;
  return g.pHome >= 0.5 ? g.homeRest - g.awayRest : g.awayRest - g.homeRest;
}

/** Implied probability of the side the market favours. */
function favProb(g: Game): number {
  return Math.max(g.pHome, 1 - g.pHome);
}

/** Did the market favourite win? Null on a tie. */
function favWon(g: Game): boolean | null {
  if (g.homeWon == null) return null;
  return g.pHome >= 0.5 ? g.homeWon : !g.homeWon;
}

// ---------------------------------------------------------------------------
// Week-clustered bootstrap on the gap
// ---------------------------------------------------------------------------
function bootstrapGap(games: Game[], iters = 4000, seed = 20260907) {
  const graded = games.filter((g) => favWon(g) !== null);
  if (graded.length === 0) return null;

  const byWeek = new Map<string, Game[]>();
  for (const g of graded) {
    const key = `${g.season}-${g.week}`;
    if (!byWeek.has(key)) byWeek.set(key, []);
    byWeek.get(key)!.push(g);
  }
  const clusters = [...byWeek.values()];

  const gapOf = (set: Game[]) => {
    if (set.length === 0) return NaN;
    const hits = set.filter((g) => favWon(g) === true).length;
    const impliedMean = set.reduce((s, g) => s + favProb(g), 0) / set.length;
    return hits / set.length - impliedMean;
  };

  const point = gapOf(graded);

  let s = seed >>> 0;
  const rnd = () => {
    s ^= s << 13; s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5; s >>>= 0;
    return s / 4294967296;
  };

  const draws: number[] = [];
  for (let i = 0; i < iters; i += 1) {
    const sample: Game[] = [];
    for (let c = 0; c < clusters.length; c += 1) {
      sample.push(...clusters[Math.floor(rnd() * clusters.length)]);
    }
    const g = gapOf(sample);
    if (Number.isFinite(g)) draws.push(g);
  }
  draws.sort((a, b) => a - b);
  return {
    n: graded.length,
    weeks: clusters.length,
    impliedMean: graded.reduce((a, g) => a + favProb(g), 0) / graded.length,
    actual: graded.filter((g) => favWon(g) === true).length / graded.length,
    gap: point,
    lo: draws[Math.floor(draws.length * 0.025)],
    hi: draws[Math.floor(draws.length * 0.975)],
  };
}

function pp(x: number): string {
  return `${x >= 0 ? "+" : ""}${(x * 100).toFixed(2)}pp`;
}

// ---------------------------------------------------------------------------
async function load(seasons: number[]): Promise<Game[]> {
  const rows = await sql`
    SELECT season, week, quoted_spread_line AS spread,
           quoted_home_ml AS hml, quoted_away_ml AS aml,
           home_score AS hs, away_score AS as_, div_game, roof,
           home_rest, away_rest,
           EXTRACT(HOUR FROM kickoff AT TIME ZONE 'America/New_York') AS hour
    FROM nfl_season_games
    WHERE season = ANY(${sql.unsafe(`ARRAY[${seasons.join(",")}]`)})
      AND quoted_home_ml IS NOT NULL AND quoted_away_ml IS NOT NULL
      AND home_score IS NOT NULL AND away_score IS NOT NULL
    ORDER BY season, week
  `;
  return rows.map((r) => {
    const hRaw = implied(Number(r.hml));
    const aRaw = implied(Number(r.aml));
    const hs = Number(r.hs);
    const as_ = Number(r.as_);
    return {
      season: Number(r.season),
      week: Number(r.week),
      spread: Number(r.spread),
      pHome: hRaw / (hRaw + aRaw),
      homeWon: hs === as_ ? null : hs > as_,
      divGame: Boolean(r.div_game),
      roof: r.roof != null ? String(r.roof) : null,
      homeRest: r.home_rest != null ? Number(r.home_rest) : null,
      awayRest: r.away_rest != null ? Number(r.away_rest) : null,
      kickoffHour: r.hour != null ? Number(r.hour) : null,
    };
  });
}

async function main() {
  const discovery = await load(DISCOVERY);
  const confirm = await load([CONFIRM]);

  console.log("=".repeat(78));
  console.log("STRAIGHT PICK'EM EDGE SCREEN");
  console.log(`Discovery ${DISCOVERY.join("+")}: ${discovery.length} games`);
  console.log(`Confirmation ${CONFIRM}: ${confirm.length} games`);
  console.log("=".repeat(78));

  // -------------------------------------------------------------------------
  console.log("\n\n1. IS THE CLOSING MONEYLINE CALIBRATED?");
  console.log("-".repeat(78));
  console.log("If it is, there is no edge in disagreeing with the price, and the");
  console.log("only lever left is WHICH game to differ on -- section 3.\n");

  for (const [label, set] of [
    [`${DISCOVERY.join("+")}`, discovery],
    [`${CONFIRM}`, confirm],
  ] as const) {
    const b = bootstrapGap(set)!;
    console.log(
      `  ${label.padEnd(10)} n=${String(b.n).padStart(3)}  ` +
      `implied ${(b.impliedMean * 100).toFixed(1)}%  actual ${(b.actual * 100).toFixed(1)}%  ` +
      `gap ${pp(b.gap)}  95% CI [${pp(b.lo)}, ${pp(b.hi)}]  ` +
      `${b.lo > 0 || b.hi < 0 ? "<-- EXCLUDES ZERO" : "includes zero"}`,
    );
  }

  console.log("\n  Reliability by implied probability of the favourite (2025):");
  const edges = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.9, 1.01];
  for (let i = 0; i < edges.length - 1; i += 1) {
    const bin = confirm.filter(
      (g) => favProb(g) >= edges[i] && favProb(g) < edges[i + 1] && favWon(g) !== null,
    );
    if (bin.length === 0) continue;
    const impliedMean = bin.reduce((s, g) => s + favProb(g), 0) / bin.length;
    const hit = bin.filter((g) => favWon(g) === true).length / bin.length;
    const bar = "#".repeat(Math.round(hit * 30));
    console.log(
      `    ${(edges[i] * 100).toFixed(0)}-${(edges[i + 1] * 100).toFixed(0)}%`.padEnd(12) +
      `n=${String(bin.length).padStart(3)}  said ${(impliedMean * 100).toFixed(1)}%  ` +
      `went ${(hit * 100).toFixed(1)}%  ${pp(hit - impliedMean).padStart(9)}  ${bar}`,
    );
  }

  // -------------------------------------------------------------------------
  console.log("\n\n2. PRE-REGISTERED SEGMENT SCREEN");
  console.log("-".repeat(78));
  console.log("Ten segments, fixed before 2025 was examined. A segment advances");
  console.log("only if its 2023-24 CI excludes zero; it confirms only if 2025");
  console.log("agrees in the same direction.\n");

  console.log(
    "  " + "Segment".padEnd(38) + "n".padStart(5) + "  " +
    "gap 2023-24".padStart(12) + "  " + "95% CI".padStart(24) + "  advance?",
  );
  const advanced: Segment[] = [];
  for (const seg of SEGMENTS) {
    const set = discovery.filter(seg.test);
    const b = bootstrapGap(set);
    if (!b || b.n < 30) {
      console.log(`  ${seg.name.padEnd(38)}${String(b?.n ?? 0).padStart(5)}   too few games`);
      continue;
    }
    const excl = b.lo > 0 || b.hi < 0;
    if (excl) advanced.push(seg);
    console.log(
      `  ${seg.name.padEnd(38)}${String(b.n).padStart(5)}  ${pp(b.gap).padStart(12)}  ` +
      `[${pp(b.lo)}, ${pp(b.hi)}]`.padStart(24) + `  ${excl ? "YES" : "no"}`,
    );
  }

  console.log(`\n  ${advanced.length} of ${SEGMENTS.length} segments advanced to confirmation.`);
  if (advanced.length === 0) {
    console.log("  Nothing to confirm. Under the pre-registered rule, H is dead:");
    console.log("  no segment showed a reliable calibration gap in discovery, so");
    console.log("  none is eligible to be tested on 2025. Reporting 2025 for the");
    console.log("  advanced set only would be reporting nothing -- which is the");
    console.log("  honest result, not a failure of the script.");
  } else {
    console.log("\n  Confirmation on 2025:\n");
    for (const seg of advanced) {
      const b = bootstrapGap(confirm.filter(seg.test));
      if (!b || b.n < 30) {
        console.log(`  ${seg.name.padEnd(38)} too few 2025 games (${b?.n ?? 0})`);
        continue;
      }
      const excl = b.lo > 0 || b.hi < 0;
      console.log(
        `  ${seg.name.padEnd(38)}${String(b.n).padStart(5)}  ${pp(b.gap).padStart(12)}  ` +
        `[${pp(b.lo)}, ${pp(b.hi)}]`.padStart(24) + `  ${excl ? "CONFIRMS" : "FAILS"}`,
      );
    }
    const fp = 1 - Math.pow(0.95, SEGMENTS.length);
    console.log(
      `\n  Multiple comparisons: ${SEGMENTS.length} segments screened, so ` +
      `P(>=1 false positive) ~ ${(fp * 100).toFixed(0)}%.`,
    );
  }

  // -------------------------------------------------------------------------
  console.log("\n\n3. WHAT ACTUALLY DECIDES A STRAIGHT PICK'EM POOL");
  console.log("-".repeat(78));
  console.log("Structural, not predictive. Uses real 2025 slates and results.\n");

  const byWeek = new Map<number, Game[]>();
  for (const g of confirm) {
    if (!byWeek.has(g.week)) byWeek.set(g.week, []);
    byWeek.get(g.week)!.push(g);
  }

  console.log("  wk  games  chalk  best   chalk%   flips available (p<=60%)");
  let chalkTotal = 0;
  let gamesTotal = 0;
  const chalkScores: number[] = [];
  const coinflipCounts: number[] = [];
  for (const week of [...byWeek.keys()].sort((a, b) => a - b)) {
    const set = byWeek.get(week)!;
    const graded = set.filter((g) => favWon(g) !== null);
    const chalk = graded.filter((g) => favWon(g) === true).length;
    const flips = set.filter((g) => favProb(g) <= 0.6).length;
    chalkTotal += chalk;
    gamesTotal += graded.length;
    chalkScores.push(chalk);
    coinflipCounts.push(flips);
    console.log(
      `  ${String(week).padStart(2)}  ${String(set.length).padStart(5)}  ` +
      `${String(chalk).padStart(5)}  ${String(graded.length).padStart(4)}  ` +
      `${((chalk / graded.length) * 100).toFixed(0).padStart(6)}%   ${flips}`,
    );
  }

  const meanChalk = chalkTotal / gamesTotal;
  const meanFlips = coinflipCounts.reduce((a, b) => a + b, 0) / coinflipCounts.length;
  console.log(
    `\n  Chalk (every favourite) went ${chalkTotal}/${gamesTotal} = ` +
    `${(meanChalk * 100).toFixed(1)}% across 2025.`,
  );
  console.log(
    `  A perfect week is rare: chalk's best week was ${Math.max(...chalkScores)} correct, ` +
    `worst ${Math.min(...chalkScores)}.`,
  );
  console.log(`  Games at or under 60% for the favourite: ${meanFlips.toFixed(1)} per week on average.`);
  console.log("  Those are the only games where a flip is cheap. Everything else costs real EV.");

  // How much does one flip cost, and how often does chalk get beaten?
  console.log("\n  Cost of one flip, by the favourite's implied probability (2025 mean):");
  for (const [lo, hi] of [[0.5, 0.55], [0.55, 0.6], [0.6, 0.65], [0.65, 0.75], [0.75, 1.01]] as const) {
    const bin = confirm.filter((g) => favProb(g) >= lo && favProb(g) < hi);
    if (bin.length === 0) continue;
    const mean = bin.reduce((s, g) => s + favProb(g), 0) / bin.length;
    console.log(
      `    fav ${(lo * 100).toFixed(0)}-${(hi * 100).toFixed(0)}%`.padEnd(16) +
      `n=${String(bin.length).padStart(3)}  ` +
      `flipping costs ${(2 * mean - 1).toFixed(3)} expected wins`,
    );
  }

  // -------------------------------------------------------------------------
  console.log("\n\n4. HOW OFTEN DOES CHALK ACTUALLY WIN A POOL?");
  console.log("-".repeat(78));
  console.log("Real 2025 results. Rivals simulated as independent pickers who take");
  console.log("the favourite with probability sigmoid(1.3 * logit(p)) -- the same");
  console.log("stated-prior field model the page uses, and its weakest assumption.\n");

  const SIMS = 20000;
  let seed = 424242;
  const rnd = () => {
    seed ^= seed << 13; seed >>>= 0;
    seed ^= seed >>> 17;
    seed ^= seed << 5; seed >>>= 0;
    return seed / 4294967296;
  };

  for (const poolSize of [10, 25, 50, 200, 1000]) {
    let chalkWins = 0;
    let weeksCounted = 0;
    for (const week of byWeek.keys()) {
      const set = byWeek.get(week)!.filter((g) => favWon(g) !== null);
      if (set.length === 0) continue;
      weeksCounted += 1;
      const chalkScore = set.filter((g) => favWon(g) === true).length;
      const shares = set.map((g) => {
        const p = favProb(g);
        const l = Math.log(p / (1 - p));
        return 1 / (1 + Math.exp(-1.3 * l));
      });
      let winShare = 0;
      for (let s = 0; s < SIMS; s += 1) {
        let best = -1;
        let ties = 0;
        for (let k = 0; k < poolSize - 1; k += 1) {
          let score = 0;
          for (let i = 0; i < set.length; i += 1) {
            const tookFav = rnd() < shares[i];
            if (tookFav === (favWon(set[i]) === true)) score += 1;
          }
          if (score > best) { best = score; ties = 1; }
          else if (score === best) ties += 1;
        }
        if (chalkScore > best) winShare += 1;
        else if (chalkScore === best) winShare += 1 / (1 + ties);
      }
      chalkWins += winShare / SIMS;
    }
    console.log(
      `  Pool of ${String(poolSize).padStart(4)}: the all-favourites entry won ` +
      `${chalkWins.toFixed(2)} of ${weeksCounted} weeks ` +
      `(${((chalkWins / weeksCounted) * 100).toFixed(1)}%), vs ` +
      `${((1 / poolSize) * 100).toFixed(1)}% for a random entry.`,
    );
  }

  console.log("\n" + "=".repeat(78));
}

main().catch((e) => { console.error(e); process.exit(1); });
