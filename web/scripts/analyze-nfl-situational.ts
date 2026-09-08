/**
 * Situational angles — NFL 2023-2025. A POWER ANALYSIS, not a hypothesis test.
 *
 * The proposed angles (west-coast body clock, international games, Thursday
 * night, Monday night, post-bye, cross-country travel) all have real, named
 * mechanisms. That is exactly why this script does NOT run them as
 * pre-registered tests: with three seasons of NFL there is not enough data to
 * answer any of them, and dressing an underpowered scan up as a test would
 * manufacture findings rather than discover them.
 *
 * So the order is deliberately reversed from the previous two studies. Section
 * 0 asks whether each question is ANSWERABLE before section 1 looks at any
 * answer. If the sample cannot resolve an effect of the size these angles
 * plausibly have, the descriptive number that follows is noise with a
 * confidence interval drawn around it, and is labelled that way.
 *
 * Why 2 percentage points is the yardstick: the closing line already prices
 * every one of these situations -- rest, travel and kickoff slot are visible to
 * every oddsmaker weeks ahead. A residual mispricing large enough to matter
 * after that is small by construction. Published estimates of situational
 * effects in efficient markets land in the low single digits, so 2pp is
 * generous, not conservative.
 *
 * This is the THIRD pass over the same 816 games. The cumulative test family
 * across all three studies is now large enough that a lone "significant"
 * result would be more likely false than true, which is a further reason this
 * one reports descriptively and concludes nothing.
 *
 * Run: npm run analyze:situational
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);

// ---------------------------------------------------------------------------
// Timezones from nfl_teams.city. Arizona is treated as Mountain; it does not
// observe DST, so for September-January it drifts between Pacific and Mountain
// offsets -- an approximation, and irrelevant at these sample sizes.
// ---------------------------------------------------------------------------
const PACIFIC = new Set(["LAC", "LAR", "SF", "SEA", "LV"]);
const MOUNTAIN = new Set(["ARI", "DEN"]);
const CENTRAL = new Set(["DAL", "HOU", "CHI", "GB", "MIN", "KC", "NO", "TEN"]);

/** 0 = Eastern, 1 = Central, 2 = Mountain, 3 = Pacific. */
function tz(abbrev: string): number {
  if (PACIFIC.has(abbrev)) return 3;
  if (MOUNTAIN.has(abbrev)) return 2;
  if (CENTRAL.has(abbrev)) return 1;
  return 0;
}

type Game = {
  season: number;
  week: number;
  home: string;
  away: string;
  pHome: number;
  homeWon: boolean;
  homeRest: number;
  awayRest: number;
  dow: number; // 0 = Sunday
  hour: number; // ET
};

function implied(a: number): number {
  return a < 0 ? -a / (-a + 100) : 100 / (a + 100);
}
function favProb(g: Game): number {
  return Math.max(g.pHome, 1 - g.pHome);
}
function favWon(g: Game): boolean {
  return g.pHome >= 0.5 ? g.homeWon : !g.homeWon;
}

// ---------------------------------------------------------------------------
// The angles. Each is expressed as "does the FAVOURITE beat its price here",
// so a negative gap means underdogs in that spot are live.
// ---------------------------------------------------------------------------
type Angle = { name: string; test: (g: Game) => boolean };

const ANGLES: Angle[] = [
  { name: "International (kickoff before 11am ET)", test: (g) => g.hour < 11 },
  { name: "West-coast team playing 1pm ET", test: (g) => tz(g.away) === 3 && g.hour === 13 },
  { name: "East-coast team at Pacific site", test: (g) => tz(g.away) === 0 && tz(g.home) === 3 },
  { name: "Cross-country travel (3 tz)", test: (g) => Math.abs(tz(g.home) - tz(g.away)) === 3 },
  { name: "Thursday night", test: (g) => g.dow === 4 && g.hour >= 19 },
  { name: "Monday night", test: (g) => g.dow === 1 && g.hour >= 19 },
  { name: "Sunday night", test: (g) => g.dow === 0 && g.hour >= 20 },
  { name: "Favourite off a bye (rest >= 13)", test: (g) => (g.pHome >= 0.5 ? g.homeRest : g.awayRest) >= 13 },
  { name: "Underdog off a bye (rest >= 13)", test: (g) => (g.pHome >= 0.5 ? g.awayRest : g.homeRest) >= 13 },
  { name: "Home teams (all)", test: () => true },
];

/** Sample size for 80% power, two-sided alpha 0.05, on a proportion near 0.5. */
function requiredN(effect: number): number {
  return Math.ceil((1.959964 + 0.841621) ** 2 * 0.25 / effect ** 2);
}

function bootstrapGap(games: Game[], iters = 5000, seed = 5150) {
  if (games.length === 0) return null;
  const byWeek = new Map<string, Game[]>();
  for (const g of games) {
    const k = `${g.season}-${g.week}`;
    if (!byWeek.has(k)) byWeek.set(k, []);
    byWeek.get(k)!.push(g);
  }
  const clusters = [...byWeek.values()];
  const gapOf = (s: Game[]) =>
    s.length === 0
      ? NaN
      : s.filter(favWon).length / s.length - s.reduce((a, g) => a + favProb(g), 0) / s.length;

  let st = seed >>> 0;
  const rnd = () => {
    st ^= st << 13; st >>>= 0;
    st ^= st >>> 17;
    st ^= st << 5; st >>>= 0;
    return st / 4294967296;
  };
  const draws: number[] = [];
  for (let i = 0; i < iters; i += 1) {
    const s: Game[] = [];
    for (let c = 0; c < clusters.length; c += 1) s.push(...clusters[Math.floor(rnd() * clusters.length)]);
    const v = gapOf(s);
    if (Number.isFinite(v)) draws.push(v);
  }
  draws.sort((a, b) => a - b);
  return {
    n: games.length,
    implied: games.reduce((a, g) => a + favProb(g), 0) / games.length,
    actual: games.filter(favWon).length / games.length,
    gap: gapOf(games),
    lo: draws[Math.floor(draws.length * 0.025)],
    hi: draws[Math.floor(draws.length * 0.975)],
  };
}

const pp = (x: number) => `${x >= 0 ? "+" : ""}${(x * 100).toFixed(1)}pp`;

async function main() {
  const rows = await sql`
    SELECT g.season, g.week, h.abbreviation home, a.abbreviation away,
           g.quoted_home_ml hml, g.quoted_away_ml aml,
           g.home_score hs, g.away_score as_, g.home_rest, g.away_rest,
           EXTRACT(DOW FROM g.kickoff AT TIME ZONE 'America/New_York') AS "dow",
           EXTRACT(HOUR FROM g.kickoff AT TIME ZONE 'America/New_York') AS "hour"
    FROM nfl_season_games g
    JOIN nfl_teams h ON h.team_id = g.home_team_id
    JOIN nfl_teams a ON a.team_id = g.away_team_id
    WHERE g.season IN (2023,2024,2025) AND g.quoted_home_ml IS NOT NULL
      AND g.home_score IS NOT NULL AND g.home_score <> g.away_score
    ORDER BY g.season, g.week
  `;
  const games: Game[] = rows.map((r) => {
    const h = implied(Number(r.hml));
    const a = implied(Number(r.aml));
    return {
      season: Number(r.season),
      week: Number(r.week),
      home: String(r.home),
      away: String(r.away),
      pHome: h / (h + a),
      homeWon: Number(r.hs) > Number(r.as_),
      homeRest: Number(r.home_rest ?? 7),
      awayRest: Number(r.away_rest ?? 7),
      dow: Number(r.dow),
      hour: Number(r.hour),
    };
  });

  console.log("=".repeat(80));
  console.log("SITUATIONAL ANGLES — NFL 2023-2025  (POWER ANALYSIS, NOT A TEST)");
  console.log(`${games.length} games with closing moneylines and a decided result`);
  console.log("=".repeat(80));

  // -------------------------------------------------------------------------
  console.log("\n\n0. CAN THIS DATA ANSWER THE QUESTION AT ALL?");
  console.log("-".repeat(80));
  console.log("Games needed to detect an effect, 80% power, two-sided 5%:\n");
  for (const d of [0.02, 0.03, 0.05, 0.1, 0.15]) {
    console.log(`    a ${(d * 100).toFixed(0).padStart(2)}pp effect needs ${String(requiredN(d)).padStart(5)} games`);
  }
  console.log(`\n    entire 2023-2025 sample:      ${games.length} games`);
  console.log("\n  The closing line already prices rest, travel and kickoff slot —");
  console.log("  they are known weeks ahead. Whatever it misses is small, and a");
  console.log("  2-3pp residual needs thousands of games to see. We have hundreds.\n");

  console.log("  angle".padEnd(44) + "n".padStart(5) + "  CI half-width   smallest effect visible");
  for (const angle of ANGLES) {
    const set = games.filter(angle.test);
    if (set.length === 0) continue;
    const half = 1.959964 * Math.sqrt(0.25 / set.length);
    console.log(
      `  ${angle.name.padEnd(42)}${String(set.length).padStart(5)}` +
      `${("+/-" + (half * 100).toFixed(1) + "pp").padStart(15)}` +
      `${(">= " + (2.8 * Math.sqrt(0.25 / set.length) * 100).toFixed(0) + "pp").padStart(24)}`,
    );
  }
  console.log("\n  Every one of these needs a double-digit effect to register.");
  console.log("  Real situational edges, if they exist at all, are low single digits.");

  // -------------------------------------------------------------------------
  console.log("\n\n1. THE NUMBERS ANYWAY — DESCRIPTIVE, CONCLUDING NOTHING");
  console.log("-".repeat(80));
  console.log("Favourite's win rate minus its de-vigged implied probability.");
  console.log("Negative = underdogs did better than priced in that spot.\n");
  console.log("  angle".padEnd(44) + "n".padStart(5) + "  implied  actual" + "      gap".padStart(10) + "        95% CI");
  for (const angle of ANGLES) {
    const b = bootstrapGap(games.filter(angle.test));
    if (!b) continue;
    const excl = b.lo > 0 || b.hi < 0;
    console.log(
      `  ${angle.name.padEnd(42)}${String(b.n).padStart(5)}` +
      `${(b.implied * 100).toFixed(1).padStart(9)}%` +
      `${(b.actual * 100).toFixed(1).padStart(8)}%` +
      `${pp(b.gap).padStart(10)}` +
      `  [${pp(b.lo)}, ${pp(b.hi)}]${excl ? "  <-- excludes zero" : ""}`,
    );
  }

  console.log("\n  A CI that excludes zero here is NOT a finding. Ten angles are");
  console.log("  screened, so about one is expected to clear that bar by chance,");
  console.log("  and this is the third pass over the same 816 games.");

  // -------------------------------------------------------------------------
  console.log("\n\n2. WHAT WOULD IT TAKE TO ACTUALLY KNOW?");
  console.log("-".repeat(80));
  for (const angle of ANGLES.slice(0, 8)) {
    const set = games.filter(angle.test);
    if (set.length === 0) continue;
    const perSeason = set.length / 3;
    const need = requiredN(0.03);
    console.log(
      `  ${angle.name.padEnd(42)}${perSeason.toFixed(0).padStart(4)}/season  ` +
      `=> ${(need / perSeason).toFixed(0).padStart(4)} seasons for a 3pp effect`,
    );
  }
  console.log("\n  Those are not waits anyone is going to sit through, and the NFL");
  console.log("  changes its rules, schedule and rosters faster than the sample");
  console.log("  accumulates -- so the older half stops describing the same game.");
  console.log("\n" + "=".repeat(80));
}

main().catch((e) => { console.error(e); process.exit(1); });
