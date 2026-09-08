/**
 * Game archetypes — NFL 2020-2025.
 *
 * A different purpose from the five studies before it, and the difference is
 * the whole point.
 *
 * Those asked "does this situation beat the closing line". The answer was no,
 * every time, and it will be no here too. This asks a question that does NOT
 * require beating anyone: **which games will the field misread?**
 *
 * The distinction that makes archetypes useful in a pick'em pool:
 *
 *   The MARKET prices every one of these situations. Rest, travel, kickoff
 *   slot and last week's result are public months ahead, and the five previous
 *   studies confirm the line absorbs them.
 *
 *   The FIELD does not. A pool entrant is not running a model. They are
 *   reacting to what they remember -- a team that just won on national
 *   television, a team humiliated last Sunday, a team "due" after a bye.
 *
 * So an archetype where the MARKET IS CALIBRATED and the STORY IS LOUD is
 * exactly where your rivals' picks will drift from the price while yours do
 * not. That is leverage, and it needs no predictive edge at all -- it needs
 * the market to be right and the room to be wrong.
 *
 * Every archetype is therefore reported on two axes:
 *
 *   OUTCOME    does the tagged team beat its implied probability? Descriptive,
 *              with n and a CI. Expected to be zero everywhere, and a non-zero
 *              cell at this many comparisons is a false positive until proven
 *              otherwise.
 *   VISIBILITY how loudly the archetype announces itself to a casual player.
 *              A STATED PRIOR, not a measurement -- this repo has no
 *              pick-share feed, and that limitation is the reason this column
 *              is a judgement rather than a number.
 *
 * Sample: 1,615 regular-season games with complete closing moneylines and
 * results, 2020-2025 -- twice the 816 used previously. It still cannot resolve
 * a 2pp effect (that needs ~4,900), and section 0 says so before any rate is
 * shown.
 *
 * Run: npm run analyze:archetypes
 */

const SCHEDULES =
  "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv";
const SEASON_MIN = 2020;
const SEASON_MAX = 2025;

// ---------------------------------------------------------------------------
const PACIFIC = new Set(["LA", "LAC", "SF", "SEA", "LV"]);
const MOUNTAIN = new Set(["ARI", "DEN"]);
const CENTRAL = new Set(["DAL", "HOU", "CHI", "GB", "MIN", "KC", "NO", "TEN"]);
/** 0 Eastern, 1 Central, 2 Mountain, 3 Pacific. */
function tz(t: string): number {
  if (PACIFIC.has(t)) return 3;
  if (MOUNTAIN.has(t)) return 2;
  if (CENTRAL.has(t)) return 1;
  return 0;
}

type Row = {
  season: number; week: number; gameday: string; weekday: string; gametime: string;
  away: string; home: string; awayScore: number; homeScore: number;
  location: string; awayRest: number; homeRest: number;
  awayMl: number; homeMl: number; spread: number; div: boolean; roof: string;
};

/** One side of one game, with everything an archetype rule can need. */
type TeamGame = {
  season: number; week: number; team: string; opp: string;
  isHome: boolean; won: boolean; impliedWin: number; margin: number;
  rest: number; oppRest: number; weekday: string; hourEt: number;
  neutralSite: boolean; div: boolean; roof: string;
  /** Filled in a second pass from the team's own previous game. */
  prev?: { neutralSite: boolean; weekday: string; margin: number; won: boolean; hourEt: number };
};

const implied = (a: number) => (a < 0 ? -a / (-a + 100) : 100 / (a + 100));

function parseCsv(text: string): Record<string, string>[] {
  const lines = text.trim().split(/\r?\n/);
  const head = lines[0].split(",");
  const out: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cells: string[] = [];
    let cur = ""; let q = false;
    for (const ch of lines[i]) {
      if (ch === '"') q = !q;
      else if (ch === "," && !q) { cells.push(cur); cur = ""; }
      else cur += ch;
    }
    cells.push(cur);
    const r: Record<string, string> = {};
    head.forEach((h, j) => { r[h] = cells[j] ?? ""; });
    out.push(r);
  }
  return out;
}

/** "13:00" -> 13. nflverse gametime is already Eastern. */
function hourOf(t: string): number {
  const m = /^(\d{1,2}):/.exec(t);
  return m ? Number(m[1]) : -1;
}

// ---------------------------------------------------------------------------
// Archetypes. Each tags ONE SIDE of a game.
// ---------------------------------------------------------------------------
type Archetype = {
  code: string;
  label: string;
  /** How loudly this announces itself to a pool entrant who is not modelling. */
  visibility: "loud" | "moderate" | "quiet";
  /** Why the field might misprice it, in one line. */
  story: string;
  test: (t: TeamGame) => boolean;
};

const ARCHETYPES: Archetype[] = [
  {
    code: "OFF_INTERNATIONAL",
    label: "Game after an international game",
    visibility: "moderate",
    story: "Travel and body clock are talked about; whether they matter is not obvious.",
    test: (t) => t.prev?.neutralSite === true,
  },
  {
    code: "AT_INTERNATIONAL",
    label: "Playing at a neutral / international site",
    visibility: "loud",
    story: "Announced for months; the novelty invites over-thinking.",
    test: (t) => t.neutralSite,
  },
  {
    code: "WEST_TEAM_EARLY",
    label: "Pacific team kicking off at 1pm ET",
    visibility: "loud",
    story: "The single most repeated angle in football media.",
    test: (t) => tz(t.team) === 3 && !t.isHome && t.hourEt === 13,
  },
  {
    code: "CROSS_COUNTRY",
    label: "Travelling three time zones",
    visibility: "moderate",
    story: "Noticed when it is coast to coast, ignored otherwise.",
    test: (t) => !t.isHome && Math.abs(tz(t.team) - tz(t.opp)) === 3,
  },
  {
    code: "OFF_MONDAY",
    label: "Sunday game after a Monday night game",
    visibility: "moderate",
    story: "A short week that is easy to miss on a Sunday-morning card.",
    test: (t) => t.prev?.weekday === "Monday" && t.weekday === "Sunday",
  },
  {
    code: "SHORT_WEEK",
    label: "Thursday game (short week)",
    visibility: "loud",
    story: "Flagged by the schedule itself.",
    test: (t) => t.weekday === "Thursday" && t.rest <= 4,
  },
  {
    code: "OFF_BYE",
    label: "Coming off a bye",
    visibility: "loud",
    story: "Universally cited, in both directions -- rested, or rusty.",
    test: (t) => t.rest >= 13,
  },
  {
    code: "REST_EDGE",
    label: "Three or more days more rest than the opponent",
    visibility: "quiet",
    story: "Requires comparing both teams' schedules; most entrants will not.",
    test: (t) => t.rest - t.oppRest >= 3,
  },
  {
    code: "OFF_BLOWOUT_WIN",
    label: "Coming off a 17+ point win",
    visibility: "loud",
    story: "The purest recency bias there is.",
    test: (t) => (t.prev?.margin ?? 0) >= 17,
  },
  {
    code: "OFF_BLOWOUT_LOSS",
    label: "Coming off a 17+ point loss",
    visibility: "loud",
    story: "Teams get written off on one bad Sunday.",
    test: (t) => (t.prev?.margin ?? 0) <= -17,
  },
  {
    code: "OFF_PRIMETIME_WIN",
    label: "Coming off a primetime win",
    visibility: "loud",
    story: "Everyone watched it. Availability bias in its cleanest form.",
    test: (t) => t.prev?.won === true && (t.prev?.hourEt ?? 0) >= 20,
  },
  {
    code: "IN_PRIMETIME",
    label: "Playing in primetime",
    visibility: "loud",
    story: "The game itself is the story of the week.",
    test: (t) => t.hourEt >= 20,
  },
  {
    code: "HOME_DOG",
    label: "Home underdog",
    visibility: "loud",
    story: "Home crowd plus a plus number is the classic upset pick.",
    test: (t) => t.isHome && t.impliedWin < 0.5,
  },
  {
    code: "DIVISIONAL",
    label: "Divisional game",
    visibility: "loud",
    story: "\"Throw the records out\" is said about every one of them.",
    test: (t) => t.div,
  },
  {
    code: "COLD_OUTDOOR_LATE",
    label: "Outdoors, week 14+, northern venue",
    visibility: "moderate",
    story: "Weather narratives spike late in the season.",
    test: (t) =>
      t.week >= 14 && t.roof === "outdoors" &&
      ["GB", "CHI", "BUF", "NE", "CLE", "PIT", "DEN", "NYJ", "NYG", "PHI", "WAS", "BAL", "CIN", "KC"].includes(t.opp),
  },
];

// ---------------------------------------------------------------------------
function mean(xs: number[]): number {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : NaN;
}

/** Week-clustered bootstrap on (actual win rate - implied win rate). */
function bootstrap(set: TeamGame[], iters = 4000, seed = 8191) {
  if (set.length === 0) return null;
  const byWeek = new Map<string, TeamGame[]>();
  for (const t of set) {
    const k = `${t.season}-${t.week}`;
    if (!byWeek.has(k)) byWeek.set(k, []);
    byWeek.get(k)!.push(t);
  }
  const clusters = [...byWeek.values()];
  const gapOf = (s: TeamGame[]) =>
    s.length === 0 ? NaN : s.filter((t) => t.won).length / s.length - mean(s.map((t) => t.impliedWin));
  let st = seed >>> 0;
  const rnd = () => {
    st ^= st << 13; st >>>= 0;
    st ^= st >>> 17;
    st ^= st << 5; st >>>= 0;
    return st / 4294967296;
  };
  const draws: number[] = [];
  for (let i = 0; i < iters; i += 1) {
    const s: TeamGame[] = [];
    for (let c = 0; c < clusters.length; c += 1) s.push(...clusters[Math.floor(rnd() * clusters.length)]);
    const v = gapOf(s);
    if (Number.isFinite(v)) draws.push(v);
  }
  draws.sort((a, b) => a - b);
  return {
    n: set.length,
    implied: mean(set.map((t) => t.impliedWin)),
    actual: set.filter((t) => t.won).length / set.length,
    gap: gapOf(set),
    lo: draws[Math.floor(draws.length * 0.025)],
    hi: draws[Math.floor(draws.length * 0.975)],
  };
}

const pp = (x: number) => `${x >= 0 ? "+" : ""}${(x * 100).toFixed(1)}pp`;

/** Stable identifier for the game a team-game belongs to. */
function gameKey(t: TeamGame): string {
  return `${t.season}-${t.week}-${[t.team, t.opp].sort().join("-")}`;
}

async function main() {
  const res = await fetch(SCHEDULES);
  if (!res.ok) throw new Error(`nflverse schedules: HTTP ${res.status}`);
  const raw = parseCsv(await res.text());

  const rows: Row[] = [];
  for (const r of raw) {
    const season = Number(r.season);
    if (r.game_type !== "REG" || season < SEASON_MIN || season > SEASON_MAX) continue;
    if (!r.home_moneyline || !r.away_moneyline || r.home_score === "" || r.away_score === "") continue;
    rows.push({
      season, week: Number(r.week), gameday: r.gameday, weekday: r.weekday, gametime: r.gametime,
      away: r.away_team, home: r.home_team,
      awayScore: Number(r.away_score), homeScore: Number(r.home_score),
      location: r.location,
      awayRest: Number(r.away_rest || 7), homeRest: Number(r.home_rest || 7),
      awayMl: Number(r.away_moneyline), homeMl: Number(r.home_moneyline),
      spread: Number(r.spread_line || 0), div: r.div_game === "1", roof: r.roof,
    });
  }
  rows.sort((a, b) => a.season - b.season || a.week - b.week);

  // Expand to team-games.
  const teamGames: TeamGame[] = [];
  for (const g of rows) {
    if (g.homeScore === g.awayScore) continue; // ties: no winner to score
    const ih = implied(g.homeMl);
    const ia = implied(g.awayMl);
    const pHome = ih / (ih + ia);
    const hour = hourOf(g.gametime);
    const neutral = g.location !== "Home";
    teamGames.push({
      season: g.season, week: g.week, team: g.home, opp: g.away, isHome: true,
      won: g.homeScore > g.awayScore, impliedWin: pHome, margin: g.homeScore - g.awayScore,
      rest: g.homeRest, oppRest: g.awayRest, weekday: g.weekday, hourEt: hour,
      neutralSite: neutral, div: g.div, roof: g.roof,
    });
    teamGames.push({
      season: g.season, week: g.week, team: g.away, opp: g.home, isHome: false,
      won: g.awayScore > g.homeScore, impliedWin: 1 - pHome, margin: g.awayScore - g.homeScore,
      rest: g.awayRest, oppRest: g.homeRest, weekday: g.weekday, hourEt: hour,
      neutralSite: neutral, div: g.div, roof: g.roof,
    });
  }

  // Second pass: attach each team's previous game in the same season.
  const bySeasonTeam = new Map<string, TeamGame[]>();
  for (const t of teamGames) {
    const k = `${t.season}-${t.team}`;
    if (!bySeasonTeam.has(k)) bySeasonTeam.set(k, []);
    bySeasonTeam.get(k)!.push(t);
  }
  for (const list of bySeasonTeam.values()) {
    list.sort((a, b) => a.week - b.week);
    for (let i = 1; i < list.length; i += 1) {
      const p = list[i - 1];
      list[i].prev = {
        neutralSite: p.neutralSite, weekday: p.weekday, margin: p.margin,
        won: p.won, hourEt: p.hourEt,
      };
    }
  }

  console.log("=".repeat(86));
  console.log("GAME ARCHETYPES — NFL 2020-2025");
  console.log(`${rows.length} games, ${teamGames.length} team-games with closing moneylines and a result`);
  console.log("=".repeat(86));

  // -------------------------------------------------------------------------
  console.log("\n\n0. WHAT THIS SAMPLE CAN AND CANNOT SETTLE");
  console.log("-".repeat(86));
  const need = (d: number) => Math.ceil((1.959964 + 0.841621) ** 2 * 0.25 / d ** 2);
  console.log(`  Detecting a  2pp effect needs ${need(0.02)} team-games`);
  console.log(`  Detecting a  5pp effect needs ${need(0.05)}`);
  console.log(`  Detecting a 10pp effect needs ${need(0.1)}`);
  console.log(`  This sample: ${teamGames.length} team-games total, far fewer per archetype.`);
  console.log("\n  Doubling the data from 816 games to 1,615 does not change the verdict");
  console.log("  on outcomes. It DOES make the descriptive rates steadier, and it is the");
  console.log("  VISIBILITY column that this analysis is actually for.");

  // -------------------------------------------------------------------------
  console.log("\n\n1. ARCHETYPE OUTCOMES — descriptive, concluding nothing");
  console.log("-".repeat(86));
  console.log("  Tagged team's actual win rate minus its implied win rate.\n");
  console.log(
    "  archetype".padEnd(38) + "n".padStart(6) + "implied".padStart(10) +
    "actual".padStart(9) + "gap".padStart(9) + "  95% CI",
  );

  // Baseline uses the favourite side for the same reason: over all team-games
  // the gap is 0 by construction and says nothing.
  const perGameAll = new Map<string, TeamGame[]>();
  for (const t of teamGames) {
    const k = gameKey(t);
    if (!perGameAll.has(k)) perGameAll.set(k, []);
    perGameAll.get(k)!.push(t);
  }
  const favouriteSides = [...perGameAll.values()].map((v) =>
    v.length === 2 ? (v[0].impliedWin >= v[1].impliedWin ? v[0] : v[1]) : v[0],
  );
  const baseline = bootstrap(favouriteSides)!;
  console.log(
    "  " + "ALL GAMES (favourite side)".padEnd(36) + String(baseline.n).padStart(6) +
    `${(baseline.implied * 100).toFixed(1)}%`.padStart(10) +
    `${(baseline.actual * 100).toFixed(1)}%`.padStart(9) +
    pp(baseline.gap).padStart(9) + `  [${pp(baseline.lo)}, ${pp(baseline.hi)}]`,
  );
  console.log("  " + "-".repeat(82));

  const results: Array<{
    a: Archetype;
    b: NonNullable<ReturnType<typeof bootstrap>>;
    mode: "team" | "favourite";
  }> = [];
  for (const a of ARCHETYPES) {
    let set = teamGames.filter(a.test);
    if (set.length === 0) continue;

    // SYMMETRIC archetypes tag BOTH sides of a game -- divisional, primetime,
    // neutral site. For those the tagged-team metric is degenerate: implied
    // sums to exactly 1 across each pair, so the win rate is 50% and the gap
    // is 0 by construction, with a zero-width CI. That is arithmetic, not a
    // finding, and reporting it as one would be worse than useless. When an
    // archetype is mostly symmetric the question becomes the same one the
    // earlier studies asked -- does the FAVOURITE beat its price in these
    // games -- so one row per game is kept, the favourite's.
    const perGame = new Map<string, TeamGame[]>();
    for (const t of set) {
      const k = gameKey(t);
      if (!perGame.has(k)) perGame.set(k, []);
      perGame.get(k)!.push(t);
    }
    const bothSides = [...perGame.values()].filter((v) => v.length === 2).length;
    const symmetric = bothSides / perGame.size > 0.5;
    const mode: "team" | "favourite" = symmetric ? "favourite" : "team";
    if (symmetric) {
      set = [...perGame.values()].map((v) =>
        v.length === 2 ? (v[0].impliedWin >= v[1].impliedWin ? v[0] : v[1]) : v[0],
      );
    }

    const b = bootstrap(set);
    if (!b) continue;
    results.push({ a, b, mode });
    const excl = b.lo > 0 || b.hi < 0;
    console.log(
      `  ${a.label.padEnd(36)}${String(b.n).padStart(6)}` +
      `${(b.implied * 100).toFixed(1)}%`.padStart(10) +
      `${(b.actual * 100).toFixed(1)}%`.padStart(9) +
      pp(b.gap).padStart(9) + `  [${pp(b.lo)}, ${pp(b.hi)}]` +
      (mode === "favourite" ? "  (fav)" : "") +
      (excl ? "  <-- excludes zero" : ""),
    );
  }
  console.log(
    "\n  (fav) = archetype tags both teams, so the tagged-team gap would be 0 by\n" +
    "  construction; the favourite's gap is shown instead.",
  );
  const fp = 1 - Math.pow(0.95, ARCHETYPES.length);
  const excluded = results.filter((r) => r.b.lo > 0 || r.b.hi < 0).length;
  console.log(
    `\n  ${excluded} of ${ARCHETYPES.length} exclude zero. With ${ARCHETYPES.length} archetypes screened, ` +
    `P(>=1 by chance) ~ ${(fp * 100).toFixed(0)}%,`,
  );
  console.log("  and this is the sixth pass over overlapping data. Treat any survivor as");
  console.log("  a false positive unless it earns its own pre-registered study.");

  // -------------------------------------------------------------------------
  // Era split for anything that flagged. 2020-2022 is genuinely fresh: every
  // previous study in this repo used 2023-2025 only, so the older half is data
  // this line of enquiry has never seen. That makes it a real replication test
  // rather than another slice of the same games -- which is exactly what a
  // lone survivor out of fifteen needs before it is worth anything.
  const flagged = results.filter((r) => r.b.lo > 0 || r.b.hi < 0);
  if (flagged.length > 0) {
    console.log("\n  Replication check on the flagged cells:");
    console.log("  2020-2022 has never been examined by any earlier study here, so it is");
    console.log("  an out-of-sample half rather than a re-slice of the same games.\n");
    for (const { a, mode } of flagged) {
      const pick = (min: number, max: number) => {
        let s = teamGames.filter((t) => a.test(t) && t.season >= min && t.season <= max);
        if (mode === "favourite") {
          const per = new Map<string, TeamGame[]>();
          for (const t of s) {
            const k = gameKey(t);
            if (!per.has(k)) per.set(k, []);
            per.get(k)!.push(t);
          }
          s = [...per.values()].map((v) =>
            v.length === 2 ? (v[0].impliedWin >= v[1].impliedWin ? v[0] : v[1]) : v[0],
          );
        }
        return bootstrap(s);
      };
      const early = pick(2020, 2022);
      const late = pick(2023, 2025);
      console.log(`  ${a.label}`);
      for (const [era, b] of [["2020-2022 (fresh)", early], ["2023-2025 (seen)", late]] as const) {
        if (!b) continue;
        const excl = b.lo > 0 || b.hi < 0;
        console.log(
          `    ${era.padEnd(20)}n=${String(b.n).padStart(4)}  ${pp(b.gap).padStart(8)}  ` +
          `[${pp(b.lo)}, ${pp(b.hi)}]  ${excl ? "holds" : "does not hold on its own"}`,
        );
      }
      const bothPositive =
        early && late && Math.sign(early.gap) === Math.sign(late.gap);
      console.log(
        `    => ${bothPositive ? "same sign in both halves — worth a pre-registered study, not a bet" : "sign disagrees between halves — noise"}\n`,
      );
    }
  }

  // -------------------------------------------------------------------------
  console.log("\n\n2. THE COLUMN THAT MATTERS — where will the ROOM be wrong?");
  console.log("-".repeat(86));
  console.log("  Leverage does not need the market to be wrong. It needs the market to be");
  console.log("  RIGHT and the room to be LOUD. An archetype whose gap is ~0 (the price");
  console.log("  already handles it) but whose story is loud (your rivals will not) is");
  console.log("  where their card drifts from the price and yours does not.\n");
  console.log("  VISIBILITY IS A STATED PRIOR. There is no pick-share feed here, so it is");
  console.log("  a judgement about how loudly a thing announces itself, not a measurement.\n");

  const ranked = [...results].sort((a, b) => {
    const rank = { loud: 0, moderate: 1, quiet: 2 } as const;
    return rank[a.a.visibility] - rank[b.a.visibility] || Math.abs(a.b.gap) - Math.abs(b.b.gap);
  });
  console.log("  archetype".padEnd(38) + "visibility".padStart(11) + "market gap".padStart(12) + "   read");
  for (const { a, b } of ranked) {
    const marketFine = Math.abs(b.gap) < 0.03;
    const read =
      a.visibility === "loud" && marketFine
        ? "LEVERAGE — priced, but the room will react"
        : a.visibility === "loud"
          ? "loud, but the gap is noisy; do not trade it"
          : a.visibility === "quiet"
            ? "quiet — the room will not notice, so no leverage either"
            : "moderate";
    console.log(
      `  ${a.label.padEnd(36)}${a.visibility.padStart(11)}${pp(b.gap).padStart(12)}   ${read}`,
    );
  }

  console.log("\n\n3. HOW TO USE THIS ON A CARD");
  console.log("-".repeat(86));
  console.log("  The archetypes do NOT tell you who wins -- section 1 is a wall of noise");
  console.log("  and that is the expected result.");
  console.log("");
  console.log("  They tell you which way the ROOM will lean, so:");
  console.log("    - a favourite carrying a LOUD positive story is one your rivals will");
  console.log("      pile onto. It is a poor differentiation target: flipping it is");
  console.log("      expensive AND crowded on the other side.");
  console.log("    - an underdog whose opponent carries a LOUD positive story is where");
  console.log("      the room is most over-committed. If that game is also near a coin");
  console.log("      flip, it is the cheapest flip on the board AND the most contrarian.");
  console.log("");
  console.log("  That second case is the one worth hunting: cheap by price, crowded by");
  console.log("  narrative. Price still decides WHETHER to flip; the archetype only");
  console.log("  breaks ties between similarly-priced games.");
  console.log("\n" + "=".repeat(86));
}

main().catch((e) => { console.error(e); process.exit(1); });
