/**
 * Why do underdogs win? — NFL 2023-2025 upset anatomy.
 *
 * This is a DIFFERENT KIND OF QUESTION from the three studies before it, and
 * the difference is the point.
 *
 * Those asked "can I predict which underdog wins", which is a question about
 * the market, and the market won all three times. This asks "when an underdog
 * wins, what actually happened in the game" -- a question about football. It
 * is answerable, because it needs no edge over anyone: every game is in the
 * sample, the effects are large, and nothing has to beat a closing line.
 *
 * The trap to avoid is obvious once stated, so it is stated. Anything that
 * explains upsets falls into exactly one of two buckets:
 *
 *   KNOWABLE BEFORE KICKOFF  -> already in the price. Cannot help you pick.
 *   ONLY KNOWABLE AFTERWARDS -> explains the result perfectly, and is
 *                               useless for picking.
 *
 * So a finding here does not become a betting angle. What it can do is settle
 * WHY the previous three studies found nothing: if upsets are driven mostly by
 * events with no week-to-week persistence, then upsets are close to
 * unpredictable in principle, and the absence of an edge stops being a
 * disappointment and becomes an explanation.
 *
 * Section 4 is therefore the load-bearing one, and it carries a positive
 * control: the same persistence method is run on a statistic that IS known to
 * persist, so "no persistence" cannot be an artefact of a broken measurement.
 *
 * RESULT NOTE, added after the first run and left here deliberately. The
 * hypothesis above -- that upsets would turn out to be driven by fluky,
 * non-persistent events like turnovers -- is WRONG, and the data says so
 * plainly: EPA separates upsets from normal games more strongly than turnover
 * margin does (1.42 SD vs 1.04), 80% of winning underdogs also won the EPA
 * battle, and EPA is the most persistent statistic measured rather than the
 * least. Underdogs mostly win by genuinely outplaying the favourite, not by
 * out-lucking it. Section 5 was added to answer the question that then becomes
 * unavoidable: if the drivers persist, why is none of it predictable?
 *
 * Data: nflverse stats_team_week (fetched at run time, not vendored) joined to
 * this repo's nfl_season_games closing lines. Regular season only.
 *
 * Run: npm run analyze:upsets
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);

const SEASONS = [2023, 2024, 2025];
const NFLVERSE = (s: number) =>
  `https://github.com/nflverse/nflverse-data/releases/download/stats_team/stats_team_week_${s}.csv`;

/**
 * nflverse team codes -> this repo's abbreviations. Only two differ, and both
 * are named rather than pattern-matched: this repo has already shipped one
 * silent-null bug from an unmapped abbreviation (AZ vs ARI), so the join is
 * verified for completeness below and fails loudly rather than dropping rows.
 */
const TEAM_FIX: Record<string, string> = { LA: "LAR", WAS: "WSH" };

type TeamGame = {
  season: number;
  week: number;
  team: string;
  turnoversLost: number;
  epa: number;
  firstDowns: number;
  yards: number;
  nonOffensiveTds: number;
  sacksTaken: number;
  penalties: number;
  plays: number;
};

function parseCsv(text: string): Record<string, string>[] {
  const lines = text.trim().split(/\r?\n/);
  const head = lines[0].split(",");
  const out: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cells: string[] = [];
    let cur = "";
    let q = false;
    for (const ch of lines[i]) {
      if (ch === '"') q = !q;
      else if (ch === "," && !q) { cells.push(cur); cur = ""; }
      else cur += ch;
    }
    cells.push(cur);
    const row: Record<string, string> = {};
    head.forEach((h, j) => { row[h] = cells[j] ?? ""; });
    out.push(row);
  }
  return out;
}

const num = (v: string | undefined) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

async function loadTeamWeeks(): Promise<Map<string, TeamGame>> {
  const map = new Map<string, TeamGame>();
  for (const season of SEASONS) {
    const res = await fetch(NFLVERSE(season));
    if (!res.ok) throw new Error(`nflverse ${season}: HTTP ${res.status}`);
    const rows = parseCsv(await res.text()).filter((r) => r.season_type === "REG");
    for (const r of rows) {
      const team = TEAM_FIX[r.team] ?? r.team;
      const tg: TeamGame = {
        season,
        week: num(r.week),
        team,
        turnoversLost:
          num(r.passing_interceptions) + num(r.sack_fumbles_lost) +
          num(r.rushing_fumbles_lost) + num(r.receiving_fumbles_lost),
        epa: num(r.passing_epa) + num(r.rushing_epa),
        firstDowns: num(r.passing_first_downs) + num(r.rushing_first_downs),
        yards: num(r.passing_yards) + num(r.rushing_yards),
        nonOffensiveTds: num(r.def_tds) + num(r.special_teams_tds),
        sacksTaken: num(r.sacks_suffered),
        penalties: num(r.penalties),
        plays: num(r.attempts) + num(r.carries) + num(r.sacks_suffered),
      };
      map.set(`${season}-${tg.week}-${team}`, tg);
    }
  }
  return map;
}

type Matchup = {
  season: number;
  week: number;
  favTeam: string;
  dogTeam: string;
  pFav: number;
  dogWon: boolean;
  margin: number; // dog score - fav score
  fav: TeamGame;
  dog: TeamGame;
};

function mean(xs: number[]): number {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : NaN;
}
function corr(xs: number[], ys: number[]): number {
  const mx = mean(xs);
  const my = mean(ys);
  let sxy = 0, sx = 0, sy = 0;
  for (let i = 0; i < xs.length; i += 1) {
    sxy += (xs[i] - mx) * (ys[i] - my);
    sx += (xs[i] - mx) ** 2;
    sy += (ys[i] - my) ** 2;
  }
  return sxy / Math.sqrt(sx * sy);
}

async function main() {
  console.log("=".repeat(80));
  console.log("WHY DO UNDERDOGS WIN? — anatomy of NFL upsets, 2023-2025");
  console.log("=".repeat(80));

  const tw = await loadTeamWeeks();

  const rows = await sql`
    SELECT g.season, g.week, h.abbreviation home, a.abbreviation away,
           g.quoted_home_ml hml, g.quoted_away_ml aml,
           g.home_score hs, g.away_score as_
    FROM nfl_season_games g
    JOIN nfl_teams h ON h.team_id = g.home_team_id
    JOIN nfl_teams a ON a.team_id = g.away_team_id
    WHERE g.season = ANY(${sql.unsafe(`ARRAY[${SEASONS.join(",")}]`)})
      AND g.quoted_home_ml IS NOT NULL AND g.home_score IS NOT NULL
      AND g.home_score <> g.away_score
    ORDER BY g.season, g.week
  `;

  const games: Matchup[] = [];
  let missing = 0;
  const missingKeys: string[] = [];
  for (const r of rows) {
    const season = Number(r.season);
    const week = Number(r.week);
    const home = String(r.home);
    const away = String(r.away);
    const ih = Number(r.hml) < 0 ? -Number(r.hml) / (-Number(r.hml) + 100) : 100 / (Number(r.hml) + 100);
    const ia = Number(r.aml) < 0 ? -Number(r.aml) / (-Number(r.aml) + 100) : 100 / (Number(r.aml) + 100);
    const pHome = ih / (ih + ia);
    const homeIsFav = pHome >= 0.5;
    const favTeam = homeIsFav ? home : away;
    const dogTeam = homeIsFav ? away : home;
    const favTg = tw.get(`${season}-${week}-${favTeam}`);
    const dogTg = tw.get(`${season}-${week}-${dogTeam}`);
    if (!favTg || !dogTg) {
      missing += 1;
      if (missingKeys.length < 6) missingKeys.push(`${season} wk${week} ${away}@${home}`);
      continue;
    }
    const hs = Number(r.hs);
    const as_ = Number(r.as_);
    const homeWon = hs > as_;
    games.push({
      season, week, favTeam, dogTeam,
      pFav: Math.max(pHome, 1 - pHome),
      dogWon: homeIsFav ? !homeWon : homeWon,
      margin: homeIsFav ? as_ - hs : hs - as_,
      fav: favTg, dog: dogTg,
    });
  }

  console.log("\n0. JOIN INTEGRITY");
  console.log("-".repeat(80));
  console.log(`  closing-line games: ${rows.length}`);
  console.log(`  matched to nflverse team-week: ${games.length}`);
  console.log(`  unmatched: ${missing}${missingKeys.length ? "  e.g. " + missingKeys.join("; ") : ""}`);
  if (missing > 0) {
    console.error("\n  ABORTING: unmatched games mean a team-code mapping is wrong.");
    console.error("  Silently analysing the matched subset is how the AZ/ARI bug happened.");
    process.exit(1);
  }
  console.log("  100% matched — no team-code gap.");

  const upsets = games.filter((g) => g.dogWon);
  const chalk = games.filter((g) => !g.dogWon);

  console.log("\n\n1. HOW OFTEN, AND HOW BIG");
  console.log("-".repeat(80));
  console.log(`  Underdogs won ${upsets.length} of ${games.length} (${((upsets.length / games.length) * 100).toFixed(1)}%).`);
  console.log(`  Market said they would win ${(mean(games.map((g) => 1 - g.pFav)) * 100).toFixed(1)}%.`);
  console.log(`  Average winning margin when the dog wins: ${mean(upsets.map((g) => g.margin)).toFixed(1)} points.`);
  console.log(`  Average margin when the favourite wins:   ${(-mean(chalk.map((g) => g.margin))).toFixed(1)} points.`);

  console.log("\n\n2. WHAT SEPARATES AN UPSET FROM A NORMAL GAME");
  console.log("-".repeat(80));
  console.log("  Dog-minus-favourite differentials. Positive = the dog had more.\n");

  const metrics: Array<{ name: string; f: (g: Matchup) => number; unit: string }> = [
    { name: "Turnover margin (fav TOs - dog TOs)", f: (g) => g.fav.turnoversLost - g.dog.turnoversLost, unit: "" },
    { name: "EPA (dog - fav)", f: (g) => g.dog.epa - g.fav.epa, unit: "" },
    { name: "First downs (dog - fav)", f: (g) => g.dog.firstDowns - g.fav.firstDowns, unit: "" },
    { name: "Total yards (dog - fav)", f: (g) => g.dog.yards - g.fav.yards, unit: "" },
    { name: "Non-offensive TDs (dog - fav)", f: (g) => g.dog.nonOffensiveTds - g.fav.nonOffensiveTds, unit: "" },
    { name: "Sacks taken (dog - fav)", f: (g) => g.dog.sacksTaken - g.fav.sacksTaken, unit: "" },
    { name: "Penalties (dog - fav)", f: (g) => g.dog.penalties - g.fav.penalties, unit: "" },
  ];

  console.log("  metric".padEnd(42) + "upsets".padStart(9) + "chalk".padStart(9) + "  swing");
  const swings: Array<{ name: string; swing: number; sd: number }> = [];
  for (const m of metrics) {
    const u = mean(upsets.map(m.f));
    const c = mean(chalk.map(m.f));
    const allVals = games.map(m.f);
    const mu = mean(allVals);
    const sd = Math.sqrt(mean(allVals.map((v) => (v - mu) ** 2)));
    swings.push({ name: m.name, swing: (u - c) / sd, sd });
    console.log(
      `  ${m.name.padEnd(40)}${u.toFixed(2).padStart(9)}${c.toFixed(2).padStart(9)}` +
      `${((u - c) / sd).toFixed(2).padStart(9)} SD`,
    );
  }
  swings.sort((a, b) => Math.abs(b.swing) - Math.abs(a.swing));
  console.log(`\n  Biggest separator: ${swings[0].name} (${swings[0].swing.toFixed(2)} SD).`);

  console.log("\n\n3. DID THE DOG OUTPLAY THE FAVOURITE, OR JUST OUT-LUCK IT?");
  console.log("-".repeat(80));
  console.log("  Of the games underdogs WON:\n");
  const wonEpa = upsets.filter((g) => g.dog.epa > g.fav.epa).length;
  const wonTo = upsets.filter((g) => g.fav.turnoversLost > g.dog.turnoversLost).length;
  const tiedTo = upsets.filter((g) => g.fav.turnoversLost === g.dog.turnoversLost).length;
  const wonYards = upsets.filter((g) => g.dog.yards > g.fav.yards).length;
  const epaOnly = upsets.filter((g) => g.dog.epa > g.fav.epa && g.fav.turnoversLost <= g.dog.turnoversLost).length;
  const toOnly = upsets.filter((g) => g.dog.epa <= g.fav.epa && g.fav.turnoversLost > g.dog.turnoversLost).length;

  const pct = (x: number) => `${((x / upsets.length) * 100).toFixed(1)}%`;
  console.log(`    dog also won the EPA battle       ${String(wonEpa).padStart(4)}  ${pct(wonEpa)}`);
  console.log(`    dog also won the yardage battle   ${String(wonYards).padStart(4)}  ${pct(wonYards)}`);
  console.log(`    dog also won the turnover battle  ${String(wonTo).padStart(4)}  ${pct(wonTo)}`);
  console.log(`    turnovers were level              ${String(tiedTo).padStart(4)}  ${pct(tiedTo)}`);
  console.log("");
  console.log(`    outplayed but did NOT win turnovers ${String(epaOnly).padStart(3)}  ${pct(epaOnly)}   <- earned it`);
  console.log(`    won turnovers but was OUTPLAYED     ${String(toOnly).padStart(3)}  ${pct(toOnly)}   <- stole it`);

  console.log("\n  For comparison, when the FAVOURITE won it took the turnover battle " +
    `${((chalk.filter((g) => g.dog.turnoversLost > g.fav.turnoversLost).length / chalk.length) * 100).toFixed(1)}% of the time.`);

  console.log("\n\n  Upset rate by turnover margin (dog's turnover edge):");
  console.log("    dog TO edge     n    upset rate");
  for (const edge of [-3, -2, -1, 0, 1, 2, 3]) {
    const set = games.filter((g) => {
      const e = g.fav.turnoversLost - g.dog.turnoversLost;
      return edge === -3 ? e <= -3 : edge === 3 ? e >= 3 : e === edge;
    });
    if (set.length < 5) continue;
    const rate = set.filter((g) => g.dogWon).length / set.length;
    const label = edge === -3 ? "<= -3" : edge === 3 ? ">= +3" : (edge > 0 ? `+${edge}` : `${edge}`);
    console.log(
      `    ${label.padEnd(12)}${String(set.length).padStart(4)}` +
      `${(rate * 100).toFixed(1).padStart(11)}%   ${"#".repeat(Math.round(rate * 40))}`,
    );
  }

  console.log("\n\n4. IS ANY OF IT PREDICTABLE? (the load-bearing section)");
  console.log("-".repeat(80));
  console.log("  Split-half correlation within a team-season: weeks 1-9 vs 10-18.");
  console.log("  A stat that persists is a team property you could forecast.");
  console.log("  A stat near zero is something that happens TO a team, not");
  console.log("  something it does.\n");

  type Split = { name: string; f: (t: TeamGame) => number };
  const splits: Split[] = [
    { name: "EPA per play  (positive control)", f: (t) => (t.plays > 0 ? t.epa / t.plays : 0) },
    { name: "First downs per game", f: (t) => t.firstDowns },
    { name: "Yards per game", f: (t) => t.yards },
    { name: "Sacks taken per game", f: (t) => t.sacksTaken },
    { name: "Penalties per game", f: (t) => t.penalties },
    { name: "Turnovers lost per game", f: (t) => t.turnoversLost },
    { name: "Non-offensive TDs per game", f: (t) => t.nonOffensiveTds },
  ];

  const byTeamSeason = new Map<string, TeamGame[]>();
  for (const tg of tw.values()) {
    const k = `${tg.season}-${tg.team}`;
    if (!byTeamSeason.has(k)) byTeamSeason.set(k, []);
    byTeamSeason.get(k)!.push(tg);
  }

  console.log("  statistic".padEnd(40) + "n teams" + "   first-half vs second-half r");
  for (const s of splits) {
    const a: number[] = [];
    const b: number[] = [];
    for (const list of byTeamSeason.values()) {
      const first = list.filter((t) => t.week <= 9).map(s.f);
      const second = list.filter((t) => t.week >= 10).map(s.f);
      if (first.length < 4 || second.length < 4) continue;
      a.push(mean(first));
      b.push(mean(second));
    }
    const r = corr(a, b);
    const bar = "#".repeat(Math.max(0, Math.round(Math.abs(r) * 30)));
    console.log(`  ${s.name.padEnd(38)}${String(a.length).padStart(7)}${r.toFixed(3).padStart(12)}   ${bar}`);
  }

  console.log("\n  Read the control first. EPA per play persisting confirms the method");
  console.log("  works, so the smaller numbers below it are real and not artefacts.");

  // -------------------------------------------------------------------------
  console.log("\n\n5. THEN WHY CAN'T WE PREDICT UPSETS?");
  console.log("-".repeat(80));
  console.log("  Section 4 spoils the tidy answer. The main driver of upsets (EPA)");
  console.log("  is the MOST persistent stat measured, not the least -- so 'upsets");
  console.log("  are random noise' is not what this data says, and the honest");
  console.log("  explanation has to be something else.\n");
  console.log("  It is this: a team's weekly performance varies far more around its");
  console.log("  own average than teams vary from each other, and the part that is");
  console.log("  stable between teams is precisely what the closing line is built");
  console.log("  from. What is left to predict is the week-to-week wobble.\n");

  const teamMeans: number[] = [];
  let withinSum = 0;
  let withinN = 0;
  const allEpa: number[] = [];
  for (const list of byTeamSeason.values()) {
    const vals = list.filter((t) => t.plays > 0).map((t) => t.epa / t.plays);
    if (vals.length < 8) continue;
    const m = mean(vals);
    teamMeans.push(m);
    for (const v of vals) {
      withinSum += (v - m) ** 2;
      withinN += 1;
      allEpa.push(v);
    }
  }
  const grand = mean(allEpa);
  const totalVar = mean(allEpa.map((v) => (v - grand) ** 2));
  const withinVar = withinSum / withinN;
  const betweenVar = Math.max(0, totalVar - withinVar);

  console.log("  Weekly EPA per play, variance decomposition:");
  console.log(`    between teams (persistent, and already in the line) ${((betweenVar / totalVar) * 100).toFixed(1)}%`);
  console.log(`    within team, week to week (the unpredictable part)  ${((withinVar / totalVar) * 100).toFixed(1)}%`);
  console.log(
    `\n    A team's own weekly swing is ${Math.sqrt(withinVar / betweenVar).toFixed(1)}x ` +
    `the spread between teams.`,
  );

  console.log("\n  So the chain is:");
  console.log("    upsets happen because the dog outplays the favourite that day  (section 3)");
  console.log("    outplaying is mostly a real, persistent team quality           (section 4)");
  console.log("    which is exactly what the closing line already encodes         (studies 1-3)");
  console.log("    leaving only the week-to-week wobble, which dwarfs the signal  (above)");
  console.log("\n  That is a complete answer, and it is not 'we did not look hard enough'.");

  console.log("\n" + "=".repeat(80));
}

main().catch((e) => { console.error(e); process.exit(1); });
