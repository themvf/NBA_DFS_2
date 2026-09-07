/**
 * Form divergence — is the dog heating up while the favourite cools?
 *
 * Motivated by this repo's own upset-anatomy result rather than by folklore:
 * EPA per play was the strongest separator of upsets from normal games AND the
 * most persistent statistic measured (split-half r = 0.593). Persistence is
 * what makes recent form a legitimate candidate — a stat that did not persist
 * could not carry a trend worth reading.
 *
 * PRE-REGISTRATION. Written and committed before 2025 was scored.
 *
 *   Discovery 2023+2024. Confirmation 2025. Point-in-time throughout: a game
 *   in week w uses only games played before week w, so nothing leaks backwards.
 *
 *   The feature. For each team entering a game:
 *     recent   = EPA/play over its last 3 games actually played
 *     baseline = EPA/play over every earlier game that season
 *     trend    = recent - baseline          (positive = heating up)
 *   divergence = dog.trend - fav.trend      (positive = dog up, fav down)
 *
 *   Requires >= 5 prior games for a stable baseline, so the sample starts
 *   around week 6 and both teams must qualify.
 *
 *   TWO STEPS, in this order, because they answer different questions:
 *
 *   STEP A (football, no market involved): does divergence predict the ON-FIELD
 *   EPA differential of the game about to be played? If it does not, the
 *   feature is noise and step B is pointless. This is the "does the mechanism
 *   exist" check that killed the totals idea before it cost a test.
 *
 *   STEP B (market): does divergence improve out-of-sample log loss over a
 *   market-anchored logistic that already knows the closing moneyline? Fit on
 *   2023-24, scored once on 2025, week-clustered bootstrap CI.
 *
 *   Kill criterion: step B CI includes zero => form divergence adds nothing to
 *   the price, and it is not a tiebreaker for choosing a dog.
 *
 *   Direction is NOT predicted. The user's hypothesis is that a heating dog is
 *   live. The behavioural literature says the public over-weights recent form,
 *   which if anything would make hot teams OVER-priced and point the other way.
 *   The test is two-sided and both outcomes are reported as found.
 *
 *   Honest prior: no edge. Books watch the same games, and recent form is the
 *   most visible thing about a team. But step A is genuinely uncertain and
 *   worth knowing on its own.
 *
 *   Cumulative testing: this is the fifth study on the same three seasons.
 *   Two more tests here. A survivor would need to be treated with more
 *   suspicion than its own CI implies, not less.
 *
 * Run: npm run analyze:form
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);

const SEASONS = [2023, 2024, 2025];
const NFLVERSE = (s: number) =>
  `https://github.com/nflverse/nflverse-data/releases/download/stats_team/stats_team_week_${s}.csv`;
const TEAM_FIX: Record<string, string> = { LA: "LAR", WAS: "WSH" };

/** Games of history required before a team can be used. */
const MIN_PRIOR_GAMES = 5;
/** Games in the "recent form" window. */
const FORM_WINDOW = 3;

type TeamGame = { season: number; week: number; team: string; epaPerPlay: number };

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
const mean = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : NaN);
function corr(xs: number[], ys: number[]): number {
  const mx = mean(xs); const my = mean(ys);
  let sxy = 0, sx = 0, sy = 0;
  for (let i = 0; i < xs.length; i += 1) {
    sxy += (xs[i] - mx) * (ys[i] - my);
    sx += (xs[i] - mx) ** 2; sy += (ys[i] - my) ** 2;
  }
  return sxy / Math.sqrt(sx * sy);
}
const implied = (a: number) => (a < 0 ? -a / (-a + 100) : 100 / (a + 100));
const logit = (p: number) => Math.log(Math.min(Math.max(p, 1e-6), 1 - 1e-6) / (1 - Math.min(Math.max(p, 1e-6), 1 - 1e-6)));
const sigmoid = (x: number) => 1 / (1 + Math.exp(-x));

function fitLogistic(X: number[][], y: number[], ridge = 1e-4): number[] {
  const k = X[0].length;
  const w = new Array<number>(k).fill(0);
  for (let it = 0; it < 60; it += 1) {
    const g = new Array<number>(k).fill(0);
    const H = Array.from({ length: k }, () => new Array<number>(k).fill(0));
    for (let i = 0; i < X.length; i += 1) {
      let z = 0;
      for (let j = 0; j < k; j += 1) z += w[j] * X[i][j];
      const p = sigmoid(z); const r = y[i] - p; const s = Math.max(p * (1 - p), 1e-9);
      for (let j = 0; j < k; j += 1) {
        g[j] += r * X[i][j];
        for (let l = 0; l < k; l += 1) H[j][l] += s * X[i][j] * X[i][l];
      }
    }
    for (let j = 0; j < k; j += 1) { g[j] -= ridge * w[j]; H[j][j] += ridge; }
    const step = solve(H, g);
    if (!step) break;
    let md = 0;
    for (let j = 0; j < k; j += 1) { w[j] += step[j]; md = Math.max(md, Math.abs(step[j])); }
    if (md < 1e-10) break;
  }
  return w;
}
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
function bootstrapMean(vals: number[], keys: string[], iters = 6000, seed = 31337) {
  const by = new Map<string, number[]>();
  keys.forEach((k, i) => { if (!by.has(k)) by.set(k, []); by.get(k)!.push(vals[i]); });
  const cl = [...by.values()];
  let s = seed >>> 0;
  const rnd = () => { s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0; return s / 4294967296; };
  const draws: number[] = [];
  for (let i = 0; i < iters; i += 1) {
    let sum = 0, n = 0;
    for (let c = 0; c < cl.length; c += 1) { const g = cl[Math.floor(rnd() * cl.length)]; for (const v of g) { sum += v; n += 1; } }
    draws.push(sum / n);
  }
  draws.sort((a, b) => a - b);
  return { point: mean(vals), lo: draws[Math.floor(draws.length * 0.025)], hi: draws[Math.floor(draws.length * 0.975)] };
}

type Row = {
  season: number; week: number;
  pFavHome: number; // market prob for the HOME side
  homeWon: boolean;
  divergence: number; // dog.trend - fav.trend
  epaDiff: number; // dog EPA/play - fav EPA/play, this game
  dogIsHome: boolean;
};

async function main() {
  console.log("=".repeat(80));
  console.log("FORM DIVERGENCE — dog heating up while the favourite cools?");
  console.log("=".repeat(80));

  // ---- load team-week EPA -------------------------------------------------
  const tw: TeamGame[] = [];
  for (const season of SEASONS) {
    const res = await fetch(NFLVERSE(season));
    if (!res.ok) throw new Error(`nflverse ${season}: HTTP ${res.status}`);
    for (const r of parseCsv(await res.text()).filter((x) => x.season_type === "REG")) {
      const plays = num(r.attempts) + num(r.carries) + num(r.sacks_suffered);
      if (plays <= 0) continue;
      tw.push({
        season,
        week: num(r.week),
        team: TEAM_FIX[r.team] ?? r.team,
        epaPerPlay: (num(r.passing_epa) + num(r.rushing_epa)) / plays,
      });
    }
  }
  const hist = new Map<string, TeamGame[]>();
  for (const t of tw) {
    const k = `${t.season}-${t.team}`;
    if (!hist.has(k)) hist.set(k, []);
    hist.get(k)!.push(t);
  }
  for (const list of hist.values()) list.sort((a, b) => a.week - b.week);
  const byKey = new Map(tw.map((t) => [`${t.season}-${t.week}-${t.team}`, t]));

  /** Point-in-time trend for a team entering `week`. Null if too little history. */
  function trend(season: number, team: string, week: number): number | null {
    const list = hist.get(`${season}-${team}`);
    if (!list) return null;
    const prior = list.filter((t) => t.week < week);
    if (prior.length < MIN_PRIOR_GAMES) return null;
    const recent = prior.slice(-FORM_WINDOW).map((t) => t.epaPerPlay);
    const baseline = prior.map((t) => t.epaPerPlay);
    return mean(recent) - mean(baseline);
  }

  const games = await sql`
    SELECT g.season, g.week, h.abbreviation home, a.abbreviation away,
           g.quoted_home_ml hml, g.quoted_away_ml aml, g.home_score hs, g.away_score as_
    FROM nfl_season_games g
    JOIN nfl_teams h ON h.team_id = g.home_team_id
    JOIN nfl_teams a ON a.team_id = g.away_team_id
    WHERE g.season = ANY(${sql.unsafe(`ARRAY[${SEASONS.join(",")}]`)})
      AND g.quoted_home_ml IS NOT NULL AND g.home_score IS NOT NULL
      AND g.home_score <> g.away_score
    ORDER BY g.season, g.week
  `;

  const rows: Row[] = [];
  let skipped = 0;
  for (const r of games) {
    const season = Number(r.season); const week = Number(r.week);
    const home = String(r.home); const away = String(r.away);
    const ih = implied(Number(r.hml)); const ia = implied(Number(r.aml));
    const pHome = ih / (ih + ia);
    const homeIsFav = pHome >= 0.5;
    const fav = homeIsFav ? home : away;
    const dog = homeIsFav ? away : home;
    const tFav = trend(season, fav, week);
    const tDog = trend(season, dog, week);
    const gFav = byKey.get(`${season}-${week}-${fav}`);
    const gDog = byKey.get(`${season}-${week}-${dog}`);
    if (tFav === null || tDog === null || !gFav || !gDog) { skipped += 1; continue; }
    rows.push({
      season, week, pFavHome: pHome,
      homeWon: Number(r.hs) > Number(r.as_),
      divergence: tDog - tFav,
      epaDiff: gDog.epaPerPlay - gFav.epaPerPlay,
      dogIsHome: !homeIsFav,
    });
  }

  console.log(`\n  usable games: ${rows.length}   (skipped ${skipped}, mostly early weeks with < ${MIN_PRIOR_GAMES} prior games)`);
  const disc = rows.filter((r) => r.season !== 2025);
  const conf = rows.filter((r) => r.season === 2025);
  console.log(`  discovery 2023-24: ${disc.length}   confirmation 2025: ${conf.length}`);

  // -------------------------------------------------------------------------
  console.log("\n\nSTEP A — DOES FORM DIVERGENCE PREDICT ON-FIELD PLAY AT ALL?");
  console.log("-".repeat(80));
  console.log("  No market involved. Pure football: does a dog trending up while");
  console.log("  the favourite trends down actually out-EPA them on the day?\n");

  for (const [label, set] of [["2023-24", disc], ["2025", conf], ["all", rows]] as const) {
    const r = corr(set.map((x) => x.divergence), set.map((x) => x.epaDiff));
    console.log(`  ${label.padEnd(10)} n=${String(set.length).padStart(4)}   corr(divergence, game EPA diff) = ${r.toFixed(4)}`);
  }

  console.log("\n  Upset rate and on-field EPA by divergence quintile (2025):");
  const sorted2025 = [...conf].sort((a, b) => a.divergence - b.divergence);
  const q = Math.floor(sorted2025.length / 5);
  console.log("    quintile          n   mean divergence   dog EPA edge   upset rate");
  for (let i = 0; i < 5; i += 1) {
    const slice = sorted2025.slice(i * q, i === 4 ? sorted2025.length : (i + 1) * q);
    if (slice.length === 0) continue;
    const ups = slice.filter((x) => (x.dogIsHome ? x.homeWon : !x.homeWon)).length;
    console.log(
      `    ${(i === 0 ? "1 (dog cold)" : i === 4 ? "5 (dog hot)" : `${i + 1}`).padEnd(15)}` +
      `${String(slice.length).padStart(4)}` +
      `${mean(slice.map((x) => x.divergence)).toFixed(4).padStart(18)}` +
      `${mean(slice.map((x) => x.epaDiff)).toFixed(4).padStart(15)}` +
      `${((ups / slice.length) * 100).toFixed(1).padStart(13)}%`,
    );
  }

  // -------------------------------------------------------------------------
  console.log("\n\nSTEP B — DOES IT BEAT THE CLOSING LINE?");
  console.log("-".repeat(80));
  console.log("  Market-anchored logistic fit on 2023-24, scored once on 2025.");
  console.log("  Target: home team wins.\n");

  // Divergence is expressed dog-minus-fav; convert to a home-referenced feature
  // so it can sit next to a home-referenced market probability without a
  // sign bug. A dog-favourable divergence helps HOME only when home is the dog.
  const homeDivergence = (r: Row) => (r.dogIsHome ? r.divergence : -r.divergence);

  const sets: Array<{ name: string; build: (r: Row) => number[] }> = [
    { name: "market only (baseline)", build: (r) => [1, logit(r.pFavHome)] },
    { name: "market + form divergence", build: (r) => [1, logit(r.pFavHome), homeDivergence(r) * 10] },
  ];
  const yD = disc.map((r) => (r.homeWon ? 1 : 0));
  const yC = conf.map((r) => (r.homeWon ? 1 : 0));
  const keys = conf.map((r) => `${r.season}-${r.week}`);
  const out: Record<string, number[]> = {};
  for (const s of sets) {
    const w = fitLogistic(disc.map(s.build), yD);
    out[s.name] = logLoss(w, conf.map(s.build), yC);
    console.log(
      `  ${s.name.padEnd(30)} 2025 log loss ${mean(out[s.name]).toFixed(5)}   ` +
      `coefs [${w.map((v) => v.toFixed(3)).join(", ")}]`,
    );
  }
  const diffs = out["market + form divergence"].map((v, i) => v - out["market only (baseline)"][i]);
  const b = bootstrapMean(diffs, keys);
  console.log(
    `\n  delta ${b.point >= 0 ? "+" : ""}${b.point.toFixed(5)}  95% CI [${b.lo.toFixed(5)}, ${b.hi.toFixed(5)}]  ` +
    `${b.hi < 0 ? "HELPS" : b.lo > 0 ? "HURTS" : "no effect (CI includes zero)"}`,
  );
  console.log("  Negative = lower log loss than the market alone = the feature helped.");

  // -------------------------------------------------------------------------
  console.log("\n\nWHAT THIS MEANS FOR PICKING A DOG");
  console.log("-".repeat(80));
  const rAll = corr(rows.map((x) => x.divergence), rows.map((x) => x.epaDiff));
  console.log(`  Step A correlation (all seasons): ${rAll.toFixed(4)}`);
  console.log(`  Step B on 2025: ${b.hi < 0 ? "beats" : b.lo > 0 ? "loses to" : "does not beat"} the closing line.`);
  console.log("");
  if (Math.abs(rAll) < 0.05) {
    console.log("  Step A is near zero, so the trend does not even predict the game");
    console.log("  on the field, never mind the price. There is nothing here to");
    console.log("  narrow a selection with.");
  } else {
    console.log("  Form divergence does carry real information about how the game");
    console.log("  will be played. Whether the market has already used it is what");
    console.log("  step B answers -- and a feature that fails step B is one the");
    console.log("  price already contains, not one that does not exist.");
  }
  console.log("\n" + "=".repeat(80));
}

main().catch((e) => { console.error(e); process.exit(1); });
