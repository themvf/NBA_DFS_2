/**
 * West-coast 1pm line-movement study — analysis.
 *
 * Design frozen in docs/nfl-line-movement-study.md BEFORE any snapshot was
 * purchased. H1 drift, H2 timing, H3 the-opener-was-right, each with a kill
 * criterion written in advance. Nothing here may be re-sliced after the fact.
 *
 * Sign convention, applied everywhere: every spread is expressed FROM THE ROAD
 * TEAM'S PERSPECTIVE, so a NEGATIVE move means the market turned against the
 * visitor — which is the direction the hypothesis predicts for west-coast
 * teams. nflverse `spread_line` and the Odds API both quote the HOME side, so
 * the flip happens once, here, rather than being re-derived per section.
 *
 * Run: npm run analyze:line-movement
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);

/** Odds API full names -> nflverse abbreviations, for the schedule join. */
const TEAM: Record<string, string> = {
  "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
  "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
  "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
  "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
  "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
  "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
  "Los Angeles Rams": "LA", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
  "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
  "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
  "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
  "Tennessee Titans": "TEN", "Washington Commanders": "WAS", "Washington Football Team": "WAS",
};

const PACIFIC = new Set(["SF", "SEA", "LA", "LAC", "LV"]);

type Snap = { label: string; spread: number | null; awayMl: number | null; homeMl: number | null; lead: number };
type Game = {
  season: number; week: number; away: string; home: string;
  /** Road-team spread at each label, positive = road team getting points. */
  s: Record<string, number | null>;
  /** Road-team de-vigged win probability at each label. */
  p: Record<string, number | null>;
  actualRoadMargin: number;
  treatment: boolean;
};

const implied = (a: number) => (a < 0 ? -a / (-a + 100) : 100 / (a + 100));

function bootstrapMean(xs: number[], iters = 8000, seed = 60606) {
  if (xs.length === 0) return null;
  let s = seed >>> 0;
  const rnd = () => { s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0; return s / 4294967296; };
  const draws: number[] = [];
  for (let i = 0; i < iters; i += 1) {
    let acc = 0;
    for (let j = 0; j < xs.length; j += 1) acc += xs[Math.floor(rnd() * xs.length)];
    draws.push(acc / xs.length);
  }
  draws.sort((a, b) => a - b);
  const mean = xs.reduce((a, b) => a + b, 0) / xs.length;
  return { mean, lo: draws[Math.floor(draws.length * 0.025)], hi: draws[Math.floor(draws.length * 0.975)], n: xs.length };
}

/** Difference in means with a bootstrap CI on the difference itself. */
function bootstrapDiff(a: number[], b: number[], iters = 8000, seed = 71717) {
  if (a.length === 0 || b.length === 0) return null;
  let s = seed >>> 0;
  const rnd = () => { s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0; return s / 4294967296; };
  const draws: number[] = [];
  for (let i = 0; i < iters; i += 1) {
    let sa = 0; let sb = 0;
    for (let j = 0; j < a.length; j += 1) sa += a[Math.floor(rnd() * a.length)];
    for (let j = 0; j < b.length; j += 1) sb += b[Math.floor(rnd() * b.length)];
    draws.push(sa / a.length - sb / b.length);
  }
  draws.sort((x, y) => x - y);
  const mean = a.reduce((x, y) => x + y, 0) / a.length - b.reduce((x, y) => x + y, 0) / b.length;
  return { mean, lo: draws[Math.floor(draws.length * 0.025)], hi: draws[Math.floor(draws.length * 0.975)], nA: a.length, nB: b.length };
}

const fmt = (x: number, d = 2) => `${x >= 0 ? "+" : ""}${x.toFixed(d)}`;

async function main() {
  // ---- schedule + results, from nflverse -----------------------------------
  const res = await fetch(
    "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv",
  );
  const lines = (await res.text()).trim().split(/\r?\n/);
  const head = lines[0].split(",");
  const idx = (k: string) => head.indexOf(k);
  type Sched = { season: number; week: number; away: string; home: string; roadMargin: number; hour: number; weekday: string };
  const sched = new Map<string, Sched>();
  for (let i = 1; i < lines.length; i += 1) {
    const c = lines[i].split(",");
    const season = Number(c[idx("season")]);
    if (season < 2022 || season > 2025 || c[idx("game_type")] !== "REG") continue;
    if (c[idx("home_score")] === "" || c[idx("away_score")] === "") continue;
    const away = c[idx("away_team")];
    const home = c[idx("home_team")];
    const hour = Number((c[idx("gametime")] || "").split(":")[0]);
    sched.set(`${season}|${away}|${home}`, {
      season, week: Number(c[idx("week")]), away, home,
      roadMargin: Number(c[idx("away_score")]) - Number(c[idx("home_score")]),
      hour, weekday: c[idx("weekday")],
    });
  }

  // ---- snapshots -----------------------------------------------------------
  const rows = await sql`
    SELECT season, label, event_id, home_team, away_team,
           home_spread, home_ml, away_ml, lead_minutes,
           commence_time::text AS commence
    FROM nfl_line_snapshots ORDER BY event_id, snapshot_at`;

  // Keyed on the MATCHUP, not on event_id.
  //
  // The provider re-keys some events -- 1,309 event_ids cover 1,093 distinct
  // season+matchup pairs, and one flexed Week 18 game appears under two ids at
  // two different kickoff times. Keying on event_id counted those as separate
  // games, which both inflated n (treatment read 89 against a schedule count of
  // 73) and broke independence by entering the same game twice. Merging the
  // traces is also the more correct reconstruction: the line is the line
  // regardless of which id the provider filed it under.
  const byEvent = new Map<string, { home: string; away: string; season: number; snaps: Snap[] }>();
  for (const r of rows) {
    const home = TEAM[String(r.home_team)];
    const away = TEAM[String(r.away_team)];
    if (!home || !away) continue;
    const k = `${r.season}|${away}|${home}`;
    if (!byEvent.has(k)) byEvent.set(k, { home, away, season: Number(r.season), snaps: [] });
    byEvent.get(k)!.snaps.push({
      label: String(r.label),
      spread: r.home_spread == null ? null : Number(r.home_spread),
      homeMl: r.home_ml == null ? null : Number(r.home_ml),
      awayMl: r.away_ml == null ? null : Number(r.away_ml),
      lead: Number(r.lead_minutes),
    });
  }

  const LABELS = ["open", "friday", "sun_am", "sun_close"] as const;
  const games: Game[] = [];
  let unmatched = 0;
  for (const [, ev] of byEvent) {
    const sg = sched.get(`${ev.season}|${ev.away}|${ev.home}`);
    if (!sg) { unmatched += 1; continue; }
    // The frozen population: Sunday 1pm ET games only.
    if (sg.weekday !== "Sunday" || sg.hour !== 13) continue;

    const s: Record<string, number | null> = {};
    const p: Record<string, number | null> = {};
    for (const L of LABELS) {
      // The last snapshot carrying that label BEFORE kickoff.
      // Merged traces are not guaranteed ordered, so pick explicitly: the
      // snapshot with the SMALLEST positive lead is the last one before kickoff.
      const cands = ev.snaps
        .filter((x) => x.label === L && x.lead > 0 && x.spread != null)
        .sort((a, b) => a.lead - b.lead);
      const pick = cands.length ? cands[0] : null;
      // Home spread flipped to the ROAD team's perspective.
      s[L] = pick?.spread == null ? null : -pick.spread;
      if (pick?.homeMl != null && pick?.awayMl != null) {
        const ih = implied(pick.homeMl);
        const ia = implied(pick.awayMl);
        p[L] = ia / (ih + ia);
      } else p[L] = null;
    }
    games.push({
      season: ev.season, week: sg.week, away: ev.away, home: ev.home,
      s, p, actualRoadMargin: sg.roadMargin, treatment: PACIFIC.has(ev.away),
    });
  }

  const complete = games.filter((g) => g.s.open != null && g.s.sun_close != null);
  const treat = complete.filter((g) => g.treatment);
  const ctrl = complete.filter((g) => !g.treatment);

  console.log("=".repeat(84));
  console.log("WEST-COAST 1PM LINE MOVEMENT — docs/nfl-line-movement-study.md");
  console.log("=".repeat(84));
  console.log(`  Sunday-1pm games with open AND close: ${complete.length}`);
  console.log(`  treatment (Pacific visitor): ${treat.length}   control: ${ctrl.length}`);
  if (unmatched) console.log(`  unmatched to schedule: ${unmatched}`);
  const bySeason: Record<number, string> = {};
  for (const s of [2022, 2023, 2024, 2025]) {
    const t = treat.filter((g) => g.season === s).length;
    const c = ctrl.filter((g) => g.season === s).length;
    bySeason[s] = `${t}T/${c}C`;
  }
  console.log(`  by season: ${JSON.stringify(bySeason)}`);
  console.log("\n  Sign convention: spreads are from the ROAD team's view, so a NEGATIVE");
  console.log("  move means the market turned AGAINST the visitor.");

  // -------------------------------------------------------------------------
  const primary = (g: Game) => g.season >= 2023;
  const report = (title: string, pool: Game[]) => {
    const T = pool.filter((g) => g.treatment);
    const C = pool.filter((g) => !g.treatment);
    if (T.length === 0) { console.log(`\n  ${title}: no treatment games`); return; }

    console.log(`\n${"-".repeat(84)}\n  ${title}  (treatment ${T.length}, control ${C.length})`);

    // ---- H1: total drift ---------------------------------------------------
    const driftT = T.map((g) => (g.s.sun_close as number) - (g.s.open as number));
    const driftC = C.map((g) => (g.s.sun_close as number) - (g.s.open as number));
    const mt = bootstrapMean(driftT)!;
    const mc = bootstrapMean(driftC)!;
    const d = bootstrapDiff(driftT, driftC)!;
    console.log("\n  H1  spread drift, open -> close (points, road perspective)");
    console.log(`      treatment ${fmt(mt.mean)}  [${fmt(mt.lo)}, ${fmt(mt.hi)}]`);
    console.log(`      control   ${fmt(mc.mean)}  [${fmt(mc.lo)}, ${fmt(mc.hi)}]`);
    console.log(
      `      diff      ${fmt(d.mean)}  [${fmt(d.lo)}, ${fmt(d.hi)}]  ` +
      `${d.lo > 0 || d.hi < 0 ? "EXCLUDES ZERO" : "includes zero — H1 dies"}`,
    );

    // ---- H2: which interval ------------------------------------------------
    console.log("\n  H2  where the drift happens (treatment minus control, points)");
    const intervals: Array<[string, string, string]> = [
      ["open -> friday      (article cycle)", "open", "friday"],
      ["friday -> sun_am    (weekend casual)", "friday", "sun_am"],
      ["sun_am -> sun_close (late / sharp)", "sun_am", "sun_close"],
    ];
    for (const [name, a, b] of intervals) {
      const ta = T.filter((g) => g.s[a] != null && g.s[b] != null)
        .map((g) => (g.s[b] as number) - (g.s[a] as number));
      const ca = C.filter((g) => g.s[a] != null && g.s[b] != null)
        .map((g) => (g.s[b] as number) - (g.s[a] as number));
      const dd = bootstrapDiff(ta, ca);
      if (!dd) { console.log(`      ${name.padEnd(38)} no data`); continue; }
      console.log(
        `      ${name.padEnd(38)} ${fmt(dd.mean).padStart(7)}  [${fmt(dd.lo)}, ${fmt(dd.hi)}]` +
        `  n=${dd.nA}/${dd.nB}${dd.lo > 0 || dd.hi < 0 ? "  <-- excludes zero" : ""}`,
      );
    }

    // ---- H3: was the opener better? ---------------------------------------
    console.log("\n  H3  |actual - open| minus |actual - close|  (positive = CLOSE was better)");
    for (const [name, pool2] of [["treatment", T], ["control", C]] as const) {
      const errs = pool2.map((g) =>
        Math.abs(g.actualRoadMargin - (g.s.open as number)) -
        Math.abs(g.actualRoadMargin - (g.s.sun_close as number)));
      const m = bootstrapMean(errs)!;
      const verdict = m.hi < 0
        ? "OPENER BETTER"
        : m.lo > 0
          ? "closer better"
          : "no difference";
      console.log(`      ${name.padEnd(10)} ${fmt(m.mean)}  [${fmt(m.lo)}, ${fmt(m.hi)}]  n=${m.n}  ${verdict}`);
    }
    const eT = T.map((g) => Math.abs(g.actualRoadMargin - (g.s.open as number)) - Math.abs(g.actualRoadMargin - (g.s.sun_close as number)));
    const eC = C.map((g) => Math.abs(g.actualRoadMargin - (g.s.open as number)) - Math.abs(g.actualRoadMargin - (g.s.sun_close as number)));
    const de = bootstrapDiff(eT, eC)!;
    console.log(
      `      diff       ${fmt(de.mean)}  [${fmt(de.lo)}, ${fmt(de.hi)}]  ` +
      `${de.lo > 0 || de.hi < 0 ? "EXCLUDES ZERO" : "includes zero — H3 dies"}`,
    );
  };

  report("PRIMARY — 2023-2025 (full ~12-day listing window)", complete.filter(primary));
  report("FOOTNOTE — 2022 only (~6-day window, NOT pooled)", complete.filter((g) => !primary(g)));

  console.log(`\n${"=".repeat(84)}`);
  console.log("  Reminder from the pre-registration: this can show the line MOVED and");
  console.log("  whether the opener was better. It cannot show the PUBLIC moved it —");
  console.log("  bet-percentage data is not available, and sharp money and real news");
  console.log("  move markets too.");
  console.log("=".repeat(84));
}

main().catch((e) => { console.error(e); process.exit(1); });
