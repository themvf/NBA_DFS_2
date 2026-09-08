/**
 * Is an archetype a leading indicator of WHERE THE LINE IS? — NFL 2020-2025.
 *
 * The question this answers, and the one it cannot.
 *
 * CANNOT: whether public money moves a line from open to close. That needs
 * OPENING lines, and they do not exist at scale for free — nflverse's
 * `initial_lines.csv` covers 2021 only (1,088 rows, one sportsbook), and this
 * repo's own `game_odds_history` holds 50 NFL games from August 2026 onward.
 * A movement study is a going-forward capture problem, not a backfill.
 *
 * CAN: whether the CLOSING line already contains the archetype, over and above
 * what team strength explains. That is the more decisive half of the question
 * anyway, because it separates two very different worlds:
 *
 *   coefficient ~ 0  the market ignores the situation. Then a room that reacts
 *                    to it is genuinely trading against the price, and the
 *                    archetype is a live source of disagreement.
 *   coefficient != 0 the market has already moved the number for it. Combined
 *                    with the calibration result — 16 of 17 archetypes show no
 *                    outcome gap — that means the market moved it by the RIGHT
 *                    amount, and anyone reacting further is simply late.
 *
 * Method: point-in-time Elo (updated game by game, regressed to the mean
 * between seasons, so no future information leaks) supplies team strength.
 * Then OLS of the closing spread on the Elo difference plus one differenced
 * dummy per archetype (home minus away), giving each archetype a coefficient
 * in POINTS OF SPREAD.
 *
 * THE CONFOUND, stated up front because it limits every conclusion below:
 * Elo is a crude strength proxy built only from scores. It cannot see injuries,
 * quarterback changes, or a coaching switch. The market can. So a non-zero
 * coefficient does NOT prove the market is reacting to the narrative — it may
 * be reacting to real information Elo lacks that happens to correlate with the
 * archetype. "Off a 17+ point loss" is the obvious case: some of those teams
 * lost badly because their quarterback got hurt, and the line knows.
 *
 * Run: npm run analyze:archetype-pricing
 */

const SCHEDULES =
  "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv";
const SEASON_MIN = 2020;
const SEASON_MAX = 2025;

const K = 20;
const HOME_ELO = 55;
/** Fraction of a team's rating carried into the next season. */
const CARRYOVER = 0.7;

const PACIFIC = new Set(["LA", "LAC", "SF", "SEA", "LV"]);
const MOUNTAIN = new Set(["ARI", "DEN"]);
const CENTRAL = new Set(["DAL", "HOU", "CHI", "GB", "MIN", "KC", "NO", "TEN"]);
function tz(t: string): number {
  if (PACIFIC.has(t)) return 3;
  if (MOUNTAIN.has(t)) return 2;
  if (CENTRAL.has(t)) return 1;
  return 0;
}
const NORTHERN = new Set([
  "GB", "CHI", "BUF", "NE", "CLE", "PIT", "DEN", "NYJ", "NYG", "PHI", "WAS", "BAL", "CIN", "KC",
]);

type Side = {
  team: string; opp: string; isHome: boolean; week: number;
  rest: number; oppRest: number; weekday: string; hourEt: number;
  neutralSite: boolean; div: boolean; roof: string; impliedWin: number;
  prevMargin?: number; prevWon?: boolean; prevHour?: number;
  prevWeekday?: string; prevNeutral?: boolean; prevOpp?: string; prevAway?: boolean;
};

type Arch = { code: string; label: string; test: (s: Side) => boolean };

const ARCHETYPES: Arch[] = [
  { code: "OFF_PRIMETIME_WIN", label: "Off a primetime win", test: (s) => s.prevWon === true && (s.prevHour ?? 0) >= 20 },
  { code: "OFF_BLOWOUT_WIN", label: "Off a 17+ point win", test: (s) => (s.prevMargin ?? 0) >= 17 },
  { code: "OFF_BLOWOUT_LOSS", label: "Off a 17+ point loss", test: (s) => (s.prevMargin ?? 0) <= -17 },
  { code: "OFF_BYE", label: "Off a bye", test: (s) => s.rest >= 13 },
  { code: "IN_PRIMETIME", label: "Primetime game", test: (s) => s.hourEt >= 20 },
  { code: "SHORT_WEEK", label: "Thursday short week", test: (s) => s.weekday === "Thursday" && s.rest <= 4 },
  { code: "DIVISIONAL", label: "Divisional game", test: (s) => s.div },
  { code: "ALTITUDE", label: "Visiting Denver", test: (s) => !s.isHome && s.opp === "DEN" },
  { code: "ALTITUDE_OFF", label: "Week after visiting Denver", test: (s) => s.prevOpp === "DEN" && s.prevAway === true },
  { code: "AT_INTERNATIONAL", label: "Neutral / international site", test: (s) => s.neutralSite },
  { code: "OFF_INTERNATIONAL", label: "After an international game", test: (s) => s.prevNeutral === true },
  { code: "WEST_TEAM_EARLY", label: "Pacific team at 1pm ET", test: (s) => tz(s.team) === 3 && !s.isHome && s.hourEt === 13 },
  { code: "CROSS_COUNTRY", label: "Travelling three time zones", test: (s) => !s.isHome && Math.abs(tz(s.team) - tz(s.opp)) === 3 },
  { code: "OFF_MONDAY", label: "Sunday after Monday night", test: (s) => s.prevWeekday === "Monday" && s.weekday === "Sunday" },
  { code: "REST_EDGE", label: "3+ days more rest", test: (s) => s.rest - s.oppRest >= 3 },
  { code: "COLD_OUTDOOR_LATE", label: "Outdoors, wk14+, northern", test: (s) => s.week >= 14 && s.roof === "outdoors" && NORTHERN.has(s.opp) },
];

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
const hourOf = (t: string) => { const m = /^(\d{1,2}):/.exec(t); return m ? Number(m[1]) : -1; };
const implied = (a: number) => (a < 0 ? -a / (-a + 100) : 100 / (a + 100));

// --- OLS with standard errors ------------------------------------------------
function invert(A: number[][]): number[][] | null {
  const n = A.length;
  const M = A.map((r, i) => [...r, ...Array.from({ length: n }, (_, j) => (i === j ? 1 : 0))]);
  for (let c = 0; c < n; c += 1) {
    let piv = c;
    for (let r = c + 1; r < n; r += 1) if (Math.abs(M[r][c]) > Math.abs(M[piv][c])) piv = r;
    if (Math.abs(M[piv][c]) < 1e-10) return null;
    [M[c], M[piv]] = [M[piv], M[c]];
    const d = M[c][c];
    for (let j = 0; j < 2 * n; j += 1) M[c][j] /= d;
    for (let r = 0; r < n; r += 1) {
      if (r === c) continue;
      const f = M[r][c];
      for (let j = 0; j < 2 * n; j += 1) M[r][j] -= f * M[c][j];
    }
  }
  return M.map((r) => r.slice(n));
}

function ols(X: number[][], y: number[]) {
  const n = X.length;
  const k = X[0].length;
  const XtX = Array.from({ length: k }, () => new Array<number>(k).fill(0));
  const Xty = new Array<number>(k).fill(0);
  for (let i = 0; i < n; i += 1) {
    for (let a = 0; a < k; a += 1) {
      Xty[a] += X[i][a] * y[i];
      for (let b = 0; b < k; b += 1) XtX[a][b] += X[i][a] * X[i][b];
    }
  }
  const inv = invert(XtX);
  if (!inv) return null;
  const beta = inv.map((row) => row.reduce((s, v, j) => s + v * Xty[j], 0));
  let sse = 0;
  for (let i = 0; i < n; i += 1) {
    let yh = 0;
    for (let a = 0; a < k; a += 1) yh += beta[a] * X[i][a];
    sse += (y[i] - yh) ** 2;
  }
  const sigma2 = sse / (n - k);
  const se = inv.map((row, j) => Math.sqrt(Math.max(0, sigma2 * row[j])));
  return { beta, se, n, rmse: Math.sqrt(sse / n) };
}

async function main() {
  const res = await fetch(SCHEDULES);
  if (!res.ok) throw new Error(`schedules: HTTP ${res.status}`);
  const raw = parseCsv(await res.text())
    .filter((r) => r.game_type === "REG")
    .filter((r) => Number(r.season) >= SEASON_MIN - 3 && Number(r.season) <= SEASON_MAX)
    .filter((r) => r.home_score !== "" && r.away_score !== "");
  raw.sort((a, b) => Number(a.season) - Number(b.season) || Number(a.week) - Number(b.week));

  // ---- point-in-time Elo, three seasons of burn-in before the study window --
  const elo = new Map<string, number>();
  const get = (t: string) => elo.get(t) ?? 1500;
  // Point-in-time scoring rates for the totals model. EWMA so early-season rows
  // are not dominated by one game, and updated only AFTER a game is priced, so
  // no future information enters the row it prices.
  const pf = new Map<string, number>();
  const pa = new Map<string, number>();
  const getPf = (t: string) => pf.get(t) ?? 22.5;
  const getPa = (t: string) => pa.get(t) ?? 22.5;
  const ALPHA = 0.2;
  let lastSeason = -1;

  type Game = {
    season: number; week: number; home: string; away: string;
    spread: number; eloDiff: number; home_: Side; away_: Side;
    total: number | null; expTotal: number;
  };
  const games: Game[] = [];
  const prevBy = new Map<string, Side>();

  for (const r of raw) {
    const season = Number(r.season);
    if (season !== lastSeason) {
      if (lastSeason >= 0) {
        for (const [t, v] of elo) elo.set(t, 1500 + (v - 1500) * CARRYOVER);
      }
      lastSeason = season;
      prevBy.clear();
    }
    const home = r.home_team;
    const away = r.away_team;
    const hs = Number(r.home_score);
    const as_ = Number(r.away_score);
    const hour = hourOf(r.gametime);
    const neutral = r.location !== "Home";
    const eloDiff = get(home) + (neutral ? 0 : HOME_ELO) - get(away);

    if (season >= SEASON_MIN && r.home_moneyline && r.away_moneyline && r.spread_line) {
      const ih = implied(Number(r.home_moneyline));
      const ia = implied(Number(r.away_moneyline));
      const mk = (team: string, opp: string, isHome: boolean): Side => {
        const p = prevBy.get(team);
        return {
          team, opp, isHome, week: Number(r.week),
          rest: Number(isHome ? r.home_rest : r.away_rest) || 7,
          oppRest: Number(isHome ? r.away_rest : r.home_rest) || 7,
          weekday: r.weekday, hourEt: hour, neutralSite: neutral,
          div: r.div_game === "1", roof: r.roof,
          impliedWin: isHome ? ih / (ih + ia) : ia / (ih + ia),
          prevMargin: p ? (p as Side & { _m?: number })._m : undefined,
          prevWon: p ? (p as Side & { _w?: boolean })._w : undefined,
          prevHour: p?.hourEt, prevWeekday: p?.weekday, prevNeutral: p?.neutralSite,
          prevOpp: p?.opp, prevAway: p ? !p.isHome : undefined,
        };
      };
      games.push({
        season, week: Number(r.week), home, away,
        spread: Number(r.spread_line), eloDiff,
        home_: mk(home, away, true), away_: mk(away, home, false),
        total: r.total_line ? Number(r.total_line) : null,
        expTotal: (getPf(home) + getPa(away)) / 2 + (getPf(away) + getPa(home)) / 2,
      });
    }

    // record for next week's "prev"
    const mkPrev = (team: string, opp: string, isHome: boolean, margin: number, won: boolean) => {
      const s = {
        team, opp, isHome, week: Number(r.week), rest: 7, oppRest: 7,
        weekday: r.weekday, hourEt: hour, neutralSite: neutral,
        div: r.div_game === "1", roof: r.roof, impliedWin: 0.5,
      } as Side & { _m: number; _w: boolean };
      s._m = margin; s._w = won;
      prevBy.set(team, s);
    };
    mkPrev(home, away, true, hs - as_, hs > as_);
    mkPrev(away, home, false, as_ - hs, as_ > hs);

    // Elo update on the actual result.
    const exp = 1 / (1 + Math.pow(10, -eloDiff / 400));
    const actual = hs > as_ ? 1 : hs < as_ ? 0 : 0.5;
    const mov = Math.log(Math.abs(hs - as_) + 1);
    const delta = K * mov * (actual - exp);
    elo.set(home, get(home) + delta);
    elo.set(away, get(away) - delta);
    pf.set(home, getPf(home) * (1 - ALPHA) + hs * ALPHA);
    pa.set(home, getPa(home) * (1 - ALPHA) + as_ * ALPHA);
    pf.set(away, getPf(away) * (1 - ALPHA) + as_ * ALPHA);
    pa.set(away, getPa(away) * (1 - ALPHA) + hs * ALPHA);
  }

  console.log("=".repeat(84));
  console.log("IS AN ARCHETYPE ALREADY IN THE LINE? — NFL 2020-2025");
  console.log(`${games.length} games. Closing spread regressed on point-in-time Elo + archetypes.`);
  console.log("=".repeat(84));

  // ---- build the design matrix ---------------------------------------------
  // spread_line is positive when HOME is favoured, so a POSITIVE coefficient
  // means the market gives the home side more points when it carries the tag.
  const X: number[][] = [];
  const y: number[] = [];
  for (const g of games) {
    const row = [1, g.eloDiff / 25];
    for (const a of ARCHETYPES) {
      row.push((a.test(g.home_) ? 1 : 0) - (a.test(g.away_) ? 1 : 0));
    }
    X.push(row);
    y.push(g.spread);
  }

  // A SYMMETRIC archetype tags both teams, so its home-minus-away column is
  // zero on every row and the design matrix is singular. That is not a data
  // problem: such an archetype CANNOT tilt a spread, because whatever it does
  // it does to both sides equally. It can still move a TOTAL, which the second
  // model below tests. Dropping them here is the correct treatment, not a
  // convenience.
  const live: number[] = [];
  const dropped: string[] = [];
  ARCHETYPES.forEach((a, i) => {
    const col = i + 2;
    const varies = X.some((row) => row[col] !== 0);
    if (varies) live.push(i);
    else dropped.push(a.label);
  });
  const Xs = X.map((row) => [row[0], row[1], ...live.map((i) => row[i + 2])]);

  const fit = ols(Xs, y);
  if (!fit) { console.error("singular design matrix even after dropping symmetric columns"); process.exit(1); }
  if (dropped.length > 0) {
    console.log(
      `
  Dropped from the SPREAD model (symmetric — tags both teams, so it cannot
` +
      `  tilt a spread by construction): ${dropped.join(", ")}.`,
    );
  }

  console.log(`\n  Elo-only baseline: intercept ${fit.beta[0].toFixed(2)} (home field, points)`);
  console.log(`  Elo slope ${fit.beta[1].toFixed(3)} points of spread per 25 Elo`);
  console.log(`  Residual RMSE ${fit.rmse.toFixed(2)} points\n`);

  console.log("  archetype".padEnd(34) + "coef (pts)".padStart(12) + "std err".padStart(9) + "     t" + "   rows" + "   read");
  // A coefficient is identified ONLY by rows where the differenced column is
  // non-zero. Thursday short week is the cautionary case: on almost every TNF
  // game BOTH teams are on short rest, so the differential is 0 and the
  // estimate rests on a handful of odd rows. Reporting a t-statistic without
  // that count invites reading a near-degenerate column as a real effect.
  const rows = live.map((archIdx, slot) => {
    const a = ARCHETYPES[archIdx];
    const b = fit.beta[slot + 2];
    const se = fit.se[slot + 2];
    const identifying = X.filter((row) => row[archIdx + 2] !== 0).length;
    return { a, b, se, t: se > 0 ? b / se : 0, identifying };
  }).sort((p, q) => Math.abs(q.t) - Math.abs(p.t));

  const MIN_IDENTIFYING = 40;
  for (const { a, b, se, t, identifying } of rows) {
    const sig = Math.abs(t) >= 1.96;
    const thin = identifying < MIN_IDENTIFYING;
    console.log(
      `  ${a.label.padEnd(32)}${b.toFixed(2).padStart(12)}${se.toFixed(2).padStart(9)}` +
      `${t.toFixed(1).padStart(6)}${String(identifying).padStart(7)}   ` +
      (thin ? "TOO FEW ROWS — ignore" : sig ? "IN THE LINE" : "not distinguishable from 0"),
    );
  }
  console.log(
    `
  "rows" counts games where the differenced column is non-zero — the only
` +
    `  games that identify the coefficient. Below ${MIN_IDENTIFYING} the estimate is not worth reading
` +
    `  however large its t-statistic looks.`,
  );

  const inLine = rows.filter((r) => Math.abs(r.t) >= 1.96 && r.identifying >= MIN_IDENTIFYING);
  console.log(
    `\n  ${inLine.length} of ${ARCHETYPES.length} archetypes move the closing spread by a ` +
    `distinguishable amount.`,
  );

  // -------------------------------------------------------------------------
  console.log("\n\nAND THE TOTAL — where symmetric archetypes CAN matter");
  console.log("-".repeat(84));
  console.log("  Divisional, primetime and neutral-site cannot tilt a spread, because they");
  console.log("  apply to both teams equally. They can still move the TOTAL, so this model");
  console.log("  counts tagged SIDES (0, 1 or 2) rather than differencing them.\n");

  const withTotals = games.filter((g) => g.total != null);
  const Xt: number[][] = [];
  const yt: number[] = [];
  for (const g of withTotals) {
    const row = [1, g.expTotal];
    for (const a of ARCHETYPES) {
      row.push((a.test(g.home_) ? 1 : 0) + (a.test(g.away_) ? 1 : 0));
    }
    Xt.push(row);
    yt.push(g.total as number);
  }
  const liveT: number[] = [];
  ARCHETYPES.forEach((_, i) => {
    if (Xt.some((row) => row[i + 2] !== 0)) liveT.push(i);
  });
  const Xts = Xt.map((row) => [row[0], row[1], ...liveT.map((i) => row[i + 2])]);
  const fitT = ols(Xts, yt);
  if (fitT) {
    console.log(
      `  n=${withTotals.length}. Scoring-rate slope ${fitT.beta[1].toFixed(3)}, ` +
      `residual RMSE ${fitT.rmse.toFixed(2)} points.\n`,
    );
    console.log(
      "  archetype".padEnd(34) + "coef (pts)".padStart(12) + "std err".padStart(9) +
      "     t" + "   rows" + "   read",
    );
    const rowsT = liveT
      .map((archIdx, slot) => {
        const a = ARCHETYPES[archIdx];
        const b = fitT.beta[slot + 2];
        const se = fitT.se[slot + 2];
        const identifying = Xt.filter((row) => row[archIdx + 2] !== 0).length;
        return { a, b, se, t: se > 0 ? b / se : 0, identifying };
      })
      .sort((p, q) => Math.abs(q.t) - Math.abs(p.t));
    for (const { a, b, se, t, identifying } of rowsT) {
      const thin = identifying < 40;
      console.log(
        `  ${a.label.padEnd(32)}${b.toFixed(2).padStart(12)}${se.toFixed(2).padStart(9)}` +
        `${t.toFixed(1).padStart(6)}${String(identifying).padStart(7)}   ` +
        (thin
          ? "TOO FEW ROWS — ignore"
          : Math.abs(t) >= 1.96
            ? "IN THE TOTAL"
            : "not distinguishable from 0"),
      );
    }
  }

  console.log("\n\nWHAT THIS MEANS, AND WHAT IT DOES NOT");
  console.log("-".repeat(84));
  console.log("  An archetype that IS in the line, combined with the earlier calibration");
  console.log("  result (16 of 17 archetypes show no outcome gap), means the market moved");
  console.log("  the number AND moved it by about the right amount. A room reacting to that");
  console.log("  story is not finding value the market missed — it is arriving late to a");
  console.log("  price that already reflects it.");
  console.log("");
  console.log("  An archetype that is NOT in the line is the more interesting case: the");
  console.log("  market is indifferent, so a room that reacts is genuinely trading against");
  console.log("  the price. That is where pick'em leverage lives.");
  console.log("");
  console.log("  THE CONFOUND. Elo sees only scores. It cannot see an injured quarterback,");
  console.log("  a coaching change, or a trade; the market can. So a non-zero coefficient");
  console.log("  does not prove the market is pricing the NARRATIVE — it may be pricing real");
  console.log("  information Elo lacks that correlates with the archetype. 'Off a 17+ point");
  console.log("  loss' is the clearest example: some of those teams lost badly BECAUSE their");
  console.log("  quarterback got hurt, and the line knows that while Elo does not.");
  console.log("");
  console.log("  NOT ANSWERED: whether public money moves a line from open to close. That");
  console.log("  needs opening lines, which do not exist at scale for free — nflverse's");
  console.log("  initial_lines.csv is 2021 only, and this repo's own NFL odds history holds");
  console.log("  50 games from August 2026. It is a going-forward capture problem.");
  console.log("\n" + "=".repeat(84));
}

main().catch((e) => { console.error(e); process.exit(1); });

export {};
