"use server";

/**
 * Server actions for the NBA DFS optimizer page.
 *
 * processDkSlate  — parse DK CSV + LineStar CSV, compute projections, save to DB
 * runOptimizer    — run ILP optimizer with given settings, return lineups
 * saveLineups     — persist generated lineups to dk_lineups
 * exportLineups   — build multi-entry upload CSV string
 */

import { revalidatePath } from "next/cache";
import { db } from "@/db";
import { teams, nbaTeamStats, nbaPlayerStats, nbaMatchups, dkSlates, dkPlayers, dkLineups } from "@/db/schema";
import { eq, sql, and, desc, inArray } from "drizzle-orm";
import { optimizeLineups, buildMultiEntryCSV } from "./optimizer";
import type { OptimizerPlayer, OptimizerSettings, GeneratedLineup } from "./optimizer";
import { optimizeMlbLineups, buildMlbMultiEntryCSV } from "./mlb-optimizer";
import type { MlbOptimizerPlayer, MlbOptimizerSettings, MlbGeneratedLineup } from "./mlb-optimizer";

/** Minimal lineup shape accepted by saveLineups — satisfied by both NBA and MLB lineup types. */
type LineupForSave = {
  players: Array<{ id: number; teamAbbrev: string }>;
  totalSalary: number;
  projFpts: number;
  leverageScore: number;
};

const LEAGUE_AVG_PACE       = 100.0;
const LEAGUE_AVG_DEF_RTG   = 112.0;
const LEAGUE_AVG_TOTAL      = 228.0;
const LEAGUE_AVG_TEAM_TOTAL = 114.0;
const LEAGUE_AVG_USAGE      = 20.0;
const CURRENT_SEASON        = "2025-26";

// ── NBA abbreviation overrides (DK → standard) ──────────────
const DK_OVERRIDES: Record<string, string> = {
  GS: "GSW", SA: "SAS", NO: "NOP", NY: "NYK",
  PHO: "PHX", OKL: "OKC", UTH: "UTA",
};

// ── CSV Parsers ───────────────────────────────────────────────

function parseDkCsv(content: string): DkApiPlayer[] {
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];

  const header = lines[0].split(",").map((h) => h.trim().replace(/^"|"$/g, ""));
  const col = (name: string) => header.findIndex((h) => h === name);

  const nameCol      = col("Name");
  const idCol        = col("ID");
  const salaryCol    = col("Salary");
  const rosterPosCol = col("Roster Position");
  const teamCol      = col("TeamAbbrev");
  const gameInfoCol  = col("Game Info");
  const avgCol       = col("AvgPointsPerGame");

  const players = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
    const name   = cells[nameCol] ?? "";
    const idStr  = cells[idCol] ?? "";
    if (!name || !idStr) continue;
    const salaryStr = (cells[salaryCol] ?? "0").replace(/[^0-9]/g, "");
    players.push({
      name,
      dkId:              parseInt(idStr, 10),
      teamAbbrev:        (cells[teamCol] ?? "").toUpperCase(),
      eligiblePositions: cells[rosterPosCol] ?? "UTIL",
      salary:            parseInt(salaryStr, 10) || 0,
      gameInfo:          cells[gameInfoCol] ?? "",
      avgFptsDk:         parseFloat(cells[avgCol] ?? "") || null,
      // CSV doesn't carry DK injury status — rely on LineStar for is_out
      dkStatus:    "None",
      isDisabled:  false,
    });
  }
  return players;
}

type LinestarEntry = { linestarProj: number; projOwnPct: number; isOut: boolean };

function parseLinestarCsv(content: string): Map<string, LinestarEntry> {
  const lines = content.split(/\r?\n/).filter(Boolean);
  const map = new Map<string, LinestarEntry>();
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(",").map((c) => c.trim());
    if (cells.length < 8) continue;
    const playerName = cells[2] ?? "";
    const salaryStr  = (cells[3] ?? "").replace(/[^0-9]/g, "");
    const projOwnStr = (cells[4] ?? "").replace("%", "");
    const projStr    = cells[7] ?? "";
    if (!playerName) continue;
    const proj    = parseFloat(projStr)    || 0;
    const projOwn = parseFloat(projOwnStr) || 0;
    if (proj === 0 && projOwn === 0) continue;
    const salary = parseInt(salaryStr, 10) || 0;
    const isOut  = proj === 0;
    map.set(`${playerName.toLowerCase()}|${salary}`, { linestarProj: proj, projOwnPct: projOwn, isOut });
  }
  return map;
}

/** Parse tab-separated data pasted directly from the LineStar web table.
 *  Columns: Pos, Team, Player, Salary, projOwn%, actualOwn%, Diff, Proj
 *  Uses the Salary cell ($NNNNN) as an anchor — position-independent. */
function parseLinestarPasteText(text: string): Map<string, LinestarEntry> {
  const lines = text.split(/\r?\n/).filter(Boolean);
  const map = new Map<string, LinestarEntry>();
  for (const line of lines) {
    const cells = line.split("\t").map((c) => c.trim());
    // Anchor on the salary cell: "$" followed by 4-5 digits
    const salaryIdx = cells.findIndex((c) => /^\$\d{4,5}$/.test(c));
    if (salaryIdx < 1) continue;
    // Handle two LineStar formats:
    //   "Pos | Team | Player | Salary | ..." → cells[salaryIdx-1] = player
    //   "Pos | Player | Team | Salary | ..." → cells[salaryIdx-1] = team abbrev (2-4 caps)
    // If the cell before salary looks like a team abbreviation, step back one more.
    let playerName = cells[salaryIdx - 1];
    if (/^[A-Z]{2,4}$/.test(playerName) && salaryIdx >= 2) {
      playerName = cells[salaryIdx - 2];
    }
    if (!playerName || playerName.toLowerCase() === "player") continue; // skip header
    const salary  = parseInt(cells[salaryIdx].replace(/\D/g, ""), 10);
    if (!salary) continue;
    // projOwn% is the column immediately after salary
    const projOwn = parseFloat((cells[salaryIdx + 1] ?? "").replace("%", "")) || 0;
    // Proj is 4 columns after salary: projOwn%, actualOwn%, Diff, Proj
    const proj    = parseFloat(cells[salaryIdx + 4] ?? "") || 0;
    const isOut   = proj === 0;
    map.set(`${playerName.toLowerCase()}|${salary}`, { linestarProj: proj, projOwnPct: projOwn, isOut });
  }
  return map;
}

// Simple Levenshtein for fuzzy name matching
function levenshtein(a: string, b: string): number {
  const m = a.length, n = b.length;
  const dp: number[][] = Array.from({ length: m + 1 }, (_, i) =>
    Array.from({ length: n + 1 }, (_, j) => (i === 0 ? j : j === 0 ? i : 0))
  );
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i - 1] === b[j - 1]
        ? dp[i - 1][j - 1]
        : 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
    }
  }
  return dp[m][n];
}

function findLinestarMatch(name: string, salary: number, map: Map<string, LinestarEntry>) {
  const exact = map.get(`${name.toLowerCase()}|${salary}`);
  if (exact) return exact;
  let best: LinestarEntry | null = null;
  let bestDist = 4;
  for (const [key, val] of map.entries()) {
    const [lsName, lsSalStr] = key.split("|");
    if (parseInt(lsSalStr, 10) !== salary) continue;
    const dist = levenshtein(name.toLowerCase(), lsName);
    if (dist < bestDist) { bestDist = dist; best = val; }
  }
  return best;
}

// ── Projection helpers ────────────────────────────────────────

function mlToProb(ml: number): number {
  return ml >= 0 ? 100 / (ml + 100) : Math.abs(ml) / (Math.abs(ml) + 100);
}

function computeTeamImpliedTotal(
  vegasTotal: number,
  homeMl: number | null,
  awayMl: number | null,
  isHome: boolean,
): number {
  if (homeMl == null || awayMl == null) return vegasTotal / 2;
  const rawHome = mlToProb(homeMl);
  const rawAway = mlToProb(awayMl);
  const vig = rawHome + rawAway;
  const homeProbClean = rawHome / vig;
  // Each 2.5% deviation from 50% ≈ 1 point of spread in NBA
  const impliedSpread = Math.max(-15, Math.min(15, (homeProbClean - 0.5) / 0.025));
  const homeImplied = vegasTotal / 2 + impliedSpread / 2;
  return isHome ? homeImplied : vegasTotal - homeImplied;
}

function computeOurProjection(
  player: {
    avgMinutes: number | null; ppg: number | null; rpg: number | null;
    apg: number | null; spg: number | null; bpg: number | null;
    tovpg: number | null; threefgmPg: number | null;
    usageRate: number | null; ddRate: number | null;
  },
  teamPace: number,
  oppPace: number,
  oppDefRtg: number,
  vegasTotal: number | null = null,
  homeMl: number | null = null,
  awayMl: number | null = null,
  isHome = false,
  props: { propPts?: number | null; propReb?: number | null; propAst?: number | null } = {},
): number | null {
  const avgMinutes = player.avgMinutes ?? 0;
  if (avgMinutes < 10) return null;

  const ppg      = player.ppg       ?? 0;
  const rpg      = player.rpg       ?? 0;
  const apg      = player.apg       ?? 0;
  const spg      = player.spg       ?? 0;
  const bpg      = player.bpg       ?? 0;
  const tovpg    = player.tovpg     ?? 0;
  const threefgm = player.threefgmPg ?? 0;
  const ddRate   = player.ddRate    ?? 0;
  const usage    = player.usageRate ?? LEAGUE_AVG_USAGE;

  // Environment factors
  const gamePace    = (teamPace + oppPace) / 2;
  const paceFactor  = gamePace / LEAGUE_AVG_PACE;

  // Team-specific implied total from moneylines (not raw O/U ÷ 2)
  const totalFactor = vegasTotal
    ? computeTeamImpliedTotal(vegasTotal, homeMl, awayMl, isHome) / LEAGUE_AVG_TEAM_TOTAL
    : 1.0;

  const combinedEnv = paceFactor * 0.4 + totalFactor * 0.6;
  const defFactor   = oppDefRtg / LEAGUE_AVG_DEF_RTG;

  // Usage rate as volume multiplier: stars capture more extra possessions
  const usageFactor  = Math.min(2.0, Math.max(0.5, usage / LEAGUE_AVG_USAGE));
  const adjustedEnv  = 1.0 + (combinedEnv - 1.0) * usageFactor;

  // Per-stat projections — use market prop lines when available (they already
  // bake in matchup, pace, and injury context), fall back to formula otherwise.
  const projPts  = props.propPts  != null ? props.propPts  : ppg  * defFactor;
  const projReb  = props.propReb  != null ? props.propReb  : rpg  * adjustedEnv;
  const projAst  = props.propAst  != null ? props.propAst  : apg  * defFactor * (1.0 + (combinedEnv - 1.0) * 0.5);
  const projStl  = spg   * adjustedEnv;
  const projBlk  = bpg   * adjustedEnv;
  const projTov  = tovpg * adjustedEnv;
  const projDd   = ddRate * adjustedEnv;  // more possessions = more DD chances

  const fpts = (
    projPts * 1.0
    + projReb * 1.25
    + projAst * 1.5
    + projStl * 2.0
    + projBlk * 2.0
    - projTov * 0.5
    + threefgm * 0.5
    + projDd   * 1.5
  );
  return Math.round(fpts * 100) / 100;
}

function computeLeverage(
  ourProj: number,
  projOwnPct: number,
  fieldProj: number | null = null,
  spg = 0,
  bpg = 0,
  contrarianFactor = 0.7,
): number {
  // edge = how much more bullish we are than the field's expectation.
  // fieldProj priority: avg_fpts_dk (DK's salary-page projection, which drives
  // most contest ownership) → linestar_proj → fallback to ourProj (old behaviour).
  const edge         = fieldProj != null ? ourProj - fieldProj : ourProj;
  const ownFraction  = Math.max(0, Math.min(1, projOwnPct / 100));
  const ceilingBonus = 1.0 + spg * 0.05 + bpg * 0.04;
  return Math.round(edge * Math.pow(1 - ownFraction, contrarianFactor) * ceilingBonus * 1000) / 1000;
}

/** Compute pool-level ownership estimates based on our projections.
 *  Model: score = ourProj / sqrt(salary/$1K)  → normalize to 800% (8 lineup slots).
 *  Returns a Map of array-index → ownership percentage. */
function computePoolOwnership(
  players: Array<{ ourProj: number | null; salary: number; isOut: boolean }>,
): Map<number, number> {
  const TOTAL_OWN = 800; // 8 roster slots × 100%

  const scores: { idx: number; score: number }[] = [];
  for (let i = 0; i < players.length; i++) {
    const p = players[i];
    if (p.isOut || p.ourProj == null || p.ourProj <= 0 || p.salary <= 0) continue;
    scores.push({ idx: i, score: p.ourProj / Math.sqrt(p.salary / 1000) });
  }

  const total = scores.reduce((s, e) => s + e.score, 0);
  if (total === 0) return new Map();

  const result = new Map<number, number>();
  for (const { idx, score } of scores) {
    result.set(idx, Math.round((score / total) * TOTAL_OWN * 10) / 10);
  }
  return result;
}

// ── NBA Stats API (stats.nba.com) backfill ─────────────────────

const NBA_STATS_HEADERS: Record<string, string> = {
  Referer: "https://stats.nba.com/",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  Accept: "application/json, text/plain, */*",
  "x-nba-stats-origin": "stats",
  "x-nba-stats-token": "true",
};

/** Parse the stats.nba.com response format: { resultSets: [{ headers, rowSet }] } */
function parseNbaResponse(data: { resultSets: Array<{ headers: string[]; rowSet: unknown[][] }> }) {
  const rs = data.resultSets[0];
  return rs.rowSet.map((row) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < rs.headers.length; i++) obj[rs.headers[i]] = row[i];
    return obj;
  });
}

/** Fetch with retry (stats.nba.com is flaky). */
async function fetchNbaStats(url: string, params: Record<string, string>, retries = 3) {
  const qs = new URLSearchParams(params).toString();
  let lastErr: unknown;
  for (let i = 0; i < retries; i++) {
    try {
      const resp = await fetch(`${url}?${qs}`, {
        headers: NBA_STATS_HEADERS,
        signal: AbortSignal.timeout(60_000),
      });
      if (!resp.ok) throw new Error(`NBA API ${resp.status}: ${resp.statusText}`);
      return await resp.json();
    } catch (e) {
      lastErr = e;
      if (i < retries - 1) await new Promise((r) => setTimeout(r, (i + 1) * 5000));
    }
  }
  throw lastErr;
}

/** NBA numeric team ID → standard 3-letter abbreviation */
const NBA_ID_TO_ABBREV: Record<number, string> = {
  1610612737:"ATL",1610612738:"BOS",1610612751:"BKN",1610612766:"CHA",
  1610612741:"CHI",1610612739:"CLE",1610612742:"DAL",1610612743:"DEN",
  1610612765:"DET",1610612744:"GSW",1610612745:"HOU",1610612754:"IND",
  1610612746:"LAC",1610612747:"LAL",1610612763:"MEM",1610612748:"MIA",
  1610612749:"MIL",1610612750:"MIN",1610612740:"NOP",1610612752:"NYK",
  1610612760:"OKC",1610612753:"ORL",1610612755:"PHI",1610612756:"PHX",
  1610612757:"POR",1610612758:"SAC",1610612759:"SAS",1610612761:"TOR",
  1610612762:"UTA",1610612764:"WAS",
};

export async function backfillTeamStats(): Promise<{ ok: boolean; message: string }> {
  try {
    const data = await fetchNbaStats("https://stats.nba.com/stats/leaguedashteamstats", {
      Season: CURRENT_SEASON,
      SeasonType: "Regular Season",
      MeasureType: "Advanced",
      PerMode: "PerGame",
    });
    const rows = parseNbaResponse(data);

    // Build abbreviation → team_id cache
    const dbTeams = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
    const abbrevMap = new Map(dbTeams.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

    // Collect all valid rows, then single batch upsert (avoids 30 serial DB round-trips)
    const batch: Array<typeof nbaTeamStats.$inferInsert> = [];
    for (const row of rows) {
      const nbaId = row.TEAM_ID as number;
      const abbrev = NBA_ID_TO_ABBREV[nbaId];
      const teamId = abbrev ? abbrevMap.get(abbrev) : undefined;
      if (!teamId) continue;
      batch.push({
        teamId,
        season: CURRENT_SEASON,
        pace: (row.PACE as number) ?? null,
        offRtg: (row.OFF_RATING as number) ?? null,
        defRtg: (row.DEF_RATING as number) ?? null,
      });
    }

    if (batch.length > 0) {
      await db.insert(nbaTeamStats).values(batch).onConflictDoUpdate({
        target: [nbaTeamStats.teamId, nbaTeamStats.season],
        set: {
          pace: sql`EXCLUDED.pace`,
          offRtg: sql`EXCLUDED.off_rtg`,
          defRtg: sql`EXCLUDED.def_rtg`,
          fetchedAt: sql`NOW()`,
        },
      });
    }

    revalidatePath("/dfs");
    return { ok: true, message: `Team stats: ${batch.length}/30 teams updated for ${CURRENT_SEASON}` };
  } catch (e) {
    return { ok: false, message: `Team stats failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

export async function backfillPlayerStats(): Promise<{ ok: boolean; message: string }> {
  try {
    // LeagueDashPlayerStats with LastNGames=10 returns pre-aggregated per-game averages
    // (one row per active player) — far smaller payload than LeagueGameLog which returns
    // one row per player-game and requires client-side grouping + averaging.
    const data = await fetchNbaStats("https://stats.nba.com/stats/leaguedashplayerstats", {
      Season: CURRENT_SEASON,
      SeasonType: "Regular Season",
      PerMode: "PerGame",
      MeasureType: "Base",
      LastNGames: "10",
    });
    const rows = parseNbaResponse(data);
    if (rows.length === 0) return { ok: false, message: "No player stats data returned" };

    // Build abbreviation → team_id cache
    const dbTeams = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
    const abbrevMap = new Map(dbTeams.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

    const batch: Array<typeof nbaPlayerStats.$inferInsert> = [];

    for (const row of rows) {
      const playerId = row.PLAYER_ID as number;
      if (!playerId) continue;

      const name = row.PLAYER_NAME as string;
      const teamAbbrev = (row.TEAM_ABBREVIATION as string ?? "").toUpperCase();
      const teamId = abbrevMap.get(teamAbbrev) ?? null;
      const n = Math.max(Number(row.GP) || 1, 1);

      const r = (v: unknown) => Math.round((Number(v) || 0) * 10) / 10;

      // DD2 = double-double count over the last N games; divide by GP for rate
      const dd2 = Number(row.DD2) || 0;
      const ddRate = Math.round((dd2 / n) * 1000) / 1000;

      // USG_PCT is a proper usage rate (0–1 scale) from the endpoint; convert to %
      const usgRaw = Number(row.USG_PCT) || 0;
      const usageRate = Math.round(usgRaw * 1000) / 10; // e.g. 0.254 → 25.4

      batch.push({
        playerId, season: CURRENT_SEASON, teamId, name, position: null, games: n,
        avgMinutes: r(row.MIN),
        ppg: r(row.PTS), rpg: r(row.REB), apg: r(row.AST),
        spg: r(row.STL), bpg: r(row.BLK), tovpg: r(row.TOV),
        threefgmPg: r(row.FG3M),
        usageRate, ddRate,
      });
    }

    // Single batch upsert (Neon handles up to ~500 rows fine)
    for (let i = 0; i < batch.length; i += 100) {
      const chunk = batch.slice(i, i + 100);
      await db.insert(nbaPlayerStats).values(chunk).onConflictDoUpdate({
        target: [nbaPlayerStats.playerId, nbaPlayerStats.season],
        set: {
          teamId: sql`EXCLUDED.team_id`, name: sql`EXCLUDED.name`,
          games: sql`EXCLUDED.games`, avgMinutes: sql`EXCLUDED.avg_minutes`,
          ppg: sql`EXCLUDED.ppg`, rpg: sql`EXCLUDED.rpg`, apg: sql`EXCLUDED.apg`,
          spg: sql`EXCLUDED.spg`, bpg: sql`EXCLUDED.bpg`, tovpg: sql`EXCLUDED.tovpg`,
          threefgmPg: sql`EXCLUDED.threefgm_pg`, usageRate: sql`EXCLUDED.usage_rate`,
          ddRate: sql`EXCLUDED.dd_rate`,
          fptsStd: sql`COALESCE(EXCLUDED.fpts_std, nba_player_stats.fpts_std)`,
          fetchedAt: sql`NOW()`,
        },
      });
    }

    revalidatePath("/dfs");
    return { ok: true, message: `Player stats: ${batch.length} players updated (last 10 games)` };
  } catch (e) {
    return { ok: false, message: `Player stats failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

// ── Player props (The Odds API) ───────────────────────────────

export async function fetchPlayerProps(): Promise<{ ok: boolean; message: string }> {
  try {
    const oddsApiKey = process.env.ODDS_API_KEY;
    if (!oddsApiKey) return { ok: false, message: "ODDS_API_KEY not set in Vercel env vars" };

    // Get current slate
    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates).orderBy(desc(dkSlates.slateDate)).limit(1);
    if (!slate) return { ok: false, message: "No slate loaded — load a slate first" };
    const targetDate = slate.slateDate; // "YYYY-MM-DD"

    // Step 1: Get events for today (36h window to handle ET→UTC offset)
    const eventsResp = await fetch(
      `https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey=${oddsApiKey}&dateFormat=iso`,
      { next: { revalidate: 0 } },
    );
    if (!eventsResp.ok) throw new Error(`Odds API events: HTTP ${eventsResp.status}`);
    const allEvents = await eventsResp.json() as Array<{ id: string; commence_time: string }>;

    const windowStart = new Date(`${targetDate}T00:00:00Z`).getTime();
    const windowEnd   = windowStart + 36 * 3_600_000;
    const todayEvents = allEvents.filter((e) => {
      const t = new Date(e.commence_time).getTime();
      return t >= windowStart && t < windowEnd;
    });
    if (todayEvents.length === 0)
      return { ok: false, message: `No NBA events found for ${targetDate}` };

    // Step 2: Get slate players for matching
    const slatePlayers = await db
      .select({ id: dkPlayers.id, name: dkPlayers.name, teamId: dkPlayers.teamId })
      .from(dkPlayers).where(eq(dkPlayers.slateId, slate.id));

    // Step 3: Collect props across all events — average across all bookmakers.
    // Taking bookmakers[0] is non-deterministic (order varies by API call).
    // Consensus average across all available books is the most stable and
    // accurate single line to use as a projection input.
    type PropSet = { pts?: number; reb?: number; ast?: number };
    // Accumulators: player → stat → [sum, count] for averaging
    type Accumulator = Record<string, [number, number]>;
    const propAccum = new Map<string, Accumulator>(); // key = lower-cased player name

    for (const event of todayEvents) {
      const qs = new URLSearchParams({
        apiKey: oddsApiKey, regions: "us",
        markets: "player_points,player_rebounds,player_assists",
        oddsFormat: "american",
      });
      try {
        const r = await fetch(
          `https://api.the-odds-api.com/v4/sports/basketball_nba/events/${event.id}/odds?${qs}`,
          { next: { revalidate: 0 } },
        );
        if (!r.ok) continue;
        const data = await r.json() as {
          bookmakers: Array<{
            key: string;
            markets: Array<{
              key: string;
              outcomes: Array<{ name: string; description: string; point?: number }>;
            }>;
          }>;
        };
        // Iterate ALL bookmakers, not just [0]
        for (const bm of (data.bookmakers ?? [])) {
          for (const market of bm.markets) {
            const statKey = market.key === "player_points" ? "pts"
                          : market.key === "player_rebounds" ? "reb"
                          : market.key === "player_assists" ? "ast" : null;
            if (!statKey) continue;
            for (const o of market.outcomes) {
              if (o.description?.toLowerCase() !== "over" || o.point == null) continue;
              const key = o.name.toLowerCase();
              const accum = propAccum.get(key) ?? {};
              const [sum, cnt] = accum[statKey] ?? [0, 0];
              accum[statKey] = [sum + o.point, cnt + 1];
              propAccum.set(key, accum);
            }
          }
        }
      } catch { /* skip individual event failures */ }
    }

    // Collapse accumulators → consensus averages
    const propData = new Map<string, PropSet>();
    for (const [player, accum] of propAccum) {
      const entry: PropSet = {};
      for (const [stat, [sum, cnt]] of Object.entries(accum)) {
        (entry as Record<string, number>)[stat] = Math.round((sum / cnt) * 2) / 2; // round to nearest 0.5
      }
      propData.set(player, entry);
    }

    if (propData.size === 0)
      return { ok: false, message: "No player props returned by Odds API (check API key / plan)" };

    // Step 4: Match props → slate players (exact then fuzzy)
    // Map name → id for slate players
    const nameToPlayer = new Map(slatePlayers.map((p) => [p.name.toLowerCase(), p]));

    let propMatched = 0;
    const updates: Array<{ id: number; pts?: number; reb?: number; ast?: number }> = [];
    for (const [propName, props] of propData) {
      let match = nameToPlayer.get(propName);
      if (!match) {
        let bestDist = 4, bestMatch: typeof slatePlayers[0] | null = null;
        for (const [dkName, p] of nameToPlayer) {
          const d = levenshtein(propName, dkName);
          if (d < bestDist) { bestDist = d; bestMatch = p; }
        }
        match = bestMatch ?? undefined;
      }
      if (!match) continue;
      updates.push({ id: match.id, ...props });
      propMatched++;
    }

    // Step 5: Store props + recompute ourProj for matched players
    if (updates.length > 0) {
      // Bulk update props
      for (const u of updates) {
        await db.update(dkPlayers)
          .set({
            ...(u.pts != null && { propPts: u.pts }),
            ...(u.reb != null && { propReb: u.reb }),
            ...(u.ast != null && { propAst: u.ast }),
          })
          .where(eq(dkPlayers.id, u.id));
      }

      // Recompute ourProj using props for all matched players
      const updatedIds = new Set(updates.map((u) => u.id));
      const pool = await db.execute<{
        id: number; name: string; salary: number;
        teamId: number | null; matchupId: number | null;
        avgFptsDk: number | null; projOwnPct: number | null;
        isOut: boolean | null; ourProj: number | null;
        propPts: number | null; propReb: number | null; propAst: number | null;
      }>(sql`
        SELECT id, name, salary, team_id AS "teamId", matchup_id AS "matchupId",
               avg_fpts_dk AS "avgFptsDk", proj_own_pct AS "projOwnPct",
               is_out AS "isOut", our_proj AS "ourProj",
               prop_pts AS "propPts", prop_reb AS "propReb", prop_ast AS "propAst"
        FROM dk_players WHERE slate_id = ${slate.id}
      `);

      const teamStatRows = await db.select().from(nbaTeamStats).where(eq(nbaTeamStats.season, CURRENT_SEASON));
      const statsByTeam  = new Map(teamStatRows.map((r) => [r.teamId, r]));

      const matchupRows   = await db.select().from(nbaMatchups).where(eq(nbaMatchups.gameDate, targetDate));
      const matchupByTeam = new Map<number, typeof matchupRows[0]>();
      for (const m of matchupRows) {
        if (m.homeTeamId) matchupByTeam.set(m.homeTeamId, m);
        if (m.awayTeamId) matchupByTeam.set(m.awayTeamId, m);
      }

      const playerStatRows = await db.select().from(nbaPlayerStats).where(eq(nbaPlayerStats.season, CURRENT_SEASON));
      const playersByTeam  = new Map<number, typeof playerStatRows>();
      for (const ps of playerStatRows) {
        if (ps.teamId == null) continue;
        const arr = playersByTeam.get(ps.teamId) ?? [];
        arr.push(ps);
        playersByTeam.set(ps.teamId, arr);
      }

      const updatedProjs = new Map<number, number>();
      for (const p of pool.rows) {
        if (!updatedIds.has(p.id)) continue;
        if (!p.teamId) continue;
        const teamStat = statsByTeam.get(p.teamId);
        const matchup  = matchupByTeam.get(p.teamId);
        if (!teamStat || !matchup) continue;

        const oppId   = matchup.homeTeamId === p.teamId ? matchup.awayTeamId : matchup.homeTeamId;
        const oppStat = oppId ? statsByTeam.get(oppId) : null;
        if (!oppStat) continue;

        const candidates = playersByTeam.get(p.teamId) ?? [];
        let bestPlayer: typeof playerStatRows[0] | null = null;
        let bestDist = 4;
        for (const ps of candidates) {
          const d = levenshtein(p.name.toLowerCase(), ps.name.toLowerCase());
          if (d < bestDist) { bestDist = d; bestPlayer = ps; }
        }
        if (!bestPlayer) continue;

        const isHome = matchup.homeTeamId === p.teamId;
        const ourProj = computeOurProjection(
          bestPlayer,
          teamStat.pace  ?? LEAGUE_AVG_PACE,
          oppStat.pace   ?? LEAGUE_AVG_PACE,
          oppStat.defRtg ?? LEAGUE_AVG_DEF_RTG,
          matchup.vegasTotal, matchup.homeMl, matchup.awayMl, isHome,
          { propPts: p.propPts, propReb: p.propReb, propAst: p.propAst },
        );
        if (ourProj == null) continue;

        const fieldProj = p.avgFptsDk ?? null;
        const projOwnPct = p.projOwnPct ?? 15;
        const ourLeverage = computeLeverage(ourProj, projOwnPct, fieldProj,
          bestPlayer.spg ?? 0, bestPlayer.bpg ?? 0);

        await db.update(dkPlayers)
          .set({ ourProj, ourLeverage })
          .where(eq(dkPlayers.id, p.id));
        updatedProjs.set(p.id, ourProj);
      }

      // Recompute ownership model after projection updates
      const ownMap = computePoolOwnership(
        pool.rows.map((p) => ({ ourProj: updatedProjs.get(p.id) ?? p.ourProj, salary: p.salary, isOut: p.isOut ?? false })),
      );
      for (const [idx, ownPct] of ownMap) {
        await db.update(dkPlayers)
          .set({ ourOwnPct: ownPct })
          .where(eq(dkPlayers.id, pool.rows[idx].id));
      }
    }

    revalidatePath("/dfs");
    return {
      ok: true,
      message: `Player props: ${propMatched}/${slatePlayers.length} players matched across ${todayEvents.length} games`,
    };
  } catch (e) {
    return { ok: false, message: `Props failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

// ── DK API fetcher ────────────────────────────────────────────

const DK_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Accept": "application/json",
};

const POS_ORDER = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"];

type DkApiPlayer = {
  name: string; dkId: number; teamAbbrev: string;
  eligiblePositions: string; salary: number;
  gameInfo: string; avgFptsDk: number | null;
  /** DK injury status: "None" | "O" | "Q" | "GTD" | "D" | "OUT" */
  dkStatus: string;
  /** True = DK has locked this player out of draftability */
  isDisabled: boolean;
};

async function fetchDkPlayersFromApi(draftGroupId: number): Promise<DkApiPlayer[]> {
  const url = `https://api.draftkings.com/draftgroups/v1/draftgroups/${draftGroupId}/draftables`;
  const resp = await fetch(url, { headers: DK_HEADERS, next: { revalidate: 0 } });
  if (!resp.ok) throw new Error(`DK API ${resp.status}: ${url}`);
  const { draftables } = await resp.json() as { draftables: Record<string, unknown>[] };

  // Group by playerId — each player has one entry per eligible roster slot
  const byPlayer = new Map<number, typeof draftables>();
  for (const entry of draftables) {
    const pid = entry.playerId as number;
    if (!byPlayer.has(pid)) byPlayer.set(pid, []);
    byPlayer.get(pid)!.push(entry);
  }

  const players: DkApiPlayer[] = [];

  for (const [, entries] of byPlayer) {
    const sorted = [...entries].sort((a, b) => (a.rosterSlotId as number) - (b.rosterSlotId as number));
    const canonical = sorted[0];

    const allPos = new Set<string>(["UTIL"]);
    for (const e of sorted) {
      const pos = e.position as string;
      if (pos) allPos.add(pos);
    }
    const eligiblePositions = [...allPos]
      .sort((a, b) => {
        const ai = POS_ORDER.indexOf(a), bi = POS_ORDER.indexOf(b);
        return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
      })
      .join("/");

    // DK's own FPTS projection (stat attribute id=219)
    let avgFptsDk: number | null = null;
    for (const attr of (canonical.draftStatAttributes as { id: number; value: string }[] ?? [])) {
      if (attr.id === 279) { avgFptsDk = parseFloat(attr.value) || null; break; }
    }

    // DK injury / availability status
    const dkStatus   = (canonical.status as string) || "None";
    const isDisabled = !!(canonical.isDisabled as boolean);

    // Game info string — use Intl to handle EDT/EST automatically
    let gameInfo = "";
    const comp = canonical.competition as Record<string, unknown> | null;
    if (comp) {
      const name  = ((comp.name as string) ?? "").replace(" @ ", "@").replace(/ /g, "");
      const start = comp.startTime as string;
      if (start) {
        try {
          const dt    = new Date(start);
          const parts = new Intl.DateTimeFormat("en-US", {
            timeZone: "America/New_York",
            month: "2-digit", day: "2-digit", year: "numeric",
            hour: "numeric", minute: "2-digit", hour12: true,
          }).formatToParts(dt);
          const p = (t: string) => parts.find((x) => x.type === t)?.value ?? "";
          gameInfo = `${name} ${p("month")}/${p("day")}/${p("year")} ${p("hour")}:${p("minute")}${p("dayPeriod")} ET`;
        } catch {
          gameInfo = name;
        }
      } else {
        gameInfo = name;
      }
    }

    players.push({
      name:              (canonical.displayName as string) ?? "",
      dkId:              canonical.draftableId as number,
      teamAbbrev:        ((canonical.teamAbbreviation as string) ?? "").toUpperCase(),
      eligiblePositions,
      salary:            canonical.salary as number ?? 0,
      gameInfo,
      avgFptsDk,
      dkStatus,
      isDisabled,
    });
  }
  return players;
}

async function resolveDraftGroupId(contestId: number): Promise<number> {
  const url = `https://api.draftkings.com/contests/v1/contests/${contestId}`;
  const resp = await fetch(url, { headers: DK_HEADERS, next: { revalidate: 0 } });
  if (!resp.ok) throw new Error(`DK API ${resp.status} for contest ${contestId}`);
  const data = await resp.json() as { contestDetail: { draftGroupId: number } };
  return data.contestDetail.draftGroupId;
}

// ── Parse slate date from game_info ──────────────────────────

function parseSlateDate(gameInfo: string): string | null {
  const m = gameInfo.match(/(\d{2}\/\d{2}\/\d{4})/);
  if (!m) return null;
  const [mm, dd, yyyy] = m[1].split("/");
  return `${yyyy}-${mm}-${dd}`;
}

// ── Server Actions ────────────────────────────────────────────

export async function processDkSlate(formData: FormData): Promise<{
  ok: boolean; message: string; playerCount?: number; matchRate?: number;
}> {
  const dkFile        = formData.get("dkFile") as File | null;
  const lsFile        = formData.get("lsFile") as File | null;
  const cashLineStr   = formData.get("cashLine") as string | null;
  const contestType   = (formData.get("contestType") as string | null) || undefined;
  const fieldSizeStr  = formData.get("fieldSize") as string | null;
  const contestFormat = (formData.get("contestFormat") as string | null) || undefined;
  if (!dkFile) return { ok: false, message: "DK CSV required" };

  const dkPlayers_ = parseDkCsv(await dkFile.text());
  if (dkPlayers_.length === 0) return { ok: false, message: "No players parsed from DK CSV" };

  const lsMap    = lsFile ? parseLinestarCsv(await lsFile.text()) : new Map<string, LinestarEntry>();
  const cashLine = cashLineStr ? parseFloat(cashLineStr) : undefined;
  const fieldSize = fieldSizeStr ? parseInt(fieldSizeStr, 10) : undefined;
  return enrichAndSave(
    dkPlayers_, lsMap,
    isNaN(cashLine!) ? undefined : cashLine,
    undefined,
    contestType,
    fieldSize && !isNaN(fieldSize) ? fieldSize : undefined,
    contestFormat,
  );
}

// ── Auto-populate matchups from DK player pool ───────────────
//
// Called when no nba_matchups rows exist for the slate date — happens when
// the web UI loads a slate before daily_stats.yml has run. Parses games from
// the "away@home" game key in each player's game_info, upserts matchup rows,
// then optionally fills Vegas totals/MLs from The Odds API if ODDS_API_KEY
// is available in the environment.

async function ensureMatchupsForSlate(
  slateDate: string,
  dkPlayers_: DkApiPlayer[],
  abbrevToId: Map<string, number>,
): Promise<void> {
  const existing = await db.select({ id: nbaMatchups.id })
    .from(nbaMatchups)
    .where(eq(nbaMatchups.gameDate, slateDate));
  if (existing.length > 0) return;

  const resolve = (abbrev: string): number | null => {
    const canonical = DK_OVERRIDES[abbrev] ?? abbrev;
    return abbrevToId.get(canonical) ?? null;
  };

  // Parse unique game keys like "CHI@OKC" → away=CHI, home=OKC
  const gameSeen = new Set<string>();
  const games: { homeTeamId: number; awayTeamId: number }[] = [];
  for (const p of dkPlayers_) {
    const key = p.gameInfo.split(" ")[0];
    if (!key || gameSeen.has(key)) continue;
    gameSeen.add(key);
    const [awayAbbr, homeAbbr] = key.split("@");
    const homeTeamId = resolve(homeAbbr ?? "");
    const awayTeamId = resolve(awayAbbr ?? "");
    if (homeTeamId && awayTeamId) games.push({ homeTeamId, awayTeamId });
  }

  if (games.length > 0) {
    await db.insert(nbaMatchups)
      .values(games.map((g) => ({ gameDate: slateDate, ...g })))
      .onConflictDoNothing();
  }

  // Fetch Vegas odds if API key is available
  const oddsKey = process.env.ODDS_API_KEY;
  if (oddsKey && games.length > 0) {
    try {
      const oddsUrl = new URL("https://api.the-odds-api.com/v4/sports/basketball_nba/odds/");
      oddsUrl.searchParams.set("apiKey", oddsKey);
      oddsUrl.searchParams.set("regions", "us");
      oddsUrl.searchParams.set("markets", "h2h,totals");
      oddsUrl.searchParams.set("oddsFormat", "american");
      const oddsResp = await fetch(oddsUrl.toString(), { next: { revalidate: 0 } });
      if (oddsResp.ok) {
        const oddsGames = await oddsResp.json() as Array<{
          home_team: string; away_team: string;
          bookmakers: Array<{ markets: Array<{ key: string; outcomes: Array<{ name: string; price: number; point?: number }> }> }>;
        }>;

        // Build home-name → matchup lookup
        const matchupRows = await db.execute<{ id: number; homeName: string }>(sql`
          SELECT m.id, t.name AS "homeName"
          FROM nba_matchups m
          JOIN teams t ON t.team_id = m.home_team_id
          WHERE m.game_date = ${slateDate}
        `);
        const byHome = new Map(matchupRows.rows.map((r) => [r.homeName, r.id]));

        for (const og of oddsGames) {
          const mid = byHome.get(og.home_team);
          if (!mid) continue;
          // Consensus across all bookmakers for moneylines and totals
          const homePrices: number[] = [], awayPrices: number[] = [], totalPoints: number[] = [];
          for (const bm of og.bookmakers ?? []) {
            for (const market of bm.markets ?? []) {
              if (market.key === "h2h") {
                const ho = market.outcomes.find((o) => o.name === og.home_team);
                const ao = market.outcomes.find((o) => o.name === og.away_team);
                if (ho) homePrices.push(ho.price);
                if (ao) awayPrices.push(ao.price);
              } else if (market.key === "totals") {
                const over = market.outcomes.find((o) => o.name === "Over");
                if (over?.point != null) totalPoints.push(over.point);
              }
            }
          }
          const homeMl = homePrices.length ? Math.round(homePrices.reduce((a, b) => a + b, 0) / homePrices.length) : null;
          const awayMl = awayPrices.length ? Math.round(awayPrices.reduce((a, b) => a + b, 0) / awayPrices.length) : null;
          const vegasTotal = totalPoints.length ? Math.round(totalPoints.reduce((a, b) => a + b, 0) / totalPoints.length * 2) / 2 : null;
          if (homeMl || awayMl || vegasTotal) {
            await db.execute(sql`
              UPDATE nba_matchups
              SET home_ml = ${homeMl}, away_ml = ${awayMl}, vegas_total = ${vegasTotal}
              WHERE id = ${mid}
            `);
          }
        }
      }
    } catch {
      // Odds fetch is best-effort — projections proceed without Vegas context
    }
  }
}

// ── Shared enrichment (used by both CSV and API paths) ───────

async function enrichAndSave(
  dkPlayers_: DkApiPlayer[],
  lsMap: Map<string, LinestarEntry>,
  cashLine?: number,
  draftGroupId?: number,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{ ok: boolean; message: string; playerCount?: number; matchRate?: number }> {
  let slateDate = "";
  for (const p of dkPlayers_) {
    const d = parseSlateDate(p.gameInfo);
    if (d) { slateDate = d; break; }
  }
  if (!slateDate) slateDate = new Date().toISOString().slice(0, 10);

  const gameCount = new Set(dkPlayers_.map((p) => p.gameInfo.split(" ")[0])).size;

  const slateValues: {
    slateDate: string; gameCount: number;
    cashLine?: number; dkDraftGroupId?: number;
    contestType?: string; fieldSize?: number; contestFormat?: string;
  } = { slateDate, gameCount };
  if (cashLine != null) slateValues.cashLine = cashLine;
  if (draftGroupId != null) slateValues.dkDraftGroupId = draftGroupId;
  if (contestType) slateValues.contestType = contestType;
  if (fieldSize != null) slateValues.fieldSize = fieldSize;
  if (contestFormat) slateValues.contestFormat = contestFormat;

  const conflictSet: Record<string, unknown> = { gameCount };
  if (cashLine != null) conflictSet.cashLine = cashLine;
  // COALESCE: don't overwrite an existing draft group ID with null (CSV re-load)
  if (draftGroupId != null) conflictSet.dkDraftGroupId = draftGroupId;
  if (contestType) conflictSet.contestType = contestType;
  if (fieldSize != null) conflictSet.fieldSize = fieldSize;
  if (contestFormat) conflictSet.contestFormat = contestFormat;

  const [slate] = await db
    .insert(dkSlates)
    .values(slateValues)
    .onConflictDoUpdate({
      target: [dkSlates.slateDate, dkSlates.contestType, dkSlates.contestFormat],
      set: conflictSet,
    })
    .returning({ id: dkSlates.id });
  const slateId = slate.id;

  const teamRows = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
  const abbrevToId = new Map(teamRows.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

  // Auto-populate matchups from DK player pool if schedule hasn't run yet
  await ensureMatchupsForSlate(slateDate, dkPlayers_, abbrevToId);

  const matchupRows = await db.select().from(nbaMatchups).where(eq(nbaMatchups.gameDate, slateDate));
  const matchupByTeam = new Map<number, typeof matchupRows[0]>();
  for (const m of matchupRows) {
    if (m.homeTeamId) matchupByTeam.set(m.homeTeamId, m);
    if (m.awayTeamId) matchupByTeam.set(m.awayTeamId, m);
  }

  const teamStatRows = await db.select().from(nbaTeamStats).where(eq(nbaTeamStats.season, CURRENT_SEASON));
  const statsByTeam = new Map(teamStatRows.map((r) => [r.teamId, r]));

  const playerStatRows = await db.select().from(nbaPlayerStats).where(eq(nbaPlayerStats.season, CURRENT_SEASON));
  const playersByTeam = new Map<number, typeof playerStatRows>();
  for (const ps of playerStatRows) {
    if (ps.teamId == null) continue;
    const arr = playersByTeam.get(ps.teamId) ?? [];
    arr.push(ps);
    playersByTeam.set(ps.teamId, arr);
  }

  let lsMatched = 0;
  let projComputed = 0;
  const insertValues = [];

  for (const p of dkPlayers_) {
    const canonical = DK_OVERRIDES[p.teamAbbrev] ?? p.teamAbbrev;
    const teamId    = abbrevToId.get(canonical) ?? null;
    const matchup   = teamId ? matchupByTeam.get(teamId) ?? null : null;
    const matchupId = matchup?.id ?? null;

    const ls = findLinestarMatch(p.name, p.salary, lsMap);
    if (ls) lsMatched++;

    let ourProj: number | null = null;
    let ourLeverage: number | null = null;
    let spgForLev = 0, bpgForLev = 0;

    if (teamId && matchup) {
      const teamStat = statsByTeam.get(teamId);
      const oppId    = matchup.homeTeamId === teamId ? matchup.awayTeamId : matchup.homeTeamId;
      const oppStat  = oppId ? statsByTeam.get(oppId) : null;

      const candidates = playersByTeam.get(teamId) ?? [];
      let bestPlayer: typeof playerStatRows[0] | null = null;
      let bestDist = 4;
      for (const ps of candidates) {
        const d = levenshtein(p.name.toLowerCase(), ps.name.toLowerCase());
        if (d < bestDist) { bestDist = d; bestPlayer = ps; }
      }

      if (bestPlayer && teamStat && oppStat) {
        const isHome = matchup.homeTeamId === teamId;
        ourProj = computeOurProjection(
          bestPlayer,
          teamStat.pace    ?? LEAGUE_AVG_PACE,
          oppStat.pace     ?? LEAGUE_AVG_PACE,
          oppStat.defRtg   ?? LEAGUE_AVG_DEF_RTG,
          matchup.vegasTotal,
          matchup.homeMl,
          matchup.awayMl,
          isHome,
        );
        spgForLev = bestPlayer.spg ?? 0;
        bpgForLev = bestPlayer.bpg ?? 0;
        if (ourProj) projComputed++;
      }
    }

    // isOut: DK status is authoritative; LineStar proj==0 is a fallback signal.
    // DK returns "O", "Out", or "OUT" for scratches — normalise to upper-case.
    const dkIsOut = p.isDisabled || ["O", "OUT"].includes(p.dkStatus.toUpperCase());
    const isOut   = dkIsOut || (ls?.isOut ?? false);

    const projForLev = isOut ? 0 : (ourProj ?? ls?.linestarProj ?? 0);
    if (projForLev && ls?.projOwnPct != null) {
      // field_proj: DK's own projection is the primary ownership driver in the field;
      // LineStar is a reasonable fallback when avg_fpts_dk is unavailable.
      const fieldProj = p.avgFptsDk ?? ls?.linestarProj ?? null;
      ourLeverage = computeLeverage(projForLev, ls.projOwnPct, fieldProj, spgForLev, bpgForLev);
    }

    insertValues.push({
      slateId, dkPlayerId: p.dkId, name: p.name,
      teamAbbrev: p.teamAbbrev, teamId, matchupId,
      eligiblePositions: p.eligiblePositions, salary: p.salary,
      gameInfo: p.gameInfo, avgFptsDk: p.avgFptsDk,
      linestarProj: ls?.linestarProj ?? null, projOwnPct: ls?.projOwnPct ?? null,
      ourProj, ourLeverage, ourOwnPct: null as number | null, isOut,
      _spg: spgForLev, _bpg: bpgForLev,  // transient: ceiling bonus for leverage recalc
    });
  }

  // ── Baseline ownership when LineStar unavailable ────────────
  // Players without proj_own_pct get a proportional estimate from avg_fpts_dk
  // or our_proj, anchored at 15% average. This ensures leverage is always
  // computed so GPP mode works even without a LineStar cookie.
  const refProjs = insertValues
    .filter((p) => p.projOwnPct == null)
    .map((p) => p.avgFptsDk ?? p.ourProj ?? 0)
    .filter((v) => v > 0);
  const poolAvg = refProjs.length > 0 ? refProjs.reduce((a, b) => a + b, 0) / refProjs.length : 0;

  if (poolAvg > 0) {
    for (const p of insertValues) {
      if (p.projOwnPct != null) continue;
      const ref = p.avgFptsDk ?? p.ourProj ?? 0;
      if (!ref) continue;
      const baseOwn = Math.max(1, Math.min(50, (ref / poolAvg) * 15));
      p.projOwnPct = Math.round(baseOwn * 100) / 100;
      // Re-compute leverage now that we have an ownership estimate
      if (p.ourProj && !p.isOut) {
        const fieldProj = p.avgFptsDk ?? null;
        p.ourLeverage = computeLeverage(p.ourProj, p.projOwnPct, fieldProj, p._spg, p._bpg);
      }
    }
  }

  // ── Our ownership model ─────────────────────────────────────
  const ownMap = computePoolOwnership(insertValues);
  for (const [idx, ownPct] of ownMap) {
    (insertValues[idx] as Record<string, unknown>).ourOwnPct = ownPct;
  }

  for (let i = 0; i < insertValues.length; i += 50) {
    const batch = insertValues.slice(i, i + 50).map(({ _spg, _bpg, ...rest }) => rest);
    await db.insert(dkPlayers).values(batch).onConflictDoUpdate({
      target: [dkPlayers.slateId, dkPlayers.dkPlayerId],
      set: {
        salary: sql`EXCLUDED.salary`, teamId: sql`EXCLUDED.team_id`,
        matchupId: sql`EXCLUDED.matchup_id`,
        linestarProj: sql`EXCLUDED.linestar_proj`, projOwnPct: sql`EXCLUDED.proj_own_pct`,
        ourProj: sql`EXCLUDED.our_proj`, ourLeverage: sql`EXCLUDED.our_leverage`,
        ourOwnPct: sql`EXCLUDED.our_own_pct`,
        isOut: sql`EXCLUDED.is_out`, avgFptsDk: sql`EXCLUDED.avg_fpts_dk`,
        eligiblePositions: sql`EXCLUDED.eligible_positions`, gameInfo: sql`EXCLUDED.game_info`,
      },
    });
  }

  revalidatePath("/dfs");
  const matchRate = lsMap.size > 0 ? Math.round((lsMatched / dkPlayers_.length) * 100) : null;
  return {
    ok: true,
    message: `Saved ${insertValues.length} players (${projComputed} with our proj)${matchRate != null ? `, LineStar ${matchRate}% matched` : ""}`,
    playerCount: insertValues.length,
    matchRate: matchRate ?? undefined,
  };
}

// ── LineStar API helpers ──────────────────────────────────────

const LS_BASE    = "https://www.linestarapp.com/DesktopModules/DailyFantasyApi/API/Fantasy";
const LS_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  "Accept": "application/json",
  "Referer": "https://www.linestarapp.com/DesktopModules/DailyFantasyApi/",
};

/** Try to fetch LineStar projections + ownership using DNN_COOKIE from env.
 *  Returns an empty map if the cookie is missing, expired, or the API fails. */
function normalizeDnnCookie(raw: string): string {
  // Strip accidental ".DOTNETNUKE=" prefix if the user pasted the full cookie string
  return raw.startsWith(".DOTNETNUKE=") ? raw.slice(".DOTNETNUKE=".length) : raw;
}

async function tryFetchLinestarMap(draftGroupId: number): Promise<Map<string, LinestarEntry>> {
  const raw = process.env.DNN_COOKIE;
  if (!raw) return new Map();
  const cookie = normalizeDnnCookie(raw);
  try {
    const periodId = await resolveLinestarPeriodId(draftGroupId, cookie);
    if (!periodId) return new Map();
    const data = await fetchLinestarSalaries(periodId, cookie);
    return parseLinestarApiResponse(data);
  } catch {
    return new Map();
  }
}

async function resolveLinestarPeriodId(draftGroupId: number, cookie: string): Promise<number | null> {
  try {
    const resp = await fetch(`${LS_BASE}/GetPeriodInformation?site=1&sport=5`, {
      headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookie}` },
      next: { revalidate: 0 },
    });
    if (!resp.ok) return null;
    const periods = await resp.json() as Array<{ PeriodId?: number; Id?: number }>;
    const list = Array.isArray(periods) ? periods : (periods as { Periods?: typeof periods }).Periods ?? [];
    for (const period of list.slice(0, 10)) {
      const pid = period.PeriodId ?? period.Id;
      if (!pid) continue;
      // Probe: fetch salaries and see if this period matches our draft group
      const probe = await fetchLinestarSalaries(pid, cookie).catch(() => null);
      if (!probe) continue;
      const slates = (probe as { Slates?: Array<{ DfsSlateId?: number }> }).Slates ?? [];
      if (slates.some((s) => s.DfsSlateId === draftGroupId)) return pid;
    }
    return null;
  } catch {
    return null;
  }
}

async function fetchLinestarSalaries(periodId: number, cookie: string): Promise<unknown> {
  const resp = await fetch(`${LS_BASE}/GetSalariesV5?periodId=${periodId}&site=1&sport=5`, {
    headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookie}` },
    next: { revalidate: 0 },
  });
  if (!resp.ok) throw new Error(`LineStar ${resp.status}`);
  return resp.json();
}

function parseLinestarApiResponse(data: unknown): Map<string, LinestarEntry> {
  const d = data as {
    SalaryContainerJson?: string;
    Ownership?: { Projected?: Record<string, Array<{ SalaryId: number; Owned: number }>> };
  };
  const map = new Map<string, LinestarEntry>();
  if (!d.SalaryContainerJson) return map;
  let container: { Salaries?: Array<{ Id: number; Name: string; SAL: number; PP: number; STAT?: number; IS?: number }> };
  try { container = JSON.parse(d.SalaryContainerJson); } catch { return map; }

  // Build salary-id → avg ownership map
  const ownTotals = new Map<number, number>(); const ownCounts = new Map<number, number>();
  for (const entries of Object.values(d.Ownership?.Projected ?? {})) {
    if (!Array.isArray(entries)) continue;
    for (const e of entries) {
      ownTotals.set(e.SalaryId, (ownTotals.get(e.SalaryId) ?? 0) + e.Owned);
      ownCounts.set(e.SalaryId, (ownCounts.get(e.SalaryId) ?? 0) + 1);
    }
  }

  for (const p of container.Salaries ?? []) {
    const isOut = p.STAT === 4 || p.IS === 1;
    const proj  = parseFloat(String(p.PP)) || 0;
    const own   = ownCounts.get(p.Id) ? (ownTotals.get(p.Id)! / ownCounts.get(p.Id)!) : 0;
    const key   = `${p.Name.toLowerCase()}|${p.SAL}`;
    map.set(key, { linestarProj: proj, projOwnPct: own, isOut });
  }
  return map;
}

/** Check if the DNN_COOKIE in Vercel env is valid without fetching full data. */
export async function checkLinestarCookie(): Promise<{ ok: boolean; message: string; status?: number }> {
  const cookie = process.env.DNN_COOKIE;
  if (!cookie) return { ok: false, message: "DNN_COOKIE not set — add it to Vercel env vars" };
  const cookieValue = normalizeDnnCookie(cookie);
  try {
    const resp = await fetch(`${LS_BASE}/GetPeriodInformation?site=1&sport=5`, {
      headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookieValue}` },
      next: { revalidate: 0 },
    });
    if (resp.status === 401 || resp.status === 403)
      return { ok: false, message: "Cookie expired — update DNN_COOKIE in Vercel → Settings → Env Vars", status: resp.status };
    if (resp.status === 404)
      return { ok: false, message: "Endpoint not found (HTTP 404) — use manual CSV upload instead", status: 404 };
    if (!resp.ok)
      return { ok: false, message: `LineStar returned HTTP ${resp.status}`, status: resp.status };
    return { ok: true, message: "Cookie is valid" };
  } catch (e) {
    return { ok: false, message: `Connection failed: ${String(e)}` };
  }
}

/** Shared: write a LineStar map into the most-recent slate's player pool. */
async function _applyLinestarMap(
  lsMap: Map<string, LinestarEntry>,
): Promise<{ ok: boolean; message: string; matched: number; total: number }> {
  const slateRows = await db
    .select({ id: dkSlates.id })
    .from(dkSlates)
    .orderBy(desc(dkSlates.slateDate))
    .limit(1);
  if (!slateRows[0]) return { ok: false, message: "No slate loaded yet", matched: 0, total: 0 };
  const slateId = slateRows[0].id;

  const pool = await db.execute<{
    id: number; name: string; salary: number; teamId: number | null;
    avgFptsDk: number | null; ourProj: number | null; isOut: boolean | null;
  }>(sql`
    SELECT id, name, salary, team_id AS "teamId",
           avg_fpts_dk AS "avgFptsDk", our_proj AS "ourProj", is_out AS "isOut"
    FROM dk_players WHERE slate_id = ${slateId}
  `);

  // Load player stats for ceiling bonus (spg/bpg)
  const playerStatRows = await db.select().from(nbaPlayerStats).where(eq(nbaPlayerStats.season, CURRENT_SEASON));
  const playersByTeam = new Map<number, typeof playerStatRows>();
  for (const ps of playerStatRows) {
    if (ps.teamId == null) continue;
    const arr = playersByTeam.get(ps.teamId) ?? [];
    arr.push(ps);
    playersByTeam.set(ps.teamId, arr);
  }

  let matched = 0;
  for (const p of pool.rows) {
    const ls = findLinestarMatch(p.name, p.salary, lsMap);
    if (!ls) continue;
    matched++;
    let ourLeverage: number | null = null;
    if (p.ourProj && !p.isOut && ls.projOwnPct != null) {
      const fieldProj = p.avgFptsDk ?? ls.linestarProj ?? null;
      // Look up spg/bpg for ceiling bonus
      let spg = 0, bpg = 0;
      if (p.teamId) {
        const candidates = playersByTeam.get(p.teamId) ?? [];
        let bestDist = 4;
        for (const ps of candidates) {
          const d = levenshtein(p.name.toLowerCase(), ps.name.toLowerCase());
          if (d < bestDist) { bestDist = d; spg = ps.spg ?? 0; bpg = ps.bpg ?? 0; }
        }
      }
      ourLeverage = computeLeverage(p.ourProj, ls.projOwnPct, fieldProj, spg, bpg);
    }
    // Do NOT touch isOut — DK API status is the authoritative source.
    // LineStar proj=0 does not mean the player is scratched.
    await db.update(dkPlayers)
      .set({ linestarProj: ls.linestarProj, projOwnPct: ls.projOwnPct, ourLeverage })
      .where(eq(dkPlayers.id, p.id));
  }

  // Recompute our ownership model after LineStar update
  const ownMap = computePoolOwnership(
    pool.rows.map((p) => ({ ourProj: p.ourProj, salary: p.salary, isOut: p.isOut ?? false })),
  );
  for (const [idx, ownPct] of ownMap) {
    const p = pool.rows[idx];
    await db.update(dkPlayers).set({ ourOwnPct: ownPct }).where(eq(dkPlayers.id, p.id));
  }

  revalidatePath("/dfs");
  const pct = pool.rows.length > 0 ? Math.round(matched / pool.rows.length * 100) : 0;
  return {
    ok: true,
    message: `LineStar data applied — ${matched}/${pool.rows.length} players matched (${pct}%)`,
    matched,
    total: pool.rows.length,
  };
}

/** Inject LineStar data from an uploaded CSV file. */
export async function uploadLinestarCsv(formData: FormData): Promise<{
  ok: boolean; message: string; matched: number; total: number;
}> {
  const file = formData.get("lsFile") as File | null;
  if (!file) return { ok: false, message: "No file provided", matched: 0, total: 0 };
  const lsMap = parseLinestarCsv(await file.text());
  if (lsMap.size === 0) return { ok: false, message: "No players parsed from LineStar CSV", matched: 0, total: 0 };
  return _applyLinestarMap(lsMap);
}

/** Inject LineStar data from text pasted directly from the LineStar web table. */
export async function applyLinestarPaste(text: string): Promise<{
  ok: boolean; message: string; matched: number; total: number;
}> {
  if (!text.trim()) return { ok: false, message: "No data provided", matched: 0, total: 0 };
  const lsMap = parseLinestarPasteText(text);
  if (lsMap.size === 0) return { ok: false, message: "No players parsed — expected tab-separated LineStar data", matched: 0, total: 0 };
  return _applyLinestarMap(lsMap);
}

// ── Historical slate import ────────────────────────────────────

type HistoricalEntry = {
  position: string;
  linestarProj: number;
  projOwnPct: number;
  actualOwnPct: number;
  actualFpts: number | null;
  teamAbbrev: string;
};

/**
 * Parse LineStar historical paste. Same column anchor as the live parser
 * (salary = $NNNN) but also captures actualOwnPct (+2) and actualFpts (+5).
 *
 * Live format:   Pos | Team | Player | Salary | projOwn% | actualOwn% | Diff | Proj
 * History adds:  ... | Actual (col +5 after salary)
 */
function parseHistoricalPaste(text: string): Map<string, HistoricalEntry> {
  const lines = text.split(/\r?\n/).filter(Boolean);
  const map = new Map<string, HistoricalEntry>();

  for (const line of lines) {
    const cells = line.split("\t").map((c) => c.trim());
    const salaryIdx = cells.findIndex((c) => /^\$\d{4,5}$/.test(c));
    if (salaryIdx < 1) continue;

    let playerName = cells[salaryIdx - 1];
    let teamAbbrev = "";
    if (/^[A-Z]{2,4}$/.test(playerName) && salaryIdx >= 2) {
      teamAbbrev = playerName;
      playerName = cells[salaryIdx - 2];
    } else if (salaryIdx >= 2) {
      teamAbbrev = cells[salaryIdx - 2] ?? "";
    }
    if (!playerName || playerName.toLowerCase() === "player") continue;

    const salary     = parseInt(cells[salaryIdx].replace(/\D/g, ""), 10);
    if (!salary) continue;

    // Position is always cells[0] (first column in every LineStar format)
    const posRaw = cells[0]?.trim() ?? "";
    const position = /^(PG|SG|SF|PF|C)(\/(?:PG|SG|SF|PF|C))*$/.test(posRaw) ? posRaw : "UTIL";

    const projOwnPct   = parseFloat((cells[salaryIdx + 1] ?? "").replace("%", "")) || 0;
    const actualOwnPct = parseFloat((cells[salaryIdx + 2] ?? "").replace("%", "")) || 0;
    const linestarProj = parseFloat(cells[salaryIdx + 4] ?? "") || 0;
    // actualFpts is in col +5 for historical data (labelled "Scored" or "Actual")
    const rawActual    = cells[salaryIdx + 5] ?? "";
    const actualFpts   = rawActual !== "" && !isNaN(parseFloat(rawActual))
      ? parseFloat(rawActual) : null;

    map.set(`${playerName.toLowerCase()}|${salary}`, {
      position, linestarProj, projOwnPct, actualOwnPct, actualFpts,
      teamAbbrev: teamAbbrev.toUpperCase(),
    });
  }
  return map;
}

/** Deterministic synthetic DK player ID for historical records.
 *  Uses a range > 10 billion to avoid collision with real DK IDs (~20–50M). */
function syntheticDkId(name: string, salary: number): number {
  let h = 5381;
  const s = `${name.toLowerCase()}_${salary}`;
  for (let i = 0; i < s.length; i++) h = ((h * 33) ^ s.charCodeAt(i)) >>> 0;
  return (h % 900_000_000) + 10_000_000_000;
}

/**
 * Save historical LineStar data (past slate results) to Neon.
 *
 * Two modes determined automatically:
 *   - Slate already exists for `date` → update actual_fpts + actual_own_pct
 *     on existing dk_players rows (updates ourProj-based training pairs)
 *   - No slate exists → create dk_slate + dk_players with synthetic IDs
 *     (stores linestarProj + actual for LineStar MAE tracking)
 */
export async function saveHistoricalSlate(
  date: string,
  text: string,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{ ok: boolean; message: string; created: number; updated: number }> {
  if (!date) return { ok: false, message: "Date is required", created: 0, updated: 0 };
  if (!text.trim()) return { ok: false, message: "No data pasted", created: 0, updated: 0 };

  const parsed = parseHistoricalPaste(text);
  if (parsed.size === 0)
    return { ok: false, message: "No players parsed — expected tab-separated LineStar data", created: 0, updated: 0 };

  // Find existing slate matching this date + contest type + format (all three must match)
  const effectiveType   = contestType   ?? "main";
  const effectiveFormat = contestFormat ?? "gpp";
  const existingSlate = await db
    .select({ id: dkSlates.id })
    .from(dkSlates)
    .where(
      and(
        eq(dkSlates.slateDate, date),
        eq(dkSlates.contestType, effectiveType),
        eq(dkSlates.contestFormat, effectiveFormat),
      )
    )
    .limit(1);

  const abbrevCache = new Map(
    (await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams))
      .map((t) => [t.abbreviation.toUpperCase(), t.teamId]),
  );

  // ── Mode 1: slate exists → update actual results on existing rows ──────────
  if (existingSlate[0]) {
    const slateId = existingSlate[0].id;
    const pool = await db.execute<{
      id: number; name: string; salary: number;
    }>(sql`SELECT id, name, salary FROM dk_players WHERE slate_id = ${slateId}`);

    let updated = 0;
    for (const p of pool.rows) {
      const entry = parsed.get(`${p.name.toLowerCase()}|${p.salary}`);
      let match = entry;
      if (!match) {
        let bestDist = 4;
        for (const [key, val] of parsed) {
          const [pName, salStr] = key.split("|");
          if (parseInt(salStr, 10) !== p.salary) continue;
          const d = levenshtein(p.name.toLowerCase(), pName);
          if (d < bestDist) { bestDist = d; match = val; }
        }
      }
      if (!match) continue;

      await db.update(dkPlayers)
        .set({
          actualFpts:   match.actualFpts,
          actualOwnPct: match.actualOwnPct || null,
          linestarProj: match.linestarProj || null,
          projOwnPct:   match.projOwnPct   || null,
        })
        .where(eq(dkPlayers.id, p.id));
      updated++;
    }

    revalidatePath("/dfs");
    return {
      ok: true,
      message: `Updated ${updated}/${pool.rows.length} players with actual results for ${date}`,
      created: 0,
      updated,
    };
  }

  // ── Mode 2: no slate → create dk_slate + dk_players with synthetic IDs ─────
  const [newSlate] = await db
    .insert(dkSlates)
    .values({
      slateDate: date,
      gameCount: 0,
      contestType:   effectiveType,
      contestFormat: effectiveFormat,
      ...(fieldSize != null && { fieldSize }),
    })
    .onConflictDoUpdate({
      target: [dkSlates.slateDate, dkSlates.contestType, dkSlates.contestFormat],
      set: { ...(fieldSize != null && { fieldSize }) },
    })
    .returning({ id: dkSlates.id });

  const slateId = newSlate.id;
  let created = 0;

  for (const [key, entry] of parsed) {
    const [playerName, salStr] = key.split("|");
    const salary    = parseInt(salStr, 10);
    const dkPlayerId = syntheticDkId(playerName, salary);
    const teamId    = abbrevCache.get(entry.teamAbbrev) ?? null;
    const name      = playerName.replace(/\b\w/g, (c) => c.toUpperCase()); // restore title case

    await db.insert(dkPlayers)
      .values({
        slateId, dkPlayerId, name,
        teamAbbrev: entry.teamAbbrev || "UNK",
        teamId, salary,
        eligiblePositions: entry.position || "UTIL",
        linestarProj:  entry.linestarProj  || null,
        projOwnPct:    entry.projOwnPct    || null,
        actualOwnPct:  entry.actualOwnPct  || null,
        actualFpts:    entry.actualFpts,
        isOut: false,
      })
      .onConflictDoUpdate({
        target: [dkPlayers.slateId, dkPlayers.dkPlayerId],
        set: {
          linestarProj:  sql`EXCLUDED.linestar_proj`,
          projOwnPct:    sql`EXCLUDED.proj_own_pct`,
          actualOwnPct:  sql`EXCLUDED.actual_own_pct`,
          actualFpts:    sql`EXCLUDED.actual_fpts`,
        },
      });
    created++;
  }

  revalidatePath("/dfs");
  return {
    ok: true,
    message: `Created historical slate for ${date} with ${created} players (synthetic IDs — ourProj will be null)`,
    created,
    updated: 0,
  };
}

export async function loadSlateFromContestId(
  contestId: string,
  cashLine?: number,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{
  ok: boolean; message: string; playerCount?: number;
}> {
  try {
    const dgId    = await resolveDraftGroupId(parseInt(contestId, 10));
    const players = await fetchDkPlayersFromApi(dgId);
    if (players.length === 0) return { ok: false, message: "No players returned from DK API" };
    // Auto-fetch LineStar if DNN_COOKIE is available in the server environment
    const lsMap = await tryFetchLinestarMap(dgId);
    const result = await enrichAndSave(players, lsMap, cashLine, dgId, contestType, fieldSize, contestFormat);
    return { ...result, message: `[API] ${result.message}` };
  } catch (e) {
    return { ok: false, message: String(e) };
  }
}

/**
 * Re-fetch the DK draftables API for the current slate and update is_out for
 * every player whose injury status has changed since the slate was loaded.
 *
 * Requires dk_draft_group_id to be saved on dk_slates (set when loading via
 * Contest ID). Players no longer present in the API response are marked OUT
 * (DK removes confirmed scratches from the draftable list before lock).
 */
export async function refreshPlayerStatus(slateId: number): Promise<{
  ok: boolean; message: string; updated: number;
}> {
  try {
    // Get the draft group ID saved when the slate was loaded
    const slateRows = await db
      .select({ dkDraftGroupId: dkSlates.dkDraftGroupId })
      .from(dkSlates)
      .where(eq(dkSlates.id, slateId));
    const dgId = slateRows[0]?.dkDraftGroupId;
    if (!dgId) {
      return {
        ok: false,
        message: "No draft group ID saved for this slate — reload via Contest ID first",
        updated: 0,
      };
    }

    // Fetch live draftables
    const url  = `https://api.draftkings.com/draftgroups/v1/draftgroups/${dgId}/draftables`;
    const resp = await fetch(url, { headers: DK_HEADERS, next: { revalidate: 0 } });
    if (!resp.ok) throw new Error(`DK API ${resp.status}`);
    const { draftables } = await resp.json() as { draftables: Record<string, unknown>[] };

    // Build map: draftableId → isOut.
    // DK uses "O", "Out", and "OUT" (case varies) for scratches; normalise.
    const liveStatus = new Map<number, boolean>();
    for (const d of draftables) {
      const draftableId = d.draftableId as number;
      const s = ((d.status as string) ?? "").toUpperCase();
      const isOut = !!(d.isDisabled as boolean) || s === "O" || s === "OUT";
      liveStatus.set(draftableId, isOut);
    }

    // Compare against stored players
    const stored = await db
      .select({ id: dkPlayers.id, dkPlayerId: dkPlayers.dkPlayerId, isOut: dkPlayers.isOut })
      .from(dkPlayers)
      .where(eq(dkPlayers.slateId, slateId));

    let updated = 0;
    for (const p of stored) {
      const live = liveStatus.get(p.dkPlayerId);
      // Player absent from API = DK removed them (scratch confirmed)
      const newIsOut = live === undefined ? true : live;
      if (newIsOut !== (p.isOut ?? false)) {
        await db.update(dkPlayers).set({ isOut: newIsOut }).where(eq(dkPlayers.id, p.id));
        updated++;
      }
    }

    revalidatePath("/dfs");
    return {
      ok: true,
      message: updated > 0
        ? `${updated} player status update${updated > 1 ? "s" : ""} applied`
        : "All player statuses are current — no changes",
      updated,
    };
  } catch (e) {
    return { ok: false, message: String(e), updated: 0 };
  }
}

export async function runOptimizer(
  slateId: number,
  gameFilter: number[],
  settings: OptimizerSettings,
): Promise<{ ok: boolean; lineups?: GeneratedLineup[]; error?: string }> {
  const rows = await db.execute<OptimizerPlayer & { slateId: number }>(sql`
    SELECT
      dp.id, dp.dk_player_id AS "dkPlayerId", dp.name, dp.team_abbrev AS "teamAbbrev",
      dp.team_id AS "teamId", dp.matchup_id AS "matchupId",
      dp.eligible_positions AS "eligiblePositions", dp.salary,
      dp.our_proj AS "ourProj", dp.our_leverage AS "ourLeverage",
      dp.linestar_proj AS "linestarProj", dp.proj_own_pct AS "projOwnPct",
      dp.is_out AS "isOut", dp.game_info AS "gameInfo",
      t.logo_url AS "teamLogo", t.name AS "teamName",
      m.home_team_id AS "homeTeamId"
    FROM dk_players dp
    LEFT JOIN teams t ON t.team_id = dp.team_id
    LEFT JOIN nba_matchups m ON m.id = dp.matchup_id
    WHERE dp.slate_id = ${slateId}
  `);

  const pool: OptimizerPlayer[] = rows.rows.filter((p) =>
    gameFilter.length === 0 || (p.matchupId != null && gameFilter.includes(p.matchupId))
  );

  try {
    const lineups = optimizeLineups(pool, settings);
    if (lineups.length === 0) {
      const eligible = pool.filter((p) => {
        if (p.isOut) return false;
        return p.ourProj != null && (p.ourProj as number) > 0 && p.salary > 0;
      });
      const guards   = eligible.filter((p) => p.eligiblePositions.includes("PG") || p.eligiblePositions.includes("SG")).length;
      const forwards = eligible.filter((p) => p.eligiblePositions.includes("SF") || p.eligiblePositions.includes("PF")).length;
      const centers  = eligible.filter((p) => p.eligiblePositions.includes("C")).length;
      const withMatchup = eligible.filter((p) => p.matchupId != null).length;
      const hint = eligible.length < 15
        ? " Pool too small — click Refresh Player Status to reset OUT flags, then re-paste LineStar data."
        : guards < 3
          ? " Not enough guards (need ≥3 PG/SG)."
          : forwards < 3
            ? " Not enough forwards (need ≥3 SF/PF)."
            : centers < 1
              ? " No centers in pool."
              : withMatchup < 8
                ? " Most players missing matchup data — reload slate via Contest ID."
                : " Try reducing lineup count or switching to Cash mode.";
      return {
        ok: false,
        error: `No lineups — ${eligible.length} eligible: ${guards}G / ${forwards}F / ${centers}C` +
          `, ${withMatchup}/${eligible.length} with matchup data.${hint}`,
      };
    }
    return { ok: true, lineups };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

export async function saveLineups(
  slateId: number,
  lineups: LineupForSave[],
  strategy: string,
): Promise<{ ok: boolean; saved: number }> {
  let saved = 0;
  for (let i = 0; i < lineups.length; i++) {
    const l = lineups[i];
    const playerIds = l.players.map((p) => p.id).join(",");
    const stackTeam = (() => {
      const counts = new Map<string, number>();
      for (const p of l.players) counts.set(p.teamAbbrev, (counts.get(p.teamAbbrev) ?? 0) + 1);
      let best = "", bestCount = 0;
      for (const [team, count] of counts) { if (count > bestCount) { bestCount = count; best = team; } }
      return bestCount >= 2 ? best : null;
    })();
    await db
      .insert(dkLineups)
      .values({
        slateId,
        strategy,
        lineupNum:   i + 1,
        playerIds,
        totalSalary: l.totalSalary,
        projFpts:    l.projFpts,
        leverage:    l.leverageScore,
        stackTeam,
      })
      .onConflictDoUpdate({
        target: [dkLineups.slateId, dkLineups.strategy, dkLineups.lineupNum],
        set: {
          playerIds:   sql`EXCLUDED.player_ids`,
          totalSalary: sql`EXCLUDED.total_salary`,
          projFpts:    sql`EXCLUDED.proj_fpts`,
          leverage:    sql`EXCLUDED.leverage`,
          stackTeam:   sql`EXCLUDED.stack_team`,
        },
      });
    saved++;
  }
  revalidatePath("/dfs");
  return { ok: true, saved };
}

export async function exportLineups(
  lineups: GeneratedLineup[],
  entryTemplate: string,
): Promise<string> {
  const entryRows = entryTemplate.split(/\r?\n/).filter(Boolean);
  return buildMultiEntryCSV(lineups, entryRows);
}

// ── MLB Optimizer ─────────────────────────────────────────────

export async function runMlbOptimizer(
  slateId: number,
  gameFilter: number[],
  settings: MlbOptimizerSettings,
): Promise<{ ok: boolean; lineups?: MlbGeneratedLineup[]; error?: string }> {
  const rows = await db.execute<MlbOptimizerPlayer>(sql`
    SELECT
      dp.id, dp.dk_player_id AS "dkPlayerId", dp.name, dp.team_abbrev AS "teamAbbrev",
      dp.mlb_team_id AS "teamId", dp.matchup_id AS "matchupId",
      dp.eligible_positions AS "eligiblePositions", dp.salary,
      dp.our_proj AS "ourProj", dp.our_leverage AS "ourLeverage",
      dp.linestar_proj AS "linestarProj", dp.proj_own_pct AS "projOwnPct",
      dp.is_out AS "isOut", dp.game_info AS "gameInfo",
      mt.logo_url AS "teamLogo", mt.name AS "teamName",
      mm.home_team_id AS "homeTeamId"
    FROM dk_players dp
    LEFT JOIN mlb_teams mt ON mt.team_id = dp.mlb_team_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE dp.slate_id = ${slateId}
  `);

  const pool: MlbOptimizerPlayer[] = rows.rows.filter((p) =>
    gameFilter.length === 0 || (p.matchupId != null && gameFilter.includes(p.matchupId)),
  );

  try {
    const lineups = optimizeMlbLineups(pool, settings);
    if (lineups.length === 0) {
      const eligible = pool.filter(
        (p) => !p.isOut && p.ourProj != null && (p.ourProj as number) > 0 && p.salary > 0,
      );
      const pitchers = eligible.filter((p) =>
        p.eligiblePositions.includes("SP") || p.eligiblePositions.includes("RP"),
      ).length;
      const catchers = eligible.filter(
        (p) => p.eligiblePositions.includes("C") && !p.eligiblePositions.includes("SP"),
      ).length;
      const hint = eligible.length < 20
        ? " Pool too small — upload the DK CSV first."
        : pitchers < 2
          ? " Not enough pitchers (need ≥2 SP/RP)."
          : catchers < 1
            ? " No catchers in pool."
            : " Try reducing lineup count or switching to Cash mode.";
      return {
        ok: false,
        error: `No lineups — ${eligible.length} eligible: ${pitchers} P / ${catchers} C.${hint}`,
      };
    }
    return { ok: true, lineups };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

export async function exportMlbLineups(
  lineups: MlbGeneratedLineup[],
  entryTemplate: string,
): Promise<string> {
  const entryRows = entryTemplate.split(/\r?\n/).filter(Boolean);
  return buildMlbMultiEntryCSV(lineups, entryRows);
}

// ── Results Upload (Phase 3) ──────────────────────────────────

type ResultPlayer = { name: string; actualFpts: number; actualOwnPct?: number };

function parseStandingsCsv(content: string): ResultPlayer[] {
  // DK contest standings: positional columns
  // Row cols: 0=Rank, 1=EntryId, ..., 7=Player, 8=Roster Position, 9=%Drafted, 10=FPTS
  const lines = content.split(/\r?\n/).filter(Boolean).slice(1); // skip header
  const seen = new Map<string, ResultPlayer>();
  for (const line of lines) {
    const cells = line.split(",");
    if (cells.length < 11) continue;
    const name    = cells[7]?.trim() ?? "";
    const ownStr  = (cells[9]?.trim() ?? "").replace("%", "");
    const fptsStr = cells[10]?.trim() ?? "";
    if (!name) continue;
    const actualFpts    = parseFloat(fptsStr);
    const actualOwnPct  = parseFloat(ownStr);
    if (isNaN(actualFpts)) continue;
    if (!seen.has(name)) {
      seen.set(name, { name, actualFpts, actualOwnPct: isNaN(actualOwnPct) ? undefined : actualOwnPct });
    }
  }
  return Array.from(seen.values());
}

function parseResultsCsv(content: string): ResultPlayer[] {
  // DK results CSV: named columns — Name, Salary, FPTS (or Total Points / ActualFpts)
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const header = lines[0].split(",").map((h) => h.trim().replace(/^"|"$/g, ""));
  const col = (name: string) => header.findIndex((h) => h === name);

  const nameCol  = col("Name");
  const fptsCol  = [col("FPTS"), col("Total Points"), col("ActualFpts"), col("Actual FPTS")]
    .find((c) => c !== -1) ?? -1;

  if (nameCol === -1 || fptsCol === -1) return [];

  const players: ResultPlayer[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
    const name = cells[nameCol] ?? "";
    if (!name) continue;
    const actualFpts = parseFloat(cells[fptsCol] ?? "");
    if (!isNaN(actualFpts)) {
      players.push({ name, actualFpts });
    }
  }
  return players;
}

export async function uploadResults(formData: FormData): Promise<{
  ok: boolean;
  message: string;
  updated?: number;
  total?: number;
  matchRate?: number;
}> {
  const file = formData.get("resultsFile") as File | null;
  if (!file) return { ok: false, message: "Results CSV required" };

  const content = await file.text();
  const firstLine = content.split("\n")[0] ?? "";
  const isStandings = firstLine.includes("EntryId") || firstLine.includes("%Drafted");

  const resultPlayers = isStandings ? parseStandingsCsv(content) : parseResultsCsv(content);

  if (resultPlayers.length === 0) {
    return { ok: false, message: "No players with FPTS found in the file" };
  }

  // Most recent slate
  const [slate] = await db
    .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
    .from(dkSlates)
    .orderBy(desc(dkSlates.slateDate))
    .limit(1);

  if (!slate) return { ok: false, message: "No slate found — load a slate first" };

  const pool = await db
    .select({ id: dkPlayers.id, name: dkPlayers.name })
    .from(dkPlayers)
    .where(eq(dkPlayers.slateId, slate.id));

  if (pool.length === 0) {
    return { ok: false, message: `No players in slate ${slate.slateDate}` };
  }

  // Match + update
  let updated = 0;
  for (const rp of resultPlayers) {
    let match = pool.find((p) => p.name === rp.name);
    if (!match) {
      let bestDist = 4;
      for (const p of pool) {
        const d = levenshtein(rp.name.toLowerCase(), p.name.toLowerCase());
        if (d < bestDist) { bestDist = d; match = p; }
      }
    }
    if (match) {
      await db
        .update(dkPlayers)
        .set({ actualFpts: rp.actualFpts, actualOwnPct: rp.actualOwnPct ?? null })
        .where(eq(dkPlayers.id, match.id));
      updated++;
    }
  }

  // Roll up lineup actuals
  const lineupRows = await db
    .select({ id: dkLineups.id, playerIds: dkLineups.playerIds })
    .from(dkLineups)
    .where(eq(dkLineups.slateId, slate.id));

  let lineupsUpdated = 0;
  for (const lineup of lineupRows) {
    const ids = (lineup.playerIds ?? "")
      .split(",")
      .map((s) => parseInt(s.trim(), 10))
      .filter((n) => !isNaN(n));
    if (ids.length === 0) continue;

    const [row] = await db
      .select({ total: sql<number | null>`SUM(${dkPlayers.actualFpts})` })
      .from(dkPlayers)
      .where(and(inArray(dkPlayers.id, ids), sql`${dkPlayers.actualFpts} IS NOT NULL`));

    if (row?.total != null) {
      await db.update(dkLineups).set({ actualFpts: row.total }).where(eq(dkLineups.id, lineup.id));
      lineupsUpdated++;
    }
  }

  revalidatePath("/dfs");

  const matchRate = Math.round((updated / resultPlayers.length) * 100);
  const lineupNote = lineupRows.length > 0 ? `, ${lineupsUpdated}/${lineupRows.length} lineup actuals updated` : "";
  return {
    ok: true,
    message: `${updated}/${resultPlayers.length} players matched (${matchRate}%)${lineupNote} — slate ${slate.slateDate}`,
    updated,
    total: resultPlayers.length,
    matchRate,
  };
}
