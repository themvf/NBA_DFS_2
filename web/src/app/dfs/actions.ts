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
import { eq, sql, and } from "drizzle-orm";
import { optimizeLineups, buildMultiEntryCSV } from "./optimizer";
import type { OptimizerPlayer, OptimizerSettings, GeneratedLineup } from "./optimizer";

const LEAGUE_AVG_PACE    = 100.0;
const LEAGUE_AVG_DEF_RTG = 112.0;
const LEAGUE_AVG_TOTAL   = 228.0;
const CURRENT_SEASON     = "2025-26";

// ── NBA abbreviation overrides (DK → standard) ──────────────
const DK_OVERRIDES: Record<string, string> = {
  GS: "GSW", SA: "SAS", NO: "NOP", NY: "NYK",
  PHO: "PHX", OKL: "OKC", UTH: "UTA",
};

// ── CSV Parsers ───────────────────────────────────────────────

function parseDkCsv(content: string): Array<{
  name: string; dkId: number; teamAbbrev: string; eligiblePositions: string;
  salary: number; gameInfo: string; avgFptsDk: number | null;
}> {
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
): number | null {
  const avgMinutes = player.avgMinutes ?? 0;
  if (avgMinutes < 10) return null;

  const ppg       = player.ppg       ?? 0;
  const rpg       = player.rpg       ?? 0;
  const apg       = player.apg       ?? 0;
  const spg       = player.spg       ?? 0;
  const bpg       = player.bpg       ?? 0;
  const tovpg     = player.tovpg     ?? 0;
  const threefgm  = player.threefgmPg ?? 0;
  const ddRate    = player.ddRate    ?? 0;

  const gamePace     = (teamPace + oppPace) / 2;
  const paceFactor   = gamePace / LEAGUE_AVG_PACE;
  const totalFactor  = vegasTotal ? vegasTotal / LEAGUE_AVG_TOTAL : 1.0;
  const combinedEnv  = paceFactor * 0.4 + totalFactor * 0.6;
  const defFactor    = oppDefRtg / LEAGUE_AVG_DEF_RTG;  // higher DefRtg = easier scoring

  const fpts = (
    ppg    * defFactor   * 1.0
    + rpg  * combinedEnv * 1.25
    + apg  * defFactor   * 1.5
    + spg  * combinedEnv * 2.0
    + bpg  * combinedEnv * 2.0
    - tovpg * combinedEnv * 0.5
    + threefgm * 0.5
    + ddRate   * 1.5
  );
  return Math.round(fpts * 100) / 100;
}

function computeLeverage(
  ourProj: number,
  projOwnPct: number,
  spg = 0,
  bpg = 0,
  contrarianFactor = 0.7,
): number {
  const ownFraction  = Math.max(0, Math.min(1, projOwnPct / 100));
  const ceilingBonus = 1.0 + spg * 0.05 + bpg * 0.04;
  return Math.round(ourProj * Math.pow(1 - ownFraction, contrarianFactor) * ceilingBonus * 1000) / 1000;
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
  ok: boolean;
  message: string;
  playerCount?: number;
  matchRate?: number;
}> {
  const dkFile = formData.get("dkFile") as File | null;
  const lsFile = formData.get("lsFile") as File | null;
  if (!dkFile) return { ok: false, message: "DK CSV required" };

  const dkContent = await dkFile.text();
  const lsContent = lsFile ? await lsFile.text() : null;

  const dkPlayers_ = parseDkCsv(dkContent);
  if (dkPlayers_.length === 0) return { ok: false, message: "No players parsed from DK CSV" };

  const lsMap: Map<string, LinestarEntry> = lsContent
    ? parseLinestarCsv(lsContent)
    : new Map();

  // Determine slate date from first game_info
  let slateDate = "";
  for (const p of dkPlayers_) {
    const d = parseSlateDate(p.gameInfo);
    if (d) { slateDate = d; break; }
  }
  if (!slateDate) slateDate = new Date().toISOString().slice(0, 10);

  // Count games
  const gameCount = new Set(dkPlayers_.map((p) => p.gameInfo.split(" ")[0])).size;

  // Upsert slate
  const [slate] = await db
    .insert(dkSlates)
    .values({ slateDate, gameCount })
    .onConflictDoUpdate({ target: dkSlates.slateDate, set: { gameCount } })
    .returning({ id: dkSlates.id });
  const slateId = slate.id;

  // Load DB context: teams, matchups, team stats, player stats
  const teamRows = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
  const abbrevToId = new Map(teamRows.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

  const matchupRows = await db
    .select()
    .from(nbaMatchups)
    .where(eq(nbaMatchups.gameDate, slateDate));
  const matchupByTeam = new Map<number, typeof matchupRows[0]>();
  for (const m of matchupRows) {
    if (m.homeTeamId) matchupByTeam.set(m.homeTeamId, m);
    if (m.awayTeamId) matchupByTeam.set(m.awayTeamId, m);
  }

  const teamStatRows = await db
    .select()
    .from(nbaTeamStats)
    .where(eq(nbaTeamStats.season, CURRENT_SEASON));
  const statsByTeam = new Map(teamStatRows.map((r) => [r.teamId, r]));

  const playerStatRows = await db
    .select()
    .from(nbaPlayerStats)
    .where(eq(nbaPlayerStats.season, CURRENT_SEASON));
  const playersByTeam = new Map<number, typeof playerStatRows>();
  for (const ps of playerStatRows) {
    const arr = playersByTeam.get(ps.teamId) ?? [];
    arr.push(ps);
    playersByTeam.set(ps.teamId, arr);
  }

  let lsMatched = 0;
  let projComputed = 0;
  const insertValues = [];

  for (const p of dkPlayers_) {
    // Team lookup
    const canonical = DK_OVERRIDES[p.teamAbbrev] ?? p.teamAbbrev;
    const teamId    = abbrevToId.get(canonical) ?? null;
    const matchup   = teamId ? matchupByTeam.get(teamId) ?? null : null;
    const matchupId = matchup?.id ?? null;

    // LineStar merge
    const ls = findLinestarMatch(p.name, p.salary, lsMap);
    if (ls) lsMatched++;

    // Our projection
    let ourProj: number | null = null;
    let ourLeverage: number | null = null;
    let spgForLev = 0, bpgForLev = 0;

    if (teamId && matchup) {
      const teamStat = statsByTeam.get(teamId);
      const oppId    = matchup.homeTeamId === teamId ? matchup.awayTeamId : matchup.homeTeamId;
      const oppStat  = oppId ? statsByTeam.get(oppId) : null;

      // Fuzzy player name match
      const candidates = playersByTeam.get(teamId) ?? [];
      let bestPlayer: typeof playerStatRows[0] | null = null;
      let bestDist = 4;
      for (const ps of candidates) {
        const d = levenshtein(p.name.toLowerCase(), ps.name.toLowerCase());
        if (d < bestDist) { bestDist = d; bestPlayer = ps; }
      }

      if (bestPlayer && teamStat && oppStat) {
        ourProj = computeOurProjection(
          bestPlayer,
          teamStat.pace ?? LEAGUE_AVG_PACE,
          oppStat.pace ?? LEAGUE_AVG_PACE,
          oppStat.defRtg ?? LEAGUE_AVG_DEF_RTG,
          matchup.vegasTotal,
        );
        spgForLev = bestPlayer.spg ?? 0;
        bpgForLev = bestPlayer.bpg ?? 0;
        if (ourProj) projComputed++;
      }
    }

    const projForLev = ls?.isOut ? 0 : (ourProj ?? ls?.linestarProj ?? 0);
    if (projForLev && ls?.projOwnPct != null) {
      ourLeverage = computeLeverage(projForLev, ls.projOwnPct, spgForLev, bpgForLev);
    }

    insertValues.push({
      slateId,
      dkPlayerId:        p.dkId,
      name:              p.name,
      teamAbbrev:        p.teamAbbrev,
      teamId,
      matchupId,
      eligiblePositions: p.eligiblePositions,
      salary:            p.salary,
      gameInfo:          p.gameInfo,
      avgFptsDk:         p.avgFptsDk,
      linestarProj:      ls?.linestarProj ?? null,
      projOwnPct:        ls?.projOwnPct  ?? null,
      ourProj,
      ourLeverage,
      isOut:             ls?.isOut ?? false,
    });
  }

  // Upsert in batches of 50
  for (let i = 0; i < insertValues.length; i += 50) {
    const batch = insertValues.slice(i, i + 50);
    await db
      .insert(dkPlayers)
      .values(batch)
      .onConflictDoUpdate({
        target: [dkPlayers.slateId, dkPlayers.dkPlayerId],
        set: {
          linestarProj:      sql`EXCLUDED.linestar_proj`,
          projOwnPct:        sql`EXCLUDED.proj_own_pct`,
          ourProj:           sql`EXCLUDED.our_proj`,
          ourLeverage:       sql`EXCLUDED.our_leverage`,
          isOut:             sql`EXCLUDED.is_out`,
          avgFptsDk:         sql`EXCLUDED.avg_fpts_dk`,
          eligiblePositions: sql`EXCLUDED.eligible_positions`,
          gameInfo:          sql`EXCLUDED.game_info`,
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
      t.logo_url AS "teamLogo", t.name AS "teamName"
    FROM dk_players dp
    LEFT JOIN teams t ON t.team_id = dp.team_id
    WHERE dp.slate_id = ${slateId}
  `);

  const pool: OptimizerPlayer[] = rows.rows.filter((p) =>
    gameFilter.length === 0 || (p.matchupId != null && gameFilter.includes(p.matchupId))
  );

  try {
    const lineups = optimizeLineups(pool, settings);
    return { ok: true, lineups };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

export async function saveLineups(
  slateId: number,
  lineups: GeneratedLineup[],
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
