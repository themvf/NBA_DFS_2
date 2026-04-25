import { db } from ".";
import { ensureDkPlayerPropColumns, ensureProjectionExperimentTables, ensureAnalyticsColumns, ensureOwnershipExperimentTables, ensureMlbBlowupTrackingTables, ensureMlbHomerunTrackingTables, ensureOddsHistoryTables } from "./ensure-schema";
import { teams, nbaTeamStats, nbaPlayerStats, nbaMatchups, dkSlates, dkPlayers, dkLineups, mlbTeams, mlbTeamStats, mlbMatchups } from "./schema";
import { eq, desc, sql, gte, and } from "drizzle-orm";
const CURRENT_SEASON = "2025-26";

type SolverModel = {
  optimize: string;
  opType: "max" | "min";
  constraints: Record<string, { min?: number; max?: number; equal?: number }>;
  variables: Record<string, Record<string, number>>;
  binaries: Record<string, number>;
};

type SolverResult = Record<string, number> & { feasible: boolean; result: number };

// ── Sport discriminator ──────────────────────────────────────
// Add new sports here as the project expands.
export type Sport = "nba" | "mlb";

// ── DFS Player Pool ──────────────────────────────────────────

export type DkPlayerRow = {
  id: number;
  slateId: number;
  dkPlayerId: number;
  name: string;
  teamAbbrev: string;
  teamId: number | null;
  matchupId: number | null;
  eligiblePositions: string;
  salary: number;
  gameInfo: string | null;
  avgFptsDk: number | null;
  linestarProj: number | null;
  linestarOwnPct?: number | null;
  projOwnPct: number | null;
  ourProj: number | null;
  liveProj: number | null;
  expectedHr: number | null;
  hrProb1Plus: number | null;
  ourOwnPct: number | null;
  ourLeverage: number | null;
  liveOwnPct: number | null;
  liveLeverage: number | null;
  projFloor: number | null;
  projCeiling: number | null;
  boomRate: number | null;
  propPts: number | null;
  propPtsPrice: number | null;
  propPtsBook: string | null;
  propReb: number | null;
  propRebPrice: number | null;
  propRebBook: string | null;
  propAst: number | null;
  propAstPrice: number | null;
  propAstBook: string | null;
  propBlk: number | null;
  propBlkPrice: number | null;
  propBlkBook: string | null;
  propStl: number | null;
  propStlPrice: number | null;
  propStlBook: string | null;
  modelPoints: number | null;
  marketPoints: number | null;
  blendPoints: number | null;
  modelProj: number | null;
  marketProj: number | null;
  blendProj: number | null;
  dkInStartingLineup: boolean | null;
  dkStartingLineupOrder: number | null;
  dkTeamLineupConfirmed: boolean | null;
  isOut: boolean | null;
  actualFpts: number | null;
  actualOwnPct: number | null;
  // Joined
  teamName: string | null;
  teamLogo: string | null;
  vegasTotal: number | null;
  homeWinProb: number | null;
  homeMl: number | null;
  awayMl: number | null;
  homeTeamId: number | null;
  awayTeamId: number | null;
  homeImplied: number | null;
  awayImplied: number | null;
  slateDate: string | null;
  sport: string | null;
};

export type DfsPagePlayerRow = Pick<
  DkPlayerRow,
  | "id"
  | "slateId"
  | "dkPlayerId"
  | "name"
  | "teamAbbrev"
  | "teamId"
  | "matchupId"
  | "eligiblePositions"
  | "salary"
  | "gameInfo"
  | "avgFptsDk"
  | "linestarProj"
  | "linestarOwnPct"
  | "projOwnPct"
  | "ourProj"
  | "liveProj"
  | "expectedHr"
  | "hrProb1Plus"
  | "ourOwnPct"
  | "ourLeverage"
  | "liveOwnPct"
  | "liveLeverage"
  | "projCeiling"
  | "boomRate"
  | "propPts"
  | "propPtsPrice"
  | "propPtsBook"
  | "propReb"
  | "propRebPrice"
  | "propRebBook"
  | "propAst"
  | "propAstPrice"
  | "propAstBook"
  | "propBlk"
  | "propBlkPrice"
  | "propBlkBook"
  | "propStl"
  | "propStlPrice"
  | "propStlBook"
  | "modelPoints"
  | "marketPoints"
  | "blendPoints"
  | "modelProj"
  | "marketProj"
  | "blendProj"
  | "dkInStartingLineup"
  | "dkStartingLineupOrder"
  | "dkTeamLineupConfirmed"
  | "isOut"
  | "teamName"
  | "teamLogo"
  | "vegasTotal"
  | "homeMl"
  | "awayMl"
  | "homeTeamId"
  | "homeImplied"
  | "awayImplied"
>;

export async function getDfsPagePlayers(sport: Sport = "nba"): Promise<DfsPagePlayerRow[]> {
  await ensureDkPlayerPropColumns();
  await ensureProjectionExperimentTables();

  if (sport === "mlb") {
    const result = await db.execute<DfsPagePlayerRow>(sql`
      SELECT
        dp.id,
        dp.slate_id           AS "slateId",
        dp.dk_player_id       AS "dkPlayerId",
        dp.name,
        dp.team_abbrev        AS "teamAbbrev",
        dp.mlb_team_id        AS "teamId",
        dp.matchup_id         AS "matchupId",
        dp.eligible_positions AS "eligiblePositions",
        dp.salary,
        dp.game_info          AS "gameInfo",
        dp.avg_fpts_dk        AS "avgFptsDk",
        dp.linestar_proj      AS "linestarProj",
        dp.linestar_own_pct   AS "linestarOwnPct",
        dp.proj_own_pct       AS "projOwnPct",
        dp.our_proj           AS "ourProj",
        NULL::REAL            AS "liveProj",
        dp.expected_hr        AS "expectedHr",
        dp.hr_prob_1plus      AS "hrProb1Plus",
        dp.our_own_pct        AS "ourOwnPct",
        dp.our_leverage       AS "ourLeverage",
        NULL::REAL            AS "liveOwnPct",
        NULL::REAL            AS "liveLeverage",
        dp.proj_ceiling       AS "projCeiling",
        dp.boom_rate          AS "boomRate",
        dp.prop_pts           AS "propPts",
        dp.prop_pts_price     AS "propPtsPrice",
        dp.prop_pts_book      AS "propPtsBook",
        dp.prop_reb           AS "propReb",
        dp.prop_reb_price     AS "propRebPrice",
        dp.prop_reb_book      AS "propRebBook",
        dp.prop_ast           AS "propAst",
        dp.prop_ast_price     AS "propAstPrice",
        dp.prop_ast_book      AS "propAstBook",
        dp.prop_blk           AS "propBlk",
        dp.prop_blk_price     AS "propBlkPrice",
        dp.prop_blk_book      AS "propBlkBook",
        dp.prop_stl           AS "propStl",
        dp.prop_stl_price     AS "propStlPrice",
        dp.prop_stl_book      AS "propStlBook",
        NULL::REAL            AS "modelPoints",
        NULL::REAL            AS "marketPoints",
        NULL::REAL            AS "blendPoints",
        NULL::REAL            AS "modelProj",
        NULL::REAL            AS "marketProj",
        NULL::REAL            AS "blendProj",
        dp.dk_in_starting_lineup AS "dkInStartingLineup",
        dp.dk_starting_lineup_order AS "dkStartingLineupOrder",
        dp.dk_team_lineup_confirmed AS "dkTeamLineupConfirmed",
        dp.is_out             AS "isOut",
        mt.name               AS "teamName",
        mt.logo_url           AS "teamLogo",
        mm.vegas_total        AS "vegasTotal",
        mm.home_ml            AS "homeMl",
        mm.away_ml            AS "awayMl",
        mm.home_team_id       AS "homeTeamId",
        mm.home_implied       AS "homeImplied",
        mm.away_implied       AS "awayImplied"
      FROM dk_players dp
      INNER JOIN dk_slates ds ON ds.id = dp.slate_id
      LEFT JOIN mlb_teams mt ON mt.team_id = dp.mlb_team_id
      LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
      WHERE ds.id = (
        SELECT id FROM dk_slates WHERE sport = 'mlb'
        ORDER BY slate_date DESC, id DESC LIMIT 1
      )
      ORDER BY dp.our_leverage DESC NULLS LAST, dp.our_proj DESC NULLS LAST
    `);
    return result.rows;
  }

  // NBA (default)
  const result = await db.execute<DfsPagePlayerRow>(sql`
    SELECT
      dp.id,
      dp.slate_id          AS "slateId",
      dp.dk_player_id      AS "dkPlayerId",
      dp.name,
      dp.team_abbrev       AS "teamAbbrev",
      dp.team_id           AS "teamId",
      dp.matchup_id        AS "matchupId",
      dp.eligible_positions AS "eligiblePositions",
      dp.salary,
      dp.game_info         AS "gameInfo",
      dp.avg_fpts_dk       AS "avgFptsDk",
      dp.linestar_proj     AS "linestarProj",
      dp.linestar_own_pct  AS "linestarOwnPct",
      dp.proj_own_pct      AS "projOwnPct",
      COALESCE(proj.model_proj_fpts, dp.our_proj) AS "ourProj",
      COALESCE(dp.live_proj, proj.final_proj_fpts, dp.our_proj, dp.linestar_proj) AS "liveProj",
      NULL::REAL           AS "expectedHr",
      NULL::REAL           AS "hrProb1Plus",
      dp.our_own_pct       AS "ourOwnPct",
      dp.our_leverage      AS "ourLeverage",
      COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "liveOwnPct",
      COALESCE(dp.live_leverage, dp.our_leverage) AS "liveLeverage",
      dp.proj_ceiling      AS "projCeiling",
      dp.boom_rate         AS "boomRate",
      dp.prop_pts          AS "propPts",
      dp.prop_pts_price    AS "propPtsPrice",
      dp.prop_pts_book     AS "propPtsBook",
      dp.prop_reb          AS "propReb",
      dp.prop_reb_price    AS "propRebPrice",
      dp.prop_reb_book     AS "propRebBook",
      dp.prop_ast          AS "propAst",
      dp.prop_ast_price    AS "propAstPrice",
      dp.prop_ast_book     AS "propAstBook",
      dp.prop_blk          AS "propBlk",
      dp.prop_blk_price    AS "propBlkPrice",
      dp.prop_blk_book     AS "propBlkBook",
      dp.prop_stl          AS "propStl",
      dp.prop_stl_price    AS "propStlPrice",
      dp.prop_stl_book     AS "propStlBook",
      proj.model_points    AS "modelPoints",
      proj.market_points   AS "marketPoints",
      proj.blend_points    AS "blendPoints",
      proj.model_proj_fpts AS "modelProj",
      proj.market_proj_fpts AS "marketProj",
      proj.final_proj_fpts AS "blendProj",
      NULL::BOOLEAN AS "dkInStartingLineup",
      NULL::INTEGER AS "dkStartingLineupOrder",
      NULL::BOOLEAN AS "dkTeamLineupConfirmed",
      dp.is_out            AS "isOut",
      t.name               AS "teamName",
      t.logo_url           AS "teamLogo",
      m.vegas_total        AS "vegasTotal",
      m.home_ml            AS "homeMl",
      m.away_ml            AS "awayMl",
      m.home_team_id       AS "homeTeamId",
      NULL::DOUBLE PRECISION AS "homeImplied",
      NULL::DOUBLE PRECISION AS "awayImplied"
    FROM dk_players dp
    INNER JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN teams t ON t.team_id = dp.team_id
    LEFT JOIN nba_matchups m ON m.id = dp.matchup_id
    LEFT JOIN LATERAL (
      SELECT
        CASE
          WHEN pps.model_stats_json ? 'pts' THEN (pps.model_stats_json->>'pts')::REAL
          ELSE NULL
        END AS model_points,
        CASE
          WHEN pps.market_stats_json ? 'pts' THEN (pps.market_stats_json->>'pts')::REAL
          ELSE NULL
        END AS market_points,
        CASE
          WHEN COALESCE(pps.model_weight, 0) + COALESCE(pps.market_weight, 0) <= 0 THEN
            COALESCE(
              CASE WHEN pps.market_stats_json ? 'pts' THEN (pps.market_stats_json->>'pts')::REAL ELSE NULL END,
              CASE WHEN pps.model_stats_json ? 'pts' THEN (pps.model_stats_json->>'pts')::REAL ELSE NULL END
            )
          ELSE ROUND((
            COALESCE(CASE WHEN pps.model_stats_json ? 'pts' THEN (pps.model_stats_json->>'pts')::REAL ELSE NULL END, 0) * COALESCE(pps.model_weight, 0)
            + COALESCE(CASE WHEN pps.market_stats_json ? 'pts' THEN (pps.market_stats_json->>'pts')::REAL ELSE NULL END, 0) * COALESCE(pps.market_weight, 0)
          )::NUMERIC / NULLIF(COALESCE(pps.model_weight, 0) + COALESCE(pps.market_weight, 0), 0)::NUMERIC, 2)::REAL
        END AS blend_points,
        pps.model_proj_fpts,
        pps.market_proj_fpts,
        pps.final_proj_fpts
      FROM projection_runs pr
      INNER JOIN projection_player_snapshots pps
        ON pps.run_id = pr.id
       AND pps.dk_player_id = dp.dk_player_id
      WHERE pr.slate_id = dp.slate_id
        AND pr.sport = 'nba'
      ORDER BY pr.created_at DESC, pr.id DESC
      LIMIT 1
    ) proj ON true
    WHERE ds.id = (
      SELECT id FROM dk_slates WHERE sport = 'nba'
      ORDER BY slate_date DESC, id DESC LIMIT 1
    )
    ORDER BY COALESCE(dp.live_leverage, dp.our_leverage) DESC NULLS LAST,
             COALESCE(dp.live_proj, proj.final_proj_fpts, dp.our_proj) DESC NULLS LAST
  `);
  return result.rows;
}

// ── Slate Info ───────────────────────────────────────────────

export async function getLatestSlateInfo(sport: Sport = "nba"): Promise<{
  slateDate: string;
  gameCount: number | null;
  contestType: string | null;
  fieldSize: number | null;
  contestFormat: string | null;
} | null> {
  const rows = await db
    .select({
      slateDate: dkSlates.slateDate,
      gameCount: dkSlates.gameCount,
      contestType: dkSlates.contestType,
      fieldSize: dkSlates.fieldSize,
      contestFormat: dkSlates.contestFormat,
    })
    .from(dkSlates)
    .where(eq(dkSlates.sport, sport))
    .orderBy(desc(dkSlates.slateDate), desc(dkSlates.id))
    .limit(1);
  return rows[0] ?? null;
}

// ── MLB Game Environment Cards ───────────────────────────────

export type MlbGameEnvironmentCard = {
  matchupId: number;
  gameId: string | null;
  gameDate: string;
  homeTeamId: number | null;
  homeAbbrev: string | null;
  homeName: string | null;
  homeLogo: string | null;
  awayTeamId: number | null;
  awayAbbrev: string | null;
  awayName: string | null;
  awayLogo: string | null;
  vegasTotal: number | null;
  homeImplied: number | null;
  awayImplied: number | null;
  homeMl: number | null;
  awayMl: number | null;
  ballpark: string | null;
  weatherTemp: number | null;
  windSpeed: number | null;
  windDirection: string | null;
  homeSpName: string | null;
  homeSpHand: string | null;
  homeSpKPer9: number | null;
  homeSpXfip: number | null;
  homeSpEra: number | null;
  awaySpName: string | null;
  awaySpHand: string | null;
  awaySpKPer9: number | null;
  awaySpXfip: number | null;
  awaySpEra: number | null;
};

export async function getMlbGameEnvironmentCards(slateDate: string | null): Promise<MlbGameEnvironmentCard[]> {
  if (!slateDate) return [];
  await ensureAnalyticsColumns();
  const result = await db.execute<MlbGameEnvironmentCard>(sql`
    WITH latest_pitcher AS (
      SELECT DISTINCT ON (player_id)
        player_id, name, hand, k_per_9, xfip, era
      FROM mlb_pitcher_stats
      ORDER BY player_id, season DESC, fetched_at DESC, id DESC
    ),
    latest_pitcher_by_name AS (
      SELECT DISTINCT ON (LOWER(name))
        LOWER(name) AS name_key,
        name,
        hand,
        k_per_9,
        xfip,
        era
      FROM mlb_pitcher_stats
      ORDER BY LOWER(name), season DESC, fetched_at DESC, id DESC
    )
    SELECT
      mm.id                 AS "matchupId",
      mm.game_id            AS "gameId",
      mm.game_date::text    AS "gameDate",
      mm.home_team_id       AS "homeTeamId",
      home.abbreviation     AS "homeAbbrev",
      home.name             AS "homeName",
      home.logo_url         AS "homeLogo",
      mm.away_team_id       AS "awayTeamId",
      away.abbreviation     AS "awayAbbrev",
      away.name             AS "awayName",
      away.logo_url         AS "awayLogo",
      mm.vegas_total        AS "vegasTotal",
      mm.home_implied       AS "homeImplied",
      mm.away_implied       AS "awayImplied",
      mm.home_ml            AS "homeMl",
      mm.away_ml            AS "awayMl",
      mm.ballpark           AS "ballpark",
      mm.weather_temp       AS "weatherTemp",
      mm.wind_speed         AS "windSpeed",
      mm.wind_direction     AS "windDirection",
      COALESCE(mm.home_sp_name, hsp_id.name, hsp_name.name) AS "homeSpName",
      COALESCE(hsp_id.hand, hsp_name.hand) AS "homeSpHand",
      COALESCE(hsp_id.k_per_9, hsp_name.k_per_9) AS "homeSpKPer9",
      COALESCE(hsp_id.xfip, hsp_name.xfip) AS "homeSpXfip",
      COALESCE(hsp_id.era, hsp_name.era) AS "homeSpEra",
      COALESCE(mm.away_sp_name, asp_id.name, asp_name.name) AS "awaySpName",
      COALESCE(asp_id.hand, asp_name.hand) AS "awaySpHand",
      COALESCE(asp_id.k_per_9, asp_name.k_per_9) AS "awaySpKPer9",
      COALESCE(asp_id.xfip, asp_name.xfip) AS "awaySpXfip",
      COALESCE(asp_id.era, asp_name.era) AS "awaySpEra"
    FROM mlb_matchups mm
    LEFT JOIN mlb_teams home ON home.team_id = mm.home_team_id
    LEFT JOIN mlb_teams away ON away.team_id = mm.away_team_id
    LEFT JOIN latest_pitcher hsp_id ON hsp_id.player_id = mm.home_sp_id
    LEFT JOIN latest_pitcher asp_id ON asp_id.player_id = mm.away_sp_id
    LEFT JOIN latest_pitcher_by_name hsp_name ON hsp_name.name_key = LOWER(mm.home_sp_name)
    LEFT JOIN latest_pitcher_by_name asp_name ON asp_name.name_key = LOWER(mm.away_sp_name)
    WHERE mm.game_date = ${slateDate}::date
    ORDER BY mm.vegas_total DESC NULLS LAST, mm.id ASC
  `);
  return result.rows;
}

export type MlbHomerunCandidate = {
  id: number;
  dkPlayerId: number;
  slateId: number;
  slateDate: string;
  contestType: string | null;
  gameCount: number | null;
  name: string;
  teamAbbrev: string;
  teamName: string | null;
  teamLogo: string | null;
  opponentAbbrev: string | null;
  eligiblePositions: string;
  salary: number;
  gameInfo: string | null;
  battingOrder: number | null;
  lineupConfirmed: boolean | null;
  isHome: boolean | null;
  expectedHr: number | null;
  hrProb1Plus: number | null;
  hitterHrPg: number | null;
  hitterIso: number | null;
  hitterSlug: number | null;
  hitterPaPg: number | null;
  hitterWrcPlus: number | null;
  hitterSplitWrcPlus: number | null;
  teamTotal: number | null;
  vegasTotal: number | null;
  ballpark: string | null;
  parkHrFactor: number | null;
  weatherTemp: number | null;
  windSpeed: number | null;
  windDirection: string | null;
  opposingPitcherName: string | null;
  opposingPitcherHand: string | null;
  opposingPitcherGames: number | null;
  opposingPitcherIpPg: number | null;
  opposingPitcherHrPer9: number | null;
  opposingPitcherHrFbPct: number | null;
  opposingPitcherKPer9: number | null;
  opposingPitcherBbPer9: number | null;
  opposingPitcherFip: number | null;
  opposingPitcherXfip: number | null;
  opposingPitcherEra: number | null;
  opposingPitcherWhip: number | null;
  marketHrLine: number | null;
  marketHrPrice: number | null;
  marketHrBook: string | null;
  marketHrImpliedPct: number | null;
  hrEdgePct: number | null;
  marketCapturedAt: string | null;
};

export type MlbHomerunBoardView = "likely" | "edge" | "leverage" | "longshots" | "features";

export type MlbHomerunBoard = {
  slateId: number | null;
  dkDraftGroupId: number | null;
  slateDate: string | null;
  requestedDate: string | null;
  requestedDkId: number | null;
  dkIdKind: "draftGroup" | "contest" | "entry" | null;
  dkIdError: string | null;
  view: MlbHomerunBoardView;
  contestType: string | null;
  gameCount: number | null;
  totalQualified: number;
  latestMarketCapturedAt: string | null;
  candidates: MlbHomerunCandidate[];
  scatterCandidates: MlbHomerunCandidate[];
};

export type MlbHomerunBoardParams = {
  date?: string | null;
  dkId?: number | string | null;
  view?: string | null;
};

const POSTGRES_INT_MAX = 2147483647;

function cleanPositiveInt(value: unknown): number | null {
  if (value == null || value === "") return null;
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

function normalizeHomerunBoardView(value: unknown): MlbHomerunBoardView {
  if (value === "edge" || value === "leverage" || value === "longshots" || value === "features") return value;
  return "likely";
}

function numberOrNegativeInfinity(value: number | null | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
}

function numberOrPositiveInfinity(value: number | null | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? value : Number.POSITIVE_INFINITY;
}

function hasMarketRank(candidate: MlbHomerunCandidate): number {
  return candidate.marketHrImpliedPct == null ? 1 : 0;
}

function compareHomerunCandidates(a: MlbHomerunCandidate, b: MlbHomerunCandidate, view: MlbHomerunBoardView): number {
  const byLikely =
    numberOrNegativeInfinity(b.hrProb1Plus) - numberOrNegativeInfinity(a.hrProb1Plus)
    || numberOrNegativeInfinity(b.expectedHr) - numberOrNegativeInfinity(a.expectedHr)
    || a.name.localeCompare(b.name);

  if (view === "likely" || view === "features") return byLikely;

  if (view === "edge") {
    return hasMarketRank(a) - hasMarketRank(b)
      || numberOrNegativeInfinity(b.hrEdgePct) - numberOrNegativeInfinity(a.hrEdgePct)
      || byLikely;
  }

  if (view === "leverage") {
    return hasMarketRank(a) - hasMarketRank(b)
      || numberOrNegativeInfinity(b.hrEdgePct) - numberOrNegativeInfinity(a.hrEdgePct)
      || numberOrPositiveInfinity(a.marketHrImpliedPct) - numberOrPositiveInfinity(b.marketHrImpliedPct)
      || byLikely;
  }

  return hasMarketRank(a) - hasMarketRank(b)
    || numberOrPositiveInfinity(a.marketHrImpliedPct) - numberOrPositiveInfinity(b.marketHrImpliedPct)
    || numberOrNegativeInfinity(b.hrEdgePct) - numberOrNegativeInfinity(a.hrEdgePct)
    || byLikely;
}

async function resolveDraftGroupIdFromContestId(contestId: number): Promise<number | null> {
  try {
    const resp = await fetch(`https://api.draftkings.com/contests/v1/contests/${contestId}`, {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
      },
      cache: "no-store",
    });
    if (!resp.ok) return null;
    const data = await resp.json() as { contestDetail?: { draftGroupId?: unknown } };
    return cleanPositiveInt(data.contestDetail?.draftGroupId);
  } catch {
    return null;
  }
}

function parseDkLobbyDate(value: unknown): string | null {
  const raw = String(value ?? "");
  const match = raw.match(/\/Date\((\d+)\)\//);
  const millis = match ? Number(match[1]) : Number(raw);
  if (!Number.isFinite(millis)) return null;
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(millis));
}

async function resolveMlbHomeRunDraftGroupFromLobby(date: string | null): Promise<number | null> {
  try {
    const resp = await fetch("https://www.draftkings.com/lobby/getcontests?sport=MLB", {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
      },
      cache: "no-store",
    });
    if (!resp.ok) return null;
    const data = await resp.json() as { Contests?: Array<Record<string, unknown>> };
    const contests = Array.isArray(data.Contests) ? data.Contests : [];
    const homeRunContests = contests
      .filter((contest) => {
        const name = String(contest.n ?? contest.name ?? "").toLowerCase();
        const gameType = String(contest.gameType ?? "").toLowerCase();
        const gameTypeId = Number(contest.gameTypeId ?? 0);
        return (
          gameTypeId === 346
          || gameType.includes("single stat - home runs")
          || (name.includes("single stat") && name.includes("home runs"))
        );
      })
      .filter((contest) => !date || parseDkLobbyDate(contest.sd) === date)
      .sort((a, b) => Number(b.po ?? 0) - Number(a.po ?? 0));

    for (const contest of homeRunContests) {
      const draftGroupId = cleanPositiveInt(contest.dg ?? contest.draftGroupId);
      if (draftGroupId != null && draftGroupId <= POSTGRES_INT_MAX) return draftGroupId;
    }
    return null;
  } catch {
    return null;
  }
}

export async function getMlbHomerunBoard(params: MlbHomerunBoardParams | string | null = {}): Promise<MlbHomerunBoard> {
  await ensureDkPlayerPropColumns();
  await ensureOddsHistoryTables();

  const normalizedParams = typeof params === "string" || params == null ? { date: params } : params;
  const requestedDkId = cleanPositiveInt(normalizedParams.dkId);
  const view = normalizeHomerunBoardView(normalizedParams.view);
  const date = normalizedParams.date ?? null;
  const requestedDate = date && /^\d{4}-\d{2}-\d{2}$/.test(date) ? date : null;
  let dkIdKind: MlbHomerunBoard["dkIdKind"] = null;
  let resolvedDkDraftGroupId: number | null = null;
  if (requestedDkId != null) {
    const contestDraftGroupId = await resolveDraftGroupIdFromContestId(requestedDkId);
    if (contestDraftGroupId != null) {
      dkIdKind = "contest";
      resolvedDkDraftGroupId = contestDraftGroupId;
    } else if (requestedDkId <= POSTGRES_INT_MAX) {
      dkIdKind = "draftGroup";
      resolvedDkDraftGroupId = requestedDkId;
    } else {
      dkIdKind = "entry";
      resolvedDkDraftGroupId = await resolveMlbHomeRunDraftGroupFromLobby(requestedDate);
    }
  }
  const dkIdError = requestedDkId != null && resolvedDkDraftGroupId == null
    ? `DraftKings ID ${requestedDkId} could not be resolved to a Home Runs draft group for this date.`
    : null;

  if (requestedDkId != null && resolvedDkDraftGroupId == null) {
    return {
      slateId: null,
      dkDraftGroupId: null,
      slateDate: requestedDate,
      requestedDate,
      requestedDkId,
      dkIdKind,
      dkIdError,
      view,
      contestType: null,
      gameCount: null,
      totalQualified: 0,
      latestMarketCapturedAt: null,
      candidates: [],
      scatterCandidates: [],
    };
  }

  const result = await db.execute<MlbHomerunCandidate & { dkDraftGroupId: number | null; totalQualified: number }>(sql`
    WITH selected_slate AS (
      SELECT id, dk_draft_group_id, slate_date, contest_type, game_count
      FROM dk_slates
      WHERE sport = 'mlb'
        AND (${resolvedDkDraftGroupId}::int IS NULL OR dk_draft_group_id = ${resolvedDkDraftGroupId}::int)
        AND (${resolvedDkDraftGroupId}::int IS NOT NULL OR ${requestedDate}::text IS NULL OR slate_date = ${requestedDate}::date)
        AND (
          ${requestedDate}::text IS NOT NULL
          OR ${resolvedDkDraftGroupId}::int IS NOT NULL
          OR EXISTS (
            SELECT 1
            FROM dk_players slate_player
            WHERE slate_player.slate_id = dk_slates.id
              AND slate_player.hr_prob_1plus IS NOT NULL
              AND COALESCE(slate_player.is_out, FALSE) = FALSE
              AND NOT (
                slate_player.eligible_positions ILIKE '%SP%'
                OR slate_player.eligible_positions ILIKE '%RP%'
            )
          )
        )
      ORDER BY
        CASE WHEN contest_type ILIKE '%homerun%' OR contest_type ILIKE '%home run%' THEN 0 ELSE 1 END,
        slate_date DESC,
        id DESC
      LIMIT 1
    ),
    latest_batter_by_name AS (
      SELECT DISTINCT ON (LOWER(name))
        LOWER(name) AS name_key,
        pa_pg,
        hr_pg,
        iso,
        slg,
        wrc_plus,
        wrc_plus_vs_l,
        wrc_plus_vs_r
      FROM mlb_batter_stats
      ORDER BY LOWER(name), season DESC, fetched_at DESC, id DESC
    ),
    latest_pitcher AS (
      SELECT DISTINCT ON (player_id)
        player_id, name, hand, games, ip_pg, hr_per_9, hr_fb_pct, k_per_9, bb_per_9, fip, xfip, era, whip
      FROM mlb_pitcher_stats
      ORDER BY player_id, season DESC, fetched_at DESC, id DESC
    ),
    latest_pitcher_by_name AS (
      SELECT DISTINCT ON (LOWER(name))
        LOWER(name) AS name_key,
        name,
        hand,
        games,
        ip_pg,
        hr_per_9,
        hr_fb_pct,
        k_per_9,
        bb_per_9,
        fip,
        xfip,
        era,
        whip
      FROM mlb_pitcher_stats
      ORDER BY LOWER(name), season DESC, fetched_at DESC, id DESC
    ),
    latest_park AS (
      SELECT DISTINCT ON (team_id)
        team_id,
        hr_factor
      FROM mlb_park_factors
      ORDER BY team_id, season DESC, id DESC
    ),
    candidates AS (
      SELECT
        dp.id,
        dp.dk_player_id AS "dkPlayerId",
        dp.slate_id AS "slateId",
        ss.dk_draft_group_id AS "dkDraftGroupId",
        ss.slate_date::text AS "slateDate",
        ss.contest_type AS "contestType",
        ss.game_count AS "gameCount",
        dp.name,
        dp.team_abbrev AS "teamAbbrev",
        team.name AS "teamName",
        team.logo_url AS "teamLogo",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN away.abbreviation
          WHEN dp.mlb_team_id = mm.away_team_id THEN home.abbreviation
          ELSE NULL
        END AS "opponentAbbrev",
        dp.eligible_positions AS "eligiblePositions",
        dp.salary,
        dp.game_info AS "gameInfo",
        dp.dk_starting_lineup_order AS "battingOrder",
        dp.dk_team_lineup_confirmed AS "lineupConfirmed",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
          WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
          ELSE NULL
        END AS "isHome",
        dp.expected_hr AS "expectedHr",
        dp.hr_prob_1plus AS "hrProb1Plus",
        batter.hr_pg AS "hitterHrPg",
        batter.iso AS "hitterIso",
        batter.slg AS "hitterSlug",
        batter.pa_pg AS "hitterPaPg",
        batter.wrc_plus AS "hitterWrcPlus",
        CASE
          WHEN (
            CASE
              WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(asp_id.hand, asp_name.hand)
              WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(hsp_id.hand, hsp_name.hand)
              ELSE NULL
            END
          ) = 'L' THEN batter.wrc_plus_vs_l
          WHEN (
            CASE
              WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(asp_id.hand, asp_name.hand)
              WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(hsp_id.hand, hsp_name.hand)
              ELSE NULL
            END
          ) = 'R' THEN batter.wrc_plus_vs_r
          ELSE NULL
        END AS "hitterSplitWrcPlus",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_implied
          WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_implied
          ELSE NULL
        END AS "teamTotal",
        mm.vegas_total AS "vegasTotal",
        mm.ballpark AS "ballpark",
        park.hr_factor AS "parkHrFactor",
        mm.weather_temp AS "weatherTemp",
        mm.wind_speed AS "windSpeed",
        mm.wind_direction AS "windDirection",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(mm.away_sp_name, asp_id.name, asp_name.name)
          WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(mm.home_sp_name, hsp_id.name, hsp_name.name)
          ELSE NULL
        END AS "opposingPitcherName",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(asp_id.hand, asp_name.hand)
          WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(hsp_id.hand, hsp_name.hand)
          ELSE NULL
        END AS "opposingPitcherHand",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(asp_id.games, asp_name.games)
          WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(hsp_id.games, hsp_name.games)
          ELSE NULL
        END AS "opposingPitcherGames",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.ip_pg, asp_name.ip_pg), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.ip_pg, hsp_name.ip_pg), 0)
          ELSE NULL
        END AS "opposingPitcherIpPg",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.hr_per_9, asp_name.hr_per_9), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.hr_per_9, hsp_name.hr_per_9), 0)
          ELSE NULL
        END AS "opposingPitcherHrPer9",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.hr_fb_pct, asp_name.hr_fb_pct), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.hr_fb_pct, hsp_name.hr_fb_pct), 0)
          ELSE NULL
        END AS "opposingPitcherHrFbPct",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.k_per_9, asp_name.k_per_9), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.k_per_9, hsp_name.k_per_9), 0)
          ELSE NULL
        END AS "opposingPitcherKPer9",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.bb_per_9, asp_name.bb_per_9), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.bb_per_9, hsp_name.bb_per_9), 0)
          ELSE NULL
        END AS "opposingPitcherBbPer9",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.fip, asp_name.fip), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.fip, hsp_name.fip), 0)
          ELSE NULL
        END AS "opposingPitcherFip",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.xfip, asp_name.xfip), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.xfip, hsp_name.xfip), 0)
          ELSE NULL
        END AS "opposingPitcherXfip",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.era, asp_name.era), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.era, hsp_name.era), 0)
          ELSE NULL
        END AS "opposingPitcherEra",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.whip, asp_name.whip), 0)
          WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.whip, hsp_name.whip), 0)
          ELSE NULL
        END AS "opposingPitcherWhip",
        hrp.line AS "marketHrLine",
        hrp.price AS "marketHrPrice",
        hrp.bookmaker_title AS "marketHrBook",
        hrp.market_implied_pct AS "marketHrImpliedPct",
        CASE
          WHEN hrp.market_implied_pct IS NULL THEN NULL
          ELSE dp.hr_prob_1plus * 100.0 - hrp.market_implied_pct
        END AS "hrEdgePct",
        hrp.captured_at::text AS "marketCapturedAt"
      FROM selected_slate ss
      INNER JOIN dk_players dp ON dp.slate_id = ss.id
      LEFT JOIN mlb_teams team ON team.team_id = dp.mlb_team_id
      LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
      LEFT JOIN mlb_teams home ON home.team_id = mm.home_team_id
      LEFT JOIN mlb_teams away ON away.team_id = mm.away_team_id
      LEFT JOIN latest_batter_by_name batter ON batter.name_key = LOWER(dp.name)
      LEFT JOIN latest_pitcher hsp_id ON hsp_id.player_id = mm.home_sp_id
      LEFT JOIN latest_pitcher asp_id ON asp_id.player_id = mm.away_sp_id
      LEFT JOIN latest_pitcher_by_name hsp_name ON hsp_name.name_key = LOWER(mm.home_sp_name)
      LEFT JOIN latest_pitcher_by_name asp_name ON asp_name.name_key = LOWER(mm.away_sp_name)
      LEFT JOIN latest_park park ON park.team_id = mm.home_team_id
      LEFT JOIN LATERAL (
        SELECT
          pph.line,
          pph.price,
          pph.bookmaker_title,
          pph.captured_at,
          CASE
            WHEN pph.price > 0 THEN 100.0 / (pph.price + 100.0) * 100.0
            WHEN pph.price < 0 THEN ABS(pph.price)::double precision / (ABS(pph.price) + 100.0) * 100.0
            ELSE NULL
          END AS market_implied_pct
        FROM player_prop_history pph
        JOIN dk_slates prop_slate ON prop_slate.id = pph.slate_id
        WHERE pph.sport = 'mlb'
          AND pph.market_key = 'batter_home_runs'
          AND prop_slate.slate_date = ss.slate_date
          AND (
            pph.dk_player_id = dp.dk_player_id
            OR (
              LOWER(pph.player_name) = LOWER(dp.name)
              AND (pph.team_id = dp.mlb_team_id OR pph.team_id IS NULL OR dp.mlb_team_id IS NULL)
            )
          )
        ORDER BY
          CASE
            WHEN pph.slate_id = dp.slate_id THEN 0
            WHEN pph.dk_player_id = dp.dk_player_id THEN 1
            ELSE 2
          END,
          pph.captured_at DESC,
          pph.id DESC
        LIMIT 1
      ) hrp ON TRUE
      WHERE COALESCE(dp.is_out, FALSE) = FALSE
        AND dp.hr_prob_1plus IS NOT NULL
        AND NOT (dp.eligible_positions ILIKE '%SP%' OR dp.eligible_positions ILIKE '%RP%')
    ),
    ranked AS (
      SELECT
        candidates.*,
        COUNT(*) OVER () AS "totalQualified"
      FROM candidates
    )
    SELECT * FROM ranked
  `);

  const first = result.rows[0];
  const sortedCandidates = result.rows.map((row) => {
    const candidate = { ...row };
    delete (candidate as Partial<typeof row>).dkDraftGroupId;
    delete (candidate as Partial<typeof row>).totalQualified;
    return candidate as MlbHomerunCandidate;
  }).sort((a, b) => compareHomerunCandidates(a, b, view));
  const latestMarketCapturedAt = sortedCandidates
    .map((candidate) => candidate.marketCapturedAt)
    .filter((capturedAt): capturedAt is string => Boolean(capturedAt))
    .sort()
    .at(-1) ?? null;

  return {
    slateId: first?.slateId ?? null,
    dkDraftGroupId: first?.dkDraftGroupId ?? resolvedDkDraftGroupId,
    slateDate: first?.slateDate ?? requestedDate,
    requestedDate,
    requestedDkId,
    dkIdKind,
    dkIdError,
    view,
    contestType: first?.contestType ?? null,
    gameCount: first?.gameCount ?? null,
    totalQualified: first?.totalQualified ?? 0,
    latestMarketCapturedAt,
    candidates: sortedCandidates.slice(0, 15),
    scatterCandidates: sortedCandidates,
  };
}

// ── DFS Accuracy ─────────────────────────────────────────────

export type MlbHomerunTrackingSummary = {
  runs: number;
  slates: number;
  rows: number;
  actualHrRows: number;
  pendingHrRows: number;
  actualFptsRows: number;
  actualOwnRows: number;
  avgPredictedPct: number | null;
  hitRate: number | null;
  brierScore: number | null;
  logLoss: number | null;
  top15Rows: number;
  top15ActualHrRows: number;
  top15HitRate: number | null;
  latestVersion: string | null;
  latestSource: string | null;
  latestCapturedAt: string | null;
  latestSlateDate: string | null;
};

export type MlbHomerunCalibrationBucket = {
  bucket: string;
  rows: number;
  actualHrRows: number;
  avgPredictedPct: number | null;
  hitRate: number | null;
  brierScore: number | null;
};

export type MlbHomerunTrackingReport = {
  summary: MlbHomerunTrackingSummary;
  buckets: MlbHomerunCalibrationBucket[];
};

export async function getMlbHomerunTrackingReport(): Promise<MlbHomerunTrackingReport | null> {
  await ensureMlbHomerunTrackingTables();

  const latestRunsCte = sql`
    WITH latest_runs AS (
      SELECT DISTINCT ON (r.slate_id)
        r.id,
        r.slate_id,
        r.analysis_version,
        r.source,
        r.created_at
      FROM mlb_homerun_runs r
      JOIN dk_slates ds ON ds.id = r.slate_id
      WHERE ds.sport = 'mlb'
      ORDER BY r.slate_id, r.created_at DESC, r.id DESC
    ),
    sample AS (
      SELECT
        s.*,
        lr.analysis_version,
        lr.source,
        lr.created_at AS run_created_at,
        ds.slate_date::text AS slate_date,
        ROW_NUMBER() OVER (
          PARTITION BY s.slate_id
          ORDER BY s.hr_prob_1plus DESC NULLS LAST, s.expected_hr DESC NULLS LAST, s.name ASC
        ) AS slate_rank
      FROM mlb_homerun_player_snapshots s
      JOIN latest_runs lr ON lr.id = s.run_id
      JOIN dk_slates ds ON ds.id = s.slate_id
      WHERE COALESCE(s.is_out, FALSE) = FALSE
        AND s.hr_prob_1plus IS NOT NULL
    )
  `;

  const [summaryResult, bucketResult] = await Promise.all([
    db.execute<{
      runs: number;
      slates: number;
      rows: number;
      actualHrRows: number;
      pendingHrRows: number;
      actualFptsRows: number;
      actualOwnRows: number;
      avgPredictedPct: number | null;
      hitRate: number | null;
      brierScore: number | null;
      logLoss: number | null;
      top15Rows: number;
      top15ActualHrRows: number;
      top15HitRate: number | null;
      latestVersion: string | null;
      latestSource: string | null;
      latestCapturedAt: string | null;
      latestSlateDate: string | null;
    }>(sql`
      ${latestRunsCte}
      SELECT
        (SELECT COUNT(*)::int FROM latest_runs) AS "runs",
        COUNT(DISTINCT sample.slate_id)::int AS "slates",
        COUNT(*)::int AS "rows",
        COUNT(*) FILTER (WHERE sample.actual_hr IS NOT NULL)::int AS "actualHrRows",
        COUNT(*) FILTER (WHERE sample.actual_hr IS NULL)::int AS "pendingHrRows",
        COUNT(*) FILTER (WHERE sample.actual_fpts IS NOT NULL)::int AS "actualFptsRows",
        COUNT(*) FILTER (WHERE sample.actual_own_pct IS NOT NULL)::int AS "actualOwnRows",
        AVG(sample.hr_prob_1plus) * 100 AS "avgPredictedPct",
        AVG(CASE WHEN sample.hit_hr_1plus THEN 1.0 ELSE 0.0 END) FILTER (WHERE sample.actual_hr IS NOT NULL) * 100 AS "hitRate",
        AVG(POWER(sample.hr_prob_1plus - CASE WHEN sample.hit_hr_1plus THEN 1.0 ELSE 0.0 END, 2))
          FILTER (WHERE sample.actual_hr IS NOT NULL) AS "brierScore",
        AVG(-(
          CASE
            WHEN sample.hit_hr_1plus THEN LN(GREATEST(LEAST(sample.hr_prob_1plus, 0.999999), 0.000001))
            ELSE LN(GREATEST(LEAST(1.0 - sample.hr_prob_1plus, 0.999999), 0.000001))
          END
        )) FILTER (WHERE sample.actual_hr IS NOT NULL) AS "logLoss",
        COUNT(*) FILTER (WHERE sample.slate_rank <= 15)::int AS "top15Rows",
        COUNT(*) FILTER (WHERE sample.slate_rank <= 15 AND sample.actual_hr IS NOT NULL)::int AS "top15ActualHrRows",
        AVG(CASE WHEN sample.hit_hr_1plus THEN 1.0 ELSE 0.0 END)
          FILTER (WHERE sample.slate_rank <= 15 AND sample.actual_hr IS NOT NULL) * 100 AS "top15HitRate",
        (
          SELECT lr.analysis_version
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestVersion",
        (
          SELECT lr.source
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestSource",
        (
          SELECT lr.created_at::text
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestCapturedAt",
        MAX(sample.slate_date) AS "latestSlateDate"
      FROM sample
    `),
    db.execute<{
      bucket: string;
      rows: number;
      actualHrRows: number;
      avgPredictedPct: number | null;
      hitRate: number | null;
      brierScore: number | null;
      bucketSort: number;
    }>(sql`
      ${latestRunsCte}
      SELECT
        CASE
          WHEN sample.hr_prob_1plus < 0.05 THEN '<5%'
          WHEN sample.hr_prob_1plus < 0.10 THEN '5-10%'
          WHEN sample.hr_prob_1plus < 0.15 THEN '10-15%'
          WHEN sample.hr_prob_1plus < 0.20 THEN '15-20%'
          WHEN sample.hr_prob_1plus < 0.25 THEN '20-25%'
          ELSE '25%+'
        END AS "bucket",
        CASE
          WHEN sample.hr_prob_1plus < 0.05 THEN 1
          WHEN sample.hr_prob_1plus < 0.10 THEN 2
          WHEN sample.hr_prob_1plus < 0.15 THEN 3
          WHEN sample.hr_prob_1plus < 0.20 THEN 4
          WHEN sample.hr_prob_1plus < 0.25 THEN 5
          ELSE 6
        END AS "bucketSort",
        COUNT(*)::int AS "rows",
        COUNT(*) FILTER (WHERE sample.actual_hr IS NOT NULL)::int AS "actualHrRows",
        AVG(sample.hr_prob_1plus) * 100 AS "avgPredictedPct",
        AVG(CASE WHEN sample.hit_hr_1plus THEN 1.0 ELSE 0.0 END) FILTER (WHERE sample.actual_hr IS NOT NULL) * 100 AS "hitRate",
        AVG(POWER(sample.hr_prob_1plus - CASE WHEN sample.hit_hr_1plus THEN 1.0 ELSE 0.0 END, 2))
          FILTER (WHERE sample.actual_hr IS NOT NULL) AS "brierScore"
      FROM sample
      GROUP BY 1, 2
      ORDER BY 2 ASC
    `),
  ]);

  const summaryRow = summaryResult.rows[0];
  if (!summaryRow || Number(summaryRow.rows ?? 0) === 0) return null;

  return {
    summary: {
      runs: Number(summaryRow.runs ?? 0),
      slates: Number(summaryRow.slates ?? 0),
      rows: Number(summaryRow.rows ?? 0),
      actualHrRows: Number(summaryRow.actualHrRows ?? 0),
      pendingHrRows: Number(summaryRow.pendingHrRows ?? 0),
      actualFptsRows: Number(summaryRow.actualFptsRows ?? 0),
      actualOwnRows: Number(summaryRow.actualOwnRows ?? 0),
      avgPredictedPct: summaryRow.avgPredictedPct == null ? null : Number(summaryRow.avgPredictedPct),
      hitRate: summaryRow.hitRate == null ? null : Number(summaryRow.hitRate),
      brierScore: summaryRow.brierScore == null ? null : Number(summaryRow.brierScore),
      logLoss: summaryRow.logLoss == null ? null : Number(summaryRow.logLoss),
      top15Rows: Number(summaryRow.top15Rows ?? 0),
      top15ActualHrRows: Number(summaryRow.top15ActualHrRows ?? 0),
      top15HitRate: summaryRow.top15HitRate == null ? null : Number(summaryRow.top15HitRate),
      latestVersion: summaryRow.latestVersion ?? null,
      latestSource: summaryRow.latestSource ?? null,
      latestCapturedAt: summaryRow.latestCapturedAt ?? null,
      latestSlateDate: summaryRow.latestSlateDate ?? null,
    },
    buckets: bucketResult.rows.map((row) => ({
      bucket: row.bucket,
      rows: Number(row.rows ?? 0),
      actualHrRows: Number(row.actualHrRows ?? 0),
      avgPredictedPct: row.avgPredictedPct == null ? null : Number(row.avgPredictedPct),
      hitRate: row.hitRate == null ? null : Number(row.hitRate),
      brierScore: row.brierScore == null ? null : Number(row.brierScore),
    })),
  };
}

export type DfsAccuracyMetrics = {
  ourMAE: number | null;
  ourBias: number | null;
  linestarMAE: number | null;
  linestarBias: number | null;
  nOur: number;
  nLinestar: number;
  ourActiveMAE: number | null;
  ourActiveBias: number | null;
  linestarActiveMAE: number | null;
  linestarActiveBias: number | null;
  nOurActive: number;
  nLinestarActive: number;
  nOutProjected: number;
  slateDate: string | null;
};

export type DfsAccuracyRow = {
  id: number;
  name: string;
  teamAbbrev: string;
  salary: number;
  eligiblePositions: string;
  ourProj: number | null;
  linestarProj: number | null;
  actualFpts: number | null;
  isOut: boolean | null;
  teamLogo: string | null;
};

export async function getDfsAccuracy(sport: Sport = "nba"): Promise<{
  metrics: DfsAccuracyMetrics;
  players: DfsAccuracyRow[];
} | null> {
  const metricResult = await db.execute<DfsAccuracyMetrics>(sql`
    SELECT
      AVG(ABS(dp.our_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourMAE",
      AVG(dp.our_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourBias",
      AVG(ABS(dp.linestar_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "linestarMAE",
      AVG(dp.linestar_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "linestarBias",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL)::int AS "nOur",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL)::int AS "nLinestar",
      AVG(ABS(dp.our_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL AND COALESCE(dp.is_out, false) = false) AS "ourActiveMAE",
      AVG(dp.our_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL AND COALESCE(dp.is_out, false) = false) AS "ourActiveBias",
      AVG(ABS(dp.linestar_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL AND COALESCE(dp.is_out, false) = false) AS "linestarActiveMAE",
      AVG(dp.linestar_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL AND COALESCE(dp.is_out, false) = false) AS "linestarActiveBias",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL AND COALESCE(dp.is_out, false) = false)::int AS "nOurActive",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL AND COALESCE(dp.is_out, false) = false)::int AS "nLinestarActive",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL AND COALESCE(dp.is_out, false) = true)::int AS "nOutProjected",
      ds.slate_date AS "slateDate"
    FROM dk_players dp
    INNER JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.id = (
      SELECT id FROM dk_slates WHERE sport = ${sport}
      ORDER BY slate_date DESC, id DESC LIMIT 1
    )
    GROUP BY ds.slate_date
  `);
  const metrics = metricResult.rows[0];
  if (!metrics || metrics.nOur === 0) return null;

  const playerResult = await db.execute<DfsAccuracyRow>(sql`
    SELECT
      dp.id, dp.name, dp.team_abbrev AS "teamAbbrev", dp.salary,
      dp.eligible_positions AS "eligiblePositions",
      dp.our_proj AS "ourProj", dp.linestar_proj AS "linestarProj",
      dp.actual_fpts AS "actualFpts", dp.is_out AS "isOut", t.logo_url AS "teamLogo"
    FROM dk_players dp
    INNER JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN teams t ON t.team_id = dp.team_id
    WHERE dp.slate_id = (SELECT id FROM dk_slates WHERE sport = ${sport} ORDER BY slate_date DESC, id DESC LIMIT 1)
      AND dp.actual_fpts IS NOT NULL
    ORDER BY ABS(COALESCE(dp.our_proj, 0) - dp.actual_fpts) DESC NULLS LAST
  `);

  return { metrics, players: playerResult.rows };
}

// ── Lineup Comparison ─────────────────────────────────────────

export type LineupStrategyRow = {
  strategy: string;
  nLineups: number;
  avgProjFpts: number | null;
  avgActualFpts: number | null;
  avgLeverage: number | null;
  topStack: string | null;
};

export async function getDkLineupComparison(sport: Sport = "nba"): Promise<LineupStrategyRow[]> {
  const result = await db.execute<LineupStrategyRow>(sql`
    SELECT
      dl.strategy,
      COUNT(*)::int AS "nLineups",
      AVG(dl.proj_fpts) AS "avgProjFpts",
      AVG(dl.actual_fpts) AS "avgActualFpts",
      AVG(dl.leverage) AS "avgLeverage",
      mode() WITHIN GROUP (ORDER BY dl.stack_team) AS "topStack"
    FROM dk_lineups dl
    WHERE dl.slate_id = (
      SELECT id FROM dk_slates WHERE sport = ${sport}
      ORDER BY slate_date DESC, id DESC LIMIT 1
    )
    GROUP BY dl.strategy
    ORDER BY AVG(dl.actual_fpts) DESC NULLS LAST, dl.strategy
  `);
  return result.rows;
}

// ── Strategy Summary (cross-slate) ───────────────────────────

export type StrategySummaryRow = {
  strategy: string;
  nSlates: number;
  totalLineups: number;
  avgActualFpts: number | null;
  totalCashed: number;
  cashRate: number | null;
  bestSingleLineup: number | null;
  avgLeverage: number | null;
};

export async function getDkStrategySummary(sport: Sport = "nba"): Promise<StrategySummaryRow[]> {
  const result = await db.execute<StrategySummaryRow>(sql`
    SELECT
      dl.strategy,
      COUNT(DISTINCT dl.slate_id)::int AS "nSlates",
      COUNT(*)::int AS "totalLineups",
      AVG(dl.actual_fpts) AS "avgActualFpts",
      COUNT(*) FILTER (WHERE dl.actual_fpts >= COALESCE(ds.cash_line, 300))::int AS "totalCashed",
      ROUND(
        (100.0 * COUNT(*) FILTER (WHERE dl.actual_fpts >= COALESCE(ds.cash_line, 300)) / COUNT(*))::NUMERIC,
        1
      )::FLOAT AS "cashRate",
      MAX(dl.actual_fpts) AS "bestSingleLineup",
      AVG(dl.leverage) AS "avgLeverage"
    FROM dk_lineups dl
    JOIN dk_slates ds ON ds.id = dl.slate_id
    WHERE dl.actual_fpts IS NOT NULL
      AND ds.sport = ${sport}
    GROUP BY dl.strategy
    ORDER BY AVG(dl.actual_fpts) DESC NULLS LAST
  `);
  return result.rows;
}

// ── Team Stats ───────────────────────────────────────────────

export type TeamStatsRow = {
  teamId: number;
  name: string;
  abbreviation: string;
  logoUrl: string | null;
  conference: string | null;
  pace: number | null;
  offRtg: number | null;
  defRtg: number | null;
};

export async function getTeamStats(season = CURRENT_SEASON): Promise<TeamStatsRow[]> {
  return db
    .select({
      teamId: teams.teamId,
      name: teams.name,
      abbreviation: teams.abbreviation,
      logoUrl: teams.logoUrl,
      conference: teams.conference,
      pace: nbaTeamStats.pace,
      offRtg: nbaTeamStats.offRtg,
      defRtg: nbaTeamStats.defRtg,
    })
    .from(nbaTeamStats)
    .innerJoin(teams, eq(teams.teamId, nbaTeamStats.teamId))
    .where(eq(nbaTeamStats.season, season))
    .orderBy(desc(nbaTeamStats.pace));
}

// ── Schedule ─────────────────────────────────────────────────

export type ScheduleRow = {
  id: number;
  gameDate: string;
  vegasTotal: number | null;
  homeWinProb: number | null;
  homeMl: number | null;
  awayMl: number | null;
  homeTeamId: number | null;
  awayTeamId: number | null;
  homeName: string | null;
  homeLogo: string | null;
  homeAbbrev: string | null;
  awayName: string | null;
  awayLogo: string | null;
  awayAbbrev: string | null;
};

export async function getRecentSchedule(days = 7): Promise<ScheduleRow[]> {
  const result = await db.execute<ScheduleRow>(sql`
    SELECT
      m.id,
      m.game_date        AS "gameDate",
      m.vegas_total      AS "vegasTotal",
      m.vegas_prob_home  AS "homeWinProb",
      m.home_ml          AS "homeMl",
      m.away_ml          AS "awayMl",
      m.home_team_id     AS "homeTeamId",
      m.away_team_id     AS "awayTeamId",
      ht.name            AS "homeName",
      ht.logo_url        AS "homeLogo",
      ht.abbreviation    AS "homeAbbrev",
      at.name            AS "awayName",
      at.logo_url        AS "awayLogo",
      at.abbreviation    AS "awayAbbrev"
    FROM nba_matchups m
    LEFT JOIN teams ht ON ht.team_id = m.home_team_id
    LEFT JOIN teams at ON at.team_id = m.away_team_id
    WHERE m.game_date >= CURRENT_DATE - (${days} * INTERVAL '1 day')
    ORDER BY m.game_date DESC, m.id
  `);
  return result.rows;
}

export type MlbScheduleRow = {
  id: number;
  gameDate: string;
  vegasTotal: number | null;
  vegasProbHome: number | null;
  homeMl: number | null;
  awayMl: number | null;
  homeImplied: number | null;
  awayImplied: number | null;
  homeTeamId: number | null;
  awayTeamId: number | null;
  homeName: string | null;
  homeLogo: string | null;
  homeAbbrev: string | null;
  awayName: string | null;
  awayLogo: string | null;
  awayAbbrev: string | null;
};

export async function getRecentMlbSchedule(days = 7): Promise<MlbScheduleRow[]> {
  const result = await db.execute<MlbScheduleRow>(sql`
    SELECT
      m.id,
      m.game_date          AS "gameDate",
      m.vegas_total        AS "vegasTotal",
      m.vegas_prob_home    AS "vegasProbHome",
      m.home_ml            AS "homeMl",
      m.away_ml            AS "awayMl",
      m.home_implied       AS "homeImplied",
      m.away_implied       AS "awayImplied",
      m.home_team_id       AS "homeTeamId",
      m.away_team_id       AS "awayTeamId",
      ht.name              AS "homeName",
      ht.logo_url          AS "homeLogo",
      ht.abbreviation      AS "homeAbbrev",
      at.name              AS "awayName",
      at.logo_url          AS "awayLogo",
      at.abbreviation      AS "awayAbbrev"
    FROM mlb_matchups m
    LEFT JOIN mlb_teams ht ON ht.team_id = m.home_team_id
    LEFT JOIN mlb_teams at ON at.team_id = m.away_team_id
    WHERE m.game_date >= CURRENT_DATE - (${days} * INTERVAL '1 day')
    ORDER BY m.game_date DESC, m.id
  `);
  return result.rows;
}

// ── MLB Team Stats ────────────────────────────────────────────

export type MlbTeamStatsRow = {
  teamId: number;
  name: string;
  abbreviation: string;
  logoUrl: string | null;
  division: string | null;
  teamWrcPlus: number | null;
  teamIso: number | null;
  teamOps: number | null;
  teamKPct: number | null;
  teamBbPct: number | null;
  bullpenEra: number | null;
  bullpenFip: number | null;
  staffKPct: number | null;
};

export async function getMlbTeamStats(season = "2025"): Promise<MlbTeamStatsRow[]> {
  return db
    .select({
      teamId: mlbTeams.teamId,
      name: mlbTeams.name,
      abbreviation: mlbTeams.abbreviation,
      logoUrl: mlbTeams.logoUrl,
      division: mlbTeams.division,
      teamWrcPlus: mlbTeamStats.teamWrcPlus,
      teamIso: mlbTeamStats.teamIso,
      teamOps: mlbTeamStats.teamOps,
      teamKPct: mlbTeamStats.teamKPct,
      teamBbPct: mlbTeamStats.teamBbPct,
      bullpenEra: mlbTeamStats.bullpenEra,
      bullpenFip: mlbTeamStats.bullpenFip,
      staffKPct: mlbTeamStats.staffKPct,
    })
    .from(mlbTeamStats)
    .innerJoin(mlbTeams, eq(mlbTeams.teamId, mlbTeamStats.teamId))
    .where(eq(mlbTeamStats.season, season))
    .orderBy(desc(mlbTeamStats.teamWrcPlus));
}

// ── Analytics (cross-slate calibration) ──────────────────────

export type CrossSlateAccuracyRow = {
  slateDate: string;
  n: number;
  nOur: number;
  nLinestar: number;
  ourMAE: number | null;
  ourBias: number | null;
  lsMAE: number | null;
  lsBias: number | null;
  ownCorr: number | null;
  vegasTeamTotalMAE: number | null;
  vegasGameTotalMAE: number | null;
};

export async function getCrossSlateAccuracy(sport: Sport = "nba"): Promise<CrossSlateAccuracyRow[]> {
  const matchupCte = sport === "mlb"
    ? sql`
      WITH matchup_stats AS (
        SELECT
          m.game_date,
          AVG(ABS((m.home_score + m.away_score)::DOUBLE PRECISION - m.vegas_total))                   AS game_total_mae,
          AVG((ABS(m.home_implied - m.home_score::DOUBLE PRECISION) + ABS(m.away_implied - m.away_score::DOUBLE PRECISION)) / 2.0) AS team_total_mae
        FROM mlb_matchups m
        WHERE m.vegas_total IS NOT NULL AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
          AND m.home_implied IS NOT NULL AND m.away_implied IS NOT NULL
        GROUP BY m.game_date
      )`
    : sql`
      WITH matchup_stats AS (
        SELECT
          nm.game_date,
          AVG(ABS((nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total))                   AS game_total_mae,
          AVG((ABS(nm.home_implied - nm.home_score::DOUBLE PRECISION) + ABS(nm.away_implied - nm.away_score::DOUBLE PRECISION)) / 2.0) AS team_total_mae
        FROM nba_matchups nm
        WHERE nm.vegas_total IS NOT NULL AND nm.home_score IS NOT NULL AND nm.away_score IS NOT NULL
          AND nm.home_implied IS NOT NULL AND nm.away_implied IS NOT NULL
        GROUP BY nm.game_date
      )`;

  const result = await db.execute<CrossSlateAccuracyRow>(sql`
    ${matchupCte}
    SELECT
      ds.slate_date                                                            AS "slateDate",
      GREATEST(
        COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL),
        COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL)
      )::int AS "n",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL)::int AS "nOur",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL)::int AS "nLinestar",
      AVG(ABS(dp.our_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourMAE",
      AVG(dp.our_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourBias",
      AVG(ABS(dp.linestar_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsMAE",
      AVG(dp.linestar_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsBias",
      CORR(dp.proj_own_pct, dp.actual_own_pct)
        FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) AS "ownCorr",
      MAX(ms.game_total_mae)                                                  AS "vegasGameTotalMAE",
      MAX(ms.team_total_mae)                                                  AS "vegasTeamTotalMAE"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN matchup_stats ms ON ms.game_date = ds.slate_date
    WHERE ds.sport = ${sport}
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY ds.slate_date
    HAVING COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL) > 0
    ORDER BY ds.slate_date ASC
  `);
  return result.rows;
}

export type SlateTypePerformanceRow = {
  contestType: string;
  label: string;
  slates: number;
  firstSlateDate: string | null;
  lastSlateDate: string | null;
  actualRows: number;
  ourProjRows: number;
  ourProjMae: number | null;
  ourProjBias: number | null;
  ourProjRank: number | null;
  ourFinalProjRows: number;
  ourFinalProjMae: number | null;
  ourFinalProjBias: number | null;
  ourFinalProjRank: number | null;
  linestarProjRows: number;
  linestarProjMae: number | null;
  linestarProjBias: number | null;
  linestarProjRank: number | null;
  ourOwnRows: number;
  ourOwnMae: number | null;
  ourOwnBias: number | null;
  ourOwnCorr: number | null;
  ourOwnRank: number | null;
  linestarOwnRows: number;
  linestarOwnMae: number | null;
  linestarOwnBias: number | null;
  linestarOwnCorr: number | null;
  linestarOwnRank: number | null;
  sampleWarning: string | null;
};

const SLATE_TYPE_MIN_SLATES = 3;
const SLATE_TYPE_MIN_ROWS = 100;

function rankSlateTypeMetric<T extends Record<string, unknown>>(
  rows: T[],
  valueKey: keyof T,
  rowsKey: keyof T,
  rankKey: keyof T,
) {
  const candidates = rows
    .filter((row) =>
      Number(row.slates ?? 0) >= SLATE_TYPE_MIN_SLATES
      && Number(row[rowsKey] ?? 0) >= SLATE_TYPE_MIN_ROWS
      && row[valueKey] != null
    )
    .sort((a, b) => Number(a[valueKey]) - Number(b[valueKey]));

  candidates.forEach((row, index) => {
    (row as Record<string, unknown>)[String(rankKey)] = index + 1;
  });
}

export async function getSlateTypePerformance(sport: Sport = "nba"): Promise<SlateTypePerformanceRow[]> {
  const result = await db.execute<Omit<
    SlateTypePerformanceRow,
    "label"
    | "ourProjRank"
    | "ourFinalProjRank"
    | "linestarProjRank"
    | "ourOwnRank"
    | "linestarOwnRank"
    | "sampleWarning"
  >>(sql`
    WITH slate_types AS (
      SELECT *
      FROM (
        VALUES
          ('turbo'::text, 1::int),
          ('early'::text, 2::int),
          ('main'::text, 3::int),
          ('night'::text, 4::int)
      ) AS slate_types(contest_type, sort_order)
    ),
    player_sample AS (
      SELECT
        CASE
          WHEN COALESCE(ds.contest_type, 'main') = 'late' THEN 'night'
          ELSE COALESCE(ds.contest_type, 'main')
        END AS contest_type,
        ds.id AS slate_id,
        ds.slate_date,
        dp.eligible_positions,
        dp.actual_fpts,
        dp.actual_own_pct,
        dp.our_proj,
        COALESCE(dp.live_proj, dp.our_proj) AS our_final_proj,
        dp.linestar_proj,
        COALESCE(dp.live_own_pct, dp.our_own_pct) AS our_own_pct,
        COALESCE(dp.linestar_own_pct, dp.proj_own_pct) AS linestar_own_pct
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE ds.sport = ${sport}
        AND COALESCE(dp.is_out, false) = false
        AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    ),
    grouped AS (
      SELECT
        st.contest_type AS "contestType",
        st.sort_order,
        COUNT(DISTINCT ps.slate_id) FILTER (WHERE ps.actual_fpts IS NOT NULL)::int AS "slates",
        MIN(ps.slate_date) FILTER (WHERE ps.actual_fpts IS NOT NULL)::text AS "firstSlateDate",
        MAX(ps.slate_date) FILTER (WHERE ps.actual_fpts IS NOT NULL)::text AS "lastSlateDate",
        COUNT(*) FILTER (WHERE ps.actual_fpts IS NOT NULL)::int AS "actualRows",
        COUNT(*) FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.our_proj IS NOT NULL)::int AS "ourProjRows",
        AVG(ABS(ps.our_proj - ps.actual_fpts))
          FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.our_proj IS NOT NULL) AS "ourProjMae",
        AVG(ps.our_proj - ps.actual_fpts)
          FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.our_proj IS NOT NULL) AS "ourProjBias",
        COUNT(*) FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.our_final_proj IS NOT NULL)::int AS "ourFinalProjRows",
        AVG(ABS(ps.our_final_proj - ps.actual_fpts))
          FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.our_final_proj IS NOT NULL) AS "ourFinalProjMae",
        AVG(ps.our_final_proj - ps.actual_fpts)
          FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.our_final_proj IS NOT NULL) AS "ourFinalProjBias",
        COUNT(*) FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.linestar_proj IS NOT NULL)::int AS "linestarProjRows",
        AVG(ABS(ps.linestar_proj - ps.actual_fpts))
          FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.linestar_proj IS NOT NULL) AS "linestarProjMae",
        AVG(ps.linestar_proj - ps.actual_fpts)
          FILTER (WHERE ps.actual_fpts IS NOT NULL AND ps.linestar_proj IS NOT NULL) AS "linestarProjBias",
        COUNT(*) FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.our_own_pct IS NOT NULL)::int AS "ourOwnRows",
        AVG(ABS(ps.our_own_pct - ps.actual_own_pct))
          FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.our_own_pct IS NOT NULL) AS "ourOwnMae",
        AVG(ps.our_own_pct - ps.actual_own_pct)
          FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.our_own_pct IS NOT NULL) AS "ourOwnBias",
        CORR(ps.our_own_pct, ps.actual_own_pct)
          FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.our_own_pct IS NOT NULL) AS "ourOwnCorr",
        COUNT(*) FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.linestar_own_pct IS NOT NULL)::int AS "linestarOwnRows",
        AVG(ABS(ps.linestar_own_pct - ps.actual_own_pct))
          FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.linestar_own_pct IS NOT NULL) AS "linestarOwnMae",
        AVG(ps.linestar_own_pct - ps.actual_own_pct)
          FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.linestar_own_pct IS NOT NULL) AS "linestarOwnBias",
        CORR(ps.linestar_own_pct, ps.actual_own_pct)
          FILTER (WHERE ps.actual_own_pct IS NOT NULL AND ps.linestar_own_pct IS NOT NULL) AS "linestarOwnCorr"
      FROM slate_types st
      LEFT JOIN player_sample ps ON ps.contest_type = st.contest_type
      GROUP BY st.contest_type, st.sort_order
    )
    SELECT *
    FROM grouped
    ORDER BY sort_order ASC
  `);

  const labels: Record<string, string> = {
    turbo: "Turbo",
    early: "Early",
    main: "Main",
    night: "Night",
  };

  const rows: SlateTypePerformanceRow[] = result.rows.map((row) => {
    const slates = Number(row.slates ?? 0);
    const actualRows = Number(row.actualRows ?? 0);
    return {
      contestType: String(row.contestType),
      label: labels[String(row.contestType)] ?? String(row.contestType),
      slates,
      firstSlateDate: row.firstSlateDate ? String(row.firstSlateDate) : null,
      lastSlateDate: row.lastSlateDate ? String(row.lastSlateDate) : null,
      actualRows,
      ourProjRows: Number(row.ourProjRows ?? 0),
      ourProjMae: row.ourProjMae != null ? Number(row.ourProjMae) : null,
      ourProjBias: row.ourProjBias != null ? Number(row.ourProjBias) : null,
      ourProjRank: null,
      ourFinalProjRows: Number(row.ourFinalProjRows ?? 0),
      ourFinalProjMae: row.ourFinalProjMae != null ? Number(row.ourFinalProjMae) : null,
      ourFinalProjBias: row.ourFinalProjBias != null ? Number(row.ourFinalProjBias) : null,
      ourFinalProjRank: null,
      linestarProjRows: Number(row.linestarProjRows ?? 0),
      linestarProjMae: row.linestarProjMae != null ? Number(row.linestarProjMae) : null,
      linestarProjBias: row.linestarProjBias != null ? Number(row.linestarProjBias) : null,
      linestarProjRank: null,
      ourOwnRows: Number(row.ourOwnRows ?? 0),
      ourOwnMae: row.ourOwnMae != null ? Number(row.ourOwnMae) : null,
      ourOwnBias: row.ourOwnBias != null ? Number(row.ourOwnBias) : null,
      ourOwnCorr: row.ourOwnCorr != null ? Number(row.ourOwnCorr) : null,
      ourOwnRank: null,
      linestarOwnRows: Number(row.linestarOwnRows ?? 0),
      linestarOwnMae: row.linestarOwnMae != null ? Number(row.linestarOwnMae) : null,
      linestarOwnBias: row.linestarOwnBias != null ? Number(row.linestarOwnBias) : null,
      linestarOwnCorr: row.linestarOwnCorr != null ? Number(row.linestarOwnCorr) : null,
      linestarOwnRank: null,
      sampleWarning: slates === 0 || actualRows === 0
        ? "No completed sample"
        : slates < SLATE_TYPE_MIN_SLATES || actualRows < SLATE_TYPE_MIN_ROWS
          ? "Low sample"
          : null,
    };
  });

  rankSlateTypeMetric(rows, "ourProjMae", "ourProjRows", "ourProjRank");
  rankSlateTypeMetric(rows, "ourFinalProjMae", "ourFinalProjRows", "ourFinalProjRank");
  rankSlateTypeMetric(rows, "linestarProjMae", "linestarProjRows", "linestarProjRank");
  rankSlateTypeMetric(rows, "ourOwnMae", "ourOwnRows", "ourOwnRank");
  rankSlateTypeMetric(rows, "linestarOwnMae", "linestarOwnRows", "linestarOwnRank");

  return rows;
}

export type PositionAccuracyRow = {
  position: string;
  ourN: number;
  ourMae: number | null;
  ourBias: number | null;
  lsN: number;
  lsMae: number | null;
  lsBias: number | null;
};

export async function getPositionAccuracy(sport: Sport = "nba"): Promise<PositionAccuracyRow[]> {
  // Position CASE expression differs by sport — NBA uses PG/SG/SF/PF/C,
  // MLB uses SP/RP/C/1B/2B/3B/SS/OF.
  const posCase = sport === "mlb"
    ? sql`CASE
        WHEN dp.eligible_positions LIKE '%SP%' THEN 'SP'
        WHEN dp.eligible_positions LIKE '%RP%' THEN 'RP'
        WHEN dp.eligible_positions LIKE '%OF%' THEN 'OF'
        WHEN dp.eligible_positions LIKE '%SS%' THEN 'SS'
        WHEN dp.eligible_positions LIKE '%3B%' THEN '3B'
        WHEN dp.eligible_positions LIKE '%2B%' THEN '2B'
        WHEN dp.eligible_positions LIKE '%1B%' THEN '1B'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`
    : sql`CASE
        WHEN dp.eligible_positions LIKE '%PG%' THEN 'PG'
        WHEN dp.eligible_positions LIKE '%SG%' THEN 'SG'
        WHEN dp.eligible_positions LIKE '%SF%' THEN 'SF'
        WHEN dp.eligible_positions LIKE '%PF%' THEN 'PF'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`;

  const result = await db.execute<PositionAccuracyRow>(sql`
    SELECT
      ${posCase} AS "position",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL)::int AS "ourN",
      AVG(ABS(dp.our_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourMae",
      AVG(dp.our_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourBias",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL)::int AS "lsN",
      AVG(ABS(dp.linestar_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsMae",
      AVG(dp.linestar_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsBias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY 1
    ORDER BY COALESCE(
      AVG(ABS(dp.our_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL),
      AVG(ABS(dp.linestar_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL)
    ) DESC NULLS LAST
  `);
  return result.rows;
}

export type SalaryTierAccuracyRow = {
  salaryTier: string;
  tierMin: number | null;
  ourN: number;
  ourMae: number | null;
  ourBias: number | null;
  lsN: number;
  lsMae: number | null;
  lsBias: number | null;
};

export async function getSalaryTierAccuracy(sport: Sport = "nba"): Promise<SalaryTierAccuracyRow[]> {
  const result = await db.execute<SalaryTierAccuracyRow>(sql`
    SELECT
      CASE
        WHEN dp.salary < 5000 THEN 'Under $5k'
        WHEN dp.salary < 6000 THEN '$5k–$6k'
        WHEN dp.salary < 7000 THEN '$6k–$7k'
        WHEN dp.salary < 8000 THEN '$7k–$8k'
        WHEN dp.salary < 9000 THEN '$8k–$9k'
        ELSE '$9k+'
      END AS "salaryTier",
      MIN(dp.salary) AS "tierMin",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL)::int AS "ourN",
      AVG(ABS(dp.our_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourMae",
      AVG(dp.our_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourBias",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL)::int AS "lsN",
      AVG(ABS(dp.linestar_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsMae",
      AVG(dp.linestar_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsBias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY 1
    ORDER BY MIN(dp.salary) ASC NULLS LAST
  `);
  return result.rows;
}

export type PositionSalaryMatrixRow = {
  position: string;
  salaryTier: string;
  tierMin: number | null;
  ourN: number;
  ourMae: number | null;
  ourBias: number | null;
};

export async function getPositionSalaryMatrix(sport: Sport = "nba"): Promise<PositionSalaryMatrixRow[]> {
  const posCase = sport === "mlb"
    ? sql`CASE
        WHEN dp.eligible_positions LIKE '%SP%' THEN 'SP'
        WHEN dp.eligible_positions LIKE '%RP%' THEN 'RP'
        WHEN dp.eligible_positions LIKE '%OF%' THEN 'OF'
        WHEN dp.eligible_positions LIKE '%SS%' THEN 'SS'
        WHEN dp.eligible_positions LIKE '%3B%' THEN '3B'
        WHEN dp.eligible_positions LIKE '%2B%' THEN '2B'
        WHEN dp.eligible_positions LIKE '%1B%' THEN '1B'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`
    : sql`CASE
        WHEN dp.eligible_positions LIKE '%PG%' THEN 'PG'
        WHEN dp.eligible_positions LIKE '%SG%' THEN 'SG'
        WHEN dp.eligible_positions LIKE '%SF%' THEN 'SF'
        WHEN dp.eligible_positions LIKE '%PF%' THEN 'PF'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`;

  const result = await db.execute<PositionSalaryMatrixRow>(sql`
    SELECT
      ${posCase} AS "position",
      CASE
        WHEN dp.salary < 5000 THEN 'Under $5k'
        WHEN dp.salary < 6000 THEN '$5k-$6k'
        WHEN dp.salary < 7000 THEN '$6k-$7k'
        WHEN dp.salary < 8000 THEN '$7k-$8k'
        WHEN dp.salary < 9000 THEN '$8k-$9k'
        ELSE '$9k+'
      END AS "salaryTier",
      MIN(dp.salary) AS "tierMin",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL)::int AS "ourN",
      AVG(ABS(dp.our_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourMae",
      AVG(dp.our_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS "ourBias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY 1, 2
    ORDER BY MIN(dp.salary) ASC NULLS LAST, 1 ASC
  `);
  return result.rows;
}

export type LsProjectionMatrixRow = {
  position: string;
  salaryTier: string;
  tierMin: number | null;
  lsN: number;
  lsMae: number | null;
  lsBias: number | null;
};

export async function getLsProjectionBiasMatrix(sport: Sport = "nba"): Promise<LsProjectionMatrixRow[]> {
  const posCase = sport === "mlb"
    ? sql`CASE
        WHEN dp.eligible_positions LIKE '%SP%' THEN 'SP'
        WHEN dp.eligible_positions LIKE '%RP%' THEN 'RP'
        WHEN dp.eligible_positions LIKE '%OF%' THEN 'OF'
        WHEN dp.eligible_positions LIKE '%SS%' THEN 'SS'
        WHEN dp.eligible_positions LIKE '%3B%' THEN '3B'
        WHEN dp.eligible_positions LIKE '%2B%' THEN '2B'
        WHEN dp.eligible_positions LIKE '%1B%' THEN '1B'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`
    : sql`CASE
        WHEN dp.eligible_positions LIKE '%PG%' THEN 'PG'
        WHEN dp.eligible_positions LIKE '%SG%' THEN 'SG'
        WHEN dp.eligible_positions LIKE '%SF%' THEN 'SF'
        WHEN dp.eligible_positions LIKE '%PF%' THEN 'PF'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`;

  const result = await db.execute<LsProjectionMatrixRow>(sql`
    SELECT
      ${posCase} AS "position",
      CASE
        WHEN dp.salary < 5000 THEN 'Under $5k'
        WHEN dp.salary < 6000 THEN '$5k-$6k'
        WHEN dp.salary < 7000 THEN '$6k-$7k'
        WHEN dp.salary < 8000 THEN '$7k-$8k'
        WHEN dp.salary < 9000 THEN '$8k-$9k'
        ELSE '$9k+'
      END AS "salaryTier",
      MIN(dp.salary) AS "tierMin",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL)::int AS "lsN",
      AVG(ABS(dp.linestar_proj - dp.actual_fpts))
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsMae",
      AVG(dp.linestar_proj - dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS "lsBias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY 1, 2
    ORDER BY MIN(dp.salary) ASC NULLS LAST, 1 ASC
  `);
  return result.rows;
}

export type OwnershipBiasMatrixRow = {
  position: string;
  salaryTier: string;
  tierMin: number | null;
  n: number;
  mae: number | null;
  bias: number | null;
};

function buildOwnershipBiasMatrixQuery(sport: Sport, ownershipCol: "our_own_pct" | "proj_own_pct") {
  const posCase = sport === "mlb"
    ? sql`CASE
        WHEN dp.eligible_positions LIKE '%SP%' THEN 'SP'
        WHEN dp.eligible_positions LIKE '%RP%' THEN 'RP'
        WHEN dp.eligible_positions LIKE '%OF%' THEN 'OF'
        WHEN dp.eligible_positions LIKE '%SS%' THEN 'SS'
        WHEN dp.eligible_positions LIKE '%3B%' THEN '3B'
        WHEN dp.eligible_positions LIKE '%2B%' THEN '2B'
        WHEN dp.eligible_positions LIKE '%1B%' THEN '1B'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`
    : sql`CASE
        WHEN dp.eligible_positions LIKE '%PG%' THEN 'PG'
        WHEN dp.eligible_positions LIKE '%SG%' THEN 'SG'
        WHEN dp.eligible_positions LIKE '%SF%' THEN 'SF'
        WHEN dp.eligible_positions LIKE '%PF%' THEN 'PF'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`;

  const col = ownershipCol === "our_own_pct" ? sql`dp.our_own_pct` : sql`dp.proj_own_pct`;

  return db.execute<OwnershipBiasMatrixRow>(sql`
    SELECT
      ${posCase} AS "position",
      CASE
        WHEN dp.salary < 5000 THEN 'Under $5k'
        WHEN dp.salary < 6000 THEN '$5k-$6k'
        WHEN dp.salary < 7000 THEN '$6k-$7k'
        WHEN dp.salary < 8000 THEN '$7k-$8k'
        WHEN dp.salary < 9000 THEN '$8k-$9k'
        ELSE '$9k+'
      END AS "salaryTier",
      MIN(dp.salary) AS "tierMin",
      COUNT(*) FILTER (WHERE dp.actual_own_pct IS NOT NULL AND ${col} IS NOT NULL)::int AS "n",
      AVG(ABS(${col} - dp.actual_own_pct))
        FILTER (WHERE dp.actual_own_pct IS NOT NULL AND ${col} IS NOT NULL) AS "mae",
      AVG(${col} - dp.actual_own_pct)
        FILTER (WHERE dp.actual_own_pct IS NOT NULL AND ${col} IS NOT NULL) AS "bias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY 1, 2
    ORDER BY MIN(dp.salary) ASC NULLS LAST, 1 ASC
  `);
}

export async function getOurOwnershipBiasMatrix(sport: Sport = "nba"): Promise<OwnershipBiasMatrixRow[]> {
  const result = await buildOwnershipBiasMatrixQuery(sport, "our_own_pct");
  return result.rows;
}

export async function getLsOwnershipBiasMatrix(sport: Sport = "nba"): Promise<OwnershipBiasMatrixRow[]> {
  const result = await buildOwnershipBiasMatrixQuery(sport, "proj_own_pct");
  return result.rows;
}

export type LsOwnershipTeamPositionRow = {
  teamAbbrev: string;
  position: string;
  n: number;
  mae: number | null;
  bias: number | null;
  avgActualOwn: number | null;
};

export async function getLsOwnershipTeamPositionMatrix(sport: Sport = "nba"): Promise<LsOwnershipTeamPositionRow[]> {
  const posCase = sport === "mlb"
    ? sql`CASE
        WHEN dp.eligible_positions LIKE '%SP%' THEN 'SP'
        WHEN dp.eligible_positions LIKE '%RP%' THEN 'RP'
        WHEN dp.eligible_positions LIKE '%OF%' THEN 'OF'
        WHEN dp.eligible_positions LIKE '%SS%' THEN 'SS'
        WHEN dp.eligible_positions LIKE '%3B%' THEN '3B'
        WHEN dp.eligible_positions LIKE '%2B%' THEN '2B'
        WHEN dp.eligible_positions LIKE '%1B%' THEN '1B'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`
    : sql`CASE
        WHEN dp.eligible_positions LIKE '%PG%' THEN 'PG'
        WHEN dp.eligible_positions LIKE '%SG%' THEN 'SG'
        WHEN dp.eligible_positions LIKE '%SF%' THEN 'SF'
        WHEN dp.eligible_positions LIKE '%PF%' THEN 'PF'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`;

  const result = await db.execute<LsOwnershipTeamPositionRow>(sql`
    SELECT
      dp.team_abbrev                                                          AS "teamAbbrev",
      ${posCase}                                                              AS "position",
      COUNT(*) FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL)::int AS "n",
      AVG(ABS(dp.proj_own_pct - dp.actual_own_pct))
        FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL)             AS "mae",
      AVG(dp.proj_own_pct - dp.actual_own_pct)
        FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL)             AS "bias",
      AVG(dp.actual_own_pct)
        FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL)             AS "avgActualOwn"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
      AND dp.team_abbrev IS NOT NULL
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY dp.team_abbrev, 2
    HAVING COUNT(*) FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) >= 3
    ORDER BY dp.team_abbrev ASC, 2 ASC
  `);
  return result.rows;
}

// ---------------------------------------------------------------------------
// MLB Team × Dimension bias matrices (projection + ownership, our + LineStar)
// ---------------------------------------------------------------------------

export type TeamPositionBiasRow = {
  teamAbbrev: string;
  position: string;
  n: number;
  mae: number | null;
  bias: number | null;
};

export type TeamSalaryBiasRow = {
  teamAbbrev: string;
  salaryTier: string;
  tierMin: number | null;
  n: number;
  mae: number | null;
  bias: number | null;
};

type PredictedCol = "our_proj" | "linestar_proj" | "our_own_pct" | "proj_own_pct";
type ReferenceCol = "actual_fpts" | "actual_own_pct";

function mlbPosCase() {
  return sql`CASE
    WHEN dp.eligible_positions LIKE '%SP%' THEN 'SP'
    WHEN dp.eligible_positions LIKE '%RP%' THEN 'RP'
    WHEN dp.eligible_positions LIKE '%OF%' THEN 'OF'
    WHEN dp.eligible_positions LIKE '%SS%' THEN 'SS'
    WHEN dp.eligible_positions LIKE '%3B%' THEN '3B'
    WHEN dp.eligible_positions LIKE '%2B%' THEN '2B'
    WHEN dp.eligible_positions LIKE '%1B%' THEN '1B'
    WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
    ELSE 'UTIL'
  END`;
}

function mlbSalaryTierCase() {
  return sql`CASE
    WHEN dp.salary < 5000 THEN 'Under $5k'
    WHEN dp.salary < 6000 THEN '$5k-$6k'
    WHEN dp.salary < 7000 THEN '$6k-$7k'
    WHEN dp.salary < 8000 THEN '$7k-$8k'
    WHEN dp.salary < 9000 THEN '$8k-$9k'
    ELSE '$9k+'
  END`;
}

async function buildTeamPositionBiasMatrix(predicted: PredictedCol, reference: ReferenceCol): Promise<TeamPositionBiasRow[]> {
  const pred = sql.raw(`dp.${predicted}`);
  const ref = sql.raw(`dp.${reference}`);
  const result = await db.execute<TeamPositionBiasRow>(sql`
    SELECT
      dp.team_abbrev                                                               AS "teamAbbrev",
      ${mlbPosCase()}                                                              AS "position",
      COUNT(*) FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL)::int      AS "n",
      AVG(ABS(${pred} - ${ref})) FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL) AS "mae",
      AVG(${pred} - ${ref})      FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL) AS "bias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = 'mlb'
      AND dp.team_abbrev IS NOT NULL
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY dp.team_abbrev, 2
    HAVING COUNT(*) FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL) >= 3
    ORDER BY dp.team_abbrev ASC, 2 ASC
  `);
  return result.rows;
}

async function buildTeamSalaryBiasMatrix(predicted: PredictedCol, reference: ReferenceCol): Promise<TeamSalaryBiasRow[]> {
  const pred = sql.raw(`dp.${predicted}`);
  const ref = sql.raw(`dp.${reference}`);
  const result = await db.execute<TeamSalaryBiasRow>(sql`
    SELECT
      dp.team_abbrev                                                                    AS "teamAbbrev",
      ${mlbSalaryTierCase()}                                                            AS "salaryTier",
      MIN(dp.salary)                                                                    AS "tierMin",
      COUNT(*) FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL)::int           AS "n",
      AVG(ABS(${pred} - ${ref})) FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL) AS "mae",
      AVG(${pred} - ${ref})      FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL) AS "bias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = 'mlb'
      AND dp.team_abbrev IS NOT NULL
      AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    GROUP BY dp.team_abbrev, 2
    HAVING COUNT(*) FILTER (WHERE ${ref} IS NOT NULL AND ${pred} IS NOT NULL) >= 3
    ORDER BY dp.team_abbrev ASC, MIN(dp.salary) ASC NULLS LAST
  `);
  return result.rows;
}

export async function getMlbOurProjTeamPositionMatrix():  Promise<TeamPositionBiasRow[]> { return buildTeamPositionBiasMatrix("our_proj",      "actual_fpts");    }
export async function getMlbOurProjTeamSalaryMatrix():    Promise<TeamSalaryBiasRow[]>   { return buildTeamSalaryBiasMatrix(  "our_proj",      "actual_fpts");    }
export async function getMlbOurOwnTeamPositionMatrix():   Promise<TeamPositionBiasRow[]> { return buildTeamPositionBiasMatrix("our_own_pct",   "actual_own_pct"); }
export async function getMlbOurOwnTeamSalaryMatrix():     Promise<TeamSalaryBiasRow[]>   { return buildTeamSalaryBiasMatrix(  "our_own_pct",   "actual_own_pct"); }
export async function getMlbLsProjTeamPositionMatrix():   Promise<TeamPositionBiasRow[]> { return buildTeamPositionBiasMatrix("linestar_proj", "actual_fpts");    }
export async function getMlbLsProjTeamSalaryMatrix():     Promise<TeamSalaryBiasRow[]>   { return buildTeamSalaryBiasMatrix(  "linestar_proj", "actual_fpts");    }
export async function getMlbLsOwnTeamPositionMatrix():    Promise<TeamPositionBiasRow[]> { return buildTeamPositionBiasMatrix("proj_own_pct",  "actual_own_pct"); }
export async function getMlbLsOwnTeamSalaryMatrix():      Promise<TeamSalaryBiasRow[]>   { return buildTeamSalaryBiasMatrix(  "proj_own_pct",  "actual_own_pct"); }

// ---------------------------------------------------------------------------
// LineStar Calibrated Ownership — correction lookup tables
// ---------------------------------------------------------------------------

export type LsPosSalaryCorrection = {
  position: string;
  salaryTier: string;
  bias: number;
  n: number;
};

export type LsTeamPositionCorrection = {
  teamAbbrev: string;
  position: string;
  bias: number;
  n: number;
};

export type LsPositionMeanBias = {
  position: string;
  meanBias: number;
};

export type LsOwnershipCorrectionTables = {
  posSalary: LsPosSalaryCorrection[];
  teamPosition: LsTeamPositionCorrection[];
  positionMeanBias: LsPositionMeanBias[];
};

export async function getLsOwnershipCorrectionTables(sport: Sport = "nba"): Promise<LsOwnershipCorrectionTables> {
  const posCase = sport === "mlb"
    ? sql`CASE
        WHEN dp.eligible_positions LIKE '%SP%' THEN 'SP'
        WHEN dp.eligible_positions LIKE '%RP%' THEN 'RP'
        WHEN dp.eligible_positions LIKE '%OF%' THEN 'OF'
        WHEN dp.eligible_positions LIKE '%SS%' THEN 'SS'
        WHEN dp.eligible_positions LIKE '%3B%' THEN '3B'
        WHEN dp.eligible_positions LIKE '%2B%' THEN '2B'
        WHEN dp.eligible_positions LIKE '%1B%' THEN '1B'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`
    : sql`CASE
        WHEN dp.eligible_positions LIKE '%PG%' THEN 'PG'
        WHEN dp.eligible_positions LIKE '%SG%' THEN 'SG'
        WHEN dp.eligible_positions LIKE '%SF%' THEN 'SF'
        WHEN dp.eligible_positions LIKE '%PF%' THEN 'PF'
        WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
        ELSE 'UTIL'
      END`;

  const [psResult, tpResult] = await Promise.all([
    // Position × salary bias (Layer 1)
    db.execute<LsPosSalaryCorrection>(sql`
      SELECT
        ${posCase} AS "position",
        CASE
          WHEN dp.salary < 5000 THEN 'Under $5k'
          WHEN dp.salary < 6000 THEN '$5k-$6k'
          WHEN dp.salary < 7000 THEN '$6k-$7k'
          WHEN dp.salary < 8000 THEN '$7k-$8k'
          WHEN dp.salary < 9000 THEN '$8k-$9k'
          ELSE '$9k+'
        END AS "salaryTier",
        AVG(dp.proj_own_pct - dp.actual_own_pct)
          FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) AS "bias",
        COUNT(*) FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL)::int AS "n"
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE ds.sport = ${sport}
        AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
      GROUP BY 1, 2
      HAVING COUNT(*) FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) >= 5
    `),
    // Team × position bias (Layer 2)
    db.execute<LsTeamPositionCorrection>(sql`
      SELECT
        dp.team_abbrev                                                           AS "teamAbbrev",
        ${posCase}                                                               AS "position",
        AVG(dp.proj_own_pct - dp.actual_own_pct)
          FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) AS "bias",
        COUNT(*) FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL)::int AS "n"
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE ds.sport = ${sport}
        AND dp.team_abbrev IS NOT NULL
        AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
      GROUP BY dp.team_abbrev, 2
      HAVING COUNT(*) FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) >= 5
    `),
  ]);

  const posSalary = psResult.rows.filter((r) => r.bias != null) as LsPosSalaryCorrection[];
  const teamPosition = tpResult.rows.filter((r) => r.bias != null) as LsTeamPositionCorrection[];

  // Pre-aggregate weighted mean bias per position across all salary tiers.
  // Used to isolate the team-specific residual (Layer 2 = teamBias - positionMean).
  const positionMap = new Map<string, { sumBiasN: number; sumN: number }>();
  for (const row of posSalary) {
    const existing = positionMap.get(row.position) ?? { sumBiasN: 0, sumN: 0 };
    positionMap.set(row.position, {
      sumBiasN: existing.sumBiasN + row.bias * row.n,
      sumN: existing.sumN + row.n,
    });
  }
  const positionMeanBias: LsPositionMeanBias[] = Array.from(positionMap.entries()).map(
    ([position, { sumBiasN, sumN }]) => ({ position, meanBias: sumN > 0 ? sumBiasN / sumN : 0 })
  );

  return { posSalary, teamPosition, positionMeanBias };
}

export type LeverageCalibrationRow = {
  leverageQuartile: number;
  avgLeverage: number | null;
  avgProj: number | null;
  avgActual: number | null;
  avgBeat: number | null;
  n: number;
};

export async function getLeverageCalibration(sport: Sport = "nba"): Promise<LeverageCalibrationRow[]> {
  const result = await db.execute<LeverageCalibrationRow>(sql`
    SELECT
      sub.quartile               AS "leverageQuartile",
      AVG(sub.optimizer_leverage)      AS "avgLeverage",
      AVG(sub.optimizer_proj)          AS "avgProj",
      AVG(sub.actual_fpts)       AS "avgActual",
      AVG(sub.actual_fpts - sub.optimizer_proj) AS "avgBeat",
      COUNT(*)::int              AS "n"
    FROM (
      SELECT
        NTILE(4) OVER (
          ORDER BY COALESCE(dp.live_leverage, dp.our_leverage) ASC NULLS LAST
        ) AS quartile,
        COALESCE(dp.live_leverage, dp.our_leverage) AS optimizer_leverage,
        COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS optimizer_proj,
        dp.actual_fpts
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE COALESCE(dp.live_leverage, dp.our_leverage) IS NOT NULL
        AND COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) IS NOT NULL
        AND dp.actual_fpts IS NOT NULL
        AND ds.sport = ${sport}
        AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    ) sub
    GROUP BY sub.quartile
    ORDER BY sub.quartile
  `);
  return result.rows;
}

// ── Ownership vs Team Total correlation ──────────────────────

export type OwnershipVsTeamTotalRow = {
  impliedBucket: string;
  bucketMin: number | null;
  nProj: number;
  nActual: number;
  nFpts: number;
  avgProjOwn: number | null;
  avgActualOwn: number | null;
  avgProjFpts: number | null;
  avgActualFpts: number | null;
};

export async function getOwnershipVsTeamTotal(sport: Sport = "nba"): Promise<OwnershipVsTeamTotalRow[]> {
  if (sport !== "nba") return [];

  const result = await db.execute<OwnershipVsTeamTotalRow>(sql`
    WITH player_implied AS (
      SELECT
        dp.proj_own_pct,
        dp.actual_own_pct,
        dp.our_proj,
        dp.actual_fpts,
        CASE
          WHEN dp.team_id = mm.home_team_id
               AND mm.home_ml IS NOT NULL AND mm.away_ml IS NOT NULL AND mm.vegas_total IS NOT NULL THEN
            mm.vegas_total * (
              CASE WHEN mm.home_ml > 0 THEN 100.0/(mm.home_ml+100)
                   ELSE ABS(mm.home_ml::FLOAT)/(ABS(mm.home_ml::FLOAT)+100.0) END
              / NULLIF(
                  CASE WHEN mm.home_ml > 0 THEN 100.0/(mm.home_ml+100)
                       ELSE ABS(mm.home_ml::FLOAT)/(ABS(mm.home_ml::FLOAT)+100.0) END
                + CASE WHEN mm.away_ml > 0 THEN 100.0/(mm.away_ml+100)
                       ELSE ABS(mm.away_ml::FLOAT)/(ABS(mm.away_ml::FLOAT)+100.0) END
              , 0.0)
            )
          WHEN dp.team_id = mm.away_team_id
               AND mm.home_ml IS NOT NULL AND mm.away_ml IS NOT NULL AND mm.vegas_total IS NOT NULL THEN
            mm.vegas_total * (
              CASE WHEN mm.away_ml > 0 THEN 100.0/(mm.away_ml+100)
                   ELSE ABS(mm.away_ml::FLOAT)/(ABS(mm.away_ml::FLOAT)+100.0) END
              / NULLIF(
                  CASE WHEN mm.home_ml > 0 THEN 100.0/(mm.home_ml+100)
                       ELSE ABS(mm.home_ml::FLOAT)/(ABS(mm.home_ml::FLOAT)+100.0) END
                + CASE WHEN mm.away_ml > 0 THEN 100.0/(mm.away_ml+100)
                       ELSE ABS(mm.away_ml::FLOAT)/(ABS(mm.away_ml::FLOAT)+100.0) END
              , 0.0)
            )
          ELSE NULL
        END AS team_implied
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      JOIN nba_matchups mm ON mm.id = dp.matchup_id
      WHERE ds.sport = 'nba'
    )
    SELECT
      CASE
        WHEN team_implied < 108 THEN 'Under 108'
        WHEN team_implied < 112 THEN '108–112'
        WHEN team_implied < 116 THEN '112–116'
        WHEN team_implied < 120 THEN '116–120'
        ELSE '120+'
      END                                                                  AS "impliedBucket",
      MIN(team_implied)::FLOAT                                             AS "bucketMin",
      COUNT(*) FILTER (WHERE proj_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL)::int AS "nProj",
      COUNT(*) FILTER (WHERE actual_own_pct IS NOT NULL AND proj_own_pct IS NOT NULL)::int AS "nActual",
      COUNT(*) FILTER (WHERE our_proj IS NOT NULL AND actual_fpts IS NOT NULL)::int        AS "nFpts",
      AVG(proj_own_pct)  FILTER (WHERE proj_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL)  AS "avgProjOwn",
      AVG(actual_own_pct) FILTER (WHERE actual_own_pct IS NOT NULL AND proj_own_pct IS NOT NULL) AS "avgActualOwn",
      AVG(our_proj)    FILTER (WHERE our_proj IS NOT NULL AND actual_fpts IS NOT NULL)    AS "avgProjFpts",
      AVG(actual_fpts) FILTER (WHERE our_proj IS NOT NULL AND actual_fpts IS NOT NULL)    AS "avgActualFpts"
    FROM player_implied
    WHERE team_implied IS NOT NULL
    GROUP BY 1
    ORDER BY MIN(team_implied) ASC NULLS LAST
  `);
  return result.rows;
}

export type OwnershipModelSourceAccuracyRow = {
  label: string;
  rows: number;
  mae: number | null;
  bias: number | null;
  corr: number | null;
};

export type OwnershipModelSegmentAccuracyRow = {
  segment: string;
  rows: number;
  linestarMae: number | null;
  fieldMae: number | null;
  maeDelta: number | null;
  linestarCorr: number | null;
  fieldCorr: number | null;
};

export type OwnershipModelBucketRow = {
  bucket: string;
  bucketMin: number | null;
  rows: number;
  avgLinestarOwnPct: number | null;
  avgFieldOwnPct: number | null;
  avgActualOwnPct: number | null;
  linestarBias: number | null;
  fieldBias: number | null;
};

export type OwnershipModelSlateAccuracyRow = {
  slateId: number;
  slateDate: string;
  ownershipVersion: string;
  source: string;
  capturedAt: string | null;
  rows: number;
  linestarMae: number | null;
  fieldMae: number | null;
  maeGain: number | null;
  linestarCorr: number | null;
  fieldCorr: number | null;
  linestarBias: number | null;
  fieldBias: number | null;
};

export type OwnershipModelVersionAccuracyRow = {
  ownershipVersion: string;
  source: string;
  slates: number;
  rows: number;
  linestarMae: number | null;
  fieldMae: number | null;
  maeGain: number | null;
  linestarCorr: number | null;
  fieldCorr: number | null;
};

export type OwnershipModelMissRow = {
  slateId: number;
  slateDate: string;
  name: string;
  eligiblePositions: string | null;
  salary: number;
  lineupOrder: number | null;
  linestarOwnPct: number | null;
  fieldOwnPct: number | null;
  actualOwnPct: number | null;
  linestarAbsError: number | null;
  fieldAbsError: number | null;
  errorGain: number | null;
};

export type OwnershipDetailSort = "field-error" | "gain" | "actual" | "field-own";

export type MlbOwnershipModelReport = {
  sample: {
    slates: number;
    rows: number;
    latestVersion: string | null;
    latestSource: string | null;
    latestCapturedAt: string | null;
  };
  findings: string[];
  sources: OwnershipModelSourceAccuracyRow[];
  segments: OwnershipModelSegmentAccuracyRow[];
  buckets: OwnershipModelBucketRow[];
  recentSlates: OwnershipModelSlateAccuracyRow[];
  versions: OwnershipModelVersionAccuracyRow[];
  latestSlateMisses: OwnershipModelMissRow[];
  selectedSlate: {
    slateId: number;
    slateDate: string;
  } | null;
  selectedSlateRows: OwnershipModelMissRow[];
};

function fmtOwnershipDelta(value: number | null | undefined): string {
  if (value == null) return "0.00";
  return Math.abs(value).toFixed(2);
}

function buildMlbOwnershipModelFindings(
  sources: OwnershipModelSourceAccuracyRow[],
  segments: OwnershipModelSegmentAccuracyRow[],
  buckets: OwnershipModelBucketRow[],
): string[] {
  const findings: string[] = [];
  const field = sources.find((row) => row.label === "Field Model");
  const linestar = sources.find((row) => row.label === "LineStar");
  if (field && linestar && field.mae != null && linestar.mae != null) {
    const delta = linestar.mae - field.mae;
    if (delta > 0) {
      findings.push(`Field model is beating LineStar by ${delta.toFixed(2)} ownership points of MAE on tracked MLB slates.`);
    } else {
      findings.push(`Field model is trailing LineStar by ${Math.abs(delta).toFixed(2)} ownership points of MAE on tracked MLB slates.`);
    }
  }

  const bestSegment = segments
    .filter((row) => row.maeDelta != null && row.maeDelta > 0)
    .sort((a, b) => (b.maeDelta ?? -999) - (a.maeDelta ?? -999))[0];
  if (bestSegment?.maeDelta != null) {
    findings.push(`${bestSegment.segment} is the strongest improvement lane, cutting MAE by ${bestSegment.maeDelta.toFixed(2)} vs LineStar.`);
  }

  const worstOver = buckets
    .filter((row) => row.fieldBias != null)
    .sort((a, b) => (b.fieldBias ?? -999) - (a.fieldBias ?? -999))[0];
  if (worstOver?.fieldBias != null && worstOver.fieldBias > 0.35) {
    findings.push(`Field model still over-projects the ${worstOver.bucket} lane by ${fmtOwnershipDelta(worstOver.fieldBias)} points on average.`);
  }

  const worstUnder = buckets
    .filter((row) => row.fieldBias != null)
    .sort((a, b) => (a.fieldBias ?? 999) - (b.fieldBias ?? 999))[0];
  if (worstUnder?.fieldBias != null && worstUnder.fieldBias < -0.35) {
    findings.push(`Field model still under-projects the ${worstUnder.bucket} lane by ${fmtOwnershipDelta(worstUnder.fieldBias)} points on average.`);
  }

  return findings.slice(0, 4);
}

export async function getMlbOwnershipModelReport(
  selectedSlateId: number | null = null,
  sortBy: OwnershipDetailSort = "field-error",
): Promise<MlbOwnershipModelReport | null> {
  await ensureOwnershipExperimentTables();

  const latestRunsCte = sql`
    WITH latest_runs AS (
      SELECT DISTINCT ON (r.slate_id)
        r.id,
        r.slate_id,
        r.ownership_version,
        r.source,
        r.created_at
      FROM ownership_runs r
      JOIN dk_slates ds ON ds.id = r.slate_id
      WHERE r.sport = 'mlb'
        AND ds.sport = 'mlb'
      ORDER BY r.slate_id, r.created_at DESC, r.id DESC
    ),
    sample AS (
      SELECT
        ops.*,
        lr.ownership_version,
        lr.source,
        lr.created_at AS run_created_at
      FROM ownership_player_snapshots ops
      JOIN latest_runs lr ON lr.id = ops.run_id
      WHERE COALESCE(ops.is_out, false) = false
        AND ops.actual_own_pct IS NOT NULL
    )
  `;

  const [summaryResult, sourceResult, segmentResult, bucketResult, recentSlateResult, versionResult, latestSlateMissResult, selectedSlateRowsResult] = await Promise.all([
    db.execute<{
      slates: number;
      rows: number;
      latestVersion: string | null;
      latestSource: string | null;
      latestCapturedAt: string | null;
    }>(sql`
      ${latestRunsCte}
      SELECT
        COUNT(DISTINCT sample.slate_id)::int AS "slates",
        COUNT(*)::int AS "rows",
        (
          SELECT lr.ownership_version
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestVersion",
        (
          SELECT lr.source
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestSource",
        (
          SELECT lr.created_at::text
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestCapturedAt"
      FROM sample
    `),
    db.execute<OwnershipModelSourceAccuracyRow>(sql`
      ${latestRunsCte}
      SELECT *
      FROM (
        SELECT
          'Field Model'::text AS "label",
          COUNT(*) FILTER (WHERE sample.field_own_pct IS NOT NULL)::int AS "rows",
          AVG(ABS(sample.field_own_pct - sample.actual_own_pct))
            FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "mae",
          AVG(sample.field_own_pct - sample.actual_own_pct)
            FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "bias",
          CORR(sample.field_own_pct, sample.actual_own_pct)
            FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "corr"
        FROM sample
        UNION ALL
        SELECT
          'LineStar'::text AS "label",
          COUNT(*) FILTER (WHERE sample.linestar_own_pct IS NOT NULL)::int AS "rows",
          AVG(ABS(sample.linestar_own_pct - sample.actual_own_pct))
            FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "mae",
          AVG(sample.linestar_own_pct - sample.actual_own_pct)
            FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "bias",
          CORR(sample.linestar_own_pct, sample.actual_own_pct)
            FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "corr"
        FROM sample
        UNION ALL
        SELECT
          'Our Own'::text AS "label",
          COUNT(*) FILTER (WHERE sample.our_own_pct IS NOT NULL)::int AS "rows",
          AVG(ABS(sample.our_own_pct - sample.actual_own_pct))
            FILTER (WHERE sample.our_own_pct IS NOT NULL) AS "mae",
          AVG(sample.our_own_pct - sample.actual_own_pct)
            FILTER (WHERE sample.our_own_pct IS NOT NULL) AS "bias",
          CORR(sample.our_own_pct, sample.actual_own_pct)
            FILTER (WHERE sample.our_own_pct IS NOT NULL) AS "corr"
        FROM sample
      ) model_rows
      ORDER BY CASE "label"
        WHEN 'Field Model' THEN 1
        WHEN 'LineStar' THEN 2
        ELSE 3
      END
    `),
    db.execute<OwnershipModelSegmentAccuracyRow>(sql`
      ${latestRunsCte}
      SELECT
        segment_rows."segment",
        segment_rows."rows",
        segment_rows."linestarMae",
        segment_rows."fieldMae",
        segment_rows."maeDelta",
        segment_rows."linestarCorr",
        segment_rows."fieldCorr"
      FROM (
        SELECT
          CASE
            WHEN sample.eligible_positions LIKE '%SP%' THEN 'SP'
            WHEN sample.lineup_order BETWEEN 1 AND 4 THEN 'Hitters 1-4'
            WHEN sample.lineup_order BETWEEN 5 AND 9 THEN 'Hitters 5-9'
            ELSE 'Hitters Unknown'
          END AS "segment",
          CASE
            WHEN sample.eligible_positions LIKE '%SP%' THEN 1
            WHEN sample.lineup_order BETWEEN 1 AND 4 THEN 2
            WHEN sample.lineup_order BETWEEN 5 AND 9 THEN 3
            ELSE 4
          END AS "segmentSort",
          COUNT(*)::int AS "rows",
          AVG(ABS(sample.linestar_own_pct - sample.actual_own_pct))
            FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarMae",
          AVG(ABS(sample.field_own_pct - sample.actual_own_pct))
            FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldMae",
          AVG(ABS(sample.linestar_own_pct - sample.actual_own_pct))
            FILTER (WHERE sample.linestar_own_pct IS NOT NULL)
            - AVG(ABS(sample.field_own_pct - sample.actual_own_pct))
            FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "maeDelta",
          CORR(sample.linestar_own_pct, sample.actual_own_pct)
            FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarCorr",
          CORR(sample.field_own_pct, sample.actual_own_pct)
            FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldCorr"
        FROM sample
        GROUP BY 1, 2
      ) segment_rows
      ORDER BY segment_rows."segmentSort"
    `),
    db.execute<OwnershipModelBucketRow>(sql`
      ${latestRunsCte}
      SELECT
        CASE
          WHEN COALESCE(sample.field_own_pct, sample.linestar_own_pct) < 1 THEN '<1%'
          WHEN COALESCE(sample.field_own_pct, sample.linestar_own_pct) < 3 THEN '1-3%'
          WHEN COALESCE(sample.field_own_pct, sample.linestar_own_pct) < 6 THEN '3-6%'
          WHEN COALESCE(sample.field_own_pct, sample.linestar_own_pct) < 10 THEN '6-10%'
          WHEN COALESCE(sample.field_own_pct, sample.linestar_own_pct) < 15 THEN '10-15%'
          ELSE '15%+'
        END AS "bucket",
        MIN(COALESCE(sample.field_own_pct, sample.linestar_own_pct)) AS "bucketMin",
        COUNT(*)::int AS "rows",
        AVG(sample.linestar_own_pct) AS "avgLinestarOwnPct",
        AVG(sample.field_own_pct) AS "avgFieldOwnPct",
        AVG(sample.actual_own_pct) AS "avgActualOwnPct",
        AVG(sample.linestar_own_pct - sample.actual_own_pct)
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarBias",
        AVG(sample.field_own_pct - sample.actual_own_pct)
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldBias"
      FROM sample
      GROUP BY 1
      ORDER BY MIN(COALESCE(sample.field_own_pct, sample.linestar_own_pct)) ASC NULLS LAST
    `),
    db.execute<OwnershipModelSlateAccuracyRow>(sql`
      ${latestRunsCte}
      SELECT
        sample.slate_id::int AS "slateId",
        ds.slate_date::text AS "slateDate",
        MIN(sample.ownership_version) AS "ownershipVersion",
        MIN(sample.source) AS "source",
        MIN(sample.run_created_at)::text AS "capturedAt",
        COUNT(*)::int AS "rows",
        AVG(ABS(sample.linestar_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarMae",
        AVG(ABS(sample.field_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldMae",
        AVG(ABS(sample.linestar_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL)
          - AVG(ABS(sample.field_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "maeGain",
        CORR(sample.linestar_own_pct, sample.actual_own_pct)
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarCorr",
        CORR(sample.field_own_pct, sample.actual_own_pct)
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldCorr",
        AVG(sample.linestar_own_pct - sample.actual_own_pct)
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarBias",
        AVG(sample.field_own_pct - sample.actual_own_pct)
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldBias"
      FROM sample
      JOIN dk_slates ds ON ds.id = sample.slate_id
      GROUP BY sample.slate_id, ds.slate_date
      ORDER BY ds.slate_date DESC, sample.slate_id DESC
      LIMIT 10
    `),
    db.execute<OwnershipModelVersionAccuracyRow>(sql`
      ${latestRunsCte}
      SELECT
        sample.ownership_version AS "ownershipVersion",
        sample.source AS "source",
        COUNT(DISTINCT sample.slate_id)::int AS "slates",
        COUNT(*)::int AS "rows",
        AVG(ABS(sample.linestar_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarMae",
        AVG(ABS(sample.field_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldMae",
        AVG(ABS(sample.linestar_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL)
          - AVG(ABS(sample.field_own_pct - sample.actual_own_pct))
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "maeGain",
        CORR(sample.linestar_own_pct, sample.actual_own_pct)
          FILTER (WHERE sample.linestar_own_pct IS NOT NULL) AS "linestarCorr",
        CORR(sample.field_own_pct, sample.actual_own_pct)
          FILTER (WHERE sample.field_own_pct IS NOT NULL) AS "fieldCorr"
      FROM sample
      GROUP BY sample.ownership_version, sample.source
      ORDER BY COUNT(DISTINCT sample.slate_id) DESC, sample.ownership_version DESC, sample.source ASC
    `),
    db.execute<OwnershipModelMissRow>(sql`
      ${latestRunsCte},
      latest_completed_slate AS (
        SELECT sample.slate_id
        FROM sample
        GROUP BY sample.slate_id
        ORDER BY MAX(sample.run_created_at) DESC, sample.slate_id DESC
        LIMIT 1
      )
      SELECT
        sample.slate_id::int AS "slateId",
        ds.slate_date::text AS "slateDate",
        sample.name AS "name",
        sample.eligible_positions AS "eligiblePositions",
        sample.salary::int AS "salary",
        sample.lineup_order::int AS "lineupOrder",
        sample.linestar_own_pct AS "linestarOwnPct",
        sample.field_own_pct AS "fieldOwnPct",
        sample.actual_own_pct AS "actualOwnPct",
        ABS(sample.linestar_own_pct - sample.actual_own_pct) AS "linestarAbsError",
        ABS(sample.field_own_pct - sample.actual_own_pct) AS "fieldAbsError",
        ABS(sample.linestar_own_pct - sample.actual_own_pct)
          - ABS(sample.field_own_pct - sample.actual_own_pct) AS "errorGain"
      FROM sample
      JOIN latest_completed_slate lcs ON lcs.slate_id = sample.slate_id
      JOIN dk_slates ds ON ds.id = sample.slate_id
      ORDER BY ABS(sample.field_own_pct - sample.actual_own_pct) DESC NULLS LAST, sample.salary DESC, sample.name ASC
      LIMIT 12
    `),
    db.execute<OwnershipModelMissRow>(sql`
      ${latestRunsCte},
      target_slate AS (
        SELECT CASE
          WHEN ${selectedSlateId}::INTEGER IS NOT NULL
            AND EXISTS (SELECT 1 FROM sample WHERE slate_id = ${selectedSlateId}::INTEGER)
            THEN ${selectedSlateId}::INTEGER
          ELSE (
            SELECT sample.slate_id
            FROM sample
            GROUP BY sample.slate_id
            ORDER BY MAX(sample.run_created_at) DESC, sample.slate_id DESC
            LIMIT 1
          )
        END AS slate_id
      )
      SELECT
        sample.slate_id::int AS "slateId",
        ds.slate_date::text AS "slateDate",
        sample.name AS "name",
        sample.eligible_positions AS "eligiblePositions",
        sample.salary::int AS "salary",
        sample.lineup_order::int AS "lineupOrder",
        sample.linestar_own_pct AS "linestarOwnPct",
        sample.field_own_pct AS "fieldOwnPct",
        sample.actual_own_pct AS "actualOwnPct",
        ABS(sample.linestar_own_pct - sample.actual_own_pct) AS "linestarAbsError",
        ABS(sample.field_own_pct - sample.actual_own_pct) AS "fieldAbsError",
        ABS(sample.linestar_own_pct - sample.actual_own_pct)
          - ABS(sample.field_own_pct - sample.actual_own_pct) AS "errorGain"
      FROM sample
      JOIN target_slate ts ON ts.slate_id = sample.slate_id
      JOIN dk_slates ds ON ds.id = sample.slate_id
    `),
  ]);

  const summary = summaryResult.rows[0];
  if (!summary || summary.rows === 0) return null;

  const sources = sourceResult.rows.map((row) => ({
    label: row.label,
    rows: Number(row.rows ?? 0),
    mae: row.mae == null ? null : Number(row.mae),
    bias: row.bias == null ? null : Number(row.bias),
    corr: row.corr == null ? null : Number(row.corr),
  }));
  const segments = segmentResult.rows.map((row) => ({
    segment: row.segment,
    rows: Number(row.rows ?? 0),
    linestarMae: row.linestarMae == null ? null : Number(row.linestarMae),
    fieldMae: row.fieldMae == null ? null : Number(row.fieldMae),
    maeDelta: row.maeDelta == null ? null : Number(row.maeDelta),
    linestarCorr: row.linestarCorr == null ? null : Number(row.linestarCorr),
    fieldCorr: row.fieldCorr == null ? null : Number(row.fieldCorr),
  }));
  const buckets = bucketResult.rows.map((row) => ({
    bucket: row.bucket,
    bucketMin: row.bucketMin == null ? null : Number(row.bucketMin),
    rows: Number(row.rows ?? 0),
    avgLinestarOwnPct: row.avgLinestarOwnPct == null ? null : Number(row.avgLinestarOwnPct),
    avgFieldOwnPct: row.avgFieldOwnPct == null ? null : Number(row.avgFieldOwnPct),
    avgActualOwnPct: row.avgActualOwnPct == null ? null : Number(row.avgActualOwnPct),
    linestarBias: row.linestarBias == null ? null : Number(row.linestarBias),
    fieldBias: row.fieldBias == null ? null : Number(row.fieldBias),
  }));
  const recentSlates = recentSlateResult.rows.map((row) => ({
    slateId: Number(row.slateId),
    slateDate: row.slateDate,
    ownershipVersion: row.ownershipVersion,
    source: row.source,
    capturedAt: row.capturedAt ?? null,
    rows: Number(row.rows ?? 0),
    linestarMae: row.linestarMae == null ? null : Number(row.linestarMae),
    fieldMae: row.fieldMae == null ? null : Number(row.fieldMae),
    maeGain: row.maeGain == null ? null : Number(row.maeGain),
    linestarCorr: row.linestarCorr == null ? null : Number(row.linestarCorr),
    fieldCorr: row.fieldCorr == null ? null : Number(row.fieldCorr),
    linestarBias: row.linestarBias == null ? null : Number(row.linestarBias),
    fieldBias: row.fieldBias == null ? null : Number(row.fieldBias),
  }));
  const versions = versionResult.rows.map((row) => ({
    ownershipVersion: row.ownershipVersion,
    source: row.source,
    slates: Number(row.slates ?? 0),
    rows: Number(row.rows ?? 0),
    linestarMae: row.linestarMae == null ? null : Number(row.linestarMae),
    fieldMae: row.fieldMae == null ? null : Number(row.fieldMae),
    maeGain: row.maeGain == null ? null : Number(row.maeGain),
    linestarCorr: row.linestarCorr == null ? null : Number(row.linestarCorr),
    fieldCorr: row.fieldCorr == null ? null : Number(row.fieldCorr),
  }));
  const latestSlateMisses = latestSlateMissResult.rows.map((row) => ({
    slateId: Number(row.slateId),
    slateDate: row.slateDate,
    name: row.name,
    eligiblePositions: row.eligiblePositions ?? null,
    salary: Number(row.salary ?? 0),
    lineupOrder: row.lineupOrder == null ? null : Number(row.lineupOrder),
    linestarOwnPct: row.linestarOwnPct == null ? null : Number(row.linestarOwnPct),
    fieldOwnPct: row.fieldOwnPct == null ? null : Number(row.fieldOwnPct),
    actualOwnPct: row.actualOwnPct == null ? null : Number(row.actualOwnPct),
    linestarAbsError: row.linestarAbsError == null ? null : Number(row.linestarAbsError),
    fieldAbsError: row.fieldAbsError == null ? null : Number(row.fieldAbsError),
    errorGain: row.errorGain == null ? null : Number(row.errorGain),
  }));
  const selectedSlateRows = selectedSlateRowsResult.rows.map((row) => ({
    slateId: Number(row.slateId),
    slateDate: row.slateDate,
    name: row.name,
    eligiblePositions: row.eligiblePositions ?? null,
    salary: Number(row.salary ?? 0),
    lineupOrder: row.lineupOrder == null ? null : Number(row.lineupOrder),
    linestarOwnPct: row.linestarOwnPct == null ? null : Number(row.linestarOwnPct),
    fieldOwnPct: row.fieldOwnPct == null ? null : Number(row.fieldOwnPct),
    actualOwnPct: row.actualOwnPct == null ? null : Number(row.actualOwnPct),
    linestarAbsError: row.linestarAbsError == null ? null : Number(row.linestarAbsError),
    fieldAbsError: row.fieldAbsError == null ? null : Number(row.fieldAbsError),
    errorGain: row.errorGain == null ? null : Number(row.errorGain),
  }));

  const compareNullLastDesc = (a: number | null, b: number | null) => {
    if (a == null && b == null) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    return b - a;
  };

  selectedSlateRows.sort((a, b) => {
    switch (sortBy) {
      case "gain":
        return compareNullLastDesc(a.errorGain, b.errorGain)
          || compareNullLastDesc(a.fieldAbsError, b.fieldAbsError)
          || (b.salary - a.salary)
          || a.name.localeCompare(b.name);
      case "actual":
        return compareNullLastDesc(a.actualOwnPct, b.actualOwnPct)
          || compareNullLastDesc(a.fieldAbsError, b.fieldAbsError)
          || (b.salary - a.salary)
          || a.name.localeCompare(b.name);
      case "field-own":
        return compareNullLastDesc(a.fieldOwnPct, b.fieldOwnPct)
          || compareNullLastDesc(a.fieldAbsError, b.fieldAbsError)
          || (b.salary - a.salary)
          || a.name.localeCompare(b.name);
      case "field-error":
      default:
        return compareNullLastDesc(a.fieldAbsError, b.fieldAbsError)
          || compareNullLastDesc(a.errorGain, b.errorGain)
          || (b.salary - a.salary)
          || a.name.localeCompare(b.name);
    }
  });

  const selectedSlate = selectedSlateRows.length > 0
    ? {
        slateId: selectedSlateRows[0].slateId,
        slateDate: selectedSlateRows[0].slateDate,
      }
    : null;

  return {
    sample: {
      slates: Number(summary.slates ?? 0),
      rows: Number(summary.rows ?? 0),
      latestVersion: summary.latestVersion ?? null,
      latestSource: summary.latestSource ?? null,
      latestCapturedAt: summary.latestCapturedAt ?? null,
    },
    findings: buildMlbOwnershipModelFindings(sources, segments, buckets),
    sources,
    segments,
    buckets,
    recentSlates,
    versions,
    latestSlateMisses,
    selectedSlate,
    selectedSlateRows,
  };
}

export type MlbBlowupRankSummaryRow = {
  candidateRank: number;
  rows: number;
  avgScore: number | null;
  avgProj: number | null;
  avgCeiling: number | null;
  avgActual: number | null;
  avgActualOwn: number | null;
  avgBeat: number | null;
  hit15Rate: number | null;
  hit20Rate: number | null;
  hit25Rate: number | null;
};

export type MlbBlowupRecentSlateRow = {
  slateId: number;
  slateDate: string;
  analysisVersion: string;
  source: string;
  capturedAt: string | null;
  rows: number;
  avgActual: number | null;
  avgActualOwn: number | null;
  hits15: number;
  hits20: number;
  hits25: number;
  bestActual: number | null;
  bestPlayer: string | null;
};

export type MlbBlowupSlateDetailRow = {
  slateId: number;
  slateDate: string;
  candidateRank: number;
  name: string;
  teamAbbrev: string | null;
  eligiblePositions: string | null;
  lineupOrder: number | null;
  salary: number;
  teamTotal: number | null;
  projectedFpts: number | null;
  projectedCeiling: number | null;
  projectedValue: number | null;
  blowupScore: number | null;
  actualFpts: number | null;
  actualOwnPct: number | null;
};

export type MlbBlowupCandidateReport = {
  sample: {
    slates: number;
    rows: number;
    latestVersion: string | null;
    latestSource: string | null;
    latestCapturedAt: string | null;
  };
  findings: string[];
  rankSummary: MlbBlowupRankSummaryRow[];
  recentSlates: MlbBlowupRecentSlateRow[];
  latestSlate: {
    slateId: number;
    slateDate: string;
  } | null;
  latestSlateRows: MlbBlowupSlateDetailRow[];
};

function buildMlbBlowupFindings(
  rankSummary: MlbBlowupRankSummaryRow[],
  recentSlates: MlbBlowupRecentSlateRow[],
): string[] {
  const findings: string[] = [];

  const topThree = rankSummary.filter((row) => row.candidateRank <= 3);
  const backHalf = rankSummary.filter((row) => row.candidateRank >= 10);
  if (topThree.length > 0 && backHalf.length > 0) {
    const topThreeHit20 = topThree.reduce((sum, row) => sum + (row.hit20Rate ?? 0), 0) / topThree.length;
    const backHalfHit20 = backHalf.reduce((sum, row) => sum + (row.hit20Rate ?? 0), 0) / backHalf.length;
    findings.push(`Top-3 blowup ranks are hitting 20+ DK points at ${topThreeHit20.toFixed(1)}% versus ${backHalfHit20.toFixed(1)}% for ranks 10-12.`);
  }

  const bestRank = [...rankSummary]
    .filter((row) => row.hit25Rate != null)
    .sort((a, b) => (b.hit25Rate ?? -999) - (a.hit25Rate ?? -999))[0];
  if (bestRank?.hit25Rate != null) {
    findings.push(`Rank #${bestRank.candidateRank} has been the strongest slate-winning lane so far, reaching 25+ DK points ${bestRank.hit25Rate.toFixed(1)}% of the time.`);
  }

  const recent = recentSlates.slice(0, 8);
  if (recent.length > 0) {
    const avgHits20 = recent.reduce((sum, row) => sum + row.hits20, 0) / recent.length;
    findings.push(`Recent MLB slates are producing ${avgHits20.toFixed(1)} 20+ DK games on average from the tracked top-12 blowup list.`);
  }

  const ownershipSignal = rankSummary
    .filter((row) => row.hit20Rate != null && row.avgActualOwn != null)
    .sort((a, b) => (b.hit20Rate ?? -999) - (a.hit20Rate ?? -999))[0];
  if (ownershipSignal?.avgActualOwn != null) {
    findings.push(`The best-performing blowup rank is still coming in at only ${ownershipSignal.avgActualOwn.toFixed(2)}% actual ownership on average.`);
  }

  return findings.slice(0, 4);
}

export async function getMlbBlowupCandidateReport(): Promise<MlbBlowupCandidateReport | null> {
  await ensureMlbBlowupTrackingTables();

  const latestRunsCte = sql`
    WITH latest_runs AS (
      SELECT DISTINCT ON (r.slate_id)
        r.id,
        r.slate_id,
        r.analysis_version,
        r.source,
        r.created_at
      FROM mlb_blowup_runs r
      JOIN dk_slates ds ON ds.id = r.slate_id
      WHERE ds.sport = 'mlb'
      ORDER BY r.slate_id, r.created_at DESC, r.id DESC
    ),
    sample AS (
      SELECT
        s.*,
        lr.analysis_version,
        lr.source,
        lr.created_at AS run_created_at,
        ds.slate_date::text AS slate_date
      FROM mlb_blowup_player_snapshots s
      JOIN latest_runs lr ON lr.id = s.run_id
      JOIN dk_slates ds ON ds.id = s.slate_id
      WHERE s.actual_fpts IS NOT NULL
    )
  `;

  const [summaryResult, rankResult, recentSlateResult, latestSlateRowsResult] = await Promise.all([
    db.execute<{
      slates: number;
      rows: number;
      latestVersion: string | null;
      latestSource: string | null;
      latestCapturedAt: string | null;
    }>(sql`
      ${latestRunsCte}
      SELECT
        COUNT(DISTINCT sample.slate_id)::int AS "slates",
        COUNT(*)::int AS "rows",
        (
          SELECT lr.analysis_version
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestVersion",
        (
          SELECT lr.source
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestSource",
        (
          SELECT lr.created_at::text
          FROM latest_runs lr
          ORDER BY lr.created_at DESC, lr.id DESC
          LIMIT 1
        ) AS "latestCapturedAt"
      FROM sample
    `),
    db.execute<MlbBlowupRankSummaryRow>(sql`
      ${latestRunsCte}
      SELECT
        sample.candidate_rank AS "candidateRank",
        COUNT(*)::int AS "rows",
        AVG(sample.blowup_score) AS "avgScore",
        AVG(sample.projected_fpts) AS "avgProj",
        AVG(sample.projected_ceiling) AS "avgCeiling",
        AVG(sample.actual_fpts) AS "avgActual",
        AVG(sample.actual_own_pct) AS "avgActualOwn",
        AVG(sample.actual_fpts - sample.projected_fpts) AS "avgBeat",
        AVG(CASE WHEN sample.actual_fpts >= 15 THEN 1 ELSE 0 END) * 100 AS "hit15Rate",
        AVG(CASE WHEN sample.actual_fpts >= 20 THEN 1 ELSE 0 END) * 100 AS "hit20Rate",
        AVG(CASE WHEN sample.actual_fpts >= 25 THEN 1 ELSE 0 END) * 100 AS "hit25Rate"
      FROM sample
      GROUP BY sample.candidate_rank
      ORDER BY sample.candidate_rank ASC
    `),
    db.execute<MlbBlowupRecentSlateRow>(sql`
      ${latestRunsCte}
      SELECT
        sample.slate_id AS "slateId",
        sample.slate_date AS "slateDate",
        sample.analysis_version AS "analysisVersion",
        sample.source AS "source",
        sample.run_created_at::text AS "capturedAt",
        COUNT(*)::int AS "rows",
        AVG(sample.actual_fpts) AS "avgActual",
        AVG(sample.actual_own_pct) AS "avgActualOwn",
        COUNT(*) FILTER (WHERE sample.actual_fpts >= 15)::int AS "hits15",
        COUNT(*) FILTER (WHERE sample.actual_fpts >= 20)::int AS "hits20",
        COUNT(*) FILTER (WHERE sample.actual_fpts >= 25)::int AS "hits25",
        MAX(sample.actual_fpts) AS "bestActual",
        (
          SELECT s2.name
          FROM sample s2
          WHERE s2.slate_id = sample.slate_id
          ORDER BY s2.actual_fpts DESC NULLS LAST, s2.candidate_rank ASC
          LIMIT 1
        ) AS "bestPlayer"
      FROM sample
      GROUP BY sample.slate_id, sample.slate_date, sample.analysis_version, sample.source, sample.run_created_at
      ORDER BY sample.slate_date DESC, sample.slate_id DESC
      LIMIT 15
    `),
    db.execute<MlbBlowupSlateDetailRow>(sql`
      ${latestRunsCte}
      , latest_completed_slate AS (
        SELECT sample.slate_id, sample.slate_date
        FROM sample
        GROUP BY sample.slate_id, sample.slate_date
        ORDER BY sample.slate_date DESC, sample.slate_id DESC
        LIMIT 1
      )
      SELECT
        sample.slate_id AS "slateId",
        sample.slate_date AS "slateDate",
        sample.candidate_rank AS "candidateRank",
        sample.name AS "name",
        sample.team_abbrev AS "teamAbbrev",
        sample.eligible_positions AS "eligiblePositions",
        sample.lineup_order AS "lineupOrder",
        sample.salary AS "salary",
        sample.team_total AS "teamTotal",
        sample.projected_fpts AS "projectedFpts",
        sample.projected_ceiling AS "projectedCeiling",
        sample.projected_value AS "projectedValue",
        sample.blowup_score AS "blowupScore",
        sample.actual_fpts AS "actualFpts",
        sample.actual_own_pct AS "actualOwnPct"
      FROM sample
      JOIN latest_completed_slate lcs ON lcs.slate_id = sample.slate_id
      ORDER BY sample.candidate_rank ASC, sample.blowup_score DESC NULLS LAST, sample.name ASC
    `),
  ]);

  const summary = summaryResult.rows[0];
  if (!summary || Number(summary.rows ?? 0) === 0) return null;

  const rankSummary = rankResult.rows.map((row) => ({
    candidateRank: Number(row.candidateRank),
    rows: Number(row.rows),
    avgScore: row.avgScore == null ? null : Number(row.avgScore),
    avgProj: row.avgProj == null ? null : Number(row.avgProj),
    avgCeiling: row.avgCeiling == null ? null : Number(row.avgCeiling),
    avgActual: row.avgActual == null ? null : Number(row.avgActual),
    avgActualOwn: row.avgActualOwn == null ? null : Number(row.avgActualOwn),
    avgBeat: row.avgBeat == null ? null : Number(row.avgBeat),
    hit15Rate: row.hit15Rate == null ? null : Number(row.hit15Rate),
    hit20Rate: row.hit20Rate == null ? null : Number(row.hit20Rate),
    hit25Rate: row.hit25Rate == null ? null : Number(row.hit25Rate),
  }));

  const recentSlates = recentSlateResult.rows.map((row) => ({
    slateId: Number(row.slateId),
    slateDate: row.slateDate,
    analysisVersion: row.analysisVersion,
    source: row.source,
    capturedAt: row.capturedAt ?? null,
    rows: Number(row.rows),
    avgActual: row.avgActual == null ? null : Number(row.avgActual),
    avgActualOwn: row.avgActualOwn == null ? null : Number(row.avgActualOwn),
    hits15: Number(row.hits15),
    hits20: Number(row.hits20),
    hits25: Number(row.hits25),
    bestActual: row.bestActual == null ? null : Number(row.bestActual),
    bestPlayer: row.bestPlayer ?? null,
  }));

  const latestSlateRows = latestSlateRowsResult.rows.map((row) => ({
    slateId: Number(row.slateId),
    slateDate: row.slateDate,
    candidateRank: Number(row.candidateRank),
    name: row.name,
    teamAbbrev: row.teamAbbrev ?? null,
    eligiblePositions: row.eligiblePositions ?? null,
    lineupOrder: row.lineupOrder == null ? null : Number(row.lineupOrder),
    salary: Number(row.salary ?? 0),
    teamTotal: row.teamTotal == null ? null : Number(row.teamTotal),
    projectedFpts: row.projectedFpts == null ? null : Number(row.projectedFpts),
    projectedCeiling: row.projectedCeiling == null ? null : Number(row.projectedCeiling),
    projectedValue: row.projectedValue == null ? null : Number(row.projectedValue),
    blowupScore: row.blowupScore == null ? null : Number(row.blowupScore),
    actualFpts: row.actualFpts == null ? null : Number(row.actualFpts),
    actualOwnPct: row.actualOwnPct == null ? null : Number(row.actualOwnPct),
  }));

  const latestSlate = latestSlateRows.length > 0
    ? {
        slateId: latestSlateRows[0].slateId,
        slateDate: latestSlateRows[0].slateDate,
      }
    : null;

  return {
    sample: {
      slates: Number(summary.slates ?? 0),
      rows: Number(summary.rows ?? 0),
      latestVersion: summary.latestVersion ?? null,
      latestSource: summary.latestSource ?? null,
      latestCapturedAt: summary.latestCapturedAt ?? null,
    },
    findings: buildMlbBlowupFindings(rankSummary, recentSlates),
    rankSummary,
    recentSlates,
    latestSlate,
    latestSlateRows,
  };
}

const MLB_POSTMORTEM_RECENT_SLATES = 5;
const MLB_POSTMORTEM_PRIOR_SLATES = 5;
const MLB_POSTMORTEM_MAX_SLATES = 30;
const MLB_POSTMORTEM_FALLBACK_WARNING_PCT = 30;
const MLB_POSTMORTEM_INDEPENDENCE_WARNING_MAE = 1;

export type MlbPostmortemProjectionRow = {
  windowLabel: string;
  windowSort: number;
  playerGroup: string;
  rows: number;
  slates: number;
  finalMae: number | null;
  finalBias: number | null;
  liveMae: number | null;
  liveBias: number | null;
  ourMae: number | null;
  ourBias: number | null;
  linestarMae: number | null;
  linestarBias: number | null;
  finalGainVsLineStar: number | null;
};

export type MlbPostmortemProjectionSourceRow = {
  windowLabel: string;
  windowSort: number;
  projectionSource: string;
  rows: number;
  pctRows: number | null;
  mae: number | null;
  bias: number | null;
};

export type MlbPostmortemIndependenceRow = {
  windowLabel: string;
  windowSort: number;
  rows: number;
  finalRows: number;
  fallbackRows: number;
  fallbackPct: number | null;
  finalMae: number | null;
  rawOurMae: number | null;
  ourSourceMae: number | null;
  nonLineStarMae: number | null;
  fallbackMae: number | null;
  nonFallbackMae: number | null;
  linestarMae: number | null;
  blendUplift: number | null;
  fallbackDelta: number | null;
  warning: string | null;
};

export type MlbPostmortemOwnershipRow = {
  windowLabel: string;
  windowSort: number;
  rows: number;
  slates: number;
  fieldRows: number;
  linestarRows: number;
  fieldMae: number | null;
  fieldBias: number | null;
  fieldCorr: number | null;
  linestarMae: number | null;
  linestarBias: number | null;
  linestarCorr: number | null;
  fieldGainVsLineStar: number | null;
};

export type MlbPostmortemOwnershipChalkRow = {
  windowLabel: string;
  windowSort: number;
  threshold: number;
  rows: number;
  actualChalkRows: number;
  projectedChalkRows: number;
  capturedRows: number;
  captureRate: number | null;
  falseLowRows: number;
  falseChalkRows: number;
};

export type MlbPostmortemOwnershipRankingRow = {
  windowLabel: string;
  windowSort: number;
  topN: number;
  rows: number;
  actualTopRows: number;
  capturedRows: number;
  overlapPct: number | null;
  spearman: number | null;
};

export type MlbPostmortemLeverageErrorRow = {
  windowLabel: string;
  windowSort: number;
  rows: number;
  highImpactRows: number;
  leverageErrorRows: number;
  leverageErrorRate: number | null;
  avgAbsError: number | null;
};

export type MlbPostmortemSignalRow = {
  signal: string;
  rows: number;
  slates: number;
  avgProjection: number | null;
  avgActual: number | null;
  avgActualOwn: number | null;
  avgBeat: number | null;
  hit15Rate: number | null;
  hit20Rate: number | null;
  hit25Rate: number | null;
  baseline20Rate: number | null;
  lift20Rate: number | null;
  baseline25Rate: number | null;
  lift25Rate: number | null;
};

export type MlbPostmortemRecentSlateRow = {
  slateId: number;
  slateDate: string;
  playerRows: number;
  finalMae: number | null;
  finalBias: number | null;
  linestarMae: number | null;
  fieldOwnMae: number | null;
  linestarOwnMae: number | null;
  hrBadgeRows: number;
  hrBadgeAvgActual: number | null;
  blowupHit20: number | null;
};

export type MlbPostmortemProjectionMissRow = {
  slateId: number;
  slateDate: string;
  name: string;
  teamAbbrev: string | null;
  eligiblePositions: string | null;
  salary: number;
  lineupOrder: number | null;
  projection: number | null;
  linestarProjection: number | null;
  actualFpts: number | null;
  actualOwnPct: number | null;
  miss: number | null;
};

export type MlbPostmortemOwnershipMissRow = {
  slateId: number;
  slateDate: string;
  name: string;
  teamAbbrev: string | null;
  eligiblePositions: string | null;
  salary: number;
  lineupOrder: number | null;
  fieldOwnPct: number | null;
  linestarOwnPct: number | null;
  actualOwnPct: number | null;
  fieldAbsError: number | null;
  linestarAbsError: number | null;
  fieldGainVsLineStar: number | null;
};

export type MlbPostmortemPitcherExploitRow = {
  slateId: number;
  slateDate: string;
  name: string;
  teamAbbrev: string | null;
  salary: number;
  projection: number | null;
  linestarProjection: number | null;
  marketGap: number | null;
  fieldOwnPct: number | null;
  actualOwnPct: number | null;
  actualFpts: number | null;
  valueMultiple: number | null;
  opponentImplied: number | null;
  moneyline: number | null;
  score: number | null;
};

export type MlbPostmortemDecisionCaptureRow = {
  windowLabel: string;
  windowSort: number;
  outcomeBucket: string;
  outcomeRows: number;
  highProjectionCaptureRate: number | null;
  ceilingCaptureRate: number | null;
  leverageCaptureRate: number | null;
  avgActualOwn: number | null;
};

export type MlbPostmortemReport = {
  sample: {
    recentSlateCount: number;
    priorSlateCount: number;
    latestSlateId: number | null;
    latestSlateDate: string | null;
    recentStartDate: string | null;
    recentEndDate: string | null;
    priorStartDate: string | null;
    priorEndDate: string | null;
    playerRows: number;
    ownershipRows: number;
  };
  findings: string[];
  warnings: string[];
  projectionSummary: MlbPostmortemProjectionRow[];
  projectionIndependence: MlbPostmortemIndependenceRow[];
  projectionSources: MlbPostmortemProjectionSourceRow[];
  ownershipSummary: MlbPostmortemOwnershipRow[];
  ownershipChalk: MlbPostmortemOwnershipChalkRow[];
  ownershipRanking: MlbPostmortemOwnershipRankingRow[];
  leverageErrors: MlbPostmortemLeverageErrorRow[];
  signalFollowThrough: MlbPostmortemSignalRow[];
  pitcherExploitWatch: MlbPostmortemPitcherExploitRow[];
  decisionCapture: MlbPostmortemDecisionCaptureRow[];
  recentSlates: MlbPostmortemRecentSlateRow[];
  projectionMisses: MlbPostmortemProjectionMissRow[];
  ownershipMisses: MlbPostmortemOwnershipMissRow[];
};

function mlbPostmortemNumber(value: unknown): number | null {
  if (value == null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function buildMlbPostmortemFindings(
  projectionSummary: MlbPostmortemProjectionRow[],
  projectionIndependence: MlbPostmortemIndependenceRow[],
  ownershipSummary: MlbPostmortemOwnershipRow[],
  signalFollowThrough: MlbPostmortemSignalRow[],
): string[] {
  const findings: string[] = [];
  const recentAll = projectionSummary.find((row) => row.windowSort === 1 && row.playerGroup === "All Active");
  const priorAll = projectionSummary.find((row) => row.windowSort === 2 && row.playerGroup === "All Active");

  if (recentAll?.finalMae != null && recentAll.linestarMae != null) {
    const gain = recentAll.linestarMae - recentAll.finalMae;
    findings.push(
      gain >= 0
        ? `Recent blended projections are beating LineStar by ${gain.toFixed(2)} DK points of MAE.`
        : `Recent blended projections are trailing LineStar by ${Math.abs(gain).toFixed(2)} DK points of MAE.`,
    );
  }

  const recentIndependence = projectionIndependence.find((row) => row.windowSort === 1);
  if (recentIndependence?.fallbackPct != null) {
    findings.push(`LineStar fallback dependency is ${recentIndependence.fallbackPct.toFixed(1)}% in the recent completed-slate window.`);
  }

  if (recentIndependence?.blendUplift != null) {
    findings.push(
      recentIndependence.blendUplift >= 0
        ? `The final projection layer improves raw our_proj by ${recentIndependence.blendUplift.toFixed(2)} DK points of MAE.`
        : `The final projection layer is ${Math.abs(recentIndependence.blendUplift).toFixed(2)} DK points worse than raw our_proj.`,
    );
  }

  if (recentAll?.finalMae != null && priorAll?.finalMae != null) {
    const delta = priorAll.finalMae - recentAll.finalMae;
    findings.push(
      delta >= 0
        ? `Projection MAE improved by ${delta.toFixed(2)} DK points versus the prior completed-slate window.`
        : `Projection MAE worsened by ${Math.abs(delta).toFixed(2)} DK points versus the prior completed-slate window.`,
    );
  }

  const recentOwn = ownershipSummary.find((row) => row.windowSort === 1);
  if (recentOwn?.fieldMae != null && recentOwn.linestarMae != null) {
    const gain = recentOwn.linestarMae - recentOwn.fieldMae;
    findings.push(
      gain >= 0
        ? `Recent field ownership is beating LineStar by ${gain.toFixed(2)} ownership points of MAE.`
        : `Recent field ownership is trailing LineStar by ${Math.abs(gain).toFixed(2)} ownership points of MAE.`,
    );
  }

  const hrSignal = signalFollowThrough.find((row) => row.signal === "HR Badge 25%+");
  if (hrSignal?.hit20Rate != null && hrSignal.rows > 0) {
    findings.push(`HR-badge hitters reached 20+ DK points ${hrSignal.hit20Rate.toFixed(1)}% of the time in the postmortem sample.`);
  }

  const blowupSignal = signalFollowThrough.find((row) => row.signal === "Blowup Top 12");
  if (blowupSignal?.hit20Rate != null && blowupSignal.rows > 0) {
    findings.push(`Tracked blowup candidates reached 20+ DK points ${blowupSignal.hit20Rate.toFixed(1)}% of the time.`);
  }

  return findings.slice(0, 6);
}

export async function getMlbPostmortemReport(): Promise<MlbPostmortemReport | null> {
  await ensureMlbBlowupTrackingTables();

  const completedSlatesCte = sql`
    WITH completed_slates AS (
      SELECT
        slate_rows.slate_id,
        slate_rows.slate_date,
        ROW_NUMBER() OVER (ORDER BY slate_rows.slate_date DESC, slate_rows.slate_id DESC) AS slate_rank
      FROM (
        SELECT ds.id AS slate_id, ds.slate_date
        FROM dk_slates ds
        JOIN dk_players dp ON dp.slate_id = ds.id
        WHERE ds.sport = 'mlb'
        GROUP BY ds.id, ds.slate_date
        HAVING COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL) > 0
      ) slate_rows
    ),
    analysis_windows AS (
      SELECT *
      FROM (
        VALUES
          ('Recent 5'::text, 1::int, 1::int, ${MLB_POSTMORTEM_RECENT_SLATES}::int),
          ('Prior 5'::text, 2::int, ${MLB_POSTMORTEM_RECENT_SLATES + 1}::int, ${MLB_POSTMORTEM_RECENT_SLATES + MLB_POSTMORTEM_PRIOR_SLATES}::int),
          ('Rolling 10'::text, 3::int, 1::int, 10::int),
          ('Rolling 30'::text, 4::int, 1::int, ${MLB_POSTMORTEM_MAX_SLATES}::int)
      ) AS windows(window_label, window_sort, start_rank, end_rank)
    ),
    windowed_slates AS (
      SELECT
        cs.slate_id,
        cs.slate_date,
        cs.slate_rank,
        aw.window_label,
        aw.window_sort
      FROM completed_slates cs
      JOIN analysis_windows aw ON cs.slate_rank BETWEEN aw.start_rank AND aw.end_rank
    ),
    player_sample AS (
      SELECT
        ws.*,
        dp.dk_player_id,
        dp.matchup_id,
        dp.mlb_team_id,
        dp.name,
        dp.team_abbrev,
        dp.eligible_positions,
        dp.salary,
        dp.dk_starting_lineup_order AS lineup_order,
        dp.actual_fpts,
        dp.actual_own_pct,
        dp.live_proj,
        dp.our_proj,
        dp.linestar_proj,
        COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS final_proj,
        CASE
          WHEN dp.live_proj IS NOT NULL THEN 'live'
          WHEN dp.our_proj IS NOT NULL THEN 'our'
          WHEN dp.linestar_proj IS NOT NULL THEN 'linestar'
          ELSE 'unknown'
        END AS projection_source,
        COALESCE(dp.live_own_pct, dp.our_own_pct) AS field_own_pct,
        COALESCE(dp.linestar_own_pct, dp.proj_own_pct) AS linestar_own_pct,
        dp.proj_ceiling,
        dp.boom_rate,
        dp.hr_prob_1plus,
        CASE WHEN dp.eligible_positions LIKE '%SP%' OR dp.eligible_positions LIKE '%RP%' THEN true ELSE false END AS is_pitcher
      FROM windowed_slates ws
      JOIN dk_players dp ON dp.slate_id = ws.slate_id
      WHERE COALESCE(dp.is_out, false) = false
        AND dp.actual_fpts IS NOT NULL
        AND NOT (dp.eligible_positions LIKE '%SP%' AND dp.actual_fpts = 0)
    )
  `;

  const summaryResult = await db.execute<{
      recentSlateCount: number;
      priorSlateCount: number;
      latestSlateId: number | null;
      latestSlateDate: string | null;
      recentStartDate: string | null;
      recentEndDate: string | null;
      priorStartDate: string | null;
      priorEndDate: string | null;
      playerRows: number;
      ownershipRows: number;
    }>(sql`
      ${completedSlatesCte}
      SELECT
        COUNT(DISTINCT slate_id) FILTER (WHERE window_sort = 1)::int AS "recentSlateCount",
        COUNT(DISTINCT slate_id) FILTER (WHERE window_sort = 2)::int AS "priorSlateCount",
        MAX(slate_id) FILTER (WHERE slate_rank = 1)::int AS "latestSlateId",
        MAX(slate_date) FILTER (WHERE slate_rank = 1)::text AS "latestSlateDate",
        MIN(slate_date) FILTER (WHERE window_sort = 1)::text AS "recentStartDate",
        MAX(slate_date) FILTER (WHERE window_sort = 1)::text AS "recentEndDate",
        MIN(slate_date) FILTER (WHERE window_sort = 2)::text AS "priorStartDate",
        MAX(slate_date) FILTER (WHERE window_sort = 2)::text AS "priorEndDate",
        COUNT(*) FILTER (WHERE window_sort IN (1, 2))::int AS "playerRows",
        COUNT(*) FILTER (WHERE window_sort IN (1, 2) AND actual_own_pct IS NOT NULL)::int AS "ownershipRows"
      FROM player_sample
    `);

  const projectionResult = await db.execute<MlbPostmortemProjectionRow>(sql`
      ${completedSlatesCte}
      SELECT
        grouped.window_label AS "windowLabel",
        grouped.window_sort::int AS "windowSort",
        grouped.player_group AS "playerGroup",
        COUNT(*)::int AS "rows",
        COUNT(DISTINCT grouped.slate_id)::int AS "slates",
        AVG(ABS(grouped.final_proj - grouped.actual_fpts)) FILTER (WHERE grouped.final_proj IS NOT NULL) AS "finalMae",
        AVG(grouped.final_proj - grouped.actual_fpts) FILTER (WHERE grouped.final_proj IS NOT NULL) AS "finalBias",
        AVG(ABS(grouped.live_proj - grouped.actual_fpts)) FILTER (WHERE grouped.live_proj IS NOT NULL) AS "liveMae",
        AVG(grouped.live_proj - grouped.actual_fpts) FILTER (WHERE grouped.live_proj IS NOT NULL) AS "liveBias",
        AVG(ABS(grouped.our_proj - grouped.actual_fpts)) FILTER (WHERE grouped.our_proj IS NOT NULL) AS "ourMae",
        AVG(grouped.our_proj - grouped.actual_fpts) FILTER (WHERE grouped.our_proj IS NOT NULL) AS "ourBias",
        AVG(ABS(grouped.linestar_proj - grouped.actual_fpts)) FILTER (WHERE grouped.linestar_proj IS NOT NULL) AS "linestarMae",
        AVG(grouped.linestar_proj - grouped.actual_fpts) FILTER (WHERE grouped.linestar_proj IS NOT NULL) AS "linestarBias",
        AVG(ABS(grouped.linestar_proj - grouped.actual_fpts)) FILTER (WHERE grouped.linestar_proj IS NOT NULL)
          - AVG(ABS(grouped.final_proj - grouped.actual_fpts)) FILTER (WHERE grouped.final_proj IS NOT NULL) AS "finalGainVsLineStar"
      FROM (
        SELECT ps.*, g.player_group
        FROM player_sample ps
        CROSS JOIN LATERAL (
          VALUES
            ('All Active'::text),
            (CASE WHEN ps.is_pitcher THEN 'Pitchers' ELSE 'Hitters' END)
        ) AS g(player_group)
      ) grouped
      GROUP BY grouped.window_label, grouped.window_sort, grouped.player_group
      ORDER BY grouped.window_sort ASC, CASE grouped.player_group WHEN 'All Active' THEN 1 WHEN 'Hitters' THEN 2 ELSE 3 END
    `);

  const projectionIndependenceResult = await db.execute<MlbPostmortemIndependenceRow>(sql`
      ${completedSlatesCte}
      SELECT
        window_label AS "windowLabel",
        window_sort::int AS "windowSort",
        COUNT(*)::int AS "rows",
        COUNT(*) FILTER (WHERE final_proj IS NOT NULL)::int AS "finalRows",
        COUNT(*) FILTER (WHERE projection_source = 'linestar' AND final_proj IS NOT NULL)::int AS "fallbackRows",
        COUNT(*) FILTER (WHERE projection_source = 'linestar' AND final_proj IS NOT NULL) * 100.0
          / NULLIF(COUNT(*) FILTER (WHERE final_proj IS NOT NULL), 0) AS "fallbackPct",
        AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE final_proj IS NOT NULL) AS "finalMae",
        AVG(ABS(our_proj - actual_fpts)) FILTER (WHERE our_proj IS NOT NULL) AS "rawOurMae",
        AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE projection_source = 'our' AND final_proj IS NOT NULL) AS "ourSourceMae",
        AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE projection_source <> 'linestar' AND final_proj IS NOT NULL) AS "nonLineStarMae",
        AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE projection_source = 'linestar' AND final_proj IS NOT NULL) AS "fallbackMae",
        AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE projection_source <> 'linestar' AND final_proj IS NOT NULL) AS "nonFallbackMae",
        AVG(ABS(linestar_proj - actual_fpts)) FILTER (WHERE linestar_proj IS NOT NULL) AS "linestarMae",
        AVG(ABS(our_proj - actual_fpts)) FILTER (WHERE our_proj IS NOT NULL)
          - AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE final_proj IS NOT NULL) AS "blendUplift",
        AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE projection_source = 'linestar' AND final_proj IS NOT NULL)
          - AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE projection_source <> 'linestar' AND final_proj IS NOT NULL) AS "fallbackDelta",
        CASE
          WHEN COUNT(*) FILTER (WHERE projection_source = 'linestar' AND final_proj IS NOT NULL) * 100.0
            / NULLIF(COUNT(*) FILTER (WHERE final_proj IS NOT NULL), 0) > ${MLB_POSTMORTEM_FALLBACK_WARNING_PCT}
            THEN 'fallback_dependency'
          WHEN AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE projection_source <> 'linestar' AND final_proj IS NOT NULL)
            - AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE final_proj IS NOT NULL) > ${MLB_POSTMORTEM_INDEPENDENCE_WARNING_MAE}
            THEN 'independent_mae_gap'
          ELSE NULL
        END AS "warning"
      FROM player_sample
      GROUP BY window_label, window_sort
      ORDER BY window_sort ASC
    `);

  const projectionSourceResult = await db.execute<MlbPostmortemProjectionSourceRow>(sql`
      ${completedSlatesCte},
      source_counts AS (
        SELECT
          window_label,
          window_sort,
          projection_source,
          COUNT(*)::int AS rows,
          AVG(ABS(final_proj - actual_fpts)) FILTER (WHERE final_proj IS NOT NULL) AS mae,
          AVG(final_proj - actual_fpts) FILTER (WHERE final_proj IS NOT NULL) AS bias
        FROM player_sample
        WHERE final_proj IS NOT NULL
        GROUP BY window_label, window_sort, projection_source
      ),
      source_totals AS (
        SELECT window_label, window_sort, SUM(rows)::int AS total_rows
        FROM source_counts
        GROUP BY window_label, window_sort
      )
      SELECT
        sc.window_label AS "windowLabel",
        sc.window_sort::int AS "windowSort",
        sc.projection_source AS "projectionSource",
        sc.rows::int AS "rows",
        sc.rows * 100.0 / NULLIF(st.total_rows, 0) AS "pctRows",
        sc.mae AS "mae",
        sc.bias AS "bias"
      FROM source_counts sc
      JOIN source_totals st
        ON st.window_label = sc.window_label
       AND st.window_sort = sc.window_sort
      ORDER BY sc.window_sort ASC, sc.rows DESC
    `);

  const ownershipResult = await db.execute<MlbPostmortemOwnershipRow>(sql`
      ${completedSlatesCte}
      SELECT
        window_label AS "windowLabel",
        window_sort::int AS "windowSort",
        COUNT(*) FILTER (WHERE actual_own_pct IS NOT NULL)::int AS "rows",
        COUNT(DISTINCT slate_id) FILTER (WHERE actual_own_pct IS NOT NULL)::int AS "slates",
        COUNT(*) FILTER (WHERE field_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL)::int AS "fieldRows",
        COUNT(*) FILTER (WHERE linestar_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL)::int AS "linestarRows",
        AVG(ABS(field_own_pct - actual_own_pct)) FILTER (WHERE field_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL) AS "fieldMae",
        AVG(field_own_pct - actual_own_pct) FILTER (WHERE field_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL) AS "fieldBias",
        CORR(field_own_pct, actual_own_pct) FILTER (WHERE field_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL) AS "fieldCorr",
        AVG(ABS(linestar_own_pct - actual_own_pct)) FILTER (WHERE linestar_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL) AS "linestarMae",
        AVG(linestar_own_pct - actual_own_pct) FILTER (WHERE linestar_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL) AS "linestarBias",
        CORR(linestar_own_pct, actual_own_pct) FILTER (WHERE linestar_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL) AS "linestarCorr",
        AVG(ABS(linestar_own_pct - actual_own_pct)) FILTER (WHERE linestar_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL)
          - AVG(ABS(field_own_pct - actual_own_pct)) FILTER (WHERE field_own_pct IS NOT NULL AND actual_own_pct IS NOT NULL) AS "fieldGainVsLineStar"
      FROM player_sample
      GROUP BY window_label, window_sort
      ORDER BY window_sort ASC
    `);

  const ownershipChalkResult = await db.execute<MlbPostmortemOwnershipChalkRow>(sql`
      ${completedSlatesCte},
      thresholds AS (
        SELECT * FROM (VALUES (20::int), (30::int), (40::int)) AS t(threshold)
      )
      SELECT
        ps.window_label AS "windowLabel",
        ps.window_sort::int AS "windowSort",
        t.threshold::int AS "threshold",
        COUNT(*) FILTER (WHERE ps.field_own_pct IS NOT NULL AND ps.actual_own_pct IS NOT NULL)::int AS "rows",
        COUNT(*) FILTER (WHERE ps.actual_own_pct >= t.threshold)::int AS "actualChalkRows",
        COUNT(*) FILTER (WHERE ps.field_own_pct >= t.threshold AND ps.actual_own_pct IS NOT NULL)::int AS "projectedChalkRows",
        COUNT(*) FILTER (WHERE ps.field_own_pct >= t.threshold AND ps.actual_own_pct >= t.threshold)::int AS "capturedRows",
        COUNT(*) FILTER (WHERE ps.field_own_pct >= t.threshold AND ps.actual_own_pct >= t.threshold) * 100.0
          / NULLIF(COUNT(*) FILTER (WHERE ps.actual_own_pct >= t.threshold), 0) AS "captureRate",
        COUNT(*) FILTER (WHERE COALESCE(ps.field_own_pct, 0) < t.threshold AND ps.actual_own_pct >= t.threshold)::int AS "falseLowRows",
        COUNT(*) FILTER (WHERE ps.field_own_pct >= t.threshold AND COALESCE(ps.actual_own_pct, 0) < t.threshold)::int AS "falseChalkRows"
      FROM player_sample ps
      CROSS JOIN thresholds t
      WHERE ps.actual_own_pct IS NOT NULL
      GROUP BY ps.window_label, ps.window_sort, t.threshold
      ORDER BY ps.window_sort ASC, t.threshold ASC
    `);

  const ownershipRankingResult = await db.execute<MlbPostmortemOwnershipRankingRow>(sql`
      ${completedSlatesCte},
      ranked AS (
        SELECT
          ps.*,
          ROW_NUMBER() OVER (PARTITION BY ps.window_sort, ps.slate_id ORDER BY ps.actual_own_pct DESC NULLS LAST, ps.salary DESC, ps.name ASC) AS actual_rank,
          ROW_NUMBER() OVER (PARTITION BY ps.window_sort, ps.slate_id ORDER BY ps.field_own_pct DESC NULLS LAST, ps.salary DESC, ps.name ASC) AS field_rank
        FROM player_sample ps
        WHERE ps.actual_own_pct IS NOT NULL
          AND ps.field_own_pct IS NOT NULL
      ),
      top_ns AS (
        SELECT * FROM (VALUES (5::int), (10::int)) AS t(top_n)
      )
      SELECT
        r.window_label AS "windowLabel",
        r.window_sort::int AS "windowSort",
        t.top_n::int AS "topN",
        COUNT(*)::int AS "rows",
        COUNT(*) FILTER (WHERE r.actual_rank <= t.top_n)::int AS "actualTopRows",
        COUNT(*) FILTER (WHERE r.actual_rank <= t.top_n AND r.field_rank <= t.top_n)::int AS "capturedRows",
        COUNT(*) FILTER (WHERE r.actual_rank <= t.top_n AND r.field_rank <= t.top_n) * 100.0
          / NULLIF(COUNT(*) FILTER (WHERE r.actual_rank <= t.top_n), 0) AS "overlapPct",
        CORR(r.actual_rank::float, r.field_rank::float) FILTER (WHERE r.actual_rank <= t.top_n OR r.field_rank <= t.top_n) AS "spearman"
      FROM ranked r
      CROSS JOIN top_ns t
      GROUP BY r.window_label, r.window_sort, t.top_n
      ORDER BY r.window_sort ASC, t.top_n ASC
    `);

  const leverageErrorResult = await db.execute<MlbPostmortemLeverageErrorRow>(sql`
      ${completedSlatesCte},
      ranked AS (
        SELECT
          ps.*,
          NTILE(4) OVER (PARTITION BY ps.window_sort, ps.slate_id ORDER BY ps.final_proj DESC NULLS LAST, ps.salary DESC, ps.name ASC) AS projection_quartile
        FROM player_sample ps
        WHERE ps.actual_own_pct IS NOT NULL
          AND ps.field_own_pct IS NOT NULL
      )
      SELECT
        window_label AS "windowLabel",
        window_sort::int AS "windowSort",
        COUNT(*)::int AS "rows",
        COUNT(*) FILTER (WHERE projection_quartile = 1 OR salary >= 7000 OR is_pitcher)::int AS "highImpactRows",
        COUNT(*) FILTER (
          WHERE (projection_quartile = 1 OR salary >= 7000 OR is_pitcher)
            AND ABS(field_own_pct - actual_own_pct) >= 10
        )::int AS "leverageErrorRows",
        COUNT(*) FILTER (
          WHERE (projection_quartile = 1 OR salary >= 7000 OR is_pitcher)
            AND ABS(field_own_pct - actual_own_pct) >= 10
        ) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE projection_quartile = 1 OR salary >= 7000 OR is_pitcher), 0) AS "leverageErrorRate",
        AVG(ABS(field_own_pct - actual_own_pct)) FILTER (WHERE projection_quartile = 1 OR salary >= 7000 OR is_pitcher) AS "avgAbsError"
      FROM ranked
      GROUP BY window_label, window_sort
      ORDER BY window_sort ASC
    `);

  const signalResult = await db.execute<MlbPostmortemSignalRow>(sql`
      ${completedSlatesCte},
      recent_sample AS (
        SELECT *
        FROM player_sample
        WHERE window_sort = 1
      ),
      signals AS (
        SELECT
          'HR Badge 25%+'::text AS signal,
          'hitter'::text AS baseline_group,
          ps.slate_id,
          ps.final_proj AS projection,
          ps.actual_fpts,
          ps.actual_own_pct
        FROM recent_sample ps
        WHERE ps.is_pitcher = false
          AND ps.hr_prob_1plus >= 0.25
        UNION ALL
        SELECT
          'Strong HR Badge 35%+'::text AS signal,
          'hitter'::text AS baseline_group,
          ps.slate_id,
          ps.final_proj AS projection,
          ps.actual_fpts,
          ps.actual_own_pct
        FROM recent_sample ps
        WHERE ps.is_pitcher = false
          AND ps.hr_prob_1plus >= 0.35
        UNION ALL
        SELECT
          'Pitcher 18+ Projection'::text AS signal,
          'pitcher'::text AS baseline_group,
          ps.slate_id,
          ps.final_proj AS projection,
          ps.actual_fpts,
          ps.actual_own_pct
        FROM recent_sample ps
        WHERE ps.is_pitcher = true
          AND ps.final_proj >= 18
        UNION ALL
        SELECT
          'Pitcher 2.5x+ Value'::text AS signal,
          'pitcher'::text AS baseline_group,
          ps.slate_id,
          ps.final_proj AS projection,
          ps.actual_fpts,
          ps.actual_own_pct
        FROM recent_sample ps
        WHERE ps.is_pitcher = true
          AND ps.final_proj IS NOT NULL
          AND ps.salary > 0
          AND ps.final_proj / (ps.salary / 1000.0) >= 2.5
        UNION ALL
        SELECT
          'Blowup Top 12'::text AS signal,
          'hitter'::text AS baseline_group,
          s.slate_id,
          s.projected_fpts AS projection,
          s.actual_fpts,
          s.actual_own_pct
        FROM mlb_blowup_player_snapshots s
        JOIN windowed_slates ws ON ws.slate_id = s.slate_id AND ws.window_sort = 1
        WHERE s.actual_fpts IS NOT NULL
      ),
      baselines AS (
        SELECT
          'hitter'::text AS baseline_group,
          AVG(CASE WHEN actual_fpts >= 20 THEN 1 ELSE 0 END) * 100 AS hit20_rate,
          AVG(CASE WHEN actual_fpts >= 25 THEN 1 ELSE 0 END) * 100 AS hit25_rate
        FROM recent_sample
        WHERE is_pitcher = false
        UNION ALL
        SELECT
          'pitcher'::text AS baseline_group,
          AVG(CASE WHEN actual_fpts >= 20 THEN 1 ELSE 0 END) * 100 AS hit20_rate,
          AVG(CASE WHEN actual_fpts >= 25 THEN 1 ELSE 0 END) * 100 AS hit25_rate
        FROM recent_sample
        WHERE is_pitcher = true
      )
      SELECT
        s.signal AS "signal",
        COUNT(*)::int AS "rows",
        COUNT(DISTINCT s.slate_id)::int AS "slates",
        AVG(s.projection) AS "avgProjection",
        AVG(s.actual_fpts) AS "avgActual",
        AVG(s.actual_own_pct) AS "avgActualOwn",
        AVG(s.actual_fpts - s.projection) FILTER (WHERE s.projection IS NOT NULL) AS "avgBeat",
        AVG(CASE WHEN s.actual_fpts >= 15 THEN 1 ELSE 0 END) * 100 AS "hit15Rate",
        AVG(CASE WHEN s.actual_fpts >= 20 THEN 1 ELSE 0 END) * 100 AS "hit20Rate",
        AVG(CASE WHEN s.actual_fpts >= 25 THEN 1 ELSE 0 END) * 100 AS "hit25Rate",
        MAX(b.hit20_rate) AS "baseline20Rate",
        AVG(CASE WHEN s.actual_fpts >= 20 THEN 1 ELSE 0 END) * 100 - MAX(b.hit20_rate) AS "lift20Rate",
        MAX(b.hit25_rate) AS "baseline25Rate",
        AVG(CASE WHEN s.actual_fpts >= 25 THEN 1 ELSE 0 END) * 100 - MAX(b.hit25_rate) AS "lift25Rate"
      FROM signals s
      LEFT JOIN baselines b ON b.baseline_group = s.baseline_group
      GROUP BY s.signal
      ORDER BY CASE s.signal
        WHEN 'HR Badge 25%+' THEN 1
        WHEN 'Strong HR Badge 35%+' THEN 2
        WHEN 'Blowup Top 12' THEN 3
        WHEN 'Pitcher 18+ Projection' THEN 4
        ELSE 5
      END
    `);

  const recentSlateResult = await db.execute<MlbPostmortemRecentSlateRow>(sql`
      ${completedSlatesCte},
      projection_by_slate AS (
        SELECT
          ps.slate_id,
          ps.slate_date,
          COUNT(*)::int AS player_rows,
          AVG(ABS(ps.final_proj - ps.actual_fpts)) FILTER (WHERE ps.final_proj IS NOT NULL) AS final_mae,
          AVG(ps.final_proj - ps.actual_fpts) FILTER (WHERE ps.final_proj IS NOT NULL) AS final_bias,
          AVG(ABS(ps.linestar_proj - ps.actual_fpts)) FILTER (WHERE ps.linestar_proj IS NOT NULL) AS linestar_mae,
          AVG(ABS(ps.field_own_pct - ps.actual_own_pct)) FILTER (WHERE ps.field_own_pct IS NOT NULL AND ps.actual_own_pct IS NOT NULL) AS field_own_mae,
          AVG(ABS(ps.linestar_own_pct - ps.actual_own_pct)) FILTER (WHERE ps.linestar_own_pct IS NOT NULL AND ps.actual_own_pct IS NOT NULL) AS linestar_own_mae,
          COUNT(*) FILTER (WHERE ps.is_pitcher = false AND ps.hr_prob_1plus >= 0.25)::int AS hr_badge_rows,
          AVG(ps.actual_fpts) FILTER (WHERE ps.is_pitcher = false AND ps.hr_prob_1plus >= 0.25) AS hr_badge_avg_actual
        FROM player_sample ps
        WHERE ps.window_sort IN (1, 2)
        GROUP BY ps.slate_id, ps.slate_date
      ),
      blowup_by_slate AS (
        SELECT
          s.slate_id,
          COUNT(*) FILTER (WHERE s.actual_fpts >= 20)::int AS blowup_hit20
        FROM mlb_blowup_player_snapshots s
        JOIN windowed_slates ws ON ws.slate_id = s.slate_id AND ws.window_sort IN (1, 2)
        WHERE s.actual_fpts IS NOT NULL
        GROUP BY s.slate_id
      )
      SELECT
        pbs.slate_id::int AS "slateId",
        pbs.slate_date::text AS "slateDate",
        pbs.player_rows AS "playerRows",
        pbs.final_mae AS "finalMae",
        pbs.final_bias AS "finalBias",
        pbs.linestar_mae AS "linestarMae",
        pbs.field_own_mae AS "fieldOwnMae",
        pbs.linestar_own_mae AS "linestarOwnMae",
        pbs.hr_badge_rows AS "hrBadgeRows",
        pbs.hr_badge_avg_actual AS "hrBadgeAvgActual",
        bbs.blowup_hit20 AS "blowupHit20"
      FROM projection_by_slate pbs
      LEFT JOIN blowup_by_slate bbs ON bbs.slate_id = pbs.slate_id
      ORDER BY pbs.slate_date DESC, pbs.slate_id DESC
    `);

  const projectionMissResult = await db.execute<MlbPostmortemProjectionMissRow>(sql`
      ${completedSlatesCte}
      SELECT
        ps.slate_id::int AS "slateId",
        ps.slate_date::text AS "slateDate",
        ps.name AS "name",
        ps.team_abbrev AS "teamAbbrev",
        ps.eligible_positions AS "eligiblePositions",
        ps.salary::int AS "salary",
        ps.lineup_order::int AS "lineupOrder",
        ps.final_proj AS "projection",
        ps.linestar_proj AS "linestarProjection",
        ps.actual_fpts AS "actualFpts",
        ps.actual_own_pct AS "actualOwnPct",
        ps.actual_fpts - ps.final_proj AS "miss"
      FROM player_sample ps
      WHERE ps.slate_rank = 1
        AND ps.window_sort = 1
        AND ps.final_proj IS NOT NULL
      ORDER BY ABS(ps.actual_fpts - ps.final_proj) DESC NULLS LAST, ps.salary DESC, ps.name ASC
      LIMIT 12
    `);

  const ownershipMissResult = await db.execute<MlbPostmortemOwnershipMissRow>(sql`
      ${completedSlatesCte}
      SELECT
        ps.slate_id::int AS "slateId",
        ps.slate_date::text AS "slateDate",
        ps.name AS "name",
        ps.team_abbrev AS "teamAbbrev",
        ps.eligible_positions AS "eligiblePositions",
        ps.salary::int AS "salary",
        ps.lineup_order::int AS "lineupOrder",
        ps.field_own_pct AS "fieldOwnPct",
        ps.linestar_own_pct AS "linestarOwnPct",
        ps.actual_own_pct AS "actualOwnPct",
        ABS(ps.field_own_pct - ps.actual_own_pct) AS "fieldAbsError",
        ABS(ps.linestar_own_pct - ps.actual_own_pct) AS "linestarAbsError",
        ABS(ps.linestar_own_pct - ps.actual_own_pct) - ABS(ps.field_own_pct - ps.actual_own_pct) AS "fieldGainVsLineStar"
      FROM player_sample ps
      WHERE ps.slate_rank = 1
        AND ps.window_sort = 1
        AND ps.actual_own_pct IS NOT NULL
        AND (ps.field_own_pct IS NOT NULL OR ps.linestar_own_pct IS NOT NULL)
      ORDER BY ABS(COALESCE(ps.field_own_pct, ps.linestar_own_pct) - ps.actual_own_pct) DESC NULLS LAST, ps.salary DESC, ps.name ASC
      LIMIT 12
    `);

  const pitcherExploitResult = await db.execute<MlbPostmortemPitcherExploitRow>(sql`
      ${completedSlatesCte},
      pitcher_context AS (
        SELECT
          ps.*,
          CASE
            WHEN ps.mlb_team_id = mm.home_team_id THEN mm.away_implied
            WHEN ps.mlb_team_id = mm.away_team_id THEN mm.home_implied
            ELSE NULL
          END AS opponent_implied,
          CASE
            WHEN ps.mlb_team_id = mm.home_team_id THEN mm.home_ml
            WHEN ps.mlb_team_id = mm.away_team_id THEN mm.away_ml
            ELSE NULL
          END AS moneyline
        FROM player_sample ps
        LEFT JOIN mlb_matchups mm ON mm.id = ps.matchup_id
        WHERE ps.window_sort = 1
          AND ps.is_pitcher = true
          AND ps.final_proj IS NOT NULL
          AND ps.salary > 0
      ),
      scored AS (
        SELECT
          pc.*,
          pc.final_proj / (pc.salary / 1000.0) AS value_multiple,
          pc.linestar_proj - pc.final_proj AS market_gap,
          (
            (pc.final_proj / NULLIF(pc.salary / 1000.0, 0)) * 2.0
            + GREATEST(0, 18 - COALESCE(pc.field_own_pct, 18)) * 0.25
            + CASE WHEN pc.opponent_implied IS NOT NULL THEN GREATEST(0, 4.5 - pc.opponent_implied) * 2.0 ELSE 0 END
            + CASE WHEN pc.moneyline IS NOT NULL AND pc.moneyline <= -120 THEN 1.5 ELSE 0 END
            + CASE WHEN pc.projection_source <> 'linestar' THEN 1.0 ELSE 0 END
          ) AS exploit_score
        FROM pitcher_context pc
      )
      SELECT
        slate_id::int AS "slateId",
        slate_date::text AS "slateDate",
        name AS "name",
        team_abbrev AS "teamAbbrev",
        salary::int AS "salary",
        final_proj AS "projection",
        linestar_proj AS "linestarProjection",
        market_gap AS "marketGap",
        field_own_pct AS "fieldOwnPct",
        actual_own_pct AS "actualOwnPct",
        actual_fpts AS "actualFpts",
        value_multiple AS "valueMultiple",
        opponent_implied AS "opponentImplied",
        moneyline::int AS "moneyline",
        exploit_score AS "score"
      FROM scored
      WHERE final_proj >= 14
        AND COALESCE(field_own_pct, 0) <= 18
        AND (
          opponent_implied IS NULL
          OR opponent_implied <= 4.5
          OR moneyline <= -120
        )
      ORDER BY exploit_score DESC, final_proj DESC, salary DESC
      LIMIT 12
    `);

  const decisionCaptureResult = await db.execute<MlbPostmortemDecisionCaptureRow>(sql`
      ${completedSlatesCte},
      ranked AS (
        SELECT
          ps.*,
          COUNT(*) OVER (PARTITION BY ps.window_sort, ps.slate_id) AS slate_rows,
          ROW_NUMBER() OVER (PARTITION BY ps.window_sort, ps.slate_id ORDER BY ps.actual_fpts DESC NULLS LAST, ps.salary DESC, ps.name ASC) AS actual_rank,
          ROW_NUMBER() OVER (PARTITION BY ps.window_sort, ps.slate_id ORDER BY ps.final_proj DESC NULLS LAST, ps.salary DESC, ps.name ASC) AS projection_rank,
          ROW_NUMBER() OVER (
            PARTITION BY ps.window_sort, ps.slate_id
            ORDER BY COALESCE(ps.proj_ceiling, ps.final_proj) DESC NULLS LAST, ps.hr_prob_1plus DESC NULLS LAST, ps.salary DESC, ps.name ASC
          ) AS ceiling_rank,
          NTILE(4) OVER (PARTITION BY ps.window_sort, ps.slate_id ORDER BY ps.final_proj DESC NULLS LAST, ps.salary DESC, ps.name ASC) AS projection_quartile
        FROM player_sample ps
        WHERE ps.final_proj IS NOT NULL
      ),
      buckets AS (
        SELECT * FROM (
          VALUES
            ('Top 1%'::text, 1::int),
            ('Top 5%'::text, 5::int),
            ('Top 10%'::text, 10::int)
        ) AS b(bucket, pct)
      )
      SELECT
        r.window_label AS "windowLabel",
        r.window_sort::int AS "windowSort",
        b.bucket AS "outcomeBucket",
        COUNT(*) FILTER (WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0)))::int AS "outcomeRows",
        COUNT(*) FILTER (
          WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0))
            AND r.projection_rank <= GREATEST(1, CEIL(r.slate_rows * 0.10))
        ) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0))), 0) AS "highProjectionCaptureRate",
        COUNT(*) FILTER (
          WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0))
            AND (
              r.ceiling_rank <= GREATEST(1, CEIL(r.slate_rows * 0.10))
              OR (r.is_pitcher = false AND r.hr_prob_1plus >= 0.25)
            )
        ) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0))), 0) AS "ceilingCaptureRate",
        COUNT(*) FILTER (
          WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0))
            AND COALESCE(r.field_own_pct, 100) <= 10
            AND r.projection_quartile = 1
        ) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0))), 0) AS "leverageCaptureRate",
        AVG(r.actual_own_pct) FILTER (WHERE r.actual_rank <= GREATEST(1, CEIL(r.slate_rows * b.pct / 100.0))) AS "avgActualOwn"
      FROM ranked r
      CROSS JOIN buckets b
      GROUP BY r.window_label, r.window_sort, b.bucket, b.pct
      ORDER BY r.window_sort ASC, b.pct ASC
    `);

  const summary = summaryResult.rows[0];
  if (!summary || Number(summary.playerRows ?? 0) === 0) return null;

  const projectionSummary = projectionResult.rows.map((row) => ({
    windowLabel: row.windowLabel,
    windowSort: Number(row.windowSort),
    playerGroup: row.playerGroup,
    rows: Number(row.rows ?? 0),
    slates: Number(row.slates ?? 0),
    finalMae: mlbPostmortemNumber(row.finalMae),
    finalBias: mlbPostmortemNumber(row.finalBias),
    liveMae: mlbPostmortemNumber(row.liveMae),
    liveBias: mlbPostmortemNumber(row.liveBias),
    ourMae: mlbPostmortemNumber(row.ourMae),
    ourBias: mlbPostmortemNumber(row.ourBias),
    linestarMae: mlbPostmortemNumber(row.linestarMae),
    linestarBias: mlbPostmortemNumber(row.linestarBias),
    finalGainVsLineStar: mlbPostmortemNumber(row.finalGainVsLineStar),
  }));

  const projectionIndependence = projectionIndependenceResult.rows.map((row) => ({
    windowLabel: row.windowLabel,
    windowSort: Number(row.windowSort),
    rows: Number(row.rows ?? 0),
    finalRows: Number(row.finalRows ?? 0),
    fallbackRows: Number(row.fallbackRows ?? 0),
    fallbackPct: mlbPostmortemNumber(row.fallbackPct),
    finalMae: mlbPostmortemNumber(row.finalMae),
    rawOurMae: mlbPostmortemNumber(row.rawOurMae),
    ourSourceMae: mlbPostmortemNumber(row.ourSourceMae),
    nonLineStarMae: mlbPostmortemNumber(row.nonLineStarMae),
    fallbackMae: mlbPostmortemNumber(row.fallbackMae),
    nonFallbackMae: mlbPostmortemNumber(row.nonFallbackMae),
    linestarMae: mlbPostmortemNumber(row.linestarMae),
    blendUplift: mlbPostmortemNumber(row.blendUplift),
    fallbackDelta: mlbPostmortemNumber(row.fallbackDelta),
    warning: row.warning ?? null,
  }));

  const projectionSources = projectionSourceResult.rows.map((row) => ({
    windowLabel: row.windowLabel,
    windowSort: Number(row.windowSort),
    projectionSource: row.projectionSource,
    rows: Number(row.rows ?? 0),
    pctRows: mlbPostmortemNumber(row.pctRows),
    mae: mlbPostmortemNumber(row.mae),
    bias: mlbPostmortemNumber(row.bias),
  }));

  const ownershipSummary = ownershipResult.rows.map((row) => ({
    windowLabel: row.windowLabel,
    windowSort: Number(row.windowSort),
    rows: Number(row.rows ?? 0),
    slates: Number(row.slates ?? 0),
    fieldRows: Number(row.fieldRows ?? 0),
    linestarRows: Number(row.linestarRows ?? 0),
    fieldMae: mlbPostmortemNumber(row.fieldMae),
    fieldBias: mlbPostmortemNumber(row.fieldBias),
    fieldCorr: mlbPostmortemNumber(row.fieldCorr),
    linestarMae: mlbPostmortemNumber(row.linestarMae),
    linestarBias: mlbPostmortemNumber(row.linestarBias),
    linestarCorr: mlbPostmortemNumber(row.linestarCorr),
    fieldGainVsLineStar: mlbPostmortemNumber(row.fieldGainVsLineStar),
  }));

  const ownershipChalk = ownershipChalkResult.rows.map((row) => ({
    windowLabel: row.windowLabel,
    windowSort: Number(row.windowSort),
    threshold: Number(row.threshold),
    rows: Number(row.rows ?? 0),
    actualChalkRows: Number(row.actualChalkRows ?? 0),
    projectedChalkRows: Number(row.projectedChalkRows ?? 0),
    capturedRows: Number(row.capturedRows ?? 0),
    captureRate: mlbPostmortemNumber(row.captureRate),
    falseLowRows: Number(row.falseLowRows ?? 0),
    falseChalkRows: Number(row.falseChalkRows ?? 0),
  }));

  const ownershipRanking = ownershipRankingResult.rows.map((row) => ({
    windowLabel: row.windowLabel,
    windowSort: Number(row.windowSort),
    topN: Number(row.topN),
    rows: Number(row.rows ?? 0),
    actualTopRows: Number(row.actualTopRows ?? 0),
    capturedRows: Number(row.capturedRows ?? 0),
    overlapPct: mlbPostmortemNumber(row.overlapPct),
    spearman: mlbPostmortemNumber(row.spearman),
  }));

  const leverageErrors = leverageErrorResult.rows.map((row) => ({
    windowLabel: row.windowLabel,
    windowSort: Number(row.windowSort),
    rows: Number(row.rows ?? 0),
    highImpactRows: Number(row.highImpactRows ?? 0),
    leverageErrorRows: Number(row.leverageErrorRows ?? 0),
    leverageErrorRate: mlbPostmortemNumber(row.leverageErrorRate),
    avgAbsError: mlbPostmortemNumber(row.avgAbsError),
  }));

  const signalFollowThrough = signalResult.rows.map((row) => ({
    signal: row.signal,
    rows: Number(row.rows ?? 0),
    slates: Number(row.slates ?? 0),
    avgProjection: mlbPostmortemNumber(row.avgProjection),
    avgActual: mlbPostmortemNumber(row.avgActual),
    avgActualOwn: mlbPostmortemNumber(row.avgActualOwn),
    avgBeat: mlbPostmortemNumber(row.avgBeat),
    hit15Rate: mlbPostmortemNumber(row.hit15Rate),
    hit20Rate: mlbPostmortemNumber(row.hit20Rate),
    hit25Rate: mlbPostmortemNumber(row.hit25Rate),
    baseline20Rate: mlbPostmortemNumber(row.baseline20Rate),
    lift20Rate: mlbPostmortemNumber(row.lift20Rate),
    baseline25Rate: mlbPostmortemNumber(row.baseline25Rate),
    lift25Rate: mlbPostmortemNumber(row.lift25Rate),
  }));

  const warnings = projectionIndependence
    .filter((row) => row.warning != null)
    .map((row) => {
      if (row.warning === "fallback_dependency") {
        return `${row.windowLabel}: model independence compromised because LineStar fallback is ${row.fallbackPct?.toFixed(1) ?? "-"}%.`;
      }
      return `${row.windowLabel}: non-LineStar MAE is materially worse than final MAE.`;
    });

  return {
    sample: {
      recentSlateCount: Number(summary.recentSlateCount ?? 0),
      priorSlateCount: Number(summary.priorSlateCount ?? 0),
      latestSlateId: summary.latestSlateId == null ? null : Number(summary.latestSlateId),
      latestSlateDate: summary.latestSlateDate ?? null,
      recentStartDate: summary.recentStartDate ?? null,
      recentEndDate: summary.recentEndDate ?? null,
      priorStartDate: summary.priorStartDate ?? null,
      priorEndDate: summary.priorEndDate ?? null,
      playerRows: Number(summary.playerRows ?? 0),
      ownershipRows: Number(summary.ownershipRows ?? 0),
    },
    findings: buildMlbPostmortemFindings(projectionSummary, projectionIndependence, ownershipSummary, signalFollowThrough),
    warnings,
    projectionSummary,
    projectionIndependence,
    projectionSources,
    ownershipSummary,
    ownershipChalk,
    ownershipRanking,
    leverageErrors,
    signalFollowThrough,
    pitcherExploitWatch: pitcherExploitResult.rows.map((row) => ({
      slateId: Number(row.slateId),
      slateDate: row.slateDate,
      name: row.name,
      teamAbbrev: row.teamAbbrev ?? null,
      salary: Number(row.salary ?? 0),
      projection: mlbPostmortemNumber(row.projection),
      linestarProjection: mlbPostmortemNumber(row.linestarProjection),
      marketGap: mlbPostmortemNumber(row.marketGap),
      fieldOwnPct: mlbPostmortemNumber(row.fieldOwnPct),
      actualOwnPct: mlbPostmortemNumber(row.actualOwnPct),
      actualFpts: mlbPostmortemNumber(row.actualFpts),
      valueMultiple: mlbPostmortemNumber(row.valueMultiple),
      opponentImplied: mlbPostmortemNumber(row.opponentImplied),
      moneyline: row.moneyline == null ? null : Number(row.moneyline),
      score: mlbPostmortemNumber(row.score),
    })),
    decisionCapture: decisionCaptureResult.rows.map((row) => ({
      windowLabel: row.windowLabel,
      windowSort: Number(row.windowSort),
      outcomeBucket: row.outcomeBucket,
      outcomeRows: Number(row.outcomeRows ?? 0),
      highProjectionCaptureRate: mlbPostmortemNumber(row.highProjectionCaptureRate),
      ceilingCaptureRate: mlbPostmortemNumber(row.ceilingCaptureRate),
      leverageCaptureRate: mlbPostmortemNumber(row.leverageCaptureRate),
      avgActualOwn: mlbPostmortemNumber(row.avgActualOwn),
    })),
    recentSlates: recentSlateResult.rows.map((row) => ({
      slateId: Number(row.slateId),
      slateDate: row.slateDate,
      playerRows: Number(row.playerRows ?? 0),
      finalMae: mlbPostmortemNumber(row.finalMae),
      finalBias: mlbPostmortemNumber(row.finalBias),
      linestarMae: mlbPostmortemNumber(row.linestarMae),
      fieldOwnMae: mlbPostmortemNumber(row.fieldOwnMae),
      linestarOwnMae: mlbPostmortemNumber(row.linestarOwnMae),
      hrBadgeRows: Number(row.hrBadgeRows ?? 0),
      hrBadgeAvgActual: mlbPostmortemNumber(row.hrBadgeAvgActual),
      blowupHit20: row.blowupHit20 == null ? null : Number(row.blowupHit20),
    })),
    projectionMisses: projectionMissResult.rows.map((row) => ({
      slateId: Number(row.slateId),
      slateDate: row.slateDate,
      name: row.name,
      teamAbbrev: row.teamAbbrev ?? null,
      eligiblePositions: row.eligiblePositions ?? null,
      salary: Number(row.salary ?? 0),
      lineupOrder: row.lineupOrder == null ? null : Number(row.lineupOrder),
      projection: mlbPostmortemNumber(row.projection),
      linestarProjection: mlbPostmortemNumber(row.linestarProjection),
      actualFpts: mlbPostmortemNumber(row.actualFpts),
      actualOwnPct: mlbPostmortemNumber(row.actualOwnPct),
      miss: mlbPostmortemNumber(row.miss),
    })),
    ownershipMisses: ownershipMissResult.rows.map((row) => ({
      slateId: Number(row.slateId),
      slateDate: row.slateDate,
      name: row.name,
      teamAbbrev: row.teamAbbrev ?? null,
      eligiblePositions: row.eligiblePositions ?? null,
      salary: Number(row.salary ?? 0),
      lineupOrder: row.lineupOrder == null ? null : Number(row.lineupOrder),
      fieldOwnPct: mlbPostmortemNumber(row.fieldOwnPct),
      linestarOwnPct: mlbPostmortemNumber(row.linestarOwnPct),
      actualOwnPct: mlbPostmortemNumber(row.actualOwnPct),
      fieldAbsError: mlbPostmortemNumber(row.fieldAbsError),
      linestarAbsError: mlbPostmortemNumber(row.linestarAbsError),
      fieldGainVsLineStar: mlbPostmortemNumber(row.fieldGainVsLineStar),
    })),
  };
}

const MLB_PITCHER_ALLOW_MIN_STARTS = 2;
const MLB_PITCHER_ALLOW_SHRINK = 4;
const MLB_PARK_ENV_MIN_GAMES = 3;
const MLB_PARK_ENV_SHRINK = 6;

export type MlbPitcherAllowedRow = {
  pitcherId: number;
  pitcherName: string;
  hand: string | null;
  xfip: number | null;
  whip: number | null;
  kPer9: number | null;
  starts: number;
  hitterRows: number;
  avgTeamFptsAllowed: number | null;
  avgHitterFptsAllowed: number | null;
  avgTopHitterFptsAllowed: number | null;
  avg15PlusHitters: number | null;
  avg20PlusHitters: number | null;
  shrunkAvgTeamFptsAllowed: number | null;
};

export type MlbParkEnvironmentRow = {
  parkTeamId: number;
  parkName: string;
  homeTeamAbbrev: string;
  games: number;
  teamGames: number;
  runsFactor: number | null;
  hrFactor: number | null;
  avgTeamHitterFpts: number | null;
  avgHitterFpts: number | null;
  avg15PlusHitters: number | null;
  avg20PlusHitters: number | null;
  avgCombinedRuns: number | null;
  avgSpFpts: number | null;
  shrunkAvgTeamHitterFpts: number | null;
};

export type MlbRunEnvironmentReport = {
  sample: {
    pitcherStarts: number;
    pitcherHitterRows: number;
    parkGames: number;
    parkTeamGames: number;
  };
  findings: string[];
  pitcherAllow: MlbPitcherAllowedRow[];
  parkEnvironment: MlbParkEnvironmentRow[];
};

function buildMlbRunEnvironmentFindings(
  pitcherAllow: MlbPitcherAllowedRow[],
  parkEnvironment: MlbParkEnvironmentRow[],
): string[] {
  const findings: string[] = [];
  const topPitcher = pitcherAllow[0];
  if (topPitcher?.shrunkAvgTeamFptsAllowed != null) {
    findings.push(`${topPitcher.pitcherName} has been the softest tracked SP context, allowing ${topPitcher.shrunkAvgTeamFptsAllowed.toFixed(2)} opposing hitter DK points per team start over ${topPitcher.starts} starts.`);
  }

  const topPark = parkEnvironment[0];
  if (topPark?.shrunkAvgTeamHitterFpts != null) {
    findings.push(`${topPark.parkName} has been the strongest hitter environment, averaging ${topPark.shrunkAvgTeamHitterFpts.toFixed(2)} hitter DK points per offense with park factors ${topPark.runsFactor?.toFixed(2) ?? "—"} runs / ${topPark.hrFactor?.toFixed(2) ?? "—"} HR.`);
  }

  const toughestParkForSp = parkEnvironment
    .filter((row) => row.avgSpFpts != null)
    .sort((a, b) => (a.avgSpFpts ?? 999) - (b.avgSpFpts ?? 999))[0];
  if (toughestParkForSp?.avgSpFpts != null) {
    findings.push(`${toughestParkForSp.parkName} has been the worst SP scoring environment so far, with starters averaging only ${toughestParkForSp.avgSpFpts.toFixed(2)} DK points there.`);
  }

  return findings.slice(0, 4);
}

export async function getMlbRunEnvironmentReport(): Promise<MlbRunEnvironmentReport | null> {
  await ensureAnalyticsColumns();
  const [pitcherAllowResult, parkEnvironmentResult] = await Promise.all([
    db.execute<{
      pitcherId: number;
      pitcherName: string | null;
      hand: string | null;
      xfip: number | null;
      whip: number | null;
      kPer9: number | null;
      starts: number;
      hitterRows: number;
      avgTeamFptsAllowed: number | null;
      avgHitterFptsAllowed: number | null;
      avgTopHitterFptsAllowed: number | null;
      avg15PlusHitters: number | null;
      avg20PlusHitters: number | null;
    }>(sql`
      WITH latest_pitchers AS (
        SELECT DISTINCT ON (ps.player_id)
          ps.player_id,
          ps.name,
          ps.hand,
          ps.xfip,
          ps.whip,
          ps.k_per_9
        FROM mlb_pitcher_stats ps
        ORDER BY ps.player_id, ps.season DESC, ps.fetched_at DESC, ps.id DESC
      ),
      latest_pitchers_by_name AS (
        SELECT DISTINCT ON (LOWER(ps.name))
          LOWER(ps.name) AS name_key,
          ps.name,
          ps.hand,
          ps.xfip,
          ps.whip,
          ps.k_per_9
        FROM mlb_pitcher_stats ps
        ORDER BY LOWER(ps.name), ps.season DESC, ps.fetched_at DESC, ps.id DESC
      ),
      hitter_context AS (
        SELECT
          ds.id AS slate_id,
          ds.slate_date,
          mm.id AS matchup_id,
          CASE
            WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_sp_id
            WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_sp_id
            ELSE NULL
          END AS pitcher_id,
          CASE
            WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_sp_name
            WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_sp_name
            ELSE NULL
          END AS pitcher_name,
          dp.actual_fpts
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        WHERE ds.sport = 'mlb'
          AND COALESCE(ds.contest_type, 'main') = 'main'
          AND COALESCE(ds.contest_format, 'gpp') = 'gpp'
          AND dp.actual_fpts IS NOT NULL
          AND COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions NOT LIKE '%SP%'
          AND dp.eligible_positions NOT LIKE '%RP%'
      ),
      pitcher_starts AS (
        SELECT
          pitcher_id,
          pitcher_name,
          matchup_id,
          COUNT(*)::int AS hitter_rows,
          AVG(actual_fpts) AS avg_hitter_fpts,
          SUM(actual_fpts) AS team_hitter_fpts,
          MAX(actual_fpts) AS top_hitter_fpts,
          COUNT(*) FILTER (WHERE actual_fpts >= 15)::int AS hitters15,
          COUNT(*) FILTER (WHERE actual_fpts >= 20)::int AS hitters20
        FROM hitter_context
        WHERE pitcher_id IS NOT NULL
        GROUP BY pitcher_id, pitcher_name, matchup_id
      )
      SELECT
        ps.pitcher_id::int AS "pitcherId",
        COALESCE(MAX(ps.pitcher_name), lp.name, lpn.name) AS "pitcherName",
        COALESCE(lp.hand, lpn.hand) AS "hand",
        COALESCE(lp.xfip, lpn.xfip) AS "xfip",
        COALESCE(lp.whip, lpn.whip) AS "whip",
        COALESCE(lp.k_per_9, lpn.k_per_9) AS "kPer9",
        COUNT(*)::int AS "starts",
        SUM(ps.hitter_rows)::int AS "hitterRows",
        AVG(ps.team_hitter_fpts) AS "avgTeamFptsAllowed",
        AVG(ps.avg_hitter_fpts) AS "avgHitterFptsAllowed",
        AVG(ps.top_hitter_fpts) AS "avgTopHitterFptsAllowed",
        AVG(ps.hitters15::DOUBLE PRECISION) AS "avg15PlusHitters",
        AVG(ps.hitters20::DOUBLE PRECISION) AS "avg20PlusHitters"
      FROM pitcher_starts ps
      LEFT JOIN latest_pitchers lp ON lp.player_id = ps.pitcher_id
      LEFT JOIN latest_pitchers_by_name lpn ON lpn.name_key = LOWER(ps.pitcher_name)
      GROUP BY ps.pitcher_id, lp.name, lp.hand, lp.xfip, lp.whip, lp.k_per_9, lpn.name, lpn.hand, lpn.xfip, lpn.whip, lpn.k_per_9
      HAVING COUNT(*) >= ${MLB_PITCHER_ALLOW_MIN_STARTS}
    `),
    db.execute<{
      parkTeamId: number;
      parkName: string | null;
      homeTeamAbbrev: string | null;
      games: number;
      teamGames: number;
      runsFactor: number | null;
      hrFactor: number | null;
      avgTeamHitterFpts: number | null;
      avgHitterFpts: number | null;
      avg15PlusHitters: number | null;
      avg20PlusHitters: number | null;
      avgCombinedRuns: number | null;
      avgSpFpts: number | null;
    }>(sql`
      WITH latest_parks AS (
        SELECT DISTINCT ON (pf.team_id)
          pf.team_id,
          pf.runs_factor,
          pf.hr_factor
        FROM mlb_park_factors pf
        ORDER BY pf.team_id, pf.season DESC, pf.id DESC
      ),
      hitter_context AS (
        SELECT
          mm.id AS matchup_id,
          mm.home_team_id AS park_team_id,
          COALESCE(mm.ballpark, mt.ballpark, mt.name) AS park_name,
          mt.abbreviation AS home_team_abbrev,
          dp.mlb_team_id AS offense_team_id,
          dp.actual_fpts,
          (mm.home_score + mm.away_score)::DOUBLE PRECISION AS combined_runs
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        LEFT JOIN mlb_teams mt ON mt.team_id = mm.home_team_id
        WHERE ds.sport = 'mlb'
          AND COALESCE(ds.contest_type, 'main') = 'main'
          AND COALESCE(ds.contest_format, 'gpp') = 'gpp'
          AND dp.actual_fpts IS NOT NULL
          AND COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions NOT LIKE '%SP%'
          AND dp.eligible_positions NOT LIKE '%RP%'
      ),
      park_team_games AS (
        SELECT
          park_team_id,
          park_name,
          home_team_abbrev,
          matchup_id,
          offense_team_id,
          MAX(combined_runs) AS combined_runs,
          SUM(actual_fpts) AS team_hitter_fpts,
          AVG(actual_fpts) AS avg_hitter_fpts,
          COUNT(*) FILTER (WHERE actual_fpts >= 15)::int AS hitters15,
          COUNT(*) FILTER (WHERE actual_fpts >= 20)::int AS hitters20
        FROM hitter_context
        GROUP BY park_team_id, park_name, home_team_abbrev, matchup_id, offense_team_id
      ),
      park_sp_games AS (
        SELECT
          mm.id AS matchup_id,
          mm.home_team_id AS park_team_id,
          AVG(dp.actual_fpts) AS avg_sp_fpts
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        WHERE ds.sport = 'mlb'
          AND COALESCE(ds.contest_type, 'main') = 'main'
          AND COALESCE(ds.contest_format, 'gpp') = 'gpp'
          AND dp.actual_fpts IS NOT NULL
          AND COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions LIKE '%SP%'
        GROUP BY mm.id, mm.home_team_id
      )
      SELECT
        ptg.park_team_id::int AS "parkTeamId",
        ptg.park_name AS "parkName",
        ptg.home_team_abbrev AS "homeTeamAbbrev",
        COUNT(DISTINCT ptg.matchup_id)::int AS "games",
        COUNT(*)::int AS "teamGames",
        lp.runs_factor AS "runsFactor",
        lp.hr_factor AS "hrFactor",
        AVG(ptg.team_hitter_fpts) AS "avgTeamHitterFpts",
        AVG(ptg.avg_hitter_fpts) AS "avgHitterFpts",
        AVG(ptg.hitters15::DOUBLE PRECISION) AS "avg15PlusHitters",
        AVG(ptg.hitters20::DOUBLE PRECISION) AS "avg20PlusHitters",
        AVG(ptg.combined_runs) AS "avgCombinedRuns",
        AVG(psg.avg_sp_fpts) AS "avgSpFpts"
      FROM park_team_games ptg
      LEFT JOIN latest_parks lp ON lp.team_id = ptg.park_team_id
      LEFT JOIN park_sp_games psg
        ON psg.matchup_id = ptg.matchup_id
       AND psg.park_team_id = ptg.park_team_id
      GROUP BY ptg.park_team_id, ptg.park_name, ptg.home_team_abbrev, lp.runs_factor, lp.hr_factor
      HAVING COUNT(DISTINCT ptg.matchup_id) >= ${MLB_PARK_ENV_MIN_GAMES}
    `),
  ]);

  const pitcherAllow: MlbPitcherAllowedRow[] = (pitcherAllowResult.rows as Record<string, unknown>[]).map((row) => ({
    pitcherId: Number(row.pitcherId),
    pitcherName: String(row.pitcherName ?? `Pitcher ${row.pitcherId}`),
    hand: row.hand == null ? null : String(row.hand),
    xfip: row.xfip == null ? null : Number(row.xfip),
    whip: row.whip == null ? null : Number(row.whip),
    kPer9: row.kPer9 == null ? null : Number(row.kPer9),
    starts: Number(row.starts ?? 0),
    hitterRows: Number(row.hitterRows ?? 0),
    avgTeamFptsAllowed: row.avgTeamFptsAllowed == null ? null : Number(row.avgTeamFptsAllowed),
    avgHitterFptsAllowed: row.avgHitterFptsAllowed == null ? null : Number(row.avgHitterFptsAllowed),
    avgTopHitterFptsAllowed: row.avgTopHitterFptsAllowed == null ? null : Number(row.avgTopHitterFptsAllowed),
    avg15PlusHitters: row.avg15PlusHitters == null ? null : Number(row.avg15PlusHitters),
    avg20PlusHitters: row.avg20PlusHitters == null ? null : Number(row.avg20PlusHitters),
    shrunkAvgTeamFptsAllowed: null as number | null,
  }));

  const parkEnvironment: MlbParkEnvironmentRow[] = (parkEnvironmentResult.rows as Record<string, unknown>[]).map((row) => ({
    parkTeamId: Number(row.parkTeamId),
    parkName: String(row.parkName ?? "Unknown Park"),
    homeTeamAbbrev: String(row.homeTeamAbbrev ?? ""),
    games: Number(row.games ?? 0),
    teamGames: Number(row.teamGames ?? 0),
    runsFactor: row.runsFactor == null ? null : Number(row.runsFactor),
    hrFactor: row.hrFactor == null ? null : Number(row.hrFactor),
    avgTeamHitterFpts: row.avgTeamHitterFpts == null ? null : Number(row.avgTeamHitterFpts),
    avgHitterFpts: row.avgHitterFpts == null ? null : Number(row.avgHitterFpts),
    avg15PlusHitters: row.avg15PlusHitters == null ? null : Number(row.avg15PlusHitters),
    avg20PlusHitters: row.avg20PlusHitters == null ? null : Number(row.avg20PlusHitters),
    avgCombinedRuns: row.avgCombinedRuns == null ? null : Number(row.avgCombinedRuns),
    avgSpFpts: row.avgSpFpts == null ? null : Number(row.avgSpFpts),
    shrunkAvgTeamHitterFpts: null as number | null,
  }));

  if (pitcherAllow.length === 0 && parkEnvironment.length === 0) return null;

  const pitcherStarts = pitcherAllow.reduce((sum, row) => sum + row.starts, 0);
  const pitcherHitterRows = pitcherAllow.reduce((sum, row) => sum + row.hitterRows, 0);
  const pitcherBaseline = pitcherStarts > 0
    ? pitcherAllow.reduce((sum, row) => sum + (row.avgTeamFptsAllowed ?? 0) * row.starts, 0) / pitcherStarts
    : 0;
  for (const row of pitcherAllow) {
    row.shrunkAvgTeamFptsAllowed = row.avgTeamFptsAllowed == null
      ? null
      : round2(((row.avgTeamFptsAllowed * row.starts) + (MLB_PITCHER_ALLOW_SHRINK * pitcherBaseline)) / (row.starts + MLB_PITCHER_ALLOW_SHRINK));
    row.avgTeamFptsAllowed = row.avgTeamFptsAllowed == null ? null : round2(row.avgTeamFptsAllowed);
    row.avgHitterFptsAllowed = row.avgHitterFptsAllowed == null ? null : round2(row.avgHitterFptsAllowed);
    row.avgTopHitterFptsAllowed = row.avgTopHitterFptsAllowed == null ? null : round2(row.avgTopHitterFptsAllowed);
    row.avg15PlusHitters = row.avg15PlusHitters == null ? null : round2(row.avg15PlusHitters);
    row.avg20PlusHitters = row.avg20PlusHitters == null ? null : round2(row.avg20PlusHitters);
    row.xfip = row.xfip == null ? null : round2(row.xfip);
    row.whip = row.whip == null ? null : round2(row.whip);
    row.kPer9 = row.kPer9 == null ? null : round2(row.kPer9);
  }
  pitcherAllow.sort((a, b) =>
    (b.shrunkAvgTeamFptsAllowed ?? -999) - (a.shrunkAvgTeamFptsAllowed ?? -999)
    || (b.avgTopHitterFptsAllowed ?? -999) - (a.avgTopHitterFptsAllowed ?? -999)
    || b.starts - a.starts
    || a.pitcherName.localeCompare(b.pitcherName)
  );

  const parkGames = parkEnvironment.reduce((sum, row) => sum + row.games, 0);
  const parkTeamGames = parkEnvironment.reduce((sum, row) => sum + row.teamGames, 0);
  const parkBaseline = parkTeamGames > 0
    ? parkEnvironment.reduce((sum, row) => sum + (row.avgTeamHitterFpts ?? 0) * row.teamGames, 0) / parkTeamGames
    : 0;
  for (const row of parkEnvironment) {
    row.shrunkAvgTeamHitterFpts = row.avgTeamHitterFpts == null
      ? null
      : round2(((row.avgTeamHitterFpts * row.teamGames) + (MLB_PARK_ENV_SHRINK * parkBaseline)) / (row.teamGames + MLB_PARK_ENV_SHRINK));
    row.avgTeamHitterFpts = row.avgTeamHitterFpts == null ? null : round2(row.avgTeamHitterFpts);
    row.avgHitterFpts = row.avgHitterFpts == null ? null : round2(row.avgHitterFpts);
    row.avg15PlusHitters = row.avg15PlusHitters == null ? null : round2(row.avg15PlusHitters);
    row.avg20PlusHitters = row.avg20PlusHitters == null ? null : round2(row.avg20PlusHitters);
    row.avgCombinedRuns = row.avgCombinedRuns == null ? null : round2(row.avgCombinedRuns);
    row.avgSpFpts = row.avgSpFpts == null ? null : round2(row.avgSpFpts);
    row.runsFactor = row.runsFactor == null ? null : round2(row.runsFactor);
    row.hrFactor = row.hrFactor == null ? null : round2(row.hrFactor);
  }
  parkEnvironment.sort((a, b) =>
    (b.shrunkAvgTeamHitterFpts ?? -999) - (a.shrunkAvgTeamHitterFpts ?? -999)
    || (b.avgCombinedRuns ?? -999) - (a.avgCombinedRuns ?? -999)
    || b.games - a.games
    || a.parkName.localeCompare(b.parkName)
  );

  return {
    sample: {
      pitcherStarts,
      pitcherHitterRows,
      parkGames,
      parkTeamGames,
    },
    findings: buildMlbRunEnvironmentFindings(pitcherAllow, parkEnvironment),
    pitcherAllow: pitcherAllow.slice(0, 12),
    parkEnvironment: parkEnvironment.slice(0, 12),
  };
}

type NbaPerfectLineupSourceRow = {
  slateId: number;
  slateDate: string;
  storedGameCount: number | null;
  playerRowId: number;
  teamId: number | null;
  teamAbbrev: string;
  teamName: string | null;
  eligiblePositions: string;
  salary: number;
  actualFpts: number;
  homeTeamId: number | null;
  awayTeamId: number | null;
};

export type NbaPerfectLineupSummaryRow = {
  slateSizeBucket: string;
  slateCount: number;
  avgSalary: number;
  avgPoints: number;
  anyTwoStackRate: number;
  anyThreeStackRate: number;
  anyFourStackRate: number;
  multiTeamStackRate: number;
};

export type NbaPerfectLineupShapeRow = {
  slateSizeBucket: string;
  shape: string;
  slateCount: number;
  rate: number;
};

export type NbaPerfectLineupTeamRateRow = {
  teamAbbrev: string;
  teamName: string | null;
  slateAppearances: number;
  perfectAppearances: number;
  avgPerfectPlayers: number;
  shrunkAvgPerfectPlayers: number;
};

export type NbaPerfectLineupOpponentAllowRow = {
  defenseAbbrev: string;
  defenseName: string | null;
  position: string;
  slateAppearances: number;
  perfectAppearances: number;
  avgAllowed: number;
  shrunkAvgAllowed: number;
};

export type NbaPerfectLineupAnalytics = {
  slateCount: number;
  opponentContextSlateCount: number;
  summary: NbaPerfectLineupSummaryRow[];
  shapes: NbaPerfectLineupShapeRow[];
  teamRates: NbaPerfectLineupTeamRateRow[];
  opponentAllow: NbaPerfectLineupOpponentAllowRow[];
};

type MlbPerfectLineupSourceRow = {
  slateId: number;
  slateDate: string;
  storedGameCount: number | null;
  playerRowId: number;
  teamId: number | null;
  teamAbbrev: string;
  teamName: string | null;
  eligiblePositions: string;
  salary: number;
  actualFpts: number;
  homeTeamId: number | null;
  awayTeamId: number | null;
};

export type MlbPerfectLineupSummaryRow = {
  slateSizeBucket: string;
  slateCount: number;
  avgSalary: number;
  avgLeftOnTable: number;
  avgPoints: number;
  anyTwoStackRate: number;
  anyThreeStackRate: number;
  anyFourStackRate: number;
  anyFiveStackRate: number;
  multiTeamStackRate: number;
};

export type MlbPerfectLineupSalaryBucketRow = {
  salaryLeftBucket: string;
  slateCount: number;
  rate: number;
  avgSalary: number;
  avgLeftOnTable: number;
  avgPoints: number;
};

export type MlbPerfectLineupSlateSalaryRow = {
  slateId: number;
  slateDate: string;
  slateSizeBucket: string;
  perfectSalary: number;
  salaryLeft: number;
  perfectPoints: number;
  hitterShape: string;
  stackCount: number;
};

export type MlbPerfectLineupShapeRow = {
  slateSizeBucket: string;
  shape: string;
  slateCount: number;
  rate: number;
};

export type MlbPerfectLineupTeamRateRow = {
  teamAbbrev: string;
  teamName: string | null;
  slateAppearances: number;
  perfectAppearances: number;
  avgPerfectHitters: number;
  shrunkAvgPerfectHitters: number;
};

export type MlbPerfectLineupOpponentAllowRow = {
  defenseAbbrev: string;
  defenseName: string | null;
  position: string;
  slateAppearances: number;
  perfectAppearances: number;
  avgAllowed: number;
  shrunkAvgAllowed: number;
};

export type MlbPerfectLineupAnalytics = {
  slateCount: number;
  opponentContextSlateCount: number;
  summary: MlbPerfectLineupSummaryRow[];
  salaryLeftDistribution: MlbPerfectLineupSalaryBucketRow[];
  slateSalaries: MlbPerfectLineupSlateSalaryRow[];
  shapes: MlbPerfectLineupShapeRow[];
  teamRates: MlbPerfectLineupTeamRateRow[];
  opponentAllow: MlbPerfectLineupOpponentAllowRow[];
};

const NBA_ANALYTICS_SLOTS = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"] as const;
const MLB_ANALYTICS_SLOTS = ["P1", "P2", "C", "1B", "2B", "3B", "SS", "OF1", "OF2", "OF3"] as const;
const PERFECT_LINEUP_SALARY_CAP = 50000;
const PERFECT_LINEUP_TEAM_SHRINK = 10;
const PERFECT_LINEUP_DEFENSE_SHRINK = 10;

function canFillNbaAnalyticsSlot(slot: typeof NBA_ANALYTICS_SLOTS[number], eligiblePositions: string): boolean {
  switch (slot) {
    case "PG":
      return eligiblePositions.includes("PG");
    case "SG":
      return eligiblePositions.includes("SG");
    case "SF":
      return eligiblePositions.includes("SF");
    case "PF":
      return eligiblePositions.includes("PF");
    case "C":
      return eligiblePositions.includes("C");
    case "G":
      return eligiblePositions.includes("G");
    case "F":
      return eligiblePositions.includes("F");
    case "UTIL":
      return true;
  }
}

function getNbaPrimaryPosition(eligiblePositions: string): string {
  for (const position of ["PG", "SG", "SF", "PF", "C"] as const) {
    if (eligiblePositions.includes(position)) return position;
  }
  return "UTIL";
}

function isMlbPitcherEligible(eligiblePositions: string): boolean {
  return eligiblePositions.includes("SP") || eligiblePositions.includes("RP") || eligiblePositions === "P" || eligiblePositions.includes("/P");
}

function canFillMlbAnalyticsSlot(slot: typeof MLB_ANALYTICS_SLOTS[number], eligiblePositions: string): boolean {
  switch (slot) {
    case "P1":
    case "P2":
      return isMlbPitcherEligible(eligiblePositions);
    case "C":
      return eligiblePositions.includes("C");
    case "1B":
      return eligiblePositions.includes("1B");
    case "2B":
      return eligiblePositions.includes("2B");
    case "3B":
      return eligiblePositions.includes("3B");
    case "SS":
      return eligiblePositions.includes("SS");
    case "OF1":
    case "OF2":
    case "OF3":
      return eligiblePositions.includes("OF");
  }
}

function getMlbPrimaryPosition(eligiblePositions: string): string {
  if (isMlbPitcherEligible(eligiblePositions)) return "P";
  for (const position of ["C", "1B", "2B", "3B", "SS", "OF"] as const) {
    if (eligiblePositions.includes(position)) return position;
  }
  return "UTIL";
}

function isMlbHitterRow(row: { eligiblePositions: string }): boolean {
  return !isMlbPitcherEligible(row.eligiblePositions);
}

function round2(value: number): number {
  return Math.round(value * 100) / 100;
}

function getNbaSlateSizeBucket(gameCount: number): string {
  if (gameCount <= 2) return "2 games";
  if (gameCount <= 4) return "3-4 games";
  if (gameCount <= 7) return "5-7 games";
  return "8+ games";
}

function getMlbSlateSizeBucket(gameCount: number): string {
  if (gameCount <= 3) return "1-3 games";
  if (gameCount <= 7) return "4-7 games";
  if (gameCount <= 11) return "8-11 games";
  return "12+ games";
}

function getMlbSalaryLeftBucket(salaryLeft: number): string {
  if (salaryLeft <= 0) return "$0 left";
  if (salaryLeft <= 200) return "$1-$200 left";
  if (salaryLeft <= 500) return "$201-$500 left";
  if (salaryLeft <= 1000) return "$501-$1k left";
  return "$1k+ left";
}

function getMlbSalaryLeftBucketOrder(bucket: string): number {
  switch (bucket) {
    case "$0 left":
      return 1;
    case "$1-$200 left":
      return 2;
    case "$201-$500 left":
      return 3;
    case "$501-$1k left":
      return 4;
    case "$1k+ left":
      return 5;
    default:
      return Number.MAX_SAFE_INTEGER;
  }
}

function getSlateSizeBucketOrder(bucket: string): number {
  const match = bucket.match(/^\d+/);
  return match ? parseInt(match[0], 10) : Number.MAX_SAFE_INTEGER;
}

function inferGameCountFromRows(rows: Array<{ teamAbbrev: string }>): number {
  const distinctTeams = new Set(rows.map((row) => row.teamAbbrev).filter(Boolean)).size;
  return Math.max(1, Math.round(distinctTeams / 2));
}

function getOpponentTeamId<T extends { teamId: number | null; homeTeamId: number | null; awayTeamId: number | null }>(row: T): number | null {
  if (row.teamId == null || row.homeTeamId == null || row.awayTeamId == null) return null;
  if (row.teamId === row.homeTeamId) return row.awayTeamId;
  if (row.teamId === row.awayTeamId) return row.homeTeamId;
  return null;
}

function solveNbaPerfectLineup(players: NbaPerfectLineupSourceRow[]): NbaPerfectLineupSourceRow[] {
  if (players.length < NBA_ANALYTICS_SLOTS.length) return [];
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const solver = require("javascript-lp-solver") as { Solve: (model: SolverModel) => SolverResult };

  const constraints: SolverModel["constraints"] = {
    salary: { max: 50000 },
  };
  for (const slot of NBA_ANALYTICS_SLOTS) {
    constraints[`slot_${slot}`] = { equal: 1 };
  }

  const variables: SolverModel["variables"] = {};
  const binaries: SolverModel["binaries"] = {};
  const variableToPlayer = new Map<string, NbaPerfectLineupSourceRow>();

  for (const player of players) {
    constraints[`player_${player.playerRowId}`] = { max: 1 };
    for (const slot of NBA_ANALYTICS_SLOTS) {
      if (!canFillNbaAnalyticsSlot(slot, player.eligiblePositions)) continue;
      const key = `p_${player.playerRowId}_${slot}`;
      variables[key] = {
        objective: player.actualFpts,
        salary: player.salary,
        [`slot_${slot}`]: 1,
        [`player_${player.playerRowId}`]: 1,
      };
      binaries[key] = 1;
      variableToPlayer.set(key, player);
    }
  }

  const result = solver.Solve({
    optimize: "objective",
    opType: "max",
    constraints,
    variables,
    binaries,
  });

  if (!result?.feasible) return [];

  const selectedById = new Map<number, NbaPerfectLineupSourceRow>();
  for (const [key, value] of Object.entries(result)) {
    if (!key.startsWith("p_") || typeof value !== "number" || value < 0.5) continue;
    const player = variableToPlayer.get(key);
    if (!player) continue;
    selectedById.set(player.playerRowId, player);
  }

  return Array.from(selectedById.values());
}

function solveMlbPerfectLineup(players: MlbPerfectLineupSourceRow[]): MlbPerfectLineupSourceRow[] {
  if (players.length < MLB_ANALYTICS_SLOTS.length) return [];
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const solver = require("javascript-lp-solver") as { Solve: (model: SolverModel) => SolverResult };

  const constraints: SolverModel["constraints"] = {
    salary: { max: 50000 },
  };
  for (const slot of MLB_ANALYTICS_SLOTS) {
    constraints[`slot_${slot}`] = { equal: 1 };
  }

  const variables: SolverModel["variables"] = {};
  const binaries: SolverModel["binaries"] = {};
  const variableToPlayer = new Map<string, MlbPerfectLineupSourceRow>();

  for (const player of players) {
    constraints[`player_${player.playerRowId}`] = { max: 1 };
    for (const slot of MLB_ANALYTICS_SLOTS) {
      if (!canFillMlbAnalyticsSlot(slot, player.eligiblePositions)) continue;
      const key = `p_${player.playerRowId}_${slot}`;
      variables[key] = {
        objective: player.actualFpts,
        salary: player.salary,
        [`slot_${slot}`]: 1,
        [`player_${player.playerRowId}`]: 1,
      };
      binaries[key] = 1;
      variableToPlayer.set(key, player);
    }
  }

  const result = solver.Solve({
    optimize: "objective",
    opType: "max",
    constraints,
    variables,
    binaries,
  });

  if (!result?.feasible) return [];

  const selectedById = new Map<number, MlbPerfectLineupSourceRow>();
  for (const [key, value] of Object.entries(result)) {
    if (!key.startsWith("p_") || typeof value !== "number" || value < 0.5) continue;
    const player = variableToPlayer.get(key);
    if (!player) continue;
    selectedById.set(player.playerRowId, player);
  }

  return Array.from(selectedById.values());
}

export async function getNbaPerfectLineupAnalytics(): Promise<NbaPerfectLineupAnalytics | null> {
  const rows = await db.execute<NbaPerfectLineupSourceRow>(sql`
    SELECT
      ds.id               AS "slateId",
      ds.slate_date       AS "slateDate",
      ds.game_count       AS "storedGameCount",
      dp.id               AS "playerRowId",
      dp.team_id          AS "teamId",
      dp.team_abbrev      AS "teamAbbrev",
      t.name              AS "teamName",
      dp.eligible_positions AS "eligiblePositions",
      dp.salary           AS "salary",
      dp.actual_fpts      AS "actualFpts",
      mm.home_team_id     AS "homeTeamId",
      mm.away_team_id     AS "awayTeamId"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN teams t ON t.team_id = dp.team_id
    LEFT JOIN nba_matchups mm ON mm.id = dp.matchup_id
    WHERE ds.sport = 'nba'
      AND dp.actual_fpts IS NOT NULL
    ORDER BY ds.slate_date ASC, ds.id ASC, dp.id ASC
  `);

  if (rows.rows.length === 0) return null;

  const rowsBySlate = new Map<number, NbaPerfectLineupSourceRow[]>();
  for (const row of rows.rows) {
    const slateRows = rowsBySlate.get(row.slateId) ?? [];
    slateRows.push(row);
    rowsBySlate.set(row.slateId, slateRows);
  }

  const summaryStats = new Map<string, {
    slateCount: number;
    totalSalary: number;
    totalPoints: number;
    anyTwo: number;
    anyThree: number;
    anyFour: number;
    multiTeam: number;
  }>();
  const shapeCounts = new Map<string, number>();
  const teamOpportunities = new Map<string, { teamAbbrev: string; teamName: string | null; slateAppearances: number }>();
  const teamPerfectCounts = new Map<string, number>();
  const defenseOpportunities = new Map<string, { defenseAbbrev: string; defenseName: string | null; slateAppearances: number }>();
  const defenseAllowCounts = new Map<string, number>();

  let slateCount = 0;
  let opponentContextSlateCount = 0;
  let totalPerfectSlots = 0;
  const totalPerfectSlotsByPosition = new Map<string, number>();

  for (const slateRows of rowsBySlate.values()) {
    const filteredRows = slateRows.filter((row) => Number.isFinite(row.actualFpts));
    if (filteredRows.length < NBA_ANALYTICS_SLOTS.length) continue;

    const perfectLineup = solveNbaPerfectLineup(filteredRows);
    if (perfectLineup.length !== NBA_ANALYTICS_SLOTS.length) continue;

    slateCount++;
    totalPerfectSlots += perfectLineup.length;

    const storedGameCount = filteredRows[0]?.storedGameCount ?? null;
    const gameCount = storedGameCount && storedGameCount > 0 ? storedGameCount : inferGameCountFromRows(filteredRows);
    const bucket = getNbaSlateSizeBucket(gameCount);
    const teamCounts = new Map<string, number>();
    const distinctSlateTeams = new Map<string, { teamAbbrev: string; teamName: string | null }>();
    const distinctDefenseTeams = new Map<string, { defenseAbbrev: string; defenseName: string | null }>();

    for (const row of filteredRows) {
      if (!row.teamAbbrev) continue;
      const teamKey = row.teamAbbrev;
      if (!distinctSlateTeams.has(teamKey)) {
        distinctSlateTeams.set(teamKey, { teamAbbrev: row.teamAbbrev, teamName: row.teamName ?? null });
      }
      const opponentTeamId = getOpponentTeamId(row);
      if (opponentTeamId == null) continue;
      const opponentRow = filteredRows.find((candidate) => candidate.teamId === opponentTeamId);
      if (!opponentRow?.teamAbbrev) continue;
      const defenseKey = opponentRow.teamAbbrev;
      if (!distinctDefenseTeams.has(defenseKey)) {
        distinctDefenseTeams.set(defenseKey, { defenseAbbrev: opponentRow.teamAbbrev, defenseName: opponentRow.teamName ?? null });
      }
    }

    for (const [teamKey, teamMeta] of distinctSlateTeams) {
      const current = teamOpportunities.get(teamKey) ?? { ...teamMeta, slateAppearances: 0 };
      current.slateAppearances += 1;
      teamOpportunities.set(teamKey, current);
    }

    for (const [defenseKey, defenseMeta] of distinctDefenseTeams) {
      const current = defenseOpportunities.get(defenseKey) ?? { ...defenseMeta, slateAppearances: 0 };
      current.slateAppearances += 1;
      defenseOpportunities.set(defenseKey, current);
    }

    let hasOpponentContext = false;
    for (const player of perfectLineup) {
      const teamKey = player.teamAbbrev;
      teamCounts.set(teamKey, (teamCounts.get(teamKey) ?? 0) + 1);
      teamPerfectCounts.set(teamKey, (teamPerfectCounts.get(teamKey) ?? 0) + 1);

      const position = getNbaPrimaryPosition(player.eligiblePositions);
      totalPerfectSlotsByPosition.set(position, (totalPerfectSlotsByPosition.get(position) ?? 0) + 1);

      const opponentTeamId = getOpponentTeamId(player);
      if (opponentTeamId == null) continue;
      const opponentRow = filteredRows.find((candidate) => candidate.teamId === opponentTeamId);
      if (!opponentRow?.teamAbbrev) continue;
      hasOpponentContext = true;
      const defenseKey = `${opponentRow.teamAbbrev}:${position}`;
      defenseAllowCounts.set(defenseKey, (defenseAllowCounts.get(defenseKey) ?? 0) + 1);
    }

    if (hasOpponentContext) opponentContextSlateCount++;

    const counts = Array.from(teamCounts.values()).sort((a, b) => b - a);
    const stackedCounts = counts.filter((count) => count >= 2);
    const shape = stackedCounts.length > 0 ? stackedCounts.join("-") : "No Stack";
    const summary = summaryStats.get(bucket) ?? {
      slateCount: 0,
      totalSalary: 0,
      totalPoints: 0,
      anyTwo: 0,
      anyThree: 0,
      anyFour: 0,
      multiTeam: 0,
    };
    summary.slateCount += 1;
    summary.totalSalary += perfectLineup.reduce((sum, player) => sum + player.salary, 0);
    summary.totalPoints += perfectLineup.reduce((sum, player) => sum + player.actualFpts, 0);
    if (stackedCounts.some((count) => count >= 2)) summary.anyTwo += 1;
    if (stackedCounts.some((count) => count >= 3)) summary.anyThree += 1;
    if (stackedCounts.some((count) => count >= 4)) summary.anyFour += 1;
    if (stackedCounts.filter((count) => count >= 2).length >= 2) summary.multiTeam += 1;
    summaryStats.set(bucket, summary);

    const shapeKey = `${bucket}::${shape}`;
    shapeCounts.set(shapeKey, (shapeCounts.get(shapeKey) ?? 0) + 1);
  }

  if (slateCount === 0) return null;

  const globalTeamRate = totalPerfectSlots / Math.max(1, Array.from(teamOpportunities.values()).reduce((sum, row) => sum + row.slateAppearances, 0));
  const globalPositionRates = new Map<string, number>();
  const totalDefenseOpportunities = Math.max(1, Array.from(defenseOpportunities.values()).reduce((sum, row) => sum + row.slateAppearances, 0));
  for (const [position, count] of totalPerfectSlotsByPosition) {
    globalPositionRates.set(position, count / totalDefenseOpportunities);
  }

  const summary = Array.from(summaryStats.entries())
    .map(([bucket, stats]) => ({
      slateSizeBucket: bucket,
      slateCount: stats.slateCount,
      avgSalary: round2(stats.totalSalary / stats.slateCount),
      avgPoints: round2(stats.totalPoints / stats.slateCount),
      anyTwoStackRate: round2((stats.anyTwo / stats.slateCount) * 100),
      anyThreeStackRate: round2((stats.anyThree / stats.slateCount) * 100),
      anyFourStackRate: round2((stats.anyFour / stats.slateCount) * 100),
      multiTeamStackRate: round2((stats.multiTeam / stats.slateCount) * 100),
    }))
    .sort((a, b) => getSlateSizeBucketOrder(a.slateSizeBucket) - getSlateSizeBucketOrder(b.slateSizeBucket));

  const bucketSlateCounts = new Map(summary.map((row) => [row.slateSizeBucket, row.slateCount]));
  const shapes = Array.from(shapeCounts.entries())
    .map(([key, count]) => {
      const [bucket, shape] = key.split("::");
      const bucketCount = bucketSlateCounts.get(bucket) ?? 1;
      return {
        slateSizeBucket: bucket,
        shape,
        slateCount: count,
        rate: round2((count / bucketCount) * 100),
      };
    })
    .sort((a, b) =>
      getSlateSizeBucketOrder(a.slateSizeBucket) - getSlateSizeBucketOrder(b.slateSizeBucket)
      || b.slateCount - a.slateCount
      || a.shape.localeCompare(b.shape)
    );

  const teamRates = Array.from(teamOpportunities.entries())
    .map(([teamKey, meta]) => {
      const perfectAppearances = teamPerfectCounts.get(teamKey) ?? 0;
      const avgPerfectPlayers = perfectAppearances / meta.slateAppearances;
      const shrunkAvgPerfectPlayers = (perfectAppearances + PERFECT_LINEUP_TEAM_SHRINK * globalTeamRate)
        / (meta.slateAppearances + PERFECT_LINEUP_TEAM_SHRINK);
      return {
        teamAbbrev: meta.teamAbbrev,
        teamName: meta.teamName,
        slateAppearances: meta.slateAppearances,
        perfectAppearances,
        avgPerfectPlayers: round2(avgPerfectPlayers),
        shrunkAvgPerfectPlayers: round2(shrunkAvgPerfectPlayers),
      };
    })
    .filter((row) => row.slateAppearances > 0)
    .sort((a, b) =>
      b.shrunkAvgPerfectPlayers - a.shrunkAvgPerfectPlayers
      || b.perfectAppearances - a.perfectAppearances
      || a.teamAbbrev.localeCompare(b.teamAbbrev)
    );

  const opponentAllow = Array.from(defenseOpportunities.entries())
    .flatMap(([defenseKey, meta]) =>
      ["PG", "SG", "SF", "PF", "C"].map((position) => {
        const perfectAppearances = defenseAllowCounts.get(`${defenseKey}:${position}`) ?? 0;
        const avgAllowed = perfectAppearances / meta.slateAppearances;
        const baseline = globalPositionRates.get(position) ?? 0;
        const shrunkAvgAllowed = (perfectAppearances + PERFECT_LINEUP_DEFENSE_SHRINK * baseline)
          / (meta.slateAppearances + PERFECT_LINEUP_DEFENSE_SHRINK);
        return {
          defenseAbbrev: meta.defenseAbbrev,
          defenseName: meta.defenseName,
          position,
          slateAppearances: meta.slateAppearances,
          perfectAppearances,
          avgAllowed: round2(avgAllowed),
          shrunkAvgAllowed: round2(shrunkAvgAllowed),
        };
      }),
    )
    .filter((row) => row.slateAppearances > 0)
    .sort((a, b) =>
      b.shrunkAvgAllowed - a.shrunkAvgAllowed
      || b.perfectAppearances - a.perfectAppearances
      || a.defenseAbbrev.localeCompare(b.defenseAbbrev)
      || a.position.localeCompare(b.position)
    );

  return {
    slateCount,
    opponentContextSlateCount,
    summary,
    shapes,
    teamRates,
    opponentAllow,
  };
}

export async function getMlbPerfectLineupAnalytics(): Promise<MlbPerfectLineupAnalytics | null> {
  const rows = await db.execute<MlbPerfectLineupSourceRow>(sql`
    SELECT
      ds.id                 AS "slateId",
      ds.slate_date         AS "slateDate",
      ds.game_count         AS "storedGameCount",
      dp.id                 AS "playerRowId",
      dp.mlb_team_id        AS "teamId",
      dp.team_abbrev        AS "teamAbbrev",
      mt.name               AS "teamName",
      dp.eligible_positions AS "eligiblePositions",
      dp.salary             AS "salary",
      dp.actual_fpts        AS "actualFpts",
      mm.home_team_id       AS "homeTeamId",
      mm.away_team_id       AS "awayTeamId"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN mlb_teams mt ON mt.team_id = dp.mlb_team_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE ds.sport = 'mlb'
      AND dp.actual_fpts IS NOT NULL
    ORDER BY ds.slate_date ASC, ds.id ASC, dp.id ASC
  `);

  if (rows.rows.length === 0) return null;

  const rowsBySlate = new Map<number, MlbPerfectLineupSourceRow[]>();
  for (const row of rows.rows) {
    const slateRows = rowsBySlate.get(row.slateId) ?? [];
    slateRows.push(row);
    rowsBySlate.set(row.slateId, slateRows);
  }

  const summaryStats = new Map<string, {
    slateCount: number;
    totalSalary: number;
    totalLeftOnTable: number;
    totalPoints: number;
    anyTwo: number;
    anyThree: number;
    anyFour: number;
    anyFive: number;
    multiTeam: number;
  }>();
  const salaryLeftStats = new Map<string, {
    slateCount: number;
    totalSalary: number;
    totalLeftOnTable: number;
    totalPoints: number;
  }>();
  const slateSalaries: MlbPerfectLineupSlateSalaryRow[] = [];
  const shapeCounts = new Map<string, number>();
  const teamOpportunities = new Map<string, { teamAbbrev: string; teamName: string | null; slateAppearances: number }>();
  const teamPerfectCounts = new Map<string, number>();
  const defenseOpportunities = new Map<string, { defenseAbbrev: string; defenseName: string | null; slateAppearances: number }>();
  const defenseAllowCounts = new Map<string, number>();

  let slateCount = 0;
  let opponentContextSlateCount = 0;
  let totalPerfectHitters = 0;
  const totalPerfectSlotsByPosition = new Map<string, number>();

  for (const slateRows of rowsBySlate.values()) {
    const filteredRows = slateRows.filter((row) => Number.isFinite(row.actualFpts));
    if (filteredRows.length < MLB_ANALYTICS_SLOTS.length) continue;

    const perfectLineup = solveMlbPerfectLineup(filteredRows);
    if (perfectLineup.length !== MLB_ANALYTICS_SLOTS.length) continue;

    const perfectHitters = perfectLineup.filter(isMlbHitterRow);
    if (perfectHitters.length === 0) continue;
    const mappedPerfectHitters = perfectHitters.filter((row) => row.teamId != null && row.teamAbbrev && row.teamAbbrev !== "UNK");
    if (mappedPerfectHitters.length !== perfectHitters.length) continue;

    slateCount++;
    totalPerfectHitters += mappedPerfectHitters.length;

    const storedGameCount = filteredRows[0]?.storedGameCount ?? null;
    const gameCount = storedGameCount && storedGameCount > 0 ? storedGameCount : inferGameCountFromRows(filteredRows);
    const bucket = getMlbSlateSizeBucket(gameCount);
    const teamCounts = new Map<string, number>();
    const distinctSlateTeams = new Map<string, { teamAbbrev: string; teamName: string | null }>();
    const distinctDefenseTeams = new Map<string, { defenseAbbrev: string; defenseName: string | null }>();

    for (const row of filteredRows.filter((candidate) => isMlbHitterRow(candidate) && candidate.teamId != null && candidate.teamAbbrev && candidate.teamAbbrev !== "UNK")) {
      const teamKey = row.teamAbbrev;
      if (!distinctSlateTeams.has(teamKey)) {
        distinctSlateTeams.set(teamKey, { teamAbbrev: row.teamAbbrev, teamName: row.teamName ?? null });
      }
      const opponentTeamId = getOpponentTeamId(row);
      if (opponentTeamId == null) continue;
      const opponentRow = filteredRows.find((candidate) =>
        candidate.teamId === opponentTeamId
        && isMlbHitterRow(candidate)
        && candidate.teamAbbrev
        && candidate.teamAbbrev !== "UNK"
      );
      if (!opponentRow?.teamAbbrev) continue;
      const defenseKey = opponentRow.teamAbbrev;
      if (!distinctDefenseTeams.has(defenseKey)) {
        distinctDefenseTeams.set(defenseKey, { defenseAbbrev: opponentRow.teamAbbrev, defenseName: opponentRow.teamName ?? null });
      }
    }

    for (const [teamKey, teamMeta] of distinctSlateTeams) {
      const current = teamOpportunities.get(teamKey) ?? { ...teamMeta, slateAppearances: 0 };
      current.slateAppearances += 1;
      teamOpportunities.set(teamKey, current);
    }

    for (const [defenseKey, defenseMeta] of distinctDefenseTeams) {
      const current = defenseOpportunities.get(defenseKey) ?? { ...defenseMeta, slateAppearances: 0 };
      current.slateAppearances += 1;
      defenseOpportunities.set(defenseKey, current);
    }

    let hasOpponentContext = false;
    for (const player of mappedPerfectHitters) {
      const teamKey = player.teamAbbrev;
      teamCounts.set(teamKey, (teamCounts.get(teamKey) ?? 0) + 1);
      teamPerfectCounts.set(teamKey, (teamPerfectCounts.get(teamKey) ?? 0) + 1);

      const position = getMlbPrimaryPosition(player.eligiblePositions);
      totalPerfectSlotsByPosition.set(position, (totalPerfectSlotsByPosition.get(position) ?? 0) + 1);

      const opponentTeamId = getOpponentTeamId(player);
      if (opponentTeamId == null) continue;
      const opponentRow = filteredRows.find((candidate) =>
        candidate.teamId === opponentTeamId
        && isMlbHitterRow(candidate)
        && candidate.teamAbbrev
        && candidate.teamAbbrev !== "UNK"
      );
      if (!opponentRow?.teamAbbrev) continue;
      hasOpponentContext = true;
      const defenseKey = `${opponentRow.teamAbbrev}:${position}`;
      defenseAllowCounts.set(defenseKey, (defenseAllowCounts.get(defenseKey) ?? 0) + 1);
    }

    if (hasOpponentContext) opponentContextSlateCount++;

    const counts = Array.from(teamCounts.values()).sort((a, b) => b - a);
    const stackedCounts = counts.filter((count) => count >= 2);
    const shape = stackedCounts.length > 0 ? stackedCounts.join("-") : "No Stack";
    const perfectSalary = perfectLineup.reduce((sum, player) => sum + player.salary, 0);
    const perfectPoints = perfectLineup.reduce((sum, player) => sum + player.actualFpts, 0);
    const salaryLeft = Math.max(0, PERFECT_LINEUP_SALARY_CAP - perfectSalary);
    const summary = summaryStats.get(bucket) ?? {
      slateCount: 0,
      totalSalary: 0,
      totalLeftOnTable: 0,
      totalPoints: 0,
      anyTwo: 0,
      anyThree: 0,
      anyFour: 0,
      anyFive: 0,
      multiTeam: 0,
    };
    summary.slateCount += 1;
    summary.totalSalary += perfectSalary;
    summary.totalLeftOnTable += salaryLeft;
    summary.totalPoints += perfectPoints;
    if (stackedCounts.some((count) => count >= 2)) summary.anyTwo += 1;
    if (stackedCounts.some((count) => count >= 3)) summary.anyThree += 1;
    if (stackedCounts.some((count) => count >= 4)) summary.anyFour += 1;
    if (stackedCounts.some((count) => count >= 5)) summary.anyFive += 1;
    if (stackedCounts.filter((count) => count >= 2).length >= 2) summary.multiTeam += 1;
    summaryStats.set(bucket, summary);

    const shapeKey = `${bucket}::${shape}`;
    shapeCounts.set(shapeKey, (shapeCounts.get(shapeKey) ?? 0) + 1);

    const salaryLeftBucket = getMlbSalaryLeftBucket(salaryLeft);
    const salaryBucket = salaryLeftStats.get(salaryLeftBucket) ?? {
      slateCount: 0,
      totalSalary: 0,
      totalLeftOnTable: 0,
      totalPoints: 0,
    };
    salaryBucket.slateCount += 1;
    salaryBucket.totalSalary += perfectSalary;
    salaryBucket.totalLeftOnTable += salaryLeft;
    salaryBucket.totalPoints += perfectPoints;
    salaryLeftStats.set(salaryLeftBucket, salaryBucket);

    slateSalaries.push({
      slateId: filteredRows[0].slateId,
      slateDate: filteredRows[0].slateDate,
      slateSizeBucket: bucket,
      perfectSalary,
      salaryLeft,
      perfectPoints: round2(perfectPoints),
      hitterShape: shape,
      stackCount: stackedCounts.length,
    });
  }

  if (slateCount === 0) return null;

  const globalTeamRate = totalPerfectHitters / Math.max(1, Array.from(teamOpportunities.values()).reduce((sum, row) => sum + row.slateAppearances, 0));
  const globalPositionRates = new Map<string, number>();
  const totalDefenseOpportunities = Math.max(1, Array.from(defenseOpportunities.values()).reduce((sum, row) => sum + row.slateAppearances, 0));
  for (const [position, count] of totalPerfectSlotsByPosition) {
    globalPositionRates.set(position, count / totalDefenseOpportunities);
  }

  const summary = Array.from(summaryStats.entries())
    .map(([bucket, stats]) => ({
      slateSizeBucket: bucket,
      slateCount: stats.slateCount,
      avgSalary: round2(stats.totalSalary / stats.slateCount),
      avgLeftOnTable: round2(stats.totalLeftOnTable / stats.slateCount),
      avgPoints: round2(stats.totalPoints / stats.slateCount),
      anyTwoStackRate: round2((stats.anyTwo / stats.slateCount) * 100),
      anyThreeStackRate: round2((stats.anyThree / stats.slateCount) * 100),
      anyFourStackRate: round2((stats.anyFour / stats.slateCount) * 100),
      anyFiveStackRate: round2((stats.anyFive / stats.slateCount) * 100),
      multiTeamStackRate: round2((stats.multiTeam / stats.slateCount) * 100),
    }))
    .sort((a, b) => getSlateSizeBucketOrder(a.slateSizeBucket) - getSlateSizeBucketOrder(b.slateSizeBucket));

  const salaryLeftDistribution = Array.from(salaryLeftStats.entries())
    .map(([salaryLeftBucket, stats]) => ({
      salaryLeftBucket,
      slateCount: stats.slateCount,
      rate: round2((stats.slateCount / slateCount) * 100),
      avgSalary: round2(stats.totalSalary / stats.slateCount),
      avgLeftOnTable: round2(stats.totalLeftOnTable / stats.slateCount),
      avgPoints: round2(stats.totalPoints / stats.slateCount),
    }))
    .sort((a, b) => getMlbSalaryLeftBucketOrder(a.salaryLeftBucket) - getMlbSalaryLeftBucketOrder(b.salaryLeftBucket));

  slateSalaries.sort((a, b) =>
    b.slateDate.localeCompare(a.slateDate)
    || b.slateId - a.slateId
  );

  const bucketSlateCounts = new Map(summary.map((row) => [row.slateSizeBucket, row.slateCount]));
  const shapes = Array.from(shapeCounts.entries())
    .map(([key, count]) => {
      const [bucket, shape] = key.split("::");
      const bucketCount = bucketSlateCounts.get(bucket) ?? 1;
      return {
        slateSizeBucket: bucket,
        shape,
        slateCount: count,
        rate: round2((count / bucketCount) * 100),
      };
    })
    .sort((a, b) =>
      getSlateSizeBucketOrder(a.slateSizeBucket) - getSlateSizeBucketOrder(b.slateSizeBucket)
      || b.slateCount - a.slateCount
      || a.shape.localeCompare(b.shape)
    );

  const teamRates = Array.from(teamOpportunities.entries())
    .map(([teamKey, meta]) => {
      const perfectAppearances = teamPerfectCounts.get(teamKey) ?? 0;
      const avgPerfectHitters = perfectAppearances / meta.slateAppearances;
      const shrunkAvgPerfectHitters = (perfectAppearances + PERFECT_LINEUP_TEAM_SHRINK * globalTeamRate)
        / (meta.slateAppearances + PERFECT_LINEUP_TEAM_SHRINK);
      return {
        teamAbbrev: meta.teamAbbrev,
        teamName: meta.teamName,
        slateAppearances: meta.slateAppearances,
        perfectAppearances,
        avgPerfectHitters: round2(avgPerfectHitters),
        shrunkAvgPerfectHitters: round2(shrunkAvgPerfectHitters),
      };
    })
    .filter((row) => row.slateAppearances > 0)
    .sort((a, b) =>
      b.shrunkAvgPerfectHitters - a.shrunkAvgPerfectHitters
      || b.perfectAppearances - a.perfectAppearances
      || a.teamAbbrev.localeCompare(b.teamAbbrev)
    );

  const opponentAllow = Array.from(defenseOpportunities.entries())
    .flatMap(([defenseKey, meta]) =>
      ["C", "1B", "2B", "3B", "SS", "OF"].map((position) => {
        const perfectAppearances = defenseAllowCounts.get(`${defenseKey}:${position}`) ?? 0;
        const avgAllowed = perfectAppearances / meta.slateAppearances;
        const baseline = globalPositionRates.get(position) ?? 0;
        const shrunkAvgAllowed = (perfectAppearances + PERFECT_LINEUP_DEFENSE_SHRINK * baseline)
          / (meta.slateAppearances + PERFECT_LINEUP_DEFENSE_SHRINK);
        return {
          defenseAbbrev: meta.defenseAbbrev,
          defenseName: meta.defenseName,
          position,
          slateAppearances: meta.slateAppearances,
          perfectAppearances,
          avgAllowed: round2(avgAllowed),
          shrunkAvgAllowed: round2(shrunkAvgAllowed),
        };
      }),
    )
    .filter((row) => row.slateAppearances > 0)
    .sort((a, b) =>
      b.shrunkAvgAllowed - a.shrunkAvgAllowed
      || b.perfectAppearances - a.perfectAppearances
      || a.defenseAbbrev.localeCompare(b.defenseAbbrev)
      || a.position.localeCompare(b.position)
    );

  return {
    slateCount,
    opponentContextSlateCount,
    summary,
    salaryLeftDistribution,
    slateSalaries,
    shapes,
    teamRates,
    opponentAllow,
  };
}

export { CURRENT_SEASON };

// ---------------------------------------------------------------------------
// MLB Batting Order Calibration
// ---------------------------------------------------------------------------

export type MlbBattingOrderCalibrationRow = {
  orderSlot: number;
  n: number;
  avgProj: number | null;
  avgActual: number | null;
  avgDelta: number | null;
  avgProjOwn: number | null;
  avgActualOwn: number | null;
};

export async function getMlbBattingOrderCalibration(): Promise<
  MlbBattingOrderCalibrationRow[]
> {
  const rows = await db.execute(sql`
    WITH hitter_sample AS (
      SELECT
        dp.dk_starting_lineup_order AS order_slot,
        COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS blended_proj,
        dp.actual_fpts,
        dp.proj_own_pct,
        dp.actual_own_pct
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE ds.sport = 'mlb'
        AND COALESCE(ds.contest_type, 'main') = 'main'
        AND COALESCE(ds.contest_format, 'gpp') = 'gpp'
        AND COALESCE(dp.is_out, false) = false
        AND dp.dk_starting_lineup_order BETWEEN 1 AND 9
        AND dp.eligible_positions NOT LIKE '%SP%'
        AND dp.eligible_positions NOT LIKE '%RP%'
    )
    SELECT
      hs.order_slot AS "orderSlot",
      COUNT(*) FILTER (WHERE hs.actual_fpts IS NOT NULL AND hs.blended_proj IS NOT NULL) AS "n",
      AVG(hs.blended_proj)
        FILTER (WHERE hs.actual_fpts IS NOT NULL AND hs.blended_proj IS NOT NULL) AS "avgProj",
      AVG(hs.actual_fpts)
        FILTER (WHERE hs.actual_fpts IS NOT NULL AND hs.blended_proj IS NOT NULL) AS "avgActual",
      AVG(hs.actual_fpts - hs.blended_proj)
        FILTER (WHERE hs.actual_fpts IS NOT NULL AND hs.blended_proj IS NOT NULL) AS "avgDelta",
      AVG(hs.proj_own_pct)
        FILTER (WHERE hs.proj_own_pct IS NOT NULL) AS "avgProjOwn",
      AVG(hs.actual_own_pct)
        FILTER (WHERE hs.actual_own_pct IS NOT NULL) AS "avgActualOwn"
    FROM hitter_sample hs
    GROUP BY hs.order_slot
    ORDER BY hs.order_slot ASC
  `);
  return (rows.rows as MlbBattingOrderCalibrationRow[]).map((r) => ({
    orderSlot: Number(r.orderSlot),
    n: Number(r.n),
    avgProj: r.avgProj != null ? Number(r.avgProj) : null,
    avgActual: r.avgActual != null ? Number(r.avgActual) : null,
    avgDelta: r.avgDelta != null ? Number(r.avgDelta) : null,
    avgProjOwn: r.avgProjOwn != null ? Number(r.avgProjOwn) : null,
    avgActualOwn: r.avgActualOwn != null ? Number(r.avgActualOwn) : null,
  }));
}

// ---------------------------------------------------------------------------
// Projection Source Breakdown (live vs our vs LineStar, per slate)
// ---------------------------------------------------------------------------

export type ProjectionSourceRow = {
  slateDate: string;
  sport: string;
  nLive: number;
  nOur: number;
  nLs: number;
  liveMae: number | null;
  liveBias: number | null;
  ourMae: number | null;
  ourBias: number | null;
  lsMae: number | null;
  lsBias: number | null;
};

export async function getProjectionSourceBreakdown(
  sport: Sport,
): Promise<ProjectionSourceRow[]> {
  const rows = await db.execute(sql`
    SELECT
      ds.slate_date                                                                AS "slateDate",
      ds.sport                                                                     AS "sport",
      COUNT(*) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.live_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "nLive",
      COUNT(*) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "nOur",
      COUNT(*) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "nLs",
      AVG(ABS(dp.live_proj - dp.actual_fpts)) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.live_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "liveMae",
      AVG(dp.live_proj - dp.actual_fpts) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.live_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "liveBias",
      AVG(ABS(dp.our_proj - dp.actual_fpts)) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "ourMae",
      AVG(dp.our_proj - dp.actual_fpts) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "ourBias",
      AVG(ABS(dp.linestar_proj - dp.actual_fpts)) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "lsMae",
      AVG(dp.linestar_proj - dp.actual_fpts) FILTER (
        WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL
          AND dp.is_out IS NOT TRUE
      )                                                                            AS "lsBias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
    GROUP BY ds.slate_date, ds.sport
    HAVING COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL) > 0
    ORDER BY ds.slate_date DESC
    LIMIT 20
  `);
  return (rows.rows as ProjectionSourceRow[]).map((r) => ({
    slateDate: String(r.slateDate),
    sport: String(r.sport),
    nLive: Number(r.nLive),
    nOur: Number(r.nOur),
    nLs: Number(r.nLs),
    liveMae: r.liveMae != null ? Number(r.liveMae) : null,
    liveBias: r.liveBias != null ? Number(r.liveBias) : null,
    ourMae: r.ourMae != null ? Number(r.ourMae) : null,
    ourBias: r.ourBias != null ? Number(r.ourBias) : null,
    lsMae: r.lsMae != null ? Number(r.lsMae) : null,
    lsBias: r.lsBias != null ? Number(r.lsBias) : null,
  }));
}

// ---------------------------------------------------------------------------
// Vegas Analysis
// ---------------------------------------------------------------------------

export type VegasMatchupRow = {
  matchupId: number;
  gameDate: string;
  homeTeam: string;
  awayTeam: string;
  homeAbbrev: string;
  awayAbbrev: string;
  vegasTotal: number | null;
  homeMl: number | null;
  awayMl: number | null;
  homeSpread: number | null;
  homeWinProb: number | null;
  homeImplied: number | null;
  awayImplied: number | null;
  homeScore: number | null;
  awayScore: number | null;
  // MLB-only SP fields (null for NBA)
  homeSpName: string | null;
  homeSpHand: string | null;
  homeSpXfip: number | null;
  homeSpKPer9: number | null;
  awaySpName: string | null;
  awaySpHand: string | null;
  awaySpXfip: number | null;
  awaySpKPer9: number | null;
};

export async function getVegasMatchups(gameDate?: string): Promise<VegasMatchupRow[]> {
  const targetDate = gameDate ?? new Date().toISOString().slice(0, 10);
  const rows = await db.execute(sql`
    SELECT
      nm.id            AS "matchupId",
      nm.game_date     AS "gameDate",
      t_home.name      AS "homeTeam",
      t_away.name      AS "awayTeam",
      t_home.abbreviation AS "homeAbbrev",
      t_away.abbreviation AS "awayAbbrev",
      nm.vegas_total   AS "vegasTotal",
      nm.home_ml       AS "homeMl",
      nm.away_ml       AS "awayMl",
      nm.home_spread   AS "homeSpread",
      nm.vegas_prob_home AS "homeWinProb",
      nm.home_implied  AS "homeImplied",
      nm.away_implied  AS "awayImplied",
      nm.home_score    AS "homeScore",
      nm.away_score    AS "awayScore"
    FROM nba_matchups nm
    LEFT JOIN teams t_home ON t_home.team_id = nm.home_team_id
    LEFT JOIN teams t_away ON t_away.team_id = nm.away_team_id
    WHERE nm.game_date = ${targetDate}
    ORDER BY nm.vegas_total DESC NULLS LAST
  `);
  return (rows.rows as VegasMatchupRow[]).map((r) => ({
    matchupId: Number(r.matchupId),
    gameDate: String(r.gameDate),
    homeTeam: String(r.homeTeam ?? ""),
    awayTeam: String(r.awayTeam ?? ""),
    homeAbbrev: String(r.homeAbbrev ?? ""),
    awayAbbrev: String(r.awayAbbrev ?? ""),
    vegasTotal: r.vegasTotal != null ? Number(r.vegasTotal) : null,
    homeMl: r.homeMl != null ? Number(r.homeMl) : null,
    awayMl: r.awayMl != null ? Number(r.awayMl) : null,
    homeSpread: r.homeSpread != null ? Number(r.homeSpread) : null,
    homeWinProb: r.homeWinProb != null ? Number(r.homeWinProb) : null,
    homeImplied: r.homeImplied != null ? Number(r.homeImplied) : null,
    awayImplied: r.awayImplied != null ? Number(r.awayImplied) : null,
    homeScore: r.homeScore != null ? Number(r.homeScore) : null,
    awayScore: r.awayScore != null ? Number(r.awayScore) : null,
    homeSpName: null,
    homeSpHand: null,
    homeSpXfip: null,
    homeSpKPer9: null,
    awaySpName: null,
    awaySpHand: null,
    awaySpXfip: null,
    awaySpKPer9: null,
  }));
}

export type OuHitRateRow = {
  totalTier: string;
  tierMin: number | null;
  n: number;
  overCount: number;
  underCount: number;
  pushCount: number;
  overRate: number | null;
  avgTotal: number | null;
  avgActual: number | null;
  gameTotalMae: number | null;
};

export async function getOuHitRate(): Promise<OuHitRateRow[]> {
  const rows = await db.execute(sql`
    SELECT
      CASE
        WHEN nm.vegas_total < 215 THEN 'Under 215'
        WHEN nm.vegas_total < 220 THEN '215–220'
        WHEN nm.vegas_total < 225 THEN '220–225'
        WHEN nm.vegas_total < 230 THEN '225–230'
        WHEN nm.vegas_total < 235 THEN '230–235'
        WHEN nm.vegas_total < 240 THEN '235–240'
        ELSE '240+'
      END                                                                 AS "totalTier",
      MIN(nm.vegas_total)                                                 AS "tierMin",
      COUNT(*)                                                            AS "n",
      COUNT(*) FILTER (WHERE nm.home_score + nm.away_score > nm.vegas_total) AS "overCount",
      COUNT(*) FILTER (WHERE nm.home_score + nm.away_score < nm.vegas_total) AS "underCount",
      COUNT(*) FILTER (WHERE nm.home_score + nm.away_score = nm.vegas_total) AS "pushCount",
      AVG(nm.vegas_total)                                                 AS "avgTotal",
      AVG((nm.home_score + nm.away_score)::DOUBLE PRECISION)             AS "avgActual",
      AVG(ABS((nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total)) AS "gameTotalMae"
    FROM nba_matchups nm
    WHERE nm.vegas_total IS NOT NULL
      AND nm.home_score IS NOT NULL
      AND nm.away_score IS NOT NULL
    GROUP BY 1
    ORDER BY MIN(nm.vegas_total) ASC NULLS LAST
  `);
  return (rows.rows as OuHitRateRow[]).map((r) => {
    const n = Number(r.n);
    const overCount = Number(r.overCount);
    return {
      totalTier: String(r.totalTier),
      tierMin: r.tierMin != null ? Number(r.tierMin) : null,
      n,
      overCount,
      underCount: Number(r.underCount),
      pushCount: Number(r.pushCount),
      overRate: n > 0 ? overCount / n : null,
      avgTotal: r.avgTotal != null ? Number(r.avgTotal) : null,
      avgActual: r.avgActual != null ? Number(r.avgActual) : null,
      gameTotalMae: r.gameTotalMae != null ? Number(r.gameTotalMae) : null,
    };
  });
}

export type TeamTotalAccuracyRow = {
  teamAbbrev: string;
  teamName: string;
  n: number;
  avgImplied: number | null;
  avgActual: number | null;
  mae: number | null;
  bias: number | null;
};

export async function getTeamTotalAccuracy(): Promise<TeamTotalAccuracyRow[]> {
  const rows = await db.execute(sql`
    WITH team_games AS (
      -- Home perspective
      SELECT
        nm.home_team_id                              AS team_id,
        nm.home_implied                              AS implied_total,
        nm.home_score::DOUBLE PRECISION              AS actual_score
      FROM nba_matchups nm
      WHERE nm.home_implied IS NOT NULL AND nm.home_score IS NOT NULL
      UNION ALL
      -- Away perspective
      SELECT
        nm.away_team_id                              AS team_id,
        nm.away_implied                              AS implied_total,
        nm.away_score::DOUBLE PRECISION              AS actual_score
      FROM nba_matchups nm
      WHERE nm.away_implied IS NOT NULL AND nm.away_score IS NOT NULL
    )
    SELECT
      t.abbreviation                                 AS "teamAbbrev",
      t.name                                         AS "teamName",
      COUNT(*)                                       AS "n",
      AVG(tg.implied_total)                          AS "avgImplied",
      AVG(tg.actual_score)                           AS "avgActual",
      AVG(ABS(tg.implied_total - tg.actual_score))   AS "mae",
      AVG(tg.implied_total - tg.actual_score)        AS "bias"
    FROM team_games tg
    JOIN teams t ON t.team_id = tg.team_id
    GROUP BY t.abbreviation, t.name
    HAVING COUNT(*) >= 3
    ORDER BY AVG(ABS(tg.implied_total - tg.actual_score)) DESC NULLS LAST
  `);
  return (rows.rows as TeamTotalAccuracyRow[]).map((r) => ({
    teamAbbrev: String(r.teamAbbrev),
    teamName: String(r.teamName),
    n: Number(r.n),
    avgImplied: r.avgImplied != null ? Number(r.avgImplied) : null,
    avgActual: r.avgActual != null ? Number(r.avgActual) : null,
    mae: r.mae != null ? Number(r.mae) : null,
    bias: r.bias != null ? Number(r.bias) : null,
  }));
}

export type SpreadCoverageRow = {
  spreadTier: string;
  tierMin: number | null;
  n: number;
  coverCount: number;
  coverRate: number | null;
  avgSpread: number | null;
  avgMargin: number | null;
};

export async function getSpreadCoverage(): Promise<SpreadCoverageRow[]> {
  const rows = await db.execute(sql`
    SELECT
      CASE
        WHEN ABS(nm.home_spread) <= 1.5  THEN 'Pick / ±1.5'
        WHEN ABS(nm.home_spread) <= 3.5  THEN '2–3.5'
        WHEN ABS(nm.home_spread) <= 6.5  THEN '4–6.5'
        WHEN ABS(nm.home_spread) <= 9.5  THEN '7–9.5'
        WHEN ABS(nm.home_spread) <= 13.5 THEN '10–13.5'
        ELSE '14+'
      END                                                         AS "spreadTier",
      MIN(ABS(nm.home_spread))                                    AS "tierMin",
      COUNT(*)                                                     AS "n",
      -- Favorite covered = actual margin > spread (in favor of favorite)
      COUNT(*) FILTER (WHERE
        (nm.home_spread < 0 AND (nm.home_score - nm.away_score) > ABS(nm.home_spread))
        OR
        (nm.home_spread > 0 AND (nm.away_score - nm.home_score) > ABS(nm.home_spread))
      )                                                            AS "coverCount",
      AVG(ABS(nm.home_spread))                                    AS "avgSpread",
      AVG(ABS(nm.home_score - nm.away_score)::DOUBLE PRECISION)  AS "avgMargin"
    FROM nba_matchups nm
    WHERE nm.home_spread IS NOT NULL
      AND nm.home_score IS NOT NULL
      AND nm.away_score IS NOT NULL
      AND nm.home_spread <> 0
    GROUP BY 1
    ORDER BY MIN(ABS(nm.home_spread)) ASC NULLS LAST
  `);
  return (rows.rows as SpreadCoverageRow[]).map((r) => {
    const n = Number(r.n);
    const coverCount = Number(r.coverCount);
    return {
      spreadTier: String(r.spreadTier),
      tierMin: r.tierMin != null ? Number(r.tierMin) : null,
      n,
      coverCount,
      coverRate: n > 0 ? coverCount / n : null,
      avgSpread: r.avgSpread != null ? Number(r.avgSpread) : null,
      avgMargin: r.avgMargin != null ? Number(r.avgMargin) : null,
    };
  });
}

// ---------------------------------------------------------------------------
// MLB Vegas Analysis
// ---------------------------------------------------------------------------

export async function getMlbVegasMatchups(gameDate?: string): Promise<VegasMatchupRow[]> {
  const targetDate = gameDate ?? new Date().toISOString().slice(0, 10);
  await ensureAnalyticsColumns();
  const rows = await db.execute(sql`
    WITH latest_pitcher AS (
      SELECT DISTINCT ON (player_id)
        player_id, name, hand, k_per_9, xfip
      FROM mlb_pitcher_stats
      ORDER BY player_id, season DESC, fetched_at DESC, id DESC
    ),
    latest_pitcher_by_name AS (
      SELECT DISTINCT ON (LOWER(name))
        LOWER(name) AS name_key,
        name,
        hand,
        k_per_9,
        xfip
      FROM mlb_pitcher_stats
      ORDER BY LOWER(name), season DESC, fetched_at DESC, id DESC
    )
    SELECT
      m.id             AS "matchupId",
      m.game_date      AS "gameDate",
      ht.name          AS "homeTeam",
      at.name          AS "awayTeam",
      ht.abbreviation  AS "homeAbbrev",
      at.abbreviation  AS "awayAbbrev",
      m.vegas_total    AS "vegasTotal",
      m.home_ml        AS "homeMl",
      m.away_ml        AS "awayMl",
      m.home_spread    AS "homeSpread",
      m.vegas_prob_home AS "homeWinProb",
      m.home_implied   AS "homeImplied",
      m.away_implied   AS "awayImplied",
      m.home_score     AS "homeScore",
      m.away_score     AS "awayScore",
      COALESCE(m.home_sp_name, hsp_id.name, hsp_name.name) AS "homeSpName",
      COALESCE(hsp_id.hand, hsp_name.hand) AS "homeSpHand",
      COALESCE(hsp_id.xfip, hsp_name.xfip) AS "homeSpXfip",
      COALESCE(hsp_id.k_per_9, hsp_name.k_per_9) AS "homeSpKPer9",
      COALESCE(m.away_sp_name, asp_id.name, asp_name.name) AS "awaySpName",
      COALESCE(asp_id.hand, asp_name.hand) AS "awaySpHand",
      COALESCE(asp_id.xfip, asp_name.xfip) AS "awaySpXfip",
      COALESCE(asp_id.k_per_9, asp_name.k_per_9) AS "awaySpKPer9"
    FROM mlb_matchups m
    LEFT JOIN mlb_teams ht ON ht.team_id = m.home_team_id
    LEFT JOIN mlb_teams at ON at.team_id = m.away_team_id
    LEFT JOIN latest_pitcher hsp_id ON hsp_id.player_id = m.home_sp_id
    LEFT JOIN latest_pitcher asp_id ON asp_id.player_id = m.away_sp_id
    LEFT JOIN latest_pitcher_by_name hsp_name ON hsp_name.name_key = LOWER(m.home_sp_name)
    LEFT JOIN latest_pitcher_by_name asp_name ON asp_name.name_key = LOWER(m.away_sp_name)
    WHERE m.game_date = ${targetDate}
    ORDER BY m.vegas_total DESC NULLS LAST
  `);
  return (rows.rows as VegasMatchupRow[]).map((r) => ({
    matchupId: Number(r.matchupId),
    gameDate: String(r.gameDate),
    homeTeam: String(r.homeTeam ?? ""),
    awayTeam: String(r.awayTeam ?? ""),
    homeAbbrev: String(r.homeAbbrev ?? ""),
    awayAbbrev: String(r.awayAbbrev ?? ""),
    vegasTotal: r.vegasTotal != null ? Number(r.vegasTotal) : null,
    homeMl: r.homeMl != null ? Number(r.homeMl) : null,
    awayMl: r.awayMl != null ? Number(r.awayMl) : null,
    homeSpread: r.homeSpread != null ? Number(r.homeSpread) : null,
    homeWinProb: r.homeWinProb != null ? Number(r.homeWinProb) : null,
    homeImplied: r.homeImplied != null ? Number(r.homeImplied) : null,
    awayImplied: r.awayImplied != null ? Number(r.awayImplied) : null,
    homeScore: r.homeScore != null ? Number(r.homeScore) : null,
    awayScore: r.awayScore != null ? Number(r.awayScore) : null,
    homeSpName: r.homeSpName != null ? String(r.homeSpName) : null,
    homeSpHand: r.homeSpHand != null ? String(r.homeSpHand) : null,
    homeSpXfip: r.homeSpXfip != null ? Number(r.homeSpXfip) : null,
    homeSpKPer9: r.homeSpKPer9 != null ? Number(r.homeSpKPer9) : null,
    awaySpName: r.awaySpName != null ? String(r.awaySpName) : null,
    awaySpHand: r.awaySpHand != null ? String(r.awaySpHand) : null,
    awaySpXfip: r.awaySpXfip != null ? Number(r.awaySpXfip) : null,
    awaySpKPer9: r.awaySpKPer9 != null ? Number(r.awaySpKPer9) : null,
  }));
}

export async function getMlbOuHitRate(): Promise<OuHitRateRow[]> {
  const rows = await db.execute(sql`
    SELECT
      CASE
        WHEN m.vegas_total < 7.5  THEN 'Under 7.5'
        WHEN m.vegas_total < 8.0  THEN '7.5'
        WHEN m.vegas_total < 8.5  THEN '8.0'
        WHEN m.vegas_total < 9.0  THEN '8.5'
        WHEN m.vegas_total < 9.5  THEN '9.0'
        WHEN m.vegas_total < 10.0 THEN '9.5'
        WHEN m.vegas_total < 10.5 THEN '10.0'
        ELSE '10.5+'
      END                                                                   AS "totalTier",
      MIN(m.vegas_total)                                                    AS "tierMin",
      COUNT(*)                                                              AS "n",
      COUNT(*) FILTER (WHERE m.home_score + m.away_score > m.vegas_total)  AS "overCount",
      COUNT(*) FILTER (WHERE m.home_score + m.away_score < m.vegas_total)  AS "underCount",
      COUNT(*) FILTER (WHERE m.home_score + m.away_score = m.vegas_total)  AS "pushCount",
      AVG(m.vegas_total)                                                    AS "avgTotal",
      AVG((m.home_score + m.away_score)::DOUBLE PRECISION)                 AS "avgActual",
      AVG(ABS((m.home_score + m.away_score)::DOUBLE PRECISION - m.vegas_total)) AS "gameTotalMae"
    FROM mlb_matchups m
    WHERE m.vegas_total IS NOT NULL
      AND m.home_score IS NOT NULL
      AND m.away_score IS NOT NULL
    GROUP BY 1
    ORDER BY MIN(m.vegas_total) ASC NULLS LAST
  `);
  return (rows.rows as OuHitRateRow[]).map((r) => {
    const n = Number(r.n);
    const overCount = Number(r.overCount);
    return {
      totalTier: String(r.totalTier),
      tierMin: r.tierMin != null ? Number(r.tierMin) : null,
      n,
      overCount,
      underCount: Number(r.underCount),
      pushCount: Number(r.pushCount),
      overRate: n > 0 ? overCount / n : null,
      avgTotal: r.avgTotal != null ? Number(r.avgTotal) : null,
      avgActual: r.avgActual != null ? Number(r.avgActual) : null,
      gameTotalMae: r.gameTotalMae != null ? Number(r.gameTotalMae) : null,
    };
  });
}

export async function getMlbTeamTotalAccuracy(): Promise<TeamTotalAccuracyRow[]> {
  const rows = await db.execute(sql`
    WITH team_games AS (
      SELECT
        m.home_team_id                             AS team_id,
        m.home_implied                             AS implied_total,
        m.home_score::DOUBLE PRECISION             AS actual_score
      FROM mlb_matchups m
      WHERE m.home_implied IS NOT NULL AND m.home_score IS NOT NULL
      UNION ALL
      SELECT
        m.away_team_id                             AS team_id,
        m.away_implied                             AS implied_total,
        m.away_score::DOUBLE PRECISION             AS actual_score
      FROM mlb_matchups m
      WHERE m.away_implied IS NOT NULL AND m.away_score IS NOT NULL
    )
    SELECT
      t.abbreviation                               AS "teamAbbrev",
      t.name                                       AS "teamName",
      COUNT(*)                                     AS "n",
      AVG(tg.implied_total)                        AS "avgImplied",
      AVG(tg.actual_score)                         AS "avgActual",
      AVG(ABS(tg.implied_total - tg.actual_score)) AS "mae",
      AVG(tg.implied_total - tg.actual_score)      AS "bias"
    FROM team_games tg
    JOIN mlb_teams t ON t.team_id = tg.team_id
    GROUP BY t.abbreviation, t.name
    HAVING COUNT(*) >= 3
    ORDER BY AVG(ABS(tg.implied_total - tg.actual_score)) DESC NULLS LAST
  `);
  return (rows.rows as TeamTotalAccuracyRow[]).map((r) => ({
    teamAbbrev: String(r.teamAbbrev),
    teamName: String(r.teamName),
    n: Number(r.n),
    avgImplied: r.avgImplied != null ? Number(r.avgImplied) : null,
    avgActual: r.avgActual != null ? Number(r.avgActual) : null,
    mae: r.mae != null ? Number(r.mae) : null,
    bias: r.bias != null ? Number(r.bias) : null,
  }));
}

export async function getMlbRunLineCoverage(): Promise<SpreadCoverageRow[]> {
  const rows = await db.execute(sql`
    SELECT
      CASE
        WHEN ABS(m.home_spread) < 1.0 THEN 'Pick'
        WHEN ABS(m.home_spread) < 2.0 THEN '±1.5 (Run Line)'
        ELSE '2.0+'
      END                                                           AS "spreadTier",
      MIN(ABS(m.home_spread))                                       AS "tierMin",
      COUNT(*)                                                       AS "n",
      COUNT(*) FILTER (WHERE
        (m.home_spread < 0 AND (m.home_score - m.away_score) > ABS(m.home_spread))
        OR
        (m.home_spread > 0 AND (m.away_score - m.home_score) > ABS(m.home_spread))
      )                                                              AS "coverCount",
      AVG(ABS(m.home_spread))                                       AS "avgSpread",
      AVG(ABS(m.home_score - m.away_score)::DOUBLE PRECISION)      AS "avgMargin"
    FROM mlb_matchups m
    WHERE m.home_spread IS NOT NULL
      AND m.home_score  IS NOT NULL
      AND m.away_score  IS NOT NULL
      AND m.home_spread <> 0
    GROUP BY 1
    ORDER BY MIN(ABS(m.home_spread)) ASC NULLS LAST
  `);
  return (rows.rows as SpreadCoverageRow[]).map((r) => {
    const n = Number(r.n);
    const coverCount = Number(r.coverCount);
    return {
      spreadTier: String(r.spreadTier),
      tierMin: r.tierMin != null ? Number(r.tierMin) : null,
      n,
      coverCount,
      coverRate: n > 0 ? coverCount / n : null,
      avgSpread: r.avgSpread != null ? Number(r.avgSpread) : null,
      avgMargin: r.avgMargin != null ? Number(r.avgMargin) : null,
    };
  });
}

// ── Vegas Summary Stats ──────────────────────────────────────

export type MlbVegasCoverageStatus = {
  availableStartDate: string | null;
  availableEndDate: string | null;
  historicalEndDate: string | null;
  dateCount: number;
  gameCount: number;
  latestScoreCompleteDate: string | null;
  latestOddsCompleteDate: string | null;
  firstMissingScoreDate: string | null;
  firstMissingOddsDate: string | null;
  firstUnattemptedOddsDate: string | null;
  oddsBackfillAttemptedThroughDate: string | null;
  recommendedBackfillStart: string | null;
  recommendedBackfillEnd: string | null;
  missingScoreDates: string[];
  missingOddsDates: string[];
  unattemptedMissingOddsDates: string[];
  providerPartialOddsDates: string[];
  yesterdayDate: string;
  yesterdayHadGames: boolean;
  yesterdayScoresComplete: boolean | null;
  yesterdayOddsComplete: boolean | null;
};

function formatEtDate(date: Date): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

export async function getMlbVegasCoverageStatus(): Promise<MlbVegasCoverageStatus | null> {
  const todayEt = formatEtDate(new Date());
  const seasonStart = `${todayEt.slice(0, 4)}-01-01`;
  const yesterdayEt = formatEtDate(new Date(new Date(`${todayEt}T12:00:00Z`).getTime() - 24 * 60 * 60 * 1000));

  const [summaryResult, missingResult] = await Promise.all([
    db.execute<{
      availableStartDate: string | null;
      availableEndDate: string | null;
      historicalEndDate: string | null;
      dateCount: number;
      gameCount: number;
      latestScoreCompleteDate: string | null;
      latestOddsCompleteDate: string | null;
      firstMissingScoreDate: string | null;
      firstMissingOddsDate: string | null;
      firstUnattemptedOddsDate: string | null;
      oddsBackfillAttemptedThroughDate: string | null;
      yesterdayGames: number;
      yesterdayScoresComplete: number | null;
      yesterdayOddsComplete: number | null;
    }>(sql`
      WITH daily AS (
        SELECT
          m.game_date::text AS game_date,
          COUNT(*)::int AS games,
          COUNT(*) FILTER (WHERE m.home_score IS NULL OR m.away_score IS NULL)::int AS missing_scores,
          COUNT(*) FILTER (WHERE m.vegas_total IS NULL OR m.home_ml IS NULL OR m.away_ml IS NULL)::int AS missing_odds
        FROM mlb_matchups m
        WHERE m.game_date >= ${seasonStart}
        GROUP BY m.game_date
      ),
      odds_history AS (
        SELECT
          h.game_date::text AS game_date,
          COUNT(*) FILTER (WHERE h.capture_key LIKE '%_backfill')::int AS backfill_rows
        FROM game_odds_history h
        WHERE h.sport = 'mlb'
          AND h.game_date >= ${seasonStart}
        GROUP BY h.game_date
      )
      SELECT
        MIN(daily.game_date) AS "availableStartDate",
        MAX(daily.game_date) AS "availableEndDate",
        MAX(daily.game_date) FILTER (WHERE daily.game_date <= ${yesterdayEt}) AS "historicalEndDate",
        COUNT(*)::int AS "dateCount",
        COALESCE(SUM(daily.games), 0)::int AS "gameCount",
        MAX(daily.game_date) FILTER (WHERE daily.game_date <= ${yesterdayEt} AND daily.missing_scores = 0) AS "latestScoreCompleteDate",
        MAX(daily.game_date) FILTER (WHERE daily.game_date <= ${yesterdayEt} AND daily.missing_odds = 0) AS "latestOddsCompleteDate",
        MIN(daily.game_date) FILTER (WHERE daily.game_date <= ${yesterdayEt} AND daily.missing_scores > 0) AS "firstMissingScoreDate",
        MIN(daily.game_date) FILTER (WHERE daily.game_date <= ${yesterdayEt} AND daily.missing_odds > 0) AS "firstMissingOddsDate",
        MIN(daily.game_date) FILTER (
          WHERE daily.game_date <= ${yesterdayEt}
            AND daily.missing_odds > 0
            AND COALESCE(odds_history.backfill_rows, 0) = 0
        ) AS "firstUnattemptedOddsDate",
        MAX(daily.game_date) FILTER (
          WHERE daily.game_date <= ${yesterdayEt}
            AND COALESCE(odds_history.backfill_rows, 0) > 0
        ) AS "oddsBackfillAttemptedThroughDate",
        COALESCE(SUM(CASE WHEN daily.game_date = ${yesterdayEt} THEN daily.games ELSE 0 END), 0)::int AS "yesterdayGames",
        MAX(
          CASE
            WHEN daily.game_date = ${yesterdayEt} AND daily.missing_scores = 0 THEN 1
            WHEN daily.game_date = ${yesterdayEt} THEN 0
            ELSE NULL
          END
        ) AS "yesterdayScoresComplete",
        MAX(
          CASE
            WHEN daily.game_date = ${yesterdayEt} AND daily.missing_odds = 0 THEN 1
            WHEN daily.game_date = ${yesterdayEt} THEN 0
            ELSE NULL
          END
        ) AS "yesterdayOddsComplete"
      FROM daily
      LEFT JOIN odds_history ON odds_history.game_date = daily.game_date
    `),
    db.execute<{
      missingScoreDates: string[] | null;
      missingOddsDates: string[] | null;
      unattemptedMissingOddsDates: string[] | null;
      providerPartialOddsDates: string[] | null;
    }>(sql`
      WITH daily AS (
        SELECT
          m.game_date::text AS game_date,
          COUNT(*) FILTER (WHERE m.home_score IS NULL OR m.away_score IS NULL)::int AS missing_scores,
          COUNT(*) FILTER (WHERE m.vegas_total IS NULL OR m.home_ml IS NULL OR m.away_ml IS NULL)::int AS missing_odds
        FROM mlb_matchups m
        WHERE m.game_date >= ${seasonStart}
        GROUP BY m.game_date
      ),
      odds_history AS (
        SELECT
          h.game_date::text AS game_date,
          COUNT(*) FILTER (WHERE h.capture_key LIKE '%_backfill')::int AS backfill_rows
        FROM game_odds_history h
        WHERE h.sport = 'mlb'
          AND h.game_date >= ${seasonStart}
        GROUP BY h.game_date
      )
      SELECT
        ARRAY(
          SELECT d.game_date
          FROM daily d
          WHERE d.game_date <= ${yesterdayEt}
            AND d.missing_scores > 0
          ORDER BY d.game_date ASC
          LIMIT 12
        ) AS "missingScoreDates",
        ARRAY(
          SELECT d.game_date
          FROM daily d
          WHERE d.game_date <= ${yesterdayEt}
            AND d.missing_odds > 0
          ORDER BY d.game_date ASC
          LIMIT 12
        ) AS "missingOddsDates",
        ARRAY(
          SELECT d.game_date
          FROM daily d
          LEFT JOIN odds_history h ON h.game_date = d.game_date
          WHERE d.game_date <= ${yesterdayEt}
            AND d.missing_odds > 0
            AND COALESCE(h.backfill_rows, 0) = 0
          ORDER BY d.game_date ASC
          LIMIT 12
        ) AS "unattemptedMissingOddsDates",
        ARRAY(
          SELECT d.game_date
          FROM daily d
          LEFT JOIN odds_history h ON h.game_date = d.game_date
          WHERE d.game_date <= ${yesterdayEt}
            AND d.missing_odds > 0
            AND COALESCE(h.backfill_rows, 0) > 0
          ORDER BY d.game_date ASC
          LIMIT 12
        ) AS "providerPartialOddsDates"
    `),
  ]);

  const summary = summaryResult.rows[0];
  const missing = missingResult.rows[0];
  if (!summary || Number(summary.dateCount ?? 0) === 0) return null;

  const firstMissingDates = [summary.firstMissingScoreDate, summary.firstUnattemptedOddsDate]
    .filter((value): value is string => Boolean(value))
    .sort();

  return {
    availableStartDate: summary.availableStartDate ?? null,
    availableEndDate: summary.availableEndDate ?? null,
    historicalEndDate: summary.historicalEndDate ?? null,
    dateCount: Number(summary.dateCount ?? 0),
    gameCount: Number(summary.gameCount ?? 0),
    latestScoreCompleteDate: summary.latestScoreCompleteDate ?? null,
    latestOddsCompleteDate: summary.latestOddsCompleteDate ?? null,
    firstMissingScoreDate: summary.firstMissingScoreDate ?? null,
    firstMissingOddsDate: summary.firstMissingOddsDate ?? null,
    firstUnattemptedOddsDate: summary.firstUnattemptedOddsDate ?? null,
    oddsBackfillAttemptedThroughDate: summary.oddsBackfillAttemptedThroughDate ?? null,
    recommendedBackfillStart: firstMissingDates[0] ?? null,
    recommendedBackfillEnd: firstMissingDates.length > 0 ? summary.historicalEndDate ?? null : null,
    missingScoreDates: missing?.missingScoreDates ?? [],
    missingOddsDates: missing?.missingOddsDates ?? [],
    unattemptedMissingOddsDates: missing?.unattemptedMissingOddsDates ?? [],
    providerPartialOddsDates: missing?.providerPartialOddsDates ?? [],
    yesterdayDate: yesterdayEt,
    yesterdayHadGames: Number(summary.yesterdayGames ?? 0) > 0,
    yesterdayScoresComplete: summary.yesterdayScoresComplete == null ? null : Number(summary.yesterdayScoresComplete) === 1,
    yesterdayOddsComplete: summary.yesterdayOddsComplete == null ? null : Number(summary.yesterdayOddsComplete) === 1,
  };
}

export type VegasSummaryStatsRow = {
  n: number;
  gameTotalMae: number | null;
  gameTotalBias: number | null;
  ouOverRate: number | null;
  teamTotalMae: number | null;
  teamTotalBias: number | null;
};

export async function getVegasSummaryStats(sport: Sport = "nba"): Promise<VegasSummaryStatsRow> {
  const empty: VegasSummaryStatsRow = {
    n: 0,
    gameTotalMae: null,
    gameTotalBias: null,
    ouOverRate: null,
    teamTotalMae: null,
    teamTotalBias: null,
  };

  if (sport === "mlb") {
    const [gameRows, teamRows] = await Promise.all([
      db.execute(sql`
        SELECT
          COUNT(*)                                                                          AS "n",
          AVG(ABS((m.home_score + m.away_score)::DOUBLE PRECISION - m.vegas_total))        AS "gameTotalMae",
          AVG((m.home_score + m.away_score)::DOUBLE PRECISION - m.vegas_total)             AS "gameTotalBias",
          AVG(CASE WHEN m.home_score + m.away_score > m.vegas_total THEN 1.0 ELSE 0.0 END) AS "ouOverRate"
        FROM mlb_matchups m
        WHERE m.vegas_total IS NOT NULL AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
      `),
      db.execute(sql`
        WITH tg AS (
          SELECT m.home_implied AS implied, m.home_score::DOUBLE PRECISION AS actual
          FROM mlb_matchups m WHERE m.home_implied IS NOT NULL AND m.home_score IS NOT NULL
          UNION ALL
          SELECT m.away_implied, m.away_score::DOUBLE PRECISION
          FROM mlb_matchups m WHERE m.away_implied IS NOT NULL AND m.away_score IS NOT NULL
        )
        SELECT
          AVG(ABS(tg.implied - tg.actual)) AS "teamTotalMae",
          AVG(tg.implied - tg.actual)      AS "teamTotalBias"
        FROM tg
      `),
    ]);
    const g = gameRows.rows[0] as Record<string, unknown>;
    const t = teamRows.rows[0] as Record<string, unknown>;
    if (!g) return empty;
    return {
      n: g.n != null ? Number(g.n) : 0,
      gameTotalMae: g.gameTotalMae != null ? Number(g.gameTotalMae) : null,
      gameTotalBias: g.gameTotalBias != null ? Number(g.gameTotalBias) : null,
      ouOverRate: g.ouOverRate != null ? Number(g.ouOverRate) : null,
      teamTotalMae: t?.teamTotalMae != null ? Number(t.teamTotalMae) : null,
      teamTotalBias: t?.teamTotalBias != null ? Number(t.teamTotalBias) : null,
    };
  } else {
    const [gameRows, teamRows] = await Promise.all([
      db.execute(sql`
        SELECT
          COUNT(*)                                                                           AS "n",
          AVG(ABS((nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total))      AS "gameTotalMae",
          AVG((nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total)           AS "gameTotalBias",
          AVG(CASE WHEN nm.home_score + nm.away_score > nm.vegas_total THEN 1.0 ELSE 0.0 END) AS "ouOverRate"
        FROM nba_matchups nm
        WHERE nm.vegas_total IS NOT NULL AND nm.home_score IS NOT NULL AND nm.away_score IS NOT NULL
      `),
      db.execute(sql`
        WITH tg AS (
          SELECT nm.home_implied AS implied, nm.home_score::DOUBLE PRECISION AS actual
          FROM nba_matchups nm WHERE nm.home_implied IS NOT NULL AND nm.home_score IS NOT NULL
          UNION ALL
          SELECT nm.away_implied, nm.away_score::DOUBLE PRECISION
          FROM nba_matchups nm WHERE nm.away_implied IS NOT NULL AND nm.away_score IS NOT NULL
        )
        SELECT
          AVG(ABS(tg.implied - tg.actual)) AS "teamTotalMae",
          AVG(tg.implied - tg.actual)      AS "teamTotalBias"
        FROM tg
      `),
    ]);
    const g = gameRows.rows[0] as Record<string, unknown>;
    const t = teamRows.rows[0] as Record<string, unknown>;
    if (!g) return empty;
    return {
      n: g.n != null ? Number(g.n) : 0,
      gameTotalMae: g.gameTotalMae != null ? Number(g.gameTotalMae) : null,
      gameTotalBias: g.gameTotalBias != null ? Number(g.gameTotalBias) : null,
      ouOverRate: g.ouOverRate != null ? Number(g.ouOverRate) : null,
      teamTotalMae: t?.teamTotalMae != null ? Number(t.teamTotalMae) : null,
      teamTotalBias: t?.teamTotalBias != null ? Number(t.teamTotalBias) : null,
    };
  }
}

// ── Team Vegas Insights ──────────────────────────────────────

export type TeamVegasInsightRow = {
  teamAbbrev: string;
  teamName: string;
  n: number;            // games with scores
  nImplied: number;     // games with implied totals
  avgImplied: number | null;
  avgActual: number | null;
  mae: number | null;
  bias: number | null;  // positive = Vegas underestimates (team scores more than expected)
  overImpliedRate: number | null;  // how often team beats their implied total
  avgGameTotal: number | null;
  gameOverRate: number | null;     // how often games go over when this team plays
  atsN: number;
  atsCoverRate: number | null;
};

export async function getTeamVegasInsights(sport: Sport = "nba"): Promise<TeamVegasInsightRow[]> {
  if (sport === "mlb") {
    const rows = await db.execute(sql`
      WITH team_appearances AS (
        SELECT
          m.home_team_id                                                           AS team_id,
          m.home_implied                                                           AS implied,
          m.home_score::DOUBLE PRECISION                                           AS actual,
          m.vegas_total,
          (m.home_score + m.away_score)                                            AS actual_total,
          CASE WHEN m.home_spread IS NOT NULL
            THEN (m.home_score - m.away_score)::DOUBLE PRECISION > -m.home_spread
          END                                                                      AS covered
        FROM mlb_matchups m
        WHERE m.home_score IS NOT NULL AND m.away_score IS NOT NULL
        UNION ALL
        SELECT
          m.away_team_id,
          m.away_implied,
          m.away_score::DOUBLE PRECISION,
          m.vegas_total,
          (m.home_score + m.away_score),
          CASE WHEN m.home_spread IS NOT NULL
            THEN (m.away_score - m.home_score)::DOUBLE PRECISION > m.home_spread
          END
        FROM mlb_matchups m
        WHERE m.home_score IS NOT NULL AND m.away_score IS NOT NULL
      )
      SELECT
        t.abbreviation                                                             AS "teamAbbrev",
        t.name                                                                     AS "teamName",
        COUNT(*)::int                                                              AS "n",
        COUNT(*) FILTER (WHERE ta.implied IS NOT NULL)::int                        AS "nImplied",
        AVG(ta.implied)                                                            AS "avgImplied",
        AVG(ta.actual)                                                             AS "avgActual",
        AVG(ABS(ta.implied - ta.actual)) FILTER (WHERE ta.implied IS NOT NULL)    AS "mae",
        AVG(ta.implied - ta.actual)      FILTER (WHERE ta.implied IS NOT NULL)    AS "bias",
        AVG(CASE WHEN ta.actual > ta.implied THEN 1.0 ELSE 0.0 END)
          FILTER (WHERE ta.implied IS NOT NULL)                                    AS "overImpliedRate",
        AVG(ta.vegas_total)              FILTER (WHERE ta.vegas_total IS NOT NULL) AS "avgGameTotal",
        AVG(CASE WHEN ta.actual_total > ta.vegas_total THEN 1.0 ELSE 0.0 END)
          FILTER (WHERE ta.vegas_total IS NOT NULL)                                AS "gameOverRate",
        COUNT(*) FILTER (WHERE ta.covered IS NOT NULL)::int                        AS "atsN",
        AVG(CASE WHEN ta.covered THEN 1.0 ELSE 0.0 END)
          FILTER (WHERE ta.covered IS NOT NULL)                                    AS "atsCoverRate"
      FROM team_appearances ta
      JOIN mlb_teams t ON t.team_id = ta.team_id
      GROUP BY t.abbreviation, t.name
      HAVING COUNT(*) >= 5
      ORDER BY AVG(ta.implied - ta.actual) FILTER (WHERE ta.implied IS NOT NULL) DESC NULLS LAST
    `);
    return (rows.rows as TeamVegasInsightRow[]).map((r) => ({
      teamAbbrev:      String(r.teamAbbrev),
      teamName:        String(r.teamName),
      n:               Number(r.n),
      nImplied:        Number(r.nImplied),
      avgImplied:      r.avgImplied      != null ? Number(r.avgImplied)      : null,
      avgActual:       r.avgActual       != null ? Number(r.avgActual)       : null,
      mae:             r.mae             != null ? Number(r.mae)             : null,
      bias:            r.bias            != null ? Number(r.bias)            : null,
      overImpliedRate: r.overImpliedRate != null ? Number(r.overImpliedRate) : null,
      avgGameTotal:    r.avgGameTotal    != null ? Number(r.avgGameTotal)    : null,
      gameOverRate:    r.gameOverRate    != null ? Number(r.gameOverRate)    : null,
      atsN:            Number(r.atsN),
      atsCoverRate:    r.atsCoverRate    != null ? Number(r.atsCoverRate)    : null,
    }));
  } else {
    const rows = await db.execute(sql`
      WITH team_appearances AS (
        SELECT
          nm.home_team_id                                                          AS team_id,
          nm.home_implied                                                          AS implied,
          nm.home_score::DOUBLE PRECISION                                          AS actual,
          nm.vegas_total,
          (nm.home_score + nm.away_score)                                          AS actual_total,
          CASE WHEN nm.home_spread IS NOT NULL
            THEN (nm.home_score - nm.away_score)::DOUBLE PRECISION > -nm.home_spread
          END                                                                      AS covered
        FROM nba_matchups nm
        WHERE nm.home_score IS NOT NULL AND nm.away_score IS NOT NULL
        UNION ALL
        SELECT
          nm.away_team_id,
          nm.away_implied,
          nm.away_score::DOUBLE PRECISION,
          nm.vegas_total,
          (nm.home_score + nm.away_score),
          CASE WHEN nm.home_spread IS NOT NULL
            THEN (nm.away_score - nm.home_score)::DOUBLE PRECISION > nm.home_spread
          END
        FROM nba_matchups nm
        WHERE nm.home_score IS NOT NULL AND nm.away_score IS NOT NULL
      )
      SELECT
        t.abbreviation                                                             AS "teamAbbrev",
        t.name                                                                     AS "teamName",
        COUNT(*)::int                                                              AS "n",
        COUNT(*) FILTER (WHERE ta.implied IS NOT NULL)::int                        AS "nImplied",
        AVG(ta.implied)                                                            AS "avgImplied",
        AVG(ta.actual)                                                             AS "avgActual",
        AVG(ABS(ta.implied - ta.actual)) FILTER (WHERE ta.implied IS NOT NULL)    AS "mae",
        AVG(ta.implied - ta.actual)      FILTER (WHERE ta.implied IS NOT NULL)    AS "bias",
        AVG(CASE WHEN ta.actual > ta.implied THEN 1.0 ELSE 0.0 END)
          FILTER (WHERE ta.implied IS NOT NULL)                                    AS "overImpliedRate",
        AVG(ta.vegas_total)              FILTER (WHERE ta.vegas_total IS NOT NULL) AS "avgGameTotal",
        AVG(CASE WHEN ta.actual_total > ta.vegas_total THEN 1.0 ELSE 0.0 END)
          FILTER (WHERE ta.vegas_total IS NOT NULL)                                AS "gameOverRate",
        COUNT(*) FILTER (WHERE ta.covered IS NOT NULL)::int                        AS "atsN",
        AVG(CASE WHEN ta.covered THEN 1.0 ELSE 0.0 END)
          FILTER (WHERE ta.covered IS NOT NULL)                                    AS "atsCoverRate"
      FROM team_appearances ta
      JOIN teams t ON t.team_id = ta.team_id
      GROUP BY t.abbreviation, t.name
      HAVING COUNT(*) >= 5
      ORDER BY AVG(ta.implied - ta.actual) FILTER (WHERE ta.implied IS NOT NULL) DESC NULLS LAST
    `);
    return (rows.rows as TeamVegasInsightRow[]).map((r) => ({
      teamAbbrev:      String(r.teamAbbrev),
      teamName:        String(r.teamName),
      n:               Number(r.n),
      nImplied:        Number(r.nImplied),
      avgImplied:      r.avgImplied      != null ? Number(r.avgImplied)      : null,
      avgActual:       r.avgActual       != null ? Number(r.avgActual)       : null,
      mae:             r.mae             != null ? Number(r.mae)             : null,
      bias:            r.bias            != null ? Number(r.bias)            : null,
      overImpliedRate: r.overImpliedRate != null ? Number(r.overImpliedRate) : null,
      avgGameTotal:    r.avgGameTotal    != null ? Number(r.avgGameTotal)    : null,
      gameOverRate:    r.gameOverRate    != null ? Number(r.gameOverRate)    : null,
      atsN:            Number(r.atsN),
      atsCoverRate:    r.atsCoverRate    != null ? Number(r.atsCoverRate)    : null,
    }));
  }
}

// ── Biggest Misses ───────────────────────────────────────────

export type BiggestMissRow = {
  gameDate: string;
  homeAbbrev: string;
  awayAbbrev: string;
  homeName: string;
  awayName: string;
  vegasTotal: number;
  actualTotal: number;
  miss: number;         // actual − vegas (positive = over, negative = under)
  absMiss: number;
  homeSpread: number | null;
  vegasProbHome: number | null;
};

export async function getBiggestMisses(sport: Sport = "nba", limit = 20): Promise<BiggestMissRow[]> {
  if (sport === "mlb") {
    const rows = await db.execute(sql`
      SELECT
        m.game_date::text                                                              AS "gameDate",
        ht.abbreviation                                                                AS "homeAbbrev",
        at.abbreviation                                                                AS "awayAbbrev",
        ht.name                                                                        AS "homeName",
        at.name                                                                        AS "awayName",
        m.vegas_total                                                                  AS "vegasTotal",
        (m.home_score + m.away_score)                                                  AS "actualTotal",
        (m.home_score + m.away_score)::DOUBLE PRECISION - m.vegas_total               AS "miss",
        ABS((m.home_score + m.away_score)::DOUBLE PRECISION - m.vegas_total)          AS "absMiss",
        m.home_spread                                                                  AS "homeSpread",
        m.vegas_prob_home                                                              AS "vegasProbHome"
      FROM mlb_matchups m
      JOIN mlb_teams ht ON ht.team_id = m.home_team_id
      JOIN mlb_teams at ON at.team_id = m.away_team_id
      WHERE m.vegas_total IS NOT NULL
        AND m.home_score  IS NOT NULL
        AND m.away_score  IS NOT NULL
      ORDER BY ABS((m.home_score + m.away_score)::DOUBLE PRECISION - m.vegas_total) DESC
      LIMIT ${limit}
    `);
    return (rows.rows as BiggestMissRow[]).map((r) => ({
      gameDate:      String(r.gameDate),
      homeAbbrev:    String(r.homeAbbrev),
      awayAbbrev:    String(r.awayAbbrev),
      homeName:      String(r.homeName),
      awayName:      String(r.awayName),
      vegasTotal:    Number(r.vegasTotal),
      actualTotal:   Number(r.actualTotal),
      miss:          Number(r.miss),
      absMiss:       Number(r.absMiss),
      homeSpread:    r.homeSpread    != null ? Number(r.homeSpread)    : null,
      vegasProbHome: r.vegasProbHome != null ? Number(r.vegasProbHome) : null,
    }));
  } else {
    const rows = await db.execute(sql`
      SELECT
        nm.game_date::text                                                             AS "gameDate",
        ht.abbreviation                                                                AS "homeAbbrev",
        at.abbreviation                                                                AS "awayAbbrev",
        ht.name                                                                        AS "homeName",
        at.name                                                                        AS "awayName",
        nm.vegas_total                                                                 AS "vegasTotal",
        (nm.home_score + nm.away_score)                                                AS "actualTotal",
        (nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total            AS "miss",
        ABS((nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total)       AS "absMiss",
        nm.home_spread                                                                 AS "homeSpread",
        nm.vegas_prob_home                                                             AS "vegasProbHome"
      FROM nba_matchups nm
      JOIN teams ht ON ht.team_id = nm.home_team_id
      JOIN teams at ON at.team_id = nm.away_team_id
      WHERE nm.vegas_total IS NOT NULL
        AND nm.home_score  IS NOT NULL
        AND nm.away_score  IS NOT NULL
      ORDER BY ABS((nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total) DESC
      LIMIT ${limit}
    `);
    return (rows.rows as BiggestMissRow[]).map((r) => ({
      gameDate:      String(r.gameDate),
      homeAbbrev:    String(r.homeAbbrev),
      awayAbbrev:    String(r.awayAbbrev),
      homeName:      String(r.homeName),
      awayName:      String(r.awayName),
      vegasTotal:    Number(r.vegasTotal),
      actualTotal:   Number(r.actualTotal),
      miss:          Number(r.miss),
      absMiss:       Number(r.absMiss),
      homeSpread:    r.homeSpread    != null ? Number(r.homeSpread)    : null,
      vegasProbHome: r.vegasProbHome != null ? Number(r.vegasProbHome) : null,
    }));
  }
}

// ── Stat-level accuracy ──────────────────────────────────────────────────────

export type StatLevelAccuracyRow = {
  stat: string;          // "pts" | "reb" | "ast"
  propCol: string;       // column used as projection ("prop_pts", etc.)
  n: number;             // games where both prop and actual are present
  mae: number | null;
  bias: number | null;   // positive = we projected more than actual
  nFormula: number;      // games where no prop — formula only (actual available but no prop)
};

export async function getStatLevelAccuracy(sport: Sport = "nba"): Promise<StatLevelAccuracyRow[]> {
  await ensureAnalyticsColumns();
  const rows = await db.execute(sql`
    SELECT
      'pts'  AS stat,
      COUNT(*) FILTER (WHERE prop_pts IS NOT NULL AND actual_pts IS NOT NULL)           AS n,
      AVG(ABS(prop_pts - actual_pts))
        FILTER (WHERE prop_pts IS NOT NULL AND actual_pts IS NOT NULL)                  AS mae,
      AVG(prop_pts - actual_pts)
        FILTER (WHERE prop_pts IS NOT NULL AND actual_pts IS NOT NULL)                  AS bias,
      COUNT(*) FILTER (WHERE prop_pts IS NULL AND actual_pts IS NOT NULL)               AS n_formula
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
    UNION ALL
    SELECT
      'reb'  AS stat,
      COUNT(*) FILTER (WHERE prop_reb IS NOT NULL AND actual_reb IS NOT NULL)           AS n,
      AVG(ABS(prop_reb - actual_reb))
        FILTER (WHERE prop_reb IS NOT NULL AND actual_reb IS NOT NULL)                  AS mae,
      AVG(prop_reb - actual_reb)
        FILTER (WHERE prop_reb IS NOT NULL AND actual_reb IS NOT NULL)                  AS bias,
      COUNT(*) FILTER (WHERE prop_reb IS NULL AND actual_reb IS NOT NULL)               AS n_formula
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
    UNION ALL
    SELECT
      'ast'  AS stat,
      COUNT(*) FILTER (WHERE prop_ast IS NOT NULL AND actual_ast IS NOT NULL)           AS n,
      AVG(ABS(prop_ast - actual_ast))
        FILTER (WHERE prop_ast IS NOT NULL AND actual_ast IS NOT NULL)                  AS mae,
      AVG(prop_ast - actual_ast)
        FILTER (WHERE prop_ast IS NOT NULL AND actual_ast IS NOT NULL)                  AS bias,
      COUNT(*) FILTER (WHERE prop_ast IS NULL AND actual_ast IS NOT NULL)               AS n_formula
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
  `);
  return (rows.rows as StatLevelAccuracyRow[]).map((r) => ({
    stat:     String(r.stat),
    propCol:  `prop_${r.stat}`,
    n:        Number(r.n),
    mae:      r.mae  != null ? Number(r.mae)  : null,
    bias:     r.bias != null ? Number(r.bias) : null,
    nFormula: Number((r as Record<string, unknown>)["n_formula"]),
  }));
}

// ── Game-total model accuracy over time ──────────────────────────────────────

export type GameTotalModelRow = {
  gameDate: string;
  homeAbbrev: string;
  awayAbbrev: string;
  vegasTotal: number;
  ourPred: number;
  actualTotal: number | null;
  vegasMiss: number | null;   // actual - vegas (positive = over)
  ourMiss: number | null;     // actual - our pred
};

export async function getGameTotalModelAccuracy(): Promise<GameTotalModelRow[]> {
  await ensureAnalyticsColumns();
  const rows = await db.execute(sql`
    SELECT
      nm.game_date::text                                          AS "gameDate",
      ht.abbreviation                                             AS "homeAbbrev",
      at.abbreviation                                             AS "awayAbbrev",
      nm.vegas_total                                              AS "vegasTotal",
      nm.our_game_total_pred                                      AS "ourPred",
      CASE WHEN nm.home_score IS NOT NULL AND nm.away_score IS NOT NULL
           THEN (nm.home_score + nm.away_score)::DOUBLE PRECISION
           ELSE NULL END                                          AS "actualTotal",
      CASE WHEN nm.home_score IS NOT NULL AND nm.away_score IS NOT NULL
           THEN (nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.vegas_total
           ELSE NULL END                                          AS "vegasMiss",
      CASE WHEN nm.home_score IS NOT NULL AND nm.away_score IS NOT NULL
           THEN (nm.home_score + nm.away_score)::DOUBLE PRECISION - nm.our_game_total_pred
           ELSE NULL END                                          AS "ourMiss"
    FROM nba_matchups nm
    JOIN teams ht ON ht.team_id = nm.home_team_id
    JOIN teams at ON at.team_id = nm.away_team_id
    WHERE nm.our_game_total_pred IS NOT NULL
    ORDER BY nm.game_date DESC
  `);
  return (rows.rows as GameTotalModelRow[]).map((r) => ({
    gameDate:    String(r.gameDate),
    homeAbbrev:  String(r.homeAbbrev),
    awayAbbrev:  String(r.awayAbbrev),
    vegasTotal:  Number(r.vegasTotal),
    ourPred:     Number(r.ourPred),
    actualTotal: r.actualTotal != null ? Number(r.actualTotal) : null,
    vegasMiss:   r.vegasMiss   != null ? Number(r.vegasMiss)   : null,
    ourMiss:     r.ourMiss     != null ? Number(r.ourMiss)     : null,
  }));
}

// MLB pitcher lineup report ─────────────────────────────────────────────────

const MLB_PITCHER_SMASH_THRESHOLD = 20;
const MLB_PITCHER_ELITE_THRESHOLD = 25;
const MLB_PITCHER_UNDEROWNED_THRESHOLD = 5;
const MLB_PITCHER_MIN_SAMPLE = 8;
const MLB_PITCHER_PIVOT_MAX_OWN = 12;

type MlbPitcherHistoricalRow = {
  playerId: number | null;
  slateId: number;
  slateDate: string;
  name: string;
  teamAbbrev: string;
  salary: number | null;
  projection: number | null;
  linestarProj: number | null;
  ourProj: number | null;
  projectedOwnPct: number | null;
  ourOwnPct: number | null;
  actualFpts: number | null;
  actualOwnPct: number | null;
  oppImplied: number | null;
  teamMl: number | null;
  isHome: boolean | null;
  projectionBucket: string;
  valueBucket: string;
  projectedOwnBucket: string;
  oppImpliedBucket: string;
  moneylineBucket: string;
  salaryBucket: string;
  projectedValueX: number | null;
};

type MlbPitcherTargetSlateRow = {
  id: number;
  slateDate: string;
  contestType: string;
  contestFormat: string;
  activeSpCount: number;
  pendingActualRows: number;
};

type MlbPitcherBucketKey =
  | "projectionBucket"
  | "valueBucket"
  | "projectedOwnBucket"
  | "oppImpliedBucket"
  | "moneylineBucket"
  | "salaryBucket";

const MLB_PITCHER_BUCKET_ORDER: Record<MlbPitcherBucketKey, string[]> = {
  projectionBucket: ["<10", "10-13.9", "14-17.9", "18-21.9", "22+", "unknown"],
  valueBucket: ["<1.4x", "1.4-1.79x", "1.8-2.19x", "2.2x+", "unknown"],
  projectedOwnBucket: ["<5", "5-9.9", "10-14.9", "15-19.9", "20+", "unknown"],
  oppImpliedBucket: ["<3.2", "3.2-3.79", "3.8-4.49", "4.5+", "unknown"],
  moneylineBucket: ["fav160+", "fav120-159", "pickem", "dog110+", "unknown"],
  salaryBucket: ["<7k", "7k-7.9k", "8k-8.9k", "9k+", "unknown"],
};

export type MlbPitcherLineupBucketRow = {
  bucket: string;
  rows: number;
  avgActualFpts: number | null;
  avgProjection: number | null;
  avgProjectedOwnPct: number | null;
  avgSalary: number | null;
  hit20Rate: number;
  hit25Rate: number;
  underownedHit20Rate: number;
  underownedHit25Rate: number;
};

export type MlbPitcherLineupCandidate = {
  playerId: number | null;
  name: string;
  teamAbbrev: string;
  salary: number | null;
  projection: number | null;
  linestarProj: number | null;
  ourProj: number | null;
  projectedOwnPct: number | null;
  ourOwnPct: number | null;
  projectedValueX: number | null;
  oppImplied: number | null;
  teamMl: number | null;
  isHome: boolean | null;
  projectionBucket: string;
  valueBucket: string;
  projectedOwnBucket: string;
  oppImpliedBucket: string;
  moneylineBucket: string;
  projectionScore: number | null;
  ceilingScore: number | null;
  contrarianScore: number | null;
  lineupScore: number | null;
  notes: string[];
  actualFpts: number | null;
  actualOwnPct: number | null;
};

export type MlbPitcherLineupSummary = {
  rows: number;
  slates: number;
  avgActualFpts: number | null;
  avgProjection: number | null;
  avgProjectedOwnPct: number | null;
  hit20Rate: number;
  hit25Rate: number;
  underownedHit20Rate: number;
  contextCoverage: {
    oppImpliedKnownRows: number;
    moneylineKnownRows: number;
  };
};

export type MlbPitcherCurrentSlateSummary = {
  id: number;
  slateDate: string;
  contestType: string;
  contestFormat: string;
  activeSpCount: number;
  pendingActualRows: number;
};

export type MlbPitcherLineupReport = {
  historical: {
    sample: MlbPitcherLineupSummary;
    findings: string[];
    buckets: {
      projection: MlbPitcherLineupBucketRow[];
      value: MlbPitcherLineupBucketRow[];
      projectedOwn: MlbPitcherLineupBucketRow[];
      oppImplied: MlbPitcherLineupBucketRow[];
      moneyline: MlbPitcherLineupBucketRow[];
    };
    topUnderownedSmashes: Array<{
      slateDate: string;
      name: string;
      teamAbbrev: string;
      salary: number | null;
      projection: number | null;
      projectedOwnPct: number | null;
      actualFpts: number | null;
      actualOwnPct: number | null;
      oppImplied: number | null;
      teamMl: number | null;
      projectedValueX: number | null;
    }>;
  };
  currentSlate: {
    slate: MlbPitcherCurrentSlateSummary;
    pitchers: MlbPitcherLineupCandidate[];
    contrarianPitchers: MlbPitcherLineupCandidate[];
  } | null;
};

export type MlbPitcherSlateSignal = {
  playerId: number;
  lineupScore: number | null;
  ceilingScore: number | null;
  contrarianScore: number | null;
  decisionBadge: {
    label: string;
    className: string;
    title: string;
  } | null;
  ceilingBadge: {
    label: string;
    className: string;
    title: string;
  } | null;
};

function mlbPitcherToNumber(value: unknown): number | null {
  if (value == null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mlbPitcherAvg(rows: MlbPitcherHistoricalRow[], key: keyof MlbPitcherHistoricalRow): number | null {
  const values = rows
    .map((row) => mlbPitcherToNumber(row[key]))
    .filter((value): value is number => value != null);
  if (!values.length) return null;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(2));
}

function mlbPitcherPct(count: number, total: number): number {
  if (!total) return 0;
  return Number(((count / total) * 100).toFixed(2));
}

function mlbPitcherWeightedMean(values: Array<[number | null, number]>): number | null {
  let total = 0;
  let totalWeight = 0;
  for (const [value, weight] of values) {
    if (value == null || weight <= 0) continue;
    total += value * weight;
    totalWeight += weight;
  }
  if (totalWeight <= 0) return null;
  return Number((total / totalWeight).toFixed(2));
}

function getMlbPitcherProjectionBucket(projection: number | null): string {
  if (projection == null || projection <= 0) return "unknown";
  if (projection < 10) return "<10";
  if (projection < 14) return "10-13.9";
  if (projection < 18) return "14-17.9";
  if (projection < 22) return "18-21.9";
  return "22+";
}

function getMlbPitcherValueBucket(projection: number | null, salary: number | null): string {
  if (projection == null || projection <= 0 || salary == null || salary <= 0) return "unknown";
  const value = projection / (salary / 1000);
  if (value < 1.4) return "<1.4x";
  if (value < 1.8) return "1.4-1.79x";
  if (value < 2.2) return "1.8-2.19x";
  return "2.2x+";
}

function getMlbPitcherProjectedOwnBucket(projectedOwnPct: number | null): string {
  if (projectedOwnPct == null) return "unknown";
  if (projectedOwnPct < 5) return "<5";
  if (projectedOwnPct < 10) return "5-9.9";
  if (projectedOwnPct < 15) return "10-14.9";
  if (projectedOwnPct < 20) return "15-19.9";
  return "20+";
}

function getMlbPitcherOppImpliedBucket(oppImplied: number | null): string {
  if (oppImplied == null) return "unknown";
  if (oppImplied < 3.2) return "<3.2";
  if (oppImplied < 3.8) return "3.2-3.79";
  if (oppImplied < 4.5) return "3.8-4.49";
  return "4.5+";
}

function getMlbPitcherMoneylineBucket(teamMl: number | null): string {
  if (teamMl == null) return "unknown";
  if (teamMl <= -160) return "fav160+";
  if (teamMl <= -120) return "fav120-159";
  if (teamMl < 110) return "pickem";
  return "dog110+";
}

function getMlbPitcherSalaryBucket(salary: number | null): string {
  if (salary == null) return "unknown";
  if (salary < 7000) return "<7k";
  if (salary < 8000) return "7k-7.9k";
  if (salary < 9000) return "8k-8.9k";
  return "9k+";
}

function normalizeMlbPitcherRow(row: Record<string, unknown>): MlbPitcherHistoricalRow {
  const projection = mlbPitcherToNumber(row.projection);
  const salary = mlbPitcherToNumber(row.salary);
  const projectedValueX = projection != null && salary != null && salary > 0
    ? Number((projection / (salary / 1000)).toFixed(2))
    : null;
  const projectedOwnPct = mlbPitcherToNumber(row.projectedOwnPct);
  const oppImplied = mlbPitcherToNumber(row.oppImplied);
  const teamMl = mlbPitcherToNumber(row.teamMl);

  return {
    playerId: mlbPitcherToNumber(row.playerId),
    slateId: Number(row.slateId),
    slateDate: String(row.slateDate),
    name: String(row.name),
    teamAbbrev: String(row.teamAbbrev),
    salary,
    projection,
    linestarProj: mlbPitcherToNumber(row.linestarProj),
    ourProj: mlbPitcherToNumber(row.ourProj),
    projectedOwnPct,
    ourOwnPct: mlbPitcherToNumber(row.ourOwnPct),
    actualFpts: mlbPitcherToNumber(row.actualFpts),
    actualOwnPct: mlbPitcherToNumber(row.actualOwnPct),
    oppImplied,
    teamMl,
    isHome: row.isHome == null ? null : Boolean(row.isHome),
    projectionBucket: getMlbPitcherProjectionBucket(projection),
    valueBucket: getMlbPitcherValueBucket(projection, salary),
    projectedOwnBucket: getMlbPitcherProjectedOwnBucket(projectedOwnPct),
    oppImpliedBucket: getMlbPitcherOppImpliedBucket(oppImplied),
    moneylineBucket: getMlbPitcherMoneylineBucket(teamMl),
    salaryBucket: getMlbPitcherSalaryBucket(salary),
    projectedValueX,
  };
}

function summarizeMlbPitcherBuckets(
  rows: MlbPitcherHistoricalRow[],
  bucketKey: MlbPitcherBucketKey,
): MlbPitcherLineupBucketRow[] {
  const grouped = new Map<string, MlbPitcherHistoricalRow[]>();
  for (const row of rows) {
    const bucket = row[bucketKey] ?? "unknown";
    grouped.set(bucket, [...(grouped.get(bucket) ?? []), row]);
  }

  const summary = Array.from(grouped.entries()).map(([bucket, bucketRows]) => {
    const rowCount = bucketRows.length;
    const hit20 = bucketRows.filter((row) => (row.actualFpts ?? 0) >= MLB_PITCHER_SMASH_THRESHOLD).length;
    const hit25 = bucketRows.filter((row) => (row.actualFpts ?? 0) >= MLB_PITCHER_ELITE_THRESHOLD).length;
    const underowned20 = bucketRows.filter(
      (row) => (row.actualFpts ?? 0) >= MLB_PITCHER_SMASH_THRESHOLD
        && (row.actualOwnPct ?? 999) < MLB_PITCHER_UNDEROWNED_THRESHOLD,
    ).length;
    const underowned25 = bucketRows.filter(
      (row) => (row.actualFpts ?? 0) >= MLB_PITCHER_ELITE_THRESHOLD
        && (row.actualOwnPct ?? 999) < MLB_PITCHER_UNDEROWNED_THRESHOLD,
    ).length;

    return {
      bucket,
      rows: rowCount,
      avgActualFpts: mlbPitcherAvg(bucketRows, "actualFpts"),
      avgProjection: mlbPitcherAvg(bucketRows, "projection"),
      avgProjectedOwnPct: mlbPitcherAvg(bucketRows, "projectedOwnPct"),
      avgSalary: mlbPitcherAvg(bucketRows, "salary"),
      hit20Rate: mlbPitcherPct(hit20, rowCount),
      hit25Rate: mlbPitcherPct(hit25, rowCount),
      underownedHit20Rate: mlbPitcherPct(underowned20, rowCount),
      underownedHit25Rate: mlbPitcherPct(underowned25, rowCount),
    };
  });

  const order = MLB_PITCHER_BUCKET_ORDER[bucketKey];
  const orderMap = new Map(order.map((value, index) => [value, index]));
  summary.sort((a, b) => (orderMap.get(a.bucket) ?? 999) - (orderMap.get(b.bucket) ?? 999));
  return summary;
}

function buildMlbPitcherBucketLookup(
  bucketRows: Record<MlbPitcherBucketKey, MlbPitcherLineupBucketRow[]>,
): Record<MlbPitcherBucketKey, Record<string, MlbPitcherLineupBucketRow>> {
  return {
    projectionBucket: Object.fromEntries(bucketRows.projectionBucket.map((row) => [row.bucket, row])),
    valueBucket: Object.fromEntries(bucketRows.valueBucket.map((row) => [row.bucket, row])),
    projectedOwnBucket: Object.fromEntries(bucketRows.projectedOwnBucket.map((row) => [row.bucket, row])),
    oppImpliedBucket: Object.fromEntries(bucketRows.oppImpliedBucket.map((row) => [row.bucket, row])),
    moneylineBucket: Object.fromEntries(bucketRows.moneylineBucket.map((row) => [row.bucket, row])),
    salaryBucket: Object.fromEntries(bucketRows.salaryBucket.map((row) => [row.bucket, row])),
  };
}

function buildMlbPitcherFindings(
  bucketRows: Record<MlbPitcherBucketKey, MlbPitcherLineupBucketRow[]>,
): string[] {
  const topBucket = (
    rows: MlbPitcherLineupBucketRow[],
    metric: keyof Pick<MlbPitcherLineupBucketRow, "hit20Rate" | "hit25Rate" | "underownedHit20Rate">,
  ) => rows
    .filter((row) => row.rows >= MLB_PITCHER_MIN_SAMPLE)
    .sort((a, b) => (b[metric] - a[metric]) || (b.rows - a.rows))[0];

  const findings: string[] = [];
  const projection = topBucket(bucketRows.projectionBucket, "hit20Rate");
  if (projection) findings.push(`Projection bucket ${projection.bucket} has the best 20+ DK rate at ${projection.hit20Rate}%.`);
  const value = topBucket(bucketRows.valueBucket, "hit25Rate");
  if (value) findings.push(`Value bucket ${value.bucket} leads elite 25+ DK outcomes at ${value.hit25Rate}%.`);
  const projectedOwn = topBucket(bucketRows.projectedOwnBucket, "underownedHit20Rate");
  if (projectedOwn) findings.push(`Projected-own bucket ${projectedOwn.bucket} is the best contrarian lane at ${projectedOwn.underownedHit20Rate}% under-owned 20+ games.`);
  const moneyline = topBucket(bucketRows.moneylineBucket, "hit20Rate");
  if (moneyline) findings.push(`Moneyline bucket ${moneyline.bucket} has the strongest known-context 20+ DK rate at ${moneyline.hit20Rate}%.`);
  const opp = topBucket(bucketRows.oppImpliedBucket, "underownedHit20Rate");
  if (opp) findings.push(`Opponent-implied bucket ${opp.bucket} best supports contrarian ceiling at ${opp.underownedHit20Rate}% under-owned 20+ games.`);
  return findings;
}

function buildMlbPitcherCandidate(
  row: MlbPitcherHistoricalRow,
  bucketLookup: Record<MlbPitcherBucketKey, Record<string, MlbPitcherLineupBucketRow>>,
): MlbPitcherLineupCandidate {
  const projectionSummary = bucketLookup.projectionBucket[row.projectionBucket];
  const valueSummary = bucketLookup.valueBucket[row.valueBucket];
  const ownSummary = bucketLookup.projectedOwnBucket[row.projectedOwnBucket];
  const oppSummary = bucketLookup.oppImpliedBucket[row.oppImpliedBucket];
  const moneylineSummary = bucketLookup.moneylineBucket[row.moneylineBucket];

  const usable = (summary?: MlbPitcherLineupBucketRow | null) => Boolean(summary && summary.rows >= MLB_PITCHER_MIN_SAMPLE);
  const projectionScore = mlbPitcherWeightedMean([
    [usable(projectionSummary) ? projectionSummary.hit20Rate : null, 0.55],
    [usable(valueSummary) ? valueSummary.hit20Rate : null, 0.45],
  ]);
  const ceilingScore = mlbPitcherWeightedMean([
    [usable(projectionSummary) ? projectionSummary.hit25Rate : null, 0.55],
    [usable(valueSummary) ? valueSummary.hit25Rate : null, 0.45],
  ]);
  const contrarianScore = mlbPitcherWeightedMean([
    [usable(ownSummary) ? ownSummary.underownedHit20Rate : null, 0.45],
    [usable(valueSummary) ? valueSummary.underownedHit20Rate : null, 0.20],
    [usable(oppSummary) ? oppSummary.underownedHit20Rate : null, 0.20],
    [usable(moneylineSummary) ? moneylineSummary.underownedHit20Rate : null, 0.15],
  ]);
  const lineupScore = mlbPitcherWeightedMean([
    [projectionScore, 0.45],
    [ceilingScore, 0.35],
    [contrarianScore, 0.20],
  ]);

  const notes: string[] = [];
  if (usable(projectionSummary)) notes.push(`Projection ${row.projectionBucket}: ${projectionSummary.hit20Rate}% hit 20+ (${projectionSummary.rows} rows)`);
  if (usable(valueSummary)) notes.push(`Value ${row.valueBucket}: ${valueSummary.hit25Rate}% hit 25+ (${valueSummary.rows} rows)`);
  if (usable(ownSummary)) notes.push(`Projected own ${row.projectedOwnBucket}: ${ownSummary.underownedHit20Rate}% under-owned 20+ (${ownSummary.rows} rows)`);
  if (usable(oppSummary) && row.oppImpliedBucket !== "unknown") notes.push(`Opp implied ${row.oppImpliedBucket}: ${oppSummary.underownedHit20Rate}% under-owned 20+ (${oppSummary.rows} rows)`);
  if (usable(moneylineSummary) && row.moneylineBucket !== "unknown") notes.push(`Moneyline ${row.moneylineBucket}: ${moneylineSummary.hit20Rate}% hit 20+ (${moneylineSummary.rows} rows)`);

  return {
    playerId: row.playerId,
    name: row.name,
    teamAbbrev: row.teamAbbrev,
    salary: row.salary,
    projection: row.projection,
    linestarProj: row.linestarProj,
    ourProj: row.ourProj,
    projectedOwnPct: row.projectedOwnPct,
    ourOwnPct: row.ourOwnPct,
    projectedValueX: row.projectedValueX,
    oppImplied: row.oppImplied,
    teamMl: row.teamMl,
    isHome: row.isHome,
    projectionBucket: row.projectionBucket,
    valueBucket: row.valueBucket,
    projectedOwnBucket: row.projectedOwnBucket,
    oppImpliedBucket: row.oppImpliedBucket,
    moneylineBucket: row.moneylineBucket,
    projectionScore,
    ceilingScore,
    contrarianScore,
    lineupScore,
    notes,
    actualFpts: row.actualFpts,
    actualOwnPct: row.actualOwnPct,
  };
}

function formatMlbPitcherSignalMetric(value: number | null, suffix = ""): string {
  if (value == null) return "—";
  return `${value.toFixed(1)}${suffix}`;
}

function buildMlbPitcherSignalTitle(
  label: string,
  summary: string,
  candidate: MlbPitcherLineupCandidate,
): string {
  const parts = [
    `${label}: ${summary}`,
    `Lineup ${formatMlbPitcherSignalMetric(candidate.lineupScore)}`,
    `Ceiling ${formatMlbPitcherSignalMetric(candidate.ceilingScore)}`,
    `Pivot ${formatMlbPitcherSignalMetric(candidate.contrarianScore)}`,
    `Proj ${formatMlbPitcherSignalMetric(candidate.projection)}`,
    `Own ${formatMlbPitcherSignalMetric(candidate.projectedOwnPct, "%")}`,
    `Value ${formatMlbPitcherSignalMetric(candidate.projectedValueX, "x")}`,
    `Buckets ${candidate.projectionBucket} / ${candidate.valueBucket} / ${candidate.projectedOwnBucket}`,
  ];
  if (candidate.notes.length) parts.push(candidate.notes.slice(0, 3).join("; "));
  return parts.join(" | ");
}

function buildMlbPitcherSlateSignals(
  candidates: MlbPitcherLineupCandidate[],
): MlbPitcherSlateSignal[] {
  const decisionBadges = new Map<number, MlbPitcherSlateSignal["decisionBadge"]>();
  const ceilingBadges = new Map<number, MlbPitcherSlateSignal["ceilingBadge"]>();

  const lineupCandidates = candidates
    .filter((candidate) => candidate.playerId != null && candidate.lineupScore != null)
    .sort((a, b) => ((b.lineupScore ?? -999) - (a.lineupScore ?? -999)) || ((b.projection ?? -999) - (a.projection ?? -999)));

  const sp1 = lineupCandidates[0];
  if (sp1?.playerId != null) {
    decisionBadges.set(sp1.playerId, {
      label: "SP1",
      className: "bg-emerald-100 text-emerald-700",
      title: buildMlbPitcherSignalTitle(
        "SP1",
        "Best overall historical lineup fit on this slate.",
        sp1,
      ),
    });
  }

  for (const candidate of lineupCandidates.slice(1, 3)) {
    if (candidate.playerId == null || decisionBadges.has(candidate.playerId)) continue;
    decisionBadges.set(candidate.playerId, {
      label: "SP2",
      className: "bg-sky-100 text-sky-700",
      title: buildMlbPitcherSignalTitle(
        "SP2",
        "Strong secondary pitcher profile for lineup construction.",
        candidate,
      ),
    });
  }

  const pivotCandidates = candidates
    .filter(
      (candidate) => candidate.playerId != null
        && candidate.contrarianScore != null
        && (candidate.projectedOwnPct == null || candidate.projectedOwnPct <= MLB_PITCHER_PIVOT_MAX_OWN),
    )
    .sort((a, b) => ((b.contrarianScore ?? -999) - (a.contrarianScore ?? -999)) || ((b.ceilingScore ?? -999) - (a.ceilingScore ?? -999)));

  for (const candidate of pivotCandidates.slice(0, 2)) {
    if (candidate.playerId == null || decisionBadges.has(candidate.playerId)) continue;
    decisionBadges.set(candidate.playerId, {
      label: "PIVOT",
      className: "bg-amber-100 text-amber-700",
      title: buildMlbPitcherSignalTitle(
        "PIVOT",
        "Best lower-owned contrarian lane from the historical pitcher cohorts.",
        candidate,
      ),
    });
  }

  const ceilingCandidates = candidates
    .filter((candidate) => candidate.playerId != null && candidate.ceilingScore != null)
    .sort((a, b) => ((b.ceilingScore ?? -999) - (a.ceilingScore ?? -999)) || ((b.projection ?? -999) - (a.projection ?? -999)));

  for (const [index, candidate] of ceilingCandidates.slice(0, 3).entries()) {
    if (candidate.playerId == null) continue;
    ceilingBadges.set(candidate.playerId, {
      label: `CEIL #${index + 1}`,
      className: index === 0 ? "bg-fuchsia-100 text-fuchsia-700" : "bg-violet-100 text-violet-700",
      title: buildMlbPitcherSignalTitle(
        `CEIL #${index + 1}`,
        "Top historical ceiling score on this slate.",
        candidate,
      ),
    });
  }

  return candidates.flatMap((candidate) => {
    if (candidate.playerId == null) return [];
    return [{
      playerId: candidate.playerId,
      lineupScore: candidate.lineupScore,
      ceilingScore: candidate.ceilingScore,
      contrarianScore: candidate.contrarianScore,
      decisionBadge: decisionBadges.get(candidate.playerId) ?? null,
      ceilingBadge: ceilingBadges.get(candidate.playerId) ?? null,
    }];
  });
}

export async function getLatestMlbPitcherSignals(): Promise<MlbPitcherSlateSignal[]> {
  const targetResult = await db.execute<MlbPitcherTargetSlateRow>(sql`
    SELECT
      ds.id AS "id",
      ds.slate_date::text AS "slateDate",
      ds.contest_type AS "contestType",
      ds.contest_format AS "contestFormat",
      COUNT(*) FILTER (
        WHERE COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions LIKE '%SP%'
      )::int AS "activeSpCount",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NULL)::int AS "pendingActualRows"
    FROM dk_slates ds
    JOIN dk_players dp ON dp.slate_id = ds.id
    WHERE ds.sport = 'mlb'
    GROUP BY ds.id, ds.slate_date, ds.contest_type, ds.contest_format
    HAVING COUNT(*) FILTER (
      WHERE COALESCE(dp.is_out, false) = false
        AND dp.eligible_positions LIKE '%SP%'
    ) > 0
    ORDER BY ds.slate_date DESC, ds.id DESC
    LIMIT 1
  `);
  const targetSlate = (targetResult.rows as MlbPitcherTargetSlateRow[])[0] ?? null;
  if (!targetSlate) return [];

  const historicalResult = await db.execute(sql`
    SELECT
      dp.id AS "playerId",
      ds.id AS "slateId",
      ds.slate_date::text AS "slateDate",
      dp.name,
      dp.team_abbrev AS "teamAbbrev",
      dp.salary,
      COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS "projection",
      dp.linestar_proj AS "linestarProj",
      dp.our_proj AS "ourProj",
      COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "projectedOwnPct",
      dp.our_own_pct AS "ourOwnPct",
      dp.actual_fpts AS "actualFpts",
      dp.actual_own_pct AS "actualOwnPct",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
        WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
        ELSE NULL
      END AS "oppImplied",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
        WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
        ELSE NULL
      END AS "teamMl",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
        WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
        ELSE NULL
      END AS "isHome"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE ds.sport = 'mlb'
      AND ds.contest_type = ${targetSlate.contestType}
      AND ds.contest_format = ${targetSlate.contestFormat}
      AND ds.id <> ${targetSlate.id}
      AND dp.actual_fpts IS NOT NULL
      AND dp.actual_own_pct IS NOT NULL
      AND COALESCE(dp.is_out, false) = false
      AND dp.eligible_positions LIKE '%SP%'
  `);
  const historicalRows = (historicalResult.rows as Record<string, unknown>[]).map(normalizeMlbPitcherRow);
  if (!historicalRows.length) return [];

  const bucketRows: Record<MlbPitcherBucketKey, MlbPitcherLineupBucketRow[]> = {
    projectionBucket: summarizeMlbPitcherBuckets(historicalRows, "projectionBucket"),
    valueBucket: summarizeMlbPitcherBuckets(historicalRows, "valueBucket"),
    projectedOwnBucket: summarizeMlbPitcherBuckets(historicalRows, "projectedOwnBucket"),
    oppImpliedBucket: summarizeMlbPitcherBuckets(historicalRows, "oppImpliedBucket"),
    moneylineBucket: summarizeMlbPitcherBuckets(historicalRows, "moneylineBucket"),
    salaryBucket: summarizeMlbPitcherBuckets(historicalRows, "salaryBucket"),
  };
  const bucketLookup = buildMlbPitcherBucketLookup(bucketRows);

  const currentResult = await db.execute(sql`
    SELECT
      dp.id AS "playerId",
      ds.id AS "slateId",
      ds.slate_date::text AS "slateDate",
      dp.name,
      dp.team_abbrev AS "teamAbbrev",
      dp.salary,
      COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS "projection",
      dp.linestar_proj AS "linestarProj",
      dp.our_proj AS "ourProj",
      COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "projectedOwnPct",
      dp.our_own_pct AS "ourOwnPct",
      dp.actual_fpts AS "actualFpts",
      dp.actual_own_pct AS "actualOwnPct",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
        WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
        ELSE NULL
      END AS "oppImplied",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
        WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
        ELSE NULL
      END AS "teamMl",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
        WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
        ELSE NULL
      END AS "isHome"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE ds.id = ${targetSlate.id}
      AND COALESCE(dp.is_out, false) = false
      AND dp.eligible_positions LIKE '%SP%'
    ORDER BY COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) DESC NULLS LAST, dp.salary DESC
  `);

  const currentRows = (currentResult.rows as Record<string, unknown>[]).map(normalizeMlbPitcherRow);
  const candidates = currentRows
    .map((row) => buildMlbPitcherCandidate(row, bucketLookup))
    .sort((a, b) => ((b.lineupScore ?? -999) - (a.lineupScore ?? -999)) || ((b.projection ?? -999) - (a.projection ?? -999)));

  return buildMlbPitcherSlateSignals(candidates);
}

export async function getMlbPitcherLineupReport(): Promise<MlbPitcherLineupReport | null> {
  const targetResult = await db.execute<MlbPitcherTargetSlateRow>(sql`
    SELECT
      ds.id AS "id",
      ds.slate_date::text AS "slateDate",
      ds.contest_type AS "contestType",
      ds.contest_format AS "contestFormat",
      COUNT(*) FILTER (
        WHERE COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions LIKE '%SP%'
      )::int AS "activeSpCount",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NULL)::int AS "pendingActualRows"
    FROM dk_slates ds
    JOIN dk_players dp ON dp.slate_id = ds.id
    WHERE ds.sport = 'mlb'
      AND ds.contest_type = 'main'
      AND ds.contest_format = 'gpp'
    GROUP BY ds.id, ds.slate_date, ds.contest_type, ds.contest_format
    HAVING COUNT(*) FILTER (
      WHERE COALESCE(dp.is_out, false) = false
        AND dp.eligible_positions LIKE '%SP%'
    ) > 0
    ORDER BY
      CASE WHEN COUNT(*) FILTER (WHERE dp.actual_fpts IS NULL) > 0 THEN 0 ELSE 1 END,
      ds.slate_date DESC,
      ds.id DESC
    LIMIT 1
  `);
  const targetSlate = (targetResult.rows as MlbPitcherTargetSlateRow[])[0] ?? null;

  const historicalResult = await db.execute(sql`
    SELECT
      dp.id AS "playerId",
      ds.id AS "slateId",
      ds.slate_date::text AS "slateDate",
      dp.name,
      dp.team_abbrev AS "teamAbbrev",
      dp.salary,
      COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS "projection",
      dp.linestar_proj AS "linestarProj",
      dp.our_proj AS "ourProj",
      COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "projectedOwnPct",
      dp.our_own_pct AS "ourOwnPct",
      dp.actual_fpts AS "actualFpts",
      dp.actual_own_pct AS "actualOwnPct",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
        WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
        ELSE NULL
      END AS "oppImplied",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
        WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
        ELSE NULL
      END AS "teamMl",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
        WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
        ELSE NULL
      END AS "isHome"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE ds.sport = 'mlb'
      AND ds.contest_type = 'main'
      AND ds.contest_format = 'gpp'
      AND dp.actual_fpts IS NOT NULL
      AND dp.actual_own_pct IS NOT NULL
      AND COALESCE(dp.is_out, false) = false
      AND dp.eligible_positions LIKE '%SP%'
      ${targetSlate ? sql`AND ds.id <> ${targetSlate.id}` : sql``}
  `);
  const historicalRows = (historicalResult.rows as Record<string, unknown>[]).map(normalizeMlbPitcherRow);
  if (!historicalRows.length) return null;

  const bucketRows: Record<MlbPitcherBucketKey, MlbPitcherLineupBucketRow[]> = {
    projectionBucket: summarizeMlbPitcherBuckets(historicalRows, "projectionBucket"),
    valueBucket: summarizeMlbPitcherBuckets(historicalRows, "valueBucket"),
    projectedOwnBucket: summarizeMlbPitcherBuckets(historicalRows, "projectedOwnBucket"),
    oppImpliedBucket: summarizeMlbPitcherBuckets(historicalRows, "oppImpliedBucket"),
    moneylineBucket: summarizeMlbPitcherBuckets(historicalRows, "moneylineBucket"),
    salaryBucket: summarizeMlbPitcherBuckets(historicalRows, "salaryBucket"),
  };
  const bucketLookup = buildMlbPitcherBucketLookup(bucketRows);

  const sample: MlbPitcherLineupSummary = {
    rows: historicalRows.length,
    slates: new Set(historicalRows.map((row) => row.slateId)).size,
    avgActualFpts: mlbPitcherAvg(historicalRows, "actualFpts"),
    avgProjection: mlbPitcherAvg(historicalRows, "projection"),
    avgProjectedOwnPct: mlbPitcherAvg(historicalRows, "projectedOwnPct"),
    hit20Rate: mlbPitcherPct(historicalRows.filter((row) => (row.actualFpts ?? 0) >= MLB_PITCHER_SMASH_THRESHOLD).length, historicalRows.length),
    hit25Rate: mlbPitcherPct(historicalRows.filter((row) => (row.actualFpts ?? 0) >= MLB_PITCHER_ELITE_THRESHOLD).length, historicalRows.length),
    underownedHit20Rate: mlbPitcherPct(
      historicalRows.filter(
        (row) => (row.actualFpts ?? 0) >= MLB_PITCHER_SMASH_THRESHOLD
          && (row.actualOwnPct ?? 999) < MLB_PITCHER_UNDEROWNED_THRESHOLD,
      ).length,
      historicalRows.length,
    ),
    contextCoverage: {
      oppImpliedKnownRows: historicalRows.filter((row) => row.oppImplied != null).length,
      moneylineKnownRows: historicalRows.filter((row) => row.teamMl != null).length,
    },
  };

  const topUnderownedSmashes = historicalRows
    .filter((row) => (row.actualFpts ?? 0) >= MLB_PITCHER_SMASH_THRESHOLD && (row.actualOwnPct ?? 999) < MLB_PITCHER_UNDEROWNED_THRESHOLD)
    .sort((a, b) => (b.actualFpts ?? -999) - (a.actualFpts ?? -999))
    .slice(0, 12)
    .map((row) => ({
      slateDate: row.slateDate,
      name: row.name,
      teamAbbrev: row.teamAbbrev,
      salary: row.salary,
      projection: row.projection,
      projectedOwnPct: row.projectedOwnPct,
      actualFpts: row.actualFpts,
      actualOwnPct: row.actualOwnPct,
      oppImplied: row.oppImplied,
      teamMl: row.teamMl,
      projectedValueX: row.projectedValueX,
    }));

  let currentSlate: MlbPitcherLineupReport["currentSlate"] = null;
  if (targetSlate) {
    const currentResult = await db.execute(sql`
      SELECT
        dp.id AS "playerId",
        ds.id AS "slateId",
        ds.slate_date::text AS "slateDate",
        dp.name,
        dp.team_abbrev AS "teamAbbrev",
        dp.salary,
        COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS "projection",
        dp.linestar_proj AS "linestarProj",
        dp.our_proj AS "ourProj",
        COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "projectedOwnPct",
        dp.our_own_pct AS "ourOwnPct",
        dp.actual_fpts AS "actualFpts",
        dp.actual_own_pct AS "actualOwnPct",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
          WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
          ELSE NULL
        END AS "oppImplied",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
          WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
          ELSE NULL
        END AS "teamMl",
        CASE
          WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
          WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
          ELSE NULL
        END AS "isHome"
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
      WHERE ds.id = ${targetSlate.id}
        AND COALESCE(dp.is_out, false) = false
        AND dp.eligible_positions LIKE '%SP%'
      ORDER BY COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) DESC NULLS LAST, dp.salary DESC
    `);

    const currentRows = (currentResult.rows as Record<string, unknown>[]).map(normalizeMlbPitcherRow);
    const candidates = currentRows
      .map((row) => buildMlbPitcherCandidate(row, bucketLookup))
      .sort((a, b) => ((b.lineupScore ?? -999) - (a.lineupScore ?? -999)) || ((b.projection ?? -999) - (a.projection ?? -999)));
    const contrarianPitchers = candidates
      .filter((row) => row.projectedOwnPct == null || row.projectedOwnPct <= MLB_PITCHER_PIVOT_MAX_OWN)
      .sort((a, b) => ((b.contrarianScore ?? -999) - (a.contrarianScore ?? -999)) || ((b.ceilingScore ?? -999) - (a.ceilingScore ?? -999)) || ((b.projection ?? -999) - (a.projection ?? -999)));

    currentSlate = {
      slate: targetSlate,
      pitchers: candidates.slice(0, 10),
      contrarianPitchers: contrarianPitchers.slice(0, 10),
    };
  }

  return {
    historical: {
      sample,
      findings: buildMlbPitcherFindings(bucketRows),
      buckets: {
        projection: bucketRows.projectionBucket,
        value: bucketRows.valueBucket,
        projectedOwn: bucketRows.projectedOwnBucket,
        oppImplied: bucketRows.oppImpliedBucket,
        moneyline: bucketRows.moneylineBucket,
      },
      topUnderownedSmashes,
    },
    currentSlate,
  };
}
