import { db } from ".";
import { ensureDkPlayerPropColumns, ensureProjectionExperimentTables, ensureAnalyticsColumns, ensureOwnershipExperimentTables } from "./ensure-schema";
import { teams, nbaTeamStats, nbaPlayerStats, nbaMatchups, dkSlates, dkPlayers, dkLineups, mlbTeams, mlbTeamStats, mlbMatchups } from "./schema";
import { eq, desc, sql, gte, and } from "drizzle-orm";
// eslint-disable-next-line @typescript-eslint/no-require-imports
const solver = require("javascript-lp-solver") as {
  Solve: (model: SolverModel) => SolverResult;
};

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

// ── DFS Accuracy ─────────────────────────────────────────────

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
        lr.created_at
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
        CASE
          WHEN sample.eligible_positions LIKE '%SP%' THEN 'SP'
          WHEN sample.lineup_order BETWEEN 1 AND 4 THEN 'Hitters 1-4'
          WHEN sample.lineup_order BETWEEN 5 AND 9 THEN 'Hitters 5-9'
          ELSE 'Hitters Unknown'
        END AS "segment",
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
      GROUP BY 1
      ORDER BY CASE "segment"
        WHEN 'SP' THEN 1
        WHEN 'Hitters 1-4' THEN 2
        WHEN 'Hitters 5-9' THEN 3
        ELSE 4
      END
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
        MIN(sample.created_at)::text AS "capturedAt",
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
        ORDER BY MAX(sample.created_at) DESC, sample.slate_id DESC
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
            ORDER BY MAX(sample.created_at) DESC, sample.slate_id DESC
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
  avgPoints: number;
  anyTwoStackRate: number;
  anyThreeStackRate: number;
  anyFourStackRate: number;
  anyFiveStackRate: number;
  multiTeamStackRate: number;
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
  shapes: MlbPerfectLineupShapeRow[];
  teamRates: MlbPerfectLineupTeamRateRow[];
  opponentAllow: MlbPerfectLineupOpponentAllowRow[];
};

const NBA_ANALYTICS_SLOTS = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"] as const;
const MLB_ANALYTICS_SLOTS = ["P1", "P2", "C", "1B", "2B", "3B", "SS", "OF1", "OF2", "OF3"] as const;
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
    totalPoints: number;
    anyTwo: number;
    anyThree: number;
    anyFour: number;
    anyFive: number;
    multiTeam: number;
  }>();
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
    const summary = summaryStats.get(bucket) ?? {
      slateCount: 0,
      totalSalary: 0,
      totalPoints: 0,
      anyTwo: 0,
      anyThree: 0,
      anyFour: 0,
      anyFive: 0,
      multiTeam: 0,
    };
    summary.slateCount += 1;
    summary.totalSalary += perfectLineup.reduce((sum, player) => sum + player.salary, 0);
    summary.totalPoints += perfectLineup.reduce((sum, player) => sum + player.actualFpts, 0);
    if (stackedCounts.some((count) => count >= 2)) summary.anyTwo += 1;
    if (stackedCounts.some((count) => count >= 3)) summary.anyThree += 1;
    if (stackedCounts.some((count) => count >= 4)) summary.anyFour += 1;
    if (stackedCounts.some((count) => count >= 5)) summary.anyFive += 1;
    if (stackedCounts.filter((count) => count >= 2).length >= 2) summary.multiTeam += 1;
    summaryStats.set(bucket, summary);

    const shapeKey = `${bucket}::${shape}`;
    shapeCounts.set(shapeKey, (shapeCounts.get(shapeKey) ?? 0) + 1);
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
      avgPoints: round2(stats.totalPoints / stats.slateCount),
      anyTwoStackRate: round2((stats.anyTwo / stats.slateCount) * 100),
      anyThreeStackRate: round2((stats.anyThree / stats.slateCount) * 100),
      anyFourStackRate: round2((stats.anyFour / stats.slateCount) * 100),
      anyFiveStackRate: round2((stats.anyFive / stats.slateCount) * 100),
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
    SELECT
      dp.dk_starting_lineup_order                                             AS "orderSlot",
      COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.live_proj IS NOT NULL) AS "n",
      AVG(dp.live_proj)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.live_proj IS NOT NULL) AS "avgProj",
      AVG(dp.actual_fpts)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.live_proj IS NOT NULL) AS "avgActual",
      AVG(dp.actual_fpts - dp.live_proj)
        FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.live_proj IS NOT NULL) AS "avgDelta",
      AVG(dp.proj_own_pct)
        FILTER (WHERE dp.proj_own_pct IS NOT NULL)                             AS "avgProjOwn",
      AVG(dp.actual_own_pct)
        FILTER (WHERE dp.actual_own_pct IS NOT NULL)                           AS "avgActualOwn"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = 'mlb'
      AND dp.dk_starting_lineup_order BETWEEN 1 AND 9
      AND dp.eligible_positions NOT LIKE '%SP%'
      AND dp.eligible_positions NOT LIKE '%RP%'
    GROUP BY dp.dk_starting_lineup_order
    ORDER BY dp.dk_starting_lineup_order ASC
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
  const rows = await db.execute(sql`
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
      m.away_score     AS "awayScore"
    FROM mlb_matchups m
    LEFT JOIN mlb_teams ht ON ht.team_id = m.home_team_id
    LEFT JOIN mlb_teams at ON at.team_id = m.away_team_id
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
