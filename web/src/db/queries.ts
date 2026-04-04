import { db } from ".";
import { ensureDkPlayerPropColumns, ensureProjectionExperimentTables } from "./ensure-schema";
import { teams, nbaTeamStats, nbaPlayerStats, nbaMatchups, dkSlates, dkPlayers, dkLineups, mlbTeams, mlbTeamStats, mlbMatchups } from "./schema";
import { eq, desc, sql, gte, and } from "drizzle-orm";

const CURRENT_SEASON = "2025-26";

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

export async function getDkPlayers(sport: Sport = "nba"): Promise<DkPlayerRow[]> {
  await ensureDkPlayerPropColumns();
  await ensureProjectionExperimentTables();

  if (sport === "mlb") {
    const result = await db.execute<DkPlayerRow>(sql`
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
        dp.proj_own_pct       AS "projOwnPct",
        dp.our_proj           AS "ourProj",
        NULL::REAL            AS "liveProj",
        dp.expected_hr        AS "expectedHr",
        dp.hr_prob_1plus      AS "hrProb1Plus",
        dp.our_own_pct        AS "ourOwnPct",
        dp.our_leverage       AS "ourLeverage",
        NULL::REAL            AS "liveOwnPct",
        NULL::REAL            AS "liveLeverage",
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
        dp.proj_floor         AS "projFloor",
        dp.proj_ceiling       AS "projCeiling",
        dp.boom_rate          AS "boomRate",
        dp.actual_fpts        AS "actualFpts",
        dp.actual_own_pct     AS "actualOwnPct",
        mt.name               AS "teamName",
        mt.logo_url           AS "teamLogo",
        mm.vegas_total        AS "vegasTotal",
        mm.vegas_prob_home    AS "homeWinProb",
        mm.home_ml            AS "homeMl",
        mm.away_ml            AS "awayMl",
        mm.home_team_id       AS "homeTeamId",
        mm.away_team_id       AS "awayTeamId",
        mm.home_implied       AS "homeImplied",
        mm.away_implied       AS "awayImplied",
        ds.slate_date         AS "slateDate",
        ds.sport              AS "sport"
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
  const result = await db.execute<DkPlayerRow>(sql`
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
      dp.proj_own_pct      AS "projOwnPct",
      COALESCE(proj.model_proj_fpts, dp.our_proj) AS "ourProj",
      COALESCE(dp.live_proj, proj.final_proj_fpts, dp.our_proj, dp.linestar_proj) AS "liveProj",
      NULL::REAL           AS "expectedHr",
      NULL::REAL           AS "hrProb1Plus",
      dp.our_own_pct       AS "ourOwnPct",
      dp.our_leverage      AS "ourLeverage",
      COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "liveOwnPct",
      COALESCE(dp.live_leverage, dp.our_leverage) AS "liveLeverage",
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
      dp.proj_floor        AS "projFloor",
      dp.proj_ceiling      AS "projCeiling",
      dp.boom_rate         AS "boomRate",
      dp.actual_fpts       AS "actualFpts",
      dp.actual_own_pct    AS "actualOwnPct",
      t.name               AS "teamName",
      t.logo_url           AS "teamLogo",
      m.vegas_total        AS "vegasTotal",
      m.vegas_prob_home    AS "homeWinProb",
      m.home_ml            AS "homeMl",
      m.away_ml            AS "awayMl",
      m.home_team_id       AS "homeTeamId",
      m.away_team_id       AS "awayTeamId",
      NULL::DOUBLE PRECISION AS "homeImplied",
      NULL::DOUBLE PRECISION AS "awayImplied",
      ds.slate_date        AS "slateDate",
      ds.sport             AS "sport"
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
          ) / NULLIF(COALESCE(pps.model_weight, 0) + COALESCE(pps.market_weight, 0), 0), 2)::REAL
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
        100.0 * COUNT(*) FILTER (WHERE dl.actual_fpts >= COALESCE(ds.cash_line, 300)) / COUNT(*),
        1
      ) AS "cashRate",
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
};

export async function getCrossSlateAccuracy(sport: Sport = "nba"): Promise<CrossSlateAccuracyRow[]> {
  const result = await db.execute<CrossSlateAccuracyRow>(sql`
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
        FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) AS "ownCorr"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = ${sport}
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
      AVG(sub.our_leverage)      AS "avgLeverage",
      AVG(sub.our_proj)          AS "avgProj",
      AVG(sub.actual_fpts)       AS "avgActual",
      AVG(sub.actual_fpts - sub.our_proj) AS "avgBeat",
      COUNT(*)::int              AS "n"
    FROM (
      SELECT
        NTILE(4) OVER (ORDER BY dp.our_leverage ASC NULLS LAST) AS quartile,
        dp.our_leverage,
        dp.our_proj,
        dp.actual_fpts
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE dp.our_leverage IS NOT NULL
        AND dp.actual_fpts IS NOT NULL
        AND ds.sport = ${sport}
    ) sub
    GROUP BY sub.quartile
    ORDER BY sub.quartile
  `);
  return result.rows;
}

export { CURRENT_SEASON };
