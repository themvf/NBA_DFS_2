import { db } from ".";
import { teams, nbaTeamStats, nbaPlayerStats, nbaMatchups, dkSlates, dkPlayers, dkLineups } from "./schema";
import { eq, desc, sql } from "drizzle-orm";

const CURRENT_SEASON = "2025-26";

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
  ourLeverage: number | null;
  isOut: boolean | null;
  actualFpts: number | null;
  actualOwnPct: number | null;
  // Joined
  teamName: string | null;
  teamLogo: string | null;
  vegasTotal: number | null;
  homeWinProb: number | null;
  homeTeamId: number | null;
  slateDate: string | null;
};

export async function getDkPlayers(): Promise<DkPlayerRow[]> {
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
      dp.our_proj          AS "ourProj",
      dp.our_leverage      AS "ourLeverage",
      dp.is_out            AS "isOut",
      dp.actual_fpts       AS "actualFpts",
      dp.actual_own_pct    AS "actualOwnPct",
      t.name               AS "teamName",
      t.logo_url           AS "teamLogo",
      m.vegas_total        AS "vegasTotal",
      m.home_win_prob      AS "homeWinProb",
      m.home_team_id       AS "homeTeamId",
      ds.slate_date        AS "slateDate"
    FROM dk_players dp
    INNER JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN teams t ON t.team_id = dp.team_id
    LEFT JOIN nba_matchups m ON m.id = dp.matchup_id
    WHERE ds.id = (
      SELECT id FROM dk_slates ORDER BY slate_date DESC LIMIT 1
    )
    ORDER BY dp.our_leverage DESC NULLS LAST, dp.our_proj DESC NULLS LAST
  `);
  return result.rows;
}

// ── Slate Info ───────────────────────────────────────────────

export async function getLatestSlateInfo(): Promise<{
  slateDate: string;
  gameCount: number | null;
} | null> {
  const rows = await db
    .select({ slateDate: dkSlates.slateDate, gameCount: dkSlates.gameCount })
    .from(dkSlates)
    .orderBy(desc(dkSlates.slateDate))
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
  teamLogo: string | null;
};

export async function getDfsAccuracy(): Promise<{
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
      ds.slate_date AS "slateDate"
    FROM dk_players dp
    INNER JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.id = (SELECT id FROM dk_slates ORDER BY slate_date DESC LIMIT 1)
    GROUP BY ds.slate_date
  `);
  const metrics = metricResult.rows[0];
  if (!metrics || metrics.nOur === 0) return null;

  const playerResult = await db.execute<DfsAccuracyRow>(sql`
    SELECT
      dp.id, dp.name, dp.team_abbrev AS "teamAbbrev", dp.salary,
      dp.eligible_positions AS "eligiblePositions",
      dp.our_proj AS "ourProj", dp.linestar_proj AS "linestarProj",
      dp.actual_fpts AS "actualFpts", t.logo_url AS "teamLogo"
    FROM dk_players dp
    LEFT JOIN teams t ON t.team_id = dp.team_id
    WHERE dp.slate_id = (SELECT id FROM dk_slates ORDER BY slate_date DESC LIMIT 1)
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

export async function getDkLineupComparison(): Promise<LineupStrategyRow[]> {
  const result = await db.execute<LineupStrategyRow>(sql`
    SELECT
      dl.strategy,
      COUNT(*)::int AS "nLineups",
      AVG(dl.proj_fpts) AS "avgProjFpts",
      AVG(dl.actual_fpts) AS "avgActualFpts",
      AVG(dl.leverage) AS "avgLeverage",
      mode() WITHIN GROUP (ORDER BY dl.stack_team) AS "topStack"
    FROM dk_lineups dl
    WHERE dl.slate_id = (SELECT id FROM dk_slates ORDER BY slate_date DESC LIMIT 1)
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

export async function getDkStrategySummary(
  cashThreshold = 300
): Promise<StrategySummaryRow[]> {
  const result = await db.execute<StrategySummaryRow>(sql`
    SELECT
      dl.strategy,
      COUNT(DISTINCT dl.slate_id)::int AS "nSlates",
      COUNT(*)::int AS "totalLineups",
      AVG(dl.actual_fpts) AS "avgActualFpts",
      COUNT(*) FILTER (WHERE dl.actual_fpts >= ${cashThreshold})::int AS "totalCashed",
      ROUND(
        100.0 * COUNT(*) FILTER (WHERE dl.actual_fpts >= ${cashThreshold}) / COUNT(*),
        1
      ) AS "cashRate",
      MAX(dl.actual_fpts) AS "bestSingleLineup",
      AVG(dl.leverage) AS "avgLeverage"
    FROM dk_lineups dl
    WHERE dl.actual_fpts IS NOT NULL
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

export { CURRENT_SEASON };
