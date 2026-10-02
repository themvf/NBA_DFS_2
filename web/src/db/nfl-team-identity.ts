import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { NflArchetypeGameRow } from "@/db/queries";

export type NflIdentityTeam = { abbreviation: string; name: string };

export type NflIdentityGame = {
  season: number;
  week: number;
  gameId: string;
  date: string;
  kickoff: string;
  homeTeam: string;
  awayTeam: string;
  opponent: string;
  isHome: boolean;
  teamScore: number | null;
  opponentScore: number | null;
  restDays: number | null;
  offensePlays: number;
  dropbacks: number;
  neutralEarlyPlays: number;
  neutralEarlyDropbacks: number;
  passOeSum: number;
  passOeCount: number;
  airYardsSum: number;
  airYardsCount: number;
  offenseEpaSum: number;
  offenseEpaCount: number;
  offenseSuccesses: number;
  offenseExplosives: number;
  defensePlays: number;
  defenseEpaSum: number;
  defenseEpaCount: number;
  defenseSuccessesAllowed: number;
  defenseExplosivesAllowed: number;
  drives: number;
  touchdownDrives: number;
  fieldGoalDrives: number;
  threeAndOutDrives: number;
  turnoverDrives: number;
  roof: string | null;
  temp: number | null;
  wind: number | null;
  formationRows: number;
  personnelRows: number;
  pressureRows: number;
  playVersion: string;
  driveVersion: string;
  market: {
    source: "captured" | "archive";
    spread: number | null;
    total: number | null;
    impliedPoints: number | null;
    observedAt: string;
    snapshotId: number;
  } | null;
};

function resultRows(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? rows as Record<string, unknown>[] : [];
}

const number = (value: unknown): number => Number(value ?? 0);
const optionalNumber = (value: unknown): number | null => value == null ? null : Number(value);
const dateText = (value: unknown): string => value instanceof Date ? value.toISOString() : String(value);

export async function getNflIdentityOptions(): Promise<{ teams: NflIdentityTeam[]; seasons: number[] }> {
  const [teamsResult, seasonsResult] = await Promise.all([
    db.execute(sql`SELECT abbreviation, name FROM nfl_teams WHERE active = TRUE ORDER BY name`),
    db.execute(sql`SELECT DISTINCT season FROM nfl_pbp_archetypes WHERE season_type = 'REG' ORDER BY season DESC LIMIT 6`),
  ]);
  return {
    teams: resultRows(teamsResult).map(row => ({ abbreviation: String(row.abbreviation), name: String(row.name) })),
    seasons: resultRows(seasonsResult).map(row => Number(row.season)),
  };
}

/** The PBP selector is intentionally short. Direct evidence links must still open older games. */
export async function getNflArchetypeGameById(gameId: string): Promise<NflArchetypeGameRow | null> {
  const result = await db.execute(sql`
    SELECT game_id, MAX(season) season, MAX(week) week, MAX(season_type) season_type,
           MAX(home_team) home_team, MAX(away_team) away_team, COUNT(*)::int plays,
           MAX(play_labeller_version) play_version, MAX(drive_labeller_version) drive_version,
           MAX(labelled_at) labelled_at
    FROM nfl_pbp_archetypes
    WHERE game_id = ${gameId}
    GROUP BY game_id`);
  const row = resultRows(result)[0];
  if (!row) return null;
  return {
    gameId: String(row.game_id), season: Number(row.season),
    week: row.week == null ? null : Number(row.week),
    seasonType: row.season_type == null ? null : String(row.season_type),
    homeTeam: String(row.home_team), awayTeam: String(row.away_team),
    plays: Number(row.plays), playVersion: String(row.play_version),
    driveVersion: String(row.drive_version),
    labelledAt: row.labelled_at == null ? null : dateText(row.labelled_at),
  };
}

export async function getNflIdentityGames(team: string, season: number): Promise<{ throughWeek: number; games: NflIdentityGame[] }> {
  const weekResult = await db.execute(sql`
    SELECT MAX(p.week)::int AS through_week
    FROM nfl_pbp_archetypes p
    JOIN nfl_season_games g ON g.nflverse_game_id = p.game_id
    WHERE p.season = ${season} AND p.season_type = 'REG'
      AND g.completed = TRUE AND g.game_type = 'REG'
      AND (p.posteam = ${team} OR p.defteam = ${team})`);
  const throughWeek = optionalNumber(resultRows(weekResult)[0]?.through_week) ?? 0;
  if (!throughWeek) return { throughWeek, games: [] };

  const result = await db.execute(sql`
    WITH game_stats AS (
      SELECT p.season, p.week, p.game_id, g.gameday, g.kickoff,
             g.matchup_id, g.home_rest, g.away_rest, g.home_score, g.away_score,
             ht.abbreviation AS home_team, at.abbreviation AS away_team,
             ht.name AS home_name, at.name AS away_name,
             COUNT(*) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass'))::int AS offense_plays,
             COUNT(*) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass') AND p.qb_dropback = TRUE)::int AS dropbacks,
             COUNT(*) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass') AND p.down IN (1,2)
               AND p.quarter <= 3 AND ABS(p.score_differential) <= 8)::int AS neutral_early_plays,
             COUNT(*) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass') AND p.down IN (1,2)
               AND p.quarter <= 3 AND ABS(p.score_differential) <= 8 AND p.qb_dropback = TRUE)::int AS neutral_early_dropbacks,
             COALESCE(SUM(p.pass_oe) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass')),0) AS pass_oe_sum,
             COUNT(p.pass_oe) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass'))::int AS pass_oe_count,
             COALESCE(SUM(p.air_yards) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass')),0) AS air_yards_sum,
             COUNT(p.air_yards) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass'))::int AS air_yards_count,
             COALESCE(SUM(p.epa) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass')),0) AS offense_epa_sum,
             COUNT(p.epa) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass'))::int AS offense_epa_count,
             COALESCE(SUM(p.success::int) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass')),0)::int AS offense_successes,
             COUNT(*) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass') AND p.yards_gained >= 20)::int AS offense_explosives,
             COUNT(*) FILTER (WHERE p.defteam = ${team} AND p.play_type IN ('run','pass'))::int AS defense_plays,
             COALESCE(SUM(p.epa) FILTER (WHERE p.defteam = ${team} AND p.play_type IN ('run','pass')),0) AS defense_epa_sum,
             COUNT(p.epa) FILTER (WHERE p.defteam = ${team} AND p.play_type IN ('run','pass'))::int AS defense_epa_count,
             COALESCE(SUM(p.success::int) FILTER (WHERE p.defteam = ${team} AND p.play_type IN ('run','pass')),0)::int AS defense_successes_allowed,
             COUNT(*) FILTER (WHERE p.defteam = ${team} AND p.play_type IN ('run','pass') AND p.yards_gained >= 20)::int AS defense_explosives_allowed,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${team} AND p.drive_archetype IS NOT NULL)::int AS drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${team} AND p.drive_archetype = 'TOUCHDOWN')::int AS touchdown_drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${team} AND p.drive_archetype = 'FIELD_GOAL')::int AS field_goal_drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${team} AND p.drive_archetype = 'THREE_AND_OUT')::int AS three_and_out_drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${team} AND p.drive_archetype = 'TURNOVER_GIVEAWAY')::int AS turnover_drives,
             MAX(p.roof) AS roof, MAX(p.temp) AS temp, MAX(p.wind) AS wind,
             COUNT(p.formation) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass'))::int AS formation_rows,
             COUNT(p.personnel_grouping) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass'))::int AS personnel_rows,
             COUNT(p.pressure) FILTER (WHERE p.posteam = ${team} AND p.play_type IN ('run','pass'))::int AS pressure_rows,
             MAX(p.play_labeller_version) AS play_version, MAX(p.drive_labeller_version) AS drive_version
      FROM nfl_pbp_archetypes p
      JOIN nfl_season_games g ON g.nflverse_game_id = p.game_id
      JOIN nfl_teams ht ON ht.team_id = g.home_team_id
      JOIN nfl_teams at ON at.team_id = g.away_team_id
      WHERE p.season IN (${season}, ${season - 1}) AND p.season_type = 'REG'
        AND g.game_type = 'REG' AND g.completed = TRUE
        AND (p.posteam = ${team} OR p.defteam = ${team})
      GROUP BY p.season, p.week, p.game_id, g.id, ht.abbreviation, at.abbreviation, ht.name, at.name
    )
    SELECT gs.*,
           market.id AS market_id, market.captured_at AS market_at,
           market.home_spread AS market_home_spread, market.vegas_total AS market_total,
           market.home_implied AS market_home_implied, market.away_implied AS market_away_implied,
           archived.id AS archive_id, archived.snapshot_at AS archive_at,
           archived.home_spread AS archive_home_spread
    FROM game_stats gs
    LEFT JOIN LATERAL (
      SELECT h.id, h.captured_at, h.home_spread, h.vegas_total, h.home_implied, h.away_implied
      FROM game_odds_history h
      WHERE h.sport = 'nfl' AND h.matchup_id = gs.matchup_id AND h.captured_at < gs.kickoff
      ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
    ) market ON TRUE
    LEFT JOIN LATERAL (
      SELECT s.id, s.snapshot_at, s.home_spread
      FROM nfl_line_snapshots s
      WHERE gs.matchup_id IS NULL AND s.season = gs.season
        AND s.home_team = gs.home_name AND s.away_team = gs.away_name
        AND s.commence_time::date = gs.gameday
        AND s.snapshot_at < s.commence_time AND s.snapshot_at < gs.kickoff
      ORDER BY s.snapshot_at DESC, s.id DESC LIMIT 1
    ) archived ON TRUE
    ORDER BY gs.season DESC, gs.week ASC, gs.game_id ASC`);

  const games = resultRows(result).map((row): NflIdentityGame => {
    const isHome = String(row.home_team) === team;
    const market = row.market_id != null ? {
      source: "captured" as const,
      spread: optionalNumber(row.market_home_spread) == null ? null : optionalNumber(row.market_home_spread)! * (isHome ? 1 : -1),
      total: optionalNumber(row.market_total),
      impliedPoints: optionalNumber(isHome ? row.market_home_implied : row.market_away_implied),
      observedAt: dateText(row.market_at), snapshotId: number(row.market_id),
    } : row.archive_id != null ? {
      source: "archive" as const,
      spread: optionalNumber(row.archive_home_spread) == null ? null : optionalNumber(row.archive_home_spread)! * (isHome ? 1 : -1),
      total: null, impliedPoints: null,
      observedAt: dateText(row.archive_at), snapshotId: number(row.archive_id),
    } : null;
    return {
      season: number(row.season), week: number(row.week), gameId: String(row.game_id),
      date: dateText(row.gameday).slice(0, 10), kickoff: dateText(row.kickoff),
      homeTeam: String(row.home_team), awayTeam: String(row.away_team),
      opponent: isHome ? String(row.away_team) : String(row.home_team), isHome,
      teamScore: optionalNumber(isHome ? row.home_score : row.away_score),
      opponentScore: optionalNumber(isHome ? row.away_score : row.home_score),
      restDays: optionalNumber(isHome ? row.home_rest : row.away_rest),
      offensePlays: number(row.offense_plays), dropbacks: number(row.dropbacks),
      neutralEarlyPlays: number(row.neutral_early_plays), neutralEarlyDropbacks: number(row.neutral_early_dropbacks),
      passOeSum: number(row.pass_oe_sum), passOeCount: number(row.pass_oe_count),
      airYardsSum: number(row.air_yards_sum), airYardsCount: number(row.air_yards_count),
      offenseEpaSum: number(row.offense_epa_sum), offenseEpaCount: number(row.offense_epa_count),
      offenseSuccesses: number(row.offense_successes), offenseExplosives: number(row.offense_explosives),
      defensePlays: number(row.defense_plays), defenseEpaSum: number(row.defense_epa_sum),
      defenseEpaCount: number(row.defense_epa_count), defenseSuccessesAllowed: number(row.defense_successes_allowed),
      defenseExplosivesAllowed: number(row.defense_explosives_allowed),
      drives: number(row.drives), touchdownDrives: number(row.touchdown_drives),
      fieldGoalDrives: number(row.field_goal_drives), threeAndOutDrives: number(row.three_and_out_drives),
      turnoverDrives: number(row.turnover_drives),
      roof: row.roof == null ? null : String(row.roof), temp: optionalNumber(row.temp), wind: optionalNumber(row.wind),
      formationRows: number(row.formation_rows), personnelRows: number(row.personnel_rows), pressureRows: number(row.pressure_rows),
      playVersion: String(row.play_version), driveVersion: String(row.drive_version), market,
    };
  });
  return { throughWeek, games };
}
