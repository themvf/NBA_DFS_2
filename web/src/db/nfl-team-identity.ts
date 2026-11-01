import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { NflArchetypeGameRow } from "@/db/queries";
import { PBP_TEAM_CODES, pbpTeamCode } from "@/lib/nfl/pbp-team-codes";

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
    moneyline: number | null;
    winProbability: number | null;
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
const moneyline = (value: unknown): number | null => {
  const parsed = optionalNumber(value);
  return parsed === 0 ? null : parsed;
};
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
  // Play-by-play carries nflverse codes; `team` stays the nfl_teams code for
  // the home/away comparison against nfl_teams below.
  const pbpTeam = pbpTeamCode(team);
  const weekResult = await db.execute(sql`
    SELECT MAX(p.week)::int AS through_week
    FROM nfl_pbp_archetypes p
    JOIN nfl_season_games g ON g.nflverse_game_id = p.game_id
    WHERE p.season = ${season} AND p.season_type = 'REG'
      AND g.completed = TRUE AND g.game_type = 'REG'
      AND (p.posteam = ${pbpTeam} OR p.defteam = ${pbpTeam})`);
  const throughWeek = optionalNumber(resultRows(weekResult)[0]?.through_week) ?? 0;
  if (!throughWeek) return { throughWeek, games: [] };

  const result = await db.execute(sql`
    WITH game_stats AS (
      SELECT p.season, p.week, p.game_id, g.gameday, g.kickoff,
             g.matchup_id, g.home_rest, g.away_rest, g.home_score, g.away_score,
             ht.abbreviation AS home_team, at.abbreviation AS away_team,
             ht.name AS home_name, at.name AS away_name,
             COUNT(*) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS offense_plays,
             COUNT(*) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass') AND p.qb_dropback = TRUE)::int AS dropbacks,
             COUNT(*) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass') AND p.down IN (1,2)
               AND p.quarter <= 3 AND ABS(p.score_differential) <= 8)::int AS neutral_early_plays,
             COUNT(*) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass') AND p.down IN (1,2)
               AND p.quarter <= 3 AND ABS(p.score_differential) <= 8 AND p.qb_dropback = TRUE)::int AS neutral_early_dropbacks,
             COALESCE(SUM(p.pass_oe) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass')),0) AS pass_oe_sum,
             COUNT(p.pass_oe) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS pass_oe_count,
             COALESCE(SUM(p.air_yards) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass')),0) AS air_yards_sum,
             COUNT(p.air_yards) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS air_yards_count,
             COALESCE(SUM(p.epa) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass')),0) AS offense_epa_sum,
             COUNT(p.epa) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS offense_epa_count,
             COALESCE(SUM(p.success::int) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass')),0)::int AS offense_successes,
             COUNT(*) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass') AND p.yards_gained >= 20)::int AS offense_explosives,
             COUNT(*) FILTER (WHERE p.defteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS defense_plays,
             COALESCE(SUM(p.epa) FILTER (WHERE p.defteam = ${pbpTeam} AND p.play_type IN ('run','pass')),0) AS defense_epa_sum,
             COUNT(p.epa) FILTER (WHERE p.defteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS defense_epa_count,
             COALESCE(SUM(p.success::int) FILTER (WHERE p.defteam = ${pbpTeam} AND p.play_type IN ('run','pass')),0)::int AS defense_successes_allowed,
             COUNT(*) FILTER (WHERE p.defteam = ${pbpTeam} AND p.play_type IN ('run','pass') AND p.yards_gained >= 20)::int AS defense_explosives_allowed,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${pbpTeam} AND p.drive_archetype IS NOT NULL)::int AS drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${pbpTeam} AND p.drive_archetype = 'TOUCHDOWN')::int AS touchdown_drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${pbpTeam} AND p.drive_archetype = 'FIELD_GOAL')::int AS field_goal_drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${pbpTeam} AND p.drive_archetype = 'THREE_AND_OUT')::int AS three_and_out_drives,
             COUNT(DISTINCT p.drive) FILTER (WHERE p.posteam = ${pbpTeam} AND p.drive_archetype = 'TURNOVER_GIVEAWAY')::int AS turnover_drives,
             MAX(p.roof) AS roof, MAX(p.temp) AS temp, MAX(p.wind) AS wind,
             COUNT(p.formation) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS formation_rows,
             COUNT(p.personnel_grouping) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS personnel_rows,
             COUNT(p.pressure) FILTER (WHERE p.posteam = ${pbpTeam} AND p.play_type IN ('run','pass'))::int AS pressure_rows,
             MAX(p.play_labeller_version) AS play_version, MAX(p.drive_labeller_version) AS drive_version
      FROM nfl_pbp_archetypes p
      JOIN nfl_season_games g ON g.nflverse_game_id = p.game_id
      JOIN nfl_teams ht ON ht.team_id = g.home_team_id
      JOIN nfl_teams at ON at.team_id = g.away_team_id
      WHERE p.season IN (${season}, ${season - 1}) AND p.season_type = 'REG'
        AND g.game_type = 'REG' AND g.completed = TRUE
        AND (p.posteam = ${pbpTeam} OR p.defteam = ${pbpTeam})
      GROUP BY p.season, p.week, p.game_id, g.id, ht.abbreviation, at.abbreviation, ht.name, at.name
    )
    SELECT gs.*,
           market.id AS market_id, market.captured_at AS market_at,
           market.home_ml AS market_home_ml, market.away_ml AS market_away_ml,
           market.vegas_prob_home AS market_home_win_probability,
           market.home_spread AS market_home_spread, market.vegas_total AS market_total,
           market.home_implied AS market_home_implied, market.away_implied AS market_away_implied,
           archived.id AS archive_id, archived.snapshot_at AS archive_at,
           archived.home_spread AS archive_home_spread,
           archived.home_ml AS archive_home_ml, archived.away_ml AS archive_away_ml
    FROM game_stats gs
    LEFT JOIN LATERAL (
      SELECT h.id, h.captured_at, h.home_ml, h.away_ml, h.vegas_prob_home,
             h.home_spread, h.vegas_total, h.home_implied, h.away_implied
      FROM game_odds_history h
      WHERE h.sport = 'nfl' AND h.matchup_id = gs.matchup_id AND h.captured_at < gs.kickoff
      ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
    ) market ON TRUE
    LEFT JOIN LATERAL (
      SELECT s.id, s.snapshot_at, s.home_ml, s.away_ml, s.home_spread
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
    const homeMoneyline = moneyline(row.market_home_ml);
    const awayMoneyline = moneyline(row.market_away_ml);
    const archivedHomeMoneyline = moneyline(row.archive_home_ml);
    const archivedAwayMoneyline = moneyline(row.archive_away_ml);
    const market = row.market_id != null ? {
      source: "captured" as const,
      moneyline: isHome ? homeMoneyline : awayMoneyline,
      winProbability: optionalNumber(row.market_home_win_probability) == null ? null
        : isHome ? optionalNumber(row.market_home_win_probability) : 1 - optionalNumber(row.market_home_win_probability)!,
      spread: optionalNumber(row.market_home_spread) == null ? null : optionalNumber(row.market_home_spread)! * (isHome ? 1 : -1),
      total: optionalNumber(row.market_total),
      impliedPoints: optionalNumber(isHome ? row.market_home_implied : row.market_away_implied),
      observedAt: dateText(row.market_at), snapshotId: number(row.market_id),
    } : row.archive_id != null ? {
      source: "archive" as const,
      moneyline: isHome ? archivedHomeMoneyline : archivedAwayMoneyline,
      winProbability: null,
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

/** One team's season-to-date scrimmage totals, offense and defense, for ranking. */
export type NflIdentityTotals = {
  team: string;
  games: number;
  offensePlays: number;
  offenseEpaSum: number;
  offenseEpaCount: number;
  offenseExplosives: number;
  passOeSum: number;
  passOeCount: number;
  drives: number;
  touchdownDrives: number;
  threeAndOutDrives: number;
  defensePlays: number;
  defenseEpaSum: number;
  defenseEpaCount: number;
};

/**
 * Every team's regular-season totals from completed games before `beforeWeek`
 * that kicked off, and were labelled, before `cutoffIso` -- what was knowable
 * before a slate. One pass over the season's plays, grouped by team, using the
 * same definitions as getNflIdentityGames (scrimmage = run/pass; drives counted
 * once per game and drive where drive_archetype is set), so ranks built from
 * this agree with the Team Identity page. Play-by-play codes are mapped back to
 * nfl_teams codes through PBP_TEAM_CODES (LA -> LAR, WAS -> WSH).
 */
/** SQL mapping a play-by-play team column back to nfl_teams codes, from PBP_TEAM_CODES. */
function toTeamCode(column: "p.posteam" | "p.defteam") {
  const whens = Object.entries(PBP_TEAM_CODES).map(([team, pbp]) => `WHEN '${pbp}' THEN '${team}'`).join(" ");
  return sql.raw(`CASE ${column} ${whens} ELSE ${column} END`);
}

export async function getNflIdentityTotals(season: number, beforeWeek: number, cutoffIso: string): Promise<NflIdentityTotals[]> {
  const result = await db.execute(sql`
    WITH base AS (
      SELECT p.game_id, p.play_type, p.epa, p.yards_gained, p.pass_oe, p.drive, p.drive_archetype,
             ${toTeamCode("p.posteam")} AS posteam,
             ${toTeamCode("p.defteam")} AS defteam
      FROM nfl_pbp_archetypes p
      JOIN nfl_season_games g ON g.nflverse_game_id = p.game_id
      WHERE p.season = ${season} AND p.season_type = 'REG' AND p.week < ${beforeWeek}
        AND g.game_type = 'REG' AND g.completed = TRUE
        AND g.kickoff < ${cutoffIso}::timestamptz AND p.labelled_at < ${cutoffIso}::timestamptz
    ),
    offense AS (
      SELECT posteam AS team,
             COUNT(DISTINCT game_id)::int AS games,
             COUNT(*) FILTER (WHERE play_type IN ('run','pass'))::int AS offense_plays,
             COALESCE(SUM(epa) FILTER (WHERE play_type IN ('run','pass')),0) AS offense_epa_sum,
             COUNT(epa) FILTER (WHERE play_type IN ('run','pass'))::int AS offense_epa_count,
             COUNT(*) FILTER (WHERE play_type IN ('run','pass') AND yards_gained >= 20)::int AS offense_explosives,
             COALESCE(SUM(pass_oe) FILTER (WHERE play_type IN ('run','pass')),0) AS pass_oe_sum,
             COUNT(pass_oe) FILTER (WHERE play_type IN ('run','pass'))::int AS pass_oe_count,
             COUNT(DISTINCT (game_id, drive)) FILTER (WHERE drive_archetype IS NOT NULL)::int AS drives,
             COUNT(DISTINCT (game_id, drive)) FILTER (WHERE drive_archetype = 'TOUCHDOWN')::int AS touchdown_drives,
             COUNT(DISTINCT (game_id, drive)) FILTER (WHERE drive_archetype = 'THREE_AND_OUT')::int AS three_and_out_drives
      FROM base WHERE posteam IS NOT NULL GROUP BY posteam
    ),
    defense AS (
      SELECT defteam AS team,
             COUNT(*) FILTER (WHERE play_type IN ('run','pass'))::int AS defense_plays,
             COALESCE(SUM(epa) FILTER (WHERE play_type IN ('run','pass')),0) AS defense_epa_sum,
             COUNT(epa) FILTER (WHERE play_type IN ('run','pass'))::int AS defense_epa_count
      FROM base WHERE defteam IS NOT NULL GROUP BY defteam
    )
    SELECT COALESCE(o.team, d.team) AS team, o.games, o.offense_plays, o.offense_epa_sum, o.offense_epa_count,
           o.offense_explosives, o.pass_oe_sum, o.pass_oe_count, o.drives, o.touchdown_drives,
           o.three_and_out_drives, d.defense_plays, d.defense_epa_sum, d.defense_epa_count
    FROM offense o FULL JOIN defense d ON d.team = o.team
    ORDER BY 1`);
  return resultRows(result).map(row => ({
    team: String(row.team), games: number(row.games),
    offensePlays: number(row.offense_plays), offenseEpaSum: number(row.offense_epa_sum),
    offenseEpaCount: number(row.offense_epa_count), offenseExplosives: number(row.offense_explosives),
    passOeSum: number(row.pass_oe_sum), passOeCount: number(row.pass_oe_count),
    drives: number(row.drives), touchdownDrives: number(row.touchdown_drives),
    threeAndOutDrives: number(row.three_and_out_drives),
    defensePlays: number(row.defense_plays), defenseEpaSum: number(row.defense_epa_sum),
    defenseEpaCount: number(row.defense_epa_count),
  }));
}
