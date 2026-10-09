import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { buildQbMatchupContexts, type QbContextMarket, type QbContextPlay, type QbMatchupContext } from "@/lib/nfl-dfs/qb-matchup-context";

const TEAM_ALIASES: Record<string, string> = { LA: "LAR", WAS: "WSH", JAC: "JAX", AZ: "ARI" };
const canonicalTeam = (team: unknown) => TEAM_ALIASES[String(team)] ?? String(team);
const optionalNumber = (value: unknown) => value == null ? null : Number(value);
const dateText = (value: unknown) => value instanceof Date ? value.toISOString() : String(value);

/** All evidence is bounded by the saved projection decision time. */
export async function readNflQbMatchupContexts(season: number, week: number, asOf: Date): Promise<Map<string, QbMatchupContext>> {
  if (!Number.isInteger(season) || !Number.isInteger(week) || week < 2 || !Number.isFinite(asOf.getTime())) return new Map();
  const [playResult, marketResult] = await Promise.all([
    db.execute(sql`
      SELECT p.game_id AS "gameId", p.posteam, p.defteam, p.play_type AS "playType",
        p.qb_dropback AS "qbDropback", p.score_differential AS "scoreDifferential",
        p.epa, p.drive, p.drive_archetype AS "driveArchetype"
      FROM nfl_pbp_archetypes p
      JOIN nfl_season_games g ON g.nflverse_game_id = p.game_id
        AND g.season = p.season AND g.week = p.week AND g.game_type = 'REG'
      JOIN nfl_teams ht ON ht.team_id = g.home_team_id
      JOIN nfl_teams at ON at.team_id = g.away_team_id
      WHERE p.season = ${season} AND p.season_type = 'REG' AND p.week < ${week}
        AND g.completed = TRUE AND g.kickoff < ${asOf} AND p.labelled_at <= ${asOf}
        AND (CASE p.home_team WHEN 'LA' THEN 'LAR' WHEN 'WAS' THEN 'WSH' WHEN 'JAC' THEN 'JAX' WHEN 'AZ' THEN 'ARI' ELSE p.home_team END) = ht.abbreviation
        AND (CASE p.away_team WHEN 'LA' THEN 'LAR' WHEN 'WAS' THEN 'WSH' WHEN 'JAC' THEN 'JAX' WHEN 'AZ' THEN 'ARI' ELSE p.away_team END) = at.abbreviation
        AND p.posteam IS NOT NULL AND p.defteam IS NOT NULL`),
    db.execute(sql`
      SELECT g.nflverse_game_id AS "gameId", ht.abbreviation AS home, at.abbreviation AS away,
        g.kickoff, odds.id AS "oddsId", odds.captured_at AS "oddsCapturedAt",
        odds.bookmaker_count AS "bookmakerCount", odds.home_spread AS "homeSpread", odds.vegas_total AS total
      FROM nfl_season_games g
      JOIN nfl_teams ht ON ht.team_id = g.home_team_id
      JOIN nfl_teams at ON at.team_id = g.away_team_id
      LEFT JOIN LATERAL (
        SELECT h.id, h.captured_at, h.bookmaker_count, h.home_spread, h.vegas_total
        FROM game_odds_history h
        WHERE h.sport = 'nfl' AND h.matchup_id = g.matchup_id
          AND h.captured_at <= ${asOf} AND h.captured_at < g.kickoff
        ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
      ) odds ON TRUE
      WHERE g.season = ${season} AND g.week = ${week} AND g.game_type = 'REG'
        AND g.nflverse_game_id IS NOT NULL AND g.kickoff > ${asOf}`),
  ]);
  const plays: QbContextPlay[] = playResult.rows.map(raw => {
    const row = raw as Record<string, unknown>;
    return { gameId: String(row.gameId), posteam: canonicalTeam(row.posteam), defteam: canonicalTeam(row.defteam),
      playType: row.playType == null ? null : String(row.playType), qbDropback: row.qbDropback === true,
      scoreDifferential: optionalNumber(row.scoreDifferential), epa: optionalNumber(row.epa),
      drive: optionalNumber(row.drive), driveArchetype: row.driveArchetype == null ? null : String(row.driveArchetype) };
  });
  const markets: QbContextMarket[] = marketResult.rows.map(raw => {
    const row = raw as Record<string, unknown>;
    return { gameId: String(row.gameId), home: canonicalTeam(row.home), away: canonicalTeam(row.away),
      kickoff: dateText(row.kickoff), oddsId: optionalNumber(row.oddsId),
      oddsCapturedAt: row.oddsCapturedAt == null ? null : dateText(row.oddsCapturedAt),
      bookmakerCount: optionalNumber(row.bookmakerCount), homeSpread: optionalNumber(row.homeSpread), total: optionalNumber(row.total) };
  });
  return new Map(buildQbMatchupContexts(plays, markets, asOf.toISOString()).map(context => [context.team, context]));
}
