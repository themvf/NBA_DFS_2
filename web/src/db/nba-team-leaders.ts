import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { NBA_POINTS_GRADE_VERSION } from "@/db/nba-points-calibration";
import type { TeamLeaderGame } from "@/lib/nba-game-leaders";

/**
 * This season's record of each team's biggest points line, from the nightly grade
 * (nba_points_calibration, each game's last pre-tip capture) and ESPN box scores:
 * one row per completed team-game before `gameDate`, with the team's biggest line
 * and his share of being his team's top scorer (1, 1/ties, or 0; a player who sat
 * scores 0). Season starts August 1. Empty before the first grade.
 */
export async function getNbaTeamLeaderGames(gameDate: string): Promise<TeamLeaderGame[]> {
  try {
    const result = await db.execute(sql`
      WITH lined AS (
        SELECT c.espn_event_id, c.player, c.player_key, c.line, b.team
        FROM nba_points_calibration c
        JOIN nba_box_score_games g ON g.espn_event_id = c.espn_event_id AND g.completed
        JOIN nba_box_score_players b ON b.espn_event_id = c.espn_event_id AND b.player_key = c.player_key
        WHERE c.grade_version = ${NBA_POINTS_GRADE_VERSION}
          AND c.game_date < ${gameDate}::date
          AND c.game_date >= (date_trunc('year', ${gameDate}::date - interval '7 months') + interval '7 months')::date
      ), top AS (
        SELECT DISTINCT ON (espn_event_id, team) * FROM lined ORDER BY espn_event_id, team, line DESC, player_key
      ), best AS (
        SELECT espn_event_id, team, MAX(points) AS top_pts
        FROM nba_box_score_players WHERE played GROUP BY 1, 2
      )
      SELECT t.team, t.player, t.player_key,
        CASE WHEN p.played AND p.points = s.top_pts THEN 1.0 / (
          SELECT COUNT(*) FROM nba_box_score_players q
          WHERE q.espn_event_id = t.espn_event_id AND q.team = t.team AND q.played AND q.points = s.top_pts
        ) ELSE 0 END AS credit
      FROM top t
      JOIN best s USING (espn_event_id, team)
      JOIN nba_box_score_players p ON p.espn_event_id = t.espn_event_id AND p.player_key = t.player_key`);
    return (result.rows as Record<string, unknown>[]).map((r) => ({
      team: String(r.team),
      player: String(r.player),
      playerKey: String(r.player_key),
      credit: Number(r.credit),
    }));
  } catch (error) {
    if (/relation .* does not exist/i.test(String(error))) return [];
    throw error;
  }
}
