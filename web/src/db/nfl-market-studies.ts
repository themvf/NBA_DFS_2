import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { gradeGame, type Game } from "@/lib/nfl/market-studies";

export async function getNflMarketStudyGames(season: number) {
  const asOf = Date.now();
  const rows = await db.execute(sql`
    WITH schedule AS (
      SELECT g.*,
             count(*) OVER (PARTITION BY g.matchup_id) AS matchup_uses
      FROM nfl_season_games g
      WHERE g.season = ${season} AND g.game_type = 'REG'
    )
    SELECT g.nflverse_game_id AS id, g.week, g.gameday::text AS date,
           g.kickoff::text AS kickoff,
           CASE WHEN m.id IS NOT NULL AND g.kickoff IS NOT NULL
                THEN least(g.kickoff, m.commence_time, frozen.boundary_at)::text END AS boundary,
           ht.abbreviation AS home, at.abbreviation AS away,
           g.home_rest AS "homeRest", g.away_rest AS "awayRest",
           CASE WHEN g.completed THEN g.home_score END AS "homeScore",
           CASE WHEN g.completed THEN g.away_score END AS "awayScore",
           CASE WHEN g.completed AND m.completed
                  AND (g.home_score, g.away_score) = (m.home_score, m.away_score)
                THEN m.final_at::text END AS "finalObservedAt",
           coalesce(q.captures, '[]'::jsonb) AS captures
    FROM schedule g
    JOIN nfl_teams ht ON ht.team_id = g.home_team_id
    JOIN nfl_teams at ON at.team_id = g.away_team_id
    LEFT JOIN nfl_matchups m ON m.id = g.matchup_id AND g.matchup_uses = 1
      AND m.season = g.season AND m.week = g.week
      AND m.game_date = g.gameday
      AND m.home_team_id = g.home_team_id AND m.away_team_id = g.away_team_id
    LEFT JOIN event_closing_lines frozen ON frozen.sport = 'nfl'
      AND frozen.matchup_id = m.id AND frozen.event_id = m.event_id
    LEFT JOIN LATERAL (
      SELECT jsonb_agg(jsonb_build_object('id', h.id::text, 'observedAt', h.captured_at, 'books', h.books)
                       ORDER BY h.captured_at, h.id) AS captures
      FROM game_odds_history h
      WHERE h.sport = 'nfl' AND h.matchup_id = m.id AND h.event_id = m.event_id
        AND h.game_date = g.gameday
        AND h.home_team_id = g.home_team_id AND h.away_team_id = g.away_team_id
        AND g.kickoff IS NOT NULL
        AND h.captured_at < least(g.kickoff, m.commence_time, frozen.boundary_at, now())
        AND jsonb_typeof(h.books) = 'object' AND h.books ? 'pinnacle'
    ) q ON true
    ORDER BY g.week, g.kickoff, id
  `);
  return (rows.rows as Game[]).map(game => gradeGame(game, asOf));
}
