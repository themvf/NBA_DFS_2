import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { grade, type StudyGame } from "@/lib/nba-market-studies";
import octoberCapture from "@/data/nba-market-studies-october-25.json";

export async function getNbaMarketStudyGames(through: string) {
  const results = await db.execute(sql`
    WITH original_slates AS (
      SELECT s.game_date, s.snapshot
      FROM nba_ghost_snapshots s
      WHERE s.game_date BETWEEN '2026-10-20' AND ${through}::date
        AND s.version = 'nba-points-ghost-v2' AND s.mode = 'prospective'
    ), decision_cutoffs AS (
      SELECT s.game_date, min((game->>'start_time')::timestamptz) - interval '30 minutes' AS cutoff
      FROM original_slates s
      CROSS JOIN LATERAL jsonb_array_elements(s.snapshot->'slate_schedule') game
      GROUP BY s.game_date, s.snapshot->>'lock_at'
      HAVING count(game->>'start_time') = count(*)
        AND min((game->>'start_time')::timestamptz) = (s.snapshot->>'lock_at')::timestamptz
    ), schedule_observations AS (
      SELECT g.espn_event_id::text AS id, g.game_date, g.home_team, g.away_team, g.start_time,
             CASE WHEN g.completed THEN g.home_score END AS home_score,
             CASE WHEN g.completed THEN g.away_score END AS away_score, 0 AS priority
      FROM nba_box_score_games g
      WHERE g.game_date BETWEEN '2026-10-20' AND ${through}::date
      UNION ALL
      SELECT game->>'event_id', s.game_date, game->>'home_team', game->>'away_team',
             (game->>'start_time')::timestamptz, NULL, NULL, 1
      FROM original_slates s
      CROSS JOIN LATERAL jsonb_array_elements(s.snapshot->'slate_schedule') game
      WHERE game->>'event_id' IS NOT NULL
    ), schedule AS (
      SELECT DISTINCT ON (id) id, game_date, start_time, home_score, away_score,
             replace(home_team, 'Los Angeles Clippers', 'LA Clippers') AS home_team,
             replace(away_team, 'Los Angeles Clippers', 'LA Clippers') AS away_team
      FROM schedule_observations
      ORDER BY id, priority, start_time
    ), slate AS (
      SELECT schedule.*, decision_cutoffs.cutoff,
             count(*) OVER (PARTITION BY schedule.game_date, home_team, away_team) AS matchup_count
      FROM schedule LEFT JOIN decision_cutoffs USING (game_date)
    ), eligible_quotes AS (
      SELECT s.id AS game_id, l.id, l.event_id, l.captured_at, l.commence_time, l.books
      FROM slate s
      JOIN nba_game_lines l ON l.game_date = s.game_date
       AND replace(l.home_team, 'Los Angeles Clippers', 'LA Clippers') = s.home_team
       AND replace(l.away_team, 'Los Angeles Clippers', 'LA Clippers') = s.away_team
       AND l.captured_at < least(s.start_time, l.commence_time, now())
      WHERE s.matchup_count = 1 AND s.start_time IS NOT NULL AND l.commence_time IS NOT NULL
    ), quotes AS (
      SELECT game_id, jsonb_agg(jsonb_build_object(
         'id', id::text, 'observedAt', captured_at, 'marketStart', commence_time, 'books', books
      ) ORDER BY captured_at, id) AS captures
      FROM eligible_quotes GROUP BY game_id HAVING count(DISTINCT event_id) = 1
    )
    SELECT s.id, s.game_date::text AS date, s.home_team AS home, s.away_team AS away,
           s.start_time::text AS start, s.cutoff::text, s.home_score::integer AS "homeScore", s.away_score::integer AS "awayScore",
           prev_home.venue IS NOT NULL AS "homeBackToBack", prev_away.venue IS NOT NULL AS "awayBackToBack",
           prev_home.venue AS "homePriorVenue", prev_away.venue AS "awayPriorVenue",
           coalesce(q.captures, '[]'::jsonb) AS captures
    FROM slate s
    LEFT JOIN quotes q ON q.game_id = s.id
    LEFT JOIN LATERAL (
      SELECT home_team AS venue FROM schedule
      WHERE game_date = s.game_date - 1 AND home_score IS NOT NULL AND away_score IS NOT NULL
        AND s.home_team IN (home_team, away_team)
      GROUP BY home_team
      HAVING count(*) = 1
    ) prev_home ON true
    LEFT JOIN LATERAL (
      SELECT home_team AS venue FROM schedule
      WHERE game_date = s.game_date - 1 AND home_score IS NOT NULL AND away_score IS NOT NULL
        AND s.away_team IN (home_team, away_team)
      GROUP BY home_team
      HAVING count(*) = 1
    ) prev_away ON true
    ORDER BY s.game_date DESC, s.start_time, s.id
  `);
  const games = results.rows as StudyGame[];
  const team = (name: string) => name === "Los Angeles Clippers" ? "LA Clippers" : name;
  for (const source of octoberCapture) {
    const matches = games.filter(g => g.date === "2026-10-25" && g.home === team(source.home) && g.away === team(source.away));
    if (matches.length !== 1) continue;
    const game = matches[0];
    if (!game.start || game.captures.length || Date.parse(source.capture.observedAt) >=
        Math.min(Date.parse(game.start), Date.parse(source.capture.marketStart))) continue;
    game.captures = [source.capture];
  }
  return games.map(grade);
}
