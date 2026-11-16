import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { queryRows } from "./query-result";
import type { CfbStudySelection } from "@/lib/daily-study-matches";

/** Original pregame selections and independently joined game outcomes. */
export async function getCfbStudySelections(date: string) {
  return queryRows<CfbStudySelection>(await db.execute(sql`
    SELECT m.id::text AS id, s.trigger, (m.commence_time AT TIME ZONE 'America/New_York')::date::text AS date,
           m.commence_time::text AS start, s.frozen_at::text AS "frozenAt", ht.name AS home, at.name AS away,
           s.payload->>'selection' AS selection, (s.payload->>'home_spread')::float8 AS spread,
           (s.payload->>'vegas_total')::float8 AS total, (s.payload->>'home_ml')::integer AS "homeMl",
           (s.payload->>'away_ml')::integer AS "awayMl", (s.payload->>'to_edge')::float8 AS "toEdge",
           CASE WHEN m.completed THEN m.home_score END AS "homeScore",
           CASE WHEN m.completed THEN m.away_score END AS "awayScore",
           CASE WHEN m.completed THEN COALESCE(m.final_at, m.score_fetched_at)::text END AS "finalObservedAt"
    FROM cfb_pattern_selections s
    JOIN cfb_matchups m ON m.id = s.matchup_id AND m.commence_time = s.commence_time
    JOIN cfb_teams ht ON ht.team_id = m.home_team_id AND ht.name = s.payload->>'home'
    JOIN cfb_teams at ON at.team_id = m.away_team_id AND at.name = s.payload->>'away'
    WHERE (m.commence_time AT TIME ZONE 'America/New_York')::date <= ${date}::date
      AND s.payload->>'study_version' = 'cfb-pattern-watch-v2'
      AND s.frozen_at < LEAST(m.commence_time, now())
      AND (s.payload->>'captured_at')::timestamptz <= s.frozen_at
      AND s.payload->>'selection' IS NOT NULL AND s.payload->>'home_spread' IS NOT NULL
      AND s.payload->>'home_ml' IS NOT NULL AND s.payload->>'away_ml' IS NOT NULL
      AND (s.trigger <> 'T4_big_fav_over' OR s.payload->>'vegas_total' IS NOT NULL)
      AND (s.trigger <> 'T7_turnover_margin_fade' OR s.payload->>'to_edge' IS NOT NULL)
      AND (s.trigger <> 'T5_sat_evening_g5_fav_ml' OR (
        extract(isodow FROM m.commence_time AT TIME ZONE 'America/New_York') = 6
        AND (m.commence_time AT TIME ZONE 'America/New_York')::time >= time '17:00'
        AND (m.commence_time AT TIME ZONE 'America/New_York')::time < time '20:30'))
    ORDER BY m.commence_time, m.id, s.trigger
  `));
}
