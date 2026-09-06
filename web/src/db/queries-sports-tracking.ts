import "server-only";
import { sql } from "drizzle-orm";
import { db } from "./index";
import { TRACKING_SIGNALS, type TrackingFilters, type TrackingGroup, type TrackingEntry } from "@/lib/sports-tracking";
/** Read original prospective game-line triggers once, not repeated movement observations. */
export async function getSportsTracking(f:TrackingFilters):Promise<{groups:TrackingGroup[];entries:TrackingEntry[]}> {
  const rows=await db.execute(sql`
    WITH source AS (
      SELECT id,sport,game_date,matchup_id,matchup,alert_type,side,created_at,details_json,grading_json,
        CASE WHEN outcome='void' AND grading_json->>'settlement_reason'='push' THEN 'push'
             WHEN outcome IN ('push','draw','won','lost','void') THEN outcome
             WHEN outcome IS NULL THEN 'pending' ELSE 'unavailable' END AS result,
        CASE WHEN details_json->>'market' IN ('spread','run_line') THEN 'spread'
             WHEN details_json->>'market' IN ('total','total_games') OR alert_type LIKE 'total_%' OR alert_type LIKE 'mlb_total_%' THEN 'total'
             WHEN alert_type LIKE 'spread_%' OR alert_type LIKE 'mlb_run_line_%' THEN 'spread'
             ELSE 'moneyline' END AS market,
        CONCAT(COALESCE(details_json->>'signal_version',details_json->>'detector_version',details_json->>'program_version','legacy'),
          CASE WHEN details_json ? 'capture_policy' THEN ' / ' || (details_json->>'capture_policy') ELSE '' END) AS version,
        CASE WHEN jsonb_typeof(details_json->'dk_decimal')='number' THEN (details_json->>'dk_decimal')::double precision
             WHEN jsonb_typeof(details_json->'exec_decimal')='number' THEN (details_json->>'exec_decimal')::double precision END AS entry_decimal
      FROM line_alerts WHERE origin='prospective' AND sport IN ('mlb','nfl','cfb','tennis')
        AND game_date BETWEEN ${f.from}::date AND ${f.to}::date
        AND alert_type IN (${sql.join(TRACKING_SIGNALS.map(s=>sql`${s}`),sql`, `)})
        ${f.sport==='all'?sql``:sql`AND sport=${f.sport}`}
        ${f.signal==='all'?sql``:sql`AND alert_type=${f.signal}`}
    ), filtered AS (
      SELECT *,CASE WHEN entry_decimal>1 THEN CASE WHEN result='won' THEN entry_decimal-1 WHEN result='lost' THEN -1 WHEN result='push' THEN 0 END END AS units
      FROM source WHERE ${f.result==='all'?sql`TRUE`:sql`result=${f.result}`}
    ), grouped AS (
      SELECT sport,alert_type AS signal,market,version,COUNT(*)::int AS total,
        COUNT(*) FILTER(WHERE result='won')::int AS wins,COUNT(*) FILTER(WHERE result='lost')::int AS losses,
        COUNT(*) FILTER(WHERE result='push')::int AS pushes,COUNT(*) FILTER(WHERE result='draw')::int AS draws,
        COUNT(*) FILTER(WHERE result='void')::int AS voids,COUNT(*) FILTER(WHERE result='pending')::int AS pending,
        COUNT(*) FILTER(WHERE result='unavailable')::int AS unavailable,SUM(units) AS units,COUNT(units)::int AS priced,
        COUNT(DISTINCT matchup_id)::int AS events
      FROM filtered GROUP BY sport,alert_type,market,version ORDER BY sport,alert_type,market,version
    ), ledger AS (
      SELECT id,sport,game_date::text AS date,matchup,alert_type AS signal,market,version,side,result,
        grading_json->>'settlement_reason' AS reason,created_at::text AS "observedAt",
        CASE WHEN jsonb_typeof(details_json->'exec_line')='number' THEN (details_json->>'exec_line')::double precision END AS "entryLine",
        CASE WHEN jsonb_typeof(details_json->'dk_odds')='number' THEN (details_json->>'dk_odds')::double precision
             WHEN entry_decimal>1 THEN CASE WHEN entry_decimal>=2 THEN ROUND((entry_decimal-1)*100) ELSE ROUND(-100/(entry_decimal-1)) END END AS "entryPrice",units
      FROM filtered ORDER BY created_at DESC,id DESC LIMIT 100 OFFSET ${(f.page-1)*100}
    ) SELECT COALESCE((SELECT jsonb_agg(grouped) FROM grouped),'[]'::jsonb) AS groups,
             COALESCE((SELECT jsonb_agg(ledger) FROM ledger),'[]'::jsonb) AS entries
  `);
  const result=rows.rows[0]; return {groups:result.groups as TrackingGroup[],entries:result.entries as TrackingEntry[]};
}
