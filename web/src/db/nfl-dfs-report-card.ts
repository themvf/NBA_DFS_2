import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { WeeklyReport } from "@/lib/nfl-dfs/report-card";

export async function getNflWeeklyReports(season: number, week?: number): Promise<{ reports: WeeklyReport[]; weeks: number[] }> {
  // No DDL or grading in a browser request. Reports are persisted by the daily job.
  const weeks = await db.execute(sql`SELECT DISTINCT week FROM nfl_dfs_weekly_report_cards WHERE season=${season} ORDER BY week`);
  const available = weeks.rows.map(r => Number(r.week));
  const target = week ?? available.at(-1);
  if (!target) return { reports: [], weeks: available };
  const rows = await db.execute(sql`SELECT payload FROM nfl_dfs_weekly_report_cards
    WHERE season=${season} AND week=${target} ORDER BY created_at DESC,report_digest DESC LIMIT 1`);
  return { reports: rows.rows.map(row => row.payload as WeeklyReport), weeks: available };
}

export async function getNflPlayerReviewHistory(season: number, playerId: number, variant: string) {
  const rows = await db.execute(sql`WITH latest AS (
    SELECT DISTINCT ON (week) week,payload FROM nfl_dfs_weekly_report_cards
    WHERE season=${season} ORDER BY week,created_at DESC,report_digest DESC
  ) SELECT l.week,(r->'forecast'->>'mean')::double precision expected,
    (r->'forecast'->>'p10')::double precision "P10",(r->'forecast'->>'p90')::double precision "P90",
    (r->>'actual')::double precision actual
    FROM latest l CROSS JOIN LATERAL jsonb_array_elements(l.payload->'rows') r
    WHERE (r->>'player_id')::bigint=${playerId} AND r->>'variant'=${variant} ORDER BY l.week`);
  return rows.rows as { week: number; expected: number | null; P10: number | null; P90: number | null; actual: number | null }[];
}
