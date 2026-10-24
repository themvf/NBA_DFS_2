import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";

/**
 * The nightly grade of the books' points lines (model/nba_points_calibration.py):
 * each game's last pre-tip capture beside the box score. Read-only here; the
 * table is written by grade_nba_points_props.yml.
 */

export const NBA_POINTS_GRADE_VERSION = "nba-points-grade-v2";

export type CalibrationCell = {
  /** "over" (the consensus line) or "20+" ... "40+" (the ladder). */
  key: string;
  n: number;
  predicted: number;
  observed: number;
  brier: number;
};

export type CalibrationSummary = {
  gradeVersion: string;
  firstDate: string | null;
  lastDate: string | null;
  nights: number;
  games: number;
  rows: number;
  graded: number;
  dnp: number;
  unmatched: number;
  noBoxScore: number;
  /** Share of graded rows whose actual beat the floor (target 0.90) and the ceiling (target 0.10). */
  floorBeatRate: number | null;
  floorN: number;
  ceilingBeatRate: number | null;
  ceilingN: number;
  /** Mean minutes between the graded capture and tip-off. */
  meanMinutesBeforeTip: number | null;
  cells: CalibrationCell[];
  lastGradedAt: string | null;
};

const num = (v: unknown) => (v == null ? null : Number(v));
const day = (v: unknown) => (v == null ? null : v instanceof Date ? v.toISOString().slice(0, 10) : String(v).slice(0, 10));
const iso = (v: unknown) => (v == null ? null : v instanceof Date ? v.toISOString() : String(v));

const CELLS: { key: string; p: string; hit: string }[] = [
  { key: "over", p: "p_over", hit: "actual_points > line" },
  { key: "20+", p: "p20", hit: "actual_points >= 20" },
  { key: "25+", p: "p25", hit: "actual_points >= 25" },
  { key: "30+", p: "p30", hit: "actual_points >= 30" },
  { key: "35+", p: "p35", hit: "actual_points >= 35" },
  { key: "40+", p: "p40", hit: "actual_points >= 40" },
];

/**
 * Pooled over every graded night (or one night with `gameDate`). Null when the
 * table does not exist yet (the first grade creates it) or holds no rows.
 */
export async function getNbaPointsCalibration(gameDate?: string, gradeVersion?: string): Promise<CalibrationSummary | null> {
  // One whole version at a time. Retain v1 until v2 has been populated; never
  // pool revised and original grades or silently discard the original record.
  const version = gradeVersion ? sql`${gradeVersion}` : sql`coalesce((
    SELECT grade_version FROM nba_points_calibration
    WHERE grade_version IN (${NBA_POINTS_GRADE_VERSION}, 'nba-points-grade-v1')
    ORDER BY grade_version DESC LIMIT 1
  ), ${NBA_POINTS_GRADE_VERSION})`;
  const where = gameDate
    ? sql`grade_version = ${version} AND game_date = ${gameDate}::date`
    : sql`grade_version = ${version}`;
  const cellSql = CELLS.map(
    (c) => sql`
      count(*) FILTER (WHERE status = 'graded' AND ${sql.raw(c.p)} IS NOT NULL) AS ${sql.raw(`${c.p}_n`)},
      avg(${sql.raw(c.p)}) FILTER (WHERE status = 'graded' AND ${sql.raw(c.p)} IS NOT NULL) AS ${sql.raw(`${c.p}_pred`)},
      avg(CASE WHEN ${sql.raw(c.hit)} THEN 1.0 ELSE 0.0 END) FILTER (WHERE status = 'graded' AND ${sql.raw(c.p)} IS NOT NULL) AS ${sql.raw(`${c.p}_obs`)},
      avg(power(${sql.raw(c.p)} - CASE WHEN ${sql.raw(c.hit)} THEN 1.0 ELSE 0.0 END, 2)) FILTER (WHERE status = 'graded' AND ${sql.raw(c.p)} IS NOT NULL) AS ${sql.raw(`${c.p}_brier`)}`,
  );
  let rows: Record<string, unknown>[];
  try {
    const result = await db.execute(sql`
      SELECT
        min(grade_version) AS grade_version,
        min(game_date) AS first_date, max(game_date) AS last_date,
        count(DISTINCT game_date) AS nights, count(DISTINCT event_id) AS games, count(*) AS rows,
        count(*) FILTER (WHERE status = 'graded') AS graded,
        count(*) FILTER (WHERE status = 'dnp') AS dnp,
        count(*) FILTER (WHERE status IN ('unmatched', 'ambiguous')) AS unmatched,
        count(*) FILTER (WHERE status = 'no_box_score') AS no_box_score,
        avg(CASE WHEN actual_points > floor THEN 1.0 ELSE 0.0 END) FILTER (WHERE status = 'graded' AND floor IS NOT NULL) AS floor_beat,
        count(*) FILTER (WHERE status = 'graded' AND floor IS NOT NULL) AS floor_n,
        avg(CASE WHEN actual_points > ceiling THEN 1.0 ELSE 0.0 END) FILTER (WHERE status = 'graded' AND ceiling IS NOT NULL) AS ceiling_beat,
        count(*) FILTER (WHERE status = 'graded' AND ceiling IS NOT NULL) AS ceiling_n,
        avg(minutes_before_tip) AS mean_minutes,
        max(graded_at) AS last_graded_at,
        ${sql.join(cellSql, sql`,`)}
      FROM nba_points_calibration
      WHERE ${where}`);
    rows = result.rows as Record<string, unknown>[];
  } catch (error) {
    // Before the first grade the table does not exist; the page says so rather than failing.
    if (/relation .* does not exist/i.test(String(error))) return null;
    throw error;
  }
  const r = rows[0];
  if (!r || Number(r.rows) === 0) return null;
  return {
    gradeVersion: String(r.grade_version),
    firstDate: day(r.first_date),
    lastDate: day(r.last_date),
    nights: Number(r.nights),
    games: Number(r.games),
    rows: Number(r.rows),
    graded: Number(r.graded),
    dnp: Number(r.dnp),
    unmatched: Number(r.unmatched),
    noBoxScore: Number(r.no_box_score),
    floorBeatRate: num(r.floor_beat),
    floorN: Number(r.floor_n),
    ceilingBeatRate: num(r.ceiling_beat),
    ceilingN: Number(r.ceiling_n),
    meanMinutesBeforeTip: num(r.mean_minutes),
    lastGradedAt: iso(r.last_graded_at),
    cells: CELLS.map((c) => ({
      key: c.key,
      n: Number(r[`${c.p}_n`] ?? 0),
      predicted: Number(r[`${c.p}_pred`] ?? 0),
      observed: Number(r[`${c.p}_obs`] ?? 0),
      brier: Number(r[`${c.p}_brier`] ?? 0),
    })),
  };
}
