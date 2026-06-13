"use server";

import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { MlbHitterProjectionCalibration } from "./mlb-projection-utils";

type CalibrationEntry = {
  factor: number;
  n: number;
};

let mlbHitterProjectionCalibrationCache: {
  loadedAtMs: number;
  calibration: MlbHitterProjectionCalibration;
} | null = null;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function sanitizeProjection(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? Math.max(0, value) : null;
}

function factorFromAverages(
  avgProj: number | null | undefined,
  avgActual: number | null | undefined,
  minFactor: number,
  maxFactor: number,
): number | null {
  if (avgProj == null || avgActual == null || !Number.isFinite(avgProj) || !Number.isFinite(avgActual) || avgProj <= 0) {
    return null;
  }
  return clamp(avgActual / avgProj, minFactor, maxFactor);
}

export async function loadMlbHitterProjectionCalibration(): Promise<MlbHitterProjectionCalibration> {
  const now = Date.now();
  if (mlbHitterProjectionCalibrationCache && now - mlbHitterProjectionCalibrationCache.loadedAtMs < 15 * 60 * 1000) {
    return mlbHitterProjectionCalibrationCache.calibration;
  }

  const [orderRows, overallAgg, impliedRows] = await Promise.all([
    // Per batting-order calibration
    db.execute<{
      bucket: string;
      n: number;
      avgProj: number | null;
      avgActual: number | null;
    }>(sql`
      SELECT
        CASE
          WHEN dp.dk_team_lineup_confirmed = true
            AND dp.dk_starting_lineup_order BETWEEN 1 AND 9
            THEN dp.dk_starting_lineup_order::text
          ELSE 'pending'
        END AS bucket,
        COUNT(*)::int AS "n",
        AVG(dp.our_proj) AS "avgProj",
        AVG(dp.actual_fpts) AS "avgActual"
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE ds.sport = 'mlb'
        AND dp.actual_fpts IS NOT NULL
        AND dp.our_proj IS NOT NULL
        AND COALESCE(dp.is_out, false) = false
        AND dp.eligible_positions NOT LIKE '%SP%'
        AND dp.eligible_positions NOT LIKE '%RP%'
        AND ds.slate_date >= CURRENT_DATE - INTERVAL '45 days'
      GROUP BY 1
    `),

    // Overall calibration
    db.execute<{
      n: number;
      avgProj: number | null;
      avgActual: number | null;
    }>(sql`
      SELECT
        COUNT(*)::int AS "n",
        AVG(dp.our_proj) AS "avgProj",
        AVG(dp.actual_fpts) AS "avgActual"
      FROM dk_players dp
      JOIN dk_slates ds ON ds.id = dp.slate_id
      WHERE ds.sport = 'mlb'
        AND dp.actual_fpts IS NOT NULL
        AND dp.our_proj IS NOT NULL
        AND COALESCE(dp.is_out, false) = false
        AND dp.eligible_positions NOT LIKE '%SP%'
        AND dp.eligible_positions NOT LIKE '%RP%'
        AND ds.slate_date >= CURRENT_DATE - INTERVAL '45 days'
    `),

    // Per implied-total-bucket calibration (residual correction after batting-order calibration)
    db.execute<{
      impliedBucket: string;
      n: number;
      avgProj: number | null;
      avgActual: number | null;
    }>(sql`
      WITH player_implied AS (
        SELECT
          dp.our_proj,
          dp.actual_fpts,
          COALESCE(
            CASE
              WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_implied
              WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_implied
            END,
            mm.vegas_total / 2.0
          ) AS team_implied
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        WHERE ds.sport = 'mlb'
          AND dp.actual_fpts IS NOT NULL
          AND dp.our_proj IS NOT NULL
          AND COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions NOT LIKE '%SP%'
          AND dp.eligible_positions NOT LIKE '%RP%'
          AND ds.slate_date >= CURRENT_DATE - INTERVAL '45 days'
      )
      SELECT
        CASE
          WHEN team_implied < 4.0 THEN 'u40'
          WHEN team_implied < 5.0 THEN 'n50'
          WHEN team_implied < 6.0 THEN 'n60'
          ELSE 'hi'
        END AS "impliedBucket",
        COUNT(*)::int AS "n",
        AVG(our_proj) AS "avgProj",
        AVG(actual_fpts) AS "avgActual"
      FROM player_implied
      GROUP BY 1
    `),
  ]);

  const overallRow = overallAgg.rows[0];
  // Require n >= 50 before trusting the overall factor; otherwise use conservative default
  const overallN = overallRow?.n ?? 0;
  const overall: CalibrationEntry = {
    n: overallN,
    factor: overallN >= 50
      ? (factorFromAverages(overallRow?.avgProj, overallRow?.avgActual, 0.70, 1.12) ?? 0.90)
      : 0.90,
  };

  let pending = overall;
  const byOrder = new Map<number, CalibrationEntry>();

  for (const row of orderRows.rows) {
    const bucket = row.bucket?.trim().toLowerCase();
    if (!bucket) continue;
    if (bucket === "pending") {
      if ((row.n ?? 0) >= 60) {
        pending = {
          n: row.n ?? 0,
          factor: factorFromAverages(row.avgProj, row.avgActual, 0.65, 1.05) ?? overall.factor,
        };
      }
      continue;
    }

    const order = Number(bucket);
    if (!Number.isInteger(order) || order < 1 || order > 9 || (row.n ?? 0) < 20) continue;
    byOrder.set(order, {
      n: row.n ?? 0,
      factor: factorFromAverages(row.avgProj, row.avgActual, 0.75, 1.18) ?? overall.factor,
    });
  }

  // Implied-total residual calibration — applied on top of batting-order calibration
  // Only activates for a bucket when n >= 30 and the factor differs meaningfully from 1.0
  const byImplied = new Map<string, CalibrationEntry>();
  for (const row of impliedRows.rows) {
    const bucket = row.impliedBucket?.trim();
    if (!bucket || (row.n ?? 0) < 30) continue;
    const factor = factorFromAverages(row.avgProj, row.avgActual, 0.82, 1.18);
    if (factor == null) continue;
    byImplied.set(bucket, { n: row.n ?? 0, factor });
  }

  const calibration = { overall, pending, byOrder, byImplied };
  mlbHitterProjectionCalibrationCache = { loadedAtMs: now, calibration };
  return calibration;
}
