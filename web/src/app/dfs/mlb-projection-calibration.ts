"use server";

import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";

type CalibrationEntry = {
  factor: number;
  n: number;
};

export type MlbHitterProjectionCalibration = {
  overall: CalibrationEntry;
  pending: CalibrationEntry;
  byOrder: Map<number, CalibrationEntry>;
};

let mlbHitterProjectionCalibrationCache:
  | { loadedAtMs: number; calibration: MlbHitterProjectionCalibration }
  | null = null;

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

  const rows = await db.execute<{
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
  `);

  const overallAgg = await db.execute<{
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
  `);

  const overallRow = overallAgg.rows[0];
  const overall: CalibrationEntry = {
    n: overallRow?.n ?? 0,
    factor: factorFromAverages(overallRow?.avgProj, overallRow?.avgActual, 0.84, 1.08) ?? 0.95,
  };

  let pending = overall;
  const byOrder = new Map<number, CalibrationEntry>();

  for (const row of rows.rows) {
    const bucket = row.bucket?.trim().toLowerCase();
    if (!bucket) continue;
    if (bucket === "pending") {
      if ((row.n ?? 0) >= 60) {
        pending = {
          n: row.n ?? 0,
          factor: factorFromAverages(row.avgProj, row.avgActual, 0.78, 1.0) ?? overall.factor,
        };
      }
      continue;
    }

    const order = Number(bucket);
    if (!Number.isInteger(order) || order < 1 || order > 9 || (row.n ?? 0) < 20) continue;
    byOrder.set(order, {
      n: row.n ?? 0,
      factor: factorFromAverages(row.avgProj, row.avgActual, 0.88, 1.15) ?? overall.factor,
    });
  }

  const calibration = { overall, pending, byOrder };
  mlbHitterProjectionCalibrationCache = { loadedAtMs: now, calibration };
  return calibration;
}

export function applyMlbHitterProjectionCalibration(
  rawProjection: number | null | undefined,
  confirmedOrder: number | null | undefined,
  teamLineupConfirmed: boolean | null | undefined,
  calibration: MlbHitterProjectionCalibration,
): number | null {
  const sanitized = sanitizeProjection(rawProjection);
  if (sanitized == null) return null;

  const entry = teamLineupConfirmed === true && confirmedOrder != null
    ? calibration.byOrder.get(confirmedOrder) ?? calibration.overall
    : calibration.pending;

  return sanitizeProjection(Math.round(sanitized * entry.factor * 100) / 100);
}
