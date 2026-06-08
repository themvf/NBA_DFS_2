/**
 * Pure synchronous helpers for MLB projection calibration.
 * No "use server" — safe to call from .map() callbacks in server actions.
 */

export type MlbHitterProjectionCalibrationEntry = {
  factor: number;
  n: number;
};

export type MlbHitterProjectionCalibration = {
  overall: MlbHitterProjectionCalibrationEntry;
  pending: MlbHitterProjectionCalibrationEntry;
  byOrder: Map<number, MlbHitterProjectionCalibrationEntry>;
  // Residual correction bucketed by team implied total (stacks on top of byOrder/overall)
  byImplied: Map<string, MlbHitterProjectionCalibrationEntry>;
};

function sanitizeProjection(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? Math.max(0, value) : null;
}

function impliedBucketKey(teamImplied: number): string {
  if (teamImplied < 4.0) return "u40";
  if (teamImplied < 5.0) return "n50";
  if (teamImplied < 6.0) return "n60";
  return "hi";
}

export function applyMlbHitterProjectionCalibration(
  rawProjection: number | null | undefined,
  confirmedOrder: number | null | undefined,
  teamLineupConfirmed: boolean | null | undefined,
  calibration: MlbHitterProjectionCalibration,
  teamImplied?: number | null,
): number | null {
  const sanitized = sanitizeProjection(rawProjection);
  if (sanitized == null) return null;

  // Primary calibration: per batting-order position (or pending for unconfirmed lineups)
  const entry =
    teamLineupConfirmed === true && confirmedOrder != null
      ? (calibration.byOrder.get(confirmedOrder) ?? calibration.overall)
      : calibration.pending;

  // Secondary calibration: residual correction per implied-total bucket
  const impliedFactor = teamImplied != null
    ? (calibration.byImplied.get(impliedBucketKey(teamImplied))?.factor ?? 1.0)
    : 1.0;

  return sanitizeProjection(Math.round(sanitized * entry.factor * impliedFactor * 100) / 100);
}
