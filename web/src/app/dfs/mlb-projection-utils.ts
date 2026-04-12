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
};

function sanitizeProjection(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? Math.max(0, value) : null;
}

export function applyMlbHitterProjectionCalibration(
  rawProjection: number | null | undefined,
  confirmedOrder: number | null | undefined,
  teamLineupConfirmed: boolean | null | undefined,
  calibration: MlbHitterProjectionCalibration,
): number | null {
  const sanitized = sanitizeProjection(rawProjection);
  if (sanitized == null) return null;

  const entry =
    teamLineupConfirmed === true && confirmedOrder != null
      ? (calibration.byOrder.get(confirmedOrder) ?? calibration.overall)
      : calibration.pending;

  return sanitizeProjection(Math.round(sanitized * entry.factor * 100) / 100);
}
