import type { LsOwnershipCorrectionTables } from "@/db/queries";

export function getSalaryTier(salary: number): string {
  if (salary < 5000) return "Under $5k";
  if (salary < 6000) return "$5k-$6k";
  if (salary < 7000) return "$6k-$7k";
  if (salary < 8000) return "$7k-$8k";
  if (salary < 9000) return "$8k-$9k";
  return "$9k+";
}

export function getPrimaryNbaPosition(eligiblePositions: string): string {
  if (eligiblePositions.includes("PG")) return "PG";
  if (eligiblePositions.includes("SG")) return "SG";
  if (eligiblePositions.includes("SF")) return "SF";
  if (eligiblePositions.includes("PF")) return "PF";
  if (eligiblePositions.includes("C")) return "C";
  return "UTIL";
}

export function getPrimaryMlbPosition(eligiblePositions: string): string {
  if (eligiblePositions.includes("SP")) return "SP";
  if (eligiblePositions.includes("RP")) return "RP";
  if (eligiblePositions.includes("C")) return "C";
  if (eligiblePositions.includes("1B")) return "1B";
  if (eligiblePositions.includes("2B")) return "2B";
  if (eligiblePositions.includes("3B")) return "3B";
  if (eligiblePositions.includes("SS")) return "SS";
  if (eligiblePositions.includes("OF")) return "OF";
  return "UTIL";
}

/**
 * Apply two-layer calibration to LineStar's raw ownership prediction.
 *
 * Layer 1 — position × salary bias (population-level correction, most data).
 * Layer 2 — team × position residual (franchise-specific, removes the
 *            position-level component already captured by Layer 1).
 *
 * Each correction is shrunk toward 0 when the underlying sample is sparse:
 *   Layer 1 reaches full weight at n ≥ 30
 *   Layer 2 reaches full weight at n ≥ 15
 */
export function computeCalibratedLsOwn(
  rawLsOwnPct: number,
  position: string,
  salaryTier: string,
  teamAbbrev: string,
  tables: LsOwnershipCorrectionTables,
): number {
  // Layer 1: position × salary correction
  const psRow = tables.posSalary.find(
    (r) => r.position === position && r.salaryTier === salaryTier,
  );
  const psCorrection = psRow
    ? psRow.bias * Math.min(1, psRow.n / 30)
    : 0;

  // Layer 2: team × position residual (only the part not captured by Layer 1)
  const tpRow = tables.teamPosition.find(
    (r) => r.teamAbbrev === teamAbbrev && r.position === position,
  );
  const posMeanRow = tables.positionMeanBias.find((r) => r.position === position);
  const posMeanBias = posMeanRow?.meanBias ?? 0;
  const teamResidual = tpRow
    ? (tpRow.bias - posMeanBias) * Math.min(1, tpRow.n / 15)
    : 0;

  return rawLsOwnPct - psCorrection - teamResidual;
}

/**
 * Compute the GPP LineStar leverage score for a single player.
 *
 * Formula:
 *   lsLeverage = (p90 − fieldProj) × (1 − calibratedOwnPct/100)^0.8
 *
 * p90 (projCeiling) replaces ourProj to target ceiling outcomes.
 * Ownership exponent 0.8 > 0.7 for more aggressive chalk fade.
 * fieldProj priority: linestarProj → ourProj (same as existing leverage).
 */
export function computeLsLeverage(
  projCeiling: number | null,
  ourProj: number | null,
  linestarProj: number | null,
  rawLsOwnPct: number | null,
  salary: number,
  eligiblePositions: string,
  teamAbbrev: string,
  tables: LsOwnershipCorrectionTables,
): number | null {
  const p90 = projCeiling ?? ourProj;
  if (p90 == null) return null;

  const fieldProj = linestarProj ?? ourProj;
  if (fieldProj == null) return null;

  if (rawLsOwnPct == null) return null;

  const position = getPrimaryNbaPosition(eligiblePositions);
  const salaryTier = getSalaryTier(salary);
  const calibratedOwn = computeCalibratedLsOwn(
    rawLsOwnPct,
    position,
    salaryTier,
    teamAbbrev,
    tables,
  );

  const edge = p90 - fieldProj;
  const ownFraction = Math.max(0, Math.min(1, calibratedOwn / 100));
  return edge * Math.pow(1 - ownFraction, 0.8);
}
