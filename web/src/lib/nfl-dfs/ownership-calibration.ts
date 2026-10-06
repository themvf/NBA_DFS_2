/** Accuracy is a separate gate from coverage. Server-supplied evidence only. */
export type OwnershipCalibration = {
  format: "classic" | "showdown"; modelVersion: string; registration: string;
  sourceDigest: string; heldOutSlateIds: string[];
  spearman: number | null; maePp: number | null; biasPp: number | null;
};

export function ownershipAccuracyReason(calibration: OwnershipCalibration | undefined,
  format: "classic" | "showdown", sources: (string | null)[]): string | null {
  if (!calibration) return "Ownership accuracy has not been qualified on independent slates; coverage alone cannot enable leverage.";
  if (format !== "classic" || calibration.format !== format) return "Showdown ownership needs its own accuracy qualification; Classic results cannot enable Captain/Flex leverage.";
  if (calibration.registration !== "nfl-ownership-classic-phase2" || !/^[a-f0-9]{64}$/.test(calibration.sourceDigest)
    || !calibration.modelVersion || sources.some(source => source !== calibration.modelVersion)) return "Ownership calibration does not match this model source.";
  if (!Array.isArray(calibration.heldOutSlateIds) || new Set(calibration.heldOutSlateIds).size < 4
    || calibration.heldOutSlateIds.some(id => typeof id !== "string" || !id)) return "Ownership requires at least four independent held-out Classic slates.";
  if (typeof calibration.spearman !== "number" || !Number.isFinite(calibration.spearman) || calibration.spearman < .70 || calibration.spearman > 1
    || typeof calibration.maePp !== "number" || !Number.isFinite(calibration.maePp) || calibration.maePp < 0 || calibration.maePp > 2
    || typeof calibration.biasPp !== "number" || !Number.isFinite(calibration.biasPp) || Math.abs(calibration.biasPp) > .5) return "Ownership did not pass the registered accuracy thresholds.";
  return null;
}
