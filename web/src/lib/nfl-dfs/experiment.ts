/** Node research harness; not a browser module or a calibrated contest optimizer. */
import { createHash } from "node:crypto";
import { rankNflCandidates, type NflExperimentInput } from "./experiment-core";

export function nflDigest(value: unknown): string {
  const canonical = (input: unknown): unknown => {
    if (Array.isArray(input)) return input.map(canonical);
    if (input && typeof input === "object") return Object.fromEntries(Object.entries(input)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, item]) => [key, canonical(item)]));
    return input;
  };
  return createHash("sha256").update(JSON.stringify(canonical(value))).digest("hex");
}

export function compareNflCandidates(input: NflExperimentInput) {
  const report = rankNflCandidates(input);
  return { ...report, manifest: { ...report.manifest,
    slateDigest: nflDigest(input.slate), candidatesDigest: nflDigest(input.candidates),
    selectionDigest: nflDigest(input.selection), evaluationDigest: nflDigest(input.evaluation),
  } };
}
