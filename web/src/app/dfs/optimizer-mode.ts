export type OptimizerMode = "cash" | "gpp" | "gpp2";

export function isTournamentMode(mode: OptimizerMode): mode is "gpp" | "gpp2" {
  return mode !== "cash";
}

export function isLargeFieldTournamentMode(mode: OptimizerMode): mode is "gpp2" {
  return mode === "gpp2";
}
