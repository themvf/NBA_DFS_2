export type OptimizerMode = "cash" | "gpp" | "gpp2" | "gpp_ls";

export function isTournamentMode(mode: OptimizerMode): mode is "gpp" | "gpp2" | "gpp_ls" {
  return mode !== "cash";
}

export function isLargeFieldTournamentMode(mode: OptimizerMode): mode is "gpp2" {
  return mode === "gpp2";
}

export function isLinestArMode(mode: OptimizerMode): mode is "gpp_ls" {
  return mode === "gpp_ls";
}
