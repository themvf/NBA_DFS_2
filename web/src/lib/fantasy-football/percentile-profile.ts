// Display config for the percentile-profile chip (PlayerProfiler-style
// percentile rankings, per the reference screenshot). Purely presentational --
// the actual percentile computation lives in
// db/queries-fantasy-football.ts::getFantasyPercentileProfile. Keeping the two
// separate means a label/grouping tweak here never risks the SQL, and vice
// versa.

export type PercentileStatKey =
  | "fantasyPoints" | "carries" | "rushingYards" | "rushingTds" | "rushingEpa"
  | "targets" | "receptions" | "receivingYards" | "receivingTds" | "targetShare"
  | "receivingEpa" | "receivingAirYards" | "airYardsShare" | "wopr" | "racr"
  | "attempts" | "completions" | "passingYards" | "passingTds" | "passingInterceptions"
  | "passingEpa" | "passingAirYards";

export type PercentileStatDisplay = {
  key: PercentileStatKey;
  label: string;
  decimals: number;
  // Some fields (target_share, air_yards_share, wopr, racr) are already
  // ratios/rates and shouldn't be read as counting stats -- purely cosmetic
  // (affects the raw-value suffix), no bearing on the percentile itself.
  suffix?: string;
};

export type PercentileStatGroup = {
  label: string;
  stats: PercentileStatDisplay[];
};

const FANTASY: PercentileStatGroup = {
  label: "Fantasy",
  stats: [{ key: "fantasyPoints", label: "Fantasy Points", decimals: 1 }],
};

const RUSHING_VOLUME: PercentileStatGroup = {
  label: "Rushing Volume",
  stats: [
    { key: "carries", label: "Carries", decimals: 2 },
    { key: "rushingYards", label: "Rushing Yards", decimals: 2 },
    { key: "rushingTds", label: "Rushing TDs", decimals: 2 },
  ],
};

const RUSHING_EFFICIENCY: PercentileStatGroup = {
  label: "Rushing Efficiency",
  stats: [{ key: "rushingEpa", label: "Rushing EPA", decimals: 2 }],
};

const RECEIVING_VOLUME: PercentileStatGroup = {
  label: "Receiving",
  stats: [
    { key: "targets", label: "Targets", decimals: 2 },
    { key: "receptions", label: "Receptions", decimals: 2 },
    { key: "receivingYards", label: "Receiving Yards", decimals: 2 },
    { key: "receivingTds", label: "Receiving TDs", decimals: 2 },
    { key: "targetShare", label: "Target Share", decimals: 1, suffix: "%" },
  ],
};

const RECEIVING_EFFICIENCY: PercentileStatGroup = {
  label: "Receiving Efficiency",
  stats: [
    { key: "receivingEpa", label: "Receiving EPA", decimals: 2 },
    { key: "receivingAirYards", label: "Receiving Air Yards", decimals: 2 },
  ],
};

const RECEIVING_EFFICIENCY_EXTENDED: PercentileStatGroup = {
  label: "Receiving Efficiency",
  stats: [
    { key: "receivingEpa", label: "Receiving EPA", decimals: 2 },
    { key: "airYardsShare", label: "Air Yards Share", decimals: 1, suffix: "%" },
    { key: "wopr", label: "WOPR", decimals: 2 },
    { key: "racr", label: "RACR", decimals: 2 },
  ],
};

const PASSING_VOLUME: PercentileStatGroup = {
  label: "Passing Volume",
  stats: [
    { key: "attempts", label: "Passing Attempts", decimals: 2 },
    { key: "completions", label: "Completions", decimals: 2 },
    { key: "passingYards", label: "Passing Yards", decimals: 2 },
    { key: "passingTds", label: "Passing TDs", decimals: 2 },
    // Percentile here is a plain ascending rank on raw volume, same as every
    // other stat -- NOT inverted for "fewer is better." A high number means
    // "threw a lot of interceptions relative to the position," not "good."
    { key: "passingInterceptions", label: "Interceptions", decimals: 2 },
  ],
};

const PASSING_EFFICIENCY: PercentileStatGroup = {
  label: "Passing Efficiency",
  stats: [
    { key: "passingEpa", label: "Passing EPA", decimals: 1 },
    { key: "passingAirYards", label: "Passing Air Yards", decimals: 2 },
  ],
};

// QB's rushing stats are a single combined group (unlike RB's split
// Volume/Efficiency) -- matches the reference chart, which shows Carries,
// Rushing Yards, Rushing TDs, and Rushing EPA together under one "Rushing"
// header for QBs.
const QB_RUSHING: PercentileStatGroup = {
  label: "Rushing",
  stats: [
    { key: "carries", label: "Carries", decimals: 2 },
    { key: "rushingYards", label: "Rushing Yards", decimals: 2 },
    { key: "rushingTds", label: "Rushing TDs", decimals: 2 },
    { key: "rushingEpa", label: "Rushing EPA", decimals: 2 },
  ],
};

// RB gets both rushing and receiving groups (matches the reference chart
// exactly). WR/TE skip rushing entirely -- most pass-catchers have too few
// designed rushes for a rushing percentile to mean anything -- and get a
// slightly richer receiving-efficiency group (air yards share/WOPR/RACR)
// since that's where the position's separation actually shows up. QB gets
// passing groups plus a combined rushing group (mobile QBs matter a lot for
// fantasy) -- no receiving group, QBs don't catch passes.
export const PERCENTILE_PROFILE_GROUPS: Record<"QB" | "RB" | "WR" | "TE", PercentileStatGroup[]> = {
  QB: [FANTASY, PASSING_VOLUME, PASSING_EFFICIENCY, QB_RUSHING],
  RB: [FANTASY, RUSHING_VOLUME, RUSHING_EFFICIENCY, RECEIVING_VOLUME, RECEIVING_EFFICIENCY],
  WR: [FANTASY, RECEIVING_VOLUME, RECEIVING_EFFICIENCY_EXTENDED],
  TE: [FANTASY, RECEIVING_VOLUME, RECEIVING_EFFICIENCY_EXTENDED],
};

export function percentileTone(percentile: number | null): string {
  if (percentile === null) return "text-muted-foreground";
  if (percentile >= 80) return "text-emerald-400";
  if (percentile >= 50) return "text-lime-400";
  if (percentile >= 20) return "text-amber-400";
  return "text-red-400";
}

export function percentileBarTone(percentile: number | null): string {
  if (percentile === null) return "bg-slate-600";
  if (percentile >= 80) return "bg-emerald-400";
  if (percentile >= 50) return "bg-lime-400";
  if (percentile >= 20) return "bg-amber-400";
  return "bg-red-400";
}
