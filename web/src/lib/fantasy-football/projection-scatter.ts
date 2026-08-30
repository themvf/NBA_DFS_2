/**
 * Shared configuration for the /fantasy-football/projections scatter.
 *
 * Kept out of the client component so the position list, colors, and the
 * per-position caveats have one definition that the page, the chart, and any
 * future print sheet all read.
 */

export type ScatterView = "QB" | "RB" | "WR" | "TE" | "FLEX" | "K" | "DEF";

/**
 * Position colors follow the app's existing convention (see
 * best-ball-draft-board.tsx: QB red, RB blue, WR emerald, TE amber), re-stepped
 * for small marks on a light surface. The only view that puts three series on
 * one chart is FLEX (RB/WR/TE); that trio was validated all-pairs in both
 * modes -- worst CVD deltaE 10.6, worst normal-vision deltaE 19.8. WR sits at
 * 2.82:1 against a white card, below the 3:1 bar, which obligates the visible
 * relief this page already ships: direct labels on the chart plus the full
 * table underneath. QB, K, and DEF never share a chart with another series, so
 * they only need to clear contrast on their own.
 */
export const POSITION_COLORS: Record<string, { light: string; dark: string }> = {
  QB: { light: "#e34948", dark: "#e66767" },
  RB: { light: "#2a78d6", dark: "#3987e5" },
  WR: { light: "#1baf7a", dark: "#199e70" },
  TE: { light: "#c98500", dark: "#c98500" },
  K: { light: "#4a3aa7", dark: "#9085e9" },
  DST: { light: "#008300", dark: "#008300" },
};

/** Roster positions each tab pulls from. FLEX is the RB/WR/TE pool. */
export const VIEW_POSITIONS: Record<ScatterView, string[]> = {
  QB: ["QB"],
  RB: ["RB"],
  WR: ["WR"],
  TE: ["TE"],
  FLEX: ["RB", "WR", "TE"],
  K: ["K"],
  DEF: ["DST"],
};

export const VIEW_ORDER: ScatterView[] = ["QB", "RB", "WR", "TE", "FLEX", "K", "DEF"];

/**
 * What a reader needs to know before trusting a given tab. These are not
 * decoration: each one records a real, documented property of the model that
 * would otherwise make the chart easy to misread.
 */
export const VIEW_NOTES: Record<ScatterView, string | null> = {
  QB: null,
  RB: null,
  WR: null,
  TE: null,
  FLEX: "Three positions on one set of axes. They are not interchangeable at equal points: a 200-point RB and a 200-point WR occupy very different roster slots and cost very different picks.",
  K: "Kickers rank on their 2025 season alone, shrunk hard toward the league average. That is a deliberate override of the more accurate 3-year blend, taken knowingly at a cost of about 1 point of held-out error.",
  DEF: "Order comes from 2025 alone; magnitude is shrunk almost to a constant, so every defense projects into a ~6-point band. That narrowness is the finding, not a bug — no Yahoo DST scoring component repeats year to year well enough to justify a wider spread. Read the vertical position as \"we do not claim to know\", and the horizontal spread as what actually happened last year.",
};

/**
 * Where the projection is shrunk almost to a constant, a "biggest mover" list
 * is not a claim about anyone improving -- with a flat projection the change is
 * just the 2025 ranking turned upside down. Say so rather than letting the
 * panel imply a forecast it is not making.
 */
export const VIEW_MOVER_NOTES: Partial<Record<ScatterView, string>> = {
  DEF: "With every defense projected into the same narrow band, this is the 2025 table inverted, not a forecast: the worst defenses last year necessarily show the largest \"gain\" toward the league average.",
};

export const VIEW_LABELS: Record<ScatterView, string> = {
  QB: "QB",
  RB: "RB",
  WR: "WR",
  TE: "TE",
  FLEX: "Flex",
  K: "K",
  DEF: "DEF",
};

/**
 * The projection is a full-season baseline of this many active games, which is
 * why the per-game view exists: on season totals, a player who missed time last
 * year shows an enormous "gain" that is really just health.
 */
export const BASELINE_GAMES = 17;

/**
 * Fantasy weeks, split the way this app already splits them everywhere else:
 * DraftKings Best Ball scores Weeks 1-14 as the regular season and Weeks 15,
 * 16 and 17 as the three tournament rounds, and `ingest/ff_playoff_sos.py`
 * computes playoff strength of schedule over exactly 15-17.
 *
 * Week 18 exists in the nflverse feed and is stored, but is deliberately not
 * shown: no standard fantasy format scores it, and starters are routinely
 * rested, so including it would drag season totals and averages toward a week
 * nobody actually plays. Add it here if a format ever needs it.
 */
export const SEASON_WEEKS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
export const PLAYOFF_WEEKS = [15, 16, 17];

/**
 * Sequential ramp for the weekly grid: one hue, light to dark, so magnitude
 * reads without the cell colours pretending to be categories. Steps are the
 * documented blue ramp.
 *
 * The BREAKPOINTS are per position group, because a single absolute ramp does
 * not survive contact with the real distributions. Measured over 2025 weeks
 * 1-17 for drafted players, the median week is 16.8 for a QB but 5.0 for a
 * defense and 9.0 for a kicker, and DEF's 95th percentile (16.3) sits below
 * QB's median. On one QB-calibrated ramp, ~95% of DEF and K cells collapse
 * into the lightest step and the grid stops encoding anything. Each group is
 * therefore anchored on its own quantiles (roughly p50/p70/p85/p95/p99).
 *
 * RB, WR and TE deliberately share one scale rather than getting three: their
 * distributions are close (medians 9.3-9.8, p95 24-29), and Flex puts all
 * three in one table, where per-position scales would make a 12-point TE week
 * look identical to a 12-point RB week's neighbour at a different value.
 */
const HEAT_HUES = [
  { bg: "#256abf", fg: "#ffffff" },
  { bg: "#3987e5", fg: "#ffffff" },
  { bg: "#6da7ec", fg: "#0b0b0b" },
  { bg: "#9ec5f4", fg: "#0b0b0b" },
  { bg: "#b7d3f6", fg: "#0b0b0b" },
  { bg: "#cde2fb", fg: "#0b0b0b" },
];

/** Highest-to-lowest minimums, one per HEAT_HUES step. */
const HEAT_BREAKPOINTS: Record<string, number[]> = {
  QB: [36, 28, 22, 17, 10, 0.01],
  SKILL: [30, 22, 16, 11, 6, 0.01],
  K: [18, 14, 11, 9, 5, 0.01],
  DST: [16, 11, 8, 5, 3, 0.01],
};

const POSITION_HEAT_GROUP: Record<string, string> = {
  QB: "QB",
  RB: "SKILL",
  WR: "SKILL",
  TE: "SKILL",
  K: "K",
  DST: "DST",
};

/**
 * A negative week is not "no production" -- a defense can genuinely go below
 * zero under Yahoo's points-allowed tiers (34 such weeks in 2025), and an
 * untinted -4 is indistinguishable from an untinted 0. It gets its own muted
 * warm step, well clear of the blue ramp.
 */
const HEAT_NEGATIVE = { bg: "#f6d7d5", fg: "#7a1f1c" };

export function weekHeat(points: number, position: string): { bg: string; fg: string } | null {
  if (points < 0) return HEAT_NEGATIVE;
  const breaks = HEAT_BREAKPOINTS[POSITION_HEAT_GROUP[position] ?? "SKILL"];
  const index = breaks.findIndex((min) => points >= min);
  return index === -1 ? null : HEAT_HUES[index];
}

/** Legend sample values for a position, lowest to highest step. */
export function heatLegendValues(position: string): number[] {
  const breaks = HEAT_BREAKPOINTS[POSITION_HEAT_GROUP[position] ?? "SKILL"];
  return [...breaks].reverse();
}

/**
 * What the shading is actually scaled to. RB, WR and TE share one scale, so a
 * single-position RB tab must not claim the ramp is RB-specific.
 */
export function heatScaleLabel(position: string): string {
  const group = POSITION_HEAT_GROUP[position] ?? "SKILL";
  return group === "SKILL" ? "RB/WR/TE" : group === "DST" ? "DEF" : group;
}
