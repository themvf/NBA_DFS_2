// Ranking and delta-heat helpers for the Weekly Player Review.
// Pure: no React, no database, no formatting — so the ordering and the
// colour-scale rules can be tested directly.
import type { ReportRow } from "./report-card";

export const REVIEW_POSITIONS = ["ALL", "QB", "RB", "WR", "TE", "FLEX", "DST"] as const;
export type ReviewPosition = (typeof REVIEW_POSITIONS)[number];

/** DraftKings Classic FLEX eligibility. Not a position a player *has*. */
export const FLEX_POSITIONS = ["RB", "WR", "TE"] as const;

export function matchesReviewPosition(position: string, filter: ReviewPosition): boolean {
  if (filter === "ALL") return true;
  if (filter === "FLEX") return (FLEX_POSITIONS as readonly string[]).includes(position);
  return position === filter;
}

/**
 * Delta is `actual - projected`, already frozen on the row as `error` by the
 * report card. Never recomputed here: a second derivation could disagree with
 * the stored audit value.
 */
export function delta(row: ReportRow): number | null {
  return row.actual === null ? null : row.error;
}

export function isScored(row: ReportRow): boolean {
  return delta(row) !== null;
}

/** Stable order: by delta, then name, then id — so equal deltas never reshuffle. */
function byDelta(direction: 1 | -1) {
  return (a: ReportRow, b: ReportRow) =>
    direction * (delta(b)! - delta(a)!) || a.name.localeCompare(b.name) || a.player_id - b.player_id;
}

/**
 * Top movers in each direction. A player appears in at most one list because
 * the lists are split on the sign of the delta, not on rank — so a week with
 * fewer than `limit` scored rows cannot show the same player as both the
 * biggest riser and the biggest faller.
 */
export function topMovers(rows: ReportRow[], limit = 10) {
  const scored = rows.filter(isScored);
  return {
    exceeded: scored.filter(r => delta(r)! > 0).sort(byDelta(1)).slice(0, limit),
    disappointed: scored.filter(r => delta(r)! < 0).sort(byDelta(-1)).slice(0, limit),
  };
}

/**
 * One scale per position, because delta magnitude is structurally
 * position-dependent: a +8 week is ordinary for a QB and exceptional for a TE.
 * A single shared scale would paint every non-QB row near-neutral — the same
 * reason the fantasy-football weekly grid shades per position group.
 */
export function deltaScales(rows: ReportRow[]): Record<string, number> {
  const scales: Record<string, number> = {};
  for (const row of rows) {
    const d = delta(row);
    if (d === null || !Number.isFinite(d)) continue;
    scales[row.position] = Math.max(scales[row.position] ?? 0, Math.abs(d));
  }
  return scales;
}

export type DeltaBucket = -2 | -1 | 0 | 1 | 2;

/** Five buckets: two per arm plus a neutral midpoint. */
export function deltaBucket(d: number | null, scale: number | undefined): DeltaBucket {
  if (d === null || !Number.isFinite(d) || !scale || scale <= 0) return 0;
  const ratio = Math.abs(d) / scale;
  const step: 0 | 1 | 2 = ratio < 0.2 ? 0 : ratio < 0.55 ? 1 : 2;
  if (step === 0) return 0;
  return (d > 0 ? step : -step) as DeltaBucket;
}

/**
 * Diverging fill, blue (exceeded) <-> red (disappointed), neutral midpoint.
 * Blue/red rather than green/red: measured against the palette validator,
 * blue<->red separates at CVD dE 23.6 where green<->red sits at 6.9 (warn band).
 * Both arms stay pale so table text keeps its own contrast — the saturated
 * poles are used only for marks, never behind text.
 */
export const DELTA_FILL: Record<DeltaBucket, string | undefined> = {
  [-2]: "#f09a9a",
  [-1]: "#fbd5d5",
  0: undefined,
  1: "#cde2fb",
  2: "#86b6ef",
};

export const DELTA_POLE = { exceeded: "#3987e5", disappointed: "#e34948" } as const;
