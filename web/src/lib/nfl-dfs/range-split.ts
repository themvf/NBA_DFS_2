/**
 * Long-memory ranges (`nfl-dfs-range-split-v1`, owner's decision 2026-10-26).
 *
 * The projection stays; each RB/WR/TE's floor and ceiling take the shape of his
 * half-life-17 distribution (docs/nfl-dfs-range-split-study.md):
 *
 *   floor   = projection x ratioP10      ratioP10 = P10_17 / mean_17
 *   ceiling = projection x ratioP90      ratioP90 = P90_17 / mean_17
 *
 * The production range comes from the last ~3 games and is too narrow on both
 * sides (2022-2026: ~13-15% below the floor and ~12-13% above the ceiling
 * against 10% each). This form cleared a blind test on 2020-2021 and the
 * one-time 2026 weeks 1-7 check (Gate Q) before shipping. The ratios are
 * written weekly by ingest/nfl_dfs_range_ratios.py.
 *
 * Applied last, after the absence commit and the market anchor: the ratio form
 * commutes with the anchor (which scales every quantile by the same factor), so
 * applying it to an anchored row equals anchoring a row that had it. The
 * median is not part of the test; it is kept and only clamped inside the new
 * range. `before` keeps the range the build would otherwise have had, which is
 * the baseline the study's forward gate grades against. Change a rule here and
 * bump the version. Pure: no I/O.
 */

export const RANGE_SPLIT_VERSION = 'nfl-dfs-range-split-v1';
export const RANGE_SPLIT_POSITIONS: readonly string[] = ['RB', 'WR', 'TE'];

export type RangeRatio = { ratioP10: number; ratioP90: number; m17: number; historyGames: number; computedAt: string };

export type RangeSplit = {
  version: string;
  ratioP10: number;
  ratioP90: number;
  historyGames: number;
  computedAt: string;
  /** The range the build would have used without this overlay. */
  before: { p10: number | null; median: number | null; p90: number | null };
};

type Ranged = { position: string; ourProj: number | null; floorFpts?: number | null; medianFpts?: number | null; ceilingFpts?: number | null };

const round = (x: number) => Math.round(x * 1000) / 1000;

/** The rescaled range for one projection, or null when it does not apply. */
export function rangeFromRatio(mean: number | null | undefined, median: number | null | undefined, r: Pick<RangeRatio, 'ratioP10' | 'ratioP90'>) {
  if (mean == null || !Number.isFinite(mean) || mean <= 0) return null;
  if (!Number.isFinite(r.ratioP10) || !Number.isFinite(r.ratioP90) || r.ratioP90 < r.ratioP10) return null;
  const p10 = round(mean * r.ratioP10), p90 = round(mean * r.ratioP90);
  const med = median == null || !Number.isFinite(median) ? null : round(Math.min(p90, Math.max(p10, median)));
  return { p10, median: med, p90 };
}

/** The overlay for one player, or null (wrong position, no projection, unusable ratio). */
export function rangeSplitFor(player: Ranged, ratio: RangeRatio | null | undefined): { split: RangeSplit; p10: number; median: number | null; p90: number } | null {
  if (!ratio || !RANGE_SPLIT_POSITIONS.includes(player.position)) return null;
  const range = rangeFromRatio(player.ourProj, player.medianFpts, ratio);
  if (!range) return null;
  return { ...range, split: { version: RANGE_SPLIT_VERSION, ratioP10: ratio.ratioP10, ratioP90: ratio.ratioP90,
    historyGames: ratio.historyGames, computedAt: ratio.computedAt,
    before: { p10: player.floorFpts ?? null, median: player.medianFpts ?? null, p90: player.ceilingFpts ?? null } } };
}
