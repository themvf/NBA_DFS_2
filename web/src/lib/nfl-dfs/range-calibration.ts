/**
 * Projection and range calibration for QB/RB/WR/TE (`nfl-range-calibration-v1`).
 *
 * Fitted 2026-11-03 on the 2026 pregame pool captures, weeks 2-8 (2,582
 * player-games, the last capture before each kickoff, graded on DraftKings
 * points; a player with no stat line in a game that has results scored 0).
 * Applied as fitted, owner's decision: no held-out check before release. The
 * weekly reviews grade it from week 9 on. Three measured problems, two fixes:
 *
 *  1. Players who never got on the field. 21.5% of players not ruled out had
 *     no stat line, and the projections carried them anyway (Quez Watkins
 *     7.4 at $3,000 in weeks 5-8, zero every week). Depth-chart rank predicts
 *     it (weeks 4-8, when ranks were recorded):
 *
 *        rank 1   1-3%      rank 2   2-3% (TE 20%)
 *        rank 3   5% WR, 23% RB, 42% TE
 *        rank 4+  16-33%    not on the chart at all  82-99%
 *
 *     With prop lines (the books expect him to play) the deep ranks fall to
 *     12-14%, so a market-anchored player's chance is capped at 15%, and a
 *     backup the absence commit promoted at 5%. An unlisted QB is left alone.
 *     `notPlay` is the chance of a 0. The projection becomes
 *     (1 - notPlay) x his if-he-plays number.
 *
 *  2. Projections spread too far. Among players who played, actual points
 *     rose 0.87-0.90 per projected point (QB 0.68, shrunk to 0.81 toward the
 *     pooled 0.89), so stars ran high and backups low. The if-he-plays mean
 *     becomes a + b x mean per position, each line passing through that
 *     position's actual average; the range is scaled with it.
 *
 *  3. Ranges: NOT widened. Where the long-memory range split
 *     (nfl-dfs-range-split-v1, 2026-10-26) applies, actual beat P90 9.6% of
 *     the time and fell under P10 6.4% (n=249), so it is calibrated. Without
 *     it (mainly QBs) P90 was beaten 14-15% of the time, but widening those
 *     ranges to match made replayed lineups worse: on 13 saved 2026
 *     Showdowns the best lineup beat 0.93 points less of the field
 *     [-2.09, 0.00], while the other two corrections alone were neutral and
 *     +0.37 [-0.10, +0.85]. Ranges are only rescaled to the new mean.
 *
 * The page's P10/median/P90/boom become the honest mix of "plays" and "does
 * not": a player with a 96% chance of a 0 has a P90 of 0. The if-he-plays
 * range is kept in `ifPlays`, which simulated games draw from (with the 0
 * drawn `notPlay` of the time). DST and K are not touched (special-teams
 * forecasts carry their own ranges). Stated priors, not fitted: the 15%
 * anchored cap and the shrink weights. Change a constant and bump the version.
 */
import { fitQuantileShape, quantile, type QuantileShape } from "./captain-simulation";
import { BOOM_THRESHOLDS } from "./replacement-upside";

export const RANGE_CALIBRATION_VERSION = "nfl-range-calibration-v1";

type Skill = "QB" | "RB" | "WR" | "TE";
const SKILL = new Set<string>(["QB", "RB", "WR", "TE"]);

/** If-he-plays mean = a + b x projected mean, per position. */
export const MEAN_CALIBRATION: Record<Skill, { b: number; a: number }> = {
  QB: { b: 0.806, a: 3.12 },
  RB: { b: 0.895, a: 1.21 },
  WR: { b: 0.868, a: 0.94 },
  TE: { b: 0.868, a: 0.91 },
};

/** Chance of no stat line by depth-chart rank (weeks 4-8, (k + 0.5) / (n + 1)). */
export const NOT_PLAY_BY_RANK: Record<Skill, { r1: number; r2: number; r3: number; r4: number; unlisted: number }> = {
  // Backup quarterbacks are already kept out of lineups by the starter rule;
  // the rate is a stated prior (they do not play unless the starter leaves).
  // An unlisted QB is left alone: no unlisted QB reached the pool in weeks
  // 4-8, and a starter with an unresolved label must not lose his projection.
  QB: { r1: 0.01, r2: 0.9, r3: 0.9, r4: 0.9, unlisted: 0 },
  RB: { r1: 0.01, r2: 0.033, r3: 0.21, r4: 0.21, unlisted: 0.82 },
  WR: { r1: 0.017, r2: 0.024, r3: 0.05, r4: 0.187, unlisted: 0.96 },
  TE: { r1: 0.031, r2: 0.204, r3: 0.4, r4: 0.4, unlisted: 0.97 },
};
/** A player the books have lines on is expected to play. */
export const ANCHORED_NOT_PLAY_CAP = 0.15;
/** A backup the absence commit already moved into the starter's role plays. */
export const PROMOTED_NOT_PLAY_CAP = 0.05;


export interface Range { mean: number; p10: number; median: number; p90: number }

export interface RangeCalibration {
  version: string;
  /** Chance he records no stat line (a 0), and what it was read from. */
  notPlay: number;
  notPlayBasis: string;
  /** The if-he-plays distribution simulated games draw from. */
  ifPlays: Range;
  /** What the page showed before calibration. */
  before: Range & { boom: number | null };
  meanLine: { a: number; b: number };
}

export interface CalibrationInput {
  position: string;
  isOut: boolean;
  projectionStatus: string;
  ourProj: number | null;
  floorFpts: number | null;
  medianFpts?: number | null;
  ceilingFpts: number | null;
  boomRate: number | null;
  depthRole?: string | null;
  availabilityState?: string | null;
  marketAnchor?: unknown;
  absenceCommit?: unknown;
}

const finite = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
const r2 = (x: number) => Math.round(x * 100) / 100;

/**
 * Depth rank from the label ("Listed WR3", "Expected starter - QB1"), "unlisted"
 * when roster evidence is current and he has no rank, null when nothing is known.
 */
export function depthRank(position: string, depthRole: string | null | undefined, availabilityState: string | null | undefined): number | "unlisted" | null {
  const m = depthRole?.match(/\b(?:QB|RB|WR|TE)\s*(\d+)\b/i);
  if (m) return Number(m[1]);
  if (!depthRole && (availabilityState === "confirmed" || availabilityState === "probable")) return "unlisted";
  return null;
}

export function notPlayChance(p: Pick<CalibrationInput, "position" | "depthRole" | "availabilityState" | "marketAnchor" | "absenceCommit">): { notPlay: number; basis: string } {
  if (!SKILL.has(p.position)) return { notPlay: 0, basis: "not modeled for this position" };
  const rank = depthRank(p.position, p.depthRole, p.availabilityState);
  if (rank == null) return { notPlay: 0, basis: "no current depth chart" };
  const t = NOT_PLAY_BY_RANK[p.position as Skill];
  const raw = rank === "unlisted" ? t.unlisted : rank === 1 ? t.r1 : rank === 2 ? t.r2 : rank === 3 ? t.r3 : t.r4;
  const label = rank === "unlisted" ? "not on the depth chart" : `${p.position}${rank} on the depth chart`;
  if (p.absenceCommit && raw > PROMOTED_NOT_PLAY_CAP) return { notPlay: PROMOTED_NOT_PLAY_CAP, basis: `${label}, but he has the absent starter's role` };
  if (p.marketAnchor && raw > ANCHORED_NOT_PLAY_CAP) return { notPlay: ANCHORED_NOT_PLAY_CAP, basis: `${label}, but the books have lines on him` };
  return { notPlay: raw, basis: label };
}

/** Quantile of "0 with chance q, else the curve". */
function mixtureQuantile(shape: QuantileShape, q: number, tau: number): number {
  const cdf = (x: number) => {
    if (x <= shape.q0) return 0;
    if (x >= shape.q1) return 1;
    let a = 0, b = 1;
    for (let i = 0; i < 50; i += 1) { const m = (a + b) / 2; if (quantile(shape, m) < x) a = m; else b = m; }
    return (a + b) / 2;
  };
  const at = (x: number) => (1 - q) * cdf(x) + (x >= 0 ? q : 0);
  let lo = Math.min(shape.q0, 0), hi = Math.max(shape.q1, 0);
  for (let i = 0; i < 60; i += 1) { const m = (lo + hi) / 2; if (at(m) < tau) lo = m; else hi = m; }
  return (lo + hi) / 2;
}

/** Which corrections to apply; both by default. Parts exist for the replay's ablation. */
export type CalibrationParts = { notPlay?: boolean; meanLine?: boolean };

/** The calibrated player, or null when he is not calibrated (and must be left as he is). */
export function calibratePlayer<P extends CalibrationInput>(p: P, parts: CalibrationParts = {}): (P & { rangeCalibration: RangeCalibration }) | null {
  if (!SKILL.has(p.position) || p.isOut || p.projectionStatus === "out") return null;
  const mean = p.ourProj, p10 = p.floorFpts, median = p.medianFpts, p90 = p.ceilingFpts;
  if (!finite(mean) || mean <= 0 || !finite(p10) || !finite(median) || !finite(p90) || !(p90 > p10)) return null;
  const line = parts.meanLine === false ? { a: 0, b: 1 } : MEAN_CALIBRATION[p.position as Skill];
  const playsMean = Math.max(0.1, line.a + line.b * mean);
  const s = playsMean / mean;
  const lo = p10 * s, mid = median * s, hi = p90 * s;
  const ifPlays: Range = { mean: r2(playsMean), p10: r2(lo), median: r2(mid), p90: r2(hi) };
  const { notPlay, basis } = parts.notPlay === false ? { notPlay: 0, basis: "not applied" } : notPlayChance(p);
  const shape = fitQuantileShape(playsMean, lo, mid, hi);
  const boomLine = BOOM_THRESHOLDS[p.position];
  const boomIfPlays = boomLine == null ? null : 1 - (() => {
    if (boomLine <= shape.q0) return 0;
    if (boomLine >= shape.q1) return 1;
    let a = 0, b = 1;
    for (let i = 0; i < 50; i += 1) { const m = (a + b) / 2; if (quantile(shape, m) < boomLine) a = m; else b = m; }
    return (a + b) / 2;
  })();
  const shown = notPlay > 0
    ? { p10: mixtureQuantile(shape, notPlay, 0.1), median: mixtureQuantile(shape, notPlay, 0.5), p90: mixtureQuantile(shape, notPlay, 0.9) }
    : { p10: lo, median: mid, p90: hi };
  return {
    ...p,
    ourProj: r2((1 - notPlay) * playsMean),
    floorFpts: r2(shown.p10), medianFpts: r2(shown.median), ceilingFpts: r2(shown.p90),
    boomRate: boomIfPlays == null ? p.boomRate : Math.round((1 - notPlay) * boomIfPlays * 1e4) / 1e4,
    rangeCalibration: {
      version: RANGE_CALIBRATION_VERSION, notPlay, notPlayBasis: basis, ifPlays,
      before: { mean, p10, median, p90, boom: p.boomRate ?? null },
      meanLine: line,
    },
  };
}

/** Calibrate every eligible player; returns the new list and how many changed. */
export function calibratePlayers<P extends CalibrationInput>(players: readonly P[], parts: CalibrationParts = {}): { players: (P & { rangeCalibration?: RangeCalibration })[]; applied: number; notPlaying: number } {
  let applied = 0, notPlaying = 0;
  const out = players.map((p) => {
    const c = calibratePlayer(p, parts);
    if (!c) return p;
    applied += 1;
    if (c.rangeCalibration.notPlay >= 0.5) notPlaying += 1;
    return c;
  });
  return { players: out, applied, notPlaying };
}
