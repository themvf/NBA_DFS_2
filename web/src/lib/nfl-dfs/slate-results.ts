/**
 * How your lineups did, from a DraftKings contest standings export.
 *
 * Before this, nothing in the workspace answered the first question after a
 * slate: where would my lineups have finished? The field audit answers a
 * different one (what did the field know that we didn't), and the pool review
 * grades projections. This scores the saved lineup set with the points
 * DraftKings actually paid -- read from the same standings file, so it exists
 * the morning after the game rather than when the stat feed republishes.
 *
 * Scores: each player's BASE points (the export's FLEX row; see
 * `parseContestExport`), times 1.5 for the captain. Rank: estimated from the
 * contest's score curve -- every entry's score is not stored, only a compact
 * curve, exact for the top 100 ranks and every half-percent after that.
 */

/** [rank, score] points, rank ascending, score non-increasing. */
export type ScoreCurve = Array<[number, number]>;

export const EXACT_TOP_RANKS = 100;
export const CURVE_STEP_SHARE = 0.005;

/** Compact the full score list into a rank -> score curve. */
export function buildScoreCurve(scores: readonly number[]): ScoreCurve {
  const sorted = [...scores].filter(Number.isFinite).sort((a, b) => b - a);
  const n = sorted.length;
  if (!n) return [];
  const ranks = new Set<number>();
  for (let r = 1; r <= Math.min(EXACT_TOP_RANKS, n); r += 1) ranks.add(r);
  const step = Math.max(1, Math.round(n * CURVE_STEP_SHARE));
  for (let r = EXACT_TOP_RANKS; r <= n; r += step) ranks.add(r);
  ranks.add(n);
  return [...ranks].sort((a, b) => a - b).map((r) => [r, sorted[r - 1]]);
}

/**
 * Where a score would have finished: 1 + the number of entries that scored
 * strictly more. Exact where the curve holds consecutive ranks (the top 100);
 * interpolated between curve points elsewhere, and flagged as an estimate.
 * `beatShare` is the share of the field that scored below it.
 */
export function estimateRank(score: number, curve: ScoreCurve, entryCount: number):
  { rank: number; beatShare: number; exact: boolean } | null {
  if (!curve.length || !Number.isFinite(score) || entryCount <= 0) return null;
  // Last curve point still strictly above the score.
  let above = -1;
  for (let i = 0; i < curve.length; i += 1) {
    if (curve[i][1] > score) above = i; else break;
  }
  if (above === -1) return { rank: 1, beatShare: 1, exact: true };
  const [r0, s0] = curve[above];
  let higher: number;
  let exact: boolean;
  if (above === curve.length - 1) {
    higher = r0; exact = true;                       // below every entry
  } else {
    const [r1, s1] = curve[above + 1];
    exact = r1 - r0 === 1;
    // As the score falls from s0 to s1, the count of higher entries runs from
    // r0 to r1 - 1 (an entry scoring exactly s1 is not higher).
    const frac = s0 === s1 ? 0 : (s0 - score) / (s0 - s1);
    higher = exact ? r0 : Math.round(r0 + frac * (r1 - 1 - r0));
  }
  return { rank: higher + 1, beatShare: Math.max(0, 1 - higher / entryCount), exact };
}

export interface LineupForScoring {
  lineupNumber: number;
  slots: Array<{ slot: string; name: string; multiplier: number; projection: number }>;
}

export interface ScoredLineup {
  lineupNumber: number;
  actual: number | null;
  projected: number;
  missing: string[];
  captain: string | null;
}

/** Actual DK points for each lineup. A player the export never listed leaves the score unknown. */
export function scoreLineups(
  lineups: readonly LineupForScoring[],
  fptsByName: ReadonlyMap<string, number>,
  normalize: (name: string) => string,
): ScoredLineup[] {
  return lineups.map((lineup) => {
    let actual = 0;
    const missing: string[] = [];
    for (const slot of lineup.slots) {
      const fpts = fptsByName.get(normalize(slot.name));
      if (fpts == null) missing.push(slot.name);
      else actual += fpts * slot.multiplier;
    }
    return {
      lineupNumber: lineup.lineupNumber,
      actual: missing.length ? null : Math.round(actual * 100) / 100,
      projected: Math.round(lineup.slots.reduce((a, s) => a + s.projection, 0) * 100) / 100,
      missing,
      captain: lineup.slots.find((s) => s.slot === "CPT")?.name ?? null,
    };
  });
}

export interface PositionError { position: string; n: number; mae: number; bias: number }

/**
 * Our projection against what DraftKings paid, by position. Bias is actual
 * minus projected, so a negative bias means we projected too high. Ruled-out
 * players and players without a projection are left out: grading a zero we
 * published for an absentee would flatter the model.
 */
export function projectionError(
  players: ReadonlyArray<{ name: string; position: string; ourProj: number | null; isOut: boolean }>,
  fptsByName: ReadonlyMap<string, number>,
  normalize: (name: string) => string,
): PositionError[] {
  const groups = new Map<string, number[][]>();
  for (const p of players) {
    if (p.isOut || p.ourProj == null) continue;
    const fpts = fptsByName.get(normalize(p.name));
    if (fpts == null) continue;
    const rows = groups.get(p.position) ?? [];
    rows.push([p.ourProj, fpts]);
    groups.set(p.position, rows);
  }
  const summarize = (position: string, rows: number[][]): PositionError => ({
    position,
    n: rows.length,
    mae: Math.round((rows.reduce((a, [p, f]) => a + Math.abs(f - p), 0) / rows.length) * 100) / 100,
    bias: Math.round((rows.reduce((a, [p, f]) => a + (f - p), 0) / rows.length) * 100) / 100,
  });
  const out = [...groups].map(([position, rows]) => summarize(position, rows))
    .sort((a, b) => b.mae - a.mae);
  const all = [...groups.values()].flat();
  if (all.length) out.push(summarize("All", all));
  return out;
}
