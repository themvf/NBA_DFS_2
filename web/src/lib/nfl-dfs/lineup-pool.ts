/**
 * The final lineup set: lineups picked from several saved runs of one slate,
 * exported together. The pool stores only references (run id and lineup
 * number); rosters are read back from the saved runs, and the server re-reads
 * and re-checks every one at export. Kept in this browser per slate.
 */
import { rosterKey, type BreakdownLineup } from "./portfolio-breakdown";

export const LINEUP_POOL_VERSION = "nfl-lineup-pool-v1";
/** DraftKings' per-contest entry ceiling. */
export const MAX_POOL_LINEUPS = 150;

export interface PoolPick { runId: string; lineupNumber: number }

export const pickKey = (pick: PoolPick) => `${pick.runId}:${pick.lineupNumber}`;

export interface AddResult {
  picks: PoolPick[];
  added: number;
  /** Already in the pool, or the same roster as a lineup already in it. */
  duplicates: number;
  /** Left out because the pool would pass `MAX_POOL_LINEUPS`. */
  overLimit: number;
}

/**
 * Add lineups from one run, in order, skipping ones already in the pool and
 * rosters identical to a pool lineup. `rosterOf` returns the roster key of a
 * pick already in the pool (from any run), or null when it is not loaded.
 */
export function addToPool(pool: readonly PoolPick[], runId: string, lineups: readonly BreakdownLineup[],
  rosterOf: (pick: PoolPick) => string | null): AddResult {
  const keys = new Set(pool.map(pickKey));
  const rosters = new Set(pool.map(rosterOf).filter((r): r is string => r != null));
  const picks = [...pool];
  let added = 0, duplicates = 0, overLimit = 0;
  for (const lineup of lineups) {
    const pick = { runId, lineupNumber: lineup.lineupNumber };
    const roster = rosterKey(lineup);
    if (keys.has(pickKey(pick)) || rosters.has(roster)) { duplicates += 1; continue; }
    if (picks.length >= MAX_POOL_LINEUPS) { overLimit += 1; continue; }
    picks.push(pick); keys.add(pickKey(pick)); rosters.add(roster); added += 1;
  }
  return { picks, added, duplicates, overLimit };
}

export function removeFromPool(pool: readonly PoolPick[], remove: readonly PoolPick[]): PoolPick[] {
  const drop = new Set(remove.map(pickKey));
  return pool.filter((p) => !drop.has(pickKey(p)));
}

export interface PoolOverlap {
  /** Most players any two pool lineups share. */
  maxShared: number;
  /** Lineup pairs sharing at least `threshold` players. */
  closePairs: number;
  threshold: number;
}

/** How alike the pool's lineups are; `threshold` is the roster size minus the build's min-unique. */
export function poolOverlap(lineups: readonly BreakdownLineup[], threshold: number): PoolOverlap {
  const sets = lineups.map((l) => new Set(l.slots.map((s) => s.player.dkPlayerId)));
  let maxShared = 0, closePairs = 0;
  for (let i = 0; i < sets.length; i++) for (let j = i + 1; j < sets.length; j++) {
    let shared = 0;
    for (const id of sets[i]) if (sets[j].has(id)) shared += 1;
    maxShared = Math.max(maxShared, shared);
    if (shared >= threshold) closePairs += 1;
  }
  return { maxShared, closePairs, threshold };
}

/** Browser storage key; one pool per saved slate. */
export const poolStorageKey = (uploadId: string) => `${LINEUP_POOL_VERSION}:${uploadId}`;

export function parsePool(raw: string | null): PoolPick[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((p): p is PoolPick => typeof p === "object" && p != null
      && typeof (p as PoolPick).runId === "string" && /^[0-9a-f-]{36}$/.test((p as PoolPick).runId)
      && Number.isInteger((p as PoolPick).lineupNumber) && (p as PoolPick).lineupNumber > 0)
      .slice(0, MAX_POOL_LINEUPS);
  } catch { return []; }
}

/** The same picks, checked before export: non-empty, within the limit, no repeats. */
export function validatePicks(picks: readonly PoolPick[]): string | null {
  if (!picks.length) return "The final set is empty. Add lineups from a saved run first.";
  if (picks.length > MAX_POOL_LINEUPS) return `The final set has ${picks.length} lineups; DraftKings allows at most ${MAX_POOL_LINEUPS}.`;
  if (new Set(picks.map(pickKey)).size !== picks.length) return "The final set lists the same lineup twice.";
  for (const p of picks) if (!/^[0-9a-f-]{36}$/.test(p.runId) || !Number.isInteger(p.lineupNumber) || p.lineupNumber < 1)
    return "The final set has an invalid lineup reference.";
  return null;
}
