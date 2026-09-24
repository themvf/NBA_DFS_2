/**
 * Writing a salary slate so that a failure cannot leave a believable lie.
 *
 * Pure: no React, no database, so the rules are testable.
 *
 * ## The failure this exists to prevent
 *
 * `loadNflSalaryCsv` used to persist a slate like this:
 *
 *   1. insert the upload header row (carrying `playerCount`)
 *   2. `for (const player of slate.players) await db.insert(...)`
 *
 * The database client is `drizzle-orm/neon-http`, which has **no interactive
 * transactions** -- every `await db.insert(...)` is its own HTTPS request,
 * committed on its own. A 13-game NFL Classic pool is ~670 players, so that
 * loop was ~670 sequential round-trips with ~670 independent commits, after
 * the header had already been committed by itself.
 *
 * On 2026-09-19 that ran out of road on a real upload. Measured afterwards:
 *
 *   upload b01ae544  DKSalaries (3).csv  playerCount 670  actual rows 10
 *
 * Ten rows -- the first ten of the file, the ten largest salaries. The header
 * row survived, so the slate looked ordinary in the saved-slate dropdown, was
 * the newest for its label, and therefore *shadowed* the complete 670-player
 * upload of the same file sitting next to it. The workspace served a ten-man
 * player pool as though it were a slate.
 *
 * The specific reason the loop stopped (a function time budget, a row that
 * threw, an aborted request) is not the interesting part and is not knowable
 * after the fact. The defect is that **it was able to stop at all and leave
 * something that reads as valid**. Two independent changes fix that:
 *
 *  - `chunkRows` turns ~670 round-trips into a handful of multi-row
 *    upserts, sent as ONE `db.batch(...)` -- which neon-http executes inside a
 *    single server-side transaction, header row included. All of it lands or
 *    none of it does.
 *  - `assertSlateFullyPersisted` re-counts afterwards and refuses to return a
 *    slate whose rows do not match what was parsed, rather than trusting the
 *    transaction claim. A write that half-happened becomes a loud error
 *    instead of a quiet ten-man pool.
 *
 * `incompleteSlateWarning` is the third leg: slates already written by the old
 * path cannot be un-corrupted retroactively, so they are named on sight.
 *
 * ## What actually stopped the loop
 *
 * Established afterwards, and worth recording because the atomicity fix above
 * would have hidden it rather than prevented it: the two tables' CHECK
 * constraints on `projection_status` had drifted apart, and the 11th row of
 * that file was the first ruled-out player. See `SLATE_PLAYER_STATUSES`.
 */

/**
 * Rows per upsert statement.
 *
 * Postgres caps a statement at 65,535 bound parameters. A slate player binds
 * ~28 columns, so the hard ceiling is ~2,300 rows; 250 leaves a wide margin
 * and keeps any single statement's payload modest, since `identityEvidence`
 * carries a roster excerpt per row. A 670-player Classic slate becomes 3
 * statements, a 68-player Showdown slate becomes 1.
 */
export const SLATE_WRITE_CHUNK = 250;

/** Split rows into `size`-length groups, preserving order. Empty in, empty out. */
export function chunkRows<T>(rows: readonly T[], size: number = SLATE_WRITE_CHUNK): T[][] {
  if (size < 1) throw new Error("Chunk size must be at least 1.");
  const out: T[][] = [];
  for (let i = 0; i < rows.length; i += size) out.push(rows.slice(i, i + size));
  return out;
}

/**
 * The write is only trustworthy if the pool that came back is the pool that
 * went in. Throws with both numbers, because "the slate is incomplete" without
 * saying how incomplete is not actionable.
 *
 * Deliberately an exact equality, not a floor: more rows than were parsed means
 * the upload id collided with someone else's players, which is worse than
 * fewer, not better.
 */
export function assertSlateFullyPersisted(parsed: number, persisted: number, fileName: string): void {
  if (parsed === persisted) return;
  throw new Error(
    `${fileName} was not saved completely: parsed ${parsed} players but stored ${persisted}. `
      + `Nothing was left half-written on purpose — upload the file again.`,
  );
}

/**
 * A sentence for the slate's warning list when a stored slate holds fewer
 * players than its own header claims, or null when it is whole.
 *
 * This is the read-side detector for damage the old write path already did.
 * It cannot repair the slate: the missing rows would have to be re-derived
 * from the original CSV against this slate's own projection run, and copying
 * them from a twin upload linked to a DIFFERENT run would attach that run's
 * projections to this one's identity -- the exact silent mismatch the rest of
 * this workspace exists to prevent.
 */
export function incompleteSlateWarning(claimed: number, actual: number, fileName?: string | null): string | null {
  if (actual >= claimed) return null;
  const file = fileName ? `${fileName} ` : "";
  return `This saved slate is incomplete: ${file}was read as ${claimed} players but only ${actual} were stored,`
    + ` so ${claimed - actual} are missing from the pool. It was written by an older upload path that could stop`
    + ` part-way and still leave the slate looking whole. Nothing here can be trusted as a full player pool —`
    + ` upload the DraftKings CSV again to rebuild it.`;
}

/** Whether a stored slate is whole enough to be offered as a choice at all. */
export function isSlateComplete(claimed: number, actual: number): boolean {
  return actual >= claimed && actual > 0;
}

/**
 * What `nfl_dfs_player_projections.projection_status` can hold. This is the
 * upstream vocabulary: the Python projection pipeline writes it, and
 * `model/nfl_dfs_availability.py`'s `zero_out()` is what introduced `out`.
 */
export const PROJECTION_RUN_STATUSES = ["historical", "position_prior", "unavailable", "out"] as const;

/**
 * What `nfl_dfs_slate_players.projection_status` can hold.
 *
 * `loadNflSalaryCsv` copies the projection row's status across verbatim and
 * falls back to `unmatched` for a salary row with no projection, so this set
 * must be a SUPERSET of `PROJECTION_RUN_STATUSES` plus that fallback.
 *
 * It was not. The two CHECK constraints were declared independently in
 * `db/schema.py` and drifted: the projections table gained `out`, the slate
 * table never did. Nothing detected it while the availability feed was empty,
 * because a run with no absent players emits no `out` rows. The moment a run
 * did (86 of 1,086 on 2026-09-19), the first ruled-out player in the salary
 * file became unwritable and killed the upload mid-loop.
 *
 * `assertSlateStatusVocabulary` pins the relationship so the next value added
 * upstream fails a test here instead of a write in production.
 */
/**
 * Statuses the WRITE layer assigns itself, on top of what a run emits.
 * `storedSlateProjection` rewrites a position-average row to "unsupported"
 * (out-projection.ts). It was added on 2026-09-23 without being added here, and
 * the first slate containing such a row -- the Thursday ATL@GB showdown, 13 of
 * 53 players -- failed to upload with a CHECK violation that production
 * reported only as "An error occurred in the Server Components render".
 * Same failure class as `out` above, one layer further down.
 */
export const WRITE_LAYER_STATUSES = ["unsupported"] as const;

export const SLATE_PLAYER_STATUSES = [...PROJECTION_RUN_STATUSES, "unmatched", ...WRITE_LAYER_STATUSES] as const;

/** Throws if the slate table could not store something a projection run emits. */
export function assertSlateStatusVocabulary(): void {
  const slate = new Set<string>(SLATE_PLAYER_STATUSES);
  const missing = [...PROJECTION_RUN_STATUSES, ...WRITE_LAYER_STATUSES].filter((status) => !slate.has(status));
  if (missing.length) {
    throw new Error(
      `nfl_dfs_slate_players cannot store projection status(es) ${missing.join(", ")}, `
        + `which nfl_dfs_player_projections is allowed to emit.`,
    );
  }
}
