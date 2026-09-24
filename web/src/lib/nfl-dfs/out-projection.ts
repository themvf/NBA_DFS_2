/**
 * What a slate shows for a player who is not playing.
 *
 * Pure: no React, no database, so both read sites (the pool table and the
 * projection drawer) can apply one rule and cannot drift apart.
 *
 * ## Why this exists
 *
 * Two availability sources meet on a slate and they do not agree:
 *
 *  - **DraftKings' own `Status` column**, parsed into `isOut` by
 *    `dk-salary-csv.ts`. This is what greys the row and what the optimizer
 *    excludes.
 *  - **The model's availability pipeline** (`model/nfl_dfs_availability.py`),
 *    which zeroes an absent player and stamps `rule: "zeroed"` on him. It runs
 *    off our own captured injury observations, NOT off the DK file.
 *
 * Measured on a real 13-game slate: DK flagged 77 players OUT, while the
 * projection run stamped **zero** of its 1,086 rows `projection_status='out'`
 * and attached no availability note to any of them -- the observation feed was
 * empty. So 74 of those 77 DK-OUT rows still carried a live projection, Nico
 * Collins among them at 17.44 points. The optimizer never rostered him
 * (`lineups.ts` rejects `isOut`), but every surface that reads a projection
 * was quoting a number for a player who is not playing.
 *
 * `zeroOutProjection` closes that gap at the slate layer, matching
 * `zero_out()` in the Python module field for field so the two cannot mean
 * different things by "out".
 *
 * ## What is deliberately NOT zeroed
 *
 * Only OUR projection and its derived interval. `avgFptsDk`,
 * `fantasyprosProj` and `linestarProj` are other people's numbers; zeroing
 * them would misreport what those sources actually published. The optimizer
 * excludes OUT players under every projection source anyway, so nothing is
 * rostered on the strength of them.
 *
 * The immutable `nfl_dfs_player_projections` row is never touched either.
 * This is a read-time slate decision, the same separation the MLB ledger
 * keeps between its decision record and its display cache.
 */

/** Matches `projection_status` written by the Python `zero_out()`. */
export const OUT_PROJECTION_STATUS = "out";

/**
 * The model's own word for "this number is his position's average, not his":
 * `model/nfl_dfs_historical.py` stamps `position_prior` when a player has
 * fewer than `minimum_historical_games` of his own, and for him it sets
 * `player_strength = 0.0` so EVERY draw in the simulation comes from peers.
 * Those peers are rows where somebody recorded stats -- i.e. starters. So a
 * third-string quarterback is handed the average NFL start.
 */
export const POSITION_PRIOR_STATUS = "position_prior";

/** What a slate stores once it refuses to publish that number. */
export const UNSUPPORTED_PROJECTION_STATUS = "unsupported";

/**
 * A position average is not a projection of this player, so we do not publish
 * one for him.
 *
 * Measured on the 2026 week-1 and week-2 slates, one observation per player
 * per week, listed-and-not-ruled-out only (n = 435 position-prior rows):
 *
 *   projected  7.15      actual  0.70      92% scored 3 points or fewer
 *   quarterbacks: projected 13.97, actual 0.04, 52 of 52 scored 3 or fewer
 *
 * against `historical` rows on the same slates, which project 7.00 and score
 * 5.28. The number is not merely noisy; it is wrong nine times in ten, and
 * for quarterbacks it was wrong every single time.
 *
 * This is a DISPLAY and ELIGIBILITY decision, not a new prior. The immutable
 * `nfl_dfs_player_projections` row keeps the model's number untouched, and
 * `model/nfl_dfs_historical.py` is not edited -- the registered v4 study
 * (docs/nfl-dfs-v4-zero-history-prior-study.md) owns whether a BETTER number
 * is possible, is frozen to weeks 4-10, and must not be pre-empted. What is
 * fixed here is narrower and is the thing that study explicitly carves out:
 * "This study cannot distinguish a promoted backup from a healthy scratch;
 * that is the QB1/depth gate's job, not the prior's."
 *
 * The seam for that study: this gates on the STATUS, not on the game count.
 * If a future prior produces a number that is genuinely about the player, it
 * stamps its own status and this rule stops applying to it without being
 * touched.
 *
 * Deliberately NOT extended to `hist_1_5` players. A player with one or two
 * games of his own carries real evidence (report-card bias -3.18 against
 * hist_0's -6.27), and silencing him would remove a genuine week-1 rookie
 * starter along with the scratches.
 */
export function isUnsupportedProjection(projectionStatus: string): boolean {
  return projectionStatus === POSITION_PRIOR_STATUS;
}

export type ZeroableProjection = {
  projectionStatus: string;
  ourProj: number | null;
  floorFpts: number | null;
  ceilingFpts: number | null;
  boomRate: number | null;
};

/**
 * A player who is not playing scores zero. Not shrunk, not hidden -- zero,
 * with the status saying why. Returns the row unchanged when he is playing,
 * so callers can apply it unconditionally.
 */
export function zeroOutProjection<T extends ZeroableProjection>(row: T, isOut: boolean): T {
  // Ruled out wins over unsupported: he is not playing, which is a stronger
  // statement than "we cannot say what he would do".
  if (!isOut && isUnsupportedProjection(row.projectionStatus)) {
    return { ...row, projectionStatus: UNSUPPORTED_PROJECTION_STATUS,
             ourProj: null, floorFpts: null, ceilingFpts: null, boomRate: null };
  }
  if (!isOut) return row;
  return {
    ...row,
    projectionStatus: OUT_PROJECTION_STATUS,
    ourProj: 0,
    floorFpts: 0,
    ceilingFpts: 0,
    boomRate: 0,
  };
}

/** The projection fields the immutable run row exposes to a slate write. */
export type RunProjectionFields = {
  projectionStatus: string;
  modelProjFpts: number | null;
  floorFpts: number | null;
  medianFpts: number | null;
  ceilingFpts: number | null;
  boomRate: number | null;
};

/**
 * What a slate row STORES for its projection. The same rule as
 * `zeroOutProjection`, applied at write time instead of read time: a
 * DK-flagged OUT player is stored at zero with status `out`, so the stored
 * row, the report cards and every export agree with what the page shows.
 * A missing projection stays `unmatched` with nulls -- absence is not zero.
 */
export function storedSlateProjection(projection: RunProjectionFields | null | undefined, isOut: boolean) {
  if (isOut) {
    return { projectionStatus: OUT_PROJECTION_STATUS, ourProj: 0, floorFpts: 0, medianFpts: 0, ceilingFpts: 0, boomRate: 0 };
  }
  // A position average is not this player's projection; absence is the honest
  // encoding, and it is what every downstream consumer already treats as "no
  // usable number". The model's own value survives on the immutable run row.
  if (projection && isUnsupportedProjection(projection.projectionStatus)) {
    return { projectionStatus: UNSUPPORTED_PROJECTION_STATUS, ourProj: null,
             floorFpts: null, medianFpts: null, ceilingFpts: null, boomRate: null };
  }
  return {
    projectionStatus: projection?.projectionStatus ?? "unmatched",
    ourProj: projection?.modelProjFpts ?? null,
    floorFpts: projection?.floorFpts ?? null,
    medianFpts: projection?.medianFpts ?? null,
    ceilingFpts: projection?.ceilingFpts ?? null,
    boomRate: projection?.boomRate ?? null,
  };
}

/**
 * The availability note as the Python module writes it onto a projection row.
 * `rule` is `"zeroed"` on the absent player and `"inherits"` on whoever picks
 * up his work -- two different players, never the same row.
 */
export type ModelAvailabilityNote = {
  rule?: string | null;
  status?: string | null;
  applied?: boolean | null;
  from_player?: string | null;
  multiplier?: number | string | null;
  capped?: boolean | null;
  reason?: string | null;
};

const num = (value: unknown): number | null => {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

/**
 * One sentence about why this player's projection looks the way it does.
 *
 * The bug this replaces: the old copy read "projection zeroed, opportunity
 * handed to the backup" for **every** row stamped `rule: "zeroed"`. But that
 * stamp lands on the absent player, and it carries no information about
 * whether anyone actually inherited his work. In production a transfer only
 * ever happens for quarterbacks -- `apply()` defaults to `positions=("QB",)`
 * and `ingest/nfl_dfs_projections.py` passes no override -- so for a receiver
 * the sentence asserted a redistribution that had not occurred.
 *
 * Each branch below now states only what its own evidence supports.
 *
 * `redistributed` is the slate layer's answer to the same question, from
 * `opportunity-redistribution.ts`: it knows whether this player's work was
 * actually placed with teammates, which the model's own note never can.
 * Passing it turns the DK branch from a denial into a statement of fact.
 */
export function availabilityNote(
  note: ModelAvailabilityNote | null | undefined,
  dk: { isOut: boolean; dkStatus?: string | null },
  redistributed?: { paidTo: string[]; units: string[] } | null,
): string | null {
  const rule = note?.rule ?? null;

  if (rule === "inherits") {
    const from = note?.from_player ? `${note.from_player}'s` : "an absent teammate's";
    if (note?.applied) {
      const multiplier = num(note.multiplier);
      const scale = multiplier === null ? "" : ` (volume ×${multiplier.toFixed(2)}${note.capped ? ", capped" : ""})`;
      return `Inherits ${from} opportunity${scale}. Volume is scaled to the absent player; this player keeps his own efficiency.`;
    }
    return `Next man up for ${from} workload, but no transfer was applied${note?.reason ? `: ${note.reason}` : "."}`;
  }

  if (rule === "zeroed") {
    // Deliberately says nothing about a handoff: this row does not know
    // whether one happened, and for a non-quarterback it did not.
    return `Ruled ${note?.status ?? "out"} by the model's availability feed — projection zeroed.`;
  }

  if (dk.isOut) {
    // The common case on a real slate: DK says out, our own feed never saw it.
    const status = dk.dkStatus ? String(dk.dkStatus).trim().toUpperCase() : "OUT";
    const zeroed = `DraftKings lists this player as ${status}, so his projection is zeroed here.`;
    if (redistributed && redistributed.paidTo.length > 0) {
      const units = [...new Set(redistributed.units)].join(" and ");
      return `${zeroed} His ${units} are redistributed on this slate to ${redistributed.paidTo.join(", ")}.`;
    }
    if (redistributed) {
      // Out, and we tried: say that nothing was placed rather than implying
      // we never looked.
      return `${zeroed} No teammate could be paid his opportunity — see the slate's redistribution report for why.`;
    }
    return `${zeroed} The model's own availability feed has no observation for him, so no teammate inherited his opportunity.`;
  }

  return null;
}
