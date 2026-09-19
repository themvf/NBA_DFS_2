/**
 * Warn when a slate is pinned to an out-of-date projection run.
 *
 * Pure: no React, no database, so the rule is testable.
 *
 * ## The failure this exists to make visible
 *
 * A slate stores `projection_run_id` at upload time and never moves. That is
 * correct -- a saved slate should keep reproducing the numbers it was built
 * from. But it means a model fix can ship and a slate can quietly keep using
 * the version from before it, with nothing on screen saying so.
 *
 * It already happened, and it silently disabled a feature. `OFFENSE_FIELDS`
 * gained `attempts` and `carries` on 2026-09-17 specifically so a ruled-out
 * player could hand on his volume. A slate uploaded on 09-18 was linked to a
 * run from 09-16, so:
 *
 *   linked run (v2, 09-16): `attempts` on    46 of 1086 rows, 0 above zero
 *   newest run (v3, 09-19): `attempts` on 1054 of 1086 rows, 461 above zero
 *
 * `opportunity-redistribution.ts` keys its `pass` pool on `attempts` and its
 * `rush` pool on `carries`. With both absent, a ruled-out quarterback handed
 * on nothing and a ruled-out back handed on nothing -- every transfer on the
 * board came from `receptions` alone. Nothing was broken and nothing threw;
 * the pools just had no input. That is the same shape as the dead
 * `scan_tennis_totals` detector recorded in CLAUDE.md: a run that "found
 * nothing" is indistinguishable in logs from one that is structurally unable
 * to find anything.
 *
 * Re-uploading the same DK CSV is the intended remedy: `loadNflSalaryCsv`
 * keys an existing upload on (file digest, run id), so once the run changes
 * the same file produces a fresh slate on the current model.
 */

export type RunStamp = {
  runId: string;
  modelVersion: string | null;
  asOfAt: Date | string | null;
};

const iso = (value: Date | string | null): string | null => {
  if (value === null) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
};

const day = (value: Date | string | null): string => iso(value)?.slice(0, 10) ?? "an unknown date";

/**
 * A sentence for the slate's warning list, or null when the slate is current.
 *
 * Deliberately names both runs and both dates: "your slate is stale" without
 * saying how stale, or what it would move to, is not actionable.
 */
export function staleRunWarning(linked: RunStamp | null, newest: RunStamp | null): string | null {
  if (!linked || !newest) return null;
  if (linked.runId === newest.runId) return null;

  const linkedAt = iso(linked.asOfAt);
  const newestAt = iso(newest.asOfAt);
  // Only warn when the other run is genuinely NEWER. An older run can be
  // linked on purpose -- reproducing a past decision is a legitimate thing to
  // do, and nagging about it would train the warning to be ignored.
  if (linkedAt && newestAt && newestAt <= linkedAt) return null;

  const versions = linked.modelVersion && newest.modelVersion && linked.modelVersion !== newest.modelVersion
    ? ` The model version also changed, ${linked.modelVersion} to ${newest.modelVersion}.`
    : "";
  return `This slate is pinned to the projection run from ${day(linked.asOfAt)};`
    + ` a newer one from ${day(newest.asOfAt)} is available.${versions}`
    + ` Projections, and anything derived from them, still reflect the older run --`
    + ` a ruled-out player can only hand on opportunity his own run recorded.`
    + ` Re-upload the same DraftKings CSV to rebuild this slate on the current model.`;
}
