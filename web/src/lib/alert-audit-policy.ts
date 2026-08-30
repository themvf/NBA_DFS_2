// Deliberately NOT `server-only`: both consumers are client components
// ("use client"), and this is pure display policy over already-fetched rows —
// no secrets, no data access.

/**
 * Single source of truth for when an alert-audit number may be DISCLOSED.
 *
 * This exists because the rule diverged. `line-alerts-panel.tsx` (MLB, soccer,
 * tennis) enforced a 30-alert floor; the NFL page has its own inline audit
 * table that enforced nothing, and was publishing a win rate for every
 * detector — "25.0%" off a 2-6 record. Same ledger, same question, two
 * different answers depending on which page you opened.
 *
 * Extracting the constant into a component would not have fixed that; a second
 * component would just grow a second copy. The policy lives here, both tables
 * import it, and the floors are now structurally incapable of diverging.
 *
 * The floor mirrors `_MIN_SETTLED_FOR_CI` in `model/line_alerts.py`. If that
 * moves, this moves with it — they answer the same question and a UI more
 * permissive than the engine behind it is a lie about how much is known.
 */
export const MIN_SETTLED_FOR_CI = 30;
export const TENNIS_PIN_FORWARD_TARGET = 100;

/** Above this mean CLV a detector is "early"; below it the move was absorbed. */
export const CLV_SIGNAL_PP = 0.5;

export type AuditRow = {
  alertType: string;
  n: number;
  nClv: number;
  avgClvPp: number | null;
  winRate: number | null;
  wins: number;
  losses: number;
  pushes: number;
};

export type Disclosure = {
  /** May a RATE or INTERVAL be rendered for this row? */
  disclosable: boolean;
  /** Graded observations behind the row. */
  settled: number;
  /** How many more are needed. 0 once disclosable. */
  needed: number;
  /** Shown in place of the withheld number. */
  lockLabel: string;
  /** Why it is withheld — surfaced on hover, never hidden. */
  reason: string;
};

/**
 * Raw counts are always permitted; derived rates are not.
 *
 * "2-6" is an observation. "25.0%" is an inference the sample cannot support,
 * and the eye reads a percentage as a finding no matter what the caption says.
 * That distinction is the whole rule.
 */
export function validationTarget(row: Pick<AuditRow, "alertType"> | { alertType?: string }): number {
  return row.alertType === "pinnacle_favorite_forward"
    ? TENNIS_PIN_FORWARD_TARGET
    : MIN_SETTLED_FOR_CI;
}

export function disclosure(row: Pick<AuditRow, "nClv"> & { alertType?: string }): Disclosure {
  const settled = row.nClv ?? 0;
  const target = validationTarget(row);
  const ok = settled >= target;
  const needed = Math.max(0, target - settled);
  return {
    disclosable: ok,
    settled,
    needed,
    lockLabel: ok ? "" : `needs ${needed} more`,
    reason: ok
      ? ""
      : `${settled} graded alert(s). Below the ${target}-alert floor ` +
        `used by model/line_alerts.py, so no rate or interval is computed — raw ` +
        `counts only. Same-slate alerts are correlated and carry less ` +
        `information than the count suggests.`,
  };
}

export type Verdict = { label: string; cls: string; tip: string };

/**
 * Green is never returned. It is reserved for a passed validation gate, and no
 * detector in this system has cleared one — positive CLV means the alert was
 * early, not that the detector is profitable.
 */
export function verdict(row: Pick<AuditRow, "nClv" | "avgClvPp"> & { alertType?: string }): Verdict {
  const d = disclosure(row);
  if (!d.disclosable) {
    return {
      label: row.alertType === "pinnacle_favorite_forward"
        ? `forward test ${d.settled}/${TENNIS_PIN_FORWARD_TARGET}`
        : "accruing",
      cls: "bg-gray-100 text-gray-500",
      tip: d.reason,
    };
  }
  return (row.avgClvPp ?? 0) > CLV_SIGNAL_PP
    ? {
        label: "positive CLV",
        cls: "bg-amber-100 text-amber-800",
        tip:
          "The market kept moving toward the flagged side after we fired, over " +
          "a sample past the floor. Evidence the detector is early — NOT a " +
          "validated edge and not a recommendation to bet.",
      }
    : {
        label: "no CLV",
        cls: "bg-red-100 text-red-600",
        tip:
          "The move was already absorbed by the time we detected it. Per the " +
          "standing rule, an alert type with no positive CLV is noise and is a " +
          "retirement candidate.",
      };
}

/**
 * Multiplicity warning. Measuring k detectors at once means the best-looking
 * one is expected to look good by chance; this is the number that says how
 * likely that is, and it belongs ABOVE the table where it cannot be dismissed.
 */
export function multiplicityNote(detectorCount: number): string | null {
  if (detectorCount < 2) return null;
  const pct = Math.round((1 - Math.pow(0.95, detectorCount)) * 100);
  return (
    `${detectorCount} detectors are measured simultaneously. At a 5% per-test ` +
    `error rate there is roughly a ${pct}% chance at least one shows a spurious ` +
    `positive. One detector clearing the floor is not a discovery.`
  );
}
