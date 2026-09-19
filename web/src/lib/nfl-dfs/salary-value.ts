/**
 * Salary value for the DFS Lab player pool: how many DK points a player is
 * projected to return per $1,000 of salary, and whether that is good.
 *
 * Pure: no React, no database, so the tiering rule can be tested directly
 * rather than inferred from a coloured chip.
 *
 * The multiple itself is the vocabulary this repo already uses --
 * `competitor-benchmark.ts` grades its top-value picks on 3x and 4x salary
 * hits, and `workload-scenario.ts` reports a 2x/3x/4x ladder. This module
 * computes the same quantity for the projection rather than for an outcome.
 *
 * Two measured facts drive the design. Both were checked against a real
 * 13-game Classic slate (567 usable players) BEFORE the thresholds were
 * chosen, because the obvious design fails on both.
 *
 * 1. A FLAT THRESHOLD IS A QUARTERBACK DETECTOR, NOT A VALUE DETECTOR.
 *    Median projected multiple by position on that slate: QB 2.94, DST 2.13,
 *    WR 1.83, TE 1.73, RB 1.40. At a flat 3x bar, 48% of quarterbacks qualify
 *    and 2% of running backs do. DK Classic rosters exactly one QB and cannot
 *    swap a RB for one, so the decision-relevant question is "good value for a
 *    RB", not "better value than a QB". Tiers are therefore per position,
 *    against the slate's own distribution -- the same rule the fantasy-football
 *    weekly grid already uses for its heat shading, and for the same reason.
 *
 * 2. A POSITION-PRIOR PROJECTION IS NOT EVIDENCE OF VALUE. Six of the nine
 *    top-decile quarterbacks were `position_prior` rows with zero games of
 *    history, priced at the $4,000 minimum: the position average divided by a
 *    floor salary mechanically yields ~4x. Those rows are excluded from the
 *    reference distribution (their presence inflated QB's p90 from 3.37 to
 *    3.82, which would then have suppressed real quarterbacks) and they never
 *    receive a value tier of their own -- they are labelled `unproven`, with
 *    the multiple still shown. The number is not hidden; the claim is.
 */
import { DK_SALARY_CAP } from "./dk-salary-csv";

/** DK Classic rosters nine players under one cap. */
export const CLASSIC_ROSTER_SIZE = 9;

/**
 * The lowest multiple that can be called value in absolute terms, applied on
 * top of the per-position rank as a conjunctive floor.
 *
 * This is arithmetic, not a taste: a full roster every one of whose players
 * returns exactly `m` times salary scores `m * DK_SALARY_CAP / 1000` points.
 * At 2.0 that is 100 points from the whole cap, which no competitive NFL
 * Classic score is near -- so a sub-2x player cannot be good value however
 * the rest of its position is priced. The floor exists so that a slate whose
 * entire pool is badly priced cannot crown its least-bad option.
 */
export const ABSOLUTE_VALUE_FLOOR = 2.0;

/** Points a full roster scores if every player returns exactly this multiple. */
export const rosterPointsAtMultiple = (multiple: number) =>
  (multiple * DK_SALARY_CAP) / 1000;

/**
 * Fewer comparable players than this and the slate cannot support a quantile,
 * so the multiple is reported with no tier rather than with a fabricated one.
 * Showdown pools are small enough for this to bite.
 */
export const MIN_POOL_FOR_TIER = 8;

export type ValueTier = "elite" | "strong" | "fair" | "poor" | "unproven" | "unknown";

export type ValuePlayerInput = {
  position: string;
  salary: number;
  ourProj: number | null;
  ceilingFpts?: number | null;
  /** `historical` means the projection rests on this player's own games. */
  projectionStatus?: string | null;
  isOut?: boolean;
};

export type ValueAssessment = {
  /** Projected DK points per $1,000 of salary. Null when not computable. */
  multiple: number | null;
  /** P90 points per $1,000, for upside context. Null when no distribution. */
  ceilingMultiple: number | null;
  tier: ValueTier;
  /** The position's slate distribution this was graded against, when tiered. */
  positionMedian: number | null;
  positionP75: number | null;
  positionP90: number | null;
  /** How many comparable players formed that reference. */
  referenceCount: number;
  /** One sentence naming what the tier rests on, or why there is none. */
  reason: string;
};

export type ValueIndex = {
  assess: (player: ValuePlayerInput) => ValueAssessment;
  /** Per-position reference distributions, for a legend or a test. */
  references: Map<string, { count: number; median: number; p75: number; p90: number; p25: number }>;
};

/** Points per $1,000 of salary. Null unless both inputs are usable. */
export function salaryMultiple(points: number | null | undefined, salary: number | null | undefined): number | null {
  if (points === null || points === undefined || !Number.isFinite(points)) return null;
  if (salary === null || salary === undefined || !Number.isFinite(salary) || salary <= 0) return null;
  return points / (salary / 1000);
}

/** Linear-interpolated quantile over an already-finite sample. */
function quantile(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  if (sorted.length === 1) return sorted[0];
  const index = (sorted.length - 1) * p;
  const low = Math.floor(index);
  return sorted[low] + (sorted[Math.ceil(index)] - sorted[low]) * (index - low);
}

/** A row may anchor the reference distribution only if it is a real, live projection. */
function isReferenceEligible(player: ValuePlayerInput): boolean {
  return (
    player.isOut !== true
    && player.projectionStatus === "historical"
    && salaryMultiple(player.ourProj, player.salary) !== null
  );
}

/**
 * Build the per-position reference distributions once for a slate, then grade
 * each player against its own position. Building once matters: the pool is
 * re-rendered on every keystroke in the search box and every page change.
 */
export function buildValueIndex(pool: readonly ValuePlayerInput[]): ValueIndex {
  const samples = new Map<string, number[]>();
  for (const player of pool) {
    if (!isReferenceEligible(player)) continue;
    const multiple = salaryMultiple(player.ourProj, player.salary) as number;
    const bucket = samples.get(player.position);
    if (bucket) bucket.push(multiple);
    else samples.set(player.position, [multiple]);
  }

  const references: ValueIndex["references"] = new Map();
  for (const [position, values] of samples) {
    values.sort((a, b) => a - b);
    references.set(position, {
      count: values.length,
      p25: quantile(values, 0.25),
      median: quantile(values, 0.5),
      p75: quantile(values, 0.75),
      p90: quantile(values, 0.9),
    });
  }

  const assess = (player: ValuePlayerInput): ValueAssessment => {
    const multiple = salaryMultiple(player.ourProj, player.salary);
    const ceilingMultiple = salaryMultiple(player.ceilingFpts ?? null, player.salary);
    const reference = references.get(player.position) ?? null;
    const base = {
      multiple,
      ceilingMultiple,
      positionMedian: reference?.median ?? null,
      positionP75: reference?.p75 ?? null,
      positionP90: reference?.p90 ?? null,
      referenceCount: reference?.count ?? 0,
    };

    if (multiple === null) {
      return { ...base, tier: "unknown", reason: "No projection or salary, so value cannot be computed." };
    }
    if (player.isOut === true) {
      return { ...base, tier: "unknown", reason: "Ruled out, so the projection is not a live price." };
    }
    if (player.projectionStatus !== "historical") {
      // Measured: a min-salary player carrying the position average returns
      // ~4x by construction. The multiple is real arithmetic; the value claim
      // would not be, so no tier is issued.
      return {
        ...base,
        tier: "unproven",
        reason: "Projection comes from position peers, not this player's own games, so the multiple is not evidence of value.",
      };
    }
    if (!reference || reference.count < MIN_POOL_FOR_TIER) {
      return {
        ...base,
        tier: "unknown",
        reason: `Only ${reference?.count ?? 0} ${player.position}s on this slate carry their own game history — too few to rank against.`,
      };
    }

    const floorNote = ` A full roster at ${ABSOLUTE_VALUE_FLOOR.toFixed(1)}x scores only ${rosterPointsAtMultiple(ABSOLUTE_VALUE_FLOOR).toFixed(0)} from the whole cap.`;
    if (multiple >= reference.p90 && multiple >= ABSOLUTE_VALUE_FLOOR) {
      return { ...base, tier: "elite", reason: `Top 10% of the ${reference.count} ${player.position}s on this slate with their own game history.` };
    }
    if (multiple >= reference.p75 && multiple >= ABSOLUTE_VALUE_FLOOR) {
      return { ...base, tier: "strong", reason: `Top 25% of the ${reference.count} ${player.position}s on this slate with their own game history.` };
    }
    if (multiple < reference.p25) {
      return { ...base, tier: "poor", reason: `Bottom 25% of the ${reference.count} ${player.position}s on this slate with their own game history.` };
    }
    if (multiple < ABSOLUTE_VALUE_FLOOR) {
      return {
        ...base,
        tier: "fair",
        reason: `Mid-pack for a ${player.position} here, and under ${ABSOLUTE_VALUE_FLOOR.toFixed(1)}x.${floorNote}`,
      };
    }
    return { ...base, tier: "fair", reason: `Mid-pack among the ${reference.count} ${player.position}s on this slate with their own game history.` };
  };

  return { assess, references };
}

/** Short label for a chip. */
export const VALUE_TIER_LABEL: Record<ValueTier, string> = {
  elite: "Elite value",
  strong: "Strong value",
  fair: "Fair value",
  poor: "Overpriced",
  unproven: "Unproven",
  unknown: "No value read",
};
