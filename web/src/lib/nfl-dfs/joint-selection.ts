/**
 * Joint portfolio selection (nfl-joint-selection-v1, 2026-10-23).
 *
 * The sequential builder commits each lineup and spends exposure as it goes,
 * so an early lineup can use up a player a later captain combination needed
 * (TNF NE@CHI 2026-10-22: Loveland hit his 10-of-20 cap by lineup 12, before
 * Burden's captain lineups were built). Here the portfolio is chosen in one
 * integer program over a pool of candidate lineups: the sequential portfolio
 * itself plus extra candidates built without spending exposure.
 *
 * Portfolio rules are constraints on per-lineup features (a player's
 * appearances, a two-QB lineup, an archetype label, ...). Each rule has a
 * target range; missing it costs a large penalty per unit rather than making
 * the model infeasible, so the solver meets every rule it can and the caller
 * recounts and reports any it could not. The sequential portfolio is always a
 * solution, so the result is never worse on the objective-minus-penalty than
 * the build it starts from; the caller keeps that build when the solver fails.
 *
 * Uniqueness between lineups is not modelled: the candidate pool is built so
 * every pair already satisfies the minimum-unique rule, which makes any
 * subset satisfy it too.
 */

export const NFL_JOINT_SELECTION_VERSION = "nfl-joint-selection-v1";

/** Cost of missing a portfolio target by one lineup. Far above any lineup's score difference. */
export const JOINT_TARGET_PENALTY = 1000;

export interface JointCandidate {
  key: string;
  /** Lineup objective, without the per-lineup jitter (so candidates compare on one scale). */
  score: number;
  /** Feature counts this lineup contributes, e.g. { o_123: 1, twoQb: 1 }. */
  features: Record<string, number>;
}

export interface JointTarget {
  min?: number;
  max?: number;
  /** The feature this target counts; defaults to the target's own name. */
  feature?: string;
  /**
   * A hard bound is never traded for score or for another target (a player's
   * exposure cap). The incumbent must already satisfy it.
   */
  hard?: boolean;
}

export interface JointSelectionInput {
  candidates: JointCandidate[];
  count: number;
  targets: Record<string, JointTarget>;
  /** Keys of a solution known to be feasible (the sequential portfolio). */
  incumbent: string[];
  timeoutMs?: number;
}

export interface JointSelectionResult {
  selected: string[];
  /** Objective minus penalties, for the selected and the incumbent portfolio. */
  value: number;
  incumbentValue: number;
  /** Target misses (in lineups) of the selected portfolio. */
  misses: Record<string, number>;
  solver: "ilp" | "incumbent";
  reason?: string;
}

/** Sum of a feature over the chosen candidates. */
export function featureTotals(candidates: JointCandidate[], keys: Iterable<string>): Record<string, number> {
  const byKey = new Map(candidates.map((c) => [c.key, c]));
  const totals: Record<string, number> = {};
  for (const key of keys) {
    const candidate = byKey.get(key);
    if (!candidate) throw new Error(`Unknown candidate ${key}.`);
    for (const [name, value] of Object.entries(candidate.features)) totals[name] = (totals[name] ?? 0) + value;
  }
  return totals;
}

/** How far each target is missed (lineups below min or above max). */
export function targetMisses(totals: Record<string, number>, targets: Record<string, JointTarget>): Record<string, number> {
  const misses: Record<string, number> = {};
  for (const [name, target] of Object.entries(targets)) {
    const value = totals[target.feature ?? name] ?? 0;
    const miss = target.min != null && value < target.min ? target.min - value
      : target.max != null && value > target.max ? value - target.max : 0;
    if (miss > 1e-9) misses[name] = miss;
  }
  return misses;
}

/** Objective minus penalties for a set of chosen candidates. */
export function portfolioValue(candidates: JointCandidate[], keys: string[], targets: Record<string, JointTarget>): number {
  const byKey = new Map(candidates.map((c) => [c.key, c]));
  const score = keys.reduce((sum, key) => sum + byKey.get(key)!.score, 0);
  const missed = Object.values(targetMisses(featureTotals(candidates, keys), targets)).reduce((sum, v) => sum + v, 0);
  return score - JOINT_TARGET_PENALTY * missed;
}

type SolverModel = {
  optimize: string; opType: "max" | "min";
  constraints: Record<string, { min?: number; max?: number; equal?: number }>;
  variables: Record<string, Record<string, number>>;
  binaries: Record<string, 1>;
  options?: { timeout?: number };
};
type SolverResult = { feasible?: boolean; result?: number; [key: string]: unknown };

export function selectJointPortfolio(input: JointSelectionInput): JointSelectionResult {
  const { candidates, count, targets, incumbent } = input;
  const keys = new Set<string>();
  for (const c of candidates) {
    if (keys.has(c.key)) throw new Error(`Duplicate candidate key ${c.key}.`);
    keys.add(c.key);
  }
  if (incumbent.length !== count || incumbent.some((key) => !keys.has(key))) throw new Error("The incumbent portfolio must be a full set of known candidates.");
  const incumbentMisses = targetMisses(featureTotals(candidates, incumbent), targets);
  const brokenHard = Object.keys(incumbentMisses).filter((name) => targets[name].hard);
  if (brokenHard.length) throw new Error(`The incumbent breaks hard bounds (${brokenHard.join(", ")}); widen them to the incumbent first.`);
  const incumbentValue = portfolioValue(candidates, incumbent, targets);
  const keepIncumbent = (reason: string): JointSelectionResult => ({
    selected: [...incumbent], value: incumbentValue, incumbentValue,
    misses: targetMisses(featureTotals(candidates, incumbent), targets), solver: "incumbent", reason,
  });
  if (candidates.length <= count) return keepIncumbent("no extra candidates to choose from");

  // Constraint names are index-based: feature names carry ids and underscores
  // the solver accepts, but a fixed prefix keeps slack and pick rows apart.
  const constraints: SolverModel["constraints"] = { pick_count: { equal: count } };
  const variables: SolverModel["variables"] = {};
  const binaries: SolverModel["binaries"] = {};
  const names = Object.keys(targets);
  // Several targets may count the same feature (a soft minimum and a hard cap).
  const byFeature = new Map<string, number[]>();
  names.forEach((name, index) => {
    const target = targets[name];
    const feature = target.feature ?? name;
    byFeature.set(feature, [...(byFeature.get(feature) ?? []), index]);
    if (target.min != null) {
      constraints[`lo_${index}`] = { min: target.min };
      if (!target.hard) variables[`under_${index}`] = { score: -JOINT_TARGET_PENALTY, [`lo_${index}`]: 1 };
    }
    if (target.max != null) {
      constraints[`hi_${index}`] = { max: target.max };
      if (!target.hard) variables[`over_${index}`] = { score: -JOINT_TARGET_PENALTY, [`hi_${index}`]: -1 };
    }
  });
  candidates.forEach((candidate, i) => {
    const variable: Record<string, number> = { score: candidate.score, pick_count: 1 };
    for (const [feature, value] of Object.entries(candidate.features)) {
      if (!value) continue;
      for (const index of byFeature.get(feature) ?? []) {
        if (targets[names[index]].min != null) variable[`lo_${index}`] = value;
        if (targets[names[index]].max != null) variable[`hi_${index}`] = value;
      }
    }
    variables[`y_${i}`] = variable;
    binaries[`y_${i}`] = 1;
  });

  let solved: SolverResult;
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const solver = require("javascript-lp-solver") as { Solve: (model: SolverModel) => SolverResult };
    solved = solver.Solve({ optimize: "score", opType: "max", constraints, variables, binaries,
      options: input.timeoutMs ? { timeout: input.timeoutMs } : undefined });
  } catch (error) {
    return keepIncumbent(`solver error: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (solved.feasible === false) return keepIncumbent("solver found no feasible portfolio");
  const selected = candidates.filter((_, i) => typeof solved[`y_${i}`] === "number" && (solved[`y_${i}`] as number) > 0.5).map((c) => c.key);
  if (selected.length !== count) return keepIncumbent(`solver returned ${selected.length} lineups instead of ${count}`);
  // The solver's own value is not trusted: recount from the chosen lineups.
  const value = portfolioValue(candidates, selected, targets);
  if (!(value > incumbentValue + 1e-6)) return keepIncumbent("no portfolio improved on the sequential build");
  return { selected, value, incumbentValue, misses: targetMisses(featureTotals(candidates, selected), targets), solver: "ilp" };
}
