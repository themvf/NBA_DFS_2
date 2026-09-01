/**
 * Objective modes and the thresholds that govern them.
 *
 * Single source of truth: components render what these functions return and
 * never recompute a threshold. If a number below moves, it moves here.
 */

import { evaluateWeek, solveSurvivor, type SurvivorSolution } from "./survivor-assignment";

export type ObjectiveMode = "survive" | "ev";

/**
 * Contrarian band width, in nats of survival-optimal net score.
 *
 * Measured, not chosen. `model/survivor_field_study.py` simulated the 2025
 * season against archived national pick share at 100 / 500 / 2,000 entries,
 * 20,000 paired trials each, and the pattern was monotone in pool size:
 *
 *   pool   tol=0.05            tol=0.10            tol=0.20
 *    100   -0.0085 [-.0121,-.0048]  -0.0089 [-.0126,-.0052]  -0.0338 [-.0369,-.0306]
 *    500   +0.0017 [-.0000,+.0035]  -0.0001 [-.0017,+.0017]  -0.0082 [-.0096,-.0068]
 *   2000   +0.0012 [+.0005,+.0019]  +0.0008 [+.0001,+.0016]  -0.0024 [-.0029,-.0018]
 *
 * So a NARROW band helps in a big pool, does nothing in a mid pool, and
 * actively hurts in a small one -- and a wide band hurts everywhere. 0.05 is
 * the best-performing width, not a round number.
 */
export const CONTRARIAN_TOLERANCE = 0.05;

/**
 * Below this pool size, EV mode measurably LOSES. The original spec guessed
 * 50; the study says that guess was wrong by more than an order of magnitude,
 * and a 100-entry pool is where contrarian play did its clearest damage.
 */
export const EV_MIN_POOL_SIZE = 1000;

/** EV mode is research-badged until three independent seasons agree. */
export const EV_SEASONS_REQUIRED = 3;
export const EV_SEASONS_OBSERVED = 1;
export const EV_IS_VALIDATED = EV_SEASONS_OBSERVED >= EV_SEASONS_REQUIRED;

/**
 * EV never defaults on, at any pool size. One season of field data cannot
 * promote a mode, and the pre-registration said so before the data was looked
 * at. The parameter is kept so the call sites read as a policy decision rather
 * than a constant, and so this becomes a real switch the day three seasons
 * agree.
 */
export function defaultObjective(poolSize: number | null): ObjectiveMode {
  void poolSize; // deliberately ignored -- see above
  return "survive";
}

export function evAdvisory(poolSize: number | null): string {
  if (poolSize == null) {
    return "Set your pool size — whether fading the field helps depends entirely on it.";
  }
  if (poolSize < EV_MIN_POOL_SIZE) {
    return `In a ${poolSize}-entry pool the 2025 simulation shows fading the field LOSING ` +
      `(-0.9pp of prize share at 100 entries). Survive is the right objective here.`;
  }
  return `In a ${poolSize}-entry pool the 2025 simulation shows a small gain ` +
    `(+0.12pp of prize share, on a 0.35pp base). One season only — treat as a lean, not a rule.`;
}

export type PlanInput = {
  /** probs[weekIndex][teamIndex], null for bye or no data. */
  probs: (number | null)[][];
  /** pickPct[weekIndex][teamIndex], null when the popularity feed has no row. */
  pickPct: (number | null)[][];
  bannedTeams: Set<number>;
  mode: ObjectiveMode;
};

export type Plan = {
  solution: SurvivorSolution;
  /** assignment[weekIndex] = teamIndex, after any contrarian substitution. */
  path: (number | null)[];
  /** Weeks where EV mode deviated from the survival-optimal pick. */
  deviations: number[];
  /** Survival probability given up to take the contrarian path. */
  survivalGivenUp: number;
};

/**
 * Build the recommended path.
 *
 * `survive` returns the exact assignment optimum. `ev` walks the season and,
 * at each week, takes the least-picked team whose net score is within
 * CONTRARIAN_TOLERANCE of the best available -- the same one-parameter policy
 * the field study measured, so what ships is what was tested.
 */
export function buildPlan(input: PlanInput): Plan {
  const base = solveSurvivor(input.probs, { bannedTeams: input.bannedTeams });
  if (input.mode === "survive" || !base.feasible) {
    return { solution: base, path: base.assignment, deviations: [], survivalGivenUp: 0 };
  }

  const path: (number | null)[] = [];
  const deviations: number[] = [];
  const used = new Set(input.bannedTeams);

  for (let week = 0; week < input.probs.length; week += 1) {
    const { picks } = evaluateWeek(input.probs, week, { bannedTeams: used });
    if (picks.length === 0) {
      path.push(null);
      continue;
    }
    const best = picks[0];
    const band = picks.filter((pick) => pick.netScore >= best.netScore - CONTRARIAN_TOLERANCE);
    const shares = input.pickPct[week] ?? [];
    // Least-picked inside the band; ties broken toward the stronger pick.
    const chosen = band.reduce((leader, candidate) => {
      const leaderShare = shares[leader.teamIndex] ?? 0;
      const candidateShare = shares[candidate.teamIndex] ?? 0;
      if (candidateShare !== leaderShare) return candidateShare < leaderShare ? candidate : leader;
      return candidate.netScore > leader.netScore ? candidate : leader;
    }, band[0]);

    if (chosen.teamIndex !== best.teamIndex) deviations.push(week);
    path.push(chosen.teamIndex);
    used.add(chosen.teamIndex);
  }

  let logValue = 0;
  let feasible = true;
  path.forEach((teamIndex, week) => {
    const probability = teamIndex == null ? null : input.probs[week][teamIndex];
    if (probability == null || probability <= 0) feasible = false;
    else logValue += Math.log(probability);
  });
  const survivalProb = feasible ? Math.exp(logValue) : 0;

  return {
    solution: {
      assignment: path,
      logValue: feasible ? logValue : -Infinity,
      survivalProb,
      feasible,
      unfillableWeek: feasible ? null : path.findIndex((team) => team == null),
    },
    path,
    deviations,
    survivalGivenUp: Math.max(base.survivalProb - survivalProb, 0),
  };
}
