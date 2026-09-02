/**
 * Exact survivor-path optimization.
 *
 * Picking a survivor path is a rectangular linear assignment problem, not a
 * search problem. Maximizing the probability of surviving every remaining week
 * means maximizing the PRODUCT of the weekly win probabilities, which is the
 * SUM of their logs -- a separable objective over a bipartite matching (one
 * team per week, each team at most once). So the Hungarian algorithm returns
 * the provably optimal path, and at 18 weeks x 32 teams it does so in
 * microseconds. There is no performance argument for a greedy heuristic here,
 * which is why none is offered.
 *
 * The same property makes "future survivor value" exact rather than a
 * heuristic: the cost of consuming a team is the difference between the
 * optimal path and the optimal path with that team forced or banned.
 */

/** Cost used for a cell that cannot be played (bye, already used, no data). */
const BLOCKED = 1e9;

/** Probabilities are floored before the log so a 0% cell stays finite. */
const MIN_PROB = 1e-6;

export type SurvivorSolution = {
  /** assignment[weekIndex] = teamIndex, or null when the week is unfilled. */
  assignment: (number | null)[];
  /** Sum of log(p) over assigned weeks. -Infinity when infeasible. */
  logValue: number;
  /** exp(logValue): the probability of surviving every assigned week. */
  survivalProb: number;
  feasible: boolean;
  /** Index of the first week that could not be filled, when infeasible. */
  unfillableWeek: number | null;
};

/**
 * Build the cost matrix. `probs[week][team]` is null when that team does not
 * play that week (bye) or has no usable probability.
 */
export function buildCostMatrix(
  probs: (number | null)[][],
  options: { bannedTeams?: Set<number>; forced?: Map<number, number> } = {},
): number[][] {
  const { bannedTeams, forced } = options;
  return probs.map((week, weekIndex) =>
    week.map((p, teamIndex) => {
      if (bannedTeams?.has(teamIndex)) return BLOCKED;
      // A forced pick pins one team to one week: every other team is blocked
      // in that week, and that team is blocked in every other week.
      if (forced) {
        const forcedTeam = forced.get(weekIndex);
        if (forcedTeam !== undefined && forcedTeam !== teamIndex) return BLOCKED;
        for (const [otherWeek, otherTeam] of forced) {
          if (otherTeam === teamIndex && otherWeek !== weekIndex) return BLOCKED;
        }
      }
      if (p === null || !Number.isFinite(p) || p <= 0) return BLOCKED;
      return -Math.log(Math.max(p, MIN_PROB));
    }),
  );
}

/**
 * Hungarian algorithm (Jonker-Volgenant shortest augmenting path form) for a
 * rectangular cost matrix with rows <= cols. Every row is assigned; columns
 * are used at most once. Minimizes total cost.
 */
export function solveAssignment(cost: number[][]): { assignment: number[]; total: number } {
  const n = cost.length;
  const m = n > 0 ? cost[0].length : 0;
  if (n === 0 || m === 0) return { assignment: [], total: 0 };
  if (n > m) throw new Error(`assignment needs rows <= cols, got ${n}x${m}`);

  const u = new Float64Array(n + 1);
  const v = new Float64Array(m + 1);
  const p = new Int32Array(m + 1); // p[j] = 1-based row matched to column j
  const way = new Int32Array(m + 1);

  for (let i = 1; i <= n; i += 1) {
    p[0] = i;
    let j0 = 0;
    const minv = new Float64Array(m + 1).fill(Infinity);
    const used = new Uint8Array(m + 1);

    do {
      used[j0] = 1;
      const i0 = p[j0];
      let delta = Infinity;
      let j1 = 0;
      for (let j = 1; j <= m; j += 1) {
        if (used[j]) continue;
        const cur = cost[i0 - 1][j - 1] - u[i0] - v[j];
        if (cur < minv[j]) {
          minv[j] = cur;
          way[j] = j0;
        }
        if (minv[j] < delta) {
          delta = minv[j];
          j1 = j;
        }
      }
      for (let j = 0; j <= m; j += 1) {
        if (used[j]) {
          u[p[j]] += delta;
          v[j] -= delta;
        } else {
          minv[j] -= delta;
        }
      }
      j0 = j1;
    } while (p[j0] !== 0);

    do {
      const j1 = way[j0];
      p[j0] = p[j1];
      j0 = j1;
    } while (j0 !== 0);
  }

  const assignment = new Array<number>(n).fill(-1);
  let total = 0;
  for (let j = 1; j <= m; j += 1) {
    if (p[j] !== 0) {
      assignment[p[j] - 1] = j - 1;
      total += cost[p[j] - 1][j - 1];
    }
  }
  return { assignment, total };
}

/** Solve a survivor path from a week x team probability grid. */
export function solveSurvivor(
  probs: (number | null)[][],
  options: { bannedTeams?: Set<number>; forced?: Map<number, number> } = {},
): SurvivorSolution {
  if (probs.length === 0) {
    return { assignment: [], logValue: 0, survivalProb: 1, feasible: true, unfillableWeek: null };
  }
  const cost = buildCostMatrix(probs, options);
  const { assignment } = solveAssignment(cost);

  const resolved: (number | null)[] = [];
  let logValue = 0;
  let unfillableWeek: number | null = null;

  assignment.forEach((teamIndex, weekIndex) => {
    if (teamIndex < 0 || cost[weekIndex][teamIndex] >= BLOCKED) {
      resolved.push(null);
      if (unfillableWeek === null) unfillableWeek = weekIndex;
      return;
    }
    resolved.push(teamIndex);
    logValue += Math.log(probs[weekIndex][teamIndex] as number);
  });

  const feasible = unfillableWeek === null;
  return {
    assignment: resolved,
    logValue: feasible ? logValue : -Infinity,
    survivalProb: feasible ? Math.exp(logValue) : 0,
    feasible,
    unfillableWeek,
  };
}

export type PickEvaluation = {
  teamIndex: number;
  /** This week's probability for the team. */
  p: number;
  /** log p for this week's pick. */
  logP: number;
  /**
   * Opportunity cost in log units: V* minus the optimal value with this team
   * forced into this week. Always >= 0.
   */
  opportunityCost: number;
  /** logP - opportunityCost. The quantity the decision should maximize. */
  netScore: number;
  /** Season survival probability if this pick is taken and the rest replanned. */
  pathSurvival: number;
  /** Survival probability given up versus the unconstrained optimum. */
  survivalCost: number;
};

/**
 * Rank every legal pick for one week by its true cost, not just its win
 * probability. A 78% team whose only other good week is a bye can be a worse
 * pick than a 72% team that is replaceable later; the forced re-solve is what
 * makes that visible.
 */
export function evaluateWeek(
  probs: (number | null)[][],
  weekIndex: number,
  options: { bannedTeams?: Set<number> } = {},
): { base: SurvivorSolution; picks: PickEvaluation[] } {
  const base = solveSurvivor(probs, options);
  const picks: PickEvaluation[] = [];
  const week = probs[weekIndex] ?? [];

  week.forEach((p, teamIndex) => {
    if (p === null || p <= 0 || options.bannedTeams?.has(teamIndex)) return;
    const forced = new Map<number, number>([[weekIndex, teamIndex]]);
    const constrained = solveSurvivor(probs, { ...options, forced });
    if (!constrained.feasible) return;
    picks.push({
      teamIndex,
      p,
      logP: Math.log(p),
      opportunityCost: base.feasible ? base.logValue - constrained.logValue : 0,
      netScore: constrained.logValue,
      pathSurvival: constrained.survivalProb,
      survivalCost: base.survivalProb - constrained.survivalProb,
    });
  });

  picks.sort((a, b) => b.netScore - a.netScore);
  return { base, picks };
}

/**
 * Future survivor value: how much the whole remaining plan degrades if a team
 * is never available again. This is the exact quantity the popular grids
 * approximate with a heuristic.
 */
export function futureSurvivorValue(
  probs: (number | null)[][],
  options: { bannedTeams?: Set<number> } = {},
): { base: SurvivorSolution; fsv: number[] } {
  const base = solveSurvivor(probs, options);
  const teamCount = probs[0]?.length ?? 0;
  const fsv = new Array<number>(teamCount).fill(0);

  for (let teamIndex = 0; teamIndex < teamCount; teamIndex += 1) {
    if (options.bannedTeams?.has(teamIndex)) continue;
    const banned = new Set(options.bannedTeams ?? []);
    banned.add(teamIndex);
    const without = solveSurvivor(probs, { ...options, bannedTeams: banned });
    fsv[teamIndex] = without.feasible && base.feasible
      ? base.survivalProb - without.survivalProb
      : base.survivalProb;
  }
  return { base, fsv };
}
