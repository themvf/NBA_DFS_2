/**
 * Tests for the survivor assignment solver.
 *
 * The load-bearing test is the brute-force equivalence check: on a grid small
 * enough to enumerate every legal path, the solver's answer must be the best
 * one, not merely a good one. Everything else in the tool trusts that claim.
 *
 * Run: npm run test:survivor
 */

import {
  CONTRARIAN_TOLERANCE,
  EV_IS_VALIDATED,
  EV_MIN_POOL_SIZE,
  buildPlan,
  defaultObjective,
} from "../src/lib/nfl/survivor-policy";
import {
  buildCostMatrix,
  evaluateWeek,
  futureSurvivorValue,
  solveAssignment,
  solveSurvivor,
} from "../src/lib/nfl/survivor-assignment";

let passed = 0;
let failed = 0;

function check(name: string, condition: boolean, detail?: string) {
  if (condition) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    console.error(`  FAIL  ${name}${detail ? ` -- ${detail}` : ""}`);
  }
}

function close(a: number, b: number, tol = 1e-9) {
  return Math.abs(a - b) < tol;
}

/** Brute force: best sum of log(p) over all injective week->team maps. */
function bruteForce(probs: (number | null)[][]): number {
  const weeks = probs.length;
  const teams = probs[0]?.length ?? 0;
  let best = -Infinity;

  const walk = (week: number, used: Set<number>, acc: number) => {
    if (acc <= best - 1e-12 && best !== -Infinity && week < weeks) {
      // no admissible pruning bound here; continue
    }
    if (week === weeks) {
      if (acc > best) best = acc;
      return;
    }
    for (let team = 0; team < teams; team += 1) {
      const p = probs[week][team];
      if (p === null || p <= 0 || used.has(team)) continue;
      used.add(team);
      walk(week + 1, used, acc + Math.log(p));
      used.delete(team);
    }
  };
  walk(0, new Set(), 0);
  return best;
}

function seededRandom(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0xffffffff;
  };
}

// ---------------------------------------------------------------------------

console.log("\nsurvivor assignment\n");

// 1. Known-optimal tiny case. Greedy picks the 0.90 in week 1 and is then
//    stuck with 0.10 in week 2; the optimal path takes 0.80 first.
{
  const probs: (number | null)[][] = [
    [0.9, 0.8],
    [0.9, 0.1],
  ];
  const solution = solveSurvivor(probs);
  check(
    "beats greedy on the classic burn-the-best-team trap",
    solution.assignment[0] === 1 && solution.assignment[1] === 0,
    `got ${JSON.stringify(solution.assignment)}`,
  );
  check("survival probability is the product", close(solution.survivalProb, 0.8 * 0.9, 1e-12));
}

// 2. Brute-force equivalence over random grids.
{
  const random = seededRandom(20260831);
  let worst = 0;
  let mismatches = 0;
  for (let trial = 0; trial < 40; trial += 1) {
    const weeks = 4 + Math.floor(random() * 3); // 4..6
    const teams = weeks + 1 + Math.floor(random() * 3);
    const probs: (number | null)[][] = [];
    for (let w = 0; w < weeks; w += 1) {
      const row: (number | null)[] = [];
      for (let t = 0; t < teams; t += 1) {
        row.push(random() < 0.18 ? null : 0.2 + random() * 0.75);
      }
      probs.push(row);
    }
    const solved = solveSurvivor(probs);
    const best = bruteForce(probs);
    if (!Number.isFinite(best)) continue;
    if (!solved.feasible) {
      mismatches += 1;
      continue;
    }
    worst = Math.max(worst, Math.abs(solved.logValue - best));
    if (Math.abs(solved.logValue - best) > 1e-9) mismatches += 1;
  }
  check(
    "matches brute force on 40 random grids",
    mismatches === 0,
    `${mismatches} mismatches, max delta ${worst.toExponential(2)}`,
  );
}

// 3. Byes and used teams are hard constraints.
{
  const probs: (number | null)[][] = [
    [0.95, 0.60, 0.55],
    [null, 0.70, 0.50],
  ];
  const solution = solveSurvivor(probs, { bannedTeams: new Set([0]) });
  check(
    "a banned team is never assigned",
    !solution.assignment.includes(0),
    JSON.stringify(solution.assignment),
  );
  check(
    "a bye cell is never assigned",
    solution.assignment[1] !== 0,
    JSON.stringify(solution.assignment),
  );
}

// 4. Infeasibility names the week rather than returning a partial path.
{
  const probs: (number | null)[][] = [
    [0.8, null],
    [0.7, null],
  ];
  const solution = solveSurvivor(probs);
  check("infeasible path is reported as infeasible", solution.feasible === false);
  check("infeasible path names a week", solution.unfillableWeek !== null);
  check("infeasible path has zero survival", solution.survivalProb === 0);
}

// 5. Opportunity cost equals V* minus the forced optimum, and is never negative.
{
  const probs: (number | null)[][] = [
    [0.90, 0.80, 0.55],
    [0.90, 0.10, 0.50],
    [0.20, 0.30, 0.75],
  ];
  const { base, picks } = evaluateWeek(probs, 0);
  const top = picks[0];
  check("week evaluation returns every legal pick", picks.length === 3);
  check(
    "the best net score matches the unconstrained optimum",
    close(top.netScore, base.logValue, 1e-9),
    `${top.netScore} vs ${base.logValue}`,
  );
  check(
    "opportunity cost is never negative",
    picks.every((pick) => pick.opportunityCost >= -1e-9),
  );
  const burn = picks.find((pick) => pick.teamIndex === 0);
  check(
    "the highest-probability pick is not always the best pick",
    burn !== undefined && burn.teamIndex !== top.teamIndex,
    `top pick was team ${top.teamIndex}`,
  );
}

// 6. FSV of a team the plan does not need is zero; of a needed team, positive.
{
  const probs: (number | null)[][] = [
    [0.90, 0.80, 0.05],
    [0.10, 0.85, 0.05],
  ];
  const { fsv } = futureSurvivorValue(probs);
  check("a replaceable team has near-zero FSV", Math.abs(fsv[2]) < 1e-9, String(fsv[2]));
  check("a load-bearing team has positive FSV", fsv[1] > 0, String(fsv[1]));
}

// 7. Determinism.
{
  const random = seededRandom(7);
  const probs: (number | null)[][] = Array.from({ length: 6 }, () =>
    Array.from({ length: 10 }, () => (random() < 0.15 ? null : 0.3 + random() * 0.6)),
  );
  const a = solveSurvivor(probs);
  const b = solveSurvivor(probs);
  check(
    "repeat solves are identical",
    JSON.stringify(a.assignment) === JSON.stringify(b.assignment) && close(a.logValue, b.logValue),
  );
}

// 8. The raw assignment routine minimizes cost on a textbook matrix.
{
  const cost = [
    [4, 1, 3],
    [2, 0, 5],
    [3, 2, 2],
  ];
  const { assignment, total } = solveAssignment(cost);
  check("textbook assignment total is 5", total === 5, `got ${total} via ${assignment}`);
}

// 9. Forced picks pin exactly one team to one week.
{
  const probs: (number | null)[][] = [
    [0.9, 0.8, 0.7],
    [0.9, 0.8, 0.7],
  ];
  const forced = new Map([[0, 2]]);
  const cost = buildCostMatrix(probs, { forced });
  check("forcing blocks other teams in that week", cost[0][0] >= 1e9 && cost[0][1] >= 1e9);
  check("forcing blocks that team in other weeks", cost[1][2] >= 1e9);
  const solution = solveSurvivor(probs, { forced });
  check("forced solve honors the pin", solution.assignment[0] === 2);
}

// 10. Objective policy: EV never defaults on, and the band is the measured one.
{
  check("EV never becomes the default objective",
    defaultObjective(50) === "survive" && defaultObjective(5000) === "survive");
  check("EV is not marked validated on one season", EV_IS_VALIDATED === false);
  check("contrarian band is the measured 0.05, not a round guess", CONTRARIAN_TOLERANCE === 0.05);
  check("EV pool floor reflects the study, not the original guess of 50", EV_MIN_POOL_SIZE >= 1000);
}

// 11. EV mode fades the crowd only inside the band, and reports what it cost.
{
  const probs: (number | null)[][] = [
    [0.80, 0.79, 0.55],
    [0.60, 0.10, 0.70],
  ];
  //          heavily picked, near-equal contrarian, ignored
  const pickPct: (number | null)[][] = [
    [0.60, 0.02, 0.05],
    [0.10, 0.10, 0.10],
  ];
  const survive = buildPlan({ probs, pickPct, bannedTeams: new Set(), mode: "survive" });
  const ev = buildPlan({ probs, pickPct, bannedTeams: new Set(), mode: "ev" });

  check("survive mode takes the strongest team", survive.path[0] === 0, JSON.stringify(survive.path));
  check(
    "EV mode fades the 60%-picked team for the near-equal one",
    ev.path[0] === 1,
    JSON.stringify(ev.path),
  );
  check("EV reports which weeks it deviated", ev.deviations.includes(0));
  check(
    "EV reports the survival probability it gave up",
    ev.survivalGivenUp > 0,
    String(ev.survivalGivenUp),
  );
}

// 12. A big probability gap is never faded, however popular the favorite.
{
  const probs: (number | null)[][] = [
    [0.90, 0.55],
    [0.50, 0.60],
  ];
  const pickPct: (number | null)[][] = [[0.90, 0.001], [0.1, 0.1]];
  const ev = buildPlan({ probs, pickPct, bannedTeams: new Set(), mode: "ev" });
  check(
    "a team outside the band is not faded no matter how crowded",
    ev.path[0] === 0,
    JSON.stringify(ev.path),
  );
}

console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);
