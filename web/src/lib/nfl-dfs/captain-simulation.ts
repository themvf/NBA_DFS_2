/**
 * Simulated captain odds for a Showdown slate: how often each player is the
 * slate's top scorer, and captain ranges built from those odds.
 *
 * The earlier suggestion (captain-recommendation.ts) shares captains by
 * projection squared among the three chalkiest names. That ignores spread:
 * on 2026-09-24 Tucker Kraft projected 12.2 with a 37.4 ceiling, the ceiling
 * search captained him in 12 of 20 lineups, and 9 of those finished below the
 * median. A captain slot pays for being the best player in the game, which is
 * a question about the whole distribution, so this answers it by simulation.
 *
 * Model, in full, so every number can be explained:
 *
 *   marginals  the model draws each player's DK points by resampling real past
 *              games (model/nfl_dfs_historical.py), so its distribution is lumpy
 *              and capped -- Kraft's mean 12.2 sits under a 37.4 P90. A smooth
 *              family cannot hold all of that (a two-piece normal inflated his
 *              mean by 1.9). So each player gets a quantile curve through the
 *              model's own P10, median and P90: linear below the median, a
 *              power curve from median to P90 whose bend is solved so the mean
 *              is exactly our projection, and a short tail past P90. A player
 *              missing any of the three is left out and listed.
 *   dependence a Gaussian copula. Every offensive player loads on a shared
 *              game factor and on his own team's factor; a DST loads
 *              negatively on the opponent's team factor. The loadings are
 *              stated priors, not fitted (GAME_LOADING, TEAM_LOADING,
 *              DST_OPPONENT_LOADING).
 *   captain    "best captain" = the top scorer among captain-eligible players.
 *              Salary is ignored: a cheap second-best scorer can occasionally
 *              anchor the optimal lineup instead, so this overstates the top
 *              names slightly.
 *   ranges     suggested share = the simulated odds; range = odds +/- the band,
 *              rounded to 5. Players under MIN_LISTED_PCT get no range.
 *
 * Unvalidated. With one top scorer per slate, calibration needs many slates;
 * Results history is where the captain choices get graded.
 */
import { nflRandom } from "./random";
import type { CaptainTarget } from "./generation-settings";
import type { CaptainCandidate, CaptainRecommendation } from "./captain-recommendation";

export const CAPTAIN_SIMULATION_VERSION = "nfl-captain-sim-v1";
export const DEFAULT_DRAWS = 5000;
export const DEFAULT_SEED = 20260925;
/** Stated priors: same-team correlation = GAME^2 + TEAM^2 = 0.40, cross-team = GAME^2 = 0.15. */
export const GAME_LOADING = Math.sqrt(0.15);
export const TEAM_LOADING = Math.sqrt(0.25);
export const DST_OPPONENT_LOADING = -Math.sqrt(0.3);
export const MIN_LISTED_PCT = 2;
export const SIM_BAND_PCT = 8;

const Z90 = 1.2815515655446004;
const DOUBT = new Set(["O", "OUT", "Q", "D", "QUESTIONABLE", "DOUBTFUL", "GTD"]);

export interface SimulationCandidate extends CaptainCandidate {
  team: string;
  opponent?: string | null;
  floorFpts: number | null;
  medianFpts?: number | null;
  ceilingFpts: number | null;
}

export interface CaptainOdds {
  dkPlayerId: number;
  name: string;
  position: string;
  team: string;
  ourProj: number;
  /** Percent of draws in which this player was the top scorer. */
  topPct: number;
}

export interface CaptainSimulation {
  version: string;
  draws: number;
  seed: number;
  odds: CaptainOdds[];
  /** Captain-eligible players left out because P10, median or P90 is missing. */
  missingTails: string[];
  /** Players whose projection no curve through their quantiles can reach (inputs disagree). */
  meanNotMatched: string[];
}

/** A player's DK points as a quantile curve; see the module comment. */
export interface QuantileShape { q0: number; p10: number; median: number; p90: number; q1: number; bend: number; meanMatched: boolean }

/** Share of the median-to-P90 gap added past P90: the resampled games have a hard top. */
export const UPPER_TAIL_EXTENSION = 0.1;
const MIN_BEND = 0.05, MAX_BEND = 50;

function shapeMean(shape: Omit<QuantileShape, "meanMatched">): number {
  const { q0, median, p90, q1, bend } = shape;
  return 0.25 * (q0 + median) + 0.4 * median + (0.4 * (p90 - median)) / (bend + 1) + 0.05 * (p90 + q1);
}

/**
 * Fit the curve so its mean is the projection. Below the median the curve is
 * the line through P10 and the median (extended to u = 0); from the median to
 * P90 it is median + (P90 - median) * ((u - 0.5) / 0.4)^bend, and the mean
 * falls as `bend` grows, so one bisection finds it. When the projection lies
 * outside what any bend can reach, the nearest bend is used and
 * `meanMatched` is false -- the inputs disagree and the caller is told.
 */
export function fitQuantileShape(mean: number, p10: number, median: number, p90: number): QuantileShape {
  const lo = Math.min(p10, median), hi = Math.max(p90, median);
  const mid = Math.min(Math.max(median, lo), hi);
  const q0 = mid - 1.25 * (mid - lo);
  const q1 = hi + UPPER_TAIL_EXTENSION * (hi - mid);
  const at = (bend: number) => shapeMean({ q0, p10: lo, median: mid, p90: hi, q1, bend });
  let bend: number;
  if (mean >= at(MIN_BEND)) bend = MIN_BEND;
  else if (mean <= at(MAX_BEND)) bend = MAX_BEND;
  else {
    let a = MIN_BEND, b = MAX_BEND;
    for (let i = 0; i < 80; i += 1) { const m = (a + b) / 2; if (at(m) > mean) a = m; else b = m; }
    bend = (a + b) / 2;
  }
  return { q0, p10: lo, median: mid, p90: hi, q1, bend, meanMatched: Math.abs(at(bend) - mean) < 0.01 };
}

export function quantile(shape: QuantileShape, u: number): number {
  const { q0, median, p90, q1, bend } = shape;
  if (u <= 0.5) return q0 + (median - q0) * (u / 0.5);
  if (u <= 0.9) return median + (p90 - median) * Math.pow((u - 0.5) / 0.4, bend);
  return p90 + (q1 - p90) * ((u - 0.9) / 0.1);
}

/** Standard normal CDF (Abramowitz & Stegun 7.1.26, error under 1.5e-7). */
function phi(z: number): number {
  const t = 1 / (1 + 0.3275911 * Math.abs(z) / Math.SQRT2);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-(z * z) / 2);
  return z >= 0 ? (1 + y) / 2 : (1 - y) / 2;
}

function normal(random: () => number): number {
  let u = 0;
  while (u === 0) u = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * random());
}

function eligible(p: SimulationCandidate): boolean {
  return !p.isOut && p.captainDkPlayerId != null && (p.ourProj ?? 0) > 0
    && !DOUBT.has((p.availabilityStatus ?? "").trim().toUpperCase())
    && !DOUBT.has((p.dkStatus ?? "").trim().toUpperCase());
}

export function simulateCaptainOdds(players: readonly SimulationCandidate[],
  opts: { draws?: number; seed?: number } = {}): CaptainSimulation {
  const draws = opts.draws ?? DEFAULT_DRAWS;
  const seed = opts.seed ?? DEFAULT_SEED;
  const pool = players.filter(eligible);
  const complete = (p: SimulationCandidate) => p.floorFpts != null && p.medianFpts != null && p.ceilingFpts != null;
  const missingTails = pool.filter((p) => !complete(p)).map((p) => p.name);
  const sim = pool.filter(complete).map((p) => ({
    player: p, fit: fitQuantileShape(p.ourProj!, p.floorFpts!, p.medianFpts!, p.ceilingFpts!),
  }));
  const teams = [...new Set(players.map((p) => p.team))];
  const wins = new Array(sim.length).fill(0);
  const random = nflRandom(seed);
  for (let d = 0; d < draws && sim.length; d += 1) {
    const game = normal(random);
    const team = Object.fromEntries(teams.map((t) => [t, normal(random)]));
    let best = -Infinity, bestIndex = -1;
    sim.forEach(({ player, fit }, i) => {
      const own = team[player.team] ?? 0;
      const opponent = player.opponent ? team[player.opponent] ?? 0 : 0;
      const z = player.position === "DST"
        ? DST_OPPONENT_LOADING * opponent + Math.sqrt(1 - DST_OPPONENT_LOADING ** 2) * normal(random)
        : GAME_LOADING * game + TEAM_LOADING * own + Math.sqrt(1 - GAME_LOADING ** 2 - TEAM_LOADING ** 2) * normal(random);
      const points = quantile(fit, phi(z));
      if (points > best) { best = points; bestIndex = i; }
    });
    if (bestIndex >= 0) wins[bestIndex] += 1;
  }
  const odds = sim.map(({ player }, i) => ({
    dkPlayerId: player.dkPlayerId, name: player.name, position: player.position, team: player.team,
    ourProj: player.ourProj!, topPct: Math.round((1000 * wins[i]) / Math.max(1, draws)) / 10,
  })).sort((a, b) => b.topPct - a.topPct || b.ourProj - a.ourProj);
  return { version: CAPTAIN_SIMULATION_VERSION, draws, seed, odds, missingTails,
    meanNotMatched: sim.filter(({ fit }) => !fit.meanMatched).map(({ player }) => player.name) };
}

const roundTo5 = (value: number) => Math.round(value / 5) * 5;

/** Captain ranges from simulated odds, in the same shape the suggestion panel takes. */
export function recommendFromSimulation(sim: CaptainSimulation, band = SIM_BAND_PCT): CaptainRecommendation {
  const listed = sim.odds.filter((o) => o.topPct >= MIN_LISTED_PCT);
  const rows = listed.map((o) => ({
    dkPlayerId: o.dkPlayerId, name: o.name, sharePct: o.topPct,
    min: Math.max(0, roundTo5(o.topPct - band)),
    max: Math.min(100, Math.max(5, roundTo5(o.topPct + band))),
  }));
  // The optimizer rejects captain minimums over 100% and needs the listed
  // maximums to cover every lineup; widen from the least-likely end.
  while (rows.reduce((a, r) => a + r.min, 0) > 100) {
    const r = [...rows].reverse().find((row) => row.min > 0)!;
    r.min -= 5;
  }
  while (rows.length && rows.reduce((a, r) => a + r.max, 0) < 100) {
    const r = rows.find((row) => row.max < 100)!;
    r.max = Math.min(100, r.max + 5);
  }
  return {
    version: CAPTAIN_SIMULATION_VERSION,
    basis: "simulation",
    simulation: { draws: sim.draws, seed: sim.seed, missingTails: sim.missingTails },
    rows,
    targets: Object.fromEntries(rows.map((r) => [String(r.dkPlayerId), { min: r.min, max: r.max } satisfies CaptainTarget])),
  };
}
