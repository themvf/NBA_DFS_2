/**
 * Ghost-lineup factorial, `nfl-ghost-factorial-v1`
 * (docs/nfl-ghost-factorial-study.md).
 *
 * Every gradable slate gets a HOUSE portfolio (the current framework, frozen
 * here as factor levels) plus a fixed 32-arm design over the factors below.
 * Each arm changes several settings at once; each factor's effect is read
 * across all arms with the slate held fixed, which is how many settings can be
 * screened from one week of builds without testing each against the house.
 *
 * The design is a nearly orthogonal array: every factor's levels are balanced
 * across the 32 arms, and the columns were chosen (from a seeded search that
 * this file reproduces exactly) to keep every pairwise correlation small. The
 * search is deterministic: same seed, same design; a test pins the digest, so
 * the design cannot drift between slates. Changing a factor, a level, the
 * house, the arm count or the seed is a new version and a new cohort.
 *
 * Pure: no I/O.
 */
import { createHash } from "node:crypto";
import { nflRandom } from "../random";

export const GHOST_DESIGN_VERSION = "nfl-ghost-factorial-v1";
export const GHOST_ARMS = 32;
export const GHOST_SEED = 20261016;
export const GHOST_SEARCH_CANDIDATES = 3000;
/** Lineups per arm (and per house build). */
export const GHOST_LINEUPS = { classic: 40, showdown: 20 } as const;

export type GhostFormat = "classic" | "showdown";
export type Level = number | string | boolean;
export type Factor = { key: string; label: string; levels: readonly Level[]; house: Level };

const COMMON = (format: GhostFormat): Factor[] => [
  { key: "maxExposure", label: "Max exposure", levels: format === "classic" ? [0.35, 0.55, 0.75] : [0.4, 0.6, 0.8],
    house: format === "classic" ? 0.55 : 0.6 },
  { key: "leverageExponent", label: "Ownership leverage exponent (0 = off)", levels: [0, 0.5, 1], house: 0.5 },
  { key: "projections", label: "Projection source", levels: ["anchored", "model"], house: "anchored" },
  { key: "absenceCommit", label: "Starter-aware absence commit", levels: [true, false], house: true },
  { key: "ceilingCap", label: "GPP ceiling cap (multiple of projection)", levels: [2.5, "off"], house: 2.5 },
  { key: "minUnique", label: "Min unique players between lineups", levels: format === "classic" ? [2, 4] : [2, 3],
    house: format === "classic" ? 4 : 2 },
  { key: "puntPolicy", label: "Cheap-player (punt) policy", levels: ["no_punts", "role_qualified"], house: "no_punts" },
];

export const GHOST_FACTORS: Record<GhostFormat, Factor[]> = {
  classic: [
    ...COMMON("classic"),
    { key: "teWrRules", label: "Winners TE/WR pool rules", levels: [true, false], house: true },
    { key: "rbDstRules", label: "Winners RB/DST pool rules", levels: [true, false], house: true },
    { key: "bringBackShare", label: "Bring-back share", levels: [0, 0.5, 1], house: 0.5 },
    { key: "qbCap", label: "Winners QB exposure cap", levels: [true, false], house: true },
    { key: "dstVsOffenseCap", label: "DST against own offense capped at 5%", levels: [true, false], house: true },
  ],
  showdown: [
    ...COMMON("showdown"),
    { key: "plan", label: "Archetype plan", levels: ["standard", "balanced", "chalk_leverage"], house: "standard" },
    { key: "kickers", label: "Kickers", levels: ["banned", "allowed"], house: "banned" },
    { key: "fourTwo", label: "4-2 team split", levels: ["banned", "allowed"], house: "banned" },
    { key: "captainCap", label: "Max captain exposure", levels: [0.3, 0.5], house: 0.3 },
    { key: "captainPositions", label: "Captain positions", levels: ["QB/RB/WR", "any"], house: "QB/RB/WR" },
  ],
};

export type ArmLevels = Record<string, Level>;
export type GhostArm = { arm: number; house: boolean; levels: ArmLevels; extra?: true };

export const houseLevels = (format: GhostFormat): ArmLevels =>
  Object.fromEntries(GHOST_FACTORS[format].map(f => [f.key, f.house]));

/**
 * Extra arm pair OUTSIDE the 32-run array (`nfl-ghost-extra-arms-v1`,
 * amendment 2026-10-30 in docs/nfl-ghost-factorial-study.md): the GPP boom
 * weight (points per unit of boom rate, `boomWeight` in the optimizer) at 1
 * and 4 against the house's 2, with every other setting at the house level.
 * Adding it as a factor would change the pinned design and start a new
 * cohort, so it rides beside the design as arms 33 and 34, with its own
 * version and digest, and is graded as paired within-slate differences
 * against the house (arm 0), never inside the factorial regression. The
 * design arms, their digests and the running cohort are untouched.
 */
export const GHOST_EXTRA_ARMS_VERSION = "nfl-ghost-extra-arms-v1";
export const GHOST_EXTRA_FACTORS: readonly Factor[] = [
  { key: "boomWeight", label: "GPP boom weight (points per unit of boom rate)", levels: [1, 4], house: 2 },
];
export const GHOST_FIRST_EXTRA_ARM = GHOST_ARMS + 1;
export const GHOST_EXTRA_ARM_COUNT = GHOST_EXTRA_FACTORS.reduce((n, f) => n + f.levels.filter(l => l !== f.house).length, 0);

export function extraArms(format: GhostFormat): { arms: GhostArm[]; digest: string } {
  const arms: GhostArm[] = [];
  let arm = GHOST_FIRST_EXTRA_ARM;
  for (const f of GHOST_EXTRA_FACTORS) for (const level of f.levels) {
    if (level === f.house) continue;
    arms.push({ arm: arm++, house: false, levels: { ...houseLevels(format), [f.key]: level }, extra: true });
  }
  const digest = createHash("sha256").update(JSON.stringify({ version: GHOST_EXTRA_ARMS_VERSION, format, factors: GHOST_EXTRA_FACTORS, arms })).digest("hex");
  return { arms, digest };
}

/** The house, the 32 design arms, then the extra arms. */
export const allArms = (format: GhostFormat): GhostArm[] => [...ghostDesign(format).arms, ...extraArms(format).arms];

/** Numeric contrasts for a factor column: 2 levels -> one +/-1 column, 3 levels -> linear and quadratic. */
function contrasts(factor: Factor, column: number[]): number[][] {
  if (factor.levels.length === 2) return [column.map(i => (i === 0 ? -1 : 1))];
  return [column.map(i => i - 1), column.map(i => (i === 1 ? -2 : 1))];
}

function correlation(a: number[], b: number[]): number {
  const n = a.length;
  const ma = a.reduce((s, x) => s + x, 0) / n, mb = b.reduce((s, x) => s + x, 0) / n;
  let sab = 0, saa = 0, sbb = 0;
  for (let i = 0; i < n; i++) { sab += (a[i] - ma) * (b[i] - mb); saa += (a[i] - ma) ** 2; sbb += (b[i] - mb) ** 2; }
  return saa && sbb ? sab / Math.sqrt(saa * sbb) : 0;
}

/** The largest |correlation| between contrast columns of DIFFERENT factors. */
export function maxCrossCorrelation(factors: Factor[], columns: number[][]): number {
  const cols = factors.map((f, j) => contrasts(f, columns[j]));
  let worst = 0;
  for (let a = 0; a < cols.length; a++) for (let b = a + 1; b < cols.length; b++)
    for (const x of cols[a]) for (const y of cols[b]) worst = Math.max(worst, Math.abs(correlation(x, y)));
  return worst;
}

function balancedColumn(levels: number, n: number, random: () => number): number[] {
  const out = Array.from({ length: n }, (_, i) => i % levels);
  for (let i = n - 1; i > 0; i--) { const j = Math.floor(random() * (i + 1)); [out[i], out[j]] = [out[j], out[i]]; }
  return out;
}

const rowKey = (columns: number[][], i: number) => columns.map(c => c[i]).join(",");
const popcount = (x: number) => { let c = 0; for (let v = x; v; v &= v - 1) c++; return c; };

/**
 * Two-level columns of the 32-run Sylvester Hadamard matrix, H[i][j] =
 * (-1)^popcount(i & j). Only odd-weight column indices are used: the product
 * of two such columns is an even-weight column, so no main effect is aliased
 * with any two-factor interaction among them (resolution IV). The five
 * weight-one columns come first, which also makes every arm distinct.
 */
const ODD_COLUMNS = Array.from({ length: GHOST_ARMS - 1 }, (_, j) => j + 1).filter(j => popcount(j) % 2 === 1)
  .sort((a, b) => popcount(a) - popcount(b) || a - b);
const hadamardColumn = (j: number) => Array.from({ length: GHOST_ARMS }, (_, i) => (popcount(i & j) % 2 === 0 ? 0 : 1));

/**
 * Sum of squared correlations between a three-level column's contrasts and
 * every other factor's contrasts: what the exchange search minimizes.
 */
function exchangeObjective(factors: Factor[], columns: number[][], j: number): number {
  const mine = contrasts(factors[j], columns[j]);
  let total = 0;
  for (let k = 0; k < factors.length; k++) {
    if (k === j || !columns[k]) continue;
    for (const x of mine) for (const y of contrasts(factors[k], columns[k])) total += correlation(x, y) ** 2;
  }
  return total;
}

/**
 * The fixed design for one format: arm 0 is the house; arms 1..32 are the
 * rows of the array. Two-level factors take orthogonal Hadamard columns
 * (correlation exactly 0 among them); each three-level factor is a balanced
 * column improved by deterministic pairwise exchanges against everything
 * placed so far. No arm repeats and none equals the house.
 */
export function buildDesign(format: GhostFormat): { arms: GhostArm[]; maxCorrelation: number; digest: string } {
  const factors = GHOST_FACTORS[format];
  const random = nflRandom(GHOST_SEED + (format === "classic" ? 0 : 1));
  const houseIdx = factors.map(f => f.levels.indexOf(f.house));
  const columns: number[][] = [];
  let next = 0;
  factors.forEach((f, j) => {
    if (f.levels.length === 2) {
      if (next >= ODD_COLUMNS.length) throw new Error("Too many two-level factors for a 32-run resolution IV design");
      columns[j] = hadamardColumn(ODD_COLUMNS[next++]);
    }
  });
  factors.forEach((f, j) => {
    if (f.levels.length === 2) return;
    let bestColumn: number[] | null = null, bestScore = Infinity;
    for (let restart = 0; restart < GHOST_SEARCH_CANDIDATES / 100; restart++) {
      columns[j] = balancedColumn(f.levels.length, GHOST_ARMS, random);
      let score = exchangeObjective(factors, columns, j);
      for (let improved = true; improved;) {
        improved = false;
        for (let a = 0; a < GHOST_ARMS; a++) for (let b = a + 1; b < GHOST_ARMS; b++) {
          if (columns[j][a] === columns[j][b]) continue;
          [columns[j][a], columns[j][b]] = [columns[j][b], columns[j][a]];
          const s = exchangeObjective(factors, columns, j);
          if (s < score - 1e-12) { score = s; improved = true; }
          else [columns[j][a], columns[j][b]] = [columns[j][b], columns[j][a]];
        }
      }
      if (score < bestScore) { bestScore = score; bestColumn = [...columns[j]]; }
    }
    columns[j] = bestColumn!;
  });
  // A row equal to the house would duplicate the control: flip a two-level
  // column (which keeps every correlation unchanged) until none does.
  const isHouse = (i: number) => factors.every((_, j) => columns[j][i] === houseIdx[j]);
  for (let j = 0; j < factors.length && Array.from({ length: GHOST_ARMS }, (_, i) => i).some(isHouse); j++)
    if (factors[j].levels.length === 2) columns[j] = columns[j].map(v => 1 - v);
  const keys = new Set(Array.from({ length: GHOST_ARMS }, (_, i) => rowKey(columns, i)));
  if (keys.size !== GHOST_ARMS || Array.from({ length: GHOST_ARMS }, (_, i) => i).some(isHouse))
    throw new Error("Ghost design has a repeated arm or an arm equal to the house");
  const best = { columns, score: maxCrossCorrelation(factors, columns) };
  const arms: GhostArm[] = [{ arm: 0, house: true, levels: houseLevels(format) }];
  for (let i = 0; i < GHOST_ARMS; i++)
    arms.push({ arm: i + 1, house: false, levels: Object.fromEntries(factors.map((f, j) => [f.key, f.levels[best!.columns[j][i]]])) });
  const digest = createHash("sha256").update(JSON.stringify({ version: GHOST_DESIGN_VERSION, format, factors, arms })).digest("hex");
  return { arms, maxCorrelation: Math.round(best.score * 1000) / 1000, digest };
}

const cache = new Map<GhostFormat, ReturnType<typeof buildDesign>>();
export function ghostDesign(format: GhostFormat): ReturnType<typeof buildDesign> {
  if (!cache.has(format)) cache.set(format, buildDesign(format));
  return cache.get(format)!;
}
