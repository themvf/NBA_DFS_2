/**
 * Correlated standard normals for one slate's players, by game
 * (`nfl-player-correlations-v1`). Shared by the simulated-game lineup scorer
 * (`lineup-draws.ts`) and the captain simulation (`captain-simulation.ts`), so
 * both read one dependence model.
 *
 * Within each game, every team's players are ranked by expected points at
 * their position (QB1/QB2, RB1-RB3, WR1-WR4, TE1/TE2, DST, K), and each pair
 * takes the correlation fitted for (same team or opponents, role, role) in
 * `player-correlations.ts` (research/nfl_dfs_player_correlations.py; 2023-25
 * DK points, 2026 held out). A role pair the fit never saw takes its coarse
 * class (QB / SKILL / DST / K). Each game's matrix is repaired to the nearest
 * valid correlation matrix if it is not one already, then factored once:
 * z = F eps with F F^T the matrix. Different games are independent.
 *
 * `stated-v1` keeps the pre-fit priors (every teammate pair 0.40, every
 * opponent pair 0.15, a DST -0.27 with each opposing player) for replays.
 */
import { CLASS_CORRELATION, CLASS_CORRELATION_TAIL, PAIR_CORRELATION, PAIR_CORRELATION_TAIL, ROLE_DEPTH } from "./player-correlations";

/** v1 stated priors (judgement, never fitted): game factor sqrt(.15), team factor sqrt(.25), DST -sqrt(.3) on the opposing team. */
export const GAME_LOADING = Math.sqrt(0.15);
export const TEAM_LOADING = Math.sqrt(0.25);
export const DST_OPPONENT_LOADING = -Math.sqrt(0.3);

/** `fitted`: overall co-movement; `fitted-tail`: matched to how often both boom (joint top 20%); `stated-v1`: pre-fit priors. */
export type CorrelationSource = "fitted" | "fitted-tail" | "stated-v1";

export interface CorrelatedMember {
  id: number;
  position: string;
  team: string;
  game: string;
  /** Expected points, used only to rank a team's players at a position into roles. */
  expected: number;
  /** False for a player who never needs a draw (no range); he takes a role but no row in the matrix. */
  correlated: boolean;
}

/** One game's factor F (row-major, n x n) over `members` (indexes into the member list). */
export interface GameFactor { members: number[]; factor: Float64Array }

export interface CorrelationStructure {
  source: CorrelationSource;
  /** Role of each member, by index. */
  roles: string[];
  factors: GameFactor[];
  /** Games whose matrix was not a valid correlation matrix as built and was repaired. */
  repairedGames: string[];
}

type Pairable = { team: string; position: string; role: string };

function roleOf(position: string, rank: number): string {
  const depth = ROLE_DEPTH[position];
  return depth && depth > 1 ? `${position}${Math.min(rank, depth)}` : position;
}

function coarse(position: string): string {
  return position === "QB" || position === "DST" || position === "K" ? position : "SKILL";
}

/** Fitted correlation for two players in one game; the coarse-class value for a role pair never seen. */
export function pairCorrelation(a: Pairable, b: Pairable, tail = false): number {
  const side = a.team === b.team ? "same" : "opp";
  const [x, y] = [a.role, b.role].sort();
  const fitted = (tail ? PAIR_CORRELATION_TAIL : PAIR_CORRELATION)[`${side}:${x}:${y}`];
  if (fitted != null) return fitted;
  const [cx, cy] = [coarse(a.position), coarse(b.position)].sort();
  return (tail ? CLASS_CORRELATION_TAIL : CLASS_CORRELATION)[`${side}:${cx}:${cy}`] ?? 0;
}

/** v1's stated priors as a pair correlation. */
function statedCorrelation(a: Pairable, b: Pairable): number {
  const dstA = a.position === "DST", dstB = b.position === "DST";
  if (dstA && dstB) return 0;
  if (dstA || dstB) return a.team === b.team ? 0 : DST_OPPONENT_LOADING * TEAM_LOADING;
  return a.team === b.team ? GAME_LOADING ** 2 + TEAM_LOADING ** 2 : GAME_LOADING ** 2;
}

/** Symmetric eigen-decomposition by cyclic Jacobi (deterministic; n is one game's players). */
function jacobiEigen(matrix: Float64Array, n: number): { values: Float64Array; vectors: Float64Array } {
  const a = Float64Array.from(matrix), v = new Float64Array(n * n);
  for (let i = 0; i < n; i++) v[i * n + i] = 1;
  for (let sweep = 0; sweep < 60; sweep++) {
    let off = 0;
    for (let p = 0; p < n; p++) for (let q = p + 1; q < n; q++) off += a[p * n + q] ** 2;
    if (off < 1e-22) break;
    for (let p = 0; p < n; p++) for (let q = p + 1; q < n; q++) {
      const apq = a[p * n + q];
      if (Math.abs(apq) < 1e-15) continue;
      const theta = (a[q * n + q] - a[p * n + p]) / (2 * apq);
      const t = (theta >= 0 ? 1 : -1) / (Math.abs(theta) + Math.sqrt(theta * theta + 1));
      const c = 1 / Math.sqrt(t * t + 1), s = t * c;
      for (let k = 0; k < n; k++) {
        const akp = a[k * n + p], akq = a[k * n + q];
        a[k * n + p] = c * akp - s * akq; a[k * n + q] = s * akp + c * akq;
      }
      for (let k = 0; k < n; k++) {
        const apk = a[p * n + k], aqk = a[q * n + k];
        a[p * n + k] = c * apk - s * aqk; a[q * n + k] = s * apk + c * aqk;
      }
      for (let k = 0; k < n; k++) {
        const vkp = v[k * n + p], vkq = v[k * n + q];
        v[k * n + p] = c * vkp - s * vkq; v[k * n + q] = s * vkp + c * vkq;
      }
    }
  }
  const values = new Float64Array(n);
  for (let i = 0; i < n; i++) values[i] = a[i * n + i];
  return { values, vectors: v };
}

/** Factor F with F F^T the nearest valid correlation matrix: eigenvalues clipped at a floor, rows rescaled to unit variance. */
export function correlationFactor(matrix: Float64Array, n: number): { factor: Float64Array; repaired: boolean } {
  const { values, vectors } = jacobiEigen(matrix, n);
  const floor = 1e-6;
  const repaired = values.some((x) => x < floor);
  const factor = new Float64Array(n * n);
  for (let i = 0; i < n; i++) {
    let norm = 0;
    for (let k = 0; k < n; k++) { const f = vectors[i * n + k] * Math.sqrt(Math.max(values[k], floor)); factor[i * n + k] = f; norm += f * f; }
    const scale = 1 / Math.sqrt(norm);
    for (let k = 0; k < n; k++) factor[i * n + k] *= scale;
  }
  return { factor, repaired };
}

export function buildCorrelationStructure(members: readonly CorrelatedMember[], source: CorrelationSource = "fitted"): CorrelationStructure {
  const roles = members.map((m) => m.position);
  const slots = new Map<string, number[]>();
  members.forEach((m, i) => { const k = `${m.game}|${m.team}|${m.position}`; const list = slots.get(k); if (list) list.push(i); else slots.set(k, [i]); });
  for (const list of slots.values()) {
    list.sort((a, b) => members[b].expected - members[a].expected || members[a].id - members[b].id);
    list.forEach((i, rank) => { roles[i] = roleOf(members[i].position, rank + 1); });
  }
  const pair = source === "stated-v1" ? statedCorrelation : (a: Pairable, b: Pairable) => pairCorrelation(a, b, source === "fitted-tail");
  const at = (i: number): Pairable => ({ team: members[i].team, position: members[i].position, role: roles[i] });
  const factors: GameFactor[] = [];
  const repairedGames: string[] = [];
  const seen = new Set<number>();
  for (const game of [...new Set(members.map((m) => m.game))].sort()) {
    // One row per player id: a repeated row reads the first one's normal.
    const rows: number[] = [];
    members.forEach((m, i) => { if (m.game === game && m.correlated && !seen.has(m.id)) { seen.add(m.id); rows.push(i); } });
    const n = rows.length;
    if (!n) continue;
    const matrix = new Float64Array(n * n);
    for (let i = 0; i < n; i++) {
      matrix[i * n + i] = 1;
      for (let j = i + 1; j < n; j++) { const r = pair(at(rows[i]), at(rows[j])); matrix[i * n + j] = r; matrix[j * n + i] = r; }
    }
    const { factor, repaired } = correlationFactor(matrix, n);
    if (repaired) repairedGames.push(game);
    factors.push({ members: rows, factor });
  }
  return { source, roles, factors, repairedGames };
}

/** Correlated normals by member index from independent normals `eps` (one per member); members outside every factor get 0. */
export function correlate(structure: CorrelationStructure, eps: readonly number[]): Float64Array {
  const z = new Float64Array(eps.length);
  for (const { members, factor } of structure.factors) {
    const n = members.length;
    for (let i = 0; i < n; i++) {
      let sum = 0;
      for (let k = 0; k < n; k++) sum += factor[i * n + k] * eps[members[k]];
      z[members[i]] = sum;
    }
  }
  return z;
}
