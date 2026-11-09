/**
 * Correlated simulated outcomes for building GPP lineups (`nfl-lineup-draws-v1`).
 *
 * Why (2026-11-03, CHI@SEA review): the GPP search scored every player by his
 * capped P90 and added the six or nine numbers up. Three things followed from
 * that sum, each visible in the build:
 *
 *   1. Exposure was set by the caps, not the model. The portfolio objective is
 *      linear, so a player who beats his replacement is used until his cap
 *      binds: Myers and Burden sat at exactly 25 of 50 in all three builds
 *      that night, Roschon Johnson at 23-25.
 *   2. A lineup's ceiling is not the sum of its players' ceilings. Summing
 *      P90s equals the lineup's P90 only if every pair is perfectly
 *      correlated, so a high-variance cheap player was paid in full for his
 *      spread and a correlated stack got no credit at all (the build paired
 *      Smith-Njigba with Swift in 3 of 50; the field's top 1% had both in
 *      every lineup).
 *   3. The per-lineup noise (uniform +/- randomness x projection, 0.59 points
 *      for Roschon at 0.08) never changed a choice worth 4.8 points.
 *
 * Here lineup `L` is solved against one simulated game instead: every player's
 * DK points are drawn from his own range, jointly. A player then appears in
 * roughly the share of draws in which he belongs in the best lineup, a pair
 * appears together when both boom together, and caps go back to being limits.
 *
 * Model, in full:
 *
 *   marginals  each player's quantile curve through his own P10, median and
 *              P90, bent so its mean is exactly his projection
 *              (`fitQuantileShape`, shared with the captain simulation). A
 *              player with no range is drawn at his projection every time and
 *              listed in `fixed`.
 *   dependence a Gaussian copula. Default (`stated-v1`, nfl-lineup-draws-v1,
 *              bit-for-bit): the captain simulation's stated priors, every
 *              offensive player loading on his game's factor (0.15 of
 *              variance) and his team's (0.25), so teammates correlate 0.40
 *              and opponents 0.15; a DST loads -sqrt(0.3) on the opposing
 *              offense's team factor. Different games are independent.
 *              Opt-in (`fitted` / `fitted-tail`, nfl-lineup-draws-v2): the
 *              role-pair correlations fitted on 2023-25 DK points
 *              (`correlated-normals.ts`, nfl-player-correlations-v1). They
 *              are much closer to how players actually move together (2026
 *              holdout error 0.010 vs 0.079) but did NOT build better
 *              Showdown portfolios on replay (2026-11-08, 13 slates: best
 *              lineup's percentile -1.10 overall-correlation table, -0.89
 *              joint-boom table, both against v1), so v1 stays the default.
 *              The 0.40 acts as a construction setting that concentrates
 *              lineups, not as a description of the sport.
 *   seed       draw `L` of a run is a pure function of (seed, L): rebuilding a
 *              run with the same seed reproduces it exactly.
 *   no-show    a player with a chance of no stat line (`notPlay`,
 *              nfl-range-calibration-v1) scores 0 that share of draws, on an
 *              independent coin; otherwise he draws from his if-he-plays range.
 *
 * Unvalidated: graded by replaying 2026 Showdowns against their real fields
 * (web/scripts/replay-nfl-draw-portfolio.ts) and then live, weekly.
 */
import { DST_OPPONENT_LOADING, GAME_LOADING, TEAM_LOADING, fitQuantileShape, normal, phi, quantile, type QuantileShape } from "./captain-simulation";
import { buildCorrelationStructure, correlate, type CorrelationSource, type CorrelationStructure } from "./correlated-normals";
import { PLAYER_CORRELATIONS_VERSION } from "./player-correlations";
import { nflRandom } from "./random";

/** Version of the default draws (stated priors). */
export const LINEUP_DRAWS_VERSION = "nfl-lineup-draws-v1";
/** Version of draws on a fitted correlation table (opt-in, replays). */
export const LINEUP_DRAWS_FITTED_VERSION = "nfl-lineup-draws-v2";
export { PLAYER_CORRELATIONS_VERSION };
/** Which dependence the draws use: `stated-v1` (default), or a fitted table from correlated-normals.ts. */
export type DrawCorrelations = CorrelationSource;
export const DEFAULT_DRAW_CORRELATIONS: DrawCorrelations = "stated-v1";
export const DEFAULT_DRAW_SEED = 20261103;

export interface DrawPlayer {
  id: number;
  position: string;
  team: string;
  opponent: string | null;
  gameKey: string | null;
  mean: number;
  p10: number | null;
  median: number | null;
  p90: number | null;
  /** Chance of a 0 (no stat line), nfl-range-calibration-v1; `mean` and the range are then if-he-plays. */
  notPlay?: number;
}

interface DrawEntry { id: number; position: string; team: string; opponent: string | null; game: string; mean: number; notPlay: number; fit: QuantileShape | null; role: string }

export interface DrawModel {
  version: string;
  seed: number;
  correlations: DrawCorrelations;
  entries: DrawEntry[];
  teams: string[];
  games: string[];
  /** Fitted sources only; null for the stated priors, which draw from game and team factors. */
  structure: CorrelationStructure | null;
  /** Games whose correlation matrix was not valid as built and was repaired to the nearest valid one. */
  repairedGames: string[];
  /** Players drawn at their projection every time (no usable range). */
  fixed: number[];
  /** Players whose projection no curve through their quantiles reaches (inputs disagree). */
  meanNotMatched: number[];
}

const finite = (v: number | null | undefined): v is number => typeof v === "number" && Number.isFinite(v);

function gameOf(p: DrawPlayer): string {
  if (p.gameKey) return p.gameKey;
  return [p.team, p.opponent ?? ""].sort().join("@");
}

export function buildDrawModel(players: readonly DrawPlayer[], seed = DEFAULT_DRAW_SEED, correlations: DrawCorrelations = DEFAULT_DRAW_CORRELATIONS): DrawModel {
  if (!Number.isInteger(seed) || seed < 0 || seed > 0xffff_ffff) throw new Error("Draw seed must be an unsigned 32-bit integer.");
  const fixed: number[] = [];
  const meanNotMatched: number[] = [];
  const entries = players.map((p): DrawEntry => {
    const usable = p.mean > 0 && finite(p.p10) && finite(p.median) && finite(p.p90) && p.p90 > p.p10;
    const fit = usable ? fitQuantileShape(p.mean, p.p10!, p.median!, p.p90!) : null;
    if (!fit) fixed.push(p.id);
    else if (!fit.meanMatched) meanNotMatched.push(p.id);
    const notPlay = finite(p.notPlay) ? Math.min(Math.max(p.notPlay, 0), 1) : 0;
    return { id: p.id, position: p.position, team: p.team, opponent: p.opponent, game: gameOf(p), mean: p.mean, notPlay, fit, role: p.position };
  });
  // Fitted sources: roles rank each team's players at a position by projection
  // (if he plays x the chance he plays), as the fit ranked them by season average.
  const structure = correlations === "stated-v1" ? null
    : buildCorrelationStructure(entries.map((e) => ({ id: e.id, position: e.position, team: e.team, game: e.game,
      expected: e.mean * (1 - e.notPlay), correlated: e.fit != null })), correlations);
  if (structure) entries.forEach((e, i) => { e.role = structure.roles[i]; });
  return {
    version: structure ? LINEUP_DRAWS_FITTED_VERSION : LINEUP_DRAWS_VERSION, seed, correlations, entries,
    teams: [...new Set(players.flatMap((p) => [p.team, p.opponent ?? ""]).filter(Boolean))].sort(),
    games: [...new Set(entries.map((e) => e.game))].sort(),
    structure, repairedGames: structure?.repairedGames ?? [], fixed, meanNotMatched,
  };
}

/** The seed for draw `index` of a run: a fixed mix of the run seed and the index. */
export function drawSeed(seed: number, index: number): number {
  let value = (seed ^ Math.imul(index + 1, 2654435761)) >>> 0;
  value = Math.imul(value ^ (value >>> 16), 2246822507) >>> 0;
  return value;
}

function unitHash(seed: number, index: number, id: number): number {
  let v = (drawSeed(seed, index) ^ Math.imul(id, 1597334677)) >>> 0;
  v = Math.imul(v ^ (v >>> 15), 2246822507) >>> 0;
  v = Math.imul(v ^ (v >>> 13), 3266489909) >>> 0;
  return ((v ^ (v >>> 16)) >>> 0) / 4294967296;
}

/** One simulated game (all games on the slate): DK points by player id. */
export function drawOutcome(model: DrawModel, index: number): Map<number, number> {
  const random = nflRandom(drawSeed(model.seed, index));
  if (model.structure) return drawFitted(model, model.structure, index, random);
  const game = Object.fromEntries(model.games.map((g) => [g, normal(random)]));
  const team = Object.fromEntries(model.teams.map((t) => [t, normal(random)]));
  const idio = Math.sqrt(1 - GAME_LOADING ** 2 - TEAM_LOADING ** 2);
  const dstIdio = Math.sqrt(1 - DST_OPPONENT_LOADING ** 2);
  const out = new Map<number, number>();
  for (const e of model.entries) {
    // Always consume the same random numbers per player so one player's
    // missing range never shifts everyone else's draws.
    const own = normal(random);
    // Whether he plays is its own coin, hashed from (seed, draw, player) so it
    // takes nothing from the shared stream: players with no chance of a 0
    // draw exactly what nfl-lineup-draws-v1 drew.
    if (e.notPlay > 0 && unitHash(model.seed, index, e.id) < e.notPlay) { out.set(e.id, 0); continue; }
    if (!e.fit) { out.set(e.id, e.mean); continue; }
    const z = e.position === "DST"
      ? DST_OPPONENT_LOADING * (e.opponent ? team[e.opponent] ?? 0 : 0) + dstIdio * own
      : GAME_LOADING * (game[e.game] ?? 0) + TEAM_LOADING * (team[e.team] ?? 0) + idio * own;
    out.set(e.id, quantile(e.fit, phi(z)));
  }
  return out;
}

/** Draws on a fitted correlation table: one normal per player in pool order, correlated within each game. */
function drawFitted(model: DrawModel, structure: CorrelationStructure, index: number, random: () => number): Map<number, number> {
  const z = correlate(structure, model.entries.map(() => normal(random)));
  const out = new Map<number, number>();
  model.entries.forEach((e, i) => {
    if (out.has(e.id)) return;
    if (e.notPlay > 0 && unitHash(model.seed, index, e.id) < e.notPlay) { out.set(e.id, 0); return; }
    if (!e.fit) { out.set(e.id, e.mean); return; }
    out.set(e.id, quantile(e.fit, phi(z[i])));
  });
  return out;
}

/** Lineup-level duplication leverage (`nfl-lineup-leverage-v1`), in points per slot.
 *
 * How many rivals hold your exact lineup is about the field size times the
 * PRODUCT of its players' slot ownerships, so its log is a SUM over slots:
 * ln D = ln N + sum ln(own). A linear search can therefore price duplication
 * exactly in log terms: each slot is credited `pointsPerHalving x log2(1/own)`.
 * The old form multiplied each player's ceiling by (1 - own)^k, which acts on
 * player points rather than on the lineup's duplication. `pointsPerHalving`
 * is a stated prior (how many points a lineup gives up to halve the number of
 * rivals holding it), not fitted. Unknown ownership is neutral (0).
 */
export const DEFAULT_LEVERAGE_POINTS_PER_HALVING = 1;
const OWNERSHIP_FLOOR = 0.5, OWNERSHIP_CEILING = 95;

export function slotLeverageBonus(ownPct: number | null | undefined, pointsPerHalving = DEFAULT_LEVERAGE_POINTS_PER_HALVING): number {
  if (!finite(ownPct) || !(pointsPerHalving > 0)) return 0;
  const own = Math.min(OWNERSHIP_CEILING, Math.max(OWNERSHIP_FLOOR, ownPct)) / 100;
  return pointsPerHalving * Math.log2(1 / own);
}
