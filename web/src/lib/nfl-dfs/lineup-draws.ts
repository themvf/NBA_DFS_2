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
 *   dependence a Gaussian copula with the captain simulation's stated priors:
 *              every offensive player loads on his game's factor (0.15 of
 *              variance) and his team's factor (0.25), so teammates correlate
 *              0.40 and opponents 0.15; a DST loads -sqrt(0.3) on the opposing
 *              offense's team factor. Different games are independent. These
 *              loadings are judgement, not fitted.
 *   seed       draw `L` of a run is a pure function of (seed, L): rebuilding a
 *              run with the same seed reproduces it exactly.
 *
 * Unvalidated: graded by replaying 2026 Showdowns against their real fields
 * (web/scripts/replay-nfl-draw-portfolio.ts) and then live, weekly.
 */
import { DST_OPPONENT_LOADING, GAME_LOADING, TEAM_LOADING, fitQuantileShape, normal, phi, quantile, type QuantileShape } from "./captain-simulation";
import { nflRandom } from "./random";

export const LINEUP_DRAWS_VERSION = "nfl-lineup-draws-v1";
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
}

interface DrawEntry { id: number; position: string; team: string; opponent: string | null; game: string; mean: number; fit: QuantileShape | null }

export interface DrawModel {
  version: string;
  seed: number;
  entries: DrawEntry[];
  teams: string[];
  games: string[];
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

export function buildDrawModel(players: readonly DrawPlayer[], seed = DEFAULT_DRAW_SEED): DrawModel {
  if (!Number.isInteger(seed) || seed < 0 || seed > 0xffff_ffff) throw new Error("Draw seed must be an unsigned 32-bit integer.");
  const fixed: number[] = [];
  const meanNotMatched: number[] = [];
  const entries = players.map((p): DrawEntry => {
    const usable = p.mean > 0 && finite(p.p10) && finite(p.median) && finite(p.p90) && p.p90 > p.p10;
    const fit = usable ? fitQuantileShape(p.mean, p.p10!, p.median!, p.p90!) : null;
    if (!fit) fixed.push(p.id);
    else if (!fit.meanMatched) meanNotMatched.push(p.id);
    return { id: p.id, position: p.position, team: p.team, opponent: p.opponent, game: gameOf(p), mean: p.mean, fit };
  });
  return {
    version: LINEUP_DRAWS_VERSION, seed, entries,
    teams: [...new Set(players.flatMap((p) => [p.team, p.opponent ?? ""]).filter(Boolean))].sort(),
    games: [...new Set(entries.map((e) => e.game))].sort(),
    fixed, meanNotMatched,
  };
}

/** The seed for draw `index` of a run: a fixed mix of the run seed and the index. */
export function drawSeed(seed: number, index: number): number {
  let value = (seed ^ Math.imul(index + 1, 2654435761)) >>> 0;
  value = Math.imul(value ^ (value >>> 16), 2246822507) >>> 0;
  return value;
}

/** One simulated game (all games on the slate): DK points by player id. */
export function drawOutcome(model: DrawModel, index: number): Map<number, number> {
  const random = nflRandom(drawSeed(model.seed, index));
  const game = Object.fromEntries(model.games.map((g) => [g, normal(random)]));
  const team = Object.fromEntries(model.teams.map((t) => [t, normal(random)]));
  const idio = Math.sqrt(1 - GAME_LOADING ** 2 - TEAM_LOADING ** 2);
  const dstIdio = Math.sqrt(1 - DST_OPPONENT_LOADING ** 2);
  const out = new Map<number, number>();
  for (const e of model.entries) {
    // Always consume the same random numbers per player so one player's
    // missing range never shifts everyone else's draws.
    const own = normal(random);
    if (!e.fit) { out.set(e.id, e.mean); continue; }
    const z = e.position === "DST"
      ? DST_OPPONENT_LOADING * (e.opponent ? team[e.opponent] ?? 0 : 0) + dstIdio * own
      : GAME_LOADING * (game[e.game] ?? 0) + TEAM_LOADING * (team[e.team] ?? 0) + idio * own;
    out.set(e.id, quantile(e.fit, phi(z)));
  }
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
