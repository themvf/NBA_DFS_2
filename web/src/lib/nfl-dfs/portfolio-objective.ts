/**
 * Research challenger for the NFL portfolio objective (`nfl-portfolio-objective-v1`).
 *
 * The production optimizer scores a lineup by adding capped player ceilings
 * (see `objective()` in nfl-optimizer.ts). This module instead scores every
 * candidate lineup against a FIELD, on JOINT player draws, and picks the
 * portfolio that most often lands at least one lineup inside the contest's
 * top 1%. Three parts, each a stated model with its own version string:
 *
 *   joint draws  `nfl-joint-copula-v1`: the captain simulation's Gaussian
 *                copula (captain-simulation.ts), extended to a multi-game
 *                slate: one factor per game, one per team, a DST loading
 *                negatively on its opponent's team factor; marginals are the
 *                model's own P10/median/P90 bent to the projection. Loadings
 *                are the same stated priors (0.40 same team, 0.15 same game).
 *   field        `nfl-field-stacked-v2`: each opponent lineup first draws its
 *                SHAPE from the measured field stacking rates
 *                (`FIELD_STACKING`, model/nfl_dfs_field_stacks.py over the
 *                stored flagships: a QB stack in 90% of Classic entries, with
 *                how many pass catchers, a running back and a bring-back;
 *                the 3-3 / 4-2 / 5-1 team split in Showdown), then fills the
 *                stack and the remaining slots from the ownership prior
 *                (captain share for CPT, flex share for FLEX), rejected when
 *                it breaks DraftKings' rules. v1 drew slot by slot with no
 *                shape, and its field failed the freeze gate because nobody
 *                in it stacked. Scored on the SAME joint draws as our
 *                candidates, so the covariance between a lineup and the
 *                field's top scores comes out by construction rather than
 *                being estimated separately (the Cov(X_i, F) term in Haugh &
 *                Singal, 2019).
 *   selection    greedy marginal coverage of P(any lineup >= the field's
 *                within-draw top-1% score), under the source build's own
 *                exposure cap and uniqueness, from a candidate pool the
 *                production optimizer generated with the same settings.
 *
 * Selection runs on one draw stream and the recorded forecast on an
 * independent one, so the forecast is not flattered by the search.
 *
 * Nothing here is validated. Within-draw field quantiles come from a few
 * thousand sampled opponents, so the top 1% is estimable and the contest
 * winner is not; no first-place or payout claim is made. The field model is
 * only as good as the ownership prior (Classic Spearman ~0.80 against
 * imported contests; Showdown weaker), and sampling opponents slot by slot
 * ignores how real entrants stack. Both limits are named in the registration
 * (docs/nfl-portfolio-objective-study.md).
 */
import { DK_SALARY_CAP, type NflDkPlayer, type NflDkSlate, type NflPosition } from "./dk-salary-csv";
import { nflSlots, validateNflLineup, type NflLineup, type NflSlot } from "./lineups";
import { nflRandom } from "./random";
import { fitQuantileShape, quantile, GAME_LOADING, TEAM_LOADING, DST_OPPONENT_LOADING } from "./captain-simulation";
import type { NflGeneratedLineup } from "@/app/dfs/nfl/nfl-optimizer";

export const PORTFOLIO_OBJECTIVE_VERSION = "nfl-portfolio-objective-v2";
export const JOINT_COPULA_VERSION = "nfl-joint-copula-v1";
export const FIELD_MODEL_VERSION = "nfl-field-stacked-v2";

/**
 * How the field stacks: MEASURED, not fitted, and frozen with the version.
 * Source: `nfl-dfs-field-stacks-v1` (model/nfl_dfs_field_stacks.py) pooled
 * over the stored 2026 flagships on 2026-10-15. Classic is the whole field
 * (five Millionaires, weeks 1-5, 1,490,188 entries; stat-api labels every
 * standings row). Showdown is the top-100 users' every lineup of eight
 * flagships (40,891 lineups): stat-api labels no showdown row, so this is an
 * outcome-selected population, named as such. Conditional rates below are
 * the pooled "all entries" shares divided by the QB-stack share. None of
 * these numbers were chosen by looking at realized score thresholds.
 */
export const FIELD_STACKING = {
  source: "nfl-dfs-field-stacks-v1 pooled 2026-10-15: classic = field of 5 Millionaires w1-5 (1,490,188 entries); showdown = top-100 users of 8 flagships (40,891 lineups, outcome-selected)",
  classic: {
    /** Share of entries whose QB has at least one skill-position teammate. */
    qbStack: 0.9016,
    /** Given a stack: pass catchers (WR/TE) with the QB, 0 / 1 / 2 / 3+. */
    catchersGivenStack: [0.083, 0.582, 0.314, 0.021],
    /** Given a stack: a running back from the QB's team is in the lineup. */
    rbGivenStack: 0.205,
    /** Given a stack: skill players from the opponent (bring-back), 0 / 1 / 2+. */
    bringBackGivenStack: [0.499, 0.410, 0.091],
  },
  showdown: {
    population: "top_users",
    /** Majority-team count 3 / 4 / 5 (the 3-3, 4-2 and 5-1 splits; no 6-0 was observed). */
    split: { 3: 0.3231, 4: 0.4636, 5: 0.2133 },
  },
} as const;

/** Stated, frozen with the registration. Change one and bump the version. */
export const PORTFOLIO_OBJECTIVE = {
  draws: 4000,
  selectionSeed: 20261016,
  evaluationSeed: 20261017,
  fieldSeed: 20261018,
  /** Sampled opponent lineups per slate; the within-draw top-1% is their 20th-best score. */
  fieldLineups: 2000,
  /** The success event: at least one of our lineups at or above the field's within-draw top-1% score. */
  successFraction: 0.01,
  /** Secondary, descriptive only: the within-draw top-0.1% (2nd-best of 2000 is too coarse; recorded, not used). */
  secondaryFraction: 0.001,
  /** Candidate pool: the production optimizer run at this multiple of the source's lineup count (bounded). */
  candidateMultiple: 5,
  candidateMin: 60,
  candidateMax: 200,
  /** Rejection sampler limits; a slate that cannot yield the field is refused, never padded. */
  maxFieldAttempts: 600_000,
  /** Ownership calibration: pilot samples per round, damped multiplicative update, multiplier bounds. */
  fieldCalibration: { rounds: 8, pilot: 800, step: 0.6, floor: 0.02, cap: 100 },
  /** A player with no P10/median/P90 is drawn from this band around his projection and counted. */
  fallbackBand: { p10: 0.74, p90: 1.28 },
} as const;

// Frozen database JSON (the research runner's player snapshot); fields beyond these are ignored.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type SnapshotPlayer = Record<string, any>;

type Marginal = { fit: ReturnType<typeof fitQuantileShape>; fallback: boolean };

const Z = (random: () => number) => {
  let u = 0;
  while (u === 0) u = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * random());
};
/** Standard normal CDF (Abramowitz & Stegun 7.1.26). */
function phi(z: number): number {
  const t = 1 / (1 + 0.3275911 * Math.abs(z) / Math.SQRT2);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-(z * z) / 2);
  return z >= 0 ? (1 + y) / 2 : (1 - y) / 2;
}

/** The research slate (`NflDkSlate`) rebuilt from a frozen optimizer snapshot; identity only, no values change. */
export function slateFromSnapshot(format: "classic" | "showdown", players: readonly SnapshotPlayer[]): NflDkSlate {
  const rows: NflDkPlayer[] = players.map(p => ({
    dkPlayerId: p.dkPlayerId, name: p.name, position: p.position as NflPosition,
    rosterPositions: Array.isArray(p.rosterPositions) && p.rosterPositions.length ? p.rosterPositions
      : format === "showdown" ? ["FLEX"] : [p.position, ...(["RB", "WR", "TE"].includes(p.position) ? ["FLEX"] : [])],
    teamAbbrev: p.team, opponent: p.opponent ?? null, homeAway: null, gameKey: p.gameKey ?? null, gameInfo: null,
    salary: p.salary, avgFptsDk: p.dkAvg ?? null, status: p.status ?? null, isOut: p.isOut === true,
    captain: p.captainDkPlayerId != null && p.captainSalary != null ? { dkPlayerId: p.captainDkPlayerId, salary: p.captainSalary } : null,
  }));
  const games = [...new Set(rows.map(r => r.gameKey).filter((g): g is string => !!g))].sort();
  const teams = [...new Set(rows.map(r => r.teamAbbrev))].sort();
  return { format, players: rows, games, teams, warnings: [] };
}

/** A generated lineup as the research lineup type (slot names without their index). */
export function rosterOf(lineup: Pick<NflGeneratedLineup, "slots">): NflLineup {
  return lineup.slots.map(s => ({ slot: s.slot.replace(/\d+$/, "") as NflSlot, playerId: s.player.dkPlayerId }));
}

export type JointBank = {
  version: typeof JOINT_COPULA_VERSION;
  seed: number;
  draws: number;
  /** Player-major: dkPlayerId -> one DK score per draw (hundredths exact). */
  scores: Map<number, Float64Array>;
  /** Players whose P10/median/P90 were missing and were drawn from the fallback band. */
  fallbackPlayerIds: number[];
  /** Players whose projection no quantile curve could reach (inputs disagree). */
  meanNotMatchedIds: number[];
};

function marginal(p: SnapshotPlayer): Marginal {
  const proj = Number(p.ourProj);
  const have = [p.floor, p.median, p.ceiling].every(v => typeof v === "number" && Number.isFinite(v));
  if (have) return { fit: fitQuantileShape(proj, p.floor, p.median, p.ceiling), fallback: false };
  return { fit: fitQuantileShape(proj, proj * PORTFOLIO_OBJECTIVE.fallbackBand.p10, proj, proj * PORTFOLIO_OBJECTIVE.fallbackBand.p90), fallback: true };
}

/** Joint DK-point draws for every player with a positive projection; players without one score 0 in every draw. */
export function drawJointBank(players: readonly SnapshotPlayer[], seed: number, draws = PORTFOLIO_OBJECTIVE.draws): JointBank {
  const random = nflRandom(seed);
  const active = players.filter(p => Number(p.ourProj) > 0 && p.isOut !== true);
  const fits = new Map(active.map(p => [p.dkPlayerId as number, marginal(p)]));
  const games = [...new Set(active.map(p => String(p.gameKey)))];
  const teams = [...new Set(active.map(p => String(p.team)))];
  const scores = new Map<number, Float64Array>(players.map(p => [p.dkPlayerId as number, new Float64Array(draws)]));
  const idio = Math.sqrt(1 - GAME_LOADING ** 2 - TEAM_LOADING ** 2), dstIdio = Math.sqrt(1 - DST_OPPONENT_LOADING ** 2);
  for (let d = 0; d < draws; d++) {
    const game = new Map(games.map(g => [g, Z(random)]));
    const team = new Map(teams.map(t => [t, Z(random)]));
    for (const p of active) {
      const z = p.position === "DST"
        ? DST_OPPONENT_LOADING * (team.get(String(p.opponent)) ?? 0) + dstIdio * Z(random)
        : GAME_LOADING * (game.get(String(p.gameKey)) ?? 0) + TEAM_LOADING * (team.get(String(p.team)) ?? 0) + idio * Z(random);
      // DK scores are exact hundredths; keep every draw on that grid so sums are exact.
      scores.get(p.dkPlayerId)![d] = Math.round(quantile(fits.get(p.dkPlayerId)!.fit, phi(z)) * 100) / 100;
    }
  }
  return { version: JOINT_COPULA_VERSION, seed, draws,
    scores, fallbackPlayerIds: active.filter(p => fits.get(p.dkPlayerId)!.fallback).map(p => p.dkPlayerId),
    meanNotMatchedIds: active.filter(p => !fits.get(p.dkPlayerId)!.fit.meanMatched).map(p => p.dkPlayerId) };
}

/** Lineup score per draw (CPT counts 1.5x), exact to the hundredth. */
export function scoreLineup(lineup: NflLineup, bank: JointBank): Float64Array {
  const out = new Float64Array(bank.draws);
  for (const entry of lineup) {
    const draws = bank.scores.get(entry.playerId);
    if (!draws) throw new Error(`No draws for player ${entry.playerId}`);
    const m = entry.slot === "CPT" ? 1.5 : 1;
    for (let d = 0; d < bank.draws; d++) out[d] = Math.round((out[d] + draws[d] * m) * 100) / 100;
  }
  return out;
}

export type FieldSample = {
  version: typeof FIELD_MODEL_VERSION;
  seed: number;
  lineups: NflLineup[];
  attempts: number;
  /** Ownership (percent of sampled lineups) each player ended up with; compare with the prior. */
  realizedOwnPct: Map<number, number>;
  /** How well the sampled field reproduces the prior it was drawn from, over players the prior puts at 5%+. */
  calibration: { rounds: number; maxAbsDeviationPct: number; meanAbsDeviationPct: number; players: number };
  /** The stack shapes the sampled lineups actually have, to set beside FIELD_STACKING. */
  shapes: FieldShapeStats;
};

export type FieldShapeStats = {
  lineups: number;
  classic?: { qbStack: number; catchersWithQb: Record<string, number>; rbWithQb: number; teWithQb: number; bringBack: Record<string, number> };
  showdown?: { split: Record<string, number>; anyQb: number; twoQbs: number; qbPlusTeammates: Record<string, number> };
};

const SKILL = new Set<string>(["WR", "TE", "RB"]);

/** Realized stack shapes of sampled lineups, read off the lineups themselves (never the draw's intent). */
export function fieldShapeStats(slate: NflDkSlate, lineups: NflLineup[]): FieldShapeStats {
  const byId = new Map(slate.players.map(p => [p.dkPlayerId, p]));
  const n = lineups.length;
  const rate = (c: number) => Math.round((10000 * c) / Math.max(1, n)) / 10000;
  const rates = (o: Record<string, number>) => Object.fromEntries(Object.entries(o).map(([a, b]) => [a, rate(b)]));
  if (slate.format === "classic") {
    let stacked = 0, rb = 0, te = 0;
    const catchers: Record<string, number> = { "0": 0, "1": 0, "2": 0, "3+": 0 }, bring: Record<string, number> = { "0": 0, "1": 0, "2+": 0 };
    for (const l of lineups) {
      const ps = l.map(e => byId.get(e.playerId)!);
      const qb = ps.find(p => p.position === "QB");
      if (!qb) continue;
      const mates = ps.filter(p => p !== qb && p.teamAbbrev === qb.teamAbbrev && SKILL.has(p.position));
      if (!mates.length) continue;
      stacked++;
      const c = mates.filter(p => p.position !== "RB").length;
      catchers[c >= 3 ? "3+" : String(c)]++;
      if (mates.some(p => p.position === "RB")) rb++;
      if (mates.some(p => p.position === "TE")) te++;
      const bb = ps.filter(p => p.teamAbbrev === qb.opponent && SKILL.has(p.position)).length;
      bring[bb >= 2 ? "2+" : String(bb)]++;
    }
    return { lineups: n, classic: { qbStack: rate(stacked), catchersWithQb: rates(catchers), rbWithQb: rate(rb), teWithQb: rate(te), bringBack: rates(bring) } };
  }
  const split: Record<string, number> = {};
  const mates: Record<string, number> = { "0": 0, "1": 0, "2": 0, "3+": 0 };
  let anyQb = 0, twoQbs = 0;
  for (const l of lineups) {
    const ps = l.map(e => byId.get(e.playerId)!);
    const teams = new Map<string, number>();
    for (const p of ps) teams.set(p.teamAbbrev, (teams.get(p.teamAbbrev) ?? 0) + 1);
    const key = [...teams.values()].sort((a, b) => b - a).join("-");
    split[key] = (split[key] ?? 0) + 1;
    const qbs = ps.filter(p => p.position === "QB");
    if (!qbs.length) continue;
    anyQb++;
    if (qbs.length >= 2) twoQbs++;
    const m = ps.filter(p => p !== qbs[0] && p.teamAbbrev === qbs[0].teamAbbrev && SKILL.has(p.position)).length;
    mates[m >= 3 ? "3+" : String(m)]++;
  }
  return { lineups: n, showdown: { split: rates(Object.fromEntries(Object.entries(split).sort())), anyQb: rate(anyQb), twoQbs: rate(twoQbs), qbPlusTeammates: rates(mates) } };
}

/** Slot-wise sampling weights after calibration: dkPlayerId -> multiplier applied to the prior. */
type Multipliers = Map<number, { base: number; captain: number }>;

type PoolEntry = { p: NflDkPlayer; w: number; cpt: boolean };

/** Draw an index from `weights` (all non-negative); -1 when they sum to zero. */
function pickWeighted(weights: number[], random: () => number): number {
  const total = weights.reduce((s, w) => s + w, 0);
  if (!total) return -1;
  let r = random() * total;
  for (let j = 0; j < weights.length; j++) { r -= weights[j]; if (r <= 0) return j; }
  return weights.length - 1;
}

/** Draw a category index from a stated distribution (the last category absorbs any rounding). */
const drawCategory = (shares: readonly number[], random: () => number) => {
  const i = pickWeighted([...shares], random);
  return i < 0 ? shares.length - 1 : i;
};

/**
 * Opponent lineups, one at a time: draw the lineup's SHAPE from
 * `FIELD_STACKING`, place the stack, then fill the open slots from the
 * ownership prior (with the calibration multipliers), without replacement,
 * under the salary reservation. A lineup that cannot seat its stack, breaks
 * the roster rules or the cap is thrown away, never repaired.
 *
 * Classic: with probability `qbStack` the lineup is built around a QB; it then
 * takes 0-3 pass catchers and maybe a running back from his team and 0-2
 * skill players from his opponent, each drawn ownership-weighted within that
 * team. Everything else is the v1 slot-wise fill, so a lineup can still end up
 * with more teammates than its shape asked for (the fill is not told to avoid
 * them), and the realized shape is what `fieldShapeStats` reports.
 *
 * Showdown: the majority-team count is drawn from `split` (3, 4 or 5), the
 * majority team by its ownership mass, and the fill is confined to those
 * counts. The pairing of a QB with his receivers is not drawn; it is whatever
 * the ownership weights produce.
 */
function drawLineups(slate: NflDkSlate, pools: PoolEntry[][], mult: Multipliers,
  random: () => number, count: number, maxAttempts: number): { lineups: NflLineup[]; attempts: number } {
  const slots = nflSlots(slate.format);
  const cost = (x: { p: NflDkPlayer; cpt: boolean }) => x.cpt ? x.p.captain!.salary : x.p.salary;
  const weightOf = (x: PoolEntry) => x.w * (x.cpt ? mult.get(x.p.dkPlayerId)!.captain : mult.get(x.p.dkPlayerId)!.base);
  // Salary to keep for the slots still open after slot i: their ownership-weighted
  // expected cost (not the cheapest fill, which let early slots spend the whole cap
  // and left the DST slot with only the minimum-priced defence on the week-5
  // Classic), floored at the cheapest fill so the cap always stays reachable.
  const expected = (pool: PoolEntry[]) => {
    const total = pool.reduce((s, x) => s + x.w, 0);
    return total ? pool.reduce((s, x) => s + x.w * cost(x), 0) / total : Math.min(...pool.map(cost));
  };
  const expectedBySlot = pools.map(expected), cheapestBySlot = pools.map(pool => Math.min(...pool.map(cost)));
  // Classic stack pools: the ownership-weighted skill players of each team, by position group.
  const classic = slate.format === "classic";
  const byTeam = new Map<string, { catchers: PoolEntry[]; rbs: PoolEntry[] }>();
  if (classic) {
    const seen = new Set<number>();
    for (const pool of pools) for (const x of pool) {
      if (seen.has(x.p.dkPlayerId) || !SKILL.has(x.p.position)) continue;
      seen.add(x.p.dkPlayerId);
      const t = byTeam.get(x.p.teamAbbrev) ?? { catchers: [], rbs: [] };
      (x.p.position === "RB" ? t.rbs : t.catchers).push(x);
      byTeam.set(x.p.teamAbbrev, t);
    }
  }
  const byId = new Map(slate.players.map(p => [p.dkPlayerId, p]));
  const teamMass = new Map<string, number>();
  for (const x of pools[pools.length - 1]) teamMass.set(x.p.teamAbbrev, (teamMass.get(x.p.teamAbbrev) ?? 0) + x.w);
  const showdownTeams = [...teamMass.keys()];
  const splitShares = Object.entries(FIELD_STACKING.showdown.split).map(([k, v]) => [Number(k), v] as const);

  // Draw `k` entries without replacement from `pool`, ownership-weighted, skipping used ids.
  const drawFrom = (pool: PoolEntry[], k: number, used: Set<number>): PoolEntry[] => {
    const out: PoolEntry[] = [];
    let candidates = pool.filter(x => !used.has(x.p.dkPlayerId));
    while (out.length < k && candidates.length) {
      const i = pickWeighted(candidates.map(weightOf), random);
      if (i < 0) break;
      out.push(candidates[i]);
      candidates = candidates.filter((_, j) => j !== i);
    }
    return out;
  };

  const lineups: NflLineup[] = [];
  let attempts = 0;
  while (lineups.length < count && attempts < maxAttempts) {
    attempts++;
    const used = new Set<number>();
    const filled: Array<{ slot: NflSlot; playerId: number } | null> = slots.map(() => null);
    let spent = 0;
    let quota: Map<string, number> | null = null;
    let seatFailed = false;
    const teamCount = new Map<string, number>();
    const seat = (x: PoolEntry) => {
      // The player's own slot first, then FLEX; a player with no open seat fails the attempt.
      let i = slots.findIndex((s, j) => filled[j] === null && s === x.p.position);
      if (i < 0 && SKILL.has(x.p.position)) i = slots.findIndex((s, j) => filled[j] === null && s === "FLEX");
      if (i < 0) { seatFailed = true; return; }
      filled[i] = { slot: slots[i], playerId: x.p.dkPlayerId };
      used.add(x.p.dkPlayerId);
      spent += cost(x);
      teamCount.set(x.p.teamAbbrev, (teamCount.get(x.p.teamAbbrev) ?? 0) + 1);
    };
    if (classic) {
      if (random() < FIELD_STACKING.classic.qbStack) {
        const [qb] = drawFrom(pools[0], 1, used);
        if (!qb) continue;
        seat(qb);
        const team = byTeam.get(qb.p.teamAbbrev), opp = qb.p.opponent ? byTeam.get(qb.p.opponent) : undefined;
        const c = drawCategory(FIELD_STACKING.classic.catchersGivenStack, random);
        for (const x of drawFrom(team?.catchers ?? [], c, used)) seat(x);
        if (random() < FIELD_STACKING.classic.rbGivenStack) for (const x of drawFrom(team?.rbs ?? [], 1, used)) seat(x);
        const b = drawCategory(FIELD_STACKING.classic.bringBackGivenStack, random);
        for (const x of drawFrom([...(opp?.catchers ?? []), ...(opp?.rbs ?? [])], b, used)) seat(x);
      }
    } else if (showdownTeams.length === 2) {
      const majority = splitShares[drawCategory(splitShares.map(([, v]) => v), random)][0];
      const major = showdownTeams[pickWeighted(showdownTeams.map(t => teamMass.get(t) ?? 0), random)];
      quota = new Map(showdownTeams.map(t => [t, t === major ? majority : slots.length - majority]));
    }
    if (seatFailed) continue;
    let broke = false;
    for (let i = 0; i < slots.length; i++) {
      if (filled[i]) continue;
      const open = slots.map((_, j) => j).filter(j => j > i && !filled[j]);
      const reserve = open.reduce((s, j) => s + expectedBySlot[j], 0), cheapest = open.reduce((s, j) => s + cheapestBySlot[j], 0);
      const allowed = (x: PoolEntry) => !used.has(x.p.dkPlayerId)
        && (!quota || (teamCount.get(x.p.teamAbbrev) ?? 0) < (quota.get(x.p.teamAbbrev) ?? 0));
      // Prefer the expected reservation; if it leaves no one, fall back to the hard (cheapest) one.
      let pool = pools[i].filter(x => allowed(x) && cost(x) <= DK_SALARY_CAP - spent - reserve);
      if (!pool.length) pool = pools[i].filter(x => allowed(x) && cost(x) <= DK_SALARY_CAP - spent - cheapest);
      const pick = pickWeighted(pool.map(weightOf), random);
      if (pick < 0) { broke = true; break; }
      const x = pool[pick];
      used.add(x.p.dkPlayerId);
      spent += cost(x);
      teamCount.set(x.p.teamAbbrev, (teamCount.get(x.p.teamAbbrev) ?? 0) + 1);
      filled[i] = { slot: slots[i], playerId: x.p.dkPlayerId };
    }
    if (broke || filled.some(f => f === null)) continue;
    const lineup = filled.map(f => f!) as NflLineup;
    if (lineup.some(e => !byId.has(e.playerId))) continue;
    try { validateNflLineup(slate, lineup); } catch { continue; }
    lineups.push(lineup);
  }
  return { lineups, attempts };
}

/**
 * Opponent lineups from the ownership prior. Each slot draws a player with
 * probability proportional to his ownership for that slot (captain share for
 * CPT, flex share for FLEX, total otherwise), without replacement; a lineup
 * that breaks the roster rules or the cap is thrown away, never repaired.
 *
 * Slot-wise sampling alone does not reproduce the prior: a 53%-owned receiver
 * came out at 20% on the week-5 Classic, because rejection and without-
 * replacement draws compress the popular names. So the weights are calibrated
 * first (iterative proportional fitting against the prior's own ownership on
 * pilot samples), and the achieved deviation is returned with the field. The
 * target is the prior, never a contest outcome.
 */
export function sampleField(slate: NflDkSlate, players: readonly SnapshotPlayer[], seed: number, count = PORTFOLIO_OBJECTIVE.fieldLineups): FieldSample {
  const random = nflRandom(seed);
  const own = new Map(players.map(p => [p.dkPlayerId as number, p.ownership ?? null]));
  const share = (id: number, slot: NflSlot): number => {
    const o = own.get(id);
    if (!o) return 0;
    const v = slot === "CPT" ? o.captain : slot === "FLEX" && slate.format === "showdown" ? o.flex : o.total;
    return typeof v === "number" && v > 0 ? v : 0;
  };
  const slots = nflSlots(slate.format);
  const pools = slots.map(slot => slate.players.filter(p => {
    if (p.isOut) return false;
    if (slate.format === "showdown") return slot === "CPT" ? p.captain !== null : p.rosterPositions.includes("FLEX");
    return p.rosterPositions.includes(slot) && (slot === "FLEX" ? ["RB", "WR", "TE"].includes(p.position) : p.position === slot);
  }).map(p => ({ p, w: share(p.dkPlayerId, slot), cpt: slot === "CPT" })).filter(x => x.w > 0));
  if (pools.some(pool => pool.length === 0)) throw new Error("Field model: a roster slot has no owned, eligible player");
  const mult: Multipliers = new Map(slate.players.map(p => [p.dkPlayerId, { base: 1, captain: 1 }]));
  // Targets: the prior's slot ownership, as a percent of lineups.
  const target = (id: number, cpt: boolean) => cpt ? share(id, "CPT") : slate.format === "showdown" ? share(id, "FLEX") : share(id, "QB");
  const { rounds, pilot, step, floor, cap } = PORTFOLIO_OBJECTIVE.fieldCalibration;
  const realized = (lineups: NflLineup[]) => {
    const c = new Map<string, number>();
    for (const l of lineups) for (const e of l) { const k = `${e.playerId}:${e.slot === "CPT" ? 1 : 0}`; c.set(k, (c.get(k) ?? 0) + 1); }
    return (id: number, cpt: boolean) => (100 * (c.get(`${id}:${cpt ? 1 : 0}`) ?? 0)) / lineups.length;
  };
  const budget = PORTFOLIO_OBJECTIVE.maxFieldAttempts;
  let spent = 0;
  for (let r = 0; r < rounds; r++) {
    const sample = drawLineups(slate, pools, mult, random, pilot, pilot * 50);
    spent += sample.attempts;
    if (sample.lineups.length < pilot) throw new Error(`Field model: calibration round ${r} drew ${sample.lineups.length} of ${pilot} lineups`);
    const got = realized(sample.lineups);
    for (const p of slate.players) {
      const m = mult.get(p.dkPlayerId)!;
      for (const cpt of [false, true]) {
        const want = target(p.dkPlayerId, cpt);
        if (want <= 0) continue;
        const have = Math.max(got(p.dkPlayerId, cpt), 0.25);
        const next = Math.min(cap, Math.max(floor, (cpt ? m.captain : m.base) * Math.pow(want / have, step)));
        if (cpt) m.captain = next; else m.base = next;
      }
    }
  }
  const final = drawLineups(slate, pools, mult, random, count, budget);
  if (final.lineups.length < count) throw new Error(`Field model: only ${final.lineups.length} of ${count} opponent lineups`);
  const got = realized(final.lineups);
  const checked = slate.players.flatMap(p => [false, true].map(cpt => ({ id: p.dkPlayerId, cpt, want: target(p.dkPlayerId, cpt) }))).filter(x => x.want >= 5);
  const deviations = checked.map(x => Math.abs(got(x.id, x.cpt) - x.want));
  const totals = new Map<number, number>();
  for (const l of final.lineups) for (const e of l) totals.set(e.playerId, (totals.get(e.playerId) ?? 0) + 1);
  return { version: FIELD_MODEL_VERSION, seed, lineups: final.lineups, attempts: spent + final.attempts,
    realizedOwnPct: new Map([...totals].map(([id, c]) => [id, Math.round((10000 * c) / count) / 100])),
    shapes: fieldShapeStats(slate, final.lineups),
    calibration: { rounds, players: checked.length,
      maxAbsDeviationPct: Math.round(100 * Math.max(0, ...deviations)) / 100,
      meanAbsDeviationPct: Math.round((100 * deviations.reduce((a, b) => a + b, 0)) / Math.max(1, deviations.length)) / 100 } };
}

/** Per-draw field thresholds: the score at the given top fraction of the sampled field. */
export function fieldThresholds(field: FieldSample, bank: JointBank, fraction: number): Float64Array {
  const scored = field.lineups.map(l => scoreLineup(l, bank));
  const k = Math.max(1, Math.ceil(fraction * field.lineups.length));
  const out = new Float64Array(bank.draws);
  const column = new Float64Array(scored.length);
  for (let d = 0; d < bank.draws; d++) {
    for (let i = 0; i < scored.length; i++) column[i] = scored[i][d];
    column.sort().reverse();
    out[d] = column[k - 1];
  }
  return out;
}

export function fieldSummary(field: FieldSample, bank: JointBank) {
  const top1 = fieldThresholds(field, bank, PORTFOLIO_OBJECTIVE.successFraction);
  const top01 = fieldThresholds(field, bank, PORTFOLIO_OBJECTIVE.secondaryFraction);
  const best = fieldThresholds(field, bank, 0);
  const median = fieldThresholds(field, bank, 0.5);
  const q = (xs: Float64Array, p: number) => { const s = Float64Array.from(xs).sort(); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
  const band = (xs: Float64Array) => ({ p5: q(xs, 0.05), p25: q(xs, 0.25), p50: q(xs, 0.5), p75: q(xs, 0.75), p95: q(xs, 0.95), mean: xs.reduce((a, b) => a + b, 0) / xs.length });
  return { top1Pct: band(top1), top01Pct: band(top01), sampledBest: band(best), median: band(median) };
}

export type SelectionConstraints = {
  count: number;
  /** Max share of lineups any player may appear in (the source build's maxExposure). */
  maxExposure: number;
  maxExposureByPlayer: Record<string, number>;
  /** Pairwise: two lineups must differ in at least this many players (the source build's minUnique). */
  minUnique: number;
};

export type SelectionResult = {
  version: typeof PORTFOLIO_OBJECTIVE_VERSION;
  selected: Array<{ key: string; index: number; marginalGain: number }>;
  /** P(any selected lineup >= within-draw field top-1%) on the selection draws (in-sample). */
  selectionSuccess: number;
  /** Candidates that could not be added under the constraints when the search stopped short. */
  shortfall: number;
};

const covered = (rows: Float64Array[], threshold: Float64Array) => {
  let hits = 0;
  for (let d = 0; d < threshold.length; d++) if (rows.some(r => r[d] >= threshold[d])) hits++;
  return hits / threshold.length;
};

/**
 * Greedy marginal coverage: add the candidate that most raises P(any lineup
 * reaches the field's top-1%) until `count`, respecting the exposure cap,
 * per-player caps and pairwise uniqueness. Ties break on the lower candidate
 * index so the result is reproducible.
 */
export function selectPortfolio(candidates: Array<{ key: string; lineup: NflLineup; draws: Float64Array }>, threshold: Float64Array,
  constraints: SelectionConstraints): SelectionResult {
  const chosen: number[] = [];
  const exposure = new Map<number, number>();
  const slotsPer = candidates[0]?.lineup.length ?? 0;
  const capFor = (id: number) => Math.min(constraints.maxExposure, constraints.maxExposureByPlayer[String(id)] ?? 1);
  const allowed = (i: number) => {
    const ids = candidates[i].lineup.map(e => e.playerId);
    for (const id of ids) if ((exposure.get(id) ?? 0) + 1 > Math.floor(capFor(id) * constraints.count + 1e-9)) return false;
    for (const j of chosen) {
      const other = new Set(candidates[j].lineup.map(e => e.playerId));
      const shared = ids.filter(id => other.has(id)).length;
      if (slotsPer - shared < constraints.minUnique) return false;
    }
    return true;
  };
  const selected: SelectionResult["selected"] = [];
  let current = 0;
  while (chosen.length < constraints.count) {
    let best: { i: number; gain: number; success: number } | null = null;
    for (let i = 0; i < candidates.length; i++) {
      if (chosen.includes(i) || !allowed(i)) continue;
      const success = covered([...chosen.map(j => candidates[j].draws), candidates[i].draws], threshold);
      const gain = success - current;
      if (!best || gain > best.gain) best = { i, gain, success };
    }
    if (!best) break;
    chosen.push(best.i);
    for (const e of candidates[best.i].lineup) exposure.set(e.playerId, (exposure.get(e.playerId) ?? 0) + 1);
    selected.push({ key: candidates[best.i].key, index: best.i, marginalGain: best.gain });
    current = best.success;
  }
  return { version: PORTFOLIO_OBJECTIVE_VERSION, selected, selectionSuccess: current, shortfall: constraints.count - chosen.length };
}

/** The forecast recorded for grading: P(any lineup >= field top-1%) on draws the search never saw. */
export function forecastSuccess(lineups: NflLineup[], bank: JointBank, threshold: Float64Array): number {
  return covered(lineups.map(l => scoreLineup(l, bank)), threshold);
}
