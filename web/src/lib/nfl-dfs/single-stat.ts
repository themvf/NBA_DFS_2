/**
 * DraftKings NFL Single Stat (`nfl-single-stat-v1`, 2026-10-29).
 *
 * Two DraftKings game types score one statistic and sell no salaries:
 * "Single Stat - Touchdowns" (most touchdowns) and "Single Stat - Total
 * Yards" (most combined rushing and receiving yards). Draft 3 players, RB/WR/
 * TE, no cap, no team or game rule. They run as multi-game Sunday slates and
 * as single-game slates (Thursday night); the format is the same.
 *
 * Without a cap there is nothing to optimise against: every entrant can take
 * the three best players. So this module does three things and no more:
 *
 *  1. **Project the stat** from what the workspace already carries. The books'
 *     calibrated line when the market anchor applied (rushing and receiving
 *     yards, or the anytime-TD rate), else the model's own stat means, scaled
 *     by the absence commit when one applied. The stat is read, never
 *     re-derived from DK points.
 *  2. **A stated ownership prior**: with no prices the field drafts on DK's
 *     printed average and on the names it knows. Ownership is proportional to
 *     the cube of the blended average, a 300% budget over three slots, 60%
 *     cap. A prior, uncalibrated; the optimizer reads it only through the GPP
 *     leverage opt-in and the page says so.
 *  3. **A 3-pick optimizer**: cash takes the top projected three; GPP ranks
 *     every three-player combination of the top candidates by
 *     `projected × (1 − own)^k` with max exposure, min unique, locks and
 *     excludes, deterministic under a seed. Exhaustive over the candidate
 *     set, so the result is the best feasible lineup rather than a search.
 *
 * Nothing here is validated against results: single-stat contests have no
 * results import yet, so every constant below is a stated prior.
 */

import { SINGLE_STAT_ROSTER_SIZE, SINGLE_STAT_LABELS, type NflSingleStat } from "./dk-salary-csv";
import { nflRandom } from "./random";

export const SINGLE_STAT_VERSION = "nfl-single-stat-v1";
export const SINGLE_STAT_OWNERSHIP_VERSION = "nfl-single-stat-ownership-prior-v1";

// ── Stat projection ──────────────────────────────────────────────────────

/** Where the projected stat came from, in the order the workspace trusts them. */
export type SingleStatSource = "market" | "model" | "dk_avg" | "none";

export interface SingleStatProjection {
  stat: NflSingleStat;
  /** The number the optimizer builds from. Null when nothing supports one. */
  projected: number | null;
  source: SingleStatSource;
  /** The model's own stat (after the absence commit's scale), for the chip. */
  model: number | null;
  /** The books' calibrated stat when the anchor applied. */
  market: number | null;
  /** DraftKings' printed per-game average of this stat. */
  dkAvg: number | null;
  /**
   * A rough range: the model's stat scaled by its own P10/P90-to-mean ratio
   * for DK points. Labelled an estimate on the page; the lineup floor and
   * ceiling sum these.
   */
  p10: number | null;
  p90: number | null;
  note: string | null;
}

export interface SingleStatPlayerInput {
  position: string;
  isOut: boolean;
  projectionStatus: string;
  statMeans?: Record<string, number> | null;
  ourProj: number | null;
  floorFpts: number | null;
  ceilingFpts: number | null;
  avgFptsDk: number | null;
  marketAnchor?: { statLine: Record<string, number> } | null;
  absenceCommit?: { scale?: number } | null;
}

const num = (value: unknown): number => (typeof value === "number" && Number.isFinite(value) ? value : 0);

/** Rushing + receiving yards, or rushing + receiving touchdowns, from a stat line. */
export function statFromLine(stat: NflSingleStat, line: Record<string, number> | null | undefined): number | null {
  if (!line) return null;
  if (stat === "yards") {
    if (line.rushing_yards == null && line.receiving_yards == null) return null;
    return num(line.rushing_yards) + num(line.receiving_yards);
  }
  // The market anchor carries one `scrimmage_tds` figure; the model splits it.
  if (line.scrimmage_tds != null) return num(line.scrimmage_tds);
  if (line.rushing_tds == null && line.receiving_tds == null) return null;
  return num(line.rushing_tds) + num(line.receiving_tds);
}

export function projectSingleStat(player: SingleStatPlayerInput, stat: NflSingleStat): SingleStatProjection {
  const dkAvg = player.avgFptsDk != null && Number.isFinite(player.avgFptsDk) ? player.avgFptsDk : null;
  const empty = (note: string | null): SingleStatProjection =>
    ({ stat, projected: null, source: "none", model: null, market: null, dkAvg, p10: null, p90: null, note });
  if (player.isOut || player.projectionStatus === "out") return { ...empty("Ruled out."), projected: 0, source: "model", model: 0 };
  const market = statFromLine(stat, player.marketAnchor?.statLine ?? null);
  const scale = player.absenceCommit?.scale != null && Number.isFinite(player.absenceCommit.scale) ? player.absenceCommit.scale : 1;
  // The stat line is read only when the workspace stands behind the points
  // projection it belongs to. A position-prior or unsupported row (no games
  // of his own) still carries the POSITION's mean stat line, and a $0 pool
  // would roster a practice-squad back at 45 yards off it.
  const supported = player.ourProj != null && Number.isFinite(player.ourProj);
  const modelRaw = supported ? statFromLine(stat, player.statMeans ?? null) : null;
  const model = modelRaw == null ? null : modelRaw * scale;
  const projected = market ?? model;
  const source: SingleStatSource = market != null ? "market" : model != null ? "model" : "none";
  // Range: the model's own DK-points shape applied to the stat.
  const ratio = (quantile: number | null) =>
    projected != null && quantile != null && player.ourProj != null && player.ourProj > 0 ? Math.max(0, projected * (quantile / player.ourProj)) : null;
  const note = source === "market" ? "Projected from the sportsbooks' lines (calibrated)."
    : source === "model" ? (scale !== 1 ? `Model stat means, scaled ×${scale.toFixed(2)} for a starter out.` : "Model stat means.")
    : !supported ? "No projection of his own (position average only) and no prop lines; not rosterable."
    : "No model stat line and no prop lines for this player.";
  return { stat, projected, source, model, market, dkAvg, p10: ratio(player.floorFpts), p90: ratio(player.ceilingFpts), note };
}

// ── Ownership prior ──────────────────────────────────────────────────────

/** Three slots at 100% each. */
export const SINGLE_STAT_OWNERSHIP_BUDGET = 100 * SINGLE_STAT_ROSTER_SIZE;
/** Nobody is in more than this share of the field's lineups. Stated prior. */
export const SINGLE_STAT_MAX_OWN_PCT = 60;
/** How hard the field concentrates on the top names. Stated prior. */
export const SINGLE_STAT_OWNERSHIP_EXPONENT = 3;
/** The field reads DK's printed average more than any projection. Stated prior. */
export const SINGLE_STAT_DK_AVG_WEIGHT = 0.6;

export interface SingleStatOwnershipInput {
  dkPlayerId: number;
  isOut: boolean;
  dkAvg: number | null;
  projected: number | null;
  dkStatus?: string | null;
}

const STATUS_MULTIPLIER: Record<string, number> = { Q: 0.7, D: 0.3 };

function fieldStat(player: SingleStatOwnershipInput): number | null {
  const avg = player.dkAvg != null && Number.isFinite(player.dkAvg) ? Math.max(0, player.dkAvg) : null;
  const proj = player.projected != null && Number.isFinite(player.projected) ? Math.max(0, player.projected) : null;
  if (avg == null && proj == null) return null;
  if (avg == null) return proj;
  if (proj == null) return avg;
  return SINGLE_STAT_DK_AVG_WEIGHT * avg + (1 - SINGLE_STAT_DK_AVG_WEIGHT) * proj;
}

/** Percent ownership per player; sums to the budget unless the pool is too thin to hold it. */
export function projectSingleStatOwnership(players: readonly SingleStatOwnershipInput[]): { version: string; budgetPct: number; unallocatedPct: number; ownPct: Map<number, number> } {
  const scores = new Map<number, number>();
  for (const player of players) {
    if (player.isOut) { scores.set(player.dkPlayerId, 0); continue; }
    const stat = fieldStat(player);
    const status = STATUS_MULTIPLIER[(player.dkStatus ?? "").trim().toUpperCase()] ?? 1;
    scores.set(player.dkPlayerId, stat == null || stat <= 0 ? 0 : Math.pow(stat, SINGLE_STAT_OWNERSHIP_EXPONENT) * status);
  }
  // Water-fill: a capped player keeps the cap and the rest share the remainder.
  const out = new Map<number, number>();
  const open = new Map([...scores].filter(([, s]) => s > 0));
  let remaining = SINGLE_STAT_OWNERSHIP_BUDGET;
  for (let pass = 0; pass < 16 && open.size; pass += 1) {
    const total = [...open.values()].reduce((a, b) => a + b, 0);
    if (total <= 0) break;
    let capped = false;
    for (const [id, s] of open) {
      const share = remaining * (s / total);
      if (share > SINGLE_STAT_MAX_OWN_PCT) { out.set(id, SINGLE_STAT_MAX_OWN_PCT); open.delete(id); remaining -= SINGLE_STAT_MAX_OWN_PCT; capped = true; }
    }
    if (!capped) { for (const [id, s] of open) out.set(id, remaining * (s / total)); remaining = 0; open.clear(); }
  }
  for (const id of scores.keys()) if (!out.has(id)) out.set(id, 0);
  const round = (v: number) => Math.round(v * 100) / 100;
  return { version: SINGLE_STAT_OWNERSHIP_VERSION, budgetPct: SINGLE_STAT_OWNERSHIP_BUDGET, unallocatedPct: round(Math.max(0, remaining)),
    ownPct: new Map([...out].map(([id, v]) => [id, round(v)])) };
}

// ── Optimizer ────────────────────────────────────────────────────────────

export interface SingleStatPlayer {
  dkPlayerId: number;
  name: string;
  position: string;
  team: string;
  opponent: string | null;
  gameKey: string | null;
  isOut: boolean;
  /** The stat this player is projected for; null means he cannot be rostered. */
  projected: number | null;
  p10: number | null;
  p90: number | null;
  /** Percent, 0–100; null is unknown and is never read as low. */
  ownPct: number | null;
  source: SingleStatSource;
}

export interface SingleStatSettings {
  mode: "cash" | "gpp";
  nLineups: number;
  /** 0..1 share of lineups a player may appear in (locks are exempt). */
  maxExposure: number;
  /** 1..3 players every lineup must differ from every other by. */
  minUnique: number;
  lockedPlayerIds: number[];
  excludedPlayerIds: number[];
  /** GPP only: `projected × (1 − own)^k`; 0 disables. Stated prior 0.5. */
  leverageExponent: number;
  /** Whether the (uncalibrated) ownership prior may drive leverage at all. */
  useOwnershipLeverage: boolean;
  /** 0..0.5: per-lineup noise as a share of each projection, so GPP sets differ. */
  randomness: number;
  seed: number;
  /** How many of the best-scoring players the exhaustive search considers. */
  candidatePool?: number;
}

export const DEFAULT_SINGLE_STAT_SETTINGS: SingleStatSettings = {
  mode: "gpp", nLineups: 5, maxExposure: 0.6, minUnique: 1, lockedPlayerIds: [], excludedPlayerIds: [],
  leverageExponent: 0.5, useOwnershipLeverage: true, randomness: 0.08, seed: 1, candidatePool: 40,
};

export interface SingleStatLineup {
  lineupNumber: number;
  players: SingleStatPlayer[];
  projected: number;
  floor: number | null;
  ceiling: number | null;
  /** Sum of the three players' ownership; null when any is unknown. */
  ownershipSum: number | null;
  /** The objective the lineup was chosen on (leverage- and noise-adjusted). */
  score: number;
}

export interface SingleStatResult {
  version: string;
  lineups: SingleStatLineup[];
  warnings: string[];
  /** Why each excluded player was left out. */
  excluded: { dkPlayerId: number; name: string; reason: string }[];
  /** Realized appearances per player. */
  exposure: { dkPlayerId: number; name: string; count: number }[];
  /** Whether leverage actually shaped the objective. */
  leverageApplied: boolean;
}

export function validateSingleStatSettings(settings: SingleStatSettings): void {
  if (settings.mode !== "cash" && settings.mode !== "gpp") throw new Error("Mode must be cash or gpp.");
  if (!Number.isInteger(settings.nLineups) || settings.nLineups < 1 || settings.nLineups > 150) throw new Error("Lineup count must be 1–150.");
  if (!(settings.maxExposure > 0 && settings.maxExposure <= 1)) throw new Error("Max exposure must be between 0 and 100%.");
  if (!Number.isInteger(settings.minUnique) || settings.minUnique < 1 || settings.minUnique > SINGLE_STAT_ROSTER_SIZE) throw new Error(`Min unique must be 1–${SINGLE_STAT_ROSTER_SIZE}.`);
  if (!(settings.leverageExponent >= 0 && settings.leverageExponent <= 3)) throw new Error("Leverage exponent must be 0–3.");
  if (!(settings.randomness >= 0 && settings.randomness <= 0.5)) throw new Error("Randomness must be 0–0.5.");
  if (!Number.isInteger(settings.seed) || settings.seed < 0) throw new Error("Seed must be a non-negative integer.");
  if (settings.lockedPlayerIds.length > SINGLE_STAT_ROSTER_SIZE) throw new Error(`At most ${SINGLE_STAT_ROSTER_SIZE} players can be locked.`);
  const both = settings.lockedPlayerIds.filter((id) => settings.excludedPlayerIds.includes(id));
  if (both.length) throw new Error("A player cannot be both locked and excluded.");
}

/** Box-Muller normal draw from a uniform generator. */
function normal(random: () => number): number {
  const u = Math.max(random(), 1e-12), v = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

export function optimizeSingleStat(players: readonly SingleStatPlayer[], settings: SingleStatSettings, stat: NflSingleStat): SingleStatResult {
  validateSingleStatSettings(settings);
  const warnings: string[] = [];
  const excluded: SingleStatResult["excluded"] = [];
  const byId = new Map(players.map((p) => [p.dkPlayerId, p]));
  for (const id of settings.lockedPlayerIds) {
    const p = byId.get(id);
    if (!p) throw new Error(`Locked player ${id} is not in this pool.`);
    if (p.isOut) throw new Error(`${p.name} is locked but ruled out.`);
    if (p.projected == null) throw new Error(`${p.name} is locked but has no ${SINGLE_STAT_LABELS[stat].toLowerCase()} projection.`);
  }
  const pool = players.filter((p) => {
    if (settings.excludedPlayerIds.includes(p.dkPlayerId)) { excluded.push({ dkPlayerId: p.dkPlayerId, name: p.name, reason: "Manually excluded." }); return false; }
    if (p.isOut) { excluded.push({ dkPlayerId: p.dkPlayerId, name: p.name, reason: "Ruled out." }); return false; }
    if (p.projected == null) { excluded.push({ dkPlayerId: p.dkPlayerId, name: p.name, reason: `No ${SINGLE_STAT_LABELS[stat].toLowerCase()} projection.` }); return false; }
    return true;
  });
  if (pool.length < SINGLE_STAT_ROSTER_SIZE) {
    return { version: SINGLE_STAT_VERSION, lineups: [], warnings: [`Only ${pool.length} rosterable players; ${SINGLE_STAT_ROSTER_SIZE} are needed.`], excluded, exposure: [], leverageApplied: false };
  }
  const leverage = settings.mode === "gpp" && settings.useOwnershipLeverage && settings.leverageExponent > 0;
  const leverageApplied = leverage && pool.some((p) => p.ownPct != null);
  if (settings.mode === "gpp" && settings.useOwnershipLeverage && !leverageApplied) warnings.push("No ownership estimate on this pool, so leverage did not apply; lineups are ranked on projection alone.");
  const base = (p: SingleStatPlayer) => {
    const projected = p.projected ?? 0;
    if (!leverageApplied || p.ownPct == null) return projected;
    return projected * Math.pow(1 - Math.min(Math.max(p.ownPct, 0), 99) / 100, settings.leverageExponent);
  };
  const locked = new Set(settings.lockedPlayerIds);
  // Candidate set: the best base scores plus every lock. Exhaustive over it.
  const ranked = [...pool].sort((a, b) => base(b) - base(a) || a.dkPlayerId - b.dkPlayerId);
  const limit = Math.max(SINGLE_STAT_ROSTER_SIZE, settings.candidatePool ?? 40);
  const candidates = [...ranked.filter((p) => locked.has(p.dkPlayerId)), ...ranked.filter((p) => !locked.has(p.dkPlayerId))].slice(0, Math.max(limit, locked.size));
  const maxCount = Math.max(1, Math.floor(settings.maxExposure * settings.nLineups + 1e-9));
  const random = nflRandom(settings.seed);
  const counts = new Map<number, number>();
  const lineups: SingleStatLineup[] = [];
  const chosenSets: Set<number>[] = [];
  const randomness = settings.mode === "cash" ? 0 : settings.randomness;
  for (let n = 0; n < settings.nLineups; n += 1) {
    // Per-lineup noise, drawn in a fixed order so a seed reproduces the run.
    const noise = new Map(candidates.map((p) => [p.dkPlayerId, randomness > 0 ? 1 + randomness * normal(random) : 1]));
    const score = (p: SingleStatPlayer) => base(p) * Math.max(0.05, noise.get(p.dkPlayerId) ?? 1);
    const usable = candidates.filter((p) => locked.has(p.dkPlayerId) || (counts.get(p.dkPlayerId) ?? 0) < maxCount);
    let best: { players: SingleStatPlayer[]; score: number } | null = null;
    const k = SINGLE_STAT_ROSTER_SIZE;
    const pick: SingleStatPlayer[] = [];
    const visit = (start: number) => {
      if (pick.length === k) {
        if (!settings.lockedPlayerIds.every((id) => pick.some((p) => p.dkPlayerId === id))) return;
        const ids = new Set(pick.map((p) => p.dkPlayerId));
        for (const prior of chosenSets) {
          let differ = 0;
          for (const id of ids) if (!prior.has(id)) differ += 1;
          if (differ < settings.minUnique) return;
        }
        const total = pick.reduce((sum, p) => sum + score(p), 0);
        if (!best || total > best.score + 1e-9) best = { players: [...pick], score: total };
        return;
      }
      for (let i = start; i < usable.length; i += 1) {
        pick.push(usable[i]);
        visit(i + 1);
        pick.pop();
      }
    };
    visit(0);
    if (!best) {
      warnings.push(`Stopped at ${lineups.length} of ${settings.nLineups} lineups: no further lineup satisfies the exposure cap (${Math.round(settings.maxExposure * 100)}%), min unique ${settings.minUnique} and locks.`);
      break;
    }
    const chosen = best as { players: SingleStatPlayer[]; score: number };
    const sorted = [...chosen.players].sort((a, b) => (b.projected ?? 0) - (a.projected ?? 0) || a.dkPlayerId - b.dkPlayerId);
    for (const p of sorted) counts.set(p.dkPlayerId, (counts.get(p.dkPlayerId) ?? 0) + 1);
    chosenSets.push(new Set(sorted.map((p) => p.dkPlayerId)));
    const sum = (field: "p10" | "p90") => sorted.every((p) => p[field] != null) ? sorted.reduce((s, p) => s + (p[field] as number), 0) : null;
    lineups.push({
      lineupNumber: lineups.length + 1, players: sorted,
      projected: sorted.reduce((s, p) => s + (p.projected ?? 0), 0),
      floor: sum("p10"), ceiling: sum("p90"),
      ownershipSum: sorted.every((p) => p.ownPct != null) ? sorted.reduce((s, p) => s + (p.ownPct as number), 0) : null,
      score: chosen.score,
    });
  }
  const exposure = [...counts].map(([dkPlayerId, count]) => ({ dkPlayerId, name: byId.get(dkPlayerId)?.name ?? String(dkPlayerId), count }))
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  return { version: SINGLE_STAT_VERSION, lineups, warnings, excluded, exposure, leverageApplied };
}
