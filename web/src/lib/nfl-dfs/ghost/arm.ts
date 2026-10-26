/**
 * One ghost arm: frozen inputs + factor levels -> the exact players and
 * optimizer settings that arm builds with (`nfl-ghost-factorial-v1`).
 *
 * The house settings are frozen with the slate (resolved ownership capability,
 * favourite/underdog, lineup count). Every arm starts from them and applies
 * only its levels. The Winners preset is resolved once with
 * `applyWinnersShape` and then decomposed: each of its rules is kept or
 * dropped by its own factor, and the arm is built with `buildShape:
 * 'standard'` so the optimizer does not reapply the preset on top.
 *
 * Ownership is the field model and is held fixed across arms (it was computed
 * once, from the house projections). Changing an arm's projections changes
 * what the optimizer believes, not what the field is assumed to do.
 *
 * Pure: no I/O.
 */
import type { NflOptimizerPlayer, NflOptimizerSettings } from "@/app/dfs/nfl/nfl-optimizer";
import { applyWinnersShape, WINNERS_SHAPE } from "../winners-shape";
import { restorePlayers, type FrozenPlayer } from "../frozen-players";
import { rangeFromRatio } from "../range-split";
import type { ArmLevels, GhostFormat } from "./design";

/** "Off" for the ceiling cap: a multiple no real projection reaches. */
export const CEILING_CAP_OFF = 1000;

type Dist = { mean: number; p10: number | null; median: number | null; p90: number | null; boom?: number | null };

function withDist(p: FrozenPlayer, d: Dist): FrozenPlayer {
  return { ...p, ourProj: d.mean, floor: d.p10, median: d.median, ceiling: d.p90,
    ...(d.boom != null ? { boomRate: d.boom } : {}) };
}

/**
 * The projection an arm uses for one player:
 *   anchored + commit on   the frozen (house) number
 *   anchored + commit off  anchored players unchanged (the line already carries the news);
 *                          others revert to the model's pre-commit number
 *   model + commit on      the model's number before the anchor (commit included)
 *   model + commit off     the model's own number, before both
 */
export function armProjection(p: FrozenPlayer, projections: "anchored" | "model", absenceCommit: boolean): FrozenPlayer {
  const chosen = armProjectionBeforeRanges(p, projections, absenceCommit);
  // Long-memory ranges (nfl-dfs-range-split-v1) are re-applied to whichever
  // projection the arm uses, so every arm carries them and the projection
  // factors stay comparable. A reverted overlay's `before` range predates them.
  const split = p.rangeSplit ?? null;
  if (!split || chosen === p) return chosen;
  const range = rangeFromRatio(chosen.ourProj, chosen.median, split);
  return range ? { ...chosen, floor: range.p10, median: range.median ?? chosen.median, ceiling: range.p90 } : chosen;
}

function armProjectionBeforeRanges(p: FrozenPlayer, projections: "anchored" | "model", absenceCommit: boolean): FrozenPlayer {
  const anchor = p.marketAnchor ?? null;
  const commit = p.absenceCommit ?? p.supersededAbsenceCommit ?? null;
  if (projections === "anchored") {
    if (absenceCommit || anchor || !commit) return p;
    return withDist(p, commit.before);
  }
  const preAnchor = anchor ? withDist(p, anchor.before) : p;
  if (absenceCommit || !commit) return preAnchor;
  return withDist(preAnchor, commit.before);
}

export type ArmBuild = { players: NflOptimizerPlayer[]; settings: NflOptimizerSettings; notes: string[]; ruleExcludedIds: number[] };

export function buildArm(format: GhostFormat, frozen: FrozenPlayer[], house: NflOptimizerSettings, levels: ArmLevels): ArmBuild {
  const projections = levels.projections as "anchored" | "model";
  const players = restorePlayers(frozen.map(p => armProjection(p, projections, levels.absenceCommit === true)));
  const base: NflOptimizerSettings = {
    ...house,
    maxExposure: Number(levels.maxExposure),
    minUnique: Number(levels.minUnique),
    leverageExponent: Number(levels.leverageExponent),
    maxCeilingMultiple: levels.ceilingCap === "off" ? CEILING_CAP_OFF : Number(levels.ceilingCap),
    puntPolicy: { ...house.puntPolicy!, mode: levels.puntPolicy as "no_punts" | "role_qualified" },
  };
  const shape = applyWinnersShape({ ...base, buildShape: "winners" }, players);
  const byId = new Map(players.map(p => [p.dkPlayerId, p]));
  const keepRule = (position: string) => format === "classic"
    && ((["TE", "WR"].includes(position) && levels.teWrRules === true) || (["RB", "DST"].includes(position) && levels.rbDstRules === true));
  const ruleExcludedIds = [...shape.excluded.keys()].filter(id => keepRule(byId.get(id)!.position));
  const s: NflOptimizerSettings = { ...shape.settings, buildShape: "standard",
    // The arm's own levels win over the preset's clamps (which raise min unique to 4 and cap exposure at 55%).
    maxExposure: base.maxExposure, minUnique: base.minUnique,
    excludedPlayerIds: [...new Set([...house.excludedPlayerIds, ...ruleExcludedIds])] };
  if (format === "classic") {
    s.bringBackShare = Number(levels.bringBackShare);
    s.bringBack = true;
    if (levels.qbCap !== true) delete s.maxQbExposure;
    if (levels.dstVsOffenseCap !== true) delete s.maxDstVsOffenseShare;
  } else {
    s.excludeKickers = levels.kickers === "banned";
    s.forbidFourTwoSplit = levels.fourTwo === "banned";
    s.maxCaptainExposure = Number(levels.captainCap);
    if (levels.captainPositions === "any") delete s.captainPositions;
    else s.captainPositions = [...WINNERS_SHAPE.captainPositions];
    s.archetypeMode = levels.plan as NflOptimizerSettings["archetypeMode"];
    s.archetypeQuotas = [];
  }
  return { players, settings: s, notes: shape.notes, ruleExcludedIds };
}
