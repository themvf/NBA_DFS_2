/**
 * The HOUSE configuration of `nfl-ghost-factorial-v1`: "the current
 * framework", frozen as code so the house build never depends on an owner's
 * build. Written 2026-10-16 from the owner's own recent builds (Winners shape,
 * GPP, historical projections, no-punts policy, randomness 0.08, the same GPP
 * signal codes), with three deliberate differences:
 *
 *  - no defensive adjustments: their opponent captures are requested only for
 *    an uploaded slate, so a system slate has none to read;
 *  - no per-slate choices (locks, exclusions, captain ranges, confirmed QBs):
 *    those are the owner's, not the framework's;
 *  - Showdown uses the Standard ceiling plan, the owner's choice for the house
 *    (the balanced plan stalled Winners builds short on week 6).
 *
 * The factor levels (exposure, leverage, minimum unique, ...) are applied on
 * top by `buildArm`, so the house is simply arm 0 at the house levels. Any
 * change here is a new version.
 */
import type { NflOptimizerSettings } from "@/app/dfs/nfl/nfl-optimizer";
import { DEFAULT_NFL_PUNT_POLICY } from "../punt-policy";
import { DEFAULT_SITUATIONS } from "../projection-audit";
import { GHOST_LINEUPS, houseLevels, type GhostFormat } from "./design";

export const GHOST_HOUSE_VERSION = "nfl-ghost-house-v1";

export function houseSettings(format: GhostFormat, context: { favoriteTeam: string | null; underdogTeam: string | null }): NflOptimizerSettings {
  const levels = houseLevels(format);
  return {
    format,
    mode: "gpp",
    projectionSource: "our",
    defensiveAdjustments: { mode: "off", profile: "gpp-integrated" },
    allowDkFallback: false,
    nLineups: GHOST_LINEUPS[format],
    minSalary: 45_000,
    minPlayerSalary: 1_000,
    requireObservedHistory: true,
    puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY },
    puntOverrides: [],
    situations: { ...DEFAULT_SITUATIONS, teams: {} },
    topProjectedCoverage: false,
    gppSignalMinPerLineup: 0,
    gppSignalCodes: ["AIR_VOLUME", "YAC_RUNWAY", "INSIDE_FIVE", "CLOSE_TARGET"],
    gppAirMatchupMinPct: 0,
    gppGoalLineMinPct: 0,
    useHeuristicOwnershipLeverage: true,
    buildShape: "winners",
    archetypeMode: format === "showdown" ? "standard" : undefined,
    archetypeQuotas: [],
    favoriteTeam: context.favoriteTeam,
    underdogTeam: context.underdogTeam,
    maxExposure: Number(levels.maxExposure),
    minUnique: Number(levels.minUnique),
    stackPassCatchers: 1,
    bringBack: true,
    randomness: 0.08,
    lockedPlayerIds: [],
    excludedPlayerIds: [],
    minExposureByPlayer: {},
    maxExposureByPlayer: {},
    exposurePolicies: [],
    confirmedStartingQbs: {},
  } as NflOptimizerSettings;
}
