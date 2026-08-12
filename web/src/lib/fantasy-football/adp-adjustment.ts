/**
 * Roster-Aware ADP Adjustment System
 * 
 * Adjusts Average Draft Position (ADP) based on league roster construction.
 * More FLEX spots, deeper benches, and additional starters all impact player value.
 * 
 * Base ADP comes from Fantasy Football Calculator (12-team standard leagues).
 * This module adjusts that baseline to match your specific roster format.
 */

import type { RosterConfig } from "./league-config";

// Standard 12-team roster used as ADP baseline
const BASELINE_ROSTER: RosterConfig = {
  QB: 1,
  RB: 2,
  WR: 2,
  TE: 1,
  FLEX: 1,
  K: 1,
  DST: 1,
  BN: 6,
};

const BASELINE_LEAGUE_SIZE = 12;

/**
 * FLEX position fill rates based on historical draft data
 * TEs rarely fill FLEX in practice despite eligibility
 */
const FLEX_FILL_RATES = {
  QB: 0,
  RB: 0.45,
  WR: 0.45,
  TE: 0.10,
  K: 0,
  DST: 0,
};

/**
 * Tier sensitivity to roster construction changes
 * Mid-rounds (3-10) are most affected by roster differences
 * Early picks (elite players) and late picks (dart throws) less sensitive
 */
function getTierSensitivity(tier: number): number {
  if (tier <= 2) return 0.3; // Rounds 1-2: Elite players, format-agnostic
  if (tier <= 5) return 0.7; // Rounds 3-5: High sensitivity
  if (tier <= 10) return 1.0; // Rounds 6-10: Maximum sensitivity
  if (tier <= 15) return 0.6; // Rounds 11-15: Moderate sensitivity
  return 0.3; // Rounds 16+: Low sensitivity (dart throws)
}

/**
 * Calculate position demand in a given roster format
 * 
 * Demand = Direct starters + FLEX eligibility + Bench depth factor
 */
function calculatePositionDemand(
  position: string,
  roster: RosterConfig,
  leagueSize: number = BASELINE_LEAGUE_SIZE
): number {
  let demand = 0;

  // Direct starting spots
  const directStarters = roster[position as keyof RosterConfig] || 0;
  demand += directStarters;

  // FLEX eligibility (RB/WR/TE can fill FLEX)
  const flexFillRate = FLEX_FILL_RATES[position as keyof typeof FLEX_FILL_RATES] || 0;
  demand += roster.FLEX * flexFillRate;

  // Bench depth factor: more bench spots = more demand for depth
  // Each bench spot adds ~8% demand (diminishing returns)
  const benchFactor = 1 + (roster.BN * 0.08);
  demand *= benchFactor;

  // League size factor: 10-team leagues have 17% less demand than 12-team
  const leagueSizeFactor = leagueSize / BASELINE_LEAGUE_SIZE;
  demand *= leagueSizeFactor;

  return demand;
}

/**
 * Calculate position scarcity adjustment
 * 
 * When a position becomes more scarce relative to demand,
 * players at that position gain value
 */
function calculateScarcityFactor(
  position: string,
  baseRoster: RosterConfig,
  targetRoster: RosterConfig,
  baseLeagueSize: number,
  targetLeagueSize: number
): number {
  const baseDemand = calculatePositionDemand(position, baseRoster, baseLeagueSize);
  const targetDemand = calculatePositionDemand(position, targetRoster, targetLeagueSize);

  // Demand ratio: >1 means position is MORE valuable in target format
  const demandRatio = targetDemand / baseDemand;

  // Convert to scarcity factor (inverted for ADP: higher demand = lower ADP number)
  // Use logarithmic scaling to avoid extreme adjustments
  const scarcityFactor = 1 / Math.pow(demandRatio, 0.4);

  return scarcityFactor;
}

/**
 * Adjust ADP based on roster construction differences
 * 
 * @param baseAdp - Original ADP from Fantasy Football Calculator (12-team standard)
 * @param position - Player position (QB, RB, WR, TE, K, DST)
 * @param targetRoster - Your league's roster configuration
 * @param targetLeagueSize - Your league size (default: 12)
 * @returns Adjusted ADP for your specific roster format
 */
export function adjustAdpForRoster(
  baseAdp: number | null,
  position: string,
  targetRoster: RosterConfig,
  targetLeagueSize: number = BASELINE_LEAGUE_SIZE
): number | null {
  if (baseAdp === null || baseAdp <= 0) return null;

  // K and DST aren't sensitive to roster construction (always late picks)
  if (position === "K" || position === "DST") return baseAdp;

  // Calculate scarcity adjustment
  const scarcityFactor = calculateScarcityFactor(
    position,
    BASELINE_ROSTER,
    targetRoster,
    BASELINE_LEAGUE_SIZE,
    targetLeagueSize
  );

  // Determine tier based on base ADP (assuming ~12 picks per round)
  const tier = Math.ceil(baseAdp / 12);
  const sensitivity = getTierSensitivity(tier);

  // Apply adjustment with tier sensitivity
  // scarcityFactor < 1 → player moves UP (lower ADP number)
  // scarcityFactor > 1 → player moves DOWN (higher ADP number)
  const adjustmentFactor = 1 + (scarcityFactor - 1) * sensitivity;
  const adjustedAdp = baseAdp * adjustmentFactor;

  // Sanity bounds: adjustments shouldn't exceed ±25 spots
  const maxDelta = 25;
  const boundedAdp = Math.max(
    baseAdp - maxDelta,
    Math.min(baseAdp + maxDelta, adjustedAdp)
  );

  return Math.round(boundedAdp * 10) / 10; // Round to 1 decimal
}

/**
 * Calculate ADP delta for display
 * 
 * @returns Object with adjusted ADP and delta from base
 */
export function calculateAdpAdjustment(
  baseAdp: number | null,
  position: string,
  targetRoster: RosterConfig,
  targetLeagueSize: number = BASELINE_LEAGUE_SIZE
): {
  baseAdp: number | null;
  adjustedAdp: number | null;
  delta: number | null;
  direction: "up" | "down" | "neutral";
} {
  const adjustedAdp = adjustAdpForRoster(baseAdp, position, targetRoster, targetLeagueSize);

  if (baseAdp === null || adjustedAdp === null) {
    return { baseAdp, adjustedAdp, delta: null, direction: "neutral" };
  }

  const delta = adjustedAdp - baseAdp;
  const direction = delta < -0.5 ? "up" : delta > 0.5 ? "down" : "neutral";

  return {
    baseAdp,
    adjustedAdp,
    delta: Math.round(delta * 10) / 10,
    direction,
  };
}

/**
 * Get human-readable description of ADP adjustment
 */
export function getAdjustmentDescription(
  targetRoster: RosterConfig,
  targetLeagueSize: number = BASELINE_LEAGUE_SIZE
): string {
  const parts: string[] = [];

  // FLEX differences
  const flexDiff = targetRoster.FLEX - BASELINE_ROSTER.FLEX;
  if (flexDiff > 0) {
    parts.push(`+${flexDiff} FLEX`);
  } else if (flexDiff < 0) {
    parts.push(`${flexDiff} FLEX`);
  }

  // WR differences
  const wrDiff = targetRoster.WR - BASELINE_ROSTER.WR;
  if (wrDiff !== 0) {
    parts.push(`${wrDiff > 0 ? "+" : ""}${wrDiff} WR`);
  }

  // RB differences
  const rbDiff = targetRoster.RB - BASELINE_ROSTER.RB;
  if (rbDiff !== 0) {
    parts.push(`${rbDiff > 0 ? "+" : ""}${rbDiff} RB`);
  }

  // Bench differences
  const bnDiff = targetRoster.BN - BASELINE_ROSTER.BN;
  if (bnDiff !== 0) {
    parts.push(`${bnDiff > 0 ? "+" : ""}${bnDiff} BN`);
  }

  // League size
  if (targetLeagueSize !== BASELINE_LEAGUE_SIZE) {
    parts.push(`${targetLeagueSize}-team`);
  }

  if (parts.length === 0) {
    return "12-team standard format";
  }

  return `Adjusted for: ${parts.join(", ")}`;
}

/**
 * Check if roster is significantly different from baseline
 * (to decide whether to show adjustment in UI)
 */
export function isRosterDifferentFromBaseline(
  roster: RosterConfig,
  leagueSize: number = BASELINE_LEAGUE_SIZE
): boolean {
  // Check league size
  if (leagueSize !== BASELINE_LEAGUE_SIZE) return true;

  // Check key positions
  if (roster.FLEX !== BASELINE_ROSTER.FLEX) return true;
  if (roster.RB !== BASELINE_ROSTER.RB) return true;
  if (roster.WR !== BASELINE_ROSTER.WR) return true;
  if (Math.abs(roster.BN - BASELINE_ROSTER.BN) >= 2) return true;

  return false;
}
