/**
 * NFL Fantasy Football League Configurations
 * 
 * Defines preset league settings including roster structure, scoring rules,
 * and league format options. These presets can be selected when creating a draft.
 */

// ============================================================================
// ROSTER CONFIGURATIONS
// ============================================================================

export type RosterPosition = "QB" | "RB" | "WR" | "TE" | "FLEX" | "K" | "DST" | "BN" | "IR";

export interface RosterConfig {
  QB: number;
  RB: number;
  WR: number;
  TE: number;
  FLEX: number;  // W/R/T flex slot
  K: number;
  DST: number;
  BN: number;    // Bench
  IR?: number;   // Injured Reserve (optional)
}

export const ROSTER_PRESETS = {
  "standard-10team": {
    name: "Standard 10-Team",
    description: "Classic roster: 1 QB, 2 RB, 2 WR, 1 TE, 1 FLEX, 1 K, 1 DST",
    config: { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6 },
  },
  "standard-12team": {
    name: "Standard 12-Team",
    description: "Classic roster: 1 QB, 2 RB, 2 WR, 1 TE, 1 FLEX, 1 K, 1 DST",
    config: { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6 },
  },
  "hood-rivals": {
    name: "Hood Rivals (Yahoo)",
    description: "Hood Rivals league: 1 QB, 2 RB, 2 WR, 1 TE, 2 FLEX, 1 K, 1 DST, 7 BN, 1 IR",
    config: { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 2, K: 1, DST: 1, BN: 7, IR: 1 },
  },
  "deep-bench": {
    name: "Deep Bench",
    description: "Deeper rosters: 1 QB, 2 RB, 3 WR, 1 TE, 2 FLEX, 1 K, 1 DST",
    config: { QB: 1, RB: 2, WR: 3, TE: 1, FLEX: 2, K: 1, DST: 1, BN: 8 },
  },
  "superflex": {
    name: "Superflex",
    description: "Superflex format: 1 QB, 2 RB, 2 WR, 1 TE, 1 FLEX, 1 SUPERFLEX, 1 K, 1 DST",
    config: { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6 },
    // Note: SUPERFLEX would need special handling as it allows QB
  },
} as const;

// ============================================================================
// SCORING CONFIGURATIONS
// ============================================================================

export interface ScoringConfig {
  preset: "STD" | "HALF" | "PPR" | "CUSTOM";
  
  // Passing
  passingYardsPerPoint?: number;
  passingTD?: number;
  passingInterception?: number;
  passing300YardBonus?: number;
  passing2PT?: number;
  
  // Rushing
  rushingYardsPerPoint?: number;
  rushingTD?: number;
  rushing100YardBonus?: number;
  rushing2PT?: number;
  
  // Receiving
  receptions?: number;  // Key difference: 0 (STD), 0.5 (HALF), 1.0 (PPR)
  receivingYardsPerPoint?: number;
  receivingTD?: number;
  receiving100YardBonus?: number;
  receiving2PT?: number;
  
  // Misc offense
  fumblesLost?: number;
  returnTD?: number;
  
  // Kicking
  fgMade0_19?: number;
  fgMade20_29?: number;
  fgMade30_39?: number;
  fgMade40_49?: number;
  fgMade50Plus?: number;
  patMade?: number;
  
  // Defense/Special Teams
  dstSack?: number;
  dstInterception?: number;
  dstFumbleRecovery?: number;
  dstTD?: number;
  dstSafety?: number;
  dstBlockedKick?: number;
  dstPointsAllowed0?: number;
  dstPointsAllowed1_6?: number;
  dstPointsAllowed7_13?: number;
  dstPointsAllowed14_20?: number;
  dstPointsAllowed21_27?: number;
  dstPointsAllowed28_34?: number;
  dstPointsAllowed35Plus?: number;
  dstExtraPointReturned?: number;
}

export const SCORING_PRESETS: Record<string, { name: string; description: string; config: ScoringConfig }> = {
  "STD": {
    name: "Standard",
    description: "No points per reception",
    config: {
      preset: "STD",
      passingYardsPerPoint: 25,
      passingTD: 4,
      passingInterception: -1,
      passing300YardBonus: 0,
      passing2PT: 2,
      rushingYardsPerPoint: 10,
      rushingTD: 6,
      rushing100YardBonus: 0,
      rushing2PT: 2,
      receptions: 0,
      receivingYardsPerPoint: 10,
      receivingTD: 6,
      receiving100YardBonus: 0,
      receiving2PT: 2,
      fumblesLost: -2,
      returnTD: 6,
      fgMade0_19: 3,
      fgMade20_29: 3,
      fgMade30_39: 3,
      fgMade40_49: 4,
      fgMade50Plus: 5,
      patMade: 1,
      dstSack: 1,
      dstInterception: 2,
      dstFumbleRecovery: 2,
      dstTD: 6,
      dstSafety: 2,
      dstBlockedKick: 2,
      dstPointsAllowed0: 10,
      dstPointsAllowed1_6: 7,
      dstPointsAllowed7_13: 4,
      dstPointsAllowed14_20: 1,
      dstPointsAllowed21_27: 0,
      dstPointsAllowed28_34: -1,
      dstPointsAllowed35Plus: -4,
      dstExtraPointReturned: 2,
    },
  },
  "HALF": {
    name: "Half PPR",
    description: "0.5 points per reception",
    config: {
      preset: "HALF",
      passingYardsPerPoint: 25,
      passingTD: 4,
      passingInterception: -1,
      passing300YardBonus: 0,
      passing2PT: 2,
      rushingYardsPerPoint: 10,
      rushingTD: 6,
      rushing100YardBonus: 0,
      rushing2PT: 2,
      receptions: 0.5,
      receivingYardsPerPoint: 10,
      receivingTD: 6,
      receiving100YardBonus: 0,
      receiving2PT: 2,
      fumblesLost: -2,
      returnTD: 6,
      fgMade0_19: 3,
      fgMade20_29: 3,
      fgMade30_39: 3,
      fgMade40_49: 4,
      fgMade50Plus: 5,
      patMade: 1,
      dstSack: 1,
      dstInterception: 2,
      dstFumbleRecovery: 2,
      dstTD: 6,
      dstSafety: 2,
      dstBlockedKick: 2,
      dstPointsAllowed0: 10,
      dstPointsAllowed1_6: 7,
      dstPointsAllowed7_13: 4,
      dstPointsAllowed14_20: 1,
      dstPointsAllowed21_27: 0,
      dstPointsAllowed28_34: -1,
      dstPointsAllowed35Plus: -4,
      dstExtraPointReturned: 2,
    },
  },
  "PPR": {
    name: "Full PPR",
    description: "1.0 point per reception",
    config: {
      preset: "PPR",
      passingYardsPerPoint: 25,
      passingTD: 4,
      passingInterception: -1,
      passing300YardBonus: 0,
      passing2PT: 2,
      rushingYardsPerPoint: 10,
      rushingTD: 6,
      rushing100YardBonus: 0,
      rushing2PT: 2,
      receptions: 1.0,
      receivingYardsPerPoint: 10,
      receivingTD: 6,
      receiving100YardBonus: 0,
      receiving2PT: 2,
      fumblesLost: -2,
      returnTD: 6,
      fgMade0_19: 3,
      fgMade20_29: 3,
      fgMade30_39: 3,
      fgMade40_49: 4,
      fgMade50Plus: 5,
      patMade: 1,
      dstSack: 1,
      dstInterception: 2,
      dstFumbleRecovery: 2,
      dstTD: 6,
      dstSafety: 2,
      dstBlockedKick: 2,
      dstPointsAllowed0: 10,
      dstPointsAllowed1_6: 7,
      dstPointsAllowed7_13: 4,
      dstPointsAllowed14_20: 1,
      dstPointsAllowed21_27: 0,
      dstPointsAllowed28_34: -1,
      dstPointsAllowed35Plus: -4,
      dstExtraPointReturned: 2,
    },
  },
};

// ============================================================================
// LEAGUE FORMAT CONFIGURATIONS
// ============================================================================

export interface LeagueFormatConfig {
  teams: number;
  rounds: number;
  playoffTeams: number;
  playoffWeeks: readonly number[];
  waiverType: "FAB" | "rolling" | "reverse";
  tradeDeadline?: number;  // Week number
}

export const LEAGUE_FORMAT_PRESETS = {
  "standard-10": {
    name: "Standard 10-Team",
    config: {
      teams: 10,
      rounds: 15,
      playoffTeams: 4,
      playoffWeeks: [16, 17],
      waiverType: "FAB" as const,
      tradeDeadline: 13,
    },
  },
  "standard-12": {
    name: "Standard 12-Team",
    config: {
      teams: 12,
      rounds: 15,
      playoffTeams: 4,
      playoffWeeks: [16, 17],
      waiverType: "FAB" as const,
      tradeDeadline: 13,
    },
  },
  "hood-rivals": {
    name: "Hood Rivals (Yahoo)",
    config: {
      teams: 10,
      rounds: 15,
      playoffTeams: 4,
      playoffWeeks: [16, 17],
      waiverType: "FAB" as const,
      tradeDeadline: 13,
    },
  },
} as const;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Get a roster preset by key
 */
export function getRosterPreset(key: string): RosterConfig {
  const preset = ROSTER_PRESETS[key as keyof typeof ROSTER_PRESETS];
  return preset?.config ?? ROSTER_PRESETS["hood-rivals"].config;
}

/**
 * Get a scoring preset by key
 */
export function getScoringPreset(key: string): ScoringConfig {
  const preset = SCORING_PRESETS[key];
  return preset?.config ?? SCORING_PRESETS.HALF.config;
}

/**
 * Get a league format preset by key
 */
export function getLeagueFormatPreset(key: string): LeagueFormatConfig {
  const preset = LEAGUE_FORMAT_PRESETS[key as keyof typeof LEAGUE_FORMAT_PRESETS];
  return preset?.config ?? LEAGUE_FORMAT_PRESETS["hood-rivals"].config;
}

/**
 * Calculate total roster size (excluding IR)
 */
export function calculateRosterSize(roster: RosterConfig): number {
  const { IR, ...activeRoster } = roster;
  return Object.values(activeRoster).reduce((sum, count) => sum + count, 0);
}

/**
 * Get human-readable scoring summary
 */
export function getScoringDescription(config: ScoringConfig): string {
  const reception = config.receptions ?? 0;
  if (reception === 0) return "Standard (No PPR)";
  if (reception === 0.5) return "Half PPR (0.5 per reception)";
  if (reception === 1.0) return "Full PPR (1.0 per reception)";
  return `Custom (${reception} per reception)`;
}

/**
 * Validate roster configuration
 */
export function validateRosterConfig(roster: RosterConfig): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  
  if (roster.QB < 0 || roster.QB > 3) {
    errors.push("QB must be between 0 and 3");
  }
  if (roster.RB < 0 || roster.RB > 4) {
    errors.push("RB must be between 0 and 4");
  }
  if (roster.WR < 0 || roster.WR > 4) {
    errors.push("WR must be between 0 and 4");
  }
  if (roster.TE < 0 || roster.TE > 3) {
    errors.push("TE must be between 0 and 3");
  }
  if (roster.FLEX < 0 || roster.FLEX > 4) {
    errors.push("FLEX must be between 0 and 4");
  }
  if (roster.BN < 0 || roster.BN > 20) {
    errors.push("Bench must be between 0 and 20");
  }
  
  const totalStarters = roster.QB + roster.RB + roster.WR + roster.TE + roster.FLEX + roster.K + roster.DST;
  if (totalStarters < 5 || totalStarters > 15) {
    errors.push("Total starting positions must be between 5 and 15");
  }
  
  return { valid: errors.length === 0, errors };
}
