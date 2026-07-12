export const MLB_GAME_LINES_TRUST = {
  state: "research" as const,
  label: "Research mode",
  description:
    "Moneyline and totals are shadow signals while the odds, feature-history, and immutable backtest repairs are completed.",
};

export const MLB_ACTIONABILITY_POLICY_VERSION = "mlb-actionability-v1";
export const MLB_MIN_PROSPECTIVE_UNIQUE_GAMES = 150;

export type MlbGameLineMarket = "moneyline" | "total";
export type MlbTrustState = "blocked" | "research" | "watch" | "actionable" | "retired";

export type MlbActionabilityEvidence = {
  market: MlbGameLineMarket;
  modelVersion: string | null;
  ledgerRows: number;
  settledUniqueGames: number;
  settledBets: number;
  roi: number | null;
  roiConfidenceLowerBound: number | null;
  clvN: number;
  avgClvPp: number | null;
  exactPriceCoverage: number;
  missingCommence: number;
  postCommenceWrites: number;
  invalidPrices: number;
  duplicateActiveRecommendations: number;
  prospectiveTrackingAvailable: boolean;
  immutableFeatureSnapshotsAvailable: boolean;
};

export type MlbValidationGate = {
  key: string;
  label: string;
  passed: boolean;
  blocking: boolean;
  detail: string;
};

export type MlbActionabilityDecision = {
  market: MlbGameLineMarket;
  policyVersion: string;
  modelVersion: string | null;
  canonicalHorizon: string | null;
  trustEvaluationId: string;
  state: MlbTrustState;
  passed: number;
  total: number;
  gates: MlbValidationGate[];
  summary: string;
};

/**
 * The only policy allowed to promote an MLB game-line market. Evidence comes
 * from the frozen bet ledger; UI components render this result and do not
 * recreate thresholds locally.
 */
export function evaluateMlbActionability(
  evidence: MlbActionabilityEvidence,
): MlbActionabilityDecision {
  const gates: MlbValidationGate[] = [
    {
      key: "commence",
      label: "Every ledger row has a start time",
      passed: evidence.missingCommence === 0,
      blocking: true,
      detail: `${evidence.missingCommence} row(s) missing commence time`,
    },
    {
      key: "pregame",
      label: "No post-first-pitch writes",
      passed: evidence.postCommenceWrites === 0,
      blocking: true,
      detail: `${evidence.postCommenceWrites} post-commence row(s)`,
    },
    {
      key: "prices",
      label: "All prices are valid and executable",
      passed:
        evidence.invalidPrices === 0 &&
        (evidence.ledgerRows === 0 || evidence.exactPriceCoverage === 1),
      blocking: true,
      detail: evidence.ledgerRows === 0
        ? "No prospective rows yet; exact-price coverage applies on first capture"
        : `${evidence.invalidPrices} invalid price(s); ${(evidence.exactPriceCoverage * 100).toFixed(0)}% exact book/price coverage`,
    },
    {
      key: "duplicates",
      label: "One active recommendation per game and market",
      passed: evidence.duplicateActiveRecommendations === 0,
      blocking: true,
      detail: `${evidence.duplicateActiveRecommendations} duplicate active group(s)`,
    },
    {
      key: "features",
      label: "Immutable point-in-time feature snapshot",
      passed: evidence.immutableFeatureSnapshotsAvailable,
      blocking: false,
      detail: evidence.immutableFeatureSnapshotsAvailable
        ? "Feature provenance is frozen"
        : "Feature snapshot provenance is not yet available",
    },
    {
      key: "prospective",
      label: "Prospective population is isolated",
      passed: evidence.prospectiveTrackingAvailable,
      blocking: false,
      detail: evidence.prospectiveTrackingAvailable
        ? "Prospective rows are separately identified"
        : "The current ledger cannot fully separate prospective and backfill rows",
    },
    {
      key: "sample",
      label: `At least ${MLB_MIN_PROSPECTIVE_UNIQUE_GAMES} unique prospective games`,
      passed:
        evidence.prospectiveTrackingAvailable &&
        evidence.settledUniqueGames >= MLB_MIN_PROSPECTIVE_UNIQUE_GAMES,
      blocking: false,
      detail: evidence.prospectiveTrackingAvailable
        ? `${evidence.settledUniqueGames}/${MLB_MIN_PROSPECTIVE_UNIQUE_GAMES} unique prospective games`
        : `${evidence.settledUniqueGames} historical ledger games; eligible prospective count unavailable`,
    },
    {
      key: "roi",
      label: "Executable ROI is positive",
      passed: evidence.roi != null && evidence.roi > 0,
      blocking: false,
      detail: evidence.roi == null ? "No eligible ROI" : `${(evidence.roi * 100).toFixed(1)}% ROI`,
    },
    {
      key: "roi_ci",
      label: "ROI confidence lower bound is above zero",
      passed:
        evidence.roiConfidenceLowerBound != null &&
        evidence.roiConfidenceLowerBound > 0,
      blocking: false,
      detail:
        evidence.roiConfidenceLowerBound == null
          ? "Prospective confidence artifact not available"
          : `${(evidence.roiConfidenceLowerBound * 100).toFixed(1)}% lower bound`,
    },
    {
      key: "clv",
      label: "Positive closing-line value",
      passed: evidence.clvN > 0 && evidence.avgClvPp != null && evidence.avgClvPp > 0,
      blocking: false,
      detail:
        evidence.clvN === 0 || evidence.avgClvPp == null
          ? "No eligible CLV sample"
          : `${evidence.avgClvPp >= 0 ? "+" : ""}${evidence.avgClvPp.toFixed(2)}pp over ${evidence.clvN} bets`,
    },
  ];

  const blocked = gates.some((gate) => gate.blocking && !gate.passed);
  const allPassed = gates.every((gate) => gate.passed);
  const researchFoundationPassed =
    evidence.immutableFeatureSnapshotsAvailable &&
    evidence.prospectiveTrackingAvailable;
  const state: MlbTrustState = allPassed
    ? "actionable"
    : blocked
      ? "blocked"
      : researchFoundationPassed
        ? "watch"
        : "research";
  const passed = gates.filter((gate) => gate.passed).length;

  return {
    market: evidence.market,
    policyVersion: MLB_ACTIONABILITY_POLICY_VERSION,
    modelVersion: evidence.modelVersion,
    // The legacy ledger has no canonical-horizon key. Keeping this explicit
    // prevents evidence from one timing policy from promoting another.
    canonicalHorizon: null,
    trustEvaluationId: [
      MLB_ACTIONABILITY_POLICY_VERSION,
      evidence.market,
      evidence.modelVersion ?? "unversioned",
      "horizon-unavailable",
    ].join(":"),
    state,
    passed,
    total: gates.length,
    gates,
    summary: allPassed
      ? "All operational and validation gates pass."
      : `${gates.length - passed} requirement(s) remain before this market can be actionable.`,
  };
}

/**
 * MLB game lines remain non-actionable until the prospective validation gates
 * documented in CLAUDE.md pass. Keep this policy centralized so a display
 * threshold cannot quietly re-enable betting language in one component.
 */
export function isMlbGameLineActionable(
  decision?: MlbActionabilityDecision | null,
): boolean {
  return decision?.state === "actionable";
}

export type MlbTotalEdgeDisplay = {
  side: "Over" | "Under" | "At market";
  signed: string;
  magnitude: number;
};

/** Neutral, descriptive formatting for our total minus the market total. */
export function describeMlbTotalEdge(
  edge: number | null | undefined,
): MlbTotalEdgeDisplay | null {
  if (edge == null || !Number.isFinite(edge)) return null;
  return {
    side: edge > 0 ? "Over" : edge < 0 ? "Under" : "At market",
    signed: `${edge > 0 ? "+" : ""}${edge.toFixed(1)}`,
    magnitude: Math.abs(edge),
  };
}
