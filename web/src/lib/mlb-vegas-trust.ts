export const MLB_GAME_LINES_TRUST = {
  state: "research" as const,
  label: "Research mode",
  description:
    "Moneyline and totals are shadow signals while the odds, feature-history, and immutable backtest repairs are completed.",
};

/**
 * MLB game lines remain non-actionable until the prospective validation gates
 * documented in CLAUDE.md pass. Keep this policy centralized so a display
 * threshold cannot quietly re-enable betting language in one component.
 */
export function isMlbGameLineActionable(): false {
  return false;
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
