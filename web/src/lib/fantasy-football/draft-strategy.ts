export const DRAFT_STRATEGIES = ["balanced", "floor", "upside"] as const;
export type DraftStrategy = (typeof DRAFT_STRATEGIES)[number];

export const DRAFT_STRATEGY_META: Record<DraftStrategy, { name: string; description: string }> = {
  balanced: { name: "Balanced", description: "Ranks starter value, roster need, ADP value, and availability evenly." },
  floor: { name: "Floor", description: "Prefers stronger low-end projections and higher-confidence profiles." },
  upside: { name: "Upside", description: "Prefers higher-end outcomes and tier-breaking potential." },
};

export function isDraftStrategy(value: string): value is DraftStrategy {
  return DRAFT_STRATEGIES.includes(value as DraftStrategy);
}
