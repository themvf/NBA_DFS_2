/**
 * Phase 1 UI support: named cheap-player policy presets and a human summary.
 * Preset names describe the RULE, not an expected outcome. Values are
 * configuration; a run stores the resolved policy it actually used.
 */
import { DEFAULT_NFL_PUNT_POLICY, type NflPuntPolicy } from "./punt-policy";

export type PuntPresetKey = "no_punts" | "role_qualified" | "custom";

export const PUNT_PRESETS: Array<{ key: PuntPresetKey; label: string }> = [
  { key: "no_punts", label: "No punts (recommended)" },
  { key: "role_qualified", label: "Role-qualified" },
  { key: "custom", label: "Custom" },
];

/** Resolve a preset key to a concrete policy, preserving any user allow/deny lists. */
export function resolvePuntPreset(key: PuntPresetKey, current: NflPuntPolicy): NflPuntPolicy {
  const keep = { allowlistedPlayerIds: current.allowlistedPlayerIds, denylistedPlayerIds: current.denylistedPlayerIds };
  switch (key) {
    case "no_punts":
      return { ...DEFAULT_NFL_PUNT_POLICY, ...keep, mode: "no_punts" };
    case "role_qualified":
      // Same evidence bar, but the absolute price block is relaxed to only the
      // true minimum-priced filler ($200–$400 tier) rather than the $1,000 tier.
      return { ...DEFAULT_NFL_PUNT_POLICY, ...keep, mode: "role_qualified", absoluteMinSalary: 400 };
    case "custom":
      return { ...current, mode: "custom" };
  }
}

/** A short, plain-language summary of what the resolved policy will do. */
export function describePuntPolicy(policy: NflPuntPolicy): string[] {
  const lines = [
    `Players priced at or below $${policy.absoluteMinSalary.toLocaleString()} are blocked unless allowed for the run with a reason.`,
    `Below $${policy.roleEvidenceRequiredBelowSalary.toLocaleString()}, a player needs fresh role evidence, role confidence ≥ ${(policy.minimumRoleConfidence * 100).toFixed(0)}%${policy.minimumProjectedOpportunities !== null ? `, and ≥ ${policy.minimumProjectedOpportunities} projected opportunity` : ""}.`,
    `At most ${policy.maxSalaryReliefPlayersPerLineup} salary-relief player${policy.maxSalaryReliefPlayersPerLineup === 1 ? "" : "s"} per lineup.`,
    "Unknown or stale role evidence fails closed. Salary alone is never a reason to keep a cheap player.",
  ];
  if (policy.allowlistedPlayerIds.length) lines.push(`${policy.allowlistedPlayerIds.length} cheap player(s) allowed for this run by recorded override.`);
  return lines;
}
