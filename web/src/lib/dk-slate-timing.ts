export const DK_SLATE_TIMING_OPTIONS = [
  { value: "turbo", label: "Turbo" },
  { value: "early", label: "Early" },
  { value: "main", label: "Main" },
  { value: "night", label: "Night" },
] as const;

export type DkSlateTiming = (typeof DK_SLATE_TIMING_OPTIONS)[number]["value"];

export function normalizeDkSlateTiming(value: string | null | undefined): string | undefined {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) return undefined;
  if (normalized === "late") return "night";
  return normalized;
}
