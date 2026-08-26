export type FantasyInjuryDetails = {
  active: boolean;
  status: string;
  bodyPart: string | null;
  injuryType: string | null;
  description: string | null;
  practiceStatus: string | null;
  expectedReturnMin: string | null;
  expectedReturnMax: string | null;
  weeksOutMin: number | null;
  weeksOutMax: number | null;
  availabilityProbability: number | null;
  estimateBasis: string;
  confidence: number | null;
  primarySource: string;
  detailSource: string | null;
  sourceConflict: boolean;
  firstSeenAt: string;
  lastConfirmedAt: string;
  providerUpdatedAt: string | null;
  clearedAt: string | null;
};

export type InjuryMarkerView = {
  label: string;
  title: string;
  cleared: boolean;
  conflict: boolean;
};

function finite(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function weeksLabel(minValue: number | null, maxValue: number | null): string | null {
  const min = finite(minValue);
  const max = finite(maxValue);
  if (min === null && max === null) return null;
  if (min !== null && max !== null && Math.abs(min - max) > 0.01) return `${min}-${max} wk`;
  return `${min ?? max} wk`;
}

function shortDate(value: string | null): string | null {
  if (!value) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp)
    ? new Date(timestamp).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" })
    : null;
}

function ageLabel(value: string | null, now: Date): string | null {
  if (!value) return null;
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return null;
  const hours = Math.max(0, Math.floor((now.getTime() - timestamp) / 3_600_000));
  if (hours < 1) return "<1h ago";
  if (hours < 48) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

export function buildInjuryMarkerView(
  injuryStatus: string | null,
  details: FantasyInjuryDetails | null,
  now = new Date(),
): InjuryMarkerView | null {
  if (details && !details.active) {
    const age = ageLabel(details.clearedAt, now);
    return {
      label: `CLEARED${age ? ` · ${age}` : ""}`,
      title: [
        `Cleared from ${details.status}`,
        details.bodyPart ? `Body part: ${details.bodyPart}` : null,
        details.clearedAt ? `Cleared: ${new Date(details.clearedAt).toLocaleString()}` : null,
        `Source: ${details.primarySource}`,
      ].filter(Boolean).join("\n"),
      cleared: true,
      conflict: details.sourceConflict,
    };
  }

  const status = details?.status || injuryStatus;
  if (!status) return null;
  const timeline = details ? weeksLabel(details.weeksOutMin, details.weeksOutMax) : null;
  const returnDate = details ? shortDate(details.expectedReturnMax ?? details.expectedReturnMin) : null;
  const visibleContext = [
    details?.bodyPart,
    timeline ?? (returnDate ? `back ${returnDate}` : null),
    !details?.bodyPart && !timeline && !returnDate ? details?.practiceStatus : null,
  ];
  const label = [status.toUpperCase(), ...visibleContext, details?.sourceConflict ? "CONFLICT" : null]
    .filter(Boolean)
    .join(" · ");
  const probability = finite(details?.availabilityProbability);
  const updated = details?.providerUpdatedAt ?? details?.lastConfirmedAt ?? null;
  return {
    label,
    title: [
      details?.injuryType ? `Injury: ${details.injuryType}` : details?.bodyPart ? `Body part: ${details.bodyPart}` : "No body-part detail supplied",
      details?.description,
      details?.practiceStatus ? `Practice: ${details.practiceStatus}` : null,
      timeline ? `Provider timeline: ${timeline}` : returnDate ? `Expected return: ${returnDate}` : "Return timeline unavailable",
      probability !== null ? `Availability probability: ${Math.round(probability * 100)}%` : null,
      details ? `Estimate basis: ${details.estimateBasis}` : null,
      details ? `Source: ${details.detailSource ?? details.primarySource}` : "Source: Sleeper status",
      updated ? `Updated: ${new Date(updated).toLocaleString()}` : null,
      details?.sourceConflict ? "Providers currently disagree" : null,
    ].filter(Boolean).join("\n"),
    cleared: false,
    conflict: details?.sourceConflict ?? false,
  };
}
