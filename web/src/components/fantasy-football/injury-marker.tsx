import type { FantasyInjuryDetails } from "@/lib/fantasy-football/injury-display";
import { buildInjuryMarkerView } from "@/lib/fantasy-football/injury-display";

export default function InjuryMarker({
  injuryStatus,
  details,
  compact = false,
}: {
  injuryStatus: string | null;
  details: FantasyInjuryDetails | null;
  compact?: boolean;
}) {
  const marker = buildInjuryMarkerView(injuryStatus, details);
  if (!marker) return null;
  const tone = marker.cleared
    ? "bg-emerald-100 text-emerald-800 ring-emerald-300"
    : marker.conflict
      ? "bg-amber-100 text-amber-900 ring-amber-300"
      : "bg-red-100 text-red-800 ring-red-200";
  return <span
    title={marker.title}
    className={`rounded-full font-bold ring-1 ring-inset ${tone} ${compact ? "px-1.5 py-0.5 text-[9px]" : "px-2 py-1 text-[10px]"}`}
  >{marker.label}</span>;
}
