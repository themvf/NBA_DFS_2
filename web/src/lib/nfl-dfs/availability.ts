export type RosterEvidence = { team: string; position: string; fetchedAt: string; sleeper: unknown };
export type Availability = { role: string; status: string; source: string; capturedAt: string | null; blockedReason: string | null; fresh: boolean };
const unavailable = new Set(["OUT", "IR", "PUP", "NFI", "SUSPENDED", "INACTIVE"]);
const normalize = (value: unknown) => String(value ?? "UNKNOWN").trim().toUpperCase();

/** Current roster evidence only; never infer a replacement starter or clear a DK exclusion. */
export function resolveAvailability(evidence: RosterEvidence | undefined, team: string, position: string, now: number): Availability {
  const unknown: Availability = { role: position === "QB" ? "QB role unresolved" : "Role unresolved", status: "UNKNOWN", source: "No matching current roster", capturedAt: null, blockedReason: null, fresh: false };
  if (!evidence || evidence.team !== team || evidence.position !== position) return unknown;
  const captured = Date.parse(evidence.fetchedAt);
  if (!Number.isFinite(captured) || captured > now || now - captured > 72 * 3600000) return { ...unknown, source: "Roster stale or invalid", capturedAt: evidence.fetchedAt };
  const s = evidence.sleeper as Record<string, unknown> | null;
  if (!s || s.team !== team || s.position !== position) return unknown;
  const depth = typeof s.depth_chart_order === "number" && Number.isInteger(s.depth_chart_order) && s.depth_chart_order > 0 ? s.depth_chart_order : null;
  const status = normalize(s.injury_status ?? s.status);
  const rosterStatus = normalize(s.status);
  const blockedReason = unavailable.has(status) || unavailable.has(rosterStatus) ? `Unavailable: ${status === "ACTIVE" ? rosterStatus : status}`
    : position === "QB" && depth !== null && depth > 1 ? `Listed QB${depth}; starter workload not supported` : null;
  return { role: depth === null ? unknown.role : position === "QB" ? depth === 1 ? "Expected starter · QB1" : `Backup · QB${depth}` : `Listed ${position}${depth}`, status, source: "Sleeper roster (retrieval time; not game-day confirmation)", capturedAt: evidence.fetchedAt, blockedReason, fresh: true };
}
