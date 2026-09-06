export type InjuryEvidence = { id: string; source: string; status: string; practice: string | null; observedAt: string; updatedAt: string | null; team: string; week: number | null; hash: string; reportType?: string; kickoff?: string; url?: string; unverifiedUpdate?: string };
export type RosterEvidence = { team: string; position: string; fetchedAt: string; sleeper: unknown; injuries?: InjuryEvidence[]; injuryReadFailed?: boolean; kickoff?: string | null };
export type Availability = { role: string; status: string; source: string; capturedAt: string | null; blockedReason: string | null; fresh: boolean; evidence?: InjuryEvidence[]; warnings?: string[]; evaluatedAt?: string; officialConfirmed?: boolean; kickoff?: string | null; freshFantasyPros?: boolean };
const unavailable = new Set(["OUT", "IR", "PUP", "NFI", "SUSPENDED", "INACTIVE"]);
const normalize = (value: unknown) => String(value ?? "UNKNOWN").trim().toUpperCase();
const aliases: Record<string, string> = { LA: "LAR", WAS: "WSH", AZ: "ARI", JAC: "JAX" };
const teamKey = (value: unknown) => { const key = normalize(value); return aliases[key] ?? key; };

/** Current roster evidence only; never infer a replacement starter or clear a DK exclusion. */
export function resolveAvailability(evidence: RosterEvidence | undefined, team: string, position: string, now: number): Availability {
  const unknown: Availability = { role: position === "QB" ? "QB role unresolved" : "Role unresolved", status: "UNKNOWN", source: "No matching current roster", capturedAt: null, blockedReason: null, fresh: false };
  if (!evidence || teamKey(evidence.team) !== teamKey(team) || evidence.position !== position) return unknown;
  const captured = Date.parse(evidence.fetchedAt);
  if (!Number.isFinite(captured) || captured > now || now - captured > 72 * 3600000) return { ...unknown, source: "Roster stale or invalid", capturedAt: evidence.fetchedAt };
  const s = evidence.sleeper as Record<string, unknown> | null;
  if (!s || teamKey(s.team) !== teamKey(team) || s.position !== position) return unknown;
  const depth = typeof s.depth_chart_order === "number" && Number.isInteger(s.depth_chart_order) && s.depth_chart_order > 0 ? s.depth_chart_order : null;
  const status = normalize(s.injury_status || s.status);
  const rosterStatus = normalize(s.status);
  const blockedReason = unavailable.has(status) || unavailable.has(rosterStatus) ? `Unavailable: ${unavailable.has(status) ? status : rosterStatus}`
    : position === "QB" && depth !== null && depth > 1 ? `Listed QB${depth}; starter workload not supported` : null;
  return { role: depth === null ? unknown.role : position === "QB" ? depth === 1 ? "Expected starter · QB1" : `Backup · QB${depth}` : `Listed ${position}${depth}`, status, source: "Sleeper roster (retrieval time; not game-day confirmation)", capturedAt: evidence.fetchedAt, blockedReason, fresh: true };
}

/** Freeze the evidence used at decision time. Never infer health from an omitted row. */
export function resolveGameAvailability(evidence: RosterEvidence | undefined, team: string, position: string, now: number, week: number | null, kickoff: string | null): Availability {
  const base = resolveAvailability(evidence, team, position, now);
  const warnings: string[] = [];
  const start = kickoff ? Date.parse(kickoff) : NaN;
  const nearKickoff = Number.isFinite(start) && start - now <= 6 * 3600000;
  const maxAge = (nearKickoff ? 2 : 24) * 3600000;
  const relevant = (evidence?.injuries ?? []).filter(row => ['fantasypros','nfl_official'].includes(row.source) && row.week === week && week !== null && teamKey(row.team) === teamKey(team));
  const usable = relevant.filter(row => {
    if (!row.updatedAt) return false; // Retrieval time cannot date the provider's report.
    const observed = Date.parse(row.observedAt), updated = row.updatedAt ? Date.parse(row.updatedAt) : observed;
    return observed <= now && updated <= now && now - observed <= maxAge && now - updated <= maxAge;
  });
  const official = usable.find(row => row.source === 'nfl_official' && row.reportType === 'inactive_list' && row.kickoff && Date.parse(row.kickoff) === start && start > now && ['ACTIVE','INACTIVE'].includes(row.status));
  if (!usable.some(row => row.source === 'fantasypros')) warnings.push('No fresh, team-matched FantasyPros injury observation for this week; absence does not mean healthy.');
  if (relevant.some(row => !row.updatedAt)) warnings.push('Provider update time or timezone is unverified; this observation cannot change eligibility.');
  if (evidence?.injuryReadFailed) warnings.push('Injury history could not be loaded.');
  if (nearKickoff && (!base.capturedAt || now - Date.parse(base.capturedAt) > maxAge)) warnings.push('Roster evidence needs a game-day refresh.');
  if (!Number.isFinite(start)) warnings.push('Kickoff unresolved; game-day freshness cannot be verified.');
  if (Number.isFinite(start) && start <= now) warnings.push('Game has started; this is not a pregame snapshot.');
  warnings.push(official ? 'Official list manually reviewed. Active does not guarantee normal workload.' : 'Official inactive list has not been verified for this player. Active does not guarantee normal workload.');
  const latest = official ?? [...usable.filter(row => row.source === 'fantasypros')].sort((a,b) => Date.parse(b.observedAt)-Date.parse(a.observedAt) || b.id.localeCompare(a.id))[0];
  const equivalentStatus = (value: string) => ['ACTIVE','HEALTHY'].includes(normalize(value)) ? 'ACTIVE' : normalize(value);
  if (latest && equivalentStatus(latest.status) !== equivalentStatus(base.status)) warnings.push(`Sources differ: Sleeper ${base.status}; ${latest.source} ${latest.status}. Review before using this player.`);
  const fpOut = latest && unavailable.has(normalize(latest.status));
  return { ...base, status: latest ? latest.status : base.status,
    source: latest ? `${base.source} + ${official ? 'official inactive list (manual review)' : 'FantasyPros injury observation'}` : base.source,
    blockedReason: base.blockedReason ?? (fpOut ? `${official ? 'Official list' : 'FantasyPros'} reports unavailable: ${latest.status}` : null),
    evidence: relevant, warnings, evaluatedAt: new Date(now).toISOString(), officialConfirmed: Boolean(official), kickoff,
    freshFantasyPros: usable.some(row => row.source === 'fantasypros') };
}
