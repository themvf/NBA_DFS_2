export type InjuryEvidence = { identityBridge?: {sourceLocalId:number;targetLocalId:number;method:string}; id: string; source: string; status: string; practice: string | null; observedAt: string; updatedAt: string | null; team: string; week: number | null; hash: string; reportType?: string; kickoff?: string; url?: string; unverifiedUpdate?: string };
/** `archived`: the Sleeper evidence is the archived capture at or before the evaluation time, not the latest row. */
export type RosterEvidence = { team: string; position: string; fetchedAt: string; sleeper: unknown; injuries?: InjuryEvidence[]; injuryReadFailed?: boolean; kickoff?: string | null; archived?: boolean };
export type Availability = { role: string; status: string; source: string; capturedAt: string | null; blockedReason: string | null; fresh: boolean;
  /** Depth-chart role block only (e.g. listed QB2), separate from health so a pinned health decision cannot drop it. */
  roleBlockedReason?: string | null;
  /** The depth chart's role when a build-form confirmation overrode it. */
  chartRole?: string;
  /** Set on the quarterback the user confirmed as the starter. */
  confirmedStarter?: boolean; evidence?: InjuryEvidence[]; warnings?: string[]; evaluatedAt?: string; officialConfirmed?: boolean; kickoff?: string | null; freshFantasyPros?: boolean; decisionId?: string; pinned?: boolean };
export type PinnedGameAvailabilityDecision = { version:string; state:string; projection_status:string|null; source:string|null;
  observation_id:number|null; source_snapshot_id:number|null; available_at:string|null; as_of_at:string; kickoff:string|null;
  reason:string; qualifying_observation_ids:number[]; display_only_observation_ids:number[] };
const unavailable = new Set(["OUT", "IR", "PUP", "NFI", "SUSPENDED", "INACTIVE"]);
const normalize = (value: unknown) => String(value ?? "UNKNOWN").trim().toUpperCase();
const aliases: Record<string, string> = { LA: "LAR", WAS: "WSH", AZ: "ARI", JAC: "JAX" };
const teamKey = (value: unknown) => { const key = normalize(value); return aliases[key] ?? key; };
/** One key per franchise across DraftKings, Sleeper and nflverse codes (LA/LAR, WAS/WSH, AZ/ARI, JAC/JAX). */
export const nflTeamKey = teamKey;

export const ROSTER_FRESH_MS = 72 * 3600000;

/** How recent a live-resolved (unpinned) evaluation must be; see `availabilityCurrent`. */
export const LIVE_AVAILABILITY_MAX_AGE_MS = 60_000;

/**
 * Two different times, kept apart. `decisionAt` is when a saved slate's
 * evidence was evaluated: its projection run's cutoff (`modelAsOf`), which is
 * also the evaluation time of every pinned decision on that run. `now` is when
 * this request runs. `onNewestRun` says whether that run is still the newest
 * one for the week -- false when the newest run cannot be established.
 */
export type DecisionClock = { now: number; decisionAt: string | null; onNewestRun: boolean };

/** A plain timestamp is a live-resolved check: no decision time, not on a known run. */
export const liveClock = (now: number): DecisionClock => ({ now, decisionAt: null, onNewestRun: false });
export const toDecisionClock = (value: number | DecisionClock): DecisionClock => typeof value === "number" ? liveClock(value) : value;

/**
 * May this availability evidence still gate a build right now?
 *
 * Before 2026-09-29 every experimental source required `now - evaluatedAt <=
 * 60s`. That was written when availability was resolved at request time. Since
 * #280 a saved slate evaluates at its projection cutoff, so the check failed for
 * every non-DST player once the cutoff was a minute old: the page showed them
 * eligible (it evaluated at the cutoff) and the server then dropped them.
 *
 * Decision-time evidence (evaluated exactly at `decisionAt`) is current when the
 * game has not started and the slate is on the NEWEST projection run. Newest-run,
 * not an age window, because supersession is the correctness signal: a newer run
 * carries newer injury and depth-chart evidence, so the old decision may now be
 * wrong; while no newer run exists the pinned decision is the best evidence there
 * is, and its age is already surfaced separately (the stale-roster warning, the
 * 72-hour forecast expiry). An age window would keep a superseded decision live
 * for up to 72 hours after a newer run ruled a player out. An unknown newest run
 * fails closed.
 *
 * Anything else is live-resolved evidence and keeps the 60-second rule.
 */
export function availabilityCurrent(a: Availability | undefined, clock: DecisionClock): { ok: boolean; reason: string } {
  if (a?.blockedReason) return { ok: false, reason: a.blockedReason };
  return evaluationCurrent(a, clock);
}

/**
 * The timing half of `availabilityCurrent`, without the eligibility block: is
 * this evaluation still usable at `clock.now`? A verified-inactive player's
 * evidence is current even though it blocks him (the injury scenario needs it).
 */
export function evaluationCurrent(a: Availability | undefined, clock: DecisionClock): { ok: boolean; reason: string } {
  const no = (reason: string) => ({ ok: false, reason });
  const { now } = clock;
  if (!a) return no("No availability evidence.");
  if (!a.fresh) return no("Roster evidence was not fresh when it was evaluated.");
  const evaluated = Date.parse(a.evaluatedAt ?? ""), kickoff = Date.parse(a.kickoff ?? ""), decision = Date.parse(clock.decisionAt ?? "");
  if (!Number.isFinite(now) || !Number.isFinite(evaluated)) return no("Availability evaluation time is unknown.");
  if (!Number.isFinite(kickoff)) return no("Kickoff is unresolved.");
  if (now >= kickoff) return no("The game has started.");
  if (Number.isFinite(decision) && evaluated === decision) {
    if (decision > now) return no("The projection decision time is in the future.");
    if (!clock.onNewestRun) return no("A newer projection run exists (or the newest run could not be checked). Move this slate to the newest projections before building.");
    return { ok: true, reason: "Decision-time evidence from the newest projection run." };
  }
  if (evaluated > now || now - evaluated > LIVE_AVAILABILITY_MAX_AGE_MS) return no("Current roster eligibility is unavailable or stale.");
  return { ok: true, reason: "Live evidence evaluated within the last minute." };
}

/**
 * Present a decision saved on the projection row; never re-run source precedence in Vercel.
 *
 * The saved decision owns HEALTH. The depth-chart role block (a listed QB2+)
 * is not a health fact and the decision does not carry it, so it comes from
 * the role evidence. Before 2026-09-28 this presenter dropped it, which left
 * every backup QB eligible on every pinned run from 2026-09-26 on.
 */
export function presentPinnedGameAvailability(decision:PinnedGameAvailabilityDecision, role:string, roleBlockedReason:string|null=null):Availability {
  const out=decision.state==='OUT_CONFIRMED';
  const warnings=[decision.reason];
  if(decision.display_only_observation_ids.length) warnings.push(`${decision.display_only_observation_ids.length} display-only observation(s) did not affect this decision.`);
  return {role,status:decision.projection_status??decision.state,source:decision.source??'Pinned availability resolver',
    capturedAt:decision.available_at,blockedReason:out?`Unavailable: ${decision.projection_status??'OUT'}`:roleBlockedReason,roleBlockedReason,
    fresh:['OUT_CONFIRMED','EXPECTED_ACTIVE','QUESTIONABLE','DOUBTFUL'].includes(decision.state),warnings,
    evaluatedAt:decision.as_of_at,kickoff:decision.kickoff,officialConfirmed:decision.source==='nfl_official',pinned:true,
    decisionId:[decision.version,decision.source_snapshot_id??'none',decision.observation_id??'none',decision.as_of_at].join(':')};
}
/**
 * Current roster evidence only; never infer a replacement starter or clear a DK exclusion.
 *
 * Stale evidence may BLOCK but never CLEAR. A four-day-old depth chart saying a player is
 * QB3 is still evidence he is not the starter -- it is only evidence of HEALTH that decays.
 * Failing open here once put three backup quarterbacks into a Showdown pool at starter
 * projections, because "my note is six days old" was treated identically to "I have never
 * heard of him". A corrupt or future-dated capture is a different thing from an old one and
 * still resolves to unknown.
 */
export function resolveAvailability(evidence: RosterEvidence | undefined, team: string, position: string, now: number): Availability {
  const unknown: Availability = { role: position === "QB" ? "QB role unresolved" : "Role unresolved", status: "UNKNOWN", source: "No matching current roster", capturedAt: null, blockedReason: null, fresh: false };
  if (!evidence || teamKey(evidence.team) !== teamKey(team) || evidence.position !== position) return unknown;
  const captured = Date.parse(evidence.fetchedAt);
  if (!Number.isFinite(captured) || captured > now) return { ...unknown, source: "Roster capture time invalid", capturedAt: evidence.fetchedAt };
  const fresh = now - captured <= ROSTER_FRESH_MS;
  const s = evidence.sleeper as Record<string, unknown> | null;
  if (!s || teamKey(s.team) !== teamKey(team) || s.position !== position) return { ...unknown, capturedAt: evidence.fetchedAt, ...(fresh ? {} : { source: "Roster stale and unmatched" }) };
  const depth = typeof s.depth_chart_order === "number" && Number.isInteger(s.depth_chart_order) && s.depth_chart_order > 0 ? s.depth_chart_order : null;
  const status = normalize(s.injury_status || s.status);
  const rosterStatus = normalize(s.status);
  const staleNote = fresh ? "" : ` (roster captured ${evidence.fetchedAt.slice(0, 10)}; blocks still apply, clearances do not)`;
  const roleBlockedReason = position === "QB" && depth !== null && depth > 1 ? `Listed QB${depth}; starter workload not supported${staleNote}` : null;
  const blockedReason = unavailable.has(status) || unavailable.has(rosterStatus) ? `Unavailable: ${unavailable.has(status) ? status : rosterStatus}${staleNote}`
    : roleBlockedReason;
  return {
    role: depth === null ? unknown.role : position === "QB" ? depth === 1 ? "Expected starter · QB1" : `Backup · QB${depth}` : `Listed ${position}${depth}`,
    // The status string is kept even when stale: a stale OUT still blocks, and the
    // opportunity-redistribution donor path reads it. `fresh` carries the caveat.
    status,
    source: fresh ? "Sleeper roster (retrieval time; not game-day confirmation)" : "Sleeper roster, STALE (depth chart used to block only; health unknown)",
    capturedAt: evidence.fetchedAt, blockedReason, roleBlockedReason, fresh,
  };
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

/**
 * Team-level QB rule. `resolveAvailability` only ever sees one player, so a
 * quarterback Sleeper carries with NO depth number resolves to "QB role
 * unresolved" and is not blocked -- while his teammate is listed QB1 in the
 * same capture. That is how a zero-game rookie (Jake Haener, NYG, 2026 week 2)
 * sat in a Showdown pool at a position-prior 13.3 points beside a resolved
 * starter. Once a team has an identified QB1, an unresolved QB on that team is
 * evidence of a backup, not of nothing; block him. This only ever ADDS a block,
 * never clears one, and a stale QB1 listing still counts (blocks apply,
 * clearances do not). A team with no identified QB1 is left exactly as it was:
 * we never infer a starter.
 */
export type TeamQb1 = { name: string; capturedAt: string | null };

export function applyTeamQbContext(availability: Availability, position: string, teamQb1: TeamQb1 | null | undefined): Availability {
  if (position !== "QB" || !teamQb1 || availability.blockedReason || availability.role !== "QB role unresolved") return availability;
  return {
    ...availability,
    role: "Backup · QB depth unlisted",
    blockedReason: `QB role unresolved while ${teamQb1.name} is listed QB1; starter workload not supported`,
    source: availability.source === "No matching current roster" ? "Team depth chart (QB1 identified for this team)" : availability.source,
  };
}

/** The identified QB1 per team, from already-resolved availabilities. Any capture age counts. */
export function identifyTeamQb1s(players: ReadonlyArray<{ team: string; position: string; name: string; availability: Availability }>): Map<string, TeamQb1> {
  const out = new Map<string, TeamQb1>();
  for (const p of players) {
    if (p.position !== "QB" || p.availability.role !== "Expected starter · QB1" || p.availability.blockedReason) continue;
    const key = teamKey(p.team);
    // Two listed QB1s on one team is conflicting evidence; block nobody on it.
    if (out.has(key)) { out.set(key, { name: "", capturedAt: null }); continue; }
    out.set(key, { name: p.name, capturedAt: p.availability.capturedAt });
  }
  for (const [key, value] of out) if (!value.name) out.delete(key);
  return out;
}
