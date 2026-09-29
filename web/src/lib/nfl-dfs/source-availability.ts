/**
 * Can the experimental projection sources produce a forecast for this slate
 * right now? Computed on the server from the same rule generation applies, so
 * the page never offers a source that the build would then reject or silently
 * fill with the historical fallback (`nfl-source-availability-v1`).
 *
 * Workload (per position): the player's forecast is attached for the slate's
 * target week, he is in the shared pregame cohort (`workloadPoolReason`: not
 * ruled out, unstarted salary game, current availability, resolved QB1), and the
 * forecast is unexpired and unstarted at request time. Calibrated (per position
 * the release qualifies): a pinned candidate is attached and its game has not
 * started. A source is usable when at least one position is.
 */
import type { DecisionClock } from './availability';
import { WORKLOAD_MAX_AGE_MS, workloadPoolReason, type WorkloadProjection, type WorkloadTarget } from './workload-projection';
import type { CalibratedProjection, CalibratedRelease } from './calibrated-projection';
import { positionWorkloadUnavailable } from './calibrated-projection';
import { LEGACY_WORKLOAD_POSITIONS, WORKLOAD_POSITIONS, type WorkloadPositions } from './workload-selection';

export const SOURCE_AVAILABILITY_VERSION = 'nfl-source-availability-v1';
export type SourcePositionAvailability = { usable: boolean; count: number; reason: string };
export type SourceAvailabilityEntry = { usable: boolean; reason: string; positions: Record<string, SourcePositionAvailability> };
export type NflSourceAvailability = {
  version: typeof SOURCE_AVAILABILITY_VERSION; evaluatedAt: string; decisionAt: string | null; onNewestRun: boolean;
  workload: SourceAvailabilityEntry; calibrated: SourceAvailabilityEntry;
};
export type SourcePlayer = Omit<WorkloadTarget, 'identity'> & {
  workload?: WorkloadProjection | null; workloadReason?: string;
  positionWorkload?: CalibratedProjection | null; positionWorkloadReason?: string;
  calibrated?: CalibratedProjection | null; calibrationReason?: string;
};
export type SourceContext = {
  /** Slate-level reason nothing can work (no projection run, unknown week). */
  slateReason?: string | null;
  /** Why the WR volume-share report is missing, when it is. */
  volumeShareReason?: string | null;
  /** Why calibrated snapshots are missing, when they are. */
  calibratedReason?: string | null;
  release: CalibratedRelease;
};

/** The most common per-player reason; ties keep the first seen. */
function commonReason(reasons: string[], fallback: string): string {
  const counts = new Map<string, number>();
  for (const r of reasons) counts.set(r, (counts.get(r) ?? 0) + 1);
  let best: string | null = null, n = 0;
  for (const [r, c] of counts) if (c > n) { best = r; n = c; }
  return best ?? fallback;
}

function summarize(positions: Record<string, SourcePositionAvailability>, label: string): { usable: boolean; reason: string } {
  const entries = Object.entries(positions);
  const usable = entries.filter(([, v]) => v.usable);
  if (usable.length) return { usable: true, reason: `${label}: ${usable.map(([p, v]) => `${p} ${v.count}`).join(' · ')} usable forecasts.` };
  const reasons = new Set(entries.map(([, v]) => v.reason));
  return { usable: false, reason: reasons.size === 1 ? [...reasons][0] : entries.map(([p, v]) => `${p}: ${v.reason}`).join(' ') };
}

function forecastCurrent(forecast: { kickoff: string; capturedAt: string }, now: number): string | null {
  const kickoff = Date.parse(forecast.kickoff), captured = Date.parse(forecast.capturedAt);
  if (!Number.isFinite(kickoff) || now >= kickoff) return 'The game has started.';
  if (!Number.isFinite(captured) || now - captured > WORKLOAD_MAX_AGE_MS) return 'Forecast expired (older than 72 hours).';
  return null;
}

/** One position's usable workload forecasts under the generation rule. */
export function workloadPositionAvailability(players: readonly SourcePlayer[], position: string, clock: DecisionClock, context: SourceContext): SourcePositionAvailability {
  const none = (reason: string) => ({ usable: false, count: 0, reason });
  if (context.slateReason) return none(context.slateReason);
  if (position === 'WR' && context.volumeShareReason) return none(context.volumeShareReason);
  if (position !== 'WR') { const study = positionWorkloadUnavailable(position, context.release); if (study) return none(study); }
  const own = players.filter((p) => p.position === position && !p.isOut);
  if (!own.length) return none(`No ${position} available on this slate.`);
  const reasons: string[] = [];
  let count = 0;
  for (const p of own) {
    const forecast = position === 'WR' ? p.workload : p.positionWorkload;
    if (!forecast) { reasons.push((position === 'WR' ? p.workloadReason : p.positionWorkloadReason) ?? 'No forecast for this player.'); continue; }
    const pool = workloadPoolReason(p, clock);
    if (!pool.ok) { reasons.push(pool.reason); continue; }
    const stale = forecastCurrent(forecast, clock.now);
    if (stale) { reasons.push(stale); continue; }
    count++;
  }
  return count ? { usable: true, count, reason: `${count} usable ${position} forecast${count === 1 ? '' : 's'}.` }
    : none(commonReason(reasons, `No usable ${position} forecast.`));
}

export function calibratedPositionAvailability(players: readonly SourcePlayer[], position: string, clock: DecisionClock, context: SourceContext): SourcePositionAvailability {
  const none = (reason: string) => ({ usable: false, count: 0, reason });
  const policy = context.release.positions[position];
  if (!policy?.enabledForOptIn) return none(`Release gate did not qualify ${position} in study ${context.release.studyId.slice(0, 8)} (study status: ${policy?.studyCandidateStatus ?? 'unknown'}).`);
  if (context.slateReason) return none(context.slateReason);
  if (context.calibratedReason) return none(context.calibratedReason);
  const own = players.filter((p) => p.position === position && !p.isOut);
  if (!own.length) return none(`No ${position} available on this slate.`);
  const reasons: string[] = [];
  let count = 0;
  for (const p of own) {
    if (!p.calibrated) { reasons.push(p.calibrationReason ?? 'No frozen candidate for this player.'); continue; }
    if (!(Date.parse(p.calibrated.kickoff) > clock.now)) { reasons.push('The game has started.'); continue; }
    count++;
  }
  return count ? { usable: true, count, reason: `${count} usable ${position} forecast${count === 1 ? '' : 's'}.` }
    : none(commonReason(reasons, `No usable ${position} forecast.`));
}

export function computeSourceAvailability(players: readonly SourcePlayer[], clock: DecisionClock, context: SourceContext): NflSourceAvailability {
  const workload = Object.fromEntries(WORKLOAD_POSITIONS.map((p) => [p, workloadPositionAvailability(players, p, clock, context)]));
  const calibrated = Object.fromEntries(Object.keys(context.release.positions).map((p) => [p, calibratedPositionAvailability(players, p, clock, context)]));
  return { version: SOURCE_AVAILABILITY_VERSION, evaluatedAt: new Date(clock.now).toISOString(), decisionAt: clock.decisionAt, onNewestRun: clock.onNewestRun,
    workload: { ...summarize(workload, 'Position workload'), positions: workload },
    calibrated: { ...summarize(calibrated, 'Calibrated'), positions: calibrated } };
}

/**
 * Generation's gate: why this source cannot run with these settings, or null.
 * Workload needs at least one ENABLED position that is usable; calibrated needs
 * any usable position. The reason is the one the page shows.
 */
export function sourceBlockedReason(availability: NflSourceAvailability, source: string, positions?: WorkloadPositions): string | null {
  if (source === 'workload') {
    const enabled = WORKLOAD_POSITIONS.filter((p) => (positions ?? LEGACY_WORKLOAD_POSITIONS)[p]);
    if (enabled.some((p) => availability.workload.positions[p]?.usable)) return null;
    if (!enabled.length) return null; // validateWorkloadPositions owns this error.
    const reasons = enabled.map((p) => `${p}: ${availability.workload.positions[p]?.reason ?? 'unavailable'}`);
    return `Position workload cannot produce a forecast for the enabled positions. ${reasons.join(' ')}`;
  }
  if (source === 'calibrated') return availability.calibrated.usable ? null : `Calibrated projections are unavailable: ${availability.calibrated.reason}`;
  return null;
}
