export const SPECIAL_TEAMS_VERSION = 'nfl-special-teams-pregame-v1';

export type SpecialTeamsProjection = {
  version: typeof SPECIAL_TEAMS_VERSION;
  status: 'candidate';
  position: 'DST' | 'K';
  mean: number;
  p10: number;
  p50: number;
  p90: number;
  boom: number;
  feature_snapshot: Record<string, unknown>;
};

const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);

/** A missing or malformed saved candidate cannot silently become an optimizer score. */
export function readSpecialTeamsProjection(
  featureSnapshot: unknown, position: string,
): { projection: SpecialTeamsProjection | null; reason: string | null } {
  if (position !== 'DST' && position !== 'K') return { projection: null, reason: null };
  const raw = record(featureSnapshot) ? featureSnapshot.special_teams_candidate : null;
  if (!record(raw)) return { projection: null, reason: 'Refresh projections to create the saved DST/kicker candidate.' };
  if (raw.status === 'unavailable') {
    const detail = record(raw.feature_snapshot) ? raw.feature_snapshot.reason : null;
    return { projection: null, reason: typeof detail === 'string' ? detail : 'Required pregame inputs are missing.' };
  }
  if (raw.version !== SPECIAL_TEAMS_VERSION || raw.status !== 'candidate' || raw.position !== position ||
      !finite(raw.mean) || !finite(raw.p10) || !finite(raw.p50) || !finite(raw.p90) ||
      !finite(raw.boom) || raw.boom < 0 || raw.boom > 1 || raw.p10 > raw.p50 || raw.p50 > raw.p90 ||
      !record(raw.feature_snapshot) || raw.feature_snapshot.authority !== 'candidate_only') {
    return { projection: null, reason: 'Saved DST/kicker candidate failed its version or scoring checks.' };
  }
  return { projection: raw as SpecialTeamsProjection, reason: null };
}
