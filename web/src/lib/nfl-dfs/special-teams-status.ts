import type { SpecialTeamsProjection } from './special-teams-projection';
import { MISSING_SPECIAL_TEAMS_REASON } from './special-teams-projection';
import { isLocked } from './workspace-stage';

type ForecastPlayer = {
  name: string; position: string; isOut: boolean;
  specialTeams?: SpecialTeamsProjection | null; specialTeamsReason?: string | null;
};

/** The same evidence drives readiness and the builder; an archive never offers a repair. */
export function specialTeamsStatus(input: {
  players: readonly ForecastPlayer[]; firstKickoff: string | null; now: number; refreshAvailable: boolean;
}) {
  const players = input.players.filter(p => (p.position === 'DST' || p.position === 'K') && !p.isOut);
  if (!players.length) return null;
  const missing = players.filter(p => !p.specialTeams);
  const archived = isLocked(input.firstKickoff, input.now);
  const knownPregame = input.firstKickoff != null && Number.isFinite(Date.parse(input.firstKickoff)) && !archived;
  const legacy = missing.some(p => !p.specialTeamsReason || p.specialTeamsReason === MISSING_SPECIAL_TEAMS_REASON);
  const coverage = (['DST', 'K'] as const).flatMap(position => {
    const rows = players.filter(p => p.position === position);
    return rows.length ? [`${rows.filter(p => p.specialTeams).length} of ${rows.length} ${position === 'DST' ? 'defenses have opponent forecasts' : 'kickers have team scoring forecasts'}`] : [];
  }).join('; ');
  const fallback = missing.length ? `${missing.length} ${missing.length === 1 ? 'player uses' : 'players use'} historical forecasts.` : '';
  const text = `${coverage}. ${fallback}${archived ? ' Saved forecasts are preserved; no updates are needed for this closed slate.'
    : missing.length && legacy && knownPregame ? ' Update data to prepare current matchup forecasts.'
    : missing.length && legacy ? ' Verify the kickoff time before updating this slate.'
    : missing.length ? ' Some matchup inputs are missing or could not be verified; see forecast details.'
    : ' These forecasts apply automatically with Our projections.'}`.trim();
  return {
    archived, missing, coverage, text,
    title: archived ? 'Saved special teams forecasts' : missing.length ? 'Special teams: historical fallback' : 'Special teams forecasts ready',
    // A newer run can repair either missing data or an unavailable input. Avoid
    // asking for repeated rebuilds when a current run explicitly lacks inputs.
    action: missing.length && knownPregame && (input.refreshAvailable || legacy)
      ? input.refreshAvailable ? 'refresh_projections' as const : 'update_data' as const : undefined,
  };
}
