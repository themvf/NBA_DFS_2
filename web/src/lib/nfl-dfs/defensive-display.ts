import type { CaptureProfile, DefensiveForecastBundle, DefensiveProfile, DefensiveSettings, ForecastSummary } from './defensive-projection';

/** One captured distribution per player. RB volume and QB efficiency are never multiplied together. */
export function captureProfileFor(profile: DefensiveProfile, position: string): CaptureProfile {
  return profile === 'gpp-integrated' ? position === 'RB' ? 'allowed-rushing-volume' : 'pfr-efficiency' : profile;
}

export const DEFAULT_DFS_DEFENSIVE_SETTINGS: DefensiveSettings = {
  mode: 'experimental', profile: 'gpp-integrated',
};

/**
 * Defensive adjustments modify only the historical ("our") forecast, and the
 * server rejects them on any other source. Every place the build form sets
 * them goes through here, so resetting the default after a slate load cannot
 * pair "experimental" with Position workload (the 2026-09-28 PHI@CHI failure).
 */
export function defensiveSettingsFor(
  projectionSource: string,
  settings: DefensiveSettings = DEFAULT_DFS_DEFENSIVE_SETTINGS,
): DefensiveSettings {
  return projectionSource === 'our' ? { ...settings } : { ...settings, mode: 'off' };
}

/** The pool may hold a frozen run from a different form selection. */
export function selectedDefensiveForecast(
  bundle: DefensiveForecastBundle | null | undefined,
  settings: DefensiveSettings | null | undefined,
  position: string,
): ForecastSummary | null {
  return settings != null && settings.mode !== 'off' && bundle?.status === 'applied'
    && bundle.mode === settings.mode && bundle.profile === captureProfileFor(settings.profile, position)
    ? bundle.selected : null;
}

/**
 * Players a defensive build deliberately leaves on their baseline, and why.
 * A market-anchored player's projection is the books' line, which already
 * prices the opponent; the integrated profile adjusts only QBs and RBs. Both
 * used to be reported as "no_matching_pregame_capture", which read as a broken
 * capture match (week 8, 2026-11-01: 321 of 347 players, though every pool
 * player had a capture row). Generation and the page coverage both use this.
 */
export type DefensiveSkip = 'market_anchored' | 'profile_excludes_position';
export function defensiveSkipFor(profile: DefensiveProfile,
  player: { position: string; marketAnchor?: unknown }): DefensiveSkip | null {
  if (player.marketAnchor) return 'market_anchored';
  if (profile === 'gpp-integrated' && player.position !== 'QB' && player.position !== 'RB') return 'profile_excludes_position';
  return null;
}

export type DefensiveReasonCounts = {
  applied: number; marketAnchored: number; outsideProfile: number; noCapture: number; out: number; failedChecks: number;
};
/** Counts each bundle once by what kept it off (or put it on) the adjusted number. */
export function countDefensiveReasons(bundles: Array<Pick<DefensiveForecastBundle, 'status' | 'reason'> | null | undefined>): DefensiveReasonCounts {
  const counts: DefensiveReasonCounts = { applied: 0, marketAnchored: 0, outsideProfile: 0, noCapture: 0, out: 0, failedChecks: 0 };
  for (const bundle of bundles) {
    if (!bundle) continue;
    if (bundle.status === 'applied') counts.applied++;
    else if (bundle.reason === 'market_anchored') counts.marketAnchored++;
    else if (bundle.reason === 'profile_excludes_position') counts.outsideProfile++;
    else if (bundle.reason === 'no_matching_pregame_capture') counts.noCapture++;
    else if (bundle.reason === 'ineligible_or_out') counts.out++;
    else if (bundle.reason !== 'defensive_mode_off') counts.failedChecks++;
  }
  return counts;
}

/** The build note: how many changed, and why each other player did not. */
export function defensiveBuildNote(mode: string, profile: string, counts: DefensiveReasonCounts, anyCaptureExists: boolean): string {
  const parts = [
    counts.marketAnchored && `${counts.marketAnchored} on the books' line, which already prices the opponent`,
    counts.outsideProfile && `${counts.outsideProfile} outside this profile (it adjusts QBs and RBs only)`,
    counts.failedChecks && `${counts.failedChecks} failed the capture checks`,
    counts.noCapture && `${counts.noCapture} with no capture`,
    counts.out && `${counts.out} ruled out`,
  ].filter(Boolean).join('; ');
  const head = `${mode} ${profile}: ${counts.applied} player${counts.applied === 1 ? '' : 's'} adjusted`;
  const tail = counts.applied || !counts.noCapture && !counts.failedChecks ? ''
    : anyCaptureExists ? ' Captures exist for this upload, but no player they cover passed their checks.'
    : ' No capture exists for this upload yet (captures run on the projection schedule, only for slates already uploaded).';
  return `${head}${parts ? `; the rest keep their baseline: ${parts}.` : '.'}${tail}`;
}
