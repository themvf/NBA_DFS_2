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
