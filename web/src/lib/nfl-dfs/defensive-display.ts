import type { DefensiveForecastBundle, DefensiveSettings, ForecastSummary } from './defensive-projection';

export const DEFAULT_DFS_DEFENSIVE_SETTINGS: DefensiveSettings = {
  mode: 'experimental', profile: 'pfr-efficiency',
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
): ForecastSummary | null {
  return settings?.mode !== 'off' && bundle?.status === 'applied'
    && bundle.mode === settings?.mode && bundle.profile === settings?.profile
    ? bundle.selected : null;
}
