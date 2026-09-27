import type { DefensiveForecastBundle, DefensiveSettings, ForecastSummary } from './defensive-projection';

export const DEFAULT_DFS_DEFENSIVE_SETTINGS: DefensiveSettings = {
  mode: 'experimental', profile: 'pfr-efficiency',
};

/** The pool may hold a frozen run from a different form selection. */
export function selectedDefensiveForecast(
  bundle: DefensiveForecastBundle | null | undefined,
  settings: DefensiveSettings | null | undefined,
): ForecastSummary | null {
  return settings?.mode !== 'off' && bundle?.status === 'applied'
    && bundle.mode === settings?.mode && bundle.profile === settings?.profile
    ? bundle.selected : null;
}
