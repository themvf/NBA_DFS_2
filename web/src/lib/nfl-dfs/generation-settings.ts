import type { NflOptimizerSettings } from '@/app/dfs/nfl/nfl-optimizer';
import { canonicalAuditJson } from './audit-json';

export function generationSettings(
  settings: Omit<NflOptimizerSettings, 'format' | 'lockedPlayerIds' | 'excludedPlayerIds' | 'minExposureByPlayer' | 'maxExposureByPlayer'>,
  format: NflOptimizerSettings['format'], locked: number[], excluded: number[], targets: Record<string, number>,
): NflOptimizerSettings {
  const exposure = Object.fromEntries(Object.entries(targets).map(([id, percent]) => {
    const n = Math.max(1, settings.nLineups);
    return [id, Math.max(0, Math.min(n, Math.round(percent / 100 * n))) / n];
  }));
  return { ...settings, format, lockedPlayerIds: locked, excludedPlayerIds: excluded,
    minExposureByPlayer: exposure, maxExposureByPlayer: exposure };
}

export function sameGenerationSettings(a: NflOptimizerSettings, b: NflOptimizerSettings): boolean {
  const normalize = (s: NflOptimizerSettings) => ({ ...s,
    lockedPlayerIds: [...s.lockedPlayerIds].sort((a,b) => a-b),
    excludedPlayerIds: [...s.excludedPlayerIds].sort((a,b) => a-b),
  });
  return canonicalAuditJson(normalize(a)) === canonicalAuditJson(normalize(b));
}
