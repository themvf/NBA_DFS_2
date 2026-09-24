import type { NflOptimizerSettings } from '@/app/dfs/nfl/nfl-optimizer';
import type { PlayerExposurePolicy } from './exposure-plan';
import { canonicalAuditJson } from './audit-json';

/** A Showdown captain range for one player, in PERCENT (0-100). Null = no bound. */
export type CaptainTarget = { min: number | null; max: number | null };

/**
 * Turn captain ranges into per-player exposure policies.
 *
 * A per-player policy REPLACES that player's flat max exposure (the optimizer
 * resolves an explicit policy first). Setting only a captain range therefore
 * used to lift his overall cap entirely -- in the Thursday ATL@GB test a
 * captain range on Tucker Kraft took him from the 60% default to 94-100% of
 * lineups. So the overall bound is carried across explicitly: his own overall
 * target when one is set, otherwise the global max exposure.
 */
export function captainExposurePolicies(
  captainTargets: Record<string, CaptainTarget>,
  overallTargets: Record<string, number>,
  maxExposure: number,
): PlayerExposurePolicy[] {
  const pct = (value: number | null) => (value == null || !Number.isFinite(value) ? null : Math.max(0, Math.min(100, value)) / 100);
  return Object.entries(captainTargets).flatMap(([id, target]) => {
    const min = pct(target.min), max = pct(target.max);
    if (min == null && max == null) return [];
    const overall = overallTargets[id];
    return [{
      playerId: Number(id),
      overall: overall != null ? { minPct: overall, maxPct: overall } : { minPct: null, maxPct: maxExposure },
      captain: { minPct: min, maxPct: max },
      flex: { minPct: null, maxPct: null },
      exactTargetMode: false,
    }];
  });
}

export function generationSettings(
  settings: Omit<NflOptimizerSettings, 'format' | 'lockedPlayerIds' | 'excludedPlayerIds' | 'minExposureByPlayer' | 'maxExposureByPlayer'>,
  format: NflOptimizerSettings['format'], locked: number[], excluded: number[], targets: Record<string, number>,
  captainTargets: Record<string, CaptainTarget> = {},
): NflOptimizerSettings {
  const exposure = Object.fromEntries(Object.entries(targets).map(([id, percent]) => {
    const n = Math.max(1, settings.nLineups);
    return [id, Math.max(0, Math.min(n, Math.round(percent / 100 * n))) / n];
  }));
  // Captain ranges only mean something on Showdown, where a captain exists.
  const policies = format === 'showdown' ? captainExposurePolicies(captainTargets, exposure, settings.maxExposure) : [];
  return { ...settings, format, lockedPlayerIds: locked, excludedPlayerIds: excluded,
    minExposureByPlayer: exposure, maxExposureByPlayer: exposure,
    ...(policies.length ? { exposurePolicies: policies } : {}) };
}

export function sameGenerationSettings(a: NflOptimizerSettings, b: NflOptimizerSettings): boolean {
  const normalize = (s: NflOptimizerSettings) => ({ ...s,
    lockedPlayerIds: [...s.lockedPlayerIds].sort((a,b) => a-b),
    excludedPlayerIds: [...s.excludedPlayerIds].sort((a,b) => a-b),
  });
  return canonicalAuditJson(normalize(a)) === canonicalAuditJson(normalize(b));
}
