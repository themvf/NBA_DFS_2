import type { RedistributionResult } from './opportunity-redistribution';
import { zeroOutProjection, type ZeroableProjection } from './out-projection';

export type ProjectionScenario = 'baseline_simulation' | 'availability_estimate' | 'unavailable';

/** One scenario for pool, explanation and optimizer. No mean-stat re-scoring. */
export function resolveOpportunityProjection(
  base: ZeroableProjection & { medianFpts: number | null; statMeans: Record<string, number> },
  adjustment: RedistributionResult | undefined,
  upstream: { rule?: string | null; applied?: boolean | null } | null | undefined,
  isOut: boolean,
) {
  const estimated = Boolean(adjustment) || (upstream?.rule === 'inherits' && upstream.applied === true);
  const current = adjustment ? { ...base, ourProj: adjustment.ourProj,
    floorFpts: adjustment.floorFpts, ceilingFpts: adjustment.ceilingFpts,
    statMeans: adjustment.statMeans } : base;
  return {
    ...zeroOutProjection(current, isOut),
    floorFpts: isOut ? 0 : estimated ? null : current.floorFpts,
    medianFpts: isOut ? 0 : estimated ? null : base.medianFpts,
    ceilingFpts: isOut ? 0 : estimated ? null : current.ceilingFpts,
    boomRate: isOut ? 0 : estimated ? null : base.boomRate,
    statMeans: isOut ? {} : Object.fromEntries(Object.entries(current.statMeans)
      .filter(([, value]) => typeof value === 'number' && Number.isFinite(value))),
    projectionScenario: (isOut ? 'unavailable' : estimated ? 'availability_estimate' : 'baseline_simulation') as ProjectionScenario,
  };
}
