import type { NflOptimizerSettings } from '@/app/dfs/nfl/nfl-optimizer';
import type { PlayerExposurePolicy } from './exposure-plan';
import { canonicalAuditJson } from './audit-json';
import type { ArchetypeQuota } from './archetypes';

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

export type ArchetypePlanMode = 'balanced' | 'standard' | 'custom' | 'chalk_leverage';

/** Everything the build form edits, in the shape the form holds it. */
export interface NflBuildForm<S extends object> {
  settings: S;
  locked: number[];
  excluded: number[];
  /** Overall exposure targets, in PERCENT. */
  targets: Record<string, number>;
  captainTargets: Record<string, CaptainTarget>;
  planMode: ArchetypePlanMode;
  quotas: ArchetypeQuota[];
  favorite: string;
  fades: number[];
}

type FormBase = Omit<NflOptimizerSettings, 'format' | 'lockedPlayerIds' | 'excludedPlayerIds' | 'minExposureByPlayer' | 'maxExposureByPlayer'>;

/** The optimizer settings a form produces. The single definition of that mapping. */
export function settingsFromForm<S extends FormBase>(form: NflBuildForm<S>, format: NflOptimizerSettings['format'], teams: readonly string[]): NflOptimizerSettings {
  const custom = form.planMode === 'custom';
  const underdog = form.favorite ? teams.find((team) => team !== form.favorite) ?? null : null;
  const fades = form.fades;
  return { ...generationSettings(form.settings, format, form.locked, form.excluded, form.targets, form.captainTargets),
    archetypeMode: form.planMode,
    archetypeQuotas: custom && form.quotas.length ? form.quotas : undefined,
    favoriteTeam: custom && form.favorite ? form.favorite : undefined,
    underdogTeam: custom && form.favorite ? underdog ?? undefined : undefined,
    archetypeConfigs: custom && fades.length
      ? { single_chalk_fade: { fadePlayerIds: fades.slice(0, 1) }, double_fade: { fadePlayerIds: fades.slice(0, 2) } } : undefined };
}

/**
 * The form that would rebuild a saved run: the inverse of settingsFromForm.
 *
 * Only keys the form already has are read, so fields the server adds when it
 * saves (ownership disclosure, a resolved Vegas favorite) never leak into the
 * form, and a field an older run predates keeps the form's current value.
 * Exposure targets come back as the lineup-count-rounded percentage that was
 * actually applied, which reproduces the same run.
 */
export function formFromSettings<S extends object>(saved: NflOptimizerSettings, defaults: S): NflBuildForm<S> {
  const settings = { ...defaults };
  const source = saved as unknown as Record<string, unknown>;
  for (const key of Object.keys(defaults) as Array<keyof S & string>) {
    if (source[key] !== undefined) (settings as Record<string, unknown>)[key] = source[key];
  }
  const percent = (value: number) => Math.round(value * 10000) / 100;
  const targets: Record<string, number> = {};
  for (const [id, min] of Object.entries(saved.minExposureByPlayer ?? {})) {
    if (min != null && saved.maxExposureByPlayer?.[id] === min) targets[id] = percent(min);
  }
  const captainTargets: Record<string, CaptainTarget> = {};
  for (const policy of saved.exposurePolicies ?? []) {
    const min = policy.captain?.minPct ?? null, max = policy.captain?.maxPct ?? null;
    if (min == null && max == null) continue;
    captainTargets[String(policy.playerId)] = { min: min == null ? null : percent(min), max: max == null ? null : percent(max) };
  }
  const planMode = (saved.archetypeMode ?? 'standard') as ArchetypePlanMode;
  const custom = planMode === 'custom';
  const configs = saved.archetypeConfigs ?? {};
  const fades = custom ? [...new Set([...(configs.double_fade?.fadePlayerIds ?? []), ...(configs.single_chalk_fade?.fadePlayerIds ?? [])])] : [];
  return {
    settings, locked: [...(saved.lockedPlayerIds ?? [])], excluded: [...(saved.excludedPlayerIds ?? [])],
    targets, captainTargets, planMode,
    quotas: custom ? [...(saved.archetypeQuotas ?? [])] : [],
    favorite: custom ? saved.favoriteTeam ?? '' : '',
    fades,
  };
}
