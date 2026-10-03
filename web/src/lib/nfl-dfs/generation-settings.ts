import type { NflOptimizerSettings } from '@/app/dfs/nfl/nfl-optimizer';
import type { PlayerExposurePolicy } from './exposure-plan';
import { canonicalAuditJson } from './audit-json';
import type { ArchetypeQuota } from './archetypes';
import { defensiveSettingsFor } from './defensive-display';

/** A Showdown captain range for one player, in PERCENT (0-100). Null = no bound. */
export type CaptainTarget = { min: number | null; max: number | null };

/**
 * A player's overall exposure range, in PERCENT. A max alone is a cap; min ==
 * max is an exact target. A bare number (older saved forms) means an exact
 * target.
 */
export type ExposureTarget = CaptainTarget;

export function exposureRange(value: ExposureTarget | number): ExposureTarget {
  return typeof value === "number" ? { min: value, max: value } : value;
}

/**
 * Percent ranges -> lineup-share bounds. An exact target rounds to the nearest
 * lineup count (unchanged behaviour); a cap never exceeds the typed percent
 * (floor) and a minimum never falls below it (ceil).
 */
export function exposureBounds(targets: Record<string, ExposureTarget | number>, nLineups: number): { min: Record<string, number>; max: Record<string, number> } {
  const n = Math.max(1, nLineups), min: Record<string, number> = {}, max: Record<string, number> = {};
  const clamp = (count: number) => Math.max(0, Math.min(n, count)) / n;
  for (const [id, raw] of Object.entries(targets)) {
    const { min: lo, max: hi } = exposureRange(raw);
    if (lo != null && hi != null && lo === hi) { min[id] = max[id] = clamp(Math.round(lo / 100 * n)); continue; }
    if (lo != null) min[id] = clamp(Math.ceil(lo / 100 * n - 1e-9));
    if (hi != null) max[id] = clamp(Math.floor(hi / 100 * n + 1e-9));
  }
  return { min, max };
}

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
  overall: { min: Record<string, number>; max: Record<string, number> },
  maxExposure: number,
): PlayerExposurePolicy[] {
  const pct = (value: number | null) => (value == null || !Number.isFinite(value) ? null : Math.max(0, Math.min(100, value)) / 100);
  return Object.entries(captainTargets).flatMap(([id, target]) => {
    const min = pct(target.min), max = pct(target.max);
    if (min == null && max == null) return [];
    const userMin = overall.min[id] ?? null, userMax = overall.max[id] ?? null;
    const fromUser = userMin != null || userMax != null;
    return [{
      playerId: Number(id),
      overall: fromUser ? { minPct: userMin, maxPct: userMax ?? maxExposure } : { minPct: null, maxPct: maxExposure },
      captain: { minPct: min, maxPct: max },
      flex: { minPct: null, maxPct: null },
      exactTargetMode: false,
      overallFromUser: fromUser,
    }];
  });
}

export function generationSettings(
  settings: Omit<NflOptimizerSettings, 'format' | 'lockedPlayerIds' | 'excludedPlayerIds' | 'minExposureByPlayer' | 'maxExposureByPlayer'>,
  format: NflOptimizerSettings['format'], locked: number[], excluded: number[], targets: Record<string, ExposureTarget | number>,
  captainTargets: Record<string, CaptainTarget> = {},
): NflOptimizerSettings {
  const bounds = exposureBounds(targets, settings.nLineups);
  // Captain ranges only mean something on Showdown, where a captain exists.
  const policies = format === 'showdown' ? captainExposurePolicies(captainTargets, bounds, settings.maxExposure) : [];
  return { ...settings, format, lockedPlayerIds: locked, excludedPlayerIds: excluded,
    gppSignalMinPerLineup: format === 'classic' && settings.mode === 'gpp' ? settings.gppSignalMinPerLineup ?? 0 : 0,
    gppAirMatchupMinPct: format === 'classic' && settings.mode === 'gpp' ? settings.gppAirMatchupMinPct ?? 0 : 0,
    minExposureByPlayer: bounds.min, maxExposureByPlayer: bounds.max,
    ...(policies.length ? { exposurePolicies: policies } : {}) };
}

export function sameGenerationSettings(a: NflOptimizerSettings, b: NflOptimizerSettings): boolean {
  const normalize = (s: NflOptimizerSettings) => ({ ...s, runEvidence:undefined,ownershipDisclosure:undefined,ownershipCapability:undefined,
    gppAirMatchupMinPct:s.gppAirMatchupMinPct??0,
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
  /** Overall exposure ranges, in PERCENT (a max alone is a cap; a bare number is an exact target). */
  targets: Record<string, ExposureTarget | number>;
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
  const defensive = form.settings.defensiveAdjustments;
  const settings = defensive ? { ...form.settings, defensiveAdjustments: defensiveSettingsFor(form.settings.projectionSource, defensive) } : form.settings;
  return { ...generationSettings(settings, format, form.locked, form.excluded, form.targets, form.captainTargets),
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
  // Runs saved before defensive adjustments existed used the historical
  // forecast. Restoring one must not silently inherit today's new-build mode.
  if (source.defensiveAdjustments === undefined && 'defensiveAdjustments' in defaults) {
    const fallback = (defaults as { defensiveAdjustments: { profile: 'pfr-efficiency' | 'allowed-rushing-volume' } }).defensiveAdjustments;
    (settings as Record<string, unknown>).defensiveAdjustments = { mode: 'off', profile: fallback.profile };
  }
  // The form shows what the user ASKED for. When the server could not honour a
  // choice (an "approved" defensive profile with no approved policy runs as
  // off), comparing the form with the effective setting reported "Settings
  // changed" right after Generate (PHI@CHI 2026-09-28). The downgrade is
  // disclosed separately instead.
  if (source.requestedDefensiveAdjustments && 'defensiveAdjustments' in defaults) {
    (settings as Record<string, unknown>).defensiveAdjustments = source.requestedDefensiveAdjustments;
  }
  const percent = (value: number) => Math.round(value * 10000) / 100;
  const targets: Record<string, ExposureTarget> = {};
  for (const id of new Set([...Object.keys(saved.minExposureByPlayer ?? {}), ...Object.keys(saved.maxExposureByPlayer ?? {})])) {
    const lo = saved.minExposureByPlayer?.[id], hi = saved.maxExposureByPlayer?.[id];
    if (lo == null && hi == null) continue;
    targets[id] = { min: lo == null ? null : percent(lo), max: hi == null ? null : percent(hi) };
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

const PLAN_MODES: readonly ArchetypePlanMode[] = ['balanced', 'standard', 'custom', 'chalk_leverage'];

/**
 * A build form saved on the server (a draft) rebuilt against the current form
 * defaults. Only keys the defaults know are taken, with the default's type, so
 * a draft written by an older page cannot inject settings the form no longer
 * has or crash it with a wrong shape.
 */
export function formFromDraft<S extends object>(raw: unknown, defaults: S): NflBuildForm<S> | null {
  if (!raw || typeof raw !== 'object') return null;
  const draft = raw as Record<string, unknown>;
  const saved = (draft.settings && typeof draft.settings === 'object' ? draft.settings : {}) as Record<string, unknown>;
  const settings = { ...defaults };
  for (const key of Object.keys(defaults) as Array<keyof S & string>) {
    const value = saved[key], fallback = (defaults as Record<string, unknown>)[key];
    if (value === undefined || value === null) continue;
    const sameKind = Array.isArray(fallback) ? Array.isArray(value) : typeof value === typeof fallback;
    if (sameKind) (settings as Record<string, unknown>)[key] = value;
  }
  const ids = (value: unknown) => Array.isArray(value) ? value.filter((v): v is number => Number.isSafeInteger(v)) : [];
  const ranges = (value: unknown): Record<string, CaptainTarget> => {
    if (!value || typeof value !== 'object') return {};
    const out: Record<string, CaptainTarget> = {};
    for (const [id, range] of Object.entries(value as Record<string, unknown>)) {
      if (typeof range === 'number') { out[id] = { min: range, max: range }; continue; }
      if (!range || typeof range !== 'object') continue;
      const r = range as { min?: unknown; max?: unknown };
      const num = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
      if (num(r.min) != null || num(r.max) != null) out[id] = { min: num(r.min), max: num(r.max) };
    }
    return out;
  };
  const planMode = PLAN_MODES.includes(draft.planMode as ArchetypePlanMode) ? draft.planMode as ArchetypePlanMode : 'balanced';
  return {
    settings, locked: ids(draft.locked), excluded: ids(draft.excluded),
    targets: ranges(draft.targets), captainTargets: ranges(draft.captainTargets), planMode,
    quotas: Array.isArray(draft.quotas) ? draft.quotas as ArchetypeQuota[] : [],
    favorite: typeof draft.favorite === 'string' ? draft.favorite : '',
    fades: ids(draft.fades),
  };
}
