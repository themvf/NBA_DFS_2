import type { NflGeneratedLineup, NflOptimizerPlayer, NflOptimizerSettings } from '@/app/dfs/nfl/nfl-optimizer';

export const DIVERSIFIED_VERSION = 'nfl-classic-winners-diversified-v3';
/** Risk preferences, not fitted performance claims. Shares are 0..1. */
export const DEFAULT_DIVERSIFIED = {
  qbMax: .20, rbMax: .55, wrMax: .40, teMax: .45, dstMax: .25,
  pairMax: .25, tripleMax: .15, playerDstMax: .20,
  softStart: .25, softPenalty: .5, minimumQbs: 0, candidateTarget: 500,
  qbDoubleStackShare: .40, bringBackShare: .50, twoTeShare: .30, maxFourWrShare: .35, maxOtherGameStackShare: .35,
};
export type DiversifiedPolicy = typeof DEFAULT_DIVERSIFIED;
/** User-selected concentration preferences; not a fitted correlation model. */
export const MORE_DIVERSITY = {
  qbMax: .12, rbMax: .35, wrMax: .35, teMax: .35, dstMax: .20,
  pairMax: .18, tripleMax: .10, playerDstMax: .12, softStart: .20, softPenalty: 1,
};
export function withDiversityPreset(policy: Partial<DiversifiedPolicy> | undefined, more: boolean): DiversifiedPolicy {
  const caps = more ? MORE_DIVERSITY : Object.fromEntries(
    Object.keys(MORE_DIVERSITY).map(key => [key, DEFAULT_DIVERSIFIED[key as keyof DiversifiedPolicy]]));
  // Composition, candidate target and an explicit QB minimum survive preset changes.
  return { ...DEFAULT_DIVERSIFIED, ...policy, ...caps };
}
export function matchesDiversityPreset(policy: Partial<DiversifiedPolicy> | undefined, more: boolean): boolean {
  const resolved = { ...DEFAULT_DIVERSIFIED, ...policy }, preset = withDiversityPreset(policy, more);
  return Object.keys(MORE_DIVERSITY).every(key => resolved[key as keyof DiversifiedPolicy] === preset[key as keyof DiversifiedPolicy]);
}

export const diversifiedCount = (share: number, n: number) => Math.max(1, Math.floor(share * n + 1e-9));

export function diversifiedPolicy(settings: Pick<NflOptimizerSettings, 'diversified'>): DiversifiedPolicy {
  const policy = { ...DEFAULT_DIVERSIFIED, ...settings.diversified };
  for (const key of ['qbMax', 'rbMax', 'wrMax', 'teMax', 'dstMax', 'pairMax', 'tripleMax', 'playerDstMax', 'softStart'] as const)
    if (!Number.isFinite(policy[key]) || policy[key] <= 0 || policy[key] > 1) throw new Error(`Invalid Diversified ${key}: use a share above 0 and at most 1.`);
  for (const key of ['qbDoubleStackShare', 'bringBackShare', 'twoTeShare', 'maxFourWrShare', 'maxOtherGameStackShare'] as const)
    if (!Number.isFinite(policy[key]) || policy[key] < 0 || policy[key] > 1) throw new Error(`Invalid Winners + Diversified ${key}: use a share from 0 to 1.`);
  if (!Number.isFinite(policy.softPenalty) || policy.softPenalty < 0 || policy.softPenalty > 5) throw new Error('Diversified soft penalty must be 0–5.');
  if (!Number.isInteger(policy.minimumQbs) || policy.minimumQbs < 0 || policy.minimumQbs > 32) throw new Error('Diversified minimum QBs must be 0–32.');
  if (!Number.isInteger(policy.candidateTarget) || policy.candidateTarget < 50 || policy.candidateTarget > 1000) throw new Error('Diversified candidate target must be 50–1000.');
  return policy;
}

/** Used by both server assessment and optimizer, so persisted settings describe the build. */
export function applyDiversified(settings: NflOptimizerSettings): NflOptimizerSettings {
  if (settings.buildShape !== 'diversified') return settings;
  if (settings.format !== 'classic' || settings.mode !== 'gpp') throw new Error('Diversified construction is available for Classic GPP only.');
  const policy = diversifiedPolicy(settings);
  if (policy.minimumQbs > settings.nLineups) throw new Error('Minimum QBs cannot exceed the lineup count.');
  if (settings.archetypeQuotas?.some(q => q.enabled)) throw new Error('Diversified uses standard Classic candidates. Clear custom archetype quotas first.');
  return { ...settings, diversified: policy, ...(settings.salaryPolicy ? {salaryPolicy:{...settings.salaryPolicy,minSalaryUsed:Math.max(48_100,settings.salaryPolicy.minSalaryUsed)}} : {}), minUnique: Math.max(4, settings.minUnique),
    minSalary: Math.max(48_100, settings.minSalary), stackPassCatchers: Math.max(1, settings.stackPassCatchers) as 1 | 2,
    maxDstVsOffenseShare: 0, maxQbExposure: policy.qbMax,
    // The new profile is ownership neutral, including restored drafts and callers outside the page.
    useHeuristicOwnershipLeverage: false, ownershipLeverageEnabled: false, leverageExponent: 0,
    bringBack: false, bringBackShare: policy.bringBackShare, qbDoubleStackShare: policy.qbDoubleStackShare,
    twoTeShare: policy.twoTeShare, maxFourWrShare: policy.maxFourWrShare, maxOtherGameStackShare: policy.maxOtherGameStackShare,
    portfolioSelection: 'sequential', diversifiedCandidateSearch: false, diversifiedMeanSearch: false,
  };
}

export function diversifiedPositionMax(position: string, policy: DiversifiedPolicy): number {
  return ({ QB: policy.qbMax, RB: policy.rbMax, WR: policy.wrMax, TE: policy.teMax, DST: policy.dstMax } as Record<string, number>)[position] ?? 1;
}

type Player = Pick<NflOptimizerPlayer, 'dkPlayerId' | 'position' | 'team' | 'name'>;
type Roster = { slots: Array<{ player: Player }> };
export type RepeatedGroup = { key: string; ids: number[]; kind: 'pair' | 'triple' | 'playerDst'; exemptTeam: string | null };

/** Primary stack = the QB and his own WR/TE. RBs are counted as ordinary combinations. */
export function diversifiedGroups(lineup: Roster): RepeatedGroup[] {
  const players = lineup.slots.map(s => s.player).sort((a, b) => a.dkPlayerId - b.dkPlayerId);
  const qb = players.find(p => p.position === 'QB');
  const result: RepeatedGroup[] = [];
  const add = (group: Player[]) => {
    if (qb && group.every(p => p.team === qb.team && ['QB', 'WR', 'TE'].includes(p.position))) return;
    const kind = group.length === 3 ? 'triple' : group.some(p => p.position === 'DST') ? 'playerDst' : 'pair';
    const exemptTeam = group.every(p => p.team === group[0].team && ['QB', 'WR', 'TE'].includes(p.position)) ? group[0].team : null;
    result.push({ key: `${kind}_${group.map(p => p.dkPlayerId).join('_')}`, ids: group.map(p => p.dkPlayerId), kind, exemptTeam });
  };
  for (let a = 0; a < players.length; a++) for (let b = a + 1; b < players.length; b++) {
    add([players[a], players[b]]);
    for (let c = b + 1; c < players.length; c++) add([players[a], players[b], players[c]]);
  }
  return result;
}

export function groupLimit(group: RepeatedGroup, policy: DiversifiedPolicy, n: number): number {
  return diversifiedCount(group.kind === 'triple' ? policy.tripleMax : group.kind === 'playerDst'
    ? Math.min(policy.pairMax, policy.playerDstMax) : policy.pairMax, n);
}

export function repeatedGroupCounts(lineups: Roster[]): Map<string, { group: RepeatedGroup; count: number }> {
  const counts = new Map<string, { group: RepeatedGroup; count: number }>();
  for (const lineup of lineups) for (const group of diversifiedGroups(lineup))
    counts.set(group.key, { group, count: (counts.get(group.key)?.count ?? 0) + 1 });
  return counts;
}

/** Marginal quadratic cost after the soft threshold; normalized to a 50-entry portfolio. */
export function concentrationCost(count: number, settings: NflOptimizerSettings): number {
  const p = diversifiedPolicy(settings), start = Math.floor(p.softStart * settings.nLineups);
  const excess = Math.max(0, count - start);
  return .5 * p.softPenalty * 50 / settings.nLineups * excess * excess;
}

export type DiversifiedReport = {
  version: string; candidateTarget: number; candidates: number; attempts: number; candidateKeys: string[];
  stopReason: 'target' | 'attempt_limit' | 'time_limit'; swaps: number;
  requested: number; generated: number; minimumQbs: number; distinctQbs: number;
  composition: ReturnType<typeof diversifiedComposition>;
  meanProjection: number; playersAt40Pct: number;
  repeated: Array<{ names: string[]; kind: string; count: number; limit: number }>;
  caps: Array<{ name: string; position: string; count: number; limit: number; override: boolean }>;
};

/** A construction search only. No scenario, finish-probability, or profitability claims. */
export function improveDiversifiedPortfolio(input: {
  initial: NflGeneratedLineup[]; settings: NflOptimizerSettings;
  generate: (attempt: number, known: NflGeneratedLineup[]) => NflGeneratedLineup | null;
  score: (lineup: NflGeneratedLineup) => number;
  valid: (lineups: NflGeneratedLineup[]) => boolean;
}): { lineups: NflGeneratedLineup[]; candidates: number; attempts: number; candidateKeys: string[]; stopReason: DiversifiedReport['stopReason']; swaps: number } {
  const { settings } = input, policy = diversifiedPolicy(settings);
  const key = (l: NflGeneratedLineup) => [...l.playerIds].sort((a,b) => a-b).join(',');
  const candidates = new Map(input.initial.map(l => [key(l), l]));
  const target = Math.max(settings.nLineups, policy.candidateTarget), start = Date.now();
  let attempts = 0;
  // The bank is frozen in the report by its input/algorithm and recorded stopping point.
  while (candidates.size < target && attempts < target * 2 && Date.now() - start < 25_000) {
    const candidate = input.generate(attempts++, [...candidates.values()]);
    if (candidate) candidates.set(key(candidate), candidate);
  }
  const stopReason = candidates.size >= target ? 'target' : attempts >= target * 2 ? 'attempt_limit' : 'time_limit';
  let chosen = [...input.initial], swaps = 0;
  if (chosen.length !== settings.nLineups || !input.valid(chosen)) return { lineups: chosen, candidates: candidates.size, attempts, candidateKeys:[...candidates.keys()], stopReason, swaps };
  const value = (ls: NflGeneratedLineup[]) => {
    const counts = new Map<number, number>();
    for (const l of ls) for (const id of l.playerIds) counts.set(id, (counts.get(id) ?? 0) + 1);
    return ls.reduce((s,l) => s + input.score(l), 0) - [...counts.values()].reduce((s,c) => s + concentrationCost(c, settings), 0);
  };
  let best = value(chosen), evaluations = 0;
  const ranked = [...candidates.values()].sort((a,b) => input.score(b)-input.score(a) || key(a).localeCompare(key(b)));
  // Bounded single-roster swaps keep a complete feasible incumbent throughout.
  for (let pass = 0; pass < 2; pass++) {
    let changed = false;
    for (const candidate of ranked) {
      if (chosen.some(l => key(l) === key(candidate))) continue;
      const slots = chosen.map((l,i) => ({ i, score: input.score(l) })).sort((a,b) => a.score-b.score || a.i-b.i);
      for (const { i } of slots) {
        if (++evaluations > 12_000) break;
        const proposed = [...chosen]; proposed[i] = candidate;
        const v = value(proposed);
        if (v <= best + 1e-6 || !input.valid(proposed)) continue;
        chosen = proposed; best = v; swaps++; changed = true; break;
      }
      if (evaluations > 12_000) break;
    }
    if (!changed || evaluations > 12_000) break;
  }
  return { lineups: chosen.map((l,i) => ({ ...l, lineupNumber: i+1 })), candidates: candidates.size, attempts, candidateKeys:[...candidates.keys()], stopReason, swaps };
}

/** Recount actual rosters: selection must preserve the requested composition. */
export function diversifiedComposition(lineups: NflGeneratedLineup[]) {
  const counts = { doubleStacks: 0, bringBacks: 0, twoTe: 0, fourWr: 0, otherGameStacks: 0 };
  for (const lineup of lineups) {
    const players = lineup.slots.map(s => s.player), qb = players.find(p => p.position === 'QB');
    if (!qb) continue;
    if (players.filter(p => p.team === qb.team && ['WR','TE'].includes(p.position)).length >= 2) counts.doubleStacks++;
    if (players.some(p => p.team === qb.opponent && ['RB','WR','TE'].includes(p.position))) counts.bringBacks++;
    if (players.filter(p => p.position === 'TE').length >= 2) counts.twoTe++;
    if (players.filter(p => p.position === 'WR').length >= 4) counts.fourWr++;
    const teams = new Map<string, number>();
    for (const p of players) if (['RB','WR','TE'].includes(p.position) && p.team !== qb.team && p.team !== qb.opponent)
      teams.set(p.team, (teams.get(p.team) ?? 0) + 1);
    if ([...teams.values()].some(count => count >= 2)) counts.otherGameStacks++;
  }
  return counts;
}
export function diversifiedCompositionValid(lineups: NflGeneratedLineup[], settings: NflOptimizerSettings) {
  const c = diversifiedComposition(lineups), p = diversifiedPolicy(settings), n = settings.nLineups;
  return c.doubleStacks === Math.round(n*p.qbDoubleStackShare) && c.bringBacks === Math.round(n*p.bringBackShare)
    && c.twoTe === Math.round(n*p.twoTeShare) && c.fourWr <= Math.floor(n*p.maxFourWrShare+1e-9)
    && c.otherGameStacks <= Math.floor(n*p.maxOtherGameStackShare+1e-9);
}
