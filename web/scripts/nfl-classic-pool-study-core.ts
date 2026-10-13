/** Research-only adapter. Production optimizer and presets remain unchanged. */
import { createHash } from 'node:crypto';
import { optimizeNflLineups, NFL_OPTIMIZER_VERSION, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { applyWinnersShape, WINNERS_SHAPE_VERSION } from '../src/lib/nfl-dfs/winners-shape';
import { canonicalAuditJson } from '../src/lib/nfl-dfs/audit-json';
import { buildCompletion } from '../src/lib/nfl-dfs/build-completion';

// Frozen database JSON uses aliases, and preserves fields beyond the optimizer type.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Raw = Record<string, any>;
export const STUDY = 'nfl-classic-te-wr-pool-v1';
export const digest = (value: unknown) => createHash('sha256').update(canonicalAuditJson(value)).digest('hex');

export function restorePlayers(snapshot: Raw[]): NflOptimizerPlayer[] {
  return snapshot.map(p => {
    const own = typeof p.ownership === 'object' && p.ownership !== null ? p.ownership : null;
    for (const key of ['dkPlayerId', 'salary', 'ourProj']) {
      if (!(key in p)) throw new Error(`Incomplete frozen player ${p.name}: missing ${key}`);
    }
    if (!p.gameKey || !p.position || !p.projectionStatus || typeof p.isOut !== 'boolean') {
      throw new Error(`Incomplete identity/eligibility for ${p.name}`);
    }
    return { ...p, id: p.id ?? p.dkPlayerId, captainDkPlayerId: p.captainDkPlayerId ?? null,
      captainSalary: p.captainSalary ?? null, dkStatus: p.status ?? null,
      floorFpts: p.floor ?? null, ceilingFpts: p.ceiling ?? null,
      avgFptsDk: p.dkAvg ?? null, fantasyprosProj: p.fantasypros ?? null,
      linestarProj: p.linestar ?? null, customProj: p.custom ?? null,
      linestarOwnPct: own?.linestar ?? (typeof p.ownership === 'number' ? p.ownership : null),
      ownPct: own?.total ?? (typeof p.ownership === 'number' ? p.ownership : null),
      ownSource: own?.source ?? null, captainOwnPct: own?.captain ?? null, flexOwnPct: own?.flex ?? null,
    } as NflOptimizerPlayer;
  });
}

export function buildPair(input: Raw) {
  const source = input.source;
  const signed: Raw = { settings: source.settings, inputSnapshot: source.input_snapshot, optimizerVersion: source.optimizer_version };
  if (source.build_info != null) signed.buildInfo = source.build_info;
  if (digest(signed) !== source.input_digest) throw new Error('Source run input digest mismatch');
  if (source.settings.researchStudy) throw new Error('A research arm cannot be the source build');
  if (source.settings.format !== 'classic' || source.settings.mode !== 'gpp') throw new Error('Classic GPP source required');
  if (!Number.isInteger(source.settings.nLineups) || source.settings.nLineups < 1) throw new Error('Invalid source lineup count');
  const settings = structuredClone(source.settings) as NflOptimizerSettings & Raw;
  delete settings.runEvidence;
  const players = restorePlayers(input.players);
  if (new Set(players.map(p => p.dkPlayerId)).size !== players.length) throw new Error('Duplicate frozen player IDs');
  // No unavailable legacy-field fallbacks or adjustment changes. Workload runs
  // were filtered before optimization; honor that frozen flag, never new data.
  const eligible = settings.projectionSource === 'workload'
    ? players.filter(p => input.players.find((r: Raw) => r.dkPlayerId === p.dkPlayerId)?.workloadEligible === true) : players;
  const nativeSettings = { ...settings, buildShape: 'winners' as const };
  const shape = applyWinnersShape(nativeSettings, eligible);
  const recordedEligibility = source.settings.runEvidence?.eligibility;
  let sourceShapeVerified = false;
  if (source.settings.buildShape === 'winners' && Array.isArray(recordedEligibility)) {
    const recorded = recordedEligibility.filter((p: Raw) => p.reasonCode === 'WINNERS_SHAPE')
      .map((p: Raw) => p.dkPlayerId).sort((a: number, b: number) => a - b);
    const reconstructed = [...shape.excluded.keys()].sort((a, b) => a - b);
    if (digest(recorded) !== digest(reconstructed)) {
      throw new Error('Captured Winners exclusions differ from the source run; refresh/build a new source before enrollment');
    }
    sourceShapeVerified = true;
  }
  const byId = new Map(players.map(p => [p.dkPlayerId, p]));
  const removed = [...shape.excluded.keys()].filter(id => ['TE', 'WR'].includes(byId.get(id)!.position));
  const sharedSettings = { ...shape.settings, buildShape: 'standard' as const };
  const common = { version: STUDY, pairId: input.pairId, sourceRunId: source.run_id,
    season: input.season, week: input.week, slateSignature: input.slateSignature,
    firstKickoff: input.firstKickoff, capturedAt: input.capturedAt,
    inputPoolDigest: digest(input.players), sourceInputDigest: source.input_digest,
    implementation: input.implementation, winnersShapeVersion: WINNERS_SHAPE_VERSION,
    randomSeed: 20260902, mainSlateAttested: true, forwardEligibility: 'pending_postgame_flagship_coverage',
    priorOpportunityCapture: input.priorOpportunityCapture, sourceShapeVerified, removedRulePlayerIds: removed,
    commonSettings: sharedSettings, shapeNotes: shape.notes };
  const before = digest(input);
  const arms = (['A', 'B'] as const).map(arm => {
    const exclusions = [...shape.excluded.keys()].filter(id => arm === 'A' || !removed.includes(id));
    const effective = { ...sharedSettings, excludedPlayerIds: [...new Set([...settings.excludedPlayerIds, ...exclusions])] };
    let result: ReturnType<typeof optimizeNflLineups> | null = null;
    let error: string | null = null;
    try { result = optimizeNflLineups(structuredClone(eligible), structuredClone(effective)); }
    catch (e) { error = e instanceof Error ? e.message : String(e); }
    const lineups = result?.lineups ?? [];
    const completion = buildCompletion({ ...result, requestedLineups: settings.nLineups, generatedLineups: lineups.length });
    const researchStudy = { ...common, arm, label: arm === 'A' ? 'Current Winners' : 'Winners without TE/WR pool exclusions',
      completion: completion.status, issues: completion.issues, error, ruleExclusions: exclusions.map(id => ({
        dkPlayerId: id, position: byId.get(id)!.position, reason: shape.excluded.get(id),
      })) };
    const persistedSettings = { ...effective, researchStudy, runEvidence: {
      eligibility: result?.eligibility ?? [], exposureReport: result?.exposureReport ?? [],
      salaryBandReport: result?.salaryBandReport ?? [], duplication: result?.duplication ?? [],
      warnings: [...(result?.warnings ?? []), ...(error ? [error] : [])], archetypePlan: result?.archetypePlan ?? [],
    } };
    const buildInfo = { commitSha: input.implementation.commit, implementationDigest: input.implementation.digest,
      buildTime: input.capturedAt, settingsSchemaVersion: STUDY, optimizerVersion: NFL_OPTIMIZER_VERSION };
    return { arm, settings: persistedSettings, inputSnapshot: input.players, buildInfo,
      optimizerVersion: NFL_OPTIMIZER_VERSION,
      inputDigest: digest({ settings: persistedSettings, inputSnapshot: input.players, optimizerVersion: NFL_OPTIMIZER_VERSION, buildInfo }),
      status: `research_${completion.status}`, failureReason: error ?? (completion.issues.join(' ') || null),
      requestedLineups: settings.nLineups,
      lineups: lineups.map(l => ({ ...l, slots: l.slots.map(s => ({ slot: s.slot, dkPlayerId: s.player.dkPlayerId,
        captainDkPlayerId: s.player.captainDkPlayerId, name: s.player.name, team: s.player.team, salary: s.salary,
        projection: s.projection, source: s.projectionSource, multiplier: s.multiplier, projectionAudit: s.player.projectionAudit })),
        stackSummary: { ...l.stackSummary, archetype: l.archetype ?? null } })),
    };
  });
  if (digest(input) !== before) throw new Error('Research adapter mutated frozen inputs');
  return { version: STUDY, pairId: input.pairId, sourceRunId: source.run_id, arms };
}
