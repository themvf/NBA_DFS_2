/** Research-only adapter. Production optimizer and presets remain unchanged. */
import { createHash } from 'node:crypto';
import { optimizeNflLineups, NFL_OPTIMIZER_VERSION, type NflOptimizerPlayer, type NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
import { applyWinnersShape, WINNERS_SHAPE_VERSION } from '../src/lib/nfl-dfs/winners-shape';
import { canonicalAuditJson } from '../src/lib/nfl-dfs/audit-json';
import { buildCompletion } from '../src/lib/nfl-dfs/build-completion';
import { drawJointBank, sampleField, fieldThresholds, fieldThresholdsMany, summarizeFieldThresholds, SUMMARY_FRACTIONS,
  scoreLineup, selectPortfolio, forecastSuccess,
  slateFromSnapshot, rosterOf, PORTFOLIO_OBJECTIVE, PORTFOLIO_OBJECTIVE_VERSION, JOINT_COPULA_VERSION, FIELD_MODEL_VERSION } from '../src/lib/nfl-dfs/portfolio-objective';
import { validateNflLineup } from '../src/lib/nfl-dfs/lineups';

// Frozen database JSON uses aliases, and preserves fields beyond the optimizer type.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Raw = Record<string, any>;
/** One entry per pre-registered study; the runner's STUDIES registry must match. */
export const STUDIES = { classic: 'nfl-classic-te-wr-pool-v1', showdown: 'nfl-showdown-kicker-rule-v1',
  portfolio: 'nfl-portfolio-objective-v3' } as const;
export const STUDY = STUDIES.classic;
type StudyKey = keyof typeof STUDIES;
function studyOf(input: Raw): StudyKey {
  if (input.version == null) return 'classic';
  const key = (Object.keys(STUDIES) as StudyKey[]).find(k => STUDIES[k] === input.version);
  if (!key) throw new Error(`Unknown study version ${input.version}`);
  return key;
}
export const digest = (value: unknown) => createHash('sha256').update(canonicalAuditJson(value)).digest('hex');
export type PairResult = { version: string; pairId: string; sourceRunId: string; arms: Raw[] };

export { restorePlayers } from '../src/lib/nfl-dfs/frozen-players';
import { restorePlayers } from '../src/lib/nfl-dfs/frozen-players';

export function buildPair(input: Raw): PairResult {
  const source = input.source;
  const signed: Raw = { settings: source.settings, inputSnapshot: source.input_snapshot, optimizerVersion: source.optimizer_version };
  if (source.build_info != null) signed.buildInfo = source.build_info;
  if (digest(signed) !== source.input_digest) throw new Error('Source run input digest mismatch');
  if (source.settings.researchStudy) throw new Error('A research arm cannot be the source build');
  const study = studyOf(input);
  const version = STUDIES[study];
  if (study === 'portfolio') {
    if (!['classic', 'showdown'].includes(source.settings.format) || source.settings.mode !== 'gpp') throw new Error('Classic or Showdown GPP source required');
    return buildPortfolioPair(input, version);
  }
  if (source.settings.format !== study || source.settings.mode !== 'gpp') {
    throw new Error(`${study === 'classic' ? 'Classic' : 'Showdown'} GPP source required`);
  }
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
  // The study's arm A is Winners WITH its pool rules (the v1-v10 construction);
  // user builds dropped them in winners-shape v11, so the rules are asked for here.
  const shape = applyWinnersShape(nativeSettings, eligible, { poolRules: true });
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
  const manual = new Set<number>(settings.excludedPlayerIds);
  // Classic: arm B admits the players the TE and WR pool rules exclude.
  // Showdown: the shape has no pool rules; arm B admits the kickers it bans.
  const removed = study === 'classic'
    ? [...shape.excluded.keys()].filter(id => ['TE', 'WR'].includes(byId.get(id)!.position))
    : eligible.filter(p => p.position === 'K' && !manual.has(p.dkPlayerId)).map(p => p.dkPlayerId);
  if (study === 'showdown' && shape.settings.excludeKickers !== true) throw new Error('Winners shape no longer bans kickers; the registered contrast is undefined');
  const sharedSettings = { ...shape.settings, buildShape: 'standard' as const };
  const common = { version, pairId: input.pairId, sourceRunId: source.run_id,
    season: input.season, week: input.week, slateSignature: input.slateSignature, gameIds: input.gameIds ?? null,
    firstKickoff: input.firstKickoff, capturedAt: input.capturedAt,
    inputPoolDigest: digest(input.players), sourceInputDigest: source.input_digest,
    implementation: input.implementation, winnersShapeVersion: WINNERS_SHAPE_VERSION,
    randomSeed: 20260902, mainSlateAttested: true, forwardEligibility: 'pending_postgame_flagship_coverage',
    priorOpportunityCapture: input.priorOpportunityCapture, sourceShapeVerified, removedRulePlayerIds: removed,
    commonSettings: sharedSettings, shapeNotes: shape.notes };
  const before = digest(input);
  const arms = (['A', 'B'] as const).map(arm => {
    const exclusions = study === 'classic'
      ? [...shape.excluded.keys()].filter(id => arm === 'A' || !removed.includes(id))
      : [...shape.excluded.keys()];
    const effective = { ...sharedSettings, excludedPlayerIds: [...new Set([...settings.excludedPlayerIds, ...exclusions])],
      ...(study === 'showdown' ? { excludeKickers: arm === 'A' } : {}) };
    const ruleExcluded = study === 'classic' ? exclusions : (arm === 'A' ? removed : []);
    let result: ReturnType<typeof optimizeNflLineups> | null = null;
    let error: string | null = null;
    try { result = optimizeNflLineups(structuredClone(eligible), structuredClone(effective)); }
    catch (e) { error = e instanceof Error ? e.message : String(e); }
    const lineups = result?.lineups ?? [];
    const completion = buildCompletion({ ...result, requestedLineups: settings.nLineups, generatedLineups: lineups.length });
    const researchStudy = { ...common, arm,
      label: arm === 'A' ? 'Current Winners' : study === 'classic' ? 'Winners without TE/WR pool exclusions' : 'Winners with kickers allowed',
      completion: completion.status, issues: completion.issues, error, ruleExclusions: ruleExcluded.map(id => ({
        dkPlayerId: id, position: byId.get(id)!.position,
        reason: shape.excluded.get(id) ?? 'Winners shape: no kicker in Showdown lineups (excludeKickers)',
      })) };
    const persistedSettings = { ...effective, researchStudy, runEvidence: {
      eligibility: result?.eligibility ?? [], exposureReport: result?.exposureReport ?? [],
      salaryBandReport: result?.salaryBandReport ?? [], duplication: result?.duplication ?? [],
      warnings: [...(result?.warnings ?? []), ...(error ? [error] : [])], archetypePlan: result?.archetypePlan ?? [],
    } };
    const buildInfo = { commitSha: input.implementation.commit, implementationDigest: input.implementation.digest,
      buildTime: input.capturedAt, settingsSchemaVersion: version, optimizerVersion: NFL_OPTIMIZER_VERSION };
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
  return { version, pairId: input.pairId, sourceRunId: source.run_id, arms };
}

/** The persisted shape of a generated lineup (slots carry ids, not player objects). */
function persistLineup(l: ReturnType<typeof optimizeNflLineups>['lineups'][number], lineupNumber: number) {
  return { ...l, lineupNumber, slots: l.slots.map(s => ({ slot: s.slot, dkPlayerId: s.player.dkPlayerId,
    captainDkPlayerId: s.player.captainDkPlayerId, name: s.player.name, team: s.player.team, salary: s.salary,
    projection: s.projection, source: s.projectionSource, multiplier: s.multiplier, projectionAudit: s.player.projectionAudit })),
    stackSummary: { ...l.stackSummary, archetype: l.archetype ?? null } };
}

/**
 * nfl-portfolio-objective-v3 (docs/nfl-portfolio-objective-study.md).
 * Arm A rebuilds the source build from its own frozen settings (Winners
 * resolved if it was Winners). Arm B keeps every setting and picks the same
 * number of lineups from a candidate pool the production optimizer generated
 * with those settings, by the field-relative joint objective. Both arms
 * record the same forecast so the challenger's model grades both.
 */
/**
 * Arm A's rebuild and arm B's candidate pool, constructed exactly as the
 * portfolio study builds them. Shared by the pair builder and the v3 freeze
 * gate's stability check, so the gate tests the selection the study would make.
 */
export function portfolioConstruction(input: Raw) {
  const source = input.source;
  const settings = structuredClone(source.settings) as NflOptimizerSettings & Raw;
  delete settings.runEvidence;
  const format = settings.format as 'classic' | 'showdown';
  const players = restorePlayers(input.players);
  if (new Set(players.map(p => p.dkPlayerId)).size !== players.length) throw new Error('Duplicate frozen player IDs');
  const eligible = settings.projectionSource === 'workload'
    ? players.filter(p => input.players.find((r: Raw) => r.dkPlayerId === p.dkPlayerId)?.workloadEligible === true) : players;
  // The source's own shape, resolved exactly as the product resolves it.
  const shape = settings.buildShape === 'winners'
    ? applyWinnersShape({ ...settings, buildShape: 'winners' as const }, eligible, { poolRules: true })
    : { settings, excluded: new Map<number, string>(), notes: [] as string[] };
  const effective: NflOptimizerSettings & Raw = { ...shape.settings, buildShape: 'standard' as const,
    excludedPlayerIds: [...new Set([...settings.excludedPlayerIds, ...shape.excluded.keys()])] };
  const n = settings.nLineups;
  const slate = slateFromSnapshot(format, input.players);
  // Arm A: the source build, rebuilt.
  let resultA: ReturnType<typeof optimizeNflLineups> | null = null, errorA: string | null = null;
  try { resultA = optimizeNflLineups(structuredClone(eligible), structuredClone(effective)); }
  catch (e) { errorA = e instanceof Error ? e.message : String(e); }
  const lineupsA = resultA?.lineups ?? [];
  // Arm B: candidate pool from the same settings.
  const candidateCount = Math.min(PORTFOLIO_OBJECTIVE.candidateMax, Math.max(PORTFOLIO_OBJECTIVE.candidateMin, PORTFOLIO_OBJECTIVE.candidateMultiple * n));
  // The source's own exposure cap and uniqueness, so the pool is diverse by construction (an uncapped run
  // concentrated on chalk and left the capped selection short on the week-5 Classic fixture).
  const candidateSettings = { ...effective, nLineups: candidateCount };
  let resultC: ReturnType<typeof optimizeNflLineups> | null = null, errorB: string | null = null;
  // Timing is reported to the console only: the record must be reproducible byte for byte.
  const started = Date.now();
  try { resultC = optimizeNflLineups(structuredClone(eligible), structuredClone(candidateSettings)); }
  catch (e) { errorB = e instanceof Error ? e.message : String(e); }
  const pool = new Map<string, ReturnType<typeof optimizeNflLineups>['lineups'][number]>();
  for (const l of [...lineupsA, ...(resultC?.lineups ?? [])]) {
    const roster = rosterOf(l);
    const { key } = validateNflLineup(slate, roster);
    if (!pool.has(key)) pool.set(key, l);
  }
  const constraints = { count: n, maxExposure: effective.maxExposure, maxExposureByPlayer: effective.maxExposureByPlayer ?? {},
    minUnique: effective.minUnique };
  return { source, settings, format, shape, effective, n, slate, resultA, errorA, lineupsA, candidateCount, resultC, errorB,
    pool, constraints, started };
}

/**
 * v3 freeze-gate stability check: arm B's selection from the same candidate
 * pool under two selection seeds (field seed fixed), and the Jaccard overlap
 * of the two chosen sets. Nothing here reads an outcome.
 */
export function portfolioSelectionStability(input: Raw, seeds: readonly [number, number]) {
  const c = portfolioConstruction(input);
  if (c.errorB) throw new Error(`Candidate pool failed: ${c.errorB}`);
  const field = sampleField(c.slate, input.players, PORTFOLIO_OBJECTIVE.fieldSeed);
  const rosters = [...c.pool].map(([key, l]) => ({ key, lineup: rosterOf(l) }));
  const picks = seeds.map(seed => {
    const bank = drawJointBank(input.players, seed);
    const threshold = fieldThresholds(field, bank, PORTFOLIO_OBJECTIVE.successFraction);
    const pick = selectPortfolio(rosters.map(r => ({ ...r, draws: scoreLineup(r.lineup, bank) })), threshold, c.constraints);
    return { seed, keys: pick.selected.map(x => x.key), selectionSuccess: pick.selectionSuccess, shortfall: pick.shortfall };
  });
  const [a, b] = picks.map(p => new Set(p.keys));
  const union = new Set([...a, ...b]).size;
  const jaccard = union ? [...a].filter(k => b.has(k)).length / union : 1;
  return { seeds: [...seeds], candidates: rosters.length, requested: c.n, picks, jaccard: Math.round(jaccard * 10000) / 10000 };
}

export function buildPortfolioPair(input: Raw, version: string): PairResult {
  const { source, settings, format, shape, effective, n, slate, resultA, errorA, lineupsA, candidateCount, resultC,
    errorB: poolError, pool, constraints, started } = portfolioConstruction(input);
  let errorB = poolError;
  const before = digest(input);
  const common = { version, pairId: input.pairId, sourceRunId: source.run_id, format,
    season: input.season, week: input.week, slateSignature: input.slateSignature, gameIds: input.gameIds ?? null,
    firstKickoff: input.firstKickoff, capturedAt: input.capturedAt,
    inputPoolDigest: digest(input.players), sourceInputDigest: source.input_digest, implementation: input.implementation,
    sourceBuildShape: settings.buildShape ?? 'standard', winnersShapeVersion: WINNERS_SHAPE_VERSION, shapeNotes: shape.notes,
    jointVersion: JOINT_COPULA_VERSION, fieldVersion: FIELD_MODEL_VERSION, objectiveVersion: PORTFOLIO_OBJECTIVE_VERSION,
    constants: PORTFOLIO_OBJECTIVE, randomSeed: 20260902, commonSettings: effective,
    forwardEligibility: 'pending_postgame_flagship_coverage' };
  const record = (arm: 'A' | 'B', label: string, result: ReturnType<typeof optimizeNflLineups> | null, error: string | null,
    lineups: ReturnType<typeof optimizeNflLineups>['lineups'], extra: Raw, settingsUsed: Raw, countOnly = false) => {
    // Arm B does not enforce the source's archetype quotas or exposure minimums (recorded as unenforced),
    // so its completion is the lineup count alone; arm A is judged as the product judges a build.
    const completion = countOnly
      ? { status: lineups.length === n ? 'complete' as const : 'partial' as const, issues: lineups.length === n ? [] : [`${lineups.length} of ${n} requested lineups selected.`] }
      : buildCompletion({ ...(result ?? { warnings: [], sourceCoverage: { requested: 0, direct: 0, fallback: 0, excluded: 0 }, lineups: [] }),
          requestedLineups: n, generatedLineups: lineups.length });
    const status = error ? 'failed' : completion.status;
    const researchStudy = { ...common, arm, label, completion: status, issues: completion.issues, error, ...extra };
    const persistedSettings = { ...settingsUsed, researchStudy, runEvidence: {
      eligibility: result?.eligibility ?? [], exposureReport: result?.exposureReport ?? [],
      salaryBandReport: result?.salaryBandReport ?? [], duplication: result?.duplication ?? [],
      warnings: [...(result?.warnings ?? []), ...(error ? [error] : [])], archetypePlan: result?.archetypePlan ?? [] } };
    const buildInfo = { commitSha: input.implementation.commit, implementationDigest: input.implementation.digest,
      buildTime: input.capturedAt, settingsSchemaVersion: version, optimizerVersion: NFL_OPTIMIZER_VERSION };
    return { arm, settings: persistedSettings, inputSnapshot: input.players, buildInfo, optimizerVersion: NFL_OPTIMIZER_VERSION,
      inputDigest: digest({ settings: persistedSettings, inputSnapshot: input.players, optimizerVersion: NFL_OPTIMIZER_VERSION, buildInfo }),
      status: `research_${status}`, failureReason: error ?? (completion.issues.join(' ') || null), requestedLineups: n,
      lineups: lineups.map((l, i) => persistLineup(l, i + 1)) };
  };
  const keyOf = (ids: number[], cpt: number | null) => format === 'classic' ? `classic:${[...ids].sort((a, b) => a - b).join(',')}`
    : `showdown:${cpt}:${[...ids].filter(id => id !== cpt).sort((a, b) => a - b).join(',')}`;
  const sourceKeys = new Set<string>((input.sourceLineups ?? []).map((l: Raw) => keyOf(l.playerIds, l.captainDkPlayerId ?? null)));
  const armAKeys = lineupsA.map(l => keyOf(l.playerIds, l.slots.find(s => s.slot === 'CPT')?.player.dkPlayerId ?? null));
  const reproduced = sourceKeys.size ? armAKeys.filter(k => sourceKeys.has(k)).length : null;
  // Joint draws and the field, shared by both arms; the forecast uses draws the search never sees.
  const selection = drawJointBank(input.players, PORTFOLIO_OBJECTIVE.selectionSeed);
  const evaluation = drawJointBank(input.players, PORTFOLIO_OBJECTIVE.evaluationSeed);
  const field = sampleField(slate, input.players, PORTFOLIO_OBJECTIVE.fieldSeed);
  const thresholdSel = fieldThresholds(field, selection, PORTFOLIO_OBJECTIVE.successFraction);
  // One pass over the evaluation draws gives the forecast thresholds and the implied-field summary.
  const evalThresholds = fieldThresholdsMany(field, evaluation, SUMMARY_FRACTIONS);
  const [thresholdEval, thresholdEval01] = evalThresholds;
  const modelRecord = { draws: selection.draws, seeds: { selection: selection.seed, evaluation: evaluation.seed, field: field.seed },
    fallbackPlayerIds: selection.fallbackPlayerIds, meanNotMatchedIds: selection.meanNotMatchedIds,
    fieldLineups: field.lineups.length, fieldAttempts: field.attempts, fieldCalibration: field.calibration, fieldShapes: field.shapes,
    impliedField: summarizeFieldThresholds(evalThresholds) };
  const forecast = (lineups: ReturnType<typeof optimizeNflLineups>['lineups']) => ({
    successTop1Pct: forecastSuccess(lineups.map(rosterOf), evaluation, thresholdEval),
    successTop01Pct: forecastSuccess(lineups.map(rosterOf), evaluation, thresholdEval01) });
  const armA = record('A', 'Source build, rebuilt', resultA, errorA, lineupsA,
    { ...modelRecord, sourceLineupsReproduced: reproduced, sourceLineupCount: sourceKeys.size || null, forecast: forecast(lineupsA) }, effective);
  // Arm B: the candidate pool (built in portfolioConstruction), selection by the field-relative objective.
  const candidates = [...pool].map(([key, l]) => ({ key, lineup: rosterOf(l), draws: scoreLineup(rosterOf(l), selection), generated: l }));
  const unenforced = [
    ...(Object.values(settings.minExposureByPlayer ?? {}).some((v: unknown) => Number(v) > 0) ? ['minExposureByPlayer'] : []),
    ...((settings.exposurePolicies?.length ?? 0) > 0 ? ['exposurePolicies'] : []),
    ...(settings.archetypeMode && settings.archetypeMode !== 'standard' ? ['archetypeMode'] : []),
    ...(settings.bringBackShare != null ? ['bringBackShare'] : []), ...(settings.maxQbExposure != null ? ['maxQbExposure'] : []),
    ...(settings.maxDstVsOffenseShare != null ? ['maxDstVsOffenseShare'] : []), ...(settings.maxCaptainExposure != null ? ['maxCaptainExposure'] : [])];
  let pick: ReturnType<typeof selectPortfolio> | null = null;
  if (!errorB) {
    try { pick = selectPortfolio(candidates, thresholdSel, constraints); }
    catch (e) { errorB = e instanceof Error ? e.message : String(e); }
  }
  const lineupsB = pick ? pick.selected.map(s => candidates[s.index].generated) : [];
  console.error(`portfolio candidate pool: ${candidates.length} distinct from ${resultC?.lineups.length ?? 0} generated in ${Date.now() - started} ms`);
  const armB = record('B', 'Field-relative joint selection from the same candidate pool', resultC, errorB, lineupsB,
    { ...modelRecord, candidatePool: { requested: candidateCount, generated: resultC?.lineups.length ?? 0, distinct: candidates.length,
        fromArmA: lineupsA.length, settings: { nLineups: candidateCount, maxExposure: effective.maxExposure, minUnique: effective.minUnique },
        // Every candidate roster, so the robustness check can re-select from the same pool under perturbed inputs.
        rosters: candidates.map(c => ({ key: c.key, lineup: c.lineup })) },
      selection: pick ? { selectionSuccess: pick.selectionSuccess, shortfall: pick.shortfall,
        picks: pick.selected.map(x => ({ key: x.key, marginalGain: x.marginalGain })) } : null,
      selectionConstraints: { maxExposure: effective.maxExposure, minUnique: effective.minUnique, maxExposureByPlayer: effective.maxExposureByPlayer ?? {} },
      unenforcedSourceConstraints: unenforced, forecast: forecast(lineupsB) }, effective, true);
  if (digest(input) !== before) throw new Error('Research adapter mutated frozen inputs');
  return { version, pairId: input.pairId, sourceRunId: source.run_id, arms: [armA, armB] };
}
