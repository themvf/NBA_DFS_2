import "server-only";

import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { db } from "@/db";
import { optimizerJobLineups, optimizerJobs } from "@/db/schema";
import type { Sport } from "@/db/queries";
import {
  buildNextNbaLineup,
  prepareNbaOptimizerRun,
  type GeneratedLineup,
  type LineupSlot,
  type OptimizerPlayer,
  type OptimizerSettings,
} from "./optimizer";
import {
  buildNextMlbLineup,
  prepareMlbOptimizerRun,
  type MlbGeneratedLineup,
  type MlbLineupSlot,
  type MlbOptimizerPlayer,
  type MlbOptimizerSettings,
} from "./mlb-optimizer";
import type { OptimizerDebugInfo } from "./optimizer-debug";
import { normalizeNbaRuleSelections, validateNbaRuleSelections } from "./nba-optimizer-rules";
import { normalizeMlbRuleSelections, validateMlbRuleSelections } from "./mlb-optimizer-rules";
import { normalizeMlbPendingLineupPolicy } from "./mlb-lineup";
import type {
  CreateOptimizerJobRequest,
  MlbPreparedOptimizerRun,
  NbaPreparedOptimizerRun,
  OptimizerJobSnapshot,
  OptimizerJobStatus,
  OptimizerJobStatusResponse,
  OptimizerJobTelemetry,
  OptimizerJobTerminationReason,
  OptimizerJobView,
  PersistedOptimizerJobLineup,
  PreparedOptimizerRun,
} from "./optimizer-job-types";

const STALE_JOB_THRESHOLD_MS = 3 * 60_000;

let ensureOptimizerJobTablesPromise: Promise<void> | null = null;

function finiteOrNull(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
}

function sanitizeProjection(value: number | null | undefined): number | null {
  const finite = finiteOrNull(value);
  return finite == null ? null : Math.max(0, finite);
}

function sanitizeOwnershipPct(value: number | null | undefined): number | null {
  const finite = finiteOrNull(value);
  return finite == null ? null : Math.max(0, Math.min(100, finite));
}

function sanitizeLeverage(value: number | null | undefined): number | null {
  return finiteOrNull(value);
}

async function runEnsureStatements() {
  await db.execute(sql.raw(`
    CREATE TABLE IF NOT EXISTS optimizer_jobs (
      id SERIAL PRIMARY KEY,
      sport TEXT NOT NULL,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      client_token TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      requested_lineups INTEGER NOT NULL,
      built_lineups INTEGER NOT NULL DEFAULT 0,
      eligible_count INTEGER,
      settings_json JSONB NOT NULL,
      snapshot_json JSONB NOT NULL,
      selected_matchups_json JSONB NOT NULL,
      pool_snapshot_json JSONB NOT NULL,
      effective_settings_json JSONB,
      probe_summary_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      relaxed_constraints_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      workflow_run_id TEXT,
      probe_ms INTEGER,
      total_ms INTEGER,
      termination_reason TEXT,
      warning TEXT,
      error TEXT,
      started_at TIMESTAMPTZ,
      finished_at TIMESTAMPTZ,
      heartbeat_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `));
  await db.execute(sql.raw(`
    CREATE TABLE IF NOT EXISTS optimizer_job_lineups (
      id SERIAL PRIMARY KEY,
      job_id INTEGER NOT NULL REFERENCES optimizer_jobs(id) ON DELETE CASCADE,
      lineup_num INTEGER NOT NULL,
      slot_player_ids_json JSONB NOT NULL,
      player_ids_json JSONB NOT NULL,
      total_salary INTEGER NOT NULL,
      proj_fpts DOUBLE PRECISION NOT NULL,
      leverage DOUBLE PRECISION NOT NULL,
      duration_ms INTEGER NOT NULL,
      winning_stage TEXT,
      attempts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(job_id, lineup_num)
    )
  `));
  await db.execute(sql.raw(`
    CREATE INDEX IF NOT EXISTS idx_optimizer_jobs_lookup
    ON optimizer_jobs(client_token, sport, slate_id, status)
  `));
  await db.execute(sql.raw(`
    CREATE INDEX IF NOT EXISTS idx_optimizer_jobs_created
    ON optimizer_jobs(created_at)
  `));
  await db.execute(sql.raw(`
    CREATE INDEX IF NOT EXISTS idx_optimizer_job_lineups_job
    ON optimizer_job_lineups(job_id, lineup_num)
  `));
}

export async function ensureOptimizerJobTables() {
  if (!ensureOptimizerJobTablesPromise) {
    ensureOptimizerJobTablesPromise = runEnsureStatements().catch((error) => {
      ensureOptimizerJobTablesPromise = null;
      throw error;
    });
  }
  await ensureOptimizerJobTablesPromise;
}

type NbaPoolRow = OptimizerPlayer;
type MlbPoolRow = MlbOptimizerPlayer;

async function loadNbaOptimizerPool(
  slateId: number,
  selectedMatchupIds: number[],
): Promise<OptimizerPlayer[]> {
  const rows = await db.execute<NbaPoolRow>(sql`
    SELECT
      dp.id,
      dp.dk_player_id AS "dkPlayerId",
      dp.name,
      dp.team_abbrev AS "teamAbbrev",
      dp.team_id AS "teamId",
      dp.matchup_id AS "matchupId",
      dp.eligible_positions AS "eligiblePositions",
      dp.salary,
      COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS "ourProj",
      COALESCE(dp.live_leverage, dp.our_leverage) AS "ourLeverage",
      dp.linestar_proj AS "linestarProj",
      COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "projOwnPct",
      dp.dk_in_starting_lineup AS "dkInStartingLineup",
      dp.dk_starting_lineup_order AS "dkStartingLineupOrder",
      dp.dk_team_lineup_confirmed AS "dkTeamLineupConfirmed",
      dp.is_out AS "isOut",
      dp.game_info AS "gameInfo",
      t.logo_url AS "teamLogo",
      t.name AS "teamName",
      m.home_team_id AS "homeTeamId"
    FROM dk_players dp
    LEFT JOIN teams t ON t.team_id = dp.team_id
    LEFT JOIN nba_matchups m ON m.id = dp.matchup_id
    WHERE dp.slate_id = ${slateId}
    ORDER BY dp.id ASC
  `);

  return rows.rows
    .map((player) => ({
      ...player,
      ourProj: sanitizeProjection(player.ourProj ?? player.linestarProj ?? null),
      ourLeverage: sanitizeLeverage(player.ourLeverage),
      linestarProj: sanitizeProjection(player.linestarProj),
      projOwnPct: sanitizeOwnershipPct(player.projOwnPct),
    }))
    .filter((player) =>
      selectedMatchupIds.length === 0
      || (player.matchupId != null && selectedMatchupIds.includes(player.matchupId))
    );
}

async function loadMlbOptimizerPool(
  slateId: number,
  selectedMatchupIds: number[],
): Promise<MlbOptimizerPlayer[]> {
  const rows = await db.execute<MlbPoolRow>(sql`
    SELECT
      dp.id,
      dp.dk_player_id AS "dkPlayerId",
      dp.name,
      dp.team_abbrev AS "teamAbbrev",
      dp.mlb_team_id AS "teamId",
      dp.matchup_id AS "matchupId",
      dp.eligible_positions AS "eligiblePositions",
      dp.salary,
      dp.our_proj AS "ourProj",
      dp.our_leverage AS "ourLeverage",
      dp.linestar_proj AS "linestarProj",
      dp.proj_own_pct AS "projOwnPct",
      dp.dk_in_starting_lineup AS "dkInStartingLineup",
      dp.dk_starting_lineup_order AS "dkStartingLineupOrder",
      dp.dk_team_lineup_confirmed AS "dkTeamLineupConfirmed",
      dp.is_out AS "isOut",
      dp.game_info AS "gameInfo",
      mt.logo_url AS "teamLogo",
      mt.name AS "teamName",
      mm.home_team_id AS "homeTeamId"
    FROM dk_players dp
    LEFT JOIN mlb_teams mt ON mt.team_id = dp.mlb_team_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE dp.slate_id = ${slateId}
    ORDER BY dp.id ASC
  `);

  return rows.rows
    .map((player) => ({
      ...player,
      ourProj: sanitizeProjection(player.ourProj ?? player.linestarProj ?? null),
      ourLeverage: sanitizeLeverage(player.ourLeverage),
      linestarProj: sanitizeProjection(player.linestarProj),
      projOwnPct: sanitizeOwnershipPct(player.projOwnPct),
    }))
    .filter((player) =>
      selectedMatchupIds.length === 0
      || (player.matchupId != null && selectedMatchupIds.includes(player.matchupId))
    );
}

async function loadOptimizerPool(
  sport: Sport,
  slateId: number,
  selectedMatchupIds: number[],
): Promise<OptimizerPlayer[] | MlbOptimizerPlayer[]> {
  return sport === "mlb"
    ? loadMlbOptimizerPool(slateId, selectedMatchupIds)
    : loadNbaOptimizerPool(slateId, selectedMatchupIds);
}

function isStaleJob(job: typeof optimizerJobs.$inferSelect | null): boolean {
  if (!job) return false;
  if (job.status !== "queued" && job.status !== "running") return false;
  const heartbeat = job.heartbeatAt ?? job.startedAt ?? job.createdAt;
  if (!heartbeat) return false;
  return Date.now() - heartbeat.getTime() > STALE_JOB_THRESHOLD_MS;
}

async function markJobStale(jobId: number) {
  const now = new Date();
  await db
    .update(optimizerJobs)
    .set({
      status: "failed",
      terminationReason: "stale",
      error: "Optimizer job stopped sending heartbeats and was marked stale.",
      finishedAt: now,
      heartbeatAt: now,
    })
    .where(eq(optimizerJobs.id, jobId));
}

export async function failOptimizerJob(
  jobId: number,
  error: string,
  terminationReason: OptimizerJobTerminationReason = "lineup_failed",
) {
  await ensureOptimizerJobTables();
  const now = new Date();
  await db
    .update(optimizerJobs)
    .set({
      status: "failed",
      error,
      terminationReason,
      heartbeatAt: now,
      finishedAt: now,
    })
    .where(eq(optimizerJobs.id, jobId));
}

function computeTelemetry(lineups: PersistedOptimizerJobLineup[]): OptimizerJobTelemetry {
  if (lineups.length === 0) {
    return { minMs: null, avgMs: null, maxMs: null, p95Ms: null };
  }

  const durations = lineups.map((lineup) => lineup.durationMs).sort((a, b) => a - b);
  const sum = durations.reduce((total, value) => total + value, 0);
  const p95Index = Math.min(durations.length - 1, Math.ceil(durations.length * 0.95) - 1);
  return {
    minMs: durations[0],
    avgMs: Math.round(sum / durations.length),
    maxMs: durations[durations.length - 1],
    p95Ms: durations[p95Index],
  };
}

function serializeDate(value: Date | null | undefined): string | null {
  return value ? value.toISOString() : null;
}

function computeJobTotalMs(job: JobRecord, now = new Date()): number {
  if (
    job.totalMs != null
    && Number.isFinite(job.totalMs)
    && job.status !== "queued"
    && job.status !== "running"
  ) {
    return job.totalMs;
  }
  const anchor = job.startedAt ?? job.createdAt;
  if (!anchor) return job.totalMs ?? 0;
  return Math.max(0, now.getTime() - anchor.getTime());
}

function normalizeTerminationReason(value: string | null | undefined): OptimizerJobTerminationReason | null {
  if (
    value === "completed"
    || value === "insufficient_pool"
    || value === "probe_infeasible"
    || value === "lineup_failed"
    || value === "stale"
  ) {
    return value;
  }
  return null;
}

type JobRecord = typeof optimizerJobs.$inferSelect;
type JobLineupRecord = typeof optimizerJobLineups.$inferSelect;

function buildJobView(job: JobRecord, stale: boolean): OptimizerJobView {
  return {
    id: job.id,
    sport: job.sport as Sport,
    slateId: job.slateId,
    clientToken: job.clientToken,
    status: job.status as OptimizerJobStatus,
    requestedLineups: job.requestedLineups,
    builtLineups: job.builtLineups,
    eligibleCount: job.eligibleCount,
    settings: job.settingsJson as OptimizerSettings | MlbOptimizerSettings,
    snapshot: job.snapshotJson as OptimizerJobSnapshot,
    effectiveSettings: (job.effectiveSettingsJson as OptimizerDebugInfo["effectiveSettings"] | null) ?? null,
    probeSummary: (job.probeSummaryJson as OptimizerDebugInfo["probeSummary"]) ?? [],
    relaxedConstraints: (job.relaxedConstraintsJson as string[]) ?? [],
    selectedMatchupIds: (job.selectedMatchupsJson as number[]) ?? [],
    warning: job.warning,
    error: job.error,
    terminationReason: normalizeTerminationReason(job.terminationReason),
    workflowRunId: job.workflowRunId,
    totalMs: computeJobTotalMs(job),
    probeMs: job.probeMs,
    stale,
    createdAt: serializeDate(job.createdAt),
    startedAt: serializeDate(job.startedAt),
    heartbeatAt: serializeDate(job.heartbeatAt),
    finishedAt: serializeDate(job.finishedAt),
  };
}

function summarizeHeuristic(
  sport: Sport,
  job: JobRecord,
  lineups: PersistedOptimizerJobLineup[],
): OptimizerDebugInfo["heuristic"] | undefined {
  if (sport !== "nba") return undefined;
  const attempts = lineups.flatMap((lineup) => lineup.attempts);
  const rejectedByReason: Record<string, number> = {};
  for (const attempt of attempts) {
    for (const [reason, count] of Object.entries(attempt.rejectedByReason ?? {})) {
      rejectedByReason[reason] = (rejectedByReason[reason] ?? 0) + count;
    }
  }

  const poolSnapshot = Array.isArray(job.poolSnapshotJson) ? job.poolSnapshotJson : [];
  return {
    prunedCandidateCount: poolSnapshot.length || job.eligibleCount || 0,
    templateCount: attempts.reduce((best, attempt) => Math.max(best, attempt.templateCount ?? 0), 0),
    templatesTried: attempts.reduce((sum, attempt) => sum + (attempt.templatesTried ?? 0), 0),
    repairAttempts: attempts.reduce((sum, attempt) => sum + (attempt.repairAttempts ?? 0), 0),
    rejectedByReason,
  };
}

function buildPersistedLineup(lineup: JobLineupRecord): PersistedOptimizerJobLineup {
  return {
    lineupNumber: lineup.lineupNum,
    slotPlayerIds: lineup.slotPlayerIdsJson as Record<string, number>,
    playerIds: lineup.playerIdsJson as number[],
    totalSalary: lineup.totalSalary,
    projFpts: lineup.projFpts,
    leverageScore: lineup.leverage,
    durationMs: lineup.durationMs,
    winningStage: lineup.winningStage ?? undefined,
    attempts: (lineup.attemptsJson as PersistedOptimizerJobLineup["attempts"]) ?? [],
  };
}

function buildDebugInfo(
  job: JobRecord,
  lineups: PersistedOptimizerJobLineup[],
): OptimizerDebugInfo | null {
  const settings = job.settingsJson as OptimizerSettings | MlbOptimizerSettings;
  const mode = settings.mode;
  if (!mode) return null;

  const terminationReason = normalizeTerminationReason(job.terminationReason)
    ?? (job.status === "completed" ? "completed" : "lineup_failed");
  const debugTerminationReason: OptimizerDebugInfo["terminationReason"] =
    terminationReason === "stale" ? "lineup_failed" : terminationReason;
  const nbaSettings = settings as OptimizerSettings;
  const mlbSettings = settings as MlbOptimizerSettings;
  const fallbackEffectiveSettings: OptimizerDebugInfo["effectiveSettings"] =
    job.sport === "mlb"
      ? {
          minStack: mlbSettings.minStack,
          bringBackThreshold: mlbSettings.bringBackThreshold,
          maxExposure: mlbSettings.maxExposure,
          minChanges: mode === "gpp" ? 3 : 2,
          antiCorrMax: mlbSettings.antiCorrMax,
          pendingLineupPolicy: normalizeMlbPendingLineupPolicy(mlbSettings.pendingLineupPolicy),
        }
      : {
          minStack: nbaSettings.minStack ?? 2,
          teamStackCount: nbaSettings.teamStackCount ?? 1,
          bringBackEnabled: nbaSettings.bringBackEnabled ?? ((nbaSettings.bringBackThreshold ?? 0) > 0),
          bringBackSize:
            (nbaSettings.bringBackEnabled ?? ((nbaSettings.bringBackThreshold ?? 0) > 0))
              ? (nbaSettings.bringBackSize ?? 1)
              : 0,
          maxExposure: nbaSettings.maxExposure,
          minChanges: mode === "gpp" ? 3 : 2,
        };

  return {
    sport: job.sport as Sport,
    mode,
    eligibleCount: job.eligibleCount ?? 0,
    requestedLineups: job.requestedLineups,
    builtLineups: lineups.length,
    totalMs: computeJobTotalMs(job),
    probeMs: job.probeMs ?? 0,
    maxExposureCount: Math.ceil(job.requestedLineups * ((settings as OptimizerSettings).maxExposure ?? 1)),
    relaxedConstraints: (job.relaxedConstraintsJson as string[]) ?? [],
    probeSummary: (job.probeSummaryJson as OptimizerDebugInfo["probeSummary"]) ?? [],
    lineupSummaries: lineups.map((lineup) => ({
      lineupNumber: lineup.lineupNumber,
      status: "built" as const,
      durationMs: lineup.durationMs,
      winningStage: lineup.winningStage,
      attempts: lineup.attempts,
    })),
    terminationReason: debugTerminationReason,
    stoppedAtLineup:
      debugTerminationReason === "lineup_failed" && lineups.length < job.requestedLineups
        ? lineups.length + 1
        : undefined,
    heuristic: summarizeHeuristic(job.sport as Sport, job, lineups),
    effectiveSettings:
      (job.effectiveSettingsJson as OptimizerDebugInfo["effectiveSettings"] | null)
      ?? fallbackEffectiveSettings,
  };
}

async function readJob(jobId: number): Promise<JobRecord | null> {
  const rows = await db
    .select()
    .from(optimizerJobs)
    .where(eq(optimizerJobs.id, jobId))
    .limit(1);
  return rows[0] ?? null;
}

async function readJobLineups(jobId: number): Promise<PersistedOptimizerJobLineup[]> {
  const rows = await db
    .select()
    .from(optimizerJobLineups)
    .where(eq(optimizerJobLineups.jobId, jobId))
    .orderBy(asc(optimizerJobLineups.lineupNum));
  return rows.map(buildPersistedLineup);
}

export async function getOptimizerJobStatus(jobId: number): Promise<OptimizerJobStatusResponse | null> {
  await ensureOptimizerJobTables();
  let job = await readJob(jobId);
  if (!job) return null;

  if (isStaleJob(job)) {
    await markJobStale(job.id);
    job = await readJob(jobId);
    if (!job) return null;
  }

  const stale = isStaleJob(job);
  const lineups = await readJobLineups(jobId);
  return {
    ok: true,
    job: buildJobView(job, stale),
    debug: buildDebugInfo(job, lineups),
    lineups,
    telemetry: computeTelemetry(lineups),
  };
}

async function readActiveJob(
  clientToken: string,
  sport: Sport,
  slateId: number,
): Promise<JobRecord | null> {
  const rows = await db
    .select()
    .from(optimizerJobs)
    .where(and(
      eq(optimizerJobs.clientToken, clientToken),
      eq(optimizerJobs.sport, sport),
      eq(optimizerJobs.slateId, slateId),
      inArray(optimizerJobs.status, ["queued", "running"]),
    ))
    .orderBy(desc(optimizerJobs.createdAt), desc(optimizerJobs.id))
    .limit(1);
  return rows[0] ?? null;
}

export async function getActiveOptimizerJobStatus(
  clientToken: string,
  sport: Sport,
  slateId: number,
): Promise<OptimizerJobStatusResponse | null> {
  await ensureOptimizerJobTables();
  let job = await readActiveJob(clientToken, sport, slateId);
  if (!job) return null;

  if (isStaleJob(job)) {
    await markJobStale(job.id);
    job = await readActiveJob(clientToken, sport, slateId);
    if (!job) return null;
  }

  return getOptimizerJobStatus(job.id);
}

export async function createOptimizerJob(input: CreateOptimizerJobRequest): Promise<{ jobId: number; existing: boolean }> {
  await ensureOptimizerJobTables();

  const existing = await getActiveOptimizerJobStatus(input.clientToken, input.sport, input.slateId);
  if (existing) {
    return { jobId: existing.job.id, existing: true };
  }

  const poolSnapshot = await loadOptimizerPool(input.sport, input.slateId, input.selectedMatchupIds);
  if (poolSnapshot.length === 0) {
    throw new Error("No players available for the selected slate and games.");
  }
  if (input.sport === "nba") {
    const validation = validateNbaRuleSelections(poolSnapshot as OptimizerPlayer[], input.settings as OptimizerSettings);
    if (!validation.ok) {
      throw new Error(validation.error);
    }
  } else {
    const validation = validateMlbRuleSelections(poolSnapshot as MlbOptimizerPlayer[], input.settings as MlbOptimizerSettings);
    if (!validation.ok) {
      throw new Error(validation.error);
    }
  }

  const snapshot: OptimizerJobSnapshot = {
    sport: input.sport,
    slateId: input.slateId,
    selectedMatchupIds: input.selectedMatchupIds,
    requestedLineups: input.settings.nLineups,
    mode: input.settings.mode,
  };

  const inserted = await db
    .insert(optimizerJobs)
    .values({
      sport: input.sport,
      slateId: input.slateId,
      clientToken: input.clientToken,
      status: "queued",
      requestedLineups: input.settings.nLineups,
      builtLineups: 0,
      settingsJson: input.settings,
      snapshotJson: snapshot,
      selectedMatchupsJson: input.selectedMatchupIds,
      poolSnapshotJson: poolSnapshot,
      probeSummaryJson: [],
      relaxedConstraintsJson: [],
    })
    .returning({ id: optimizerJobs.id });

  return { jobId: inserted[0].id, existing: false };
}

export async function attachWorkflowRunId(jobId: number, workflowRunId: string) {
  await ensureOptimizerJobTables();
  await db
    .update(optimizerJobs)
    .set({ workflowRunId })
    .where(eq(optimizerJobs.id, jobId));
}

type PrepareResult =
  | { ok: true; prepared: PreparedOptimizerRun }
  | { ok: false; debug: OptimizerDebugInfo; error: string };

export async function prepareOptimizerJob(jobId: number): Promise<PrepareResult> {
  await ensureOptimizerJobTables();
  const job = await readJob(jobId);
  if (!job) {
    throw new Error("Optimizer job not found.");
  }

  const now = new Date();
  const settings = job.settingsJson as OptimizerSettings | MlbOptimizerSettings;
  const pool = job.poolSnapshotJson as OptimizerPlayer[] | MlbOptimizerPlayer[];

  await db
    .update(optimizerJobs)
    .set({
      status: "running",
      startedAt: job.startedAt ?? now,
      heartbeatAt: now,
      terminationReason: null,
      error: null,
      warning: null,
    })
    .where(eq(optimizerJobs.id, jobId));

  const preparedResult = job.sport === "mlb"
    ? prepareMlbOptimizerRun(pool as MlbOptimizerPlayer[], settings as MlbOptimizerSettings)
    : prepareNbaOptimizerRun(pool as OptimizerPlayer[], settings as OptimizerSettings);

  if (!preparedResult.prepared) {
    const errorMessage = ("error" in preparedResult ? preparedResult.error : undefined)
      ?? "Optimizer preflight failed before a lineup could be built.";
    await db
      .update(optimizerJobs)
      .set({
        status: "failed",
        builtLineups: 0,
        eligibleCount: preparedResult.debug.eligibleCount,
        probeMs: preparedResult.debug.probeMs,
        probeSummaryJson: preparedResult.debug.probeSummary,
        relaxedConstraintsJson: preparedResult.debug.relaxedConstraints,
        effectiveSettingsJson: preparedResult.debug.effectiveSettings,
        terminationReason: preparedResult.debug.terminationReason,
        error: errorMessage,
        startedAt: job.startedAt ?? now,
        heartbeatAt: now,
        finishedAt: now,
        totalMs: preparedResult.debug.totalMs,
      })
      .where(eq(optimizerJobs.id, jobId));

    return {
      ok: false,
      debug: preparedResult.debug,
      error: errorMessage,
    };
  }

  await db
    .update(optimizerJobs)
    .set({
      status: "running",
      eligibleCount: preparedResult.debug.eligibleCount,
      poolSnapshotJson: preparedResult.prepared.pool,
      probeMs: preparedResult.debug.probeMs,
      probeSummaryJson: preparedResult.debug.probeSummary,
      relaxedConstraintsJson: preparedResult.debug.relaxedConstraints,
      effectiveSettingsJson: preparedResult.prepared.effectiveSettings,
      terminationReason: null,
      error: null,
      warning: null,
      startedAt: job.startedAt ?? now,
      heartbeatAt: now,
    })
    .where(eq(optimizerJobs.id, jobId));

  return { ok: true, prepared: preparedResult.prepared };
}

function toNbaSlotPlayerIds(lineup: GeneratedLineup): Record<LineupSlot, number> {
  return {
    PG: lineup.slots.PG.id,
    SG: lineup.slots.SG.id,
    SF: lineup.slots.SF.id,
    PF: lineup.slots.PF.id,
    C: lineup.slots.C.id,
    G: lineup.slots.G.id,
    F: lineup.slots.F.id,
    UTIL: lineup.slots.UTIL.id,
  };
}

function toMlbSlotPlayerIds(lineup: MlbGeneratedLineup): Record<MlbLineupSlot, number> {
  return {
    P1: lineup.slots.P1.id,
    P2: lineup.slots.P2.id,
    C: lineup.slots.C.id,
    "1B": lineup.slots["1B"].id,
    "2B": lineup.slots["2B"].id,
    "3B": lineup.slots["3B"].id,
    SS: lineup.slots.SS.id,
    OF1: lineup.slots.OF1.id,
    OF2: lineup.slots.OF2.id,
    OF3: lineup.slots.OF3.id,
  };
}

function buildPreparedFromJob(job: JobRecord): PreparedOptimizerRun {
  const settings = job.settingsJson as OptimizerSettings | MlbOptimizerSettings;
  const effectiveSettings = job.effectiveSettingsJson as OptimizerDebugInfo["effectiveSettings"] | null;
  const pool = job.poolSnapshotJson as OptimizerPlayer[] | MlbOptimizerPlayer[];
  if (!effectiveSettings) {
    throw new Error("Optimizer job is missing prepared effective settings.");
  }

  if (job.sport === "mlb") {
    return {
      sport: "mlb",
      mode: settings.mode,
      requestedLineups: job.requestedLineups,
      maxExposureCount: Math.ceil(job.requestedLineups * (settings as MlbOptimizerSettings).maxExposure),
      eligibleCount: job.eligibleCount ?? pool.length,
      pool: pool as MlbOptimizerPlayer[],
      ruleSelections: normalizeMlbRuleSelections(settings as MlbOptimizerSettings),
      effectiveSettings: {
        minStack: effectiveSettings.minStack,
        bringBackThreshold: effectiveSettings.bringBackThreshold ?? 0,
        maxExposure: effectiveSettings.maxExposure,
        minChanges: effectiveSettings.minChanges,
        antiCorrMax: effectiveSettings.antiCorrMax ?? 10,
        pendingLineupPolicy: normalizeMlbPendingLineupPolicy(effectiveSettings.pendingLineupPolicy),
      },
      relaxedConstraints: (job.relaxedConstraintsJson as string[]) ?? [],
      probeSummary: (job.probeSummaryJson as OptimizerDebugInfo["probeSummary"]) ?? [],
    };
  }

  return {
    sport: "nba",
    mode: settings.mode,
    requestedLineups: job.requestedLineups,
    maxExposureCount: Math.ceil(job.requestedLineups * (settings as OptimizerSettings).maxExposure),
    eligibleCount: job.eligibleCount ?? pool.length,
    pool: pool as OptimizerPlayer[],
    ruleSelections: normalizeNbaRuleSelections(settings as OptimizerSettings),
    effectiveSettings: {
      minStack: effectiveSettings.minStack,
      teamStackCount: effectiveSettings.teamStackCount ?? 1,
      bringBackEnabled: effectiveSettings.bringBackEnabled ?? ((effectiveSettings.bringBackThreshold ?? 0) > 0),
      bringBackSize:
        effectiveSettings.bringBackSize
        ?? (((effectiveSettings.bringBackEnabled ?? ((effectiveSettings.bringBackThreshold ?? 0) > 0)) ? 1 : 0)),
      maxExposure: effectiveSettings.maxExposure,
      minChanges: effectiveSettings.minChanges,
      salaryFloor: effectiveSettings.salaryFloor ?? 49000,
    },
    relaxedConstraints: (job.relaxedConstraintsJson as string[]) ?? [],
    probeSummary: (job.probeSummaryJson as OptimizerDebugInfo["probeSummary"]) ?? [],
  };
}

export async function buildAndPersistOptimizerJobLineup(jobId: number, lineupNumber: number): Promise<{ built: boolean }> {
  await ensureOptimizerJobTables();
  const job = await readJob(jobId);
  if (!job) {
    throw new Error("Optimizer job not found.");
  }
  if (job.status === "completed" || job.status === "failed") {
    return { built: false };
  }

  const existing = await db
    .select()
    .from(optimizerJobLineups)
    .where(and(eq(optimizerJobLineups.jobId, jobId), eq(optimizerJobLineups.lineupNum, lineupNumber)))
    .limit(1);
  if (existing[0]) {
    const now = new Date();
    await db
      .update(optimizerJobs)
      .set({
        builtLineups: Math.max(job.builtLineups, lineupNumber),
        heartbeatAt: now,
        totalMs: computeJobTotalMs(job, now),
      })
      .where(eq(optimizerJobs.id, jobId));
    return { built: true };
  }

  const priorLineups = await readJobLineups(jobId);
  const priorLineupPlayerIds = priorLineups.map((lineup) => lineup.playerIds);
  const prepared = buildPreparedFromJob(job);
  const nextResult = prepared.sport === "mlb"
    ? buildNextMlbLineup(prepared as MlbPreparedOptimizerRun, priorLineupPlayerIds)
    : buildNextNbaLineup(prepared as NbaPreparedOptimizerRun, priorLineupPlayerIds);

  const now = new Date();
  if (!nextResult.lineup) {
    await db
      .update(optimizerJobs)
      .set({
        builtLineups: priorLineups.length,
        heartbeatAt: now,
        terminationReason: "lineup_failed",
        error: buildLineupFailureError(nextResult.summary),
        totalMs: computeJobTotalMs(job, now),
      })
      .where(eq(optimizerJobs.id, jobId));
    return { built: false };
  }

  const slotPlayerIds = prepared.sport === "mlb"
    ? toMlbSlotPlayerIds(nextResult.lineup as MlbGeneratedLineup)
    : toNbaSlotPlayerIds(nextResult.lineup as GeneratedLineup);
  const playerIds = nextResult.lineup.players.map((player) => player.id);

  await db
    .insert(optimizerJobLineups)
    .values({
      jobId,
      lineupNum: lineupNumber,
      slotPlayerIdsJson: slotPlayerIds,
      playerIdsJson: playerIds,
      totalSalary: nextResult.lineup.totalSalary,
      projFpts: nextResult.lineup.projFpts,
      leverage: nextResult.lineup.leverageScore,
      durationMs: nextResult.summary.durationMs,
      winningStage: nextResult.summary.winningStage ?? null,
      attemptsJson: nextResult.summary.attempts,
    })
    .onConflictDoNothing();

  await db
    .update(optimizerJobs)
    .set({
      builtLineups: Math.max(job.builtLineups, lineupNumber),
      heartbeatAt: now,
      totalMs: computeJobTotalMs(job, now),
    })
    .where(eq(optimizerJobs.id, jobId));

  return { built: true };
}

function buildPartialWarning(built: number, requested: number): string | null {
  if (built === 0 || built >= requested) return null;
  return `Built ${built} of ${requested} lineups. Additional lineups were infeasible under the current exposure, lock, block, or stack constraints.`;
}

function buildLineupFailureError(
  summary: OptimizerDebugInfo["lineupSummaries"][number],
): string {
  const failedAttempt = [...summary.attempts].reverse().find((attempt) => !attempt.success);
  switch (failedAttempt?.failureReason) {
    case "exposure_exhausted":
      return "No additional lineups could be built because the exposure cap exhausted the remaining candidate pool.";
    case "no_valid_templates":
      return "No valid stack templates remained for the current stack and bring-back requirements.";
    case "diversity_repair_exhausted":
      return "A lineup candidate was found, but diversity repair could not produce a valid alternative under the current constraints.";
    case "salary_feasible_fill_not_found":
      return "No salary-feasible lineup fill was found for the remaining stack templates.";
    default:
      return "No additional lineups could be built under the current constraints.";
  }
}

export async function finalizeOptimizerJob(jobId: number) {
  await ensureOptimizerJobTables();
  const job = await readJob(jobId);
  if (!job) return;

  const lineups = await readJobLineups(jobId);
  const built = lineups.length;
  const now = new Date();
  const totalMs = job.startedAt ? Math.max(0, now.getTime() - job.startedAt.getTime()) : job.totalMs ?? 0;

  const status = built === 0 && normalizeTerminationReason(job.terminationReason) !== "completed"
    ? "failed"
    : "completed";
  const terminationReason = built >= job.requestedLineups
    ? "completed"
    : normalizeTerminationReason(job.terminationReason) ?? "lineup_failed";
  const warning = status === "completed" ? buildPartialWarning(built, job.requestedLineups) : null;
  const error = status === "failed"
    ? (job.error ?? "Optimizer did not finish successfully.")
    : null;

  await db
    .update(optimizerJobs)
    .set({
      status,
      builtLineups: built,
      warning,
      error,
      terminationReason,
      totalMs,
      heartbeatAt: now,
      finishedAt: now,
    })
    .where(eq(optimizerJobs.id, jobId));
}
