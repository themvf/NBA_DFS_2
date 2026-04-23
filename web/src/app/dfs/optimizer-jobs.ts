import "server-only";

import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { db } from "@/db";
import { optimizerJobLineups, optimizerJobs } from "@/db/schema";
import type { Sport } from "@/db/queries";
import {
  buildNextNbaLineup,
  computeNbaCeilingBonusMap,
  prepareNbaOptimizerRun,
  type GeneratedLineup,
  type LineupSlot,
  type OptimizerPlayer,
  type OptimizerSettings,
} from "./optimizer";
import {
  buildNextMlbLineup,
  computeMlbGpp2HitterOrderBonus,
  computeMlbHrInfluenceScore,
  computePitcherCeilingBonusMap,
  computeHrBonusMap,
  isPitcher,
  prepareMlbOptimizerRun,
  type MlbGeneratedLineup,
  type MlbLineupSlot,
  type MlbOptimizerPlayer,
  type MlbOptimizerSettings,
} from "./mlb-optimizer";
import { loadMlbHitterProjectionCalibration } from "./mlb-projection-calibration";
import { applyMlbHitterProjectionCalibration } from "./mlb-projection-utils";
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
  MlbLineupHrSignal,
  MlbLineupHrSignalPlayer,
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

function sanitizeProbability(value: number | null | undefined): number | null {
  const finite = finiteOrNull(value);
  return finite == null ? null : Math.max(0, Math.min(0.9999, finite));
}

function canonicalLineupSignature(playerIds: readonly number[]): string {
  return [...playerIds]
    .map((playerId) => Number(playerId))
    .filter((playerId) => Number.isFinite(playerId))
    .sort((a, b) => a - b)
    .join(",");
}

function roundMetric(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function roundMetricOrNull(value: number | null | undefined): number | null {
  return value == null || !Number.isFinite(value) ? null : roundMetric(value);
}

function average(values: Array<number | null | undefined>): number | null {
  const numeric = values.filter((value): value is number => value != null && Number.isFinite(value));
  if (numeric.length === 0) return null;
  return numeric.reduce((sum, value) => sum + value, 0) / numeric.length;
}

function americanOddsToProbability(price: number | null | undefined): number | null {
  const odds = finiteOrNull(price);
  if (odds == null || odds === 0) return null;
  return odds > 0
    ? 100 / (odds + 100)
    : Math.abs(odds) / (Math.abs(odds) + 100);
}

function computePoolLeverage(
  ourProj: number,
  projOwnPct: number,
  fieldProj: number | null = null,
): number {
  const edge = fieldProj != null ? ourProj - fieldProj : ourProj;
  const ownFraction = Math.max(0, Math.min(1, projOwnPct / 100));
  return Math.round(edge * Math.pow(1 - ownFraction, 0.7) * 1000) / 1000;
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
    ALTER TABLE optimizer_job_lineups
    ADD COLUMN IF NOT EXISTS actual_fpts DOUBLE PRECISION
  `));
  await db.execute(sql.raw(`
    ALTER TABLE optimizer_job_lineups
    ADD COLUMN IF NOT EXISTS mlb_hr_signal_json JSONB
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
type MlbPoolRow = MlbOptimizerPlayer & { avgFptsDk: number | null };

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
      dp.proj_ceiling AS "projCeiling",
      dp.boom_rate AS "boomRate",
      dp.prop_pts AS "propPts",
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
  const hitterProjectionCalibration = await loadMlbHitterProjectionCalibration();
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
      dp.avg_fpts_dk AS "avgFptsDk",
      dp.proj_ceiling AS "projCeiling",
      dp.boom_rate AS "boomRate",
      dp.expected_hr AS "expectedHr",
      dp.dk_in_starting_lineup AS "dkInStartingLineup",
      dp.dk_starting_lineup_order AS "dkStartingLineupOrder",
      dp.dk_team_lineup_confirmed AS "dkTeamLineupConfirmed",
      dp.is_out AS "isOut",
      dp.game_info AS "gameInfo",
      mt.logo_url AS "teamLogo",
      mt.name AS "teamName",
      mm.home_team_id AS "homeTeamId",
      mm.away_team_id AS "awayTeamId",
      mm.vegas_total AS "vegasTotal",
      mm.home_implied AS "homeImplied",
      mm.away_implied AS "awayImplied",
      dp.hr_prob_1plus AS "hrProb1Plus",
      dp.prop_pts AS "propPts",
      dp.prop_reb AS "propReb",
      dp.prop_ast AS "propAst",
      dp.prop_stl AS "propStl",
      dp.prop_stl_price AS "propStlPrice",
      dp.prop_stl_book AS "propStlBook"
    FROM dk_players dp
    LEFT JOIN mlb_teams mt ON mt.team_id = dp.mlb_team_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE dp.slate_id = ${slateId}
    ORDER BY dp.id ASC
  `);

  return rows.rows
    .map((player) => {
      const linestarProj = sanitizeProjection(player.linestarProj);
      const projOwnPct = sanitizeOwnershipPct(player.projOwnPct);
      const calibratedProj = isPitcher(player.eligiblePositions)
        ? sanitizeProjection(player.ourProj ?? linestarProj ?? null)
        : applyMlbHitterProjectionCalibration(
            sanitizeProjection(player.ourProj ?? linestarProj ?? null),
            player.dkTeamLineupConfirmed ? player.dkStartingLineupOrder ?? null : null,
            player.dkTeamLineupConfirmed ?? null,
            hitterProjectionCalibration,
          );
      const fieldProj = sanitizeProjection(player.avgFptsDk ?? linestarProj ?? null);
      const calibratedLeverage = !player.isOut && calibratedProj != null && projOwnPct != null
        ? sanitizeLeverage(computePoolLeverage(calibratedProj, projOwnPct, fieldProj))
        : sanitizeLeverage(player.ourLeverage);
      return {
        ...player,
        ourProj: calibratedProj,
        ourLeverage: calibratedLeverage,
        linestarProj,
        projOwnPct,
        expectedHr: sanitizeProjection(player.expectedHr),
        hrProb1Plus: sanitizeProbability(player.hrProb1Plus),
        propStl: finiteOrNull(player.propStl),
        propStlPrice: player.propStlPrice == null ? null : Math.round(Number(player.propStlPrice)),
        propStlBook: player.propStlBook ?? null,
      };
    })
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
    actualFpts: lineup.actualFpts ?? null,
    mlbHrSignal: (lineup.mlbHrSignalJson as MlbLineupHrSignal | null) ?? null,
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
          minChanges: mode === "gpp2" ? 4 : mode === "gpp" ? 3 : 2,
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
          minChanges: mode === "gpp2" ? 4 : mode === "gpp" ? 3 : 2,
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

export async function updateOptimizerJobLineupActualsForSlate(
  slateId: number,
): Promise<{ total: number; updated: number }> {
  await ensureOptimizerJobTables();

  await db.execute(sql`
    WITH lineup_totals AS (
      SELECT
        ojl.id AS lineup_id,
        COUNT(pid.player_id_text)::int AS player_count,
        COUNT(dp.actual_fpts)::int AS actual_count,
        SUM(dp.actual_fpts) AS total_fpts
      FROM optimizer_job_lineups ojl
      INNER JOIN optimizer_jobs oj
        ON oj.id = ojl.job_id
      LEFT JOIN LATERAL jsonb_array_elements_text(ojl.player_ids_json) AS pid(player_id_text)
        ON TRUE
      LEFT JOIN dk_players dp
        ON dp.id = pid.player_id_text::INTEGER
       AND dp.slate_id = oj.slate_id
      WHERE oj.slate_id = ${slateId}
      GROUP BY ojl.id
    )
    UPDATE optimizer_job_lineups ojl
    SET actual_fpts = CASE
      WHEN lt.player_count > 0 AND lt.actual_count = lt.player_count THEN lt.total_fpts
      ELSE NULL
    END
    FROM lineup_totals lt
    WHERE ojl.id = lt.lineup_id
  `);

  const counts = await db.execute<{ total: number; updated: number }>(sql`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE ojl.actual_fpts IS NOT NULL)::int AS updated
    FROM optimizer_job_lineups ojl
    INNER JOIN optimizer_jobs oj
      ON oj.id = ojl.job_id
    WHERE oj.slate_id = ${slateId}
  `);

  const row = counts.rows[0];
  return {
    total: row?.total ?? 0,
    updated: row?.updated ?? 0,
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

type OptimizerFeatureMetrics = {
  nJobs: number;
  nSlates: number;
  totalLineups: number;
  avgProjFpts: number | null;
  avgActualFpts: number | null;
  avgBeat: number | null;
  avgLeverage: number | null;
  cashRate: number | null;
  bestSingleLineup: number | null;
};

export type MlbHrCorrelationImpactRow = OptimizerFeatureMetrics & {
  hrCorrelation: boolean;
  hrCorrelationThreshold: number | null;
};

export type MlbPitcherCeilingImpactRow = OptimizerFeatureMetrics & {
  pitcherCeilingBoost: boolean;
  pitcherCeilingCount: number | null;
};

export type MlbAntiCorrelationImpactRow = OptimizerFeatureMetrics & {
  effectiveAntiCorrMax: number;
};

export type MlbOptimizerFeatureCombinationRow = OptimizerFeatureMetrics & {
  hrCorrelation: boolean;
  hrCorrelationThreshold: number | null;
  pitcherCeilingBoost: boolean;
  pitcherCeilingCount: number | null;
  effectiveAntiCorrMax: number;
};

export type MlbOptimizerHrSignalImpactRow = OptimizerFeatureMetrics & {
  bucket: string;
  avgHrTargets: number | null;
  avgOrder23HrTargets: number | null;
  avgLowOwnedHrTargets: number | null;
  avgTop15HrSelected: number | null;
  avgTop15EdgeSelected: number | null;
  avgTotalExpectedHr: number | null;
  avgPositiveEdgePts: number | null;
  avgHrInfluenceScore: number | null;
};

export type MlbOptimizerHrPlayerImpactRow = {
  bucket: string;
  exposures: number;
  uniquePlayers: number;
  nJobs: number;
  nSlates: number;
  avgHrProbPct: number | null;
  avgExpectedHr: number | null;
  avgMarketHrProbPct: number | null;
  avgEdgePct: number | null;
  avgInfluenceScore: number | null;
  avgProjectedOwnPct: number | null;
  avgActualOwnPct: number | null;
  avgProjection: number | null;
  avgActualFpts: number | null;
  avgBeat: number | null;
  hrRate: number | null;
  avgActualHr: number | null;
};

export type MlbOptimizerHrPlayerLeaderRow = {
  playerId: number;
  name: string;
  teamAbbrev: string;
  exposures: number;
  nSlates: number;
  avgHrProbPct: number | null;
  avgExpectedHr: number | null;
  avgEdgePct: number | null;
  avgInfluenceScore: number | null;
  avgProjectedOwnPct: number | null;
  avgActualOwnPct: number | null;
  avgActualFpts: number | null;
  avgBeat: number | null;
  hrRate: number | null;
  actualHr: number;
};

export type MlbOptimizerFeatureImpactSummary = {
  hrCorrelation: MlbHrCorrelationImpactRow[];
  pitcherCeiling: MlbPitcherCeilingImpactRow[];
  antiCorrelation: MlbAntiCorrelationImpactRow[];
  hrSignal: MlbOptimizerHrSignalImpactRow[];
  hrPlayerBuckets: MlbOptimizerHrPlayerImpactRow[];
  hrPlayerLeaders: MlbOptimizerHrPlayerLeaderRow[];
  combinations: MlbOptimizerFeatureCombinationRow[];
};

export async function getMlbOptimizerFeatureImpactSummary(): Promise<MlbOptimizerFeatureImpactSummary> {
  await ensureOptimizerJobTables();

  const baseCte = `
    WITH lineup_actuals AS (
      SELECT
        ojl.id AS lineup_id,
        oj.id AS job_id,
        oj.slate_id,
        COALESCE((oj.settings_json ->> 'hrCorrelation')::boolean, false) AS hr_correlation,
        CASE
          WHEN COALESCE((oj.settings_json ->> 'hrCorrelation')::boolean, false)
          THEN (oj.settings_json ->> 'hrCorrelationThreshold')::double precision
          ELSE NULL::double precision
        END AS hr_correlation_threshold,
        COALESCE((oj.settings_json ->> 'pitcherCeilingBoost')::boolean, false) AS pitcher_ceiling_boost,
        CASE
          WHEN COALESCE((oj.settings_json ->> 'pitcherCeilingBoost')::boolean, false)
          THEN (oj.settings_json ->> 'pitcherCeilingCount')::integer
          ELSE NULL::integer
        END AS pitcher_ceiling_count,
        COALESCE(
          (oj.effective_settings_json ->> 'antiCorrMax')::integer,
          (oj.settings_json ->> 'antiCorrMax')::integer,
          10
        ) AS effective_anti_corr_max,
        ojl.proj_fpts,
        ojl.actual_fpts AS stored_actual_fpts,
        ojl.leverage,
        ds.cash_line,
        ojl.mlb_hr_signal_json,
        COUNT(pid.player_id_text)::int AS player_count,
        COUNT(dp.actual_fpts)::int AS actual_count,
        SUM(dp.actual_fpts) AS derived_actual_fpts
      FROM optimizer_job_lineups ojl
      INNER JOIN optimizer_jobs oj
        ON oj.id = ojl.job_id
      INNER JOIN dk_slates ds
        ON ds.id = oj.slate_id
      LEFT JOIN LATERAL jsonb_array_elements_text(ojl.player_ids_json) AS pid(player_id_text)
        ON TRUE
      LEFT JOIN dk_players dp
        ON dp.id = pid.player_id_text::INTEGER
       AND dp.slate_id = oj.slate_id
      WHERE oj.sport = 'mlb'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    ),
    base AS (
      SELECT
        job_id,
        slate_id,
        hr_correlation,
        hr_correlation_threshold,
        pitcher_ceiling_boost,
        pitcher_ceiling_count,
        effective_anti_corr_max,
        proj_fpts,
        COALESCE(
          stored_actual_fpts,
          CASE
            WHEN player_count > 0 AND actual_count = player_count THEN derived_actual_fpts
            ELSE NULL::double precision
          END
        ) AS actual_fpts,
        leverage,
        cash_line,
        (mlb_hr_signal_json IS NOT NULL) AS has_hr_signal,
        COALESCE((mlb_hr_signal_json ->> 'hrTargetCount')::integer, 0) AS hr_target_count,
        COALESCE((mlb_hr_signal_json ->> 'highHrTargetCount')::integer, 0) AS high_hr_target_count,
        COALESCE((mlb_hr_signal_json ->> 'order23HrTargetCount')::integer, 0) AS order23_hr_target_count,
        COALESCE((mlb_hr_signal_json ->> 'positiveEdgeCount')::integer, 0) AS positive_edge_count,
        COALESCE((mlb_hr_signal_json ->> 'lowOwnedHrTargetCount')::integer, 0) AS low_owned_hr_target_count,
        COALESCE((mlb_hr_signal_json ->> 'selectedTop15HrCount')::integer, 0) AS selected_top15_hr_count,
        COALESCE((mlb_hr_signal_json ->> 'selectedTop15EdgeCount')::integer, 0) AS selected_top15_edge_count,
        COALESCE((mlb_hr_signal_json ->> 'totalExpectedHr')::double precision, 0) AS total_expected_hr,
        COALESCE((mlb_hr_signal_json ->> 'totalPositiveEdgePct')::double precision, 0) AS total_positive_edge_pct,
        COALESCE((mlb_hr_signal_json ->> 'totalHrInfluenceScore')::double precision, 0) AS total_hr_influence_score
      FROM lineup_actuals
      WHERE COALESCE(
        stored_actual_fpts,
        CASE
          WHEN player_count > 0 AND actual_count = player_count THEN derived_actual_fpts
          ELSE NULL::double precision
        END
      ) IS NOT NULL
    )
  `;

  const sharedMetrics = `
      COUNT(DISTINCT job_id)::int AS "nJobs",
      COUNT(DISTINCT slate_id)::int AS "nSlates",
      COUNT(*)::int AS "totalLineups",
      AVG(proj_fpts) AS "avgProjFpts",
      AVG(actual_fpts) AS "avgActualFpts",
      AVG(actual_fpts - proj_fpts) AS "avgBeat",
      AVG(leverage) AS "avgLeverage",
      ROUND(
        (100.0 * COUNT(*) FILTER (WHERE actual_fpts >= COALESCE(cash_line, 300)) / NULLIF(COUNT(*), 0))::numeric,
        1
      )::double precision AS "cashRate",
      MAX(actual_fpts) AS "bestSingleLineup"
  `;

  const hrCorrelation = await db.execute<MlbHrCorrelationImpactRow>(sql.raw(`
    ${baseCte}
    SELECT
      hr_correlation AS "hrCorrelation",
      hr_correlation_threshold AS "hrCorrelationThreshold",
      ${sharedMetrics}
    FROM base
    GROUP BY 1, 2
    ORDER BY 1 DESC, 2 NULLS FIRST
  `));

  const pitcherCeiling = await db.execute<MlbPitcherCeilingImpactRow>(sql.raw(`
    ${baseCte}
    SELECT
      pitcher_ceiling_boost AS "pitcherCeilingBoost",
      pitcher_ceiling_count AS "pitcherCeilingCount",
      ${sharedMetrics}
    FROM base
    GROUP BY 1, 2
    ORDER BY 1 DESC, 2 NULLS FIRST
  `));

  const antiCorrelation = await db.execute<MlbAntiCorrelationImpactRow>(sql.raw(`
    ${baseCte}
    SELECT
      effective_anti_corr_max AS "effectiveAntiCorrMax",
      ${sharedMetrics}
    FROM base
    GROUP BY 1
    ORDER BY 1 ASC
  `));

  const hrSignal = await db.execute<MlbOptimizerHrSignalImpactRow>(sql.raw(`
    ${baseCte}
    SELECT
      CASE
        WHEN hr_target_count >= 4 THEN '4+ HR targets'
        WHEN hr_target_count >= 2 THEN '2-3 HR targets'
        WHEN hr_target_count = 1 THEN '1 HR target'
        ELSE '0 HR targets'
      END AS "bucket",
      AVG(hr_target_count) AS "avgHrTargets",
      AVG(order23_hr_target_count) AS "avgOrder23HrTargets",
      AVG(low_owned_hr_target_count) AS "avgLowOwnedHrTargets",
      AVG(selected_top15_hr_count) AS "avgTop15HrSelected",
      AVG(selected_top15_edge_count) AS "avgTop15EdgeSelected",
      AVG(total_expected_hr) AS "avgTotalExpectedHr",
      AVG(total_positive_edge_pct) AS "avgPositiveEdgePts",
      AVG(total_hr_influence_score) AS "avgHrInfluenceScore",
      ${sharedMetrics}
    FROM base
    WHERE has_hr_signal
    GROUP BY 1
    ORDER BY
      CASE
        WHEN MIN(hr_target_count) >= 4 THEN 4
        WHEN MIN(hr_target_count) >= 2 THEN 3
        WHEN MIN(hr_target_count) = 1 THEN 2
        ELSE 1
      END DESC
  `));

  const playerBaseCte = `
    WITH player_exposures AS (
      SELECT
        oj.id AS job_id,
        oj.slate_id,
        (player_signal ->> 'playerId')::integer AS player_id,
        COALESCE(NULLIF(player_signal ->> 'name', ''), dp.name) AS name,
        COALESCE(NULLIF(player_signal ->> 'teamAbbrev', ''), dp.team_abbrev) AS team_abbrev,
        (player_signal ->> 'projection')::double precision AS projection,
        (player_signal ->> 'projectedOwnership')::double precision AS projected_own_pct,
        (player_signal ->> 'hrProb1Plus')::double precision AS hr_prob,
        (player_signal ->> 'expectedHr')::double precision AS expected_hr,
        (player_signal ->> 'marketHrProb')::double precision AS market_hr_prob,
        (player_signal ->> 'hrEdgePct')::double precision AS hr_edge_pct,
        (player_signal ->> 'hrInfluenceScore')::double precision AS hr_influence_score,
        COALESCE((player_signal ->> 'isHrTarget')::boolean, false) AS is_hr_target,
        COALESCE((player_signal ->> 'isHighHrTarget')::boolean, false) AS is_high_hr_target,
        COALESCE((player_signal ->> 'isOrder23')::boolean, false) AS is_order23,
        COALESCE((player_signal ->> 'isPositiveEdge')::boolean, false) AS is_positive_edge,
        COALESCE((player_signal ->> 'isTop15Hr')::boolean, false) AS is_top15_hr,
        COALESCE((player_signal ->> 'isTop15Edge')::boolean, false) AS is_top15_edge,
        COALESCE((player_signal ->> 'isHrTarget')::boolean, false)
          AND COALESCE((player_signal ->> 'projectedOwnership')::double precision, 999) <= 12 AS is_low_owned_hr_target,
        dp.actual_fpts,
        dp.actual_own_pct,
        dp.actual_hr
      FROM optimizer_job_lineups ojl
      INNER JOIN optimizer_jobs oj
        ON oj.id = ojl.job_id
      INNER JOIN LATERAL jsonb_array_elements(ojl.mlb_hr_signal_json -> 'players') AS player_json(player_signal)
        ON TRUE
      LEFT JOIN dk_players dp
        ON dp.id = (player_signal ->> 'playerId')::integer
       AND dp.slate_id = oj.slate_id
      WHERE oj.sport = 'mlb'
        AND ojl.mlb_hr_signal_json IS NOT NULL
        AND dp.actual_fpts IS NOT NULL
    ),
    bucketed AS (
      SELECT 'All selected hitters' AS bucket, * FROM player_exposures
      UNION ALL
      SELECT 'HR targets', * FROM player_exposures WHERE is_hr_target
      UNION ALL
      SELECT 'High HR targets', * FROM player_exposures WHERE is_high_hr_target
      UNION ALL
      SELECT '#2/#3 HR targets', * FROM player_exposures WHERE is_order23 AND is_hr_target
      UNION ALL
      SELECT 'Low-owned HR targets', * FROM player_exposures WHERE is_low_owned_hr_target
      UNION ALL
      SELECT 'Positive-edge hitters', * FROM player_exposures WHERE is_positive_edge
      UNION ALL
      SELECT 'Top-15 HR selected', * FROM player_exposures WHERE is_top15_hr
      UNION ALL
      SELECT 'Top-15 edge selected', * FROM player_exposures WHERE is_top15_edge
    )
  `;

  const playerBucketMetrics = `
      COUNT(*)::int AS "exposures",
      COUNT(DISTINCT player_id)::int AS "uniquePlayers",
      COUNT(DISTINCT job_id)::int AS "nJobs",
      COUNT(DISTINCT slate_id)::int AS "nSlates",
      AVG(hr_prob) * 100 AS "avgHrProbPct",
      AVG(expected_hr) AS "avgExpectedHr",
      AVG(market_hr_prob) * 100 AS "avgMarketHrProbPct",
      AVG(hr_edge_pct) AS "avgEdgePct",
      AVG(hr_influence_score) AS "avgInfluenceScore",
      AVG(projected_own_pct) AS "avgProjectedOwnPct",
      AVG(actual_own_pct) AS "avgActualOwnPct",
      AVG(projection) AS "avgProjection",
      AVG(actual_fpts) AS "avgActualFpts",
      AVG(actual_fpts - projection) AS "avgBeat",
      AVG(CASE WHEN actual_hr > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE actual_hr IS NOT NULL) * 100 AS "hrRate",
      AVG(actual_hr) FILTER (WHERE actual_hr IS NOT NULL) AS "avgActualHr"
  `;

  const hrPlayerBuckets = await db.execute<MlbOptimizerHrPlayerImpactRow>(sql.raw(`
    ${playerBaseCte}
    SELECT
      bucket AS "bucket",
      ${playerBucketMetrics}
    FROM bucketed
    GROUP BY 1
    ORDER BY
      CASE bucket
        WHEN 'All selected hitters' THEN 1
        WHEN 'HR targets' THEN 2
        WHEN 'High HR targets' THEN 3
        WHEN '#2/#3 HR targets' THEN 4
        WHEN 'Low-owned HR targets' THEN 5
        WHEN 'Positive-edge hitters' THEN 6
        WHEN 'Top-15 HR selected' THEN 7
        WHEN 'Top-15 edge selected' THEN 8
        ELSE 99
      END ASC
  `));

  const hrPlayerLeaders = await db.execute<MlbOptimizerHrPlayerLeaderRow>(sql.raw(`
    ${playerBaseCte}
    SELECT
      player_id AS "playerId",
      MIN(name) AS "name",
      MIN(team_abbrev) AS "teamAbbrev",
      COUNT(*)::int AS "exposures",
      COUNT(DISTINCT slate_id)::int AS "nSlates",
      AVG(hr_prob) * 100 AS "avgHrProbPct",
      AVG(expected_hr) AS "avgExpectedHr",
      AVG(hr_edge_pct) AS "avgEdgePct",
      AVG(hr_influence_score) AS "avgInfluenceScore",
      AVG(projected_own_pct) AS "avgProjectedOwnPct",
      AVG(actual_own_pct) AS "avgActualOwnPct",
      AVG(actual_fpts) AS "avgActualFpts",
      AVG(actual_fpts - projection) AS "avgBeat",
      AVG(CASE WHEN actual_hr > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE actual_hr IS NOT NULL) * 100 AS "hrRate",
      COALESCE(SUM(actual_hr) FILTER (WHERE actual_hr IS NOT NULL), 0)::int AS "actualHr"
    FROM player_exposures
    WHERE is_hr_target
      OR is_high_hr_target
      OR is_top15_hr
      OR is_top15_edge
      OR is_positive_edge
      OR hr_influence_score > 0
    GROUP BY player_id
    ORDER BY
      COALESCE(SUM(actual_hr) FILTER (WHERE actual_hr IS NOT NULL), 0) DESC,
      AVG(actual_fpts - projection) DESC NULLS LAST,
      AVG(hr_influence_score) DESC NULLS LAST,
      COUNT(*) DESC
    LIMIT 12
  `));

  const combinations = await db.execute<MlbOptimizerFeatureCombinationRow>(sql.raw(`
    ${baseCte}
    SELECT
      hr_correlation AS "hrCorrelation",
      hr_correlation_threshold AS "hrCorrelationThreshold",
      pitcher_ceiling_boost AS "pitcherCeilingBoost",
      pitcher_ceiling_count AS "pitcherCeilingCount",
      effective_anti_corr_max AS "effectiveAntiCorrMax",
      ${sharedMetrics}
    FROM base
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1 DESC, 3 DESC, 5 ASC, 2 NULLS FIRST, 4 NULLS FIRST
  `));

  return {
    hrCorrelation: hrCorrelation.rows,
    pitcherCeiling: pitcherCeiling.rows,
    antiCorrelation: antiCorrelation.rows,
    hrSignal: hrSignal.rows,
    hrPlayerBuckets: hrPlayerBuckets.rows,
    hrPlayerLeaders: hrPlayerLeaders.rows,
    combinations: combinations.rows,
  };
}

export async function createOptimizerJob(input: CreateOptimizerJobRequest): Promise<{ jobId: number; existing: boolean }> {
  await ensureOptimizerJobTables();

  const existing = await getActiveOptimizerJobStatus(input.clientToken, input.sport, input.slateId);
  if (existing) {
    return { jobId: existing.job.id, existing: true };
  }

  if (input.sport === "nba" || input.sport === "mlb") {
    const { refreshPlayerStatus } = await import("./actions");
    const refreshResult = await refreshPlayerStatus(input.slateId);
    if (!refreshResult.ok) {
      throw new Error(`${input.sport.toUpperCase()} status refresh failed before optimize: ${refreshResult.message}`);
    }
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

const MLB_HR_SIGNAL_TARGET_THRESHOLD = 0.12;
const MLB_HR_SIGNAL_HIGH_THRESHOLD = 0.18;
const MLB_HR_SIGNAL_LOW_OWNERSHIP_THRESHOLD = 12;

function getMlbPlayerHrEdgePct(player: MlbOptimizerPlayer): number | null {
  const hrProb = sanitizeProbability(player.hrProb1Plus);
  const marketProb = americanOddsToProbability(player.propStlPrice);
  if (hrProb == null || marketProb == null) return null;
  return (hrProb - marketProb) * 100;
}

function buildMlbLineupHrSignal(
  lineup: MlbGeneratedLineup,
  prepared: MlbPreparedOptimizerRun,
): MlbLineupHrSignal {
  const poolHitters = prepared.pool.filter((player) => !isPitcher(player.eligiblePositions));
  const top15HrIds = new Set(
    [...poolHitters]
      .filter((player) => sanitizeProbability(player.hrProb1Plus) != null)
      .sort((a, b) => (sanitizeProbability(b.hrProb1Plus) ?? -1) - (sanitizeProbability(a.hrProb1Plus) ?? -1))
      .slice(0, 15)
      .map((player) => player.id),
  );
  const top15EdgeIds = new Set(
    [...poolHitters]
      .map((player) => ({ player, edge: getMlbPlayerHrEdgePct(player) }))
      .filter((entry): entry is { player: MlbOptimizerPlayer; edge: number } => entry.edge != null)
      .sort((a, b) => b.edge - a.edge)
      .slice(0, 15)
      .map((entry) => entry.player.id),
  );

  const players: MlbLineupHrSignalPlayer[] = lineup.players
    .filter((player) => !isPitcher(player.eligiblePositions))
    .map((player) => {
      const hrProb = sanitizeProbability(player.hrProb1Plus);
      const expectedHr = sanitizeProjection(player.expectedHr);
      const projectedOwnership = sanitizeOwnershipPct(player.projOwnPct);
      const marketHrProb = americanOddsToProbability(player.propStlPrice);
      const hrEdgePct = getMlbPlayerHrEdgePct(player);
      const lineupOrder = player.dkStartingLineupOrder ?? null;
      const hrCorrelationBonus = roundMetric(Number(prepared.hrBonusRecord?.[player.id] ?? 0));
      const gpp2OrderBonus = computeMlbGpp2HitterOrderBonus(player, prepared.mode);
      const hrInfluenceScore = computeMlbHrInfluenceScore(player, prepared.mode, hrCorrelationBonus);

      return {
        playerId: player.id,
        dkPlayerId: player.dkPlayerId == null ? null : Number(player.dkPlayerId),
        name: player.name,
        teamAbbrev: player.teamAbbrev,
        eligiblePositions: player.eligiblePositions,
        salary: player.salary,
        lineupOrder,
        projection: roundMetricOrNull(player.ourProj),
        projectedOwnership: roundMetricOrNull(projectedOwnership),
        hrProb1Plus: roundMetricOrNull(hrProb),
        expectedHr: roundMetricOrNull(expectedHr),
        marketHrProb: roundMetricOrNull(marketHrProb),
        hrEdgePct: roundMetricOrNull(hrEdgePct),
        hrInfluenceScore,
        gpp2OrderBonus,
        hrCorrelationBonus,
        isHrTarget: (hrProb ?? 0) >= MLB_HR_SIGNAL_TARGET_THRESHOLD,
        isHighHrTarget: (hrProb ?? 0) >= MLB_HR_SIGNAL_HIGH_THRESHOLD,
        isOrder23: lineupOrder === 2 || lineupOrder === 3,
        isPositiveEdge: (hrEdgePct ?? Number.NEGATIVE_INFINITY) > 0,
        isTop15Hr: top15HrIds.has(player.id),
        isTop15Edge: top15EdgeIds.has(player.id),
      };
    });

  const hrProbs = players.map((player) => player.hrProb1Plus);
  const expectedHrs = players.map((player) => player.expectedHr);
  const marketProbs = players.map((player) => player.marketHrProb);
  const ownerships = players.map((player) => player.projectedOwnership);
  const edgePcts = players.map((player) => player.hrEdgePct);

  return {
    version: "mlb_dfs_hr_signal_v1",
    mode: prepared.mode,
    hitterCount: players.length,
    hrTargetCount: players.filter((player) => player.isHrTarget).length,
    highHrTargetCount: players.filter((player) => player.isHighHrTarget).length,
    order23HitterCount: players.filter((player) => player.isOrder23).length,
    order23HrTargetCount: players.filter((player) => player.isOrder23 && player.isHrTarget).length,
    positiveEdgeCount: players.filter((player) => player.isPositiveEdge).length,
    lowOwnedHrTargetCount: players.filter((player) =>
      player.isHrTarget
      && player.projectedOwnership != null
      && player.projectedOwnership <= MLB_HR_SIGNAL_LOW_OWNERSHIP_THRESHOLD
    ).length,
    selectedTop15HrCount: players.filter((player) => player.isTop15Hr).length,
    selectedTop15EdgeCount: players.filter((player) => player.isTop15Edge).length,
    avgHrProb: roundMetricOrNull(average(hrProbs)),
    maxHrProb: roundMetricOrNull(hrProbs.reduce<number | null>((best, value) =>
      value == null ? best : best == null ? value : Math.max(best, value), null)),
    totalExpectedHr: roundMetric(expectedHrs.reduce<number>((sum, value) => sum + (value ?? 0), 0)),
    avgExpectedHr: roundMetricOrNull(average(expectedHrs)),
    avgMarketHrProb: roundMetricOrNull(average(marketProbs)),
    maxEdgePct: roundMetricOrNull(edgePcts.reduce<number | null>((best, value) =>
      value == null ? best : best == null ? value : Math.max(best, value), null)),
    totalPositiveEdgePct: roundMetric(edgePcts.reduce<number>((sum, value) => sum + Math.max(0, value ?? 0), 0)),
    avgProjectedOwnership: roundMetricOrNull(average(ownerships)),
    totalHrInfluenceScore: roundMetric(players.reduce((sum, player) => sum + player.hrInfluenceScore, 0)),
    gpp2OrderBonusTotal: roundMetric(players.reduce((sum, player) => sum + player.gpp2OrderBonus, 0)),
    hrCorrelationBonusTotal: roundMetric(players.reduce((sum, player) => sum + player.hrCorrelationBonus, 0)),
    players,
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
    const mlbSettings = settings as MlbOptimizerSettings;
    const mlbPool = pool as MlbOptimizerPlayer[];
    const batters = mlbPool.filter((p) => !isPitcher(p.eligiblePositions));
    const pitchers = mlbPool.filter((p) => isPitcher(p.eligiblePositions));
    const hrBonusMap = mlbSettings.hrCorrelation
      ? computeHrBonusMap(batters, mlbSettings.hrCorrelationThreshold)
      : new Map<number, number>();
    const pitcherCeilingBonusMap = mlbSettings.pitcherCeilingBoost
      ? computePitcherCeilingBonusMap(pitchers, mlbSettings.pitcherCeilingCount)
      : new Map<number, number>();
    const hrBonusRecord: Record<number, number> = Object.fromEntries(hrBonusMap);
    const pitcherCeilingBonusRecord: Record<number, number> = Object.fromEntries(pitcherCeilingBonusMap);
    return {
      sport: "mlb",
      mode: settings.mode,
      requestedLineups: job.requestedLineups,
      maxExposureCount: Math.ceil(job.requestedLineups * mlbSettings.maxExposure),
      eligibleCount: job.eligibleCount ?? pool.length,
      pool: mlbPool,
      ruleSelections: normalizeMlbRuleSelections(mlbSettings),
      effectiveSettings: {
        minStack: effectiveSettings.minStack,
        bringBackThreshold: effectiveSettings.bringBackThreshold ?? 0,
        maxExposure: effectiveSettings.maxExposure,
        minChanges: effectiveSettings.minChanges,
        antiCorrMax: effectiveSettings.antiCorrMax ?? 10,
        pendingLineupPolicy: normalizeMlbPendingLineupPolicy(effectiveSettings.pendingLineupPolicy),
      },
      hrBonusRecord,
      pitcherCeilingBonusRecord,
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
      ceilingBoost: effectiveSettings.ceilingBoost ?? false,
      ceilingCount: effectiveSettings.ceilingCount ?? 3,
      maxExposure: effectiveSettings.maxExposure,
      minChanges: effectiveSettings.minChanges,
      salaryFloor: effectiveSettings.salaryFloor ?? 49000,
    },
    ceilingBonusRecord: Object.fromEntries(
      (effectiveSettings.ceilingBoost ?? false)
        ? computeNbaCeilingBonusMap(
            pool as OptimizerPlayer[],
            effectiveSettings.ceilingCount ?? 3,
          )
        : new Map<number, number>(),
    ),
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
  const priorLineupSignatures = new Set(
    priorLineupPlayerIds.map((playerIds) => canonicalLineupSignature(playerIds)),
  );
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
  const lineupSignature = canonicalLineupSignature(playerIds);
  if (priorLineupSignatures.has(lineupSignature)) {
    await db
      .update(optimizerJobs)
      .set({
        builtLineups: priorLineups.length,
        heartbeatAt: now,
        terminationReason: "lineup_failed",
        error: "Optimizer generated a duplicate lineup. No additional unique lineups could be built under the current constraints.",
        totalMs: computeJobTotalMs(job, now),
      })
      .where(eq(optimizerJobs.id, jobId));
    return { built: false };
  }
  const mlbHrSignal = prepared.sport === "mlb"
    ? buildMlbLineupHrSignal(nextResult.lineup as MlbGeneratedLineup, prepared as MlbPreparedOptimizerRun)
    : null;

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
      mlbHrSignalJson: mlbHrSignal,
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
