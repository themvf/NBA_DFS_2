import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { ensureNflContextTables } from "@/db/ensure-schema";

export type NflContextUsage =
  | "descriptive"
  | "predictive"
  | "scenario"
  | "decision";

export type NflContextMeasurement = {
  contextSnapshotId: string;
  definitionId: string;
  subject: { type: string; id: string };
  targetId: string;
  asOfAt: string;
  availableAt: string;
  window: Record<string, unknown>;
  measurement: {
    numerator: number | null;
    denominator: number | null;
    value: number | null;
    state: "observed" | "estimated" | "scenario";
  };
  coverage: Record<string, unknown>;
  uncertainty: Record<string, unknown> | null;
  estimation: Record<string, unknown> | null;
  sourceSnapshotIds: string[];
  factReleaseId: string;
  payload: Record<string, unknown>;
};

export type NflResolvedContext = {
  measurement: NflContextMeasurement;
  manifest: {
    contextSnapshotId: string;
    definitionId: string;
    factReleaseId: string;
    sourceSnapshotIds: string[];
    policyVersion: string;
    eligibility: {
      consumerId: string;
      useCase: string;
      cohort: string;
      usage: NflContextUsage;
      approved: true;
      fallbackUsed: string | null;
    };
  };
};

export type NflContextResearchSummary = {
  dfs: { status: string; maeGain: number | null; runId: string } | null;
  market: {
    sampleRows: number;
    folds: Array<{ season: number; maeGain: number }>;
    associations: Array<{ feature: string; standardizedPoints: number }>;
    runId: string;
  } | null;
  props: { observations: number; latestCapture: string | null };
};

export type NflAvailabilityCoverage = {
  players: NflResolvedContext[];
  quarterbacks: NflResolvedContext[];
  stateCounts: Record<string, number>;
  conflicts: number;
  stale: number;
  unknown: number;
};

type Request = {
  consumerId: string;
  useCase: string;
  cohort: string;
  usage: NflContextUsage;
  requestedAsOf: Date;
};

type Qualification = {
  definitionId: string;
  policyVersion: string;
  maxAgeSeconds: number | null;
  fallbackDefinitionId: string | null;
};

type SnapshotRow = {
  snapshot_id: string;
  definition_id: string;
  subject_type: string;
  subject_id: string;
  target_id: string;
  as_of_at: Date | string;
  available_at: Date | string;
  measurement_window: Record<string, unknown>;
  numerator: number | null;
  denominator: number | null;
  value: number | null;
  value_state: "observed" | "estimated" | "scenario";
  coverage: Record<string, unknown>;
  uncertainty: Record<string, unknown> | null;
  estimation: Record<string, unknown> | null;
  source_snapshot_ids: string[];
  fact_release_id: string;
  payload: Record<string, unknown>;
};

async function qualification(
  request: Request,
  definitionId: string,
): Promise<Qualification> {
  const result = await db.execute(sql`
    SELECT q.definition_id, q.policy_version, q.max_age_seconds,
           q.fallback_definition_id
    FROM nfl_context_qualifications q
    JOIN nfl_consumer_policy_pointers p
      ON p.consumer_id = q.consumer_id
     AND p.policy_version = q.policy_version
    WHERE q.consumer_id = ${request.consumerId}
      AND q.definition_id = ${definitionId}
      AND q.use_case = ${request.useCase}
      AND q.cohort = ${request.cohort}
      AND q.usage = ${request.usage}
      AND q.approved = TRUE
    LIMIT 1
  `);
  const row = result.rows[0] as Record<string, unknown> | undefined;
  if (!row) {
    throw new Error(
      `Unqualified NFL context dependency: ${request.consumerId}/${definitionId}/${request.useCase}/${request.cohort}/${request.usage}`,
    );
  }
  return {
    definitionId: String(row.definition_id),
    policyVersion: String(row.policy_version),
    maxAgeSeconds:
      row.max_age_seconds == null ? null : Number(row.max_age_seconds),
    fallbackDefinitionId:
      row.fallback_definition_id == null
        ? null
        : String(row.fallback_definition_id),
  };
}

function resolved(
  row: SnapshotRow,
  request: Request,
  policy: Qualification,
  fallbackUsed: string | null,
): NflResolvedContext {
  const asOf = new Date(row.as_of_at);
  const availableAt = new Date(row.available_at ?? row.as_of_at);
  if (asOf > request.requestedAsOf) {
    throw new Error("NFL context decision time is after the requested as-of time");
  }
  if (availableAt > request.requestedAsOf) {
    throw new Error("NFL context snapshot was not published by the requested as-of time");
  }
  if (
    policy.maxAgeSeconds != null &&
    (request.requestedAsOf.getTime() - asOf.getTime()) / 1000 >
      policy.maxAgeSeconds
  ) {
    throw new Error("NFL context snapshot is stale under the active consumer policy");
  }
  const sourceSnapshotIds = Array.isArray(row.source_snapshot_ids)
    ? row.source_snapshot_ids.map(String)
    : [];
  return {
    measurement: {
      contextSnapshotId: row.snapshot_id,
      definitionId: row.definition_id,
      subject: { type: row.subject_type, id: row.subject_id },
      targetId: row.target_id,
      asOfAt: asOf.toISOString(),
      availableAt: availableAt.toISOString(),
      window: row.measurement_window,
      measurement: {
        numerator: row.numerator,
        denominator: row.denominator,
        value: row.value,
        state: row.value_state,
      },
      coverage: row.coverage,
      uncertainty: row.uncertainty,
      estimation: row.estimation,
      sourceSnapshotIds,
      factReleaseId: row.fact_release_id,
      payload: row.payload ?? {},
    },
    manifest: {
      contextSnapshotId: row.snapshot_id,
      definitionId: row.definition_id,
      factReleaseId: row.fact_release_id,
      sourceSnapshotIds,
      policyVersion: policy.policyVersion,
      eligibility: {
        consumerId: request.consumerId,
        useCase: request.useCase,
        cohort: request.cohort,
        usage: request.usage,
        approved: true,
        fallbackUsed,
      },
    },
  };
}

async function currentSnapshot(
  definitionId: string,
  subjectId: string,
  targetId: string,
  requestedAsOf: Date,
): Promise<SnapshotRow | null> {
  const result = await db.execute(sql`
    SELECT * FROM nfl_context_snapshots
    WHERE definition_id = ${definitionId}
      AND subject_id = ${subjectId}
      AND target_id = ${targetId}
      AND publication_status = 'current'
      AND as_of_at <= ${requestedAsOf}
      AND available_at <= ${requestedAsOf}
    ORDER BY as_of_at DESC, snapshot_id DESC
    LIMIT 1
  `);
  return (result.rows[0] as SnapshotRow | undefined) ?? null;
}

export async function getPinnedNflContext(
  request: Request & { snapshotId: string },
): Promise<NflResolvedContext> {
  await ensureNflContextTables();
  const result = await db.execute(sql`
    SELECT * FROM nfl_context_snapshots WHERE snapshot_id = ${request.snapshotId}
  `);
  const row = result.rows[0] as SnapshotRow | undefined;
  if (!row) throw new Error(`Unknown NFL context snapshot ${request.snapshotId}`);
  const policy = await qualification(request, row.definition_id);
  return resolved(row, request, policy, null);
}

export async function getCurrentEligibleNflContext(
  request: Request & {
    definitionId: string;
    subjectId: string;
    targetId: string;
  },
): Promise<NflResolvedContext> {
  await ensureNflContextTables();
  const policy = await qualification(request, request.definitionId);
  const primary = await currentSnapshot(
    request.definitionId,
    request.subjectId,
    request.targetId,
    request.requestedAsOf,
  );
  if (primary) {
    try {
      return resolved(primary, request, policy, null);
    } catch (error) {
      if (!policy.fallbackDefinitionId) throw error;
    }
  } else if (!policy.fallbackDefinitionId) {
    throw new Error(`No eligible NFL context snapshot for ${request.definitionId}`);
  }

  const fallbackId = policy.fallbackDefinitionId!;
  const fallbackPolicy = await qualification(request, fallbackId);
  const fallback = await currentSnapshot(
    fallbackId,
    request.subjectId,
    request.targetId,
    request.requestedAsOf,
  );
  if (!fallback) throw new Error(`No eligible NFL context fallback ${fallbackId}`);
  return resolved(fallback, request, fallbackPolicy, fallbackId);
}

export async function getCurrentNflAvailabilityCoverage(
  targetId: string,
  requestedAsOf: Date,
): Promise<NflAvailabilityCoverage> {
  await ensureNflContextTables();
  const request: Request = {
    consumerId: "nfl_availability_vercel",
    useCase: "availability_display",
    cohort: "all",
    usage: "descriptive",
    requestedAsOf,
  };
  const definitionIds = ["player_game_availability@v1", "team_qb_state@v1"];
  const policies = new Map<string, Qualification>();
  for (const definitionId of definitionIds) {
    policies.set(definitionId, await qualification(request, definitionId));
  }
  const result = await db.execute(sql`
    SELECT DISTINCT ON (definition_id, subject_id, target_id) *
    FROM nfl_context_snapshots
    WHERE definition_id IN ('player_game_availability@v1', 'team_qb_state@v1')
      AND target_id = ${targetId}
      AND publication_status = 'current'
      AND as_of_at <= ${requestedAsOf}
      AND available_at <= ${requestedAsOf}
    ORDER BY definition_id, subject_id, target_id, as_of_at DESC, snapshot_id DESC
  `);
  const values = (result.rows as SnapshotRow[]).map(row =>
    resolved(row, request, policies.get(row.definition_id)!, null),
  );
  const players = values.filter(value => value.measurement.definitionId === "player_game_availability@v1");
  const quarterbacks = values.filter(value => value.measurement.definitionId === "team_qb_state@v1");
  const stateCounts: Record<string, number> = {};
  for (const value of players) {
    const state = String(value.measurement.payload.resolved_availability_state ?? "UNKNOWN");
    stateCounts[state] = (stateCounts[state] ?? 0) + 1;
  }
  return {
    players,
    quarterbacks,
    stateCounts,
    conflicts: stateCounts.CONFLICT ?? 0,
    stale: stateCounts.STALE ?? 0,
    unknown: stateCounts.UNKNOWN ?? 0,
  };
}

export async function getNflContextResearchSummary(): Promise<NflContextResearchSummary> {
  await ensureNflContextTables();
  const [dfsResult, marketResult, propResult] = await Promise.all([
    db.execute(sql`SELECT run_id, status, report FROM nfl_context_research_runs ORDER BY created_at DESC LIMIT 1`),
    db.execute(sql`SELECT run_id, report FROM nfl_market_context_research_runs ORDER BY created_at DESC LIMIT 1`),
    db.execute(sql`SELECT COUNT(*)::int AS observations, MAX(captured_at)::text AS latest_capture
                   FROM prop_odds_history WHERE sport='nfl'`),
  ]);
  const dfsRow = dfsResult.rows[0] as Record<string, unknown> | undefined;
  const marketRow = marketResult.rows[0] as Record<string, unknown> | undefined;
  const propRow = propResult.rows[0] as Record<string, unknown> | undefined;
  const dfsReport = dfsRow?.report as Record<string, unknown> | undefined;
  const aggregate = dfsReport?.aggregate as Record<string, unknown> | undefined;
  const marketReport = marketRow?.report as Record<string, unknown> | undefined;
  const folds = Array.isArray(marketReport?.folds) ? marketReport.folds : [];
  const associations = Array.isArray(marketReport?.fullSampleStandardizedAssociations)
    ? marketReport.fullSampleStandardizedAssociations
    : [];
  return {
    dfs: dfsRow ? {
      status: String(dfsRow.status),
      maeGain: aggregate?.maeGain == null ? null : Number(aggregate.maeGain),
      runId: String(dfsRow.run_id),
    } : null,
    market: marketRow ? {
      sampleRows: Number(marketReport?.sampleRows ?? 0),
      folds: folds.map((row) => {
        const value = row as Record<string, unknown>;
        return { season: Number(value.season), maeGain: Number(value.maeGain) };
      }),
      associations: associations.slice(0, 4).map((row) => {
        const value = row as Record<string, unknown>;
        return { feature: String(value.feature), standardizedPoints: Number(value.standardizedPoints) };
      }),
      runId: String(marketRow.run_id),
    } : null,
    props: {
      observations: Number(propRow?.observations ?? 0),
      latestCapture: propRow?.latest_capture == null ? null : String(propRow.latest_capture),
    },
  };
}
