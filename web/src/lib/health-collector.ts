/**
 * Gathers every input of the /health checklist (lib/health-checklist),
 * evaluates it, and stores the result in health_checks, which the page and the
 * daily failure sweep both read. Run every 30 minutes by /api/cron/health-check
 * and once a day by the sweep (.github/workflows/daily_failure_sweep.yml).
 *
 * A source that cannot be read becomes a FAIL row saying so. Nothing is
 * skipped quietly.
 */
import { sql } from "drizzle-orm";
import manifestJson from "@/data/workflow-manifest.json";
import { DISPATCH_JOBS, GITHUB_OWNER, GITHUB_REPO, NO_CONTEXT } from "@/lib/cron-dispatch";
import { readCronHeartbeats } from "@/lib/cron-heartbeat";
import { buildChecklist, type HealthItem, type ManifestWorkflow } from "@/lib/health-checklist";
import { toRunLite, type WorkflowRunLite } from "@/lib/workflow-health";

const database = async () => (await import("@/db")).db;

export const HEALTH_CHECK_EVERY_MINUTES = 30;
/** The Pipeline Health job's cadence (dispatched every 3 h). */
export const DATASET_CADENCE_HOURS = 3;
const GITHUB_TIMEOUT_MS = 8_000;
const TICK_MINUTES = [7, 22, 37, 52];

export const MANIFEST = (manifestJson as { workflows: ManifestWorkflow[] }).workflows;

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** Dispatcher due times per workflow file, 8 days back and 8 days ahead (half-hour cadence; near-kickoff extras not included). */
export function dispatcherTimes(now: Date): Record<string, { past: Date[]; future: Date[] }> {
  const out: Record<string, { past: Date[]; future: Date[] }> = {};
  const startHour = Math.floor(now.getTime() / 3600_000) - 8 * 24;
  for (let h = startHour; h <= startHour + 16 * 24; h++) {
    for (const m of TICK_MINUTES) {
      const t = new Date(h * 3600_000 + m * 60_000);
      for (const job of DISPATCH_JOBS) {
        if (!job.due(t, NO_CONTEXT)) continue;
        const slot = (out[job.workflow] ??= { past: [], future: [] });
        (t.getTime() <= now.getTime() ? slot.past : slot.future).push(t);
      }
    }
  }
  return out;
}

async function github(path: string, token: string, fetchImpl: typeof fetch): Promise<Record<string, unknown>> {
  let response: Response;
  try {
    response = await fetchImpl(`https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions${path}`, {
      headers: { Accept: "application/vnd.github+json", Authorization: `Bearer ${token}`, "X-GitHub-Api-Version": "2022-11-28" },
      signal: AbortSignal.timeout(GITHUB_TIMEOUT_MS), cache: "no-store",
    });
  } catch (error) {
    throw new Error(error instanceof Error && error.name === "TimeoutError" ? `GitHub did not answer within ${GITHUB_TIMEOUT_MS / 1000}s` : `GitHub request failed: ${errorText(error)}`);
  }
  if (!response.ok) throw new Error(`GitHub answered ${response.status} for ${path}`);
  return await response.json() as Record<string, unknown>;
}

/**
 * Each workflow is read on its own: one that cannot be read (or is not on
 * GitHub's default branch yet) becomes its own row, never a gap that hides
 * the rest.
 */
async function readGithub(token: string | null, fetchImpl: typeof fetch) {
  if (!token) return { runs: null, states: null, workflowErrors: {}, error: "no GitHub token is configured for this checker" };
  const [workflows, ...perWorkflow] = await Promise.allSettled([
    github("/workflows?per_page=100", token, fetchImpl),
    ...MANIFEST.map((w) => github(`/workflows/${w.file}/runs?per_page=10&exclude_pull_requests=true`, token, fetchImpl)),
  ]);
  const runs: Record<string, WorkflowRunLite[]> = {};
  const workflowErrors: Record<string, string> = {};
  MANIFEST.forEach((w, i) => {
    const result = perWorkflow[i];
    if (result.status === "fulfilled") runs[w.file] = ((result.value.workflow_runs as Record<string, unknown>[] | undefined) ?? []).map(toRunLite);
    else workflowErrors[w.file] = errorText(result.reason);
  });
  // Every read failed: GitHub itself is unreachable or the token is bad. Say that once.
  if (MANIFEST.length && Object.keys(workflowErrors).length === MANIFEST.length) {
    return { runs: null, states: null, workflowErrors: {}, error: workflowErrors[MANIFEST[0].file] };
  }
  let states: Record<string, string> | null = null;
  if (workflows.status === "fulfilled") {
    states = {};
    for (const w of (workflows.value.workflows as Record<string, unknown>[] | undefined) ?? []) {
      states[String(w.path ?? "").replace(/^\.github\/workflows\//, "")] = String(w.state ?? "");
    }
  }
  return { runs, states, workflowErrors, error: null };
}

async function readDatasets() {
  try {
    const db = await database();
    const rows = await db.execute(sql`SELECT DISTINCT ON (dataset_key) dataset_key, label, status, last_row_at, age_hours, max_age_hours,
        owner_workflow, checked_at, detail_json FROM pipeline_health_snapshots ORDER BY dataset_key, checked_at DESC`);
    return { datasets: rows.rows.map((r) => ({
      key: String(r.dataset_key), label: String(r.label), status: String(r.status),
      lastRowAt: r.last_row_at == null ? null : new Date(String(r.last_row_at)).toISOString(),
      ageHours: r.age_hours == null ? null : Number(r.age_hours), maxAgeHours: Number(r.max_age_hours),
      ownerWorkflow: String(r.owner_workflow), checkedAt: new Date(String(r.checked_at)).toISOString(),
      note: ((r.detail_json ?? {}) as Record<string, unknown>).note == null ? null : String(((r.detail_json ?? {}) as Record<string, unknown>).note),
    })), error: null };
  } catch (error) { return { datasets: null, error: errorText(error) }; }
}

async function readSlates(now: Date) {
  try {
    const db = await database();
    const exists = await db.execute(sql`SELECT to_regclass('public.nfl_dfs_slate_checks') IS NOT NULL AS ok`);
    if (!exists.rows[0]?.ok) return { slates: [], error: null };
    const since = new Date(now.getTime() - 26 * 3600_000).toISOString();
    const rows = await db.execute(sql`SELECT DISTINCT ON (slate_signature) slate_signature, headline, needs, checked_at
      FROM nfl_dfs_slate_checks WHERE checked_at > ${since}::timestamptz ORDER BY slate_signature, checked_at DESC, id DESC`);
    return { slates: rows.rows
      .map((r) => ({ signature: String(r.slate_signature), headline: String(r.headline), needs: Number(r.needs), checkedAt: new Date(String(r.checked_at)).toISOString() }))
      // A started slate has nothing left to act on; its record is history, not a check.
      .filter((s) => !/games have started/i.test(s.headline)), error: null };
  } catch (error) { return { slates: null, error: errorText(error) }; }
}

async function readAvailabilityOps() {
  try {
    const db = await database();
    const rows = await db.execute(sql`SELECT status, evaluated_at, week, report FROM nfl_availability_operation_runs ORDER BY evaluated_at DESC LIMIT 1`);
    const r = rows.rows[0];
    if (!r) return { ops: null, error: null };
    const alerts = (((r.report ?? {}) as Record<string, unknown>).alerts as Record<string, unknown>[] | undefined) ?? [];
    return { ops: { status: String(r.status), evaluatedAt: new Date(String(r.evaluated_at)).toISOString(), week: Number(r.week),
      alerts: alerts.map((a) => `${a.severity ?? ""} ${a.message ?? a.code ?? JSON.stringify(a)}`.trim()) }, error: null };
  } catch (error) { return { ops: null, error: errorText(error) }; }
}

/** Gather and evaluate. Never throws for a source failure: that source becomes a FAIL row. */
export async function collectHealth(options: { githubToken: string | null; now?: Date; fetchImpl?: typeof fetch }): Promise<HealthItem[]> {
  const now = options.now ?? new Date();
  const [gh, data, slates, ops, heartbeats] = await Promise.all([
    readGithub(options.githubToken, options.fetchImpl ?? fetch), readDatasets(), readSlates(now), readAvailabilityOps(),
    readCronHeartbeats().then((h) => ({ h, error: null as string | null })).catch((error) => ({ h: null, error: errorText(error) })),
  ]);
  return buildChecklist({
    now, nextCheckAt: new Date(now.getTime() + HEALTH_CHECK_EVERY_MINUTES * 60_000),
    manifest: MANIFEST, dispatchTimes: dispatcherTimes(now),
    runs: gh.runs, workflowStates: gh.states, workflowErrors: gh.workflowErrors, githubError: gh.error,
    datasets: data.datasets, datasetsError: data.error, datasetCadenceHours: DATASET_CADENCE_HOURS,
    heartbeats: heartbeats.h, heartbeatsError: heartbeats.error,
    slates: slates.slates, slatesError: slates.error,
    availabilityOps: ops.ops, availabilityOpsError: ops.error,
  });
}

let ensured: Promise<void> | null = null;
async function ensureTable(): Promise<void> {
  ensured ??= database().then((db) => db.execute(sql`CREATE TABLE IF NOT EXISTS health_checks (
      item_key TEXT PRIMARY KEY,
      grp TEXT NOT NULL,
      label TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('pass','fail','info')),
      detail TEXT NOT NULL,
      url TEXT,
      last_event_at TIMESTAMPTZ,
      next_event_at TIMESTAMPTZ,
      next_event_note TEXT,
      last_checked_at TIMESTAMPTZ NOT NULL,
      next_check_at TIMESTAMPTZ,
      failing_since TIMESTAMPTZ,
      run_at TIMESTAMPTZ NOT NULL
    )`)).then(() => undefined).catch((error) => { ensured = null; throw error; });
  return ensured;
}

/**
 * Replace the stored checklist with this run's items, in one transaction. A
 * failing item keeps the time it first failed; items that no longer exist are
 * removed.
 */
export async function storeHealth(items: HealthItem[], runAt: Date): Promise<void> {
  await ensureTable();
  const db = await database();
  const at = runAt.toISOString();
  const values = items.map((i) => sql`(${i.key}, ${i.group}, ${i.label}, ${i.status}, ${i.detail}, ${i.url}, ${i.lastEventAt}::timestamptz,
    ${i.nextEventAt}::timestamptz, ${i.nextEventNote ?? null}, ${i.lastCheckedAt}::timestamptz, ${i.nextCheckAt}::timestamptz,
    ${i.status === "fail" ? at : null}::timestamptz, ${at}::timestamptz)`);
  await db.batch([
    db.execute(sql`INSERT INTO health_checks (item_key, grp, label, status, detail, url, last_event_at, next_event_at, next_event_note,
        last_checked_at, next_check_at, failing_since, run_at) VALUES ${sql.join(values, sql`, `)}
      ON CONFLICT (item_key) DO UPDATE SET grp = EXCLUDED.grp, label = EXCLUDED.label, status = EXCLUDED.status, detail = EXCLUDED.detail,
        url = EXCLUDED.url, last_event_at = EXCLUDED.last_event_at, next_event_at = EXCLUDED.next_event_at, next_event_note = EXCLUDED.next_event_note,
        last_checked_at = EXCLUDED.last_checked_at, next_check_at = EXCLUDED.next_check_at, run_at = EXCLUDED.run_at,
        failing_since = CASE WHEN EXCLUDED.status <> 'fail' THEN NULL ELSE COALESCE(health_checks.failing_since, EXCLUDED.failing_since) END`),
    db.execute(sql`DELETE FROM health_checks WHERE run_at < ${at}::timestamptz`),
  ]);
}

export interface StoredHealthItem extends HealthItem { failingSince: string | null; runAt: string }

export async function readStoredHealth(): Promise<StoredHealthItem[]> {
  await ensureTable();
  const db = await database();
  const rows = await db.execute(sql`SELECT * FROM health_checks`);
  const iso = (v: unknown) => (v == null ? null : new Date(String(v)).toISOString());
  return rows.rows.map((r) => ({
    key: String(r.item_key), group: String(r.grp) as HealthItem["group"], label: String(r.label), status: String(r.status) as HealthItem["status"],
    detail: String(r.detail), url: r.url == null ? null : String(r.url), lastEventAt: iso(r.last_event_at), nextEventAt: iso(r.next_event_at),
    nextEventNote: r.next_event_note == null ? null : String(r.next_event_note), lastCheckedAt: iso(r.last_checked_at)!,
    nextCheckAt: iso(r.next_check_at), failingSince: iso(r.failing_since), runAt: iso(r.run_at)!,
  }));
}
