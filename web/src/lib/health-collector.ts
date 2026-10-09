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
import { DISPATCH_JOBS, GITHUB_OWNER, GITHUB_REPO, NO_CONTEXT, parseTokenExpiry } from "@/lib/cron-dispatch";
import { observation, readCronHeartbeats, recordObservation, type CronHeartbeat } from "@/lib/cron-heartbeat";
import { buildChecklist, type HealthItem, type ManifestWorkflow, type OddsApiReading } from "@/lib/health-checklist";
import { toRunLite, type WorkflowRunLite } from "@/lib/workflow-health";

const database = async () => (await import("@/db")).db;

export const HEALTH_CHECK_EVERY_MINUTES = 30;
/** The Pipeline Health job's cadence (dispatched every 3 h). */
export const DATASET_CADENCE_HOURS = 3;
const GITHUB_TIMEOUT_MS = 8_000;
/** The dispatcher's tick minutes; a test pins these to vercel.json's schedule for /api/cron/dispatch. */
export const TICK_MINUTES = [7, 22, 37, 52];
/** Observation keys (lib/cron-heartbeat): facts recorded by one process for the checklist to read from any. */
export const OBS_DISPATCH_TOKEN = "github-dispatch-token";
export const OBS_DEPLOYED_COMMIT = "deployed-commit";

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

/** One GitHub read under /repos/{owner}/{repo}; the body plus the token's expiry header when the token has one. */
async function github(path: string, token: string, fetchImpl: typeof fetch): Promise<{ body: Record<string, unknown>; tokenExpiresAt: string | null }> {
  let response: Response;
  try {
    response = await fetchImpl(`https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}${path}`, {
      headers: { Accept: "application/vnd.github+json", Authorization: `Bearer ${token}`, "X-GitHub-Api-Version": "2022-11-28" },
      signal: AbortSignal.timeout(GITHUB_TIMEOUT_MS), cache: "no-store",
    });
  } catch (error) {
    throw new Error(error instanceof Error && error.name === "TimeoutError" ? `GitHub did not answer within ${GITHUB_TIMEOUT_MS / 1000}s` : `GitHub request failed: ${errorText(error)}`);
  }
  if (!response.ok) throw new Error(`GitHub answered ${response.status} for ${path}`);
  return { body: await response.json() as Record<string, unknown>, tokenExpiresAt: parseTokenExpiry(response.headers.get("github-authentication-token-expiration")) };
}

/**
 * Each workflow is read on its own: one that cannot be read (or is not on
 * GitHub's default branch yet) becomes its own row, never a gap that hides
 * the rest.
 */
async function readGithub(token: string | null, fetchImpl: typeof fetch) {
  if (!token) return { runs: null, states: null, workflowErrors: {}, error: "no GitHub token is configured for this checker", tokenExpiresAt: null };
  const [workflows, ...perWorkflow] = await Promise.allSettled([
    github("/actions/workflows?per_page=100", token, fetchImpl),
    ...MANIFEST.map((w) => github(`/actions/workflows/${w.file}/runs?per_page=10&exclude_pull_requests=true`, token, fetchImpl)),
  ]);
  const runs: Record<string, WorkflowRunLite[]> = {};
  const workflowErrors: Record<string, string> = {};
  MANIFEST.forEach((w, i) => {
    const result = perWorkflow[i];
    if (result.status === "fulfilled") runs[w.file] = ((result.value.body.workflow_runs as Record<string, unknown>[] | undefined) ?? []).map(toRunLite);
    else workflowErrors[w.file] = errorText(result.reason);
  });
  const tokenExpiresAt = [workflows, ...perWorkflow].map((r) => (r.status === "fulfilled" ? r.value.tokenExpiresAt : null)).find((t) => t) ?? null;
  // Every read failed: GitHub itself is unreachable or the token is bad. Say that once.
  if (MANIFEST.length && Object.keys(workflowErrors).length === MANIFEST.length) {
    return { runs: null, states: null, workflowErrors: {}, error: workflowErrors[MANIFEST[0].file], tokenExpiresAt };
  }
  let states: Record<string, string> | null = null;
  if (workflows.status === "fulfilled") {
    states = {};
    for (const w of (workflows.value.body.workflows as Record<string, unknown>[] | undefined) ?? []) {
      states[String(w.path ?? "").replace(/^\.github\/workflows\//, "")] = String(w.state ?? "");
    }
  }
  return { runs, states, workflowErrors, error: null, tokenExpiresAt };
}

export interface MainHead { sha: string; at: string; source: string }

/**
 * main's head as GitHub knows it. `commits/main` needs Contents: read, which
 * the dispatch PAT (Actions only) lacks, so the fallback is the newest
 * push-triggered run on main: tests.yml runs on every push to main, so its
 * head_sha is main's head and its creation time is the push time.
 */
export async function readMainHead(token: string | null, fetchImpl: typeof fetch): Promise<{ head: MainHead | null; error: string | null }> {
  if (!token) return { head: null, error: "no GitHub token is configured for this checker" };
  const errors: string[] = [];
  try {
    const { body } = await github("/commits/main", token, fetchImpl);
    const sha = String(body.sha ?? ""), at = String((body.commit as { committer?: { date?: string } } | undefined)?.committer?.date ?? "");
    if (sha && Number.isFinite(Date.parse(at))) return { head: { sha, at: new Date(at).toISOString(), source: "commits/main" }, error: null };
    errors.push("commits/main answered without a sha and date");
  } catch (error) { errors.push(errorText(error)); }
  try {
    const { body } = await github("/actions/runs?branch=main&event=push&per_page=1", token, fetchImpl);
    const run = ((body.workflow_runs as Record<string, unknown>[] | undefined) ?? [])[0];
    const sha = String(run?.head_sha ?? ""), at = String(run?.created_at ?? "");
    if (sha && Number.isFinite(Date.parse(at))) return { head: { sha, at: new Date(at).toISOString(), source: "newest push run on main" }, error: null };
    errors.push("no push-triggered run on main");
  } catch (error) { errors.push(errorText(error)); }
  return { head: null, error: errors.join("; ") };
}

/** The Odds API quota as the capture jobs recorded it: the newest reading and the past 7 days of readings, oldest first. */
async function readOddsApi(): Promise<{ usage: { latest: OddsApiReading; series: OddsApiReading[] } | null; error: string | null }> {
  try {
    const db = await database();
    const reading = (r: Record<string, unknown>): OddsApiReading => ({ requestedAt: new Date(String(r.requested_at)).toISOString(), used: Number(r.requests_used), remaining: Number(r.requests_remaining) });
    const latest = await db.execute(sql`SELECT requested_at, requests_used, requests_remaining FROM odds_api_usage
      WHERE requests_used IS NOT NULL AND requests_remaining IS NOT NULL ORDER BY requested_at DESC, id DESC LIMIT 1`);
    if (!latest.rows[0]) return { usage: null, error: null };
    const series = await db.execute(sql`SELECT requested_at, requests_used, requests_remaining FROM odds_api_usage
      WHERE requests_used IS NOT NULL AND requests_remaining IS NOT NULL AND requested_at > NOW() - INTERVAL '7 days' ORDER BY requested_at, id`);
    return { usage: { latest: reading(latest.rows[0]), series: series.rows.map(reading) }, error: null };
  } catch (error) { return { usage: null, error: errorText(error) }; }
}

const text = (value: unknown) => (value == null || value === "" ? null : String(value));

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
      note: text((r.detail_json as Record<string, unknown> | null)?.note),
      detail: text((r.detail_json as Record<string, unknown> | null)?.detail),
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
 * Replace the stored checklist with this run's items, in one transaction
 * (neon-http's `batch` runs its statements in one transaction). A failing
 * item keeps the time it first failed; items that no longer exist are removed.
 *
 * Two checkers can overlap (the 30-minute cron and the daily sweep both
 * store), and the one that started earlier can finish later. A row is only
 * ever replaced by a newer reading, and whatever is older than the newest
 * reading in the table is removed, so the table always holds one reading and
 * an older run cannot roll a fresher verdict back.
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
        failing_since = CASE WHEN EXCLUDED.status <> 'fail' THEN NULL ELSE COALESCE(health_checks.failing_since, EXCLUDED.failing_since) END
      WHERE health_checks.run_at < EXCLUDED.run_at`),
    db.execute(sql`DELETE FROM health_checks WHERE run_at < (SELECT max(run_at) FROM health_checks)`),
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
