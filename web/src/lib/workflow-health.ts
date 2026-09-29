/**
 * Which scheduled GitHub Actions jobs are failing right now.
 *
 * /health judges pipelines on the data they write, which catches a job that
 * goes green while writing nothing. It cannot catch the opposite: a job that
 * fails while its table still looks fresh from another writer. On 2026-09-29
 * nine scheduled runs failed overnight (DeepSeek out of credit, a dead YouTube
 * feed, a seasonal check left on, the research pick'em freeze) and /health
 * read "ok" for most of them. This reads workflow status directly, so a failed
 * job is visible on /health and, for the NFL jobs, on the DFS Slate Check.
 *
 * A workflow is failing when its latest finished run (pull-request runs
 * excluded) failed, timed out or could not start. A run GitHub cancelled
 * because a newer one replaced it is not a failure.
 */
import { GITHUB_OWNER, GITHUB_REPO } from "@/lib/cron-dispatch";

export interface WorkflowRunLite {
  id: number;
  workflowId: number;
  name: string;
  /** File name under .github/workflows/, e.g. refresh_tennis.yml. */
  workflow: string;
  event: string;
  status: string;
  conclusion: string | null;
  createdAt: string;
  url: string;
}

export interface FailingWorkflow {
  workflow: string;
  name: string;
  failedAt: string;
  url: string;
  /** Consecutive failed finished runs, newest first (within what was read). */
  streak: number;
  /** True when every run read failed, so the real streak may be longer. */
  streakCapped: boolean;
  /** Start of the oldest failed run in the streak that was read. */
  failingSince: string;
  /** Latest successful run seen, if any. */
  lastSuccessAt: string | null;
}

const FAILED = new Set(["failure", "timed_out", "startup_failure"]);

export function toRunLite(run: Record<string, unknown>): WorkflowRunLite {
  const path = String(run.path ?? "");
  return {
    id: Number(run.id), workflowId: Number(run.workflow_id), name: String(run.name ?? ""),
    workflow: path.replace(/^\.github\/workflows\//, "").replace(/@.*$/, ""),
    event: String(run.event ?? ""), status: String(run.status ?? ""),
    conclusion: run.conclusion == null ? null : String(run.conclusion),
    createdAt: String(run.created_at ?? ""), url: String(run.html_url ?? ""),
  };
}

/**
 * From each workflow's recent runs (any order), the ones whose latest finished,
 * non-pull-request run failed. Pure.
 */
export function failingWorkflows(runsByWorkflow: Map<number, WorkflowRunLite[]>): FailingWorkflow[] {
  const out: FailingWorkflow[] = [];
  for (const runs of runsByWorkflow.values()) {
    const finished = runs
      .filter((r) => r.event !== "pull_request" && r.status === "completed" && r.conclusion !== "cancelled" && r.conclusion !== "skipped")
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
    const latest = finished[0];
    if (!latest || !FAILED.has(latest.conclusion ?? "")) continue;
    let streak = 0;
    for (const run of finished) { if (FAILED.has(run.conclusion ?? "")) streak += 1; else break; }
    const success = finished.find((r) => r.conclusion === "success");
    out.push({ workflow: latest.workflow, name: latest.name, failedAt: latest.createdAt, url: latest.url, streak,
      streakCapped: streak === finished.length, failingSince: finished[streak - 1].createdAt,
      lastSuccessAt: success?.createdAt ?? null });
  }
  return out.sort((a, b) => b.failedAt.localeCompare(a.failedAt));
}

const headers = (token: string) => ({
  Accept: "application/vnd.github+json",
  Authorization: `Bearer ${token}`,
  "X-GitHub-Api-Version": "2022-11-28",
});

/** Each GitHub call gets this long; a slow GitHub must not slow the slate. */
export const GITHUB_TIMEOUT_MS = 5_000;
const CACHE_MS = 5 * 60_000;
let cache: { key: string; at: number; value: FailingWorkflow[] } | null = null;

async function getJson(url: string, token: string, fetchImpl: typeof fetch, timeoutMs: number): Promise<Record<string, unknown>> {
  let response: Response;
  try {
    response = await fetchImpl(url, { headers: headers(token), signal: AbortSignal.timeout(timeoutMs), cache: "no-store" });
  } catch (error) {
    const timedOut = error instanceof Error && (error.name === "TimeoutError" || error.name === "AbortError");
    throw new Error(timedOut ? `GitHub did not answer within ${timeoutMs / 1000}s` : `GitHub request failed: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!response.ok) throw new Error(`GitHub answered ${response.status} for ${url.replace(/^https:\/\/api\.github\.com/, "")}`);
  return await response.json() as Record<string, unknown>;
}

/**
 * Read GitHub: every failed run in the window, then the latest runs of each
 * workflow that had one, to see whether it has since recovered. Calls run in
 * parallel, each capped at GITHUB_TIMEOUT_MS; the answer is cached in memory
 * for five minutes so the DFS page does not call GitHub on every load. Any
 * failure throws: the caller reports "couldn't check", never "all clear".
 */
export async function readFailingWorkflows(token: string, options: { now?: Date; windowHours?: number; fetchImpl?: typeof fetch; timeoutMs?: number } = {}): Promise<FailingWorkflow[]> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutMs = options.timeoutMs ?? GITHUB_TIMEOUT_MS;
  const now = options.now ?? new Date();
  const since = new Date(Math.floor((now.getTime() - (options.windowHours ?? 48) * 3600_000) / 3600_000) * 3600_000).toISOString();
  const useCache = !options.fetchImpl;
  if (useCache && cache && cache.key === since && now.getTime() - cache.at < CACHE_MS) return cache.value;
  const base = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions`;
  const failedBodies = await Promise.all(["failure", "timed_out", "startup_failure"].map((status) =>
    getJson(`${base}/runs?status=${status}&created=${encodeURIComponent(`>=${since}`)}&per_page=100`, token, fetchImpl, timeoutMs)));
  const failed = failedBodies.flatMap((body) => ((body.workflow_runs as Record<string, unknown>[] | undefined) ?? []).map(toRunLite));
  const workflowIds = [...new Set(failed.filter((r) => r.event !== "pull_request").map((r) => r.workflowId))];
  const recent = await Promise.all(workflowIds.map((id) =>
    getJson(`${base}/workflows/${id}/runs?per_page=10&exclude_pull_requests=true`, token, fetchImpl, timeoutMs)));
  const byWorkflow = new Map<number, WorkflowRunLite[]>();
  workflowIds.forEach((id, i) => byWorkflow.set(id, ((recent[i].workflow_runs as Record<string, unknown>[] | undefined) ?? []).map(toRunLite)));
  const value = failingWorkflows(byWorkflow);
  if (useCache) cache = { key: since, at: now.getTime(), value };
  return value;
}

/** The NFL data jobs the DFS page depends on, and whether a failure can change a build. */
export const NFL_PIPELINE_WORKFLOWS: Record<string, { label: string; affectsBuild: boolean }> = {
  "refresh_nfl_dfs_projections.yml": { label: "Projection refresh", affectsBuild: true },
  "refresh_nfl_availability_context.yml": { label: "Injury and depth-chart refresh", affectsBuild: true },
  "refresh_nfl_dk_pool.yml": { label: "DraftKings status refresh", affectsBuild: true },
  "capture_nfl_availability.yml": { label: "Injury report capture", affectsBuild: true },
  "refresh_nfl_vegas.yml": { label: "NFL betting-line refresh", affectsBuild: true },
  "refresh_nfl_dfs_research.yml": { label: "Research and report-card job", affectsBuild: false },
  "refresh_nfl_dfs_postweek.yml": { label: "Post-week results job", affectsBuild: false },
  "refresh_nfl_pbp_archetypes.yml": { label: "Play-by-play refresh", affectsBuild: false },
};
