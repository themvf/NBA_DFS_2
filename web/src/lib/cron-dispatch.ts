/**
 * Vercel Cron -> GitHub Actions dispatch bridge: the job table.
 *
 * GitHub's own `schedule:` trigger is best-effort and drops slots under load
 * (the MLB capture ran every 60-95 minutes on a 15-minute schedule; the NFL
 * DK poller skipped its first four Thursday slots; the NFL projection rebuild
 * skipped its 21:35 UTC slot on 2026-09-26 and left Sunday's slate blocked;
 * the NFL availability context refresh fired once in its first four hours).
 * Vercel Pro cron fires within the configured minute, so it is the clock.
 *
 * One Vercel tick every fifteen minutes (:07, :22, :37, :52) calls
 * /api/cron/dispatch, which fires a `workflow_dispatch` for every job whose
 * `due(now, context)` is true. Most jobs keep their half-hour cadence (the
 * :07 and :37 ticks); the NFL injury/depth and DraftKings status jobs also
 * fire on the quarter-hour ticks in the two hours before a kickoff, read from
 * the schedule the route passes in. The work itself stays in Python inside
 * GitHub Actions. Each workflow keeps a thin GitHub `schedule:` as a fallback
 * for a broken bridge, and its own in-job cadence gate (if any) still applies.
 */
import { dkPoolDispatchDue } from "@/lib/nfl-dfs/dk-pool-cadence";
import { nflProjectionDispatchDue } from "@/lib/nfl-dfs/projection-cadence";

export const GITHUB_OWNER = "themvf";
export const GITHUB_REPO = "NBA_DFS_2";
export const WORKFLOW_REF = "main";

/** What the route knows at a tick, beyond the clock. */
export interface DispatchContext {
  /** NFL kickoffs in the next few hours; null when the schedule could not be read. */
  nflKickoffs: Date[] | null;
}

export const NO_CONTEXT: DispatchContext = { nflKickoffs: null };

export interface DispatchJob {
  key: string;
  /** File name under .github/workflows/. */
  workflow: string;
  /** Whether this tick should dispatch. `now` is the tick time. */
  due: (now: Date, context: DispatchContext) => boolean;
  /** `workflow_dispatch` inputs, when the workflow needs any. */
  inputs?: Record<string, string>;
  why: string;
}

/** The :07 and :37 ticks: every half hour. */
export const halfHourTick = (now: Date) => now.getUTCMinutes() % 30 < 15;
/** The :07 tick only: once an hour. */
const hourTick = (now: Date) => now.getUTCMinutes() < 15;

/** How long before a kickoff the NFL availability jobs run on every tick. */
export const NEAR_KICKOFF_MS = 2 * 60 * 60 * 1000;

/**
 * True within the two hours before any NFL kickoff: inactives, late scratches
 * and DraftKings status flips land here, so the quarter-hour ticks run too.
 * False when the schedule could not be read; the half-hour cadence still runs.
 */
export function nearNflKickoff(now: Date, context: DispatchContext): boolean {
  return (context.nflKickoffs ?? []).some((kickoff) => {
    const lead = kickoff.getTime() - now.getTime();
    return lead > 0 && lead <= NEAR_KICKOFF_MS;
  });
}

export const DISPATCH_JOBS: readonly DispatchJob[] = [
  {
    key: "mlb-odds-capture",
    workflow: "capture_odds_history.yml",
    // Every half hour, 14:00-03:59 UTC (the former `7,37 14-23,0-3` schedule).
    // Half-hour ticks only: each capture spends Odds API credits.
    due: (now) => { const h = now.getUTCHours(); return halfHourTick(now) && (h >= 14 || h <= 3); },
    why: "MLB movement board goes STALE after 35 minutes without a capture.",
  },
  {
    key: "nfl-dk-pool",
    workflow: "refresh_nfl_dk_pool.yml",
    due: (now, context) => (halfHourTick(now) && dkPoolDispatchDue(now)) || nearNflKickoff(now, context),
    why: "DraftKings' live NFL statuses: dense through game windows, every 15 minutes in the two hours before a kickoff.",
  },
  {
    key: "nfl-projections",
    workflow: "refresh_nfl_dfs_projections.yml",
    due: (now) => halfHourTick(now) && nflProjectionDispatchDue(now),
    why: "Production projection rebuild: 13:35/21:35 UTC daily, 16:05/19:05 UTC Sundays.",
  },
  {
    key: "nfl-availability-context",
    workflow: "refresh_nfl_availability_context.yml",
    // Hourly, September-February (weeks 17-18 fall in January, the playoffs
    // run into February), plus every 15 minutes in the two hours before a
    // kickoff. The job's own gate (should_capture in
    // ingest/nfl_availability_operations.py) captures on every run within six
    // hours of a kickoff and every two hours otherwise; dispatching without
    // `force` leaves that decision where the developer put it.
    due: (now, context) => {
      const m = now.getUTCMonth() + 1;
      return (m >= 9 || m <= 2) && (hourTick(now) || nearNflKickoff(now, context));
    },
    why: "Sleeper injury/depth capture, the projection rebuild and the pinned availability decisions the DFS slate reads.",
  },
  {
    key: "nfl-dfs-postweek",
    workflow: "refresh_nfl_dfs_postweek.yml",
    // Tuesday and Wednesday, the 10:07 UTC tick: after Monday night's game is
    // published, then a second pass for late corrections. Every step is
    // idempotent, so the workflow's later GitHub fallback slot is harmless.
    due: (now) => [2, 3].includes(now.getUTCDay()) && now.getUTCHours() === 10 && hourTick(now),
    why: "Realized DK points, slate report cards, and the weekly replacement-upside grade.",
  },
  {
    key: "nfl-pbp-archetypes",
    workflow: "refresh_nfl_pbp_archetypes.yml",
    // Monday 12:07 and Tuesday 09:07 UTC (its former GitHub slots, which
    // GitHub skipped on 09-14 and 09-21 and ran seven hours late on 09-28).
    // Dispatched with no inputs = the self-healing --relabel-stale mode.
    due: (now) => ((now.getUTCDay() === 1 && now.getUTCHours() === 12) || (now.getUTCDay() === 2 && now.getUTCHours() === 9)) && hourTick(now),
    why: "Play-by-play labels behind DST scoring and the matchup studies.",
  },
  {
    key: "pipeline-health",
    workflow: "pipeline_health.yml",
    // Every three hours: the data-freshness readings on /health.
    due: (now) => now.getUTCHours() % 3 === 0 && hourTick(now),
    why: "Data-freshness readings for the /health checklist.",
  },
  {
    key: "mlb-terminal-settlement",
    workflow: "refresh_mlb_terminal_settlement.yml",
    // Hourly, March-November. Its GitHub schedule said every 30 minutes but GitHub
    // started it only every 4-6 hours (2026-09-23..29). Free scores + stored quotes, no credits.
    due: (now) => { const m = now.getUTCMonth() + 1; return m >= 3 && m <= 11 && hourTick(now); },
    why: "Grades recorded MLB terminal signals once games are final.",
  },
  {
    key: "daily-failure-sweep",
    workflow: "daily_failure_sweep.yml",
    // 11:07 UTC (7:07 am ET): the daily email of everything failing.
    due: (now) => now.getUTCHours() === 11 && hourTick(now),
    why: "Daily failure sweep: opens/updates a GitHub issue that emails the owner.",
  },
];

export function dueJobs(now: Date, context: DispatchContext = NO_CONTEXT): DispatchJob[] {
  return DISPATCH_JOBS.filter((job) => job.due(now, context));
}

export interface DispatchOutcome {
  key: string; workflow: string; ok: boolean; status: number; detail?: string;
  /** Present when GitHub returned the run it created (`returnRunDetails`). */
  runId?: number; htmlUrl?: string;
}

/**
 * Fire one `workflow_dispatch`. GitHub answers 204 on success, or 200 with the
 * new run's id and URL when `returnRunDetails` is set.
 */
export async function dispatchWorkflow(job: Pick<DispatchJob, "key" | "workflow" | "inputs">, token: string,
  fetchImpl: typeof fetch = fetch, options: { returnRunDetails?: boolean } = {}): Promise<DispatchOutcome> {
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${job.workflow}/dispatches`;
  try {
    const response = await fetchImpl(url, {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${token}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ref: WORKFLOW_REF, ...(job.inputs ? { inputs: job.inputs } : {}),
        ...(options.returnRunDetails ? { return_run_details: true } : {}) }),
    });
    // A fine-grained PAT expires within a year; when it does every bridged job
    // goes quiet at once. GitHub reports the date on each response, so log it.
    const expires = response.headers.get("github-authentication-token-expiration");
    if (expires) {
      const daysLeft = (Date.parse(expires) - Date.now()) / 86_400_000;
      if (Number.isFinite(daysLeft) && daysLeft < 30) console.error(`cron dispatch: GITHUB_DISPATCH_TOKEN expires in ${Math.floor(daysLeft)} days (${expires})`);
    }
    if (response.status === 204) return { key: job.key, workflow: job.workflow, ok: true, status: 204 };
    if (response.status === 200) {
      const body = await response.json().catch(() => null) as { workflow_run_id?: number; html_url?: string } | null;
      return { key: job.key, workflow: job.workflow, ok: true, status: 200,
        ...(typeof body?.workflow_run_id === "number" ? { runId: body.workflow_run_id } : {}),
        ...(typeof body?.html_url === "string" ? { htmlUrl: body.html_url } : {}) };
    }
    const detail = (await response.text()).slice(0, 300);
    return { key: job.key, workflow: job.workflow, ok: false, status: response.status, detail };
  } catch (error) {
    return { key: job.key, workflow: job.workflow, ok: false, status: 0, detail: error instanceof Error ? error.message : String(error) };
  }
}

export interface WorkflowRunState {
  id: number;
  status: string;            // queued | in_progress | completed | waiting | pending ...
  conclusion: string | null; // success | failure | cancelled | ... (completed only)
  htmlUrl: string;
  createdAt: string;
  updatedAt: string;
}

const githubHeaders = (token: string) => ({
  Accept: "application/vnd.github+json",
  Authorization: `Bearer ${token}`,
  "X-GitHub-Api-Version": "2022-11-28",
});

const toRunState = (run: Record<string, unknown>): WorkflowRunState => ({
  id: Number(run.id), status: String(run.status), conclusion: run.conclusion == null ? null : String(run.conclusion),
  htmlUrl: String(run.html_url), createdAt: String(run.created_at), updatedAt: String(run.updated_at),
});

/** One workflow run's current state. */
export async function readWorkflowRun(runId: number, token: string, fetchImpl: typeof fetch = fetch): Promise<WorkflowRunState> {
  const response = await fetchImpl(`https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/runs/${runId}`, { headers: githubHeaders(token), cache: "no-store" });
  if (!response.ok) throw new Error(`GitHub run ${runId}: HTTP ${response.status}`);
  return toRunState(await response.json() as Record<string, unknown>);
}

/**
 * Dispatched runs of a workflow created at or after `since`, oldest first.
 * Used when a queued run is replaced: GitHub keeps one pending run per
 * concurrency group and cancels the older one when a newer dispatch arrives.
 */
export async function listDispatchedRuns(workflow: string, since: Date, token: string, fetchImpl: typeof fetch = fetch): Promise<WorkflowRunState[]> {
  const created = encodeURIComponent(`>=${new Date(since.getTime() - 5_000).toISOString()}`);
  const response = await fetchImpl(`https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${workflow}/runs?event=workflow_dispatch&created=${created}&per_page=20`,
    { headers: githubHeaders(token), cache: "no-store" });
  if (!response.ok) throw new Error(`GitHub ${workflow} runs: HTTP ${response.status}`);
  const body = await response.json() as { workflow_runs?: Record<string, unknown>[] };
  return (body.workflow_runs ?? []).map(toRunState).sort((a, b) => a.createdAt.localeCompare(b.createdAt));
}
