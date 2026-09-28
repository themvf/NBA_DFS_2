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
 * One Vercel tick every half hour calls /api/cron/dispatch, which fires a
 * `workflow_dispatch` for every job whose `due(now)` is true. The work itself
 * stays in Python inside GitHub Actions; this never touches the database.
 * Each workflow keeps a thin GitHub `schedule:` as a fallback for a broken
 * bridge, and its own in-job cadence gate (if any) still applies.
 */
import { dkPoolDispatchDue } from "@/lib/nfl-dfs/dk-pool-cadence";
import { nflProjectionDispatchDue } from "@/lib/nfl-dfs/projection-cadence";

export const GITHUB_OWNER = "themvf";
export const GITHUB_REPO = "NBA_DFS_2";
export const WORKFLOW_REF = "main";

export interface DispatchJob {
  key: string;
  /** File name under .github/workflows/. */
  workflow: string;
  /** Whether this half-hour tick should dispatch. `now` is the tick time. */
  due: (now: Date) => boolean;
  /** `workflow_dispatch` inputs, when the workflow needs any. */
  inputs?: Record<string, string>;
  why: string;
}

const firstHalf = (now: Date) => now.getUTCMinutes() < 30;

export const DISPATCH_JOBS: readonly DispatchJob[] = [
  {
    key: "mlb-odds-capture",
    workflow: "capture_odds_history.yml",
    // Every half hour, 14:00-03:59 UTC (the former `7,37 14-23,0-3` schedule).
    due: (now) => { const h = now.getUTCHours(); return h >= 14 || h <= 3; },
    why: "MLB movement board goes STALE after 35 minutes without a capture.",
  },
  {
    key: "nfl-dk-pool",
    workflow: "refresh_nfl_dk_pool.yml",
    due: dkPoolDispatchDue,
    why: "DraftKings' live NFL statuses, dense through game windows.",
  },
  {
    key: "nfl-projections",
    workflow: "refresh_nfl_dfs_projections.yml",
    due: nflProjectionDispatchDue,
    why: "Production projection rebuild: 13:35/21:35 UTC daily, 16:05/19:05 UTC Sundays.",
  },
  {
    key: "nfl-availability-context",
    workflow: "refresh_nfl_availability_context.yml",
    // Hourly, September-December. The job's own gate (should_capture in
    // ingest/nfl_availability_operations.py) then captures hourly within six
    // hours of a kickoff and every two hours otherwise; dispatching without
    // `force` leaves that decision where the developer put it.
    due: (now) => { const m = now.getUTCMonth() + 1; return m >= 9 && m <= 12 && firstHalf(now); },
    why: "Sleeper injury/depth capture and the pinned availability decisions the DFS slate reads.",
  },
  {
    key: "nfl-dfs-postweek",
    workflow: "refresh_nfl_dfs_postweek.yml",
    // Tuesday and Wednesday, the 10:07 UTC tick: after Monday night's game is
    // published, then a second pass for late corrections. Every step is
    // idempotent, so the workflow's later GitHub fallback slot is harmless.
    due: (now) => [2, 3].includes(now.getUTCDay()) && now.getUTCHours() === 10 && firstHalf(now),
    why: "Realized DK points, slate report cards, and the weekly replacement-upside grade.",
  },
];

export function dueJobs(now: Date): DispatchJob[] {
  return DISPATCH_JOBS.filter((job) => job.due(now));
}

export interface DispatchOutcome { key: string; workflow: string; ok: boolean; status: number; detail?: string }

/** Fire one `workflow_dispatch`. GitHub answers 204 on success. */
export async function dispatchWorkflow(job: DispatchJob, token: string, fetchImpl: typeof fetch = fetch): Promise<DispatchOutcome> {
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
      body: JSON.stringify({ ref: WORKFLOW_REF, ...(job.inputs ? { inputs: job.inputs } : {}) }),
    });
    // A fine-grained PAT expires within a year; when it does every bridged job
    // goes quiet at once. GitHub reports the date on each response, so log it.
    const expires = response.headers.get("github-authentication-token-expiration");
    if (expires) {
      const daysLeft = (Date.parse(expires) - Date.now()) / 86_400_000;
      if (Number.isFinite(daysLeft) && daysLeft < 30) console.error(`cron dispatch: GITHUB_DISPATCH_TOKEN expires in ${Math.floor(daysLeft)} days (${expires})`);
    }
    if (response.status === 204) return { key: job.key, workflow: job.workflow, ok: true, status: 204 };
    const detail = (await response.text()).slice(0, 300);
    return { key: job.key, workflow: job.workflow, ok: false, status: response.status, detail };
  } catch (error) {
    return { key: job.key, workflow: job.workflow, ok: false, status: 0, detail: error instanceof Error ? error.message : String(error) };
  }
}
