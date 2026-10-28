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
  /** Upcoming CFB kickoffs; null when the schedule read fails. */
  cfbKickoffs?: Date[] | null;
  /**
   * Opponent-capture requests waiting for a worker (pending past the dispatch
   * grace, or with an expired lease). Null or absent when it could not be read.
   */
  nflDefensiveCapturesPending?: boolean | null;
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
  /** Inputs that depend on the tick; `dueJobs` resolves them into `inputs`. */
  inputsAt?: (now: Date) => Record<string, string>;
  why: string;
}

/** The :07 and :37 ticks: every half hour. */
export const halfHourTick = (now: Date) => now.getUTCMinutes() % 30 < 15;
/** The :07 tick only: once an hour. */
const hourTick = (now: Date) => now.getUTCMinutes() < 15;

/** How long before a kickoff the NFL availability jobs run on every tick. */
export const NEAR_KICKOFF_MS = 2 * 60 * 60 * 1000;

/** How far ahead the route reads kickoffs: the day-before prop capture looks a day out. Every rule checks its own lead. */
export const KICKOFF_LOOKAHEAD_MS = 48 * 60 * 60 * 1000;

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

/** Which of refresh_cfb_terminal.yml's former cron lines a tick replaces. */
export function cfbTerminalSlot(now: Date): "schedule" | "scores" | "events" {
  if (!hourTick(now)) return "events";              // was `22,37,52 * * * *`
  return now.getUTCHours() % 6 === 0 ? "schedule"   // was `7 */6 * * *`
    : "scores";                                     // was `7 1-5,7-11,13-17,19-23 * * *`
}

/**
 * Lead time at which a kickoff group's prop capture fires: one quarter-hour tick
 * per kickoff time, 60-75 minutes out, after the inactives are posted (owner's
 * schedule, 2026-10-12; was 90-105 minutes).
 */
export const PROP_CAPTURE_LEAD_MS = { min: 60 * 60 * 1000, max: 75 * 60 * 1000 };

/** Eastern calendar date of an instant, as YYYY-MM-DD. */
function easternDate(at: Date): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }).format(at);
}

/**
 * The day-before prop capture: the 22:07 UTC tick (6:07 PM ET) on any day with
 * an NFL game tomorrow (Eastern date). Wednesday takes Thursday, Saturday takes
 * Sunday, Sunday takes Monday.
 */
export function nflPropDayAheadDue(now: Date, context: DispatchContext): boolean {
  if (now.getUTCHours() !== 22 || !hourTick(now)) return false;
  const tomorrow = easternDate(new Date(now.getTime() + 24 * 3600_000));
  return (context.nflKickoffs ?? []).some((kickoff) => easternDate(kickoff) === tomorrow);
}

/**
 * Midweek prop reads for the field-lag study (docs/nfl-prop-field-lag-study.md):
 * the 22:07 UTC tick (6:07 PM ET) on Wednesday and Friday when an NFL game
 * kicks off 30 hours or more out and inside the window. The window stops short
 * of the following Thursday, and the 30-hour floor leaves tomorrow's game to the
 * day-ahead capture. Main lines only (12 markets), so about 180 credits a round.
 */
export const PROP_MIDWEEK_WINDOW_HOURS = { wed: 130, fri: 100 } as const;
export const PROP_MIDWEEK_MIN_HOURS = 30;

function nflPropMidweekDue(day: 3 | 5, now: Date, context: DispatchContext): boolean {
  if (now.getUTCDay() !== day || now.getUTCHours() !== 22 || !hourTick(now)) return false;
  const within = day === 3 ? PROP_MIDWEEK_WINDOW_HOURS.wed : PROP_MIDWEEK_WINDOW_HOURS.fri;
  return (context.nflKickoffs ?? []).some((kickoff) => {
    const lead = kickoff.getTime() - now.getTime();
    return lead > PROP_MIDWEEK_MIN_HOURS * 3600_000 && lead <= within * 3600_000;
  });
}
export const nflPropWednesdayDue = (now: Date, context: DispatchContext) => nflPropMidweekDue(3, now, context);
export const nflPropFridayDue = (now: Date, context: DispatchContext) => nflPropMidweekDue(5, now, context);

/** True on the one tick that falls 60-75 minutes before any NFL kickoff. */
export function nflPropCaptureDue(now: Date, context: DispatchContext): boolean {
  return (context.nflKickoffs ?? []).some((kickoff) => {
    const lead = kickoff.getTime() - now.getTime();
    return lead > PROP_CAPTURE_LEAD_MS.min && lead <= PROP_CAPTURE_LEAD_MS.max;
  });
}

/** Daily 10:07 UTC, plus one tick 3–3.25 hours before each kickoff group. */
export function cfbForecastDue(now: Date, context: DispatchContext): boolean {
  const month = now.getUTCMonth() + 1;
  if (month > 2 && month < 8) return false;
  if (now.getUTCHours() === 10 && hourTick(now)) return true;
  return (context.cfbKickoffs ?? []).some((kickoff) => {
    const lead = kickoff.getTime() - now.getTime();
    return lead > 3 * 3600_000 && lead <= 3.25 * 3600_000;
  });
}

/**
 * Game Leaders republish lead: one tick 150-165 minutes before each kickoff
 * group. The run takes ~10 minutes and Vercel then deploys the page file, so
 * the new forecast is live before that group's T-75 prop capture prices it.
 */
export const GAME_LEADERS_LEAD_MS = { min: 150 * 60 * 1000, max: 165 * 60 * 1000 };

/**
 * Game Leaders: the weekly build on Wednesday 14:07 UTC (last week's stats are
 * in by then), then a fresh-availability republish before every kickoff group.
 */
export function nflGameLeadersDue(now: Date, context: DispatchContext): boolean {
  if (now.getUTCDay() === 3 && now.getUTCHours() === 14 && hourTick(now)) return true;
  return (context.nflKickoffs ?? []).some((kickoff) => {
    const lead = kickoff.getTime() - now.getTime();
    return lead > GAME_LEADERS_LEAD_MS.min && lead <= GAME_LEADERS_LEAD_MS.max;
  });
}

/**
 * Expert-coverage pass windows (docs/nfl-expert-coverage-study.md): for each
 * kickoff group, the one 15-minute tick whose lead lands in (H - 15 min, H] for
 * H in T-48h, T-24h, T-6h, T-90m. T-48h fits because the route reads kickoffs
 * up to KICKOFF_LOOKAHEAD_MS (48 h, inclusive) ahead.
 */
export const EXPERT_COVERAGE_LEADS_MIN = [48 * 60, 24 * 60, 6 * 60, 90] as const;

export function nflExpertCoverageDue(now: Date, context: DispatchContext): boolean {
  return (context.nflKickoffs ?? []).some((kickoff) => {
    const leadMin = (kickoff.getTime() - now.getTime()) / 60_000;
    return EXPERT_COVERAGE_LEADS_MIN.some((h) => leadMin > h - 15 && leadMin <= h);
  });
}

/** NBA season as calendar months (preseason lines in early October, Finals in June). */
const NBA_MONTHS = new Set([10, 11, 12, 1, 2, 3, 4, 5, 6]);

/**
 * NBA points-props capture for /nba/top-scorers: the 15:07 UTC tick (11:07 AM
 * ET, after the books post the morning's player props), the 22:07 UTC tick
 * (6:07 PM ET, the closing read before the 7 PM ET tips) and the 01:07 UTC
 * tick (9:07 PM ET, the closing read for the 9:30-10:30 PM ET tips; only the
 * games still to tip are fetched, so it is a few credits), October through
 * June. The page shows each game's latest capture, and the nightly grade
 * scores each game's last capture before its tip. Each run costs about 2
 * credits a game; a game that has already tipped is skipped by the script.
 */
export function nbaPointsPropsDue(now: Date): boolean {
  if (!NBA_MONTHS.has(now.getUTCMonth() + 1) || !hourTick(now)) return false;
  return now.getUTCHours() === 15 || now.getUTCHours() === 22 || now.getUTCHours() === 1;
}

/**
 * Nightly grade of those lines against ESPN box scores (grade_nba_points_props.yml):
 * 11:07 UTC (7:07 AM ET), after the last West Coast game has gone final. No
 * Odds API credits; three Eastern dates each run, idempotent.
 */
export function nbaPointsGradeDue(now: Date): boolean {
  return NBA_MONTHS.has(now.getUTCMonth() + 1) && hourTick(now) && now.getUTCHours() === 11;
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
    key: "nfl-prop-capture",
    workflow: "capture_nfl_props.yml",
    due: nflPropCaptureDue,
    // Only the group kicking off 60-75 minutes from now: min 1h keeps an earlier
    // group (already priced at its own T-75) from being bought twice.
    inputs: { event_limit: "0", max_credits: "420", within_hours: "1.25", min_hours: "1" },
    why: "Pregame player over/unders and DraftKings ladders (Odds API, ~21 credits a game), 60-75 minutes before each kickoff group: the closing read the DFS projection anchors on.",
  },
  {
    key: "nfl-prop-capture-day-ahead",
    workflow: "capture_nfl_props.yml",
    due: nflPropDayAheadDue,
    inputs: { event_limit: "0", max_credits: "420", within_hours: "36", day_ahead: "true" },
    why: "The same prop lines the day before (Wednesday for Thursday, Saturday for Sunday, Sunday for Monday), so an early build has real lines.",
  },
  {
    key: "nfl-prop-capture-wed",
    workflow: "capture_nfl_props.yml",
    due: nflPropWednesdayDue,
    inputs: { event_limit: "0", max_credits: "420", within_hours: String(PROP_MIDWEEK_WINDOW_HOURS.wed), min_hours: String(PROP_MIDWEEK_MIN_HOURS), markets: "main" },
    why: "Wednesday-evening main lines for Sunday and Monday (12 markets, ~180 credits): what the field saw when it started building, for the field-lag study.",
  },
  {
    key: "nfl-prop-capture-fri",
    workflow: "capture_nfl_props.yml",
    due: nflPropFridayDue,
    inputs: { event_limit: "0", max_credits: "420", within_hours: String(PROP_MIDWEEK_WINDOW_HOURS.fri), min_hours: String(PROP_MIDWEEK_MIN_HOURS), markets: "main" },
    why: "Friday-evening main lines for Sunday and Monday, after the week's practice reports, for the field-lag study.",
  },
  {
    key: "nba-points-props",
    workflow: "capture_nba_points_props.yml",
    due: nbaPointsPropsDue,
    // Blank date = today ET inside the script. 40 credits covers a 15-game night
    // (2 per game) plus a few; the script aborts before any paid call past it.
    inputs: { date: "", max_credits: "40" },
    why: "Tonight's player-points lines and alternate ladders for /nba/top-scorers (Odds API, ~2 credits a game), 11:07 AM, 6:07 PM and 9:07 PM ET, October-June.",
  },
  {
    key: "nba-points-grade",
    workflow: "grade_nba_points_props.yml",
    due: nbaPointsGradeDue,
    inputs: { date: "", days_back: "3" },
    why: "ESPN box scores and the calibration grade of last night's pre-tip points lines for /nba/top-scorers, 7:07 AM ET, October-June.",
  },
  {
    key: "nfl-game-leaders",
    workflow: "refresh_nfl_game_leaders.yml",
    due: nflGameLeadersDue,
    why: "Game Leaders page: the weekly build Wednesday, then fresh availability before each kickoff group; commits web/src/data/game-leaders.json.",
  },
  {
    key: "nfl-expert-coverage",
    workflow: "capture_nfl_expert_coverage.yml",
    due: nflExpertCoverageDue,
    why: "Pregame expert DFS articles (robots-checked, append-only) and their DeepSeek extraction at T-48h, T-24h, T-6h and T-90m of every kickoff group: the ownership-signal study's inputs.",
  },
  {
    key: "nfl-dfs-postweek",
    workflow: "refresh_nfl_dfs_postweek.yml",
    // Monday 13:07 UTC (owner's decision 2026-10-19): Thursday and Sunday,
    // Sunday night included, are published by then (SNF was in nflverse by
    // Monday morning on 2026-10-19), an hour after the Monday 12:07 pbp
    // relabel whose labels DST scoring reads (nfl_dfs_results.pbp_components).
    // Then Tuesday 14:07 UTC, after Monday night's stats land (~11:34 UTC on
    // 2026-09-29) and the 13:07 relabel. Every step grades completed games
    // only and is idempotent, so the GitHub fallback slot is harmless.
    due: (now) => ((now.getUTCDay() === 1 && now.getUTCHours() === 13) || (now.getUTCDay() === 2 && now.getUTCHours() === 14)) && hourTick(now),
    why: "Realized DK points, slate report cards, and the weekly replacement-upside grade.",
  },
  {
    key: "nfl-pbp-archetypes",
    workflow: "refresh_nfl_pbp_archetypes.yml",
    // Monday 12:07 and Tuesday 13:07 UTC. GitHub skipped its own slots on 09-14,
    // 09-21 and 09-29 and ran one seven hours late on 09-28. Tuesday is after
    // Monday night's data is published (~11:34 UTC on 2026-09-29) and an hour
    // before the post-week DST scoring that reads these labels.
    // Dispatched with no inputs = the self-healing --relabel-stale mode.
    due: (now) => ((now.getUTCDay() === 1 && now.getUTCHours() === 12) || (now.getUTCDay() === 2 && now.getUTCHours() === 13)) && hourTick(now),
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
    key: "cfb-forecasts",
    workflow: "refresh_cfb_plays.yml",
    due: cfbForecastDue,
    why: "Daily CFB forecasts and a pregame refresh about three hours before each kickoff group, with input and publication checks.",
  },
  {
    key: "cfb-terminal",
    workflow: "refresh_cfb_terminal.yml",
    // Every tick, August-January. Its GitHub schedule asked for :07/:22/:37/:52
    // but GitHub started it every 1-3 hours, so the CFB pilot monitor (which
    // expects those four slots) alarmed on gaps nobody chose. The job makes no
    // paid Odds API request (closes are capture_event_closes.yml's), so the
    // extra runs cost GitHub minutes only. `slot` tells the job which of its
    // three former cron lines this tick stands in for, so it still refreshes
    // the full CFBD schedule every six hours and scores hourly, not on every
    // tick (a plain dispatch would refresh the schedule each time).
    due: (now) => { const m = now.getUTCMonth() + 1; return m >= 8 || m <= 1; },
    inputsAt: (now) => ({ force_schedule: "false", capture_now: "false", slot: cfbTerminalSlot(now) }),
    why: "CFB line terminal: event mappings every 15 minutes, scores hourly, full schedule every 6 hours.",
  },
  {
    key: "cfb-pattern-board",
    workflow: "cfb_pattern_watch.yml",
    // Every half hour, August-January. The board freezes a pre-registered
    // selection the first time its pattern fires on a fresh line, so it has to
    // see every capture window: Saturday slates, weeknight MACtion and bowls.
    // A run with no game in the next 36 hours still writes its heartbeat. No
    // Odds API credits: it reads the stored captures.
    due: (now) => { const m = now.getUTCMonth() + 1; return (m >= 8 || m <= 1) && halfHourTick(now); },
    inputs: { job: "board" },
    why: "CFB pattern study: frozen selections, stale-line refusals and late-move flags before kickoff.",
  },
  {
    key: "cfb-pattern-grade",
    workflow: "cfb_pattern_watch.yml",
    // 10:07 UTC daily, August-January: after the last West Coast finals and
    // the overnight close capture. Idempotent; a re-run grades nothing twice.
    due: (now) => { const m = now.getUTCMonth() + 1; return (m >= 8 || m <= 1) && now.getUTCHours() === 10 && hourTick(now); },
    inputs: { job: "grade" },
    why: "CFB pattern study: grades finished games at the verified close and refreshes the reader page.",
  },
  {
    key: "nfl-defensive-capture",
    workflow: "capture_nfl_defensive_on_upload.yml",
    // Only when the database says a request is waiting. The upload itself
    // dispatches the worker; this is the retry when that dispatch failed or the
    // run died holding a lease. Never on a schedule of its own, so an idle week
    // costs nothing.
    due: (_now, context) => context.nflDefensiveCapturesPending === true,
    inputs: { upload_id: "" },
    why: "Upload-triggered opponent captures: retries any request still pending after its own dispatch.",
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
  return DISPATCH_JOBS.filter((job) => job.due(now, context))
    .map((job) => job.inputsAt ? { ...job, inputs: { ...job.inputs, ...job.inputsAt(now) } } : job);
}

export interface DispatchOutcome {
  key: string; workflow: string; ok: boolean; status: number; detail?: string;
  /** Present when GitHub returned the run it created (`returnRunDetails`). */
  runId?: number; htmlUrl?: string;
  /** When the token used expires (ISO), from GitHub's response header; absent for tokens without one. */
  tokenExpiresAt?: string;
}

/**
 * GitHub's `github-authentication-token-expiration` header ("2026-11-15
 * 12:00:00 UTC" for a fine-grained PAT), as an ISO time; null when absent or
 * unparseable. A fine-grained PAT expires within a year, and when it does every
 * bridged job goes quiet at once, so the date is surfaced on /health.
 */
export function parseTokenExpiry(header: string | null): string | null {
  if (!header) return null;
  const t = Date.parse(header);
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
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
    const tokenExpiresAt = parseTokenExpiry(response.headers.get("github-authentication-token-expiration"));
    const expiry = tokenExpiresAt ? { tokenExpiresAt } : {};
    if (tokenExpiresAt) {
      const daysLeft = (Date.parse(tokenExpiresAt) - Date.now()) / 86_400_000;
      if (daysLeft < 30) console.error(`cron dispatch: GITHUB_DISPATCH_TOKEN expires in ${Math.floor(daysLeft)} days (${tokenExpiresAt})`);
    }
    if (response.status === 204) return { key: job.key, workflow: job.workflow, ok: true, status: 204, ...expiry };
    if (response.status === 200) {
      const body = await response.json().catch(() => null) as { workflow_run_id?: number; html_url?: string } | null;
      return { key: job.key, workflow: job.workflow, ok: true, status: 200, ...expiry,
        ...(typeof body?.workflow_run_id === "number" ? { runId: body.workflow_run_id } : {}),
        ...(typeof body?.html_url === "string" ? { htmlUrl: body.html_url } : {}) };
    }
    const detail = (await response.text()).slice(0, 300);
    return { key: job.key, workflow: job.workflow, ok: false, status: response.status, detail, ...expiry };
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
