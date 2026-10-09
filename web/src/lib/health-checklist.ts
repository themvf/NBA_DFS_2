/**
 * The /health checklist: every item that can fail, one row each, PASS or FAIL,
 * with when it last ran, when it should next run, when it was last checked and
 * when it will next be checked.
 *
 * Items:
 *   - every GitHub workflow: last run failed -> FAIL; overdue against its own
 *     schedule (GitHub cron and/or the Vercel dispatcher) -> FAIL; never ran
 *     although scheduled -> FAIL; manual-only -> INFO unless its last run
 *     (within 14 days) failed.
 *   - every dataset the freshness monitor watches (pipeline_health_snapshots).
 *   - every Vercel cron route (cron_heartbeats).
 *   - every upcoming NFL slate's Slate Check.
 *   - the NFL injury/depth availability monitor (nfl_availability_operation_runs).
 *   - any source the checklist could not read: its own FAIL row, never omitted.
 *
 * Pure: the collector (lib/health-collector) gathers inputs and stores results.
 */
import { cronTimes, lastCronTime, longestGapMs, nextCronTime } from "@/lib/cron-schedule";
import { cronStatuses, type CronHeartbeat } from "@/lib/cron-heartbeat";
import { didNotRun, isFailedRun, isFinishedRun, type WorkflowRunLite } from "@/lib/workflow-health";

export type HealthStatus = "pass" | "fail" | "info";
export type HealthGroup = "NFL DFS" | "Scheduled jobs" | "Data freshness" | "Clocks" | "Checklist";

export interface HealthItem {
  key: string;
  group: HealthGroup;
  label: string;
  status: HealthStatus;
  detail: string;
  url: string | null;
  /** When the checked thing last happened (job run, data write, clock tick). */
  lastEventAt: string | null;
  /** When it should next happen, if scheduled. */
  nextEventAt: string | null;
  /** Free-text next-run description when there is no time (e.g. "after Projection refresh"). */
  nextEventNote?: string | null;
  lastCheckedAt: string;
  nextCheckAt: string | null;
}

export interface ManifestWorkflow {
  file: string; name: string; crons: string[]; dispatch: boolean; afterWorkflows: string[]; push: boolean; pullRequest: boolean;
}

export interface ChecklistInputs {
  now: Date;
  /** When the next checklist run is due (the collector's cadence). */
  nextCheckAt: Date;
  manifest: ManifestWorkflow[];
  /** workflow file -> dispatcher due times in the past 8 days and next 8 days. */
  dispatchTimes: Record<string, { past: Date[]; future: Date[] }>;
  /** workflow file -> recent runs, any order (pull-request runs are ignored). Null when GitHub could not be read. */
  runs: Record<string, WorkflowRunLite[]> | null;
  /** workflow file -> GitHub workflow state (active, disabled_manually, ...). */
  workflowStates: Record<string, string> | null;
  /** workflow file -> why its runs could not be read (one bad read never hides the rest). */
  workflowErrors?: Record<string, string>;
  githubError: string | null;
  datasets: { key: string; label: string; status: string; lastRowAt: string | null; ageHours: number | null; maxAgeHours: number; ownerWorkflow: string; checkedAt: string; note?: string | null; detail?: string | null }[] | null;
  datasetsError: string | null;
  /** How often the freshness monitor runs, for its next-check time. */
  datasetCadenceHours: number;
  heartbeats: CronHeartbeat[] | null;
  heartbeatsError: string | null;
  slates: { signature: string; headline: string; needs: number; checkedAt: string }[] | null;
  slatesError: string | null;
  availabilityOps: { status: string; evaluatedAt: string; week: number; alerts: string[] } | null;
  availabilityOpsError: string | null;
  /** When the dispatch PAT expires (ISO) and when that was observed; null when never observed. */
  dispatchToken?: { expiresAt: string; observedAt: string } | null;
  /** The Odds API quota as the capture jobs recorded it. */
  oddsApi?: { latest: OddsApiReading; series: OddsApiReading[] } | null;
  oddsApiError?: string | null;
  /** The commit production runs, as the deployed health-check route last reported it. */
  deployedCommit?: { sha: string; observedAt: string } | null;
  /** main's head commit. */
  mainHead?: { sha: string; at: string; source: string } | null;
  mainHeadError?: string | null;
}

/** One Odds API quota reading from `odds_api_usage`. */
export interface OddsApiReading { requestedAt: string; used: number; remaining: number }

/**
 * The only way to quiet a row. A FAIL row whose key equals `match`, or starts
 * with it, is shown as INFO ("Muted by owner since ...") and is never emailed,
 * because the daily sweep takes only FAIL rows. Never disable a workflow, drop a
 * dataset or loosen a threshold to stop an email; add an entry here, with a date
 * and a reason, and remove it to bring the row back.
 */
export const MUTED: ReadonlyArray<{ match: string; since: string; reason: string }> = [
  { match: "workflow:refresh_tennis", since: "2026-09-29", reason: "tennis is out of scope for now" },
  { match: "data:tennis", since: "2026-09-29", reason: "tennis is out of scope for now" },
  { match: "workflow:load_mlb_slate.yml", since: "2026-09-29", reason: "MLB DFS is out of scope for now" },
  { match: "workflow:refresh_mlb_beat_articles.yml", since: "2026-09-29", reason: "the MLB beat-writer pilot is out of scope for now" },
  { match: "data:mlb_beat", since: "2026-09-29", reason: "the MLB beat-writer pilot is out of scope for now" },
  { match: "workflow:refresh_youtube_picks.yml", since: "2026-09-29", reason: "YouTube picks are out of scope for now" },
  { match: "data:youtube_picks", since: "2026-09-29", reason: "YouTube picks are out of scope for now" },
];

/** A fail row covered by MUTED becomes INFO and keeps what it would have said. */
export function applyMutes(items: HealthItem[], mutes = MUTED): HealthItem[] {
  return items.map((item) => {
    if (item.status !== "fail") return item;
    const mute = mutes.find((m) => item.key === m.match || item.key.startsWith(m.match));
    return mute ? { ...item, status: "info", detail: `Muted by owner since ${mute.since}: ${mute.reason}. Underlying: ${item.detail}` } : item;
  });
}

/** The dispatch PAT fails its row this many days before expiry: every bridged job stops when it lapses. */
export const TOKEN_WARN_DAYS = 30;
/** The Odds API row fails below this share of the plan left. */
export const ODDS_API_LOW_SHARE = 0.10;
/** A quota reading older than this is not current. */
const ODDS_API_STALE_HOURS = 48;

/**
 * Credits spent per day over the series, oldest first. A rise in `remaining`
 * is the monthly reset, not an error, so the window restarts there.
 */
export function oddsApiDailySpend(series: OddsApiReading[]): number | null {
  if (series.length < 2) return null;
  let spent = 0;
  let prev = series[0];
  let start = Date.parse(series[0].requestedAt);
  for (let i = 1; i < series.length; i += 1) {
    const r = series[i];
    if (r.remaining > prev.remaining) { spent = 0; start = Date.parse(r.requestedAt); }
    else spent += prev.remaining - r.remaining;
    prev = r;
  }
  const days = (Date.parse(series[series.length - 1].requestedAt) - start) / 86_400_000;
  return days >= 0.5 ? spent / days : null;
}

const REPO = "https://github.com/themvf/NBA_DFS_2";
/**
 * How late a run may be before it counts as missed depends on who starts it.
 * The Vercel dispatcher fires on time, so a dispatched slot gets 2 h (queueing).
 * GitHub's own scheduler is best effort: measured 2026-09-26..29 it started
 * this repo's schedules about every 4-8 h whatever the cron asked for (every-
 * 15-minute jobs ran 3.8-4.7 h apart, every-3-hour jobs 5.7-7 h, daily ones
 * ~26 h), with the longest gap ~10 h. A 2 h grace there would flag most GitHub
 * jobs most of the time. 12 h still catches a schedule GitHub has stopped
 * running; a job whose timing matters belongs on the dispatcher.
 */
export const DISPATCH_GRACE_MS = 2 * 3600_000;
export const GITHUB_CRON_GRACE_MS = 12 * 3600_000;
const MANUAL_FAIL_WINDOW_MS = 14 * 86400_000;
/**
 * A cancelled run, or one GitHub never assigned a runner, did no work. One is
 * shown and forgiven (a replaced queued run, a person stopping it, a GitHub
 * outage); this many in a row means the job is not getting to run, which
 * "Last run succeeded <weeks ago>" used to hide.
 */
export const UNRUN_FAIL_STREAK = 3;
const NFL_WORKFLOWS = new Set(["refresh_nfl_dfs_projections.yml", "refresh_nfl_availability_context.yml", "refresh_nfl_dk_pool.yml",
  "capture_nfl_availability.yml", "refresh_nfl_vegas.yml", "refresh_nfl_dfs_research.yml", "refresh_nfl_dfs_postweek.yml",
  "refresh_nfl_pbp_archetypes.yml", "refresh_nfl_specials.yml", "refresh_nfl_survivor.yml", "capture_nfl_odds.yml"]);

const iso = (d: Date | null | undefined) => (d ? d.toISOString() : null);
const et = (value: string | Date) => {
  const t = typeof value === "string" ? new Date(value) : value;
  return Number.isFinite(t.getTime())
    ? t.toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) + " ET"
    : String(value);
};

/** Hours past its cadence before the freshness monitor itself is overdue. */
export const MONITOR_GRACE_HOURS = 1;

const hours = (h: number) => (h < 1 ? `${Math.round(h * 60)} min` : h < 48 ? `${h.toFixed(1)} h` : `${(h / 24).toFixed(1)} days`);

/**
 * What the freshness reading saw, stated as of that reading: its age figure was
 * true when it was taken, not now. A row that is not due explains why in the
 * checker's own words (a season, a weekly deadline) instead of a generic label.
 */
export function datasetDetail(d: NonNullable<ChecklistInputs["datasets"]>[number]): string {
  const at = `at the ${et(d.checkedAt)} reading`;
  const reason = d.detail?.trim();
  // pipeline_health reads a step's `if:` gate: "paused on the schedule: '<step>' in <file> runs only when ...".
  if (d.status === "dormant" && reason && /^paused\b[^:]*:/i.test(reason)) return `Paused on purpose: ${reason.replace(/^paused\b[^:]*:\s*/i, "")}.`;
  if (d.status === "dormant") return `Not due now: ${reason || d.note || "not expected to write"}.`;
  if (d.ageHours == null) return reason && !/^no rows at all$/i.test(reason) ? `${reason[0].toUpperCase()}${reason.slice(1)} (${at}).` : `No rows at all ${at}; written by ${d.ownerWorkflow}.`;
  const over = d.status === "stale" ? `, ${(d.ageHours / d.maxAgeHours).toFixed(1)}x its ${d.maxAgeHours} h budget` : `; budget ${d.maxAgeHours} h`;
  return `Newest row was ${hours(d.ageHours)} old ${at}${over}; written by ${d.ownerWorkflow}.`;
}

function unreadable(key: string, group: HealthGroup, label: string, error: string, input: ChecklistInputs): HealthItem {
  return { key, group, label, status: "fail", detail: `Could not be checked: ${error}`, url: null, lastEventAt: null, nextEventAt: null,
    lastCheckedAt: input.now.toISOString(), nextCheckAt: iso(input.nextCheckAt) };
}

function workflowItem(w: ManifestWorkflow, input: ChecklistInputs, runsByName: Map<string, WorkflowRunLite[]>): HealthItem {
  const now = input.now.getTime();
  const runs = (input.runs?.[w.file] ?? []).filter((r) => r.event !== "pull_request").sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  const finished = runs.filter(isFinishedRun);
  const last = runs[0] ?? null;
  const lastFinished = finished[0] ?? null;
  let unrun = 0;
  for (const r of runs) { if (didNotRun(r)) unrun += 1; else break; }
  const unrunRuns = runs.slice(0, unrun);
  const unrunWhy = unrunRuns.every((r) => r.neverStarted) ? "never started by GitHub (no runner was assigned)"
    : unrunRuns.some((r) => r.neverStarted) ? "cancelled or never started by GitHub" : "cancelled before finishing";
  const dispatch = input.dispatchTimes[w.file] ?? { past: [], future: [] };
  // The next 8 days of fire times give the cadence text; a cron that fires less
  // often than that (monthly, seasonal) still gets its true next time.
  const futureCron = w.crons.length ? cronTimes(w.crons, input.now, 8 * 1440, 50) : [];
  const future = [...futureCron, ...dispatch.future].sort((a, b) => a.getTime() - b.getTime());
  const nextEvent = future[0] ?? (w.crons.length ? nextCronTime(w.crons, input.now) : null);
  const scheduled = w.crons.length > 0 || dispatch.future.length > 0 || dispatch.past.length > 0;
  const followsOthers = w.afterWorkflows.length > 0;
  const group: HealthGroup = NFL_WORKFLOWS.has(w.file) ? "NFL DFS" : "Scheduled jobs";
  const state = input.workflowStates?.[w.file];
  const readError = input.workflowErrors?.[w.file];
  if (readError) {
    const base0 = { key: `workflow:${w.file}`, group, label: w.name, url: `${REPO}/actions/workflows/${w.file}`, lastEventAt: null, nextEventAt: iso(nextEvent),
      lastCheckedAt: input.now.toISOString(), nextCheckAt: iso(input.nextCheckAt) };
    // A 404 is a workflow added in code but not yet on GitHub's default branch.
    return /\b404\b/.test(readError)
      ? { ...base0, status: "info", detail: "Not on GitHub's default branch yet (a new workflow); checked once it is merged." }
      : { ...base0, status: "fail", detail: `Its runs could not be read: ${readError}` };
  }
  const base = {
    key: `workflow:${w.file}`, group, label: w.name, url: last?.url ?? `${REPO}/actions/workflows/${w.file}`,
    lastEventAt: last?.createdAt ?? null, nextEventAt: iso(nextEvent),
    nextEventNote: !nextEvent && followsOthers ? `after ${w.afterWorkflows.join(", ")}` : !nextEvent && !scheduled ? "manual" : null,
    lastCheckedAt: input.now.toISOString(), nextCheckAt: iso(input.nextCheckAt),
  };
  // Disabling a workflow is a deliberate act (e.g. soccer after the World Cup): shown, not emailed.
  if (state && state !== "active") return { ...base, nextEventAt: null, nextEventNote: "disabled", status: "info",
    detail: `Workflow is ${state.replace(/_/g, " ")} in GitHub, so its schedule does not run. Re-enable it in GitHub Actions if that was not intended.` };

  // Failed last run.
  if (lastFinished && isFailedRun(lastFinished)) {
    let streak = 0;
    for (const r of finished) { if (isFailedRun(r)) streak += 1; else break; }
    const success = finished.find((r) => r.conclusion === "success");
    const manualOld = !scheduled && !followsOthers && now - Date.parse(lastFinished.createdAt) > MANUAL_FAIL_WINDOW_MS;
    if (!manualOld) {
      const run = streak === finished.length && streak > 1 ? `all of its last ${streak} runs failed` : streak > 1 ? `${streak} runs in a row failed` : "last run failed";
      return { ...base, url: lastFinished.url, status: "fail",
        detail: `${run} (latest ${et(lastFinished.createdAt)}); last success ${success ? et(success.createdAt) : "not in recent runs"}.` };
    }
  }

  // Overdue against its own schedule: it should have run at `due` and has not run since.
  // Each slot gets the grace of whoever starts it (see DISPATCH_GRACE_MS). The GitHub
  // slot is the newest fire time that is already past its grace, found by walking
  // the cron back up to 400 days, so a monthly or seasonal schedule is judged too
  // (an 8-day window used to let a missed monthly slot age out into PASS, and a
  // 400-slot cap left a */5 cron judged only against slots 6.6 days old).
  const githubDue = w.crons.length ? lastCronTime(w.crons, new Date(now - GITHUB_CRON_GRACE_MS)) : null;
  const dispatchDue = dispatch.past.filter((t) => now - t.getTime() >= DISPATCH_GRACE_MS).sort((a, b) => b.getTime() - a.getTime())[0] ?? null;
  const slots = [
    ...(githubDue ? [{ t: githubDue, by: "GitHub's scheduler", grace: GITHUB_CRON_GRACE_MS }] : []),
    ...(dispatchDue ? [{ t: dispatchDue, by: "the dispatcher", grace: DISPATCH_GRACE_MS }] : []),
  ];
  const due = slots.sort((a, b) => b.t.getTime() - a.t.getTime())[0];
  if (scheduled && due && (!last || Date.parse(last.createdAt) < due.t.getTime() - 30 * 60_000)) {
    const late = `${Math.round(due.grace / 3600_000)} h`;
    return { ...base, status: "fail",
      detail: last ? `Overdue: ${due.by} was due to start it ${et(due.t)} (allowing ${late}), but it has not run since ${et(last.createdAt)}.`
        : `Scheduled by ${due.by} (${et(due.t)}, allowing ${late}) but no run found.` };
  }

  // A workflow_run follower that did not follow its trigger's latest success.
  if (followsOthers) {
    // The newest trigger success that is more than an hour old: the follower has had time to start after it.
    const trigger = w.afterWorkflows.flatMap((name) => runsByName.get(name) ?? [])
      .filter((r) => r.conclusion === "success" && now - Date.parse(r.createdAt) > 60 * 60_000)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0];
    if (trigger && (!last || last.createdAt < trigger.createdAt)) {
      return { ...base, status: "fail", detail: `Did not run after ${trigger.name} succeeded at ${et(trigger.createdAt)}.` };
    }
  }

  if (!last) return { ...base, status: scheduled || followsOthers ? "fail" : "info", detail: scheduled || followsOthers ? "No run found." : "Manual job; never run." };
  if (last.status !== "completed") return { ...base, status: lastFinished && isFailedRun(lastFinished) ? "fail" : "pass", detail: `Running now (started ${et(last.createdAt)}).` };
  const gap = longestGapMs(future, input.now);
  const cadence = gap == null ? "" : gap < 3600_000 ? ` Runs about every ${Math.round(gap / 60_000)} min.` : gap < 86400_000 * 2 ? ` Runs at least every ${Math.round(gap / 3600_000)} h.` : "";
  if (!scheduled && !followsOthers) return { ...base, status: lastFinished?.conclusion === "success" ? "pass" : "info", detail: `Manual job; last run ${lastFinished?.conclusion ?? last.status} ${et(last.createdAt)}.` };
  // Every recent run was cancelled or skipped: nothing finished, so there is no success to report.
  if (!lastFinished) return { ...base, status: "fail", detail: `None of its last ${runs.length} runs finished (newest was ${last.conclusion} at ${et(last.createdAt)}).` };
  if (unrun >= UNRUN_FAIL_STREAK) {
    return { ...base, status: "fail", detail: `Its last ${unrun} runs were ${unrunWhy} (latest ${et(last.createdAt)}); last success ${lastFinished.conclusion === "success" ? et(lastFinished.createdAt) : "not in recent runs"}.` };
  }
  if (unrun) return { ...base, status: "pass", detail: `Last completed run succeeded ${et(lastFinished.createdAt)}; its newest ${unrun === 1 ? "run" : `${unrun} runs`} (latest ${et(last.createdAt)}) ${unrun === 1 ? "was" : "were"} ${unrunWhy}.${cadence}` };
  return { ...base, status: "pass", detail: `Last run succeeded ${et(lastFinished.createdAt)}.${cadence}` };
}

/** A workflow the checklist could not judge (a bad cron in the manifest, an unexpected run shape) is its own FAIL row, never a crash that empties the page. */
function unjudgedWorkflow(w: ManifestWorkflow, error: unknown, input: ChecklistInputs): HealthItem {
  const group: HealthGroup = NFL_WORKFLOWS.has(w.file) ? "NFL DFS" : "Scheduled jobs";
  return { ...unreadable(`workflow:${w.file}`, group, w.name, `the checklist could not judge this workflow (${error instanceof Error ? error.message : String(error)})`, input),
    url: `${REPO}/actions/workflows/${w.file}` };
}

export function buildChecklist(input: ChecklistInputs): HealthItem[] {
  const items: HealthItem[] = [];
  const now = input.now.toISOString();
  const nextCheck = iso(input.nextCheckAt);

  // Workflows.
  const nextRunByWorkflow = new Map<string, string | null>();
  if (input.githubError || !input.runs) items.push(unreadable("checklist:github", "Checklist", "GitHub job status", input.githubError ?? "no data", input));
  else {
    const runsByName = new Map<string, WorkflowRunLite[]>();
    for (const w of input.manifest) runsByName.set(w.name, input.runs[w.file] ?? []);
    for (const w of input.manifest) {
      let item: HealthItem;
      try { item = workflowItem(w, input, runsByName); } catch (error) { item = unjudgedWorkflow(w, error, input); }
      nextRunByWorkflow.set(w.file, item.nextEventAt);
      items.push(item);
    }
  }

  // Datasets.
  if (input.datasetsError || !input.datasets) items.push(unreadable("checklist:datasets", "Checklist", "Data freshness readings", input.datasetsError ?? "no data", input));
  else {
    const newest = input.datasets.reduce((m, d) => Math.max(m, Date.parse(d.checkedAt)), 0);
    const monitorAgeH = newest ? (input.now.getTime() - newest) / 3600_000 : null;
    // A reading is due every cadence; one hour of grace covers queueing. Past that the
    // readings below describe the past, so this row fails rather than vouching for them.
    const monitorLimitH = input.datasetCadenceHours + MONITOR_GRACE_HOURS;
    const monitorNext = nextRunByWorkflow.get("pipeline_health.yml") ?? (newest ? new Date(newest + input.datasetCadenceHours * 3600_000).toISOString() : null);
    items.push({ key: "checklist:freshness-monitor", group: "Checklist", label: "Data freshness monitor (Pipeline Health job)",
      status: monitorAgeH != null && monitorAgeH <= monitorLimitH ? "pass" : "fail",
      detail: monitorAgeH == null ? "It has never recorded a reading."
        : monitorAgeH <= monitorLimitH ? `Last reading ${monitorAgeH.toFixed(1)} h ago; it runs every ${input.datasetCadenceHours} h.`
        : `Overdue: last reading ${monitorAgeH.toFixed(1)} h ago, but it runs every ${input.datasetCadenceHours} h. The data rows show that old reading.`,
      url: `${REPO}/actions/workflows/pipeline_health.yml`, lastEventAt: newest ? new Date(newest).toISOString() : null,
      nextEventAt: monitorNext, lastCheckedAt: now, nextCheckAt: nextCheck });
    for (const d of input.datasets) {
      items.push({ key: `data:${d.key}`, group: d.key.startsWith("nfl") ? "NFL DFS" : "Data freshness", label: d.label,
        status: d.status === "fresh" ? "pass" : d.status === "dormant" ? "info" : "fail",
        detail: datasetDetail(d),
        // The next write is expected when the job that writes it next runs.
        url: `${REPO}/actions/workflows/${d.ownerWorkflow}`, lastEventAt: d.lastRowAt, nextEventAt: nextRunByWorkflow.get(d.ownerWorkflow) ?? null,
        // The verdict belongs to the reading, so "checked" is the reading time and the next
        // check is the monitor's next run; a lagging monitor is the monitor row's FAIL.
        lastCheckedAt: d.checkedAt, nextCheckAt: monitorNext ?? new Date(Date.parse(d.checkedAt) + input.datasetCadenceHours * 3600_000).toISOString() });
    }
  }

  // Vercel crons.
  if (input.heartbeatsError || !input.heartbeats) items.push(unreadable("checklist:heartbeats", "Checklist", "Vercel cron heartbeats", input.heartbeatsError ?? "no data", input));
  else for (const c of cronStatuses(input.heartbeats, input.now.getTime())) {
    items.push({ key: `clock:${c.route}`, group: "Clocks", label: c.label, status: c.state === "ok" ? "pass" : "fail",
      detail: c.state === "ok" ? `Last run succeeded.` : `${c.state === "never" ? "Never run" : c.state === "late" ? "Late" : "Failing"}: ${c.text}`,
      url: null, lastEventAt: c.heartbeat?.lastRunAt ?? null, nextEventAt: c.nextRunAt, lastCheckedAt: now, nextCheckAt: nextCheck });
  }

  // NFL slates.
  if (input.slatesError || !input.slates) items.push(unreadable("checklist:slates", "Checklist", "NFL Slate Checks", input.slatesError ?? "no data", input));
  else for (const s of input.slates) {
    items.push({ key: `slate:${s.signature}`, group: "NFL DFS", label: `Slate Check: ${s.headline.split(":")[0]}`, status: s.needs > 0 ? "fail" : "pass",
      detail: s.headline, url: "/dfs/nfl", lastEventAt: s.checkedAt, nextEventAt: null, lastCheckedAt: s.checkedAt, nextCheckAt: nextCheck });
  }

  // NFL availability monitor.
  if (input.availabilityOpsError) items.push(unreadable("checklist:availability-ops", "Checklist", "NFL availability monitor", input.availabilityOpsError, input));
  else {
    const ops = input.availabilityOps;
    items.push({ key: "nfl:availability-monitor", group: "NFL DFS", label: "Injury and depth-chart monitor",
      status: !ops ? "fail" : ops.status === "healthy" ? "pass" : ops.status === "critical" ? "fail" : "info",
      detail: !ops ? "No monitor result recorded." : `Week ${ops.week}: ${ops.status}${ops.alerts.length ? ` (${ops.alerts.slice(0, 3).join("; ")}${ops.alerts.length > 3 ? "; …" : ""})` : ""}.`,
      url: `${REPO}/actions/workflows/refresh_nfl_availability_context.yml`, lastEventAt: ops?.evaluatedAt ?? null,
      nextEventAt: nextRunByWorkflow.get("refresh_nfl_availability_context.yml") ?? null,
      lastCheckedAt: now, nextCheckAt: nextCheck });
  }

  // The dispatch token. When it lapses, every job on the Vercel dispatcher stops at once.
  if (input.dispatchToken !== undefined) {
    const t = input.dispatchToken;
    const days = t ? (Date.parse(t.expiresAt) - input.now.getTime()) / 86_400_000 : null;
    items.push({ key: "checklist:dispatch-token", group: "Checklist", label: "GitHub dispatch token (GITHUB_DISPATCH_TOKEN)",
      status: days == null ? "info" : days < TOKEN_WARN_DAYS ? "fail" : "pass",
      detail: days == null || !t ? "No expiry has been observed yet; the dispatcher records it on its next tick."
        : days < 0 ? `Expired ${et(t.expiresAt)}: every dispatched job has stopped. Create a new token and update it in Vercel.`
        : days < TOKEN_WARN_DAYS ? `Expires ${et(t.expiresAt)} (${Math.floor(days)} days). Renew it before then, or every dispatched job stops.`
        : `Expires ${et(t.expiresAt)} (${Math.floor(days)} days).`,
      url: null, lastEventAt: t?.observedAt ?? null, nextEventAt: null, lastCheckedAt: now, nextCheckAt: nextCheck });
  }

  // The shared Odds API quota. Exhaustion answers 401 on every paid call, across every sport.
  if (input.oddsApiError) items.push(unreadable("checklist:odds-api", "Checklist", "Odds API credits", input.oddsApiError, input));
  else if (input.oddsApi !== undefined) {
    const u = input.oddsApi;
    const plan = u ? u.latest.used + u.latest.remaining : null;
    const ageH = u ? (input.now.getTime() - Date.parse(u.latest.requestedAt)) / 3600_000 : null;
    const stale = ageH != null && ageH > ODDS_API_STALE_HOURS;
    const rate = u ? oddsApiDailySpend(u.series) : null;
    const daysLeft = u && rate && rate > 0 ? u.latest.remaining / rate : null;
    const low = u && plan ? u.latest.remaining / plan < ODDS_API_LOW_SHARE : false;
    items.push({ key: "data:odds-api-credits", group: "Data freshness", label: "Odds API credits (shared by every sport)",
      status: !u || stale ? "info" : low ? "fail" : "pass",
      detail: !u || plan == null ? "No quota reading has been recorded."
        : `${u.latest.remaining.toLocaleString("en-US")} of ${plan.toLocaleString("en-US")} left as of ${et(u.latest.requestedAt)}`
          + (rate != null ? `; spending about ${Math.round(rate).toLocaleString("en-US")} a day` : "")
          + (daysLeft != null ? ` (about ${Math.floor(daysLeft)} days at that rate)` : "")
          + (low ? `. Below ${ODDS_API_LOW_SHARE * 100}% of the plan: at zero every paid call answers 401 for every sport.` : ".")
          + (stale && ageH != null ? ` No reading for ${Math.floor(ageH)} h.` : ""),
      url: null, lastEventAt: u?.latest.requestedAt ?? null, nextEventAt: null, lastCheckedAt: now, nextCheckAt: nextCheck });
  }

  // Which commit production runs. Informational: Vercel skips a build when nothing under
  // web/ changed (web/vercel.json ignoreCommand), so production can trail main by design.
  if (input.deployedCommit !== undefined || input.mainHead !== undefined) {
    const d = input.deployedCommit ?? null;
    const m = input.mainHead ?? null;
    const short = (sha: string) => sha.slice(0, 7);
    items.push({ key: "checklist:deployed-commit", group: "Checklist", label: "Production deployment",
      status: "info",
      detail: (d ? `Production runs ${short(d.sha)} (reported ${et(d.observedAt)})` : "Production has not reported its commit yet")
        + (m ? `; main is at ${short(m.sha)} (${et(m.at)})` : input.mainHeadError ? `; main could not be read (${input.mainHeadError})` : "")
        + (d && m && d.sha !== m.sha ? ". They differ, which is expected when the newer commits changed nothing under web/." : "."),
      url: null, lastEventAt: d?.observedAt ?? null, nextEventAt: null, lastCheckedAt: now, nextCheckAt: nextCheck });
  }

  const order: Record<HealthStatus, number> = { fail: 0, info: 1, pass: 2 };
  return applyMutes(items).sort((a, b) => order[a.status] - order[b.status] || a.group.localeCompare(b.group) || a.label.localeCompare(b.label));
}
