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
import { cronTimes, longestGapMs } from "@/lib/cron-schedule";
import { cronStatuses, type CronHeartbeat } from "@/lib/cron-heartbeat";
import type { WorkflowRunLite } from "@/lib/workflow-health";

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
}

const REPO = "https://github.com/themvf/NBA_DFS_2";
const FAILED = new Set(["failure", "timed_out", "startup_failure"]);
/** GitHub's scheduler runs up to ~95 min late; a dispatched run can queue behind another. */
const OVERDUE_GRACE_MS = 2 * 3600_000;
const MANUAL_FAIL_WINDOW_MS = 14 * 86400_000;
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
  if (d.status === "dormant" && reason && /^paused:/i.test(reason)) return `Paused on purpose: ${reason.replace(/^paused:\s*/i, "")}.`;
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
  const finished = runs.filter((r) => r.status === "completed" && r.conclusion !== "cancelled" && r.conclusion !== "skipped");
  const last = runs[0] ?? null;
  const lastFinished = finished[0] ?? null;
  const dispatch = input.dispatchTimes[w.file] ?? { past: [], future: [] };
  const pastCron = w.crons.length ? cronTimes(w.crons, new Date(now - 8 * 86400_000), 8 * 1440).filter((t) => t.getTime() <= now) : [];
  const futureCron = w.crons.length ? cronTimes(w.crons, input.now, 8 * 1440, 50) : [];
  const future = [...futureCron, ...dispatch.future].sort((a, b) => a.getTime() - b.getTime());
  const past = [...pastCron, ...dispatch.past].sort((a, b) => a.getTime() - b.getTime());
  const scheduled = w.crons.length > 0 || dispatch.future.length > 0 || dispatch.past.length > 0;
  const followsOthers = w.afterWorkflows.length > 0;
  const group: HealthGroup = NFL_WORKFLOWS.has(w.file) ? "NFL DFS" : "Scheduled jobs";
  const state = input.workflowStates?.[w.file];
  const readError = input.workflowErrors?.[w.file];
  if (readError) {
    const base0 = { key: `workflow:${w.file}`, group, label: w.name, url: `${REPO}/actions/workflows/${w.file}`, lastEventAt: null, nextEventAt: iso(future[0]),
      lastCheckedAt: input.now.toISOString(), nextCheckAt: iso(input.nextCheckAt) };
    // A 404 is a workflow added in code but not yet on GitHub's default branch.
    return /\b404\b/.test(readError)
      ? { ...base0, status: "info", detail: "Not on GitHub's default branch yet (a new workflow); checked once it is merged." }
      : { ...base0, status: "fail", detail: `Its runs could not be read: ${readError}` };
  }
  const base = {
    key: `workflow:${w.file}`, group, label: w.name, url: last?.url ?? `${REPO}/actions/workflows/${w.file}`,
    lastEventAt: last?.createdAt ?? null, nextEventAt: iso(future[0]),
    nextEventNote: !future.length && followsOthers ? `after ${w.afterWorkflows.join(", ")}` : !future.length && !scheduled ? "manual" : null,
    lastCheckedAt: input.now.toISOString(), nextCheckAt: iso(input.nextCheckAt),
  };
  // Disabling a workflow is a deliberate act (e.g. soccer after the World Cup): shown, not emailed.
  if (state && state !== "active") return { ...base, nextEventAt: null, nextEventNote: "disabled", status: "info",
    detail: `Workflow is ${state.replace(/_/g, " ")} in GitHub, so its schedule does not run. Re-enable it in GitHub Actions if that was not intended.` };

  // Failed last run.
  if (lastFinished && FAILED.has(lastFinished.conclusion ?? "")) {
    let streak = 0;
    for (const r of finished) { if (FAILED.has(r.conclusion ?? "")) streak += 1; else break; }
    const success = finished.find((r) => r.conclusion === "success");
    const manualOld = !scheduled && !followsOthers && now - Date.parse(lastFinished.createdAt) > MANUAL_FAIL_WINDOW_MS;
    if (!manualOld) {
      const run = streak === finished.length && streak > 1 ? `all of its last ${streak} runs failed` : streak > 1 ? `${streak} runs in a row failed` : "last run failed";
      return { ...base, url: lastFinished.url, status: "fail",
        detail: `${run} (latest ${et(lastFinished.createdAt)}); last success ${success ? et(success.createdAt) : "not in recent runs"}.` };
    }
  }

  // Overdue against its own schedule: it should have run at `due` and has not run since.
  const due = [...past].reverse().find((t) => now - t.getTime() >= OVERDUE_GRACE_MS);
  if (scheduled && due && (!last || Date.parse(last.createdAt) < due.getTime() - 30 * 60_000)) {
    return { ...base, status: "fail",
      detail: last ? `Overdue: scheduled ${et(due)} but has not run since ${et(last.createdAt)}.` : `Scheduled (${et(due)}) but no run found.` };
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
  if (last.status !== "completed") return { ...base, status: lastFinished && FAILED.has(lastFinished.conclusion ?? "") ? "fail" : "pass", detail: `Running now (started ${et(last.createdAt)}).` };
  const gap = longestGapMs(future, input.now);
  const cadence = gap == null ? "" : gap < 3600_000 ? ` Runs about every ${Math.round(gap / 60_000)} min.` : gap < 86400_000 * 2 ? ` Runs at least every ${Math.round(gap / 3600_000)} h.` : "";
  if (!scheduled && !followsOthers) return { ...base, status: lastFinished?.conclusion === "success" ? "pass" : "info", detail: `Manual job; last run ${lastFinished?.conclusion ?? last.status} ${et(last.createdAt)}.` };
  return { ...base, status: "pass", detail: `Last run succeeded ${et(lastFinished!.createdAt)}.${cadence}` };
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
      const item = workflowItem(w, input, runsByName);
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

  const order: Record<HealthStatus, number> = { fail: 0, info: 1, pass: 2 };
  return items.sort((a, b) => order[a.status] - order[b.status] || a.group.localeCompare(b.group) || a.label.localeCompare(b.label));
}
