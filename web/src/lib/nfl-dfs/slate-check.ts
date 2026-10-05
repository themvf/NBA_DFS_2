/**
 * The Slate Check: one card that says, in plain words, whether every part of
 * the pipeline actually did its job for this slate -- before Generate.
 *
 * Why it exists (PHI@CHI, 2026-09-28): features failed quietly. QB roles went
 * blank, backups stayed eligible, the backup-QB promotion never ran, opponent
 * adjustments applied to 0 of 56 players while the page said "Experimental",
 * and the ownership estimate read 111%. Each fell back to a default and the
 * page looked normal. Every line here is computed from the evidence the build
 * will actually use, so a silent failure becomes one visible sentence with a
 * fix.
 *
 * Pure and deterministic: the server gathers the inputs, this decides.
 */

export type SlateCheckLevel = "blocked" | "attention" | "ok" | "info";
export type SlateCheckAction = "refresh_projections" | "pick_starter" | "retry_capture" | "update_data";

export interface SlateCheckItem {
  id: string;
  level: SlateCheckLevel;
  text: string;
  action?: SlateCheckAction;
  team?: string;
  /** A page that shows the evidence (e.g. the failed GitHub run). */
  href?: string;
  /** Research evidence is retained but does not compete with build readiness. */
  category?: 'research';
}

export interface SlateCheck {
  headline: string;
  /** Items that need the user: blocked + attention. */
  needs: number;
  items: SlateCheckItem[];
  archived?: boolean;
}

export interface SlateCheckQb {
  name: string;
  team: string;
  /** Not playing: DK OUT or an injury status. */
  injured: boolean;
  /** Current role after any override, e.g. "Expected starter · QB1". */
  role: string | null;
  /** The depth chart's role when an override replaced it. */
  chartRole: string | null;
  confirmedByUser: boolean;
  /** Took over a ruled-out starter's workload (projectionScenario availability_estimate). */
  promoted: boolean;
  /** Led his team in pass attempts in its most recent game (the starter-evidence rule). */
  startedLastGame: boolean;
}

export interface SlateCheckInput {
  label: string;
  now: number;
  firstKickoff: string | null;
  deployedBuild: boolean;
  incompleteWarning: string | null;
  refreshAvailable: boolean;
  /** Why the check for newer projections failed; null when it ran. "No newer run" is not "current" when this is set. */
  refreshError?: string | null;
  projectionAsOf: string | null;
  rosterStaleWarning: string | null;
  rosterCapturedAt: string | null;
  qbs: SlateCheckQb[];
  /**
   * Per defensive profile: players adjusted / eligible, or why the captures
   * could not be read. `captured`: players with ANY capture for this upload;
   * 0 means none exists yet, which is a different problem from captures that
   * all failed their checks. Null when not computed.
   */
  opponentAdjustments: { label: string; applied: number; eligible: number; captured?: number; error?: string | null }[] | null;
  /** When the next scheduled opponent capture starts (ISO); null when unknown. */
  nextOpponentCapture?: string | null;
  /**
   * Per profile, what happened to this upload's own capture request
   * (lib/nfl-dfs/defensive-capture-status). Absent on reads that predate it;
   * the scheduled-capture wording is then used.
   */
  defensiveCaptures?: { status: string; text: string; retryable: boolean }[] | null;
  /** `coverage`: how many players' ownership came from each source, when LineStar supplied any. */
  ownership: { source: string | null; errors: string[]; coverage?: { linestar: number; estimate: number; total: number } | null } | null;
  availability: { state: "blind" | "thin" | "adequate"; resolved: number; considered: number } | null;
  /**
   * `capturedAt`: the successful poll the overlay used. `lastSuccessfulAt`: the
   * latest successful poll whether or not it was applied. `lastPollOk`: whether
   * the very latest poll, successful or not, worked.
   */
  liveDk: { applied: boolean; reason: string | null; capturedAt: string | null; lastSuccessfulAt?: string | null; lastPollOk?: boolean | null } | null;
  upside: { flagged: number; skipped: { name: string; reason: string }[]; error?: string } | null;
  unmatched: string[];
  /**
   * NFL data jobs whose latest run failed (lib/workflow-health), or why their
   * status could not be read. Null when not checked at all.
   */
  pipeline?: { failing: { label: string; failedAt: string; url: string; streak: number; streakCapped: boolean; affectsBuild: boolean }[]; error: string | null } | null;
  /** The experimental projection sources (workload, calibrated): usable now, or why not. */
  experimentalSources?: { label: string; usable: boolean; reason: string }[] | null;
  specialTeams?: { text: string; action?: 'refresh_projections' | 'update_data'; missing: readonly unknown[] } | null;
}

const STARTER = "Expected starter · QB1";

/**
 * Age limits, measured from NOW (the request), not from the projection
 * cutoff: "no newer run exists" only says nothing newer was built, not that
 * what exists is recent. Projections and depth charts rebuild about hourly in
 * season, so 12 h and 24 h mean the pipeline has stopped. DraftKings statuses
 * are polled every 15-30 minutes on game days and every few hours otherwise,
 * so 2 h needs you only close to kickoff (or when the latest poll failed).
 */
export const PROJECTION_STALE_HOURS = 12;
export const ROSTER_STALE_HOURS = 24;
export const DK_STATUS_STALE_HOURS = 2;
const DK_STATUS_URGENT_WITHIN_HOURS = 12;

const clock = (iso: string | null) => {
  if (!iso) return null;
  const t = new Date(iso);
  return Number.isFinite(t.getTime())
    ? t.toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit" }) + " ET"
    : null;
};

/**
 * The check with a note that it could not be saved. The slate picker shows the
 * last RECORDED check, so a failed save leaves it showing an older one; before
 * 2026-09-29 that went only to the server log.
 */
export function withRecordFailure(check: SlateCheck, error: unknown): SlateCheck {
  const reason = error instanceof Error && error.message ? error.message : String(error);
  return { ...check, items: [...check.items, { id: "record", level: "info",
    text: `This check couldn't be saved, so the slate list may still show an older one: ${reason}` }] };
}

/**
 * Why no player has an opponent adjustment, in words that say what to do.
 *
 * Captures are written by the scheduled projection workflow for the uploads
 * that exist when it runs, keyed to the exact upload. A slate uploaded (or
 * refreshed onto newer projections, which makes a new upload) after the last
 * run before kickoff therefore never gets one. PIT@CLE 2026-10-01 was uploaded
 * after the 5:35 PM ET run; the page said "not available yet" and the build
 * note said "no player passed the frozen capture checks", though none existed.
 */
function opponentMissingText(input: SlateCheckInput): string {
  const captured = input.opponentAdjustments?.some((p) => (p.captured ?? 0) > 0) ?? false;
  if (captured) return "Opponent adjustments were captured for this upload, but no player passed their checks, so builds use unadjusted projections.";
  const next = input.nextOpponentCapture ?? null;
  const nextAt = next ? Date.parse(next) : NaN;
  const kickoff = input.firstKickoff ? Date.parse(input.firstKickoff) : NaN;
  const when = clock(next);
  const timing = !when ? ""
    : Number.isFinite(kickoff) && nextAt >= kickoff
      ? ` The next scheduled capture starts ${when}, after kickoff, so this slate won't get them.`
      : ` The next scheduled capture starts ${when} and takes several minutes; reload after it to pick them up.`;
  return `No opponent adjustments have been captured for this upload yet, so builds use unadjusted projections. They're captured on a schedule, only for slates already uploaded.${timing}`;
}

export function buildSlateCheck(input: SlateCheckInput): SlateCheck {
  const items: SlateCheckItem[] = [];
  const add = (item: SlateCheckItem) => items.push(item);

  if (!input.deployedBuild) add({ id: "code", level: "blocked", text: "This page is running a local copy of the code. Lineups built here can't be exported; build on the live site." });
  if (input.incompleteWarning) add({ id: "slate", level: "blocked", text: input.incompleteWarning });
  const started = input.firstKickoff != null && Date.parse(input.firstKickoff) <= input.now;
  if (started) add({ id: "started", level: "info", text: "Games have started. Saved projections and lineups are preserved. Open Results to review this slate." });

  // Projections and roster freshness, both relative to now.
  const hoursSince = (iso: string | null | undefined) => {
    const t = iso ? Date.parse(iso) : NaN;
    return Number.isFinite(t) ? (input.now - t) / 3.6e6 : null;
  };
  const ago = (hours: number) => hours < 1 ? `${Math.max(1, Math.round(hours * 60))} minutes` : `${Math.round(hours)} hour${Math.round(hours) === 1 ? "" : "s"}`;
  const projectionAge = hoursSince(input.projectionAsOf);
  const projectionStale = !started && projectionAge != null && projectionAge > PROJECTION_STALE_HOURS;
  if (started && input.projectionAsOf) add({ id: "projections", level: "ok", text: `Saved projections were built ${clock(input.projectionAsOf)}; this snapshot is retained.` });
  else if (input.refreshAvailable) add({ id: "projections", level: "attention", action: "refresh_projections", text: "Newer projections are available. Refresh before building so the pool reflects the latest injuries and roles." });
  else if (input.refreshError) add({ id: "projections", level: "attention", text: `Couldn't check for newer projections: ${input.refreshError}${input.projectionAsOf ? ` These were built ${clock(input.projectionAsOf)}.` : ""}` });
  else if (projectionStale) add({ id: "projections", level: "attention", text: `Projections were built ${ago(projectionAge!)} ago (${clock(input.projectionAsOf)}) and nothing newer exists; they normally rebuild every hour in season. Use Update data before building.` });
  else if (input.projectionAsOf) add({ id: "projections", level: "ok", text: `Projections are current (built ${clock(input.projectionAsOf)}).` });
  const rosterAge = hoursSince(input.rosterCapturedAt);
  if (input.rosterStaleWarning) add({ id: "roster", level: "attention", text: input.rosterStaleWarning });
  else if (!started && rosterAge != null && rosterAge > ROSTER_STALE_HOURS) add({ id: "roster", level: "attention", text: `Depth charts were captured ${ago(rosterAge)} ago (${clock(input.rosterCapturedAt)}), so roles and injuries may have changed since. Use Update data before building.` });
  else if (input.rosterCapturedAt) add({ id: "roster", level: "ok", text: `Depth charts as of ${clock(input.rosterCapturedAt)}.` });

  // Quarterbacks, team by team.
  const teams = [...new Set(input.qbs.map((q) => q.team))].sort();
  const confirmedStarters: string[] = [];
  for (const team of teams) {
    const qbs = input.qbs.filter((q) => q.team === team);
    // Only a ruled-out STARTER matters; an injured backup changes nothing.
    const injured = qbs.filter((q) => q.injured && ((q.chartRole ?? q.role) === STARTER || q.startedLastGame)).map((q) => q.name);
    const starter = qbs.find((q) => !q.injured && q.role === STARTER);
    const chart = qbs.find((q) => !q.injured && (q.chartRole ?? q.role) === STARTER);
    const promoted = qbs.find((q) => !q.injured && q.promoted);
    if (starter?.confirmedByUser && chart && chart.name !== starter.name) {
      add({ id: `qb:${team}`, team, level: "attention", action: "pick_starter", text: `${team}: you picked ${starter.name}, but the depth chart lists ${chart.name} as QB1. The build uses your pick.` });
      continue;
    }
    if (injured.length) {
      if (promoted) add({ id: `qb:${team}`, team, level: "ok", text: `${team}: ${injured.join(", ")} ${injured.length > 1 ? "are" : "is"} out. ${promoted.name} takes over the starter's workload.` });
      else if (starter) add({ id: `qb:${team}`, team, level: "attention", action: "pick_starter", text: `${team}: ${injured.join(", ")} ${injured.length > 1 ? "are" : "is"} out and ${starter.name} is listed QB1, but his projection still carries backup volume. Confirm the starter to give him the starter's workload.` });
      else add({ id: `qb:${team}`, team, level: "attention", action: "pick_starter", text: `${team}: ${injured.join(", ")} ${injured.length > 1 ? "are" : "is"} out and no replacement starter could be confirmed. Pick the starter.` });
      if (promoted) add({ id: `qb-teammates:${team}`, team, level: "info", text: `${team}'s receivers still use projections built with the previous starter; they are not adjusted for the QB change.` });
      continue;
    }
    if (!starter) add({ id: `qb:${team}`, team, level: "attention", action: "pick_starter", text: `${team}: no quarterback could be confirmed as QB1, so backups aren't blocked. Pick the starter.` });
    else confirmedStarters.push(`${starter.name} (${team})`);
  }
  if (confirmedStarters.length) add({ id: "qb:starters", level: "ok", text: `Starting QBs confirmed: ${confirmedStarters.join(", ")}. Backups are blocked.` });

  // Opponent (defensive) adjustments: how many players actually changed.
  if (input.opponentAdjustments) {
    const any = input.opponentAdjustments.filter((p) => p.applied > 0);
    // A capture that could not be READ is a different problem from one that
    // does not exist yet; before 2026-09-29 both said "not available yet".
    const failed = input.opponentAdjustments.filter((p) => p.error);
    if (any.length) add({ id: "opponent", level: "ok", text: `Opponent adjustments ready: ${any.map((p) => `${p.label} ${p.applied} of ${p.eligible} players`).join("; ")}.` });
    if (failed.length && !started) add({ id: "opponent-error", level: "attention", text: `Couldn't read opponent adjustments (${failed.map((p) => `${p.label}: ${p.error}`).join("; ")}), so builds that use them get unadjusted projections.` });
    else if (!any.length && !started) {
      const captures = input.defensiveCaptures ?? [];
      const pending = captures.filter((c) => c.status === "pending");
      const failedCaptures = captures.filter((c) => c.status === "failed");
      if (!captures.length) add({ id: "opponent", level: "attention", text: opponentMissingText(input) });
      else if (failedCaptures.length) add({ id: "opponent", level: "attention", action: failedCaptures.some((c) => c.retryable) ? "retry_capture" : undefined,
        text: `Opponent adjustments failed for this upload, so builds use unadjusted projections. ${captures.map((c) => c.text).join(" ")}` });
      // Waiting is not something to fix: say what is happening and that a build now is unadjusted.
      else if (pending.length) add({ id: "opponent", level: "info",
        text: `Opponent adjustments are being captured for this upload; builds before they finish use unadjusted projections. ${captures.map((c) => c.text).join(" ")}` });
      else add({ id: "opponent", level: "attention", action: captures.some((c) => c.retryable) ? "retry_capture" : undefined,
        text: `No opponent adjustments apply to this upload, so builds use unadjusted projections. ${captures.map((c) => c.text).join(" ")}` });
    }
  }

  // Ownership estimate.
  if (input.ownership) {
    const mix = input.ownership.coverage;
    if (input.ownership.errors.length) add({ id: "ownership", level: "attention", text: `The ownership estimate failed its checks, so the chalk fade will be off: ${input.ownership.errors[0]}` });
    else if (mix && mix.linestar > 0 && mix.estimate > 0) add({ id: "ownership", level: "attention", text: `Ownership mixes two sources: LineStar for ${mix.linestar} of ${mix.total} players and our rough estimate for the other ${mix.estimate}, so the chalk fade compares numbers from different sources. Import a complete LineStar file to use LineStar for everyone.` });
    else add({ id: "ownership", level: "ok", text: input.ownership.source === "linestar" ? "Ownership from your LineStar import." : "Ownership estimate ready (a rough estimate, not a real feed)." });
  }

  // Injury status coverage.
  if (input.availability) {
    const a = input.availability;
    if (a.state === "blind") add({ id: "availability", level: "blocked", text: `Only ${a.resolved} of ${a.considered} players have an injury status. Refresh data before building.` });
    else if (a.state === "thin") add({ id: "availability", level: "attention", text: `Injury status is known for only ${a.resolved} of ${a.considered} players.` });
    else add({ id: "availability", level: "ok", text: `Injury status known for ${a.resolved} of ${a.considered} players.` });
  }

  // Live DraftKings statuses.
  if (input.liveDk) {
    const live = input.liveDk;
    const lastOk = live.lastSuccessfulAt ?? live.capturedAt;
    const lastOkAge = hoursSince(lastOk);
    const pollFailed = live.lastPollOk === false;
    const statusStale = lastOkAge != null && lastOkAge > DK_STATUS_STALE_HOURS;
    const kickoffIn = input.firstKickoff ? (Date.parse(input.firstKickoff) - input.now) / 3.6e6 : null;
    if (!started && (pollFailed || statusStale)) {
      const urgent = pollFailed || (kickoffIn != null && kickoffIn <= DK_STATUS_URGENT_WITHIN_HOURS);
      const last = lastOk ? `the last successful check was ${ago(lastOkAge!)} ago (${clock(lastOk)})` : "no check has succeeded for this slate yet";
      add({ id: "live-dk", level: urgent ? "attention" : "info",
        text: `${pollFailed ? "The latest DraftKings status check failed" : "DraftKings statuses haven't been checked recently"}; ${last}. `
          + (urgent ? "A late scratch could be missing; use Update data before building." : "Between game days they're checked every few hours, so this is normal until game day.") });
    }
    else if (live.applied) add({ id: "live-dk", level: "ok", text: `DraftKings player statuses checked ${clock(live.capturedAt) ?? "recently"}.` });
    else if (!started) add({ id: "live-dk", level: "info", text: `Live DraftKings statuses weren't applied${live.reason ? `: ${live.reason}` : "."}` });
  }

  // Replacement upside.
  if (input.upside) {
    if (input.upside.error) add({ id: "upside", category: 'research', level: "info", text: `Replacement ranges unavailable: ${input.upside.error}` });
    else if (input.upside.flagged) add({ id: "upside", level: "ok", text: `${input.upside.flagged} backup${input.upside.flagged === 1 ? "" : "s"} show an "if he gets the job" range.` });
    for (const skip of input.upside.skipped) add({ id: `upside-skip:${skip.name}`, category: 'research', level: "info", text: `${skip.name}: ${skip.reason}` });
  }

  // Data jobs. A failed injury/projection/DraftKings job can leave the slate on
  // older data while every other line still reads fine, so it needs the user;
  // a failed research job changes nothing about a build, so it is a note.
  if (input.pipeline) {
    for (const job of input.pipeline.failing) {
      const times = job.streak > 1 ? ` (${job.streakCapped ? `at least ${job.streak}` : job.streak} runs in a row)` : "";
      add({ id: `pipeline:${job.label}`, category: job.affectsBuild ? undefined : 'research', level: job.affectsBuild && !started ? "attention" : "info", href: job.url,
        text: job.affectsBuild
          ? `The ${job.label.toLowerCase()} failed ${clock(job.failedAt)}${times}. This slate may be on older data until it runs cleanly.`
          : `The ${job.label.toLowerCase()} failed ${clock(job.failedAt)}${times}. Builds are unaffected; results and report cards may lag.` });
    }
    if (input.pipeline.error) add({ id: "pipeline:unknown", level: "info", text: `Couldn't check whether the data jobs are running: ${input.pipeline.error}` });
    else if (!input.pipeline.failing.length) add({ id: "pipeline", level: "ok", text: "Every NFL data job's latest run succeeded." });
  }

  // Experimental sources are opt-in, so their state is a note, never a nag.
  for (const source of input.experimentalSources ?? []) {
    add({ id: `source:${source.label}`, category: 'research', level: source.usable ? "ok" : "info",
      text: source.usable ? `${source.label} source ready. ${source.reason}` : `${source.label} source unavailable: ${source.reason}` });
  }

  if (input.unmatched.length) add({ id: "unmatched", level: "attention", text: `${input.unmatched.length} player${input.unmatched.length === 1 ? "" : "s"} couldn't be matched to our model and have no projection: ${input.unmatched.slice(0, 3).join(", ")}${input.unmatched.length > 3 ? ", …" : ""}.` });

  if (input.specialTeams) add({ id: 'special-teams', level: input.specialTeams.missing.length ? !started && input.specialTeams.action ? 'attention' : 'info' : 'ok',
    text: input.specialTeams.text, action: started ? undefined : input.specialTeams.action });
  // Keep the diagnostic record without presenting past preparation chores as
  // current advice, or permitting an old action from a saved check.
  if (started) for (const item of items) delete item.action;
  const order: Record<SlateCheckLevel, number> = { blocked: 0, attention: 1, ok: 2, info: 3 };
  items.sort((a, b) => order[a.level] - order[b.level]);
  // After kickoff nothing can be acted on; the items stay as a record, but the
  // card stops asking for action.
  const needs = started ? 0 : items.filter((i) => i.level === "blocked" || i.level === "attention").length;
  return {
    headline: started ? `${input.label}: games have started, building is closed`
      : needs ? `${input.label}: ${needs} thing${needs === 1 ? "" : "s"} need${needs === 1 ? "s" : ""} you` : `${input.label}: everything checked out`,
    needs, items, archived: started,
  };
}
