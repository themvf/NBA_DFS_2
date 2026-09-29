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
export type SlateCheckAction = "refresh_projections" | "pick_starter";

export interface SlateCheckItem {
  id: string;
  level: SlateCheckLevel;
  text: string;
  action?: SlateCheckAction;
  team?: string;
  /** A page that shows the evidence (e.g. the failed GitHub run). */
  href?: string;
}

export interface SlateCheck {
  headline: string;
  /** Items that need the user: blocked + attention. */
  needs: number;
  items: SlateCheckItem[];
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
  projectionAsOf: string | null;
  rosterStaleWarning: string | null;
  rosterCapturedAt: string | null;
  qbs: SlateCheckQb[];
  /** Per defensive profile: players adjusted / eligible. Null when not computed. */
  opponentAdjustments: { label: string; applied: number; eligible: number }[] | null;
  ownership: { source: string | null; errors: string[] } | null;
  availability: { state: "blind" | "thin" | "adequate"; resolved: number; considered: number } | null;
  liveDk: { applied: boolean; reason: string | null; capturedAt: string | null } | null;
  upside: { flagged: number; skipped: { name: string; reason: string }[]; error?: string } | null;
  unmatched: string[];
  /**
   * NFL data jobs whose latest run failed (lib/workflow-health), or why their
   * status could not be read. Null when not checked at all.
   */
  pipeline?: { failing: { label: string; failedAt: string; url: string; streak: number; streakCapped: boolean; affectsBuild: boolean }[]; error: string | null } | null;
  /** The experimental projection sources (workload, calibrated): usable now, or why not. */
  experimentalSources?: { label: string; usable: boolean; reason: string }[] | null;
}

const STARTER = "Expected starter · QB1";

const clock = (iso: string | null) => {
  if (!iso) return null;
  const t = new Date(iso);
  return Number.isFinite(t.getTime())
    ? t.toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit" }) + " ET"
    : null;
};

export function buildSlateCheck(input: SlateCheckInput): SlateCheck {
  const items: SlateCheckItem[] = [];
  const add = (item: SlateCheckItem) => items.push(item);

  if (!input.deployedBuild) add({ id: "code", level: "blocked", text: "This page is running a local copy of the code. Lineups built here can't be exported; build on the live site." });
  if (input.incompleteWarning) add({ id: "slate", level: "blocked", text: input.incompleteWarning });
  const started = input.firstKickoff != null && Date.parse(input.firstKickoff) <= input.now;
  if (started) add({ id: "started", level: "info", text: "Games have started. Building and export are closed for this slate; results arrive after the games." });

  // Projections and roster freshness.
  if (input.refreshAvailable) add({ id: "projections", level: "attention", action: "refresh_projections", text: "Newer projections are available. Refresh before building so the pool reflects the latest injuries and roles." });
  else if (input.projectionAsOf) add({ id: "projections", level: "ok", text: `Projections are current (built ${clock(input.projectionAsOf)}).` });
  if (input.rosterStaleWarning) add({ id: "roster", level: "attention", text: input.rosterStaleWarning });
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
    if (any.length) add({ id: "opponent", level: "ok", text: `Opponent adjustments ready: ${any.map((p) => `${p.label} ${p.applied} of ${p.eligible} players`).join("; ")}.` });
    else if (!started) add({ id: "opponent", level: "attention", text: "Opponent adjustments aren't available for this slate yet, so builds use unadjusted projections." });
  }

  // Ownership estimate.
  if (input.ownership) {
    if (input.ownership.errors.length) add({ id: "ownership", level: "attention", text: `The ownership estimate failed its checks, so the chalk fade will be off: ${input.ownership.errors[0]}` });
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
    if (input.liveDk.applied) add({ id: "live-dk", level: "ok", text: `DraftKings player statuses checked ${clock(input.liveDk.capturedAt) ?? "recently"}.` });
    else if (!started) add({ id: "live-dk", level: "info", text: `Live DraftKings statuses weren't applied${input.liveDk.reason ? `: ${input.liveDk.reason}` : "."}` });
  }

  // Replacement upside.
  if (input.upside) {
    if (input.upside.error) add({ id: "upside", level: "info", text: `Replacement ranges unavailable: ${input.upside.error}` });
    else if (input.upside.flagged) add({ id: "upside", level: "ok", text: `${input.upside.flagged} backup${input.upside.flagged === 1 ? "" : "s"} show an "if he gets the job" range.` });
    for (const skip of input.upside.skipped) add({ id: `upside-skip:${skip.name}`, level: "info", text: `${skip.name}: ${skip.reason}` });
  }

  // Data jobs. A failed injury/projection/DraftKings job can leave the slate on
  // older data while every other line still reads fine, so it needs the user;
  // a failed research job changes nothing about a build, so it is a note.
  if (input.pipeline) {
    for (const job of input.pipeline.failing) {
      const times = job.streak > 1 ? ` (${job.streakCapped ? `at least ${job.streak}` : job.streak} runs in a row)` : "";
      add({ id: `pipeline:${job.label}`, level: job.affectsBuild && !started ? "attention" : "info", href: job.url,
        text: job.affectsBuild
          ? `The ${job.label.toLowerCase()} failed ${clock(job.failedAt)}${times}. This slate may be on older data until it runs cleanly.`
          : `The ${job.label.toLowerCase()} failed ${clock(job.failedAt)}${times}. Builds are unaffected; results and report cards may lag.` });
    }
    if (input.pipeline.error) add({ id: "pipeline:unknown", level: "info", text: `Couldn't check whether the data jobs are running: ${input.pipeline.error}` });
    else if (!input.pipeline.failing.length) add({ id: "pipeline", level: "ok", text: "Every NFL data job's latest run succeeded." });
  }

  // Experimental sources are opt-in, so their state is a note, never a nag.
  for (const source of input.experimentalSources ?? []) {
    add({ id: `source:${source.label}`, level: source.usable ? "ok" : "info",
      text: source.usable ? `${source.label} source ready. ${source.reason}` : `${source.label} source unavailable: ${source.reason}` });
  }

  if (input.unmatched.length) add({ id: "unmatched", level: "attention", text: `${input.unmatched.length} player${input.unmatched.length === 1 ? "" : "s"} couldn't be matched to our model and have no projection: ${input.unmatched.slice(0, 3).join(", ")}${input.unmatched.length > 3 ? ", …" : ""}.` });

  const order: Record<SlateCheckLevel, number> = { blocked: 0, attention: 1, ok: 2, info: 3 };
  items.sort((a, b) => order[a.level] - order[b.level]);
  // After kickoff nothing can be acted on; the items stay as a record, but the
  // card stops asking for action.
  const needs = started ? 0 : items.filter((i) => i.level === "blocked" || i.level === "attention").length;
  return {
    headline: started ? `${input.label}: games have started, building is closed`
      : needs ? `${input.label}: ${needs} thing${needs === 1 ? "" : "s"} need${needs === 1 ? "s" : ""} you` : `${input.label}: everything checked out`,
    needs, items,
  };
}
