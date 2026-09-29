/**
 * The Slate Check turns every silent failure from PHI@CHI 2026-09-28 into one
 * plain sentence, and says nothing alarming when all is well.
 */
import assert from "node:assert/strict";
import { buildSlateCheck, withRecordFailure, type SlateCheckInput, type SlateCheckQb } from "../src/lib/nfl-dfs/slate-check";
import { ruledOutPlayer } from "../src/lib/nfl-dfs/confirmed-starter";

const qb = (name: string, team: string, over: Partial<SlateCheckQb> = {}): SlateCheckQb =>
  ({ name, team, injured: false, role: "Backup · QB2", chartRole: null, confirmedByUser: false, promoted: false, startedLastGame: false, ...over });
const base = (over: Partial<SlateCheckInput> = {}): SlateCheckInput => ({
  label: "PHI @ CHI", now: Date.parse("2026-09-28T20:00:00Z"), firstKickoff: "2026-09-29T00:15:00Z", deployedBuild: true,
  incompleteWarning: null, refreshAvailable: false, projectionAsOf: "2026-09-28T19:00:00Z", rosterStaleWarning: null,
  rosterCapturedAt: "2026-09-28T19:30:00Z",
  qbs: [qb("Jalen Hurts", "PHI", { role: "Expected starter · QB1" }), qb("Andy Dalton", "PHI"),
    qb("Case Keenum", "CHI", { role: "Expected starter · QB1" }), qb("Tyson Bagent", "CHI")],
  opponentAdjustments: [{ label: "PFR efficiency", applied: 16, eligible: 56 }],
  ownership: { source: "nfl-ownership-prior-v2", errors: [] },
  availability: { state: "adequate", resolved: 50, considered: 52 },
  liveDk: { applied: true, reason: null, capturedAt: "2026-09-28T19:55:00Z" },
  upside: { flagged: 2, skipped: [] }, unmatched: [], ...over,
});

// All well: nothing needs you, and the card says so.
const clean = buildSlateCheck(base());
assert.equal(clean.needs, 0);
assert.equal(clean.headline, "PHI @ CHI: everything checked out");
assert.ok(clean.items.some((i) => i.id === "qb:starters" && /Hurts \(PHI\), Case Keenum \(CHI\)|Jalen Hurts \(PHI\)/.test(i.text)));

// The night as it actually was.
const night = buildSlateCheck(base({
  refreshAvailable: true,
  qbs: [qb("Jalen Hurts", "PHI", { role: "QB role unresolved" }), qb("Andy Dalton", "PHI", { role: "QB role unresolved" }),
    qb("Caleb Williams", "CHI", { injured: true, role: "Backup · QB3", startedLastGame: true }), qb("Case Keenum", "CHI", { role: "Expected starter · QB1" }), qb("Tyson Bagent", "CHI")],
  opponentAdjustments: [{ label: "PFR efficiency", applied: 0, eligible: 56 }],
  ownership: { source: "nfl-ownership-prior-v1", errors: ["Captain + flex ownership for player 44282407 exceeds 100%."] },
}));
const text = night.items.map((i) => `${i.level}:${i.text}`).join("\n");
assert.match(text, /attention:.*Newer projections are available/);
assert.match(text, /attention:PHI: no quarterback could be confirmed as QB1, so backups aren't blocked/);
assert.match(text, /attention:CHI: Caleb Williams is out and Case Keenum is listed QB1, but his projection still carries backup volume/);
assert.match(text, /attention:Opponent adjustments aren't available for this slate yet/);
assert.match(text, /attention:The ownership estimate failed its checks/);
assert.equal(night.needs, 5);
assert.equal(night.headline, "PHI @ CHI: 5 things need you");
assert.equal(night.items[0].level, "attention", "problems come first");
assert.equal(night.items.find((i) => i.id === "projections")!.action, "refresh_projections");
assert.equal(night.items.find((i) => i.id === "qb:CHI")!.action, "pick_starter");

// After the fixes: the promotion ran, and teammates carry an honest note.
const fixed = buildSlateCheck(base({ qbs: [qb("Jalen Hurts", "PHI", { role: "Expected starter · QB1" }),
  qb("Caleb Williams", "CHI", { injured: true, startedLastGame: true }), qb("Case Keenum", "CHI", { role: "Expected starter · QB1", promoted: true })] }));
assert.match(fixed.items.find((i) => i.id === "qb:CHI")!.text, /Caleb Williams is out. Case Keenum takes over the starter's workload/);
assert.equal(fixed.items.find((i) => i.id === "qb:CHI")!.level, "ok");
assert.ok(fixed.items.some((i) => i.id === "qb-teammates:CHI" && i.level === "info"));

// An injured BACKUP is not a starter problem (Skylar Thompson, BAL, 2026-09-27).
const backupOut = buildSlateCheck(base({ qbs: [qb("Lamar Jackson", "BAL", { role: "Expected starter · QB1" }),
  qb("Skylar Thompson", "BAL", { injured: true, role: "Backup · QB3" })] }));
assert.equal(backupOut.needs, 0, "an injured backup raises nothing");
assert.ok(backupOut.items.some((i) => i.id === "qb:starters" && /Lamar Jackson \(BAL\)/.test(i.text)));

// An override against the depth chart is flagged, not silent.
const override = buildSlateCheck(base({ qbs: [qb("Jalen Hurts", "PHI", { role: "Expected starter · QB1" }),
  qb("Tyson Bagent", "CHI", { role: "Expected starter · QB1", chartRole: "Backup · QB2", confirmedByUser: true }),
  qb("Case Keenum", "CHI", { role: "Backup · starter confirmed", chartRole: "Expected starter · QB1" })] }));
assert.match(override.items.find((i) => i.id === "qb:CHI")!.text, /you picked Tyson Bagent, but the depth chart lists Case Keenum as QB1/);

// A local build blocks; after kickoff the card says building is closed and stops nagging about adjustments.
assert.equal(buildSlateCheck(base({ deployedBuild: false })).items[0].level, "blocked");
const after = buildSlateCheck(base({ now: Date.parse("2026-09-29T01:00:00Z"), opponentAdjustments: [{ label: "PFR efficiency", applied: 0, eligible: 56 }] }));
assert.ok(after.items.some((i) => i.id === "started"));
assert.equal(after.needs, 0, "nothing to act on after kickoff");
assert.match(after.headline, /games have started, building is closed/);
assert.equal(after.items.some((i) => i.id === "opponent"), false);

// Experimental sources: a note when unavailable, a passed check when usable; never a nag.
const sources = buildSlateCheck(base({ experimentalSources: [
  { label: "Workload (experimental)", usable: false, reason: "No WR volume-share run has been recorded yet." },
  { label: "Calibrated (experimental)", usable: true, reason: "Calibrated: DST 2 usable forecasts." }] }));
assert.equal(sources.needs, 0);
assert.equal(sources.items.find((i) => i.id === "source:Workload (experimental)")!.level, "info");
assert.match(sources.items.find((i) => i.id === "source:Calibrated (experimental)")!.text, /^Calibrated \(experimental\) source ready\. Calibrated: DST 2/);

// Data jobs: a failed injury/projection job needs the user; a failed research job is a note;
// a GitHub read failure is said plainly, never shown as "all clear".
const jobs = buildSlateCheck(base({ pipeline: { error: null, failing: [
  { label: "Injury and depth-chart refresh", affectsBuild: true, failedAt: "2026-09-28T19:07:00Z", url: "https://github.com/r/1", streak: 3, streakCapped: false },
  { label: "Research and report-card job", affectsBuild: false, failedAt: "2026-09-28T01:45:00Z", url: "https://github.com/r/2", streak: 1, streakCapped: false }] } }));
const injury = jobs.items.find((i) => i.id === "pipeline:Injury and depth-chart refresh")!;
assert.equal(injury.level, "attention"); assert.equal(injury.href, "https://github.com/r/1");
assert.match(injury.text, /failed .* \(3 runs in a row\)\. This slate may be on older data/);
assert.equal(jobs.items.find((i) => i.id === "pipeline:Research and report-card job")!.level, "info");
assert.equal(jobs.needs, 1);
const unknown = buildSlateCheck(base({ pipeline: { error: "GitHub answered 401", failing: [] } }));
assert.match(unknown.items.find((i) => i.id === "pipeline:unknown")!.text, /Couldn't check whether the data jobs are running: GitHub answered 401/);
assert.equal(unknown.items.some((i) => i.id === "pipeline"), false, "no all-clear when the status is unknown");
assert.equal(buildSlateCheck(base({ pipeline: { error: null, failing: [] } })).items.find((i) => i.id === "pipeline")!.level, "ok");

// --- 2026-09-29 audit: freshness is judged from NOW, and a failed check is never "current" ---
const line = (check: ReturnType<typeof buildSlateCheck>, id: string) => check.items.find((i) => i.id === id);
const refreshFailed = buildSlateCheck(base({ refreshError: "Salary Game Info must include a game date." }));
assert.equal(line(refreshFailed, "projections")!.level, "attention");
assert.match(line(refreshFailed, "projections")!.text, /^Couldn't check for newer projections: Salary Game Info must include a game date\. These were built /);
assert.equal(refreshFailed.items.some((i) => /Projections are current/.test(i.text)), false, "unknown is not current");
const oldProjections = buildSlateCheck(base({ projectionAsOf: "2026-09-28T06:00:00Z" }));
assert.equal(line(oldProjections, "projections")!.level, "attention");
assert.match(line(oldProjections, "projections")!.text, /^Projections were built 14 hours ago .* and nothing newer exists/);
assert.equal(line(buildSlateCheck(base({ projectionAsOf: "2026-09-28T09:00:00Z" })), "projections")!.level, "ok", "11 hours is within the limit");
assert.equal(line(buildSlateCheck(base({ now: Date.parse("2026-09-29T01:00:00Z"), projectionAsOf: "2026-09-28T06:00:00Z" })), "projections")!.level, "ok", "after kickoff there is nothing to act on");
const oldRoster = buildSlateCheck(base({ rosterCapturedAt: "2026-09-27T18:00:00Z" }));
assert.equal(line(oldRoster, "roster")!.level, "attention");
assert.match(line(oldRoster, "roster")!.text, /^Depth charts were captured 26 hours ago/);

// DraftKings statuses: judged by the last SUCCESSFUL poll, urgent near kickoff or after a failed poll.
const dkOld = buildSlateCheck(base({ liveDk: { applied: true, reason: null, capturedAt: "2026-09-28T17:00:00Z", lastSuccessfulAt: "2026-09-28T17:00:00Z", lastPollOk: true } }));
assert.equal(line(dkOld, "live-dk")!.level, "attention");
assert.match(line(dkOld, "live-dk")!.text, /^DraftKings statuses haven't been checked recently; the last successful check was 3 hours ago .*A late scratch could be missing/);
const dkFarOut = buildSlateCheck(base({ now: Date.parse("2026-09-26T20:00:00Z"),
  liveDk: { applied: true, reason: null, capturedAt: "2026-09-26T14:00:00Z", lastSuccessfulAt: "2026-09-26T14:00:00Z", lastPollOk: true },
  projectionAsOf: "2026-09-26T19:00:00Z", rosterCapturedAt: "2026-09-26T19:00:00Z" }));
assert.equal(line(dkFarOut, "live-dk")!.level, "info", "two days out, the 6-hour off-day cadence is expected");
assert.match(line(dkFarOut, "live-dk")!.text, /normal until game day/);
const dkFailed = buildSlateCheck(base({ now: Date.parse("2026-09-26T20:00:00Z"),
  liveDk: { applied: true, reason: null, capturedAt: "2026-09-26T19:40:00Z", lastSuccessfulAt: "2026-09-26T19:40:00Z", lastPollOk: false },
  projectionAsOf: "2026-09-26T19:00:00Z", rosterCapturedAt: "2026-09-26T19:00:00Z" }));
assert.equal(line(dkFailed, "live-dk")!.level, "attention", "a failed poll needs you even far from kickoff");
assert.match(line(dkFailed, "live-dk")!.text, /^The latest DraftKings status check failed; the last successful check was 20 minutes ago/);
assert.equal(line(clean, "live-dk")!.level, "ok");

// A capture read error is reported as an error, not as "not available yet".
const unreadable = buildSlateCheck(base({ opponentAdjustments: [{ label: "PFR efficiency", applied: 0, eligible: 56, error: "connection reset" }] }));
assert.match(line(unreadable, "opponent-error")!.text, /^Couldn't read opponent adjustments \(PFR efficiency: connection reset\)/);
assert.equal(line(unreadable, "opponent"), undefined);

// A partial LineStar import says it is a mix.
const mixed = buildSlateCheck(base({ ownership: { source: "linestar", errors: [], coverage: { linestar: 40, estimate: 16, total: 56 } } }));
assert.equal(line(mixed, "ownership")!.level, "attention");
assert.match(line(mixed, "ownership")!.text, /LineStar for 40 of 56 players and our rough estimate for the other 16/);
assert.equal(line(buildSlateCheck(base({ ownership: { source: "linestar", errors: [], coverage: { linestar: 56, estimate: 0, total: 56 } } })), "ownership")!.text, "Ownership from your LineStar import.");

// One out rule for quarterbacks: a DraftKings IR/PUP/SUSP/NA tag counts, a depth-chart block does not.
for (const tag of ["O", "OUT", "IR", "PUP", "SUSP", "NA"]) assert.equal(ruledOutPlayer({ dkStatus: tag }), true, tag);
assert.equal(ruledOutPlayer({ dkStatus: "Q" }), false);
assert.equal(ruledOutPlayer({ availability: { status: "ACTIVE", blockedReason: "Listed QB2; starter workload not supported" } }), false, "a listed backup is not out");
assert.equal(ruledOutPlayer({ projectionStatus: "out" }), true);
assert.equal(ruledOutPlayer({ platformOut: true }), true);
const irStarter = buildSlateCheck(base({ qbs: [qb("Jalen Hurts", "PHI", { role: "Expected starter · QB1" }),
  qb("Caleb Williams", "CHI", { injured: ruledOutPlayer({ dkStatus: "IR" }), role: "Expected starter · QB1" }), qb("Case Keenum", "CHI")] }));
assert.equal(line(irStarter, "qb:CHI")!.action, "pick_starter", "an IR-tagged starter prompts a pick");

// A check that couldn't be saved says so on the card, not only in the server log.
const unsaved = withRecordFailure(clean, new Error("deadlock detected"));
assert.equal(unsaved.items.at(-1)!.text, "This check couldn't be saved, so the slate list may still show an older one: deadlock detected");
assert.equal(unsaved.needs, clean.needs, "a note, not a new problem to fix");

console.log("Slate Check: clean slate is quiet; the PHI@CHI failures each become one plain line with a fix; overrides and local builds are flagged; stale projections, depth charts and DraftKings statuses, unreadable captures, mixed ownership and unsaved checks are said.");
