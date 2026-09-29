/**
 * The Slate Check turns every silent failure from PHI@CHI 2026-09-28 into one
 * plain sentence, and says nothing alarming when all is well.
 */
import assert from "node:assert/strict";
import { buildSlateCheck, type SlateCheckInput, type SlateCheckQb } from "../src/lib/nfl-dfs/slate-check";

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

console.log("Slate Check: clean slate is quiet; the PHI@CHI failures each become one plain line with a fix; overrides and local builds are flagged.");
