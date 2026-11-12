/**
 * Showdown form defaults and pre-build notices (nfl-showdown-defaults-v1):
 * the starting Max exposure, the low-cap warning, and the wait-for-inactives notice.
 */
import assert from "node:assert/strict";
import { DEFAULT_MAX_EXPOSURE, INACTIVE_RISK_MIN_POINTS, INACTIVES_POSTED_MINUTES, LOW_EXPOSURE_NOTICE_AT, SHOWDOWN_DEFAULTS_VERSION, SHOWDOWN_WINNERS_MAX_EXPOSURE,
  defaultMaxExposure, inactivesNotice, lowExposureForOwnershipFloor, type InactivesNoticePlayer } from "../src/lib/nfl-dfs/showdown-defaults";

assert.equal(SHOWDOWN_DEFAULTS_VERSION, "nfl-showdown-defaults-v1");
assert.equal(DEFAULT_MAX_EXPOSURE, 0.6, "the form's own starting value is unchanged");
assert.equal(SHOWDOWN_WINNERS_MAX_EXPOSURE, 0.9);

// --- Starting Max exposure: 90% only for a Winners Showdown tournament build ---
assert.equal(defaultMaxExposure({ format: "showdown", mode: "gpp", buildShape: "winners" }), 0.9);
assert.equal(defaultMaxExposure({ format: "showdown", mode: "gpp", buildShape: "standard" }), 0.6, "no ownership floor without the Winners shape");
assert.equal(defaultMaxExposure({ format: "showdown", mode: "cash", buildShape: "winners" }), 0.6, "the shape applies to GPP only");
assert.equal(defaultMaxExposure({ format: "classic", mode: "gpp", buildShape: "winners" }), 0.6, "Classic is untouched");
assert.equal(defaultMaxExposure({ format: "classic", mode: "gpp", buildShape: "diversified" }), 0.6);

// --- Low-cap warning: only where the ownership floor is in force, only at the caps that stopped short ---
const sd = { format: "showdown" as const, mode: "gpp" as const, buildShape: "winners" };
assert.equal(lowExposureForOwnershipFloor({ ...sd, maxExposure: 0.5 }), true);
assert.equal(lowExposureForOwnershipFloor({ ...sd, maxExposure: LOW_EXPOSURE_NOTICE_AT }), true, "60% stopped short too");
assert.equal(lowExposureForOwnershipFloor({ ...sd, maxExposure: 0.61 }), false, "no evidence between 60% and 90%, so no warning");
assert.equal(lowExposureForOwnershipFloor({ ...sd, maxExposure: 0.9 }), false);
assert.equal(lowExposureForOwnershipFloor({ ...sd, buildShape: "standard", maxExposure: 0.5 }), false);
assert.equal(lowExposureForOwnershipFloor({ ...sd, mode: "cash", maxExposure: 0.5 }), false);
assert.equal(lowExposureForOwnershipFloor({ format: "classic", mode: "gpp", buildShape: "winners", maxExposure: 0.5 }), false);

// --- Inactives notice ---
const kickoff = "2026-11-13T01:15:00Z";                                   // Thursday 8:15 PM ET
const at = (minutesBeforeKickoff: number) => Date.parse(kickoff) - minutesBeforeKickoff * 60_000;
const p = (over: Partial<InactivesNoticePlayer> & { dkPlayerId: number }): InactivesNoticePlayer =>
  ({ name: `P${over.dkPlayerId}`, position: "WR", team: "AAA", isOut: false, ourProj: 10, dkStatus: null, availabilityStatus: null, ...over });
const players = [
  p({ dkPlayerId: 1, dkStatus: "Q", ourProj: 25.3, name: "Star" }),
  p({ dkPlayerId: 2, dkStatus: "q ", ourProj: 8, name: "Second" }),              // spelling and case are normalized
  p({ dkPlayerId: 3, availabilityStatus: "Questionable", ourProj: 12, name: "Feed" }), // our availability feed counts as well
  p({ dkPlayerId: 4, dkStatus: "Q", ourProj: INACTIVE_RISK_MIN_POINTS - 0.1 }),   // too small a number to matter
  p({ dkPlayerId: 5, dkStatus: "Q", ourProj: 14, isOut: true }),                   // already out of the pool
  p({ dkPlayerId: 6, dkStatus: "D", ourProj: 9 }),                                  // Doubtful is left out by its own rule
  p({ dkPlayerId: 7, ourProj: 30 }),                                                // healthy
  p({ dkPlayerId: 8, dkStatus: "GTD", ourProj: 6, name: "Late" }),
];
const early = inactivesNotice("showdown", kickoff, at(300), players)!;
assert.ok(early, "five hours out: the notice shows");
assert.equal(early.postedAt, at(INACTIVES_POSTED_MINUTES), "inactives are due 90 minutes before kickoff");
assert.equal(early.minutesUntilPosted, 210);
assert.deepEqual(early.questionable.map(q => q.name), ["Star", "Feed", "Second", "Late"], "questionable players with a real number, largest first");
assert.equal(early.questionable[0].points, 25.3);
assert.ok(inactivesNotice("showdown", kickoff, at(91), players), "one minute before they are due");
assert.equal(inactivesNotice("showdown", kickoff, at(90), players), null, "at the time they are due the notice stops");
assert.equal(inactivesNotice("showdown", kickoff, at(10), players), null);
assert.equal(inactivesNotice("showdown", kickoff, at(-5), players), null, "after kickoff");
assert.equal(inactivesNotice("classic", kickoff, at(300), players), null, "Showdown only");
assert.equal(inactivesNotice("showdown", null, at(300), players), null, "no kickoff time: nothing is claimed");
assert.equal(inactivesNotice("showdown", "not a time", at(300), players), null);
const clean = inactivesNotice("showdown", kickoff, at(300), [p({ dkPlayerId: 9 })])!;
assert.deepEqual(clean.questionable, [], "nobody tagged: the notice still shows, with an empty list");

console.log("Showdown defaults: 90% starting Max exposure for a Winners Showdown GPP build only, a warning at 50-60%, and a wait-for-inactives notice until 90 minutes before kickoff.");
