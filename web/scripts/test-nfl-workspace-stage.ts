/**
 * The workspace's sense of time, and the post-game results math.
 */
import assert from "node:assert/strict";
import {
  isLocked, parseDkGameInfoKickoff, prioritizeStatus, recommendedStage, type StatusItem,
} from "../src/lib/nfl-dfs/workspace-stage";
import { buildScoreCurve, estimateRank, projectionError, scoreLineups, summarizeSet } from "../src/lib/nfl-dfs/slate-results";

const KICKOFF = "2026-09-25T00:15:00.000Z"; // ATL@GB, 8:15pm ET Thursday
const before = Date.parse("2026-09-24T18:00:00Z");
const after = Date.parse("2026-09-25T13:00:00Z");

// ── Stage ───────────────────────────────────────────────────────────────────
assert.equal(recommendedStage({ hasSlate: false, lineupCount: 0, firstKickoff: null, now: before }), "slate");
assert.equal(recommendedStage({ hasSlate: true, lineupCount: 0, firstKickoff: KICKOFF, now: before }), "build");
assert.equal(recommendedStage({ hasSlate: true, lineupCount: 20, firstKickoff: KICKOFF, now: before }), "review");
// The morning after: nothing can be built or entered, so open on Results --
// even with lineups loaded. This is the case the old page got wrong.
assert.equal(recommendedStage({ hasSlate: true, lineupCount: 20, firstKickoff: KICKOFF, now: after }), "results");
assert.equal(recommendedStage({ hasSlate: true, lineupCount: 0, firstKickoff: null, now: after }), "build",
  "an unknown kickoff never claims the slate is over");
assert.equal(isLocked(KICKOFF, Date.parse(KICKOFF)), true, "locked at kickoff, not a minute after");

// ── Status line ─────────────────────────────────────────────────────────────
const items: StatusItem[] = [
  { id: "refresh", tone: "warning", text: "Newer projection run", preLockOnly: true },
  { id: "live", tone: "info", text: "DraftKings status unchanged", preLockOnly: true },
  { id: "blind", tone: "danger", text: "We do not know who is playing" },
  { id: "warn", tone: "warning", text: "Roster evidence is stale" },
];
assert.deepEqual(prioritizeStatus(items, false).map((i) => i.id), ["blind", "refresh", "warn", "live"],
  "most urgent first, stable within a tone");
assert.deepEqual(prioritizeStatus(items, true).map((i) => i.id), ["blind", "warn"],
  "after kickoff, a newer projection run and a live status stop mattering");

// ── DraftKings Game Info kickoff ────────────────────────────────────────────
assert.equal(parseDkGameInfoKickoff("ATL@GB 09/24/2026 08:15PM ET"), KICKOFF, "daylight time: ET is UTC-4");
assert.equal(parseDkGameInfoKickoff("KC@BUF 12/06/2026 01:00PM ET"), "2026-12-06T18:00:00.000Z", "standard time: UTC-5");
assert.equal(parseDkGameInfoKickoff("NYJ@NE 10/04/2026 12:00AM ET"), "2026-10-04T04:00:00.000Z", "12AM is midnight");
assert.equal(parseDkGameInfoKickoff("no date here"), null);

// ── Score curve and rank ────────────────────────────────────────────────────
const scores = Array.from({ length: 1000 }, (_, i) => 200 - i * 0.1); // 200.0, 199.9, ... 100.1
const curve = buildScoreCurve(scores);
assert.equal(curve[0][0], 1); assert.equal(curve[0][1], 200);
assert.equal(curve[curve.length - 1][0], 1000, "the last entry is always on the curve");
assert.ok(curve.length < 400, "compact: exact top 100, then every half-percent");

assert.deepEqual(estimateRank(250, curve, 1000), { rank: 1, beatShare: 1, exact: true }, "above the winner is first");
const tenth = estimateRank(199.1, curve, 1000)!;   // 9 entries (200.0..199.2) scored more
assert.equal(tenth.rank, 10); assert.equal(tenth.exact, true, "the top 100 is exact");
const mid = estimateRank(150.05, curve, 1000)!;     // 499 entries scored more (200.0..150.2 = 499 values... incl. 150.1)
assert.ok(Math.abs(mid.rank - 500) <= 3, `interpolated rank near 500, got ${mid.rank}`);
assert.equal(mid.exact, false, "outside the top 100 it is an estimate, and says so");
assert.equal(estimateRank(50, curve, 1000)!.rank, 1001, "below every entry");
assert.equal(estimateRank(150, [], 1000), null, "no curve, no rank");

// ── Lineup scoring ──────────────────────────────────────────────────────────
const norm = (s: string) => s.toLowerCase().replace(/[^a-z]/g, "");
const fpts = new Map([["bijanrobinson", 30], ["christianwatson", 20], ["jordanlove", 18]]);
const [scored, incomplete] = scoreLineups([
  { lineupNumber: 1, slots: [
    { slot: "CPT", name: "Bijan Robinson", multiplier: 1.5, projection: 31.2 },
    { slot: "FLEX1", name: "Christian Watson", multiplier: 1, projection: 17.4 },
    { slot: "FLEX2", name: "Jordan Love", multiplier: 1, projection: 16.5 }] },
  { lineupNumber: 2, slots: [
    { slot: "CPT", name: "Jordan Love", multiplier: 1.5, projection: 24.75 },
    { slot: "FLEX1", name: "Nobody Drafted", multiplier: 1, projection: 3 }] },
], fpts, norm);
assert.equal(scored.actual, 30 * 1.5 + 20 + 18, "captain paid at 1.5x");
assert.equal(scored.captain, "Bijan Robinson");
assert.equal(scored.projected, 65.1);
assert.equal(incomplete.actual, null, "an unknown player leaves the score unknown, never zero");
assert.deepEqual(incomplete.missing, ["Nobody Drafted"]);

// ── Projection error ────────────────────────────────────────────────────────
const errors = projectionError([
  { name: "Bijan Robinson", position: "RB", ourProj: 20, isOut: false },
  { name: "Christian Watson", position: "WR", ourProj: 25, isOut: false },
  { name: "Jordan Love", position: "QB", ourProj: 16, isOut: false },
  { name: "Josh Jacobs", position: "RB", ourProj: 0, isOut: true },
  { name: "No Model", position: "WR", ourProj: null, isOut: false },
], fpts, norm);
const rb = errors.find((e) => e.position === "RB")!;
assert.deepEqual(rb, { position: "RB", n: 1, mae: 10, bias: 10 }, "ruled-out players are not graded");
assert.deepEqual(errors.find((e) => e.position === "WR"), { position: "WR", n: 1, mae: 5, bias: -5 },
  "negative bias = projected too high");
assert.equal(errors[errors.length - 1].position, "All");
assert.equal(errors[errors.length - 1].n, 3);

// ── Set comparison ──────────────────────────────────────────────────────────
const ranked = (lineupNumber: number, captain: string, actual: number | null, beatShare: number | null) =>
  ({ lineupNumber, captain, actual, projected: 80, missing: actual == null ? ["X"] : [], rank: null, beatShare, exactRank: false });
const set = summarizeSet([
  ranked(1, "Bijan Robinson", 122.4, 0.76), ranked(2, "Tucker Kraft", 94.2, 0.18),
  ranked(3, "Tucker Kraft", 112.8, 0.55), ranked(4, "Drake London", null, null),
], 109.94);
assert.equal(set.lineups, 4); assert.equal(set.scored, 3, "an unknown score is counted, not scored");
assert.equal(set.aboveMedian, 2); assert.equal(set.topFifth, 0, "76% is not the top fifth");
assert.equal(set.best?.lineupNumber, 1);
assert.deepEqual(set.captains.map((c) => [c.captain, c.lineups, c.average, c.aboveMedian]),
  [["Tucker Kraft", 2, 103.5, 1], ["Bijan Robinson", 1, 122.4, 1]], "most-used captain first; unknown scores excluded");
assert.equal(summarizeSet([], null).averageActual, null);

console.log("Workspace stage and results:");
console.log("  - opens on the step the slate is in; Results after kickoff");
console.log("  - one prioritized status line; pre-lock notices drop after kickoff");
console.log("  - ET kickoffs resolve daylight and standard time");
console.log("  - rank exact in the top 100, interpolated and labelled below");
console.log("  - captain at 1.5x; unknown players leave a score unknown, not zero");
