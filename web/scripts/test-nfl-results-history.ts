/**
 * Running totals across slates: repeats count once, rates keep their
 * denominators, and captains are grouped by the field's CPT ownership.
 */
import assert from "node:assert/strict";
import { builtAfterStart, captainBucket, distinctSets, planGroup, summarizeHistory, type HistorySet, type HistorySlate } from "../src/lib/nfl-dfs/results-history";

const lineup = (n: number, captain: string, actual: number | null, beatShare: number | null, captainFieldPct: number | null) =>
  ({ lineupNumber: n, captain, actual, projected: 80, missing: actual == null ? ["X"] : [], rank: null, beatShare, exactRank: false, captainFieldPct });
const set = (runId: string, createdAt: string, planKey: string, fingerprint: string, lineups: HistorySet["lineups"], uploadId = "thu"): HistorySet =>
  ({ uploadId, runId, createdAt, planKey, source: "our", mode: "gpp", fingerprint, lineups });

const thursday: HistorySlate = {
  uploadId: "thu", label: "ATL@GB", startsAt: "2026-09-25T00:15:00.000Z", format: "showdown", contestId: "1",
  entryCount: 83234, medianScore: 110, winningScore: 151.7,
  sets: [
    set("a", "2026-09-24T23:12:00Z", "balanced", "same", [lineup(1, "Bijan", 122, 0.76, 23.8), lineup(2, "Kraft", 95, 0.2, 9)]),
    set("b", "2026-09-24T23:34:00Z", "balanced", "same", [lineup(1, "Bijan", 122, 0.76, 23.8), lineup(2, "Kraft", 95, 0.2, 9)]),
    set("c", "2026-09-24T23:50:00Z", "chalk_leverage", "other", [lineup(1, "Love", 130, 0.9, 11.8), lineup(2, "Rookie", null, null, 2)]),
  ],
};
const sunday: HistorySlate = {
  uploadId: "sun", label: "Main", startsAt: "2026-09-20T17:00:00.000Z", format: "classic", contestId: "2",
  entryCount: 300000, medianScore: 112, winningScore: 243,
  sets: [set("d", "2026-09-20T15:00:00Z", "balanced", "x", [lineup(1, "", 130, 0.85, null), lineup(2, "", 100, 0.3, null)], "sun")],
};

// Repeats
const { kept, duplicates } = distinctSets(thursday.sets);
assert.equal(duplicates, 1, "the same lineups built twice are one decision");
assert.deepEqual(kept.map((s) => s.runId), ["c", "b"], "the newest copy is kept");

const history = summarizeHistory([thursday, sunday]);
assert.equal(history.duplicatesDropped, 1);
assert.equal(history.overall.slates, 2);
assert.equal(history.overall.sets, 3);
assert.equal(history.overall.lineups, 5, "an unknown score is not a lineup result");
assert.equal(history.overall.aboveMedian, 3, "122 and 130 beat 110; 130 beats 112");
assert.equal(history.overall.topFifth, 2);

// Plans: Showdown plans separate, Classic grouped under its format.
const plan = Object.fromEntries(history.byPlan.map((r) => [r.group, r]));
assert.equal(plan.balanced.lineups, 2); assert.equal(plan.balanced.aboveMedian, 1);
assert.equal(plan.chalk_leverage.lineups, 1); assert.equal(plan.Classic.lineups, 2);
assert.equal(plan.balanced.averageMargin, -1.5, "(12 + -15) / 2");
assert.equal(planGroup("classic", "chalk_leverage"), "Classic");

// Captains: Showdown only, by the field's CPT ownership.
const captains = Object.fromEntries(history.byCaptain.map((r) => [r.group, r.lineups]));
assert.equal(captains[captainBucket(23.8)], 1);
assert.equal(captains[captainBucket(11.8)], 2, "11.8% and 9% are both Middle");
assert.equal(captainBucket(9), captainBucket(11.8));
assert.equal(Object.values(captains).reduce((a, b) => a + b, 0), 3, "Classic lineups and unknown scores are left out");
assert.equal(captainBucket(20), "Chalk (20%+ CPT owned)");
assert.equal(captainBucket(7.99), "Contrarian (under 8%)");
assert.equal(captainBucket(null), "Unknown");

assert.equal(summarizeHistory([]).overall.averageMargin, null);
assert.equal(history.overall.ranked, 5, "every scored lineup here has a rank; the unknown score is not a lineup result");

// A set saved after kickoff is shown, never counted.
const late = set("e", "2026-09-23T21:51:00Z", "balanced", "hindsight", [lineup(1, "", 150, 0.99, null)], "sun");
assert.equal(builtAfterStart(late, sunday), true);
assert.equal(builtAfterStart(sunday.sets[0], sunday), false);
const withLate = summarizeHistory([{ ...sunday, sets: [...sunday.sets, late] }]);
assert.equal(withLate.builtAfterStart, 1);
assert.equal(withLate.overall.lineups, 2, "the hindsight set adds nothing");
assert.equal(builtAfterStart(late, { ...sunday, startsAt: null }), false, "an unknown start never hides a set");

// No score curve: no rank, so no top-20% claim either way.
const unranked = summarizeHistory([{ ...sunday, sets: [set("f", "2026-09-20T15:00:00Z", "balanced", "y", [lineup(1, "", 140, null, null)], "sun")] }]);
assert.equal(unranked.overall.ranked, 0); assert.equal(unranked.overall.topFifth, 0);

console.log("Results history: repeats count once, rates keep denominators, captains grouped by field CPT ownership.");
