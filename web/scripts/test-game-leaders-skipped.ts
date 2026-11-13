import assert from "node:assert/strict";
import { skippedNotices } from "../src/lib/nfl/game-leaders-skipped";

// Tagged entries (written since 2026-11-13) are read as tagged.
const tagged = skippedNotices([
  { game_id: "2026_10_LA_ARI", reason: "Unresolved recent yardage events for LAR: ['2026_09_LA_WAS']", kind: "data_error",
    away: "LAR", home: "ARI", kickoff: "2026-11-15T21:05:00+00:00" },
  { game_id: "2026_10_WAS_NYG", reason: "started before any forecast was saved", kind: "expected", away: "WAS", home: "NYG" },
]);
assert.deepEqual(tagged.map((n) => [n.matchup, n.kind]), [["LAR at ARI", "data_error"], ["WAS at NYG", "expected"]]);
assert.equal(tagged[0].kickoff, "2026-11-15T21:05:00+00:00");
assert.equal(tagged[1].kickoff, null);

// Older pages: kind and teams come from the reason and the game id; unknown reasons are errors.
const old = skippedNotices([
  { game_id: "2026_10_WAS_NYG", reason: "started before any forecast was saved" },
  { game_id: "2026_10_LA_ARI", reason: "something nobody has seen yet" },
]);
assert.deepEqual(old.map((n) => [n.matchup, n.kind]), [["WAS at NYG", "expected"], ["LA at ARI", "data_error"]]);

// A game kept on an older forecast says so, and its reason drops the prefix.
const kept = skippedNotices([{ game_id: "2026_10_BUF_MIA", reason: "kept previous forecast: unassigned share above 5%" }])[0];
assert.equal(kept.olderForecast, true);
assert.equal(kept.reason, "unassigned share above 5%");
assert.equal(kept.kind, "data_error");

assert.deepEqual(skippedNotices(undefined), []);
console.log("game-leaders-skipped: ok");
