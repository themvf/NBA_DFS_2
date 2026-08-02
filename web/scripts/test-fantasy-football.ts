import assert from "node:assert/strict";
import { queryRows } from "../src/db/query-result";
import { buildSnakeSlots, nextControlledPick, picksUntilControlled } from "../src/lib/fantasy-football/draft-engine";
import { recommendPlayers } from "../src/lib/fantasy-football/recommendations";
import { fantasyBadgeClass } from "../src/lib/fantasy-football/badge-style";
import { filterFantasyRankings } from "../src/lib/fantasy-football/ranking-filters";
import type { FantasyRankingRow } from "../src/db/queries-fantasy-football";
import { buildProjectionExplanation } from "../src/lib/fantasy-football/projection-explanation";

assert.deepEqual(queryRows<{ id: number }>({ rows: [{ id: 3 }] }), [{ id: 3 }]);
assert.deepEqual(queryRows<{ id: number }>([{ id: 2 }]), [{ id: 2 }]);
assert.deepEqual(queryRows<{ id: number }>({}), []);

const slots = buildSnakeSlots(4, 3);
assert.deepEqual(slots.map((slot) => slot.teamSlot), [1, 2, 3, 4, 4, 3, 2, 1, 1, 2, 3, 4]);
assert.equal(nextControlledPick(3, 2, 4, 3), 7);
assert.equal(picksUntilControlled(3, 2, 4, 3), 4);

const recommendations = recommendPlayers([
  { playerId: 1, position: "RB", ourRank: 12, ecr: 15, adp: 25, projectedPoints: 250, tier: 2, confidence: 0.8 },
  { playerId: 2, position: "QB", ourRank: 8, ecr: 8, adp: 8, projectedPoints: 330, tier: 1, confidence: 0.9 },
], ["QB"], 10);
assert.equal(recommendations[0].playerId, 1);
assert.equal(recommendations[0].adpDelta, 13);
assert.match(fantasyBadgeClass({ code: "NFL_TOP_10_TARGETS", class: "fact" }), /blue/);
assert.match(fantasyBadgeClass({ code: "NFL_TOP_10_RUSH_TDS", class: "fact" }), /orange/);
assert.match(fantasyBadgeClass({ code: "TEAM_TARGET_LEADER", class: "fact" }), /cyan/);
assert.match(fantasyBadgeClass({ code: "INJURY", class: "risk" }), /red/);
assert.match(fantasyBadgeClass({ code: "TOP_3_POSITION_POINTS", class: "fact" }), /yellow/);
const filterRows = [
  { playerId: 1, name: "A.J. Brown", position: "WR", team: "NE" },
  { playerId: 2, name: "Bijan Robinson", position: "RB", team: "ATL" },
] as FantasyRankingRow[];
assert.deepEqual(filterFantasyRankings(filterRows, { name: "brown", position: "WR", team: "NE" }).map((row) => row.playerId), [1]);
assert.equal(filterFantasyRankings(filterRows, { name: "", position: "RB", team: "" }).length, 1);
const explanation = buildProjectionExplanation({
  method: "history_regression",
  season_inputs: [{ season: 2025, ppg: 20.61, weight: 0.55 }],
  weighted_history_ppg: 17.79,
  position_prior_ppg: 14,
  regression_prior_games: 4,
  regressed_ppg: 17.06,
  expected_games_before_injury: 15.9,
  role_factor: 1,
  base_points_before_injury: 271.25,
  injury_factor: 1,
  final_points: 271.25,
  not_modeled: ["current teammates", "future schedule"],
});
assert.equal(explanation.method, "Weighted history + regression");
assert.ok(explanation.lines.includes("2025: 20.6 PPG × 55%"));
assert.ok(explanation.lines.includes("17.06 PPG × 15.90 games × 1.00 role = 271.3"));
assert.deepEqual(explanation.notModeled, ["current teammates", "future schedule"]);
console.log("fantasy-football tests passed");
