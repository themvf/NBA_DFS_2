import assert from "node:assert/strict";
import { buildQbMatchupContexts, opponentAdjustedPassEffect } from "../src/lib/nfl-dfs/qb-matchup-context.ts";

const make = (gameId, offense, defense, epa, index, driveArchetype = "FIELD_GOAL") => ({
  gameId, posteam: offense, defteam: defense, playType: "pass", qbDropback: true,
  scoreDifferential: 0, epa, drive: Math.floor(index / 10) + 1, driveArchetype,
});
const plays = [
  ...Array.from({ length: 35 }, (_, i) => make("g1", "A", "D", .2, i, i < 10 ? "TOUCHDOWN" : "FIELD_GOAL")),
  ...Array.from({ length: 35 }, (_, i) => make("g2", "B", "D", .1, i)),
  ...Array.from({ length: 35 }, (_, i) => make("g3", "A", "E", 0, i)),
  ...Array.from({ length: 35 }, (_, i) => make("g4", "B", "F", 0, i)),
];
const effect = opponentAdjustedPassEffect(plays, "D");
assert.equal(effect.dropbacks, 70);
assert.equal(effect.games, 2);
assert.ok(Math.abs(effect.effect - .15) < 1e-10);
assert.equal(opponentAdjustedPassEffect(plays.slice(0, 35), "D").effect, null);

const market = { gameId: "upcoming", home: "D", away: "A", kickoff: "2026-10-04T17:00:00Z",
  oddsId: 123, oddsCapturedAt: "2026-10-03T17:00:00Z", bookmakerCount: 6,
  homeSpread: -3, total: 47 };
const contexts = buildQbMatchupContexts(plays, [market], "2026-10-03T18:00:00Z");
assert.equal(contexts.find(row => row.team === "D").impliedPoints, 25);
assert.equal(contexts.find(row => row.team === "A").impliedPoints, 22);
assert.equal(contexts.find(row => row.team === "A").teamSpread, 3);
assert.equal(contexts.find(row => row.team === "A").opponentTouchdownDrives, 1);
assert.equal(contexts.find(row => row.team === "A").opponentCompetitiveDrives, 8);
assert.equal(buildQbMatchupContexts(plays, [{ ...market, oddsId: null }], "2026-10-03T18:00:00Z")[0].status, "unavailable");
console.log("QB matchup calculation checks passed");
