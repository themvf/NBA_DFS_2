import assert from "node:assert/strict";
import { recalibrateSurvival, rng, simulatePoints, type ContestPlayer } from "../src/lib/nba-contest-optimizer";
import { fairAmerican, gameLeaders, gameTag, leaderChances, teamRecords, teamTag } from "../src/lib/nba-game-leaders";

const player = (key: string, line: number, eventId = "e1", team: string | null = "a", books = 6): ContestPlayer => ({
  key, player: key.toUpperCase(), eventId, team, line, pOver: 0.5, books,
  curve: [{ point: line - 8.5, p: 0.9 }, { point: line, p: 0.5 }, { point: line + 7.5, p: 0.15 }, { point: line + 12.5, p: 0.05 }],
});

// Chances in a game sum to one, ties split.
const pool = [player("star", 32.5), player("two", 21.5, "e1", "b"), player("three", 17.5), player("four", 12.5, "e1", "b")];
const both = leaderChances(pool, 8000, 5);
const c = both.game;
assert.ok(Math.abs(c.reduce((a, v) => a + v, 0) - 1) < 1e-9, "chances sum to 1");
assert.ok(c[0] > 0.5 && c[0] > c[1] && c[1] > c[2] && c[2] > c[3], `ordered by line: ${c}`);

// Team shares: each team's shares sum to one, and a team leader is at least as likely as a game leader.
assert.ok(Math.abs((both.team[0]! + both.team[2]!) - 1) < 1e-9, "team a sums to 1");
assert.ok(Math.abs((both.team[1]! + both.team[3]!) - 1) < 1e-9, "team b sums to 1");
for (let i = 0; i < 4; i++) assert.ok(both.team[i]! >= c[i] - 1e-12, `team share >= game share for ${i}`);
const noTeam = leaderChances([player("solo", 20.5, "e1", null), player("other", 18.5)], 2000, 1);
assert.equal(noTeam.team[0], null);
assert.equal(noTeam.team[1], 1);

// The correction moves chance from the longshots to the favourite.
const lead = (pts: Float64Array, n: number, sims: number, i: number) => {
  let w = 0;
  for (let s = 0; s < sims; s++) {
    let top = -1, cnt = 0;
    for (let j = 0; j < n; j++) { const v = pts[s * n + j]; if (v > top) { top = v; cnt = 1; } else if (v === top) cnt++; }
    if (pts[s * n + i] === top) w += 1 / cnt;
  }
  return w / sims;
};
const plain = lead(simulatePoints(pool, 8000, rng(5), false), 4, 8000, 0);
assert.ok(c[0] > plain + 0.02, `favourite higher after the correction: ${c[0]} vs ${plain}`);

// The correction reads a top-end draw lower on the curve and leaves the ends fixed.
assert.equal(recalibrateSurvival(0), 0);
assert.equal(recalibrateSurvival(1), 1);
assert.ok(recalibrateSurvival(0.003) > 0.003);

// Grouped by game, most likely first; a game with no quoted players is empty.
const games = gameLeaders([...pool, player("x", 25.5, "e2"), player("y", 20.5, "e2", "b")],
  [{ eventId: "e1", game: "A @ B", commenceTime: "2026-11-07T00:00:00Z" },
   { eventId: "e2", game: "C @ D", commenceTime: "2026-11-07T01:00:00Z" },
   { eventId: "e3", game: "E @ F", commenceTime: "2026-11-07T02:00:00Z" }], 4000);
assert.equal(games[0].players[0].key, "star");
assert.deepEqual(games[1].players.map((p) => p.key), ["x", "y"]);
assert.equal(games[2].players.length, 0);
// deterministic
assert.deepEqual(gameLeaders(pool, [{ eventId: "e1", game: "A @ B", commenceTime: "t" }], 2000),
  gameLeaders(pool, [{ eventId: "e1", game: "A @ B", commenceTime: "t" }], 2000));

// Season records by team and by the player who held the team's biggest line.
const recs = teamRecords([
  { team: "LA Clippers", player: "A", playerKey: "a", credit: 1 },
  { team: "LA Clippers", player: "A", playerKey: "a", credit: 0.5 },
  { team: "LA Clippers", player: "B", playerKey: "b", credit: 0 },
  { team: "Utah Jazz", player: "C", playerKey: "c", credit: 0 },
], (t) => t.toLowerCase().replace(/[^a-z]/g, ""));
assert.deepEqual(recs.laclippers, { games: 3, led: 1.5, players: { a: { player: "A", games: 2, led: 1.5 }, b: { player: "B", games: 1, led: 0 } } });
assert.equal(recs.utahjazz.led, 0);
assert.ok(games[0].players.every((p) => p.teamChance != null));

// Tags.
const lc = (key: string, chance: number, team: string | null, teamChance: number | null) =>
  ({ key, player: key.toUpperCase(), team, line: 20, books: 6, chance, teamChance });
assert.equal(gameTag([lc("a", 0.29, "x", 0.5)]), "wide_open");
assert.equal(gameTag([lc("a", 0.3, "x", 0.5)]), null);
assert.equal(gameTag([lc("a", 0.5, "x", 0.5)]), "clear_favourite");
assert.equal(gameTag([]), null);
const team = [lc("a", 0.3, "x", 0.44), lc("b", 0.2, "x", 0.31), lc("c", 0.1, "x", 0.25), lc("d", 0.4, "y", 0.9)];
assert.deepEqual(teamTag(team[0], team), { kind: "toss_up", rival: team[1] });
assert.deepEqual(teamTag(team[3], team), { kind: "clear" });
assert.equal(teamTag(lc("e", 0.3, "x", 0.5), team), null);
assert.equal(teamTag(lc("f", 0.3, null, null), team), null);
assert.deepEqual(teamTag(lc("g", 0.3, "z", 0.4), team), { kind: "toss_up", rival: null });

assert.equal(fairAmerican(0.5), "-100");
assert.equal(fairAmerican(0.75), "-300");
assert.equal(fairAmerican(0.2), "+400");
assert.equal(fairAmerican(0), "-");

console.log("test-nba-game-leaders: all assertions passed");
