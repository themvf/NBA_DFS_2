import assert from "node:assert/strict";
import source from "../src/data/nfl-player-context-2025.json";
import { contextPoints, historicalRange, participationLabel, type PlayerContext } from "../src/lib/nfl-dfs/player-context";
const data = source as unknown as PlayerContext;
assert.equal(data.audit.scheduledGames, 272);
assert.equal(Object.keys(data.games).length, 544);
const identities = new Set<string>();
for (const row of data.rows) {
  const identity = `${row.playerId}:${row.gameKey}`;
  assert.ok(!identities.has(identity)); identities.add(identity);
  const game = data.games[row.gameKey];
  assert.ok(game && game.roster.some(m => m.id === row.playerId));
  assert.ok(contextPoints(row) === null || Number.isFinite(contextPoints(row)));
}
for (const game of Object.values(data.games)) {
  assert.ok(game.covered <= game.plays);
  for (const member of game.roster) assert.ok(member.recordedPlays === null || member.recordedPlays <= game.covered);
}
const empty = { ...data.rows[0], stats: null };
assert.equal(contextPoints(empty), null);
assert.equal(historicalRange([empty]), null);
const scored = { ...empty, stats: { passYds: 300, passTds: 2, interceptions: 1, rushYds: 0, rushTds: 0, recYds: 0, recTds: 0, receptions: 0, fumblesLost: 1, returnTds: 0, offensiveFumbleRecoveryTds: 0, twoPointConversions: 0 } };
assert.equal(contextPoints(scored), 21);
assert.equal(historicalRange([scored, empty])?.n, 1);
const game = Object.values(data.games)[0];
const member = { ...game.roster[0], recordedPlays: 0 };
assert.match(participationLabel(member, { ...game, plays: 2, covered: 1 }), /incomplete/);
assert.match(participationLabel(member, { ...game, plays: 2, covered: 2 }), /No recorded/);
console.log(`Player context: ${data.rows.length} identities, scoring, missingness and participation coverage passed.`);
