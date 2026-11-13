import assert from "node:assert/strict";
import { CONTEST_RUN_VERSION, checkRunRequest, runPayload, runRequest, scoreRun, type RunRequest } from "../src/lib/nba-contest-runs";
import { RECAL_VERSION, SIT_VERSION, optimizeContest, type ContestPlayer } from "../src/lib/nba-contest-optimizer";
import { playerKey } from "../src/lib/nba-top-scorers";

// A real optimizer run, so the request is the shape the browser sends.
const names = ["Luka Doncic", "Stephen Curry", "Victor Wembanyama", "Cade Cunningham", "Cooper Flagg", "Nikola Jokic", "Devin Booker", "Jalen Brunson"];
const players: ContestPlayer[] = names.map((player, i) => ({
  key: playerKey(player), player, eventId: i < 4 ? "evt-a" : "evt-b", team: i % 2 ? "hometeam" : "awayteam",
  line: 34.5 - i * 1.5, pOver: 0.5, books: 5, curve: [],
}));
const run = optimizeContest(players, { fieldSize: 300, entries: 4, sims: 400 });
const request = runRequest("2026-11-10", ["evt-b", "evt-a"], 4, "2026-11-10T22:07:00.000Z", RECAL_VERSION, run);
assert.deepEqual(request.eventIds, ["evt-a", "evt-b"]);            // sorted, so the same choice hashes the same
assert.equal(request.entries.length, 4);
assert.ok(!("curve" in request.pool[0]));                           // the ladder stays in the browser

const ok = checkRunRequest(JSON.parse(JSON.stringify(request)));
assert.ok(ok.ok);
if (ok.ok) assert.deepEqual(ok.value, request);                      // a clean request survives the check unchanged

const refused = (mutate: (r: RunRequest) => void, reason: RegExp) => {
  const r = JSON.parse(JSON.stringify(request)) as RunRequest;
  mutate(r);
  const out = checkRunRequest(r);
  assert.ok(!out.ok, `expected a refusal matching ${reason}`);
  if (!out.ok) assert.match(out.error, reason);
};
refused((r) => { r.date = "11/10/2026"; }, /date/);
refused((r) => { r.eventIds = []; }, /games/);
refused((r) => { r.fieldSize = 5; }, /field size/);
refused((r) => { r.pool[0].eventId = "evt-z"; }, /not in a chosen game/);
refused((r) => { r.pool[0].key = "someoneelse"; }, /key does not match/);
refused((r) => { r.entries[0].players[0] = "nobody"; }, /not in the pool/);
refused((r) => { r.entries[0].players[1] = r.entries[0].players[0]; }, /three different players/);
refused((r) => { r.entries[1].players = [...r.entries[0].players].reverse(); }, /same trio twice/);
refused((r) => { r.entries[1].rank = 5; }, /rank out of order/);
refused((r) => { r.entriesRequested = 2; }, /more entries than requested/);
refused((r) => { r.linesCapturedAt = "yesterday"; }, /capture time/);
refused((r) => { r.pool[0].sit = 1.4; }, /pool player/);
refused((r) => { (r as { sitVersion: unknown }).sitVersion = 7; }, /sitting rule/);

// The sitting rule and each player's simulated chance are on the record. Without lead times the run used the book-count rates.
assert.equal(request.sitVersion, null);
assert.ok(request.pool.every((p) => p.sit === 0.002));
const timedRun = optimizeContest(players.map((p, i) => ({ ...p, leadHours: i < 4 ? 0.5 : 3.0 })), { fieldSize: 300, entries: 4, sims: 400 });
const timedRequest = runRequest("2026-11-10", ["evt-a", "evt-b"], 4, "2026-11-10T22:07:00.000Z", RECAL_VERSION, timedRun);
assert.equal(timedRequest.sitVersion, SIT_VERSION);
assert.deepEqual([...new Set(timedRequest.pool.map((p) => p.sit))].sort(), [0.002, 0.021]);
const timedOk = checkRunRequest(JSON.parse(JSON.stringify(timedRequest)));
assert.ok(timedOk.ok);
if (timedOk.ok) assert.deepEqual(timedOk.value, timedRequest);
// A request from a page loaded before this change has neither field: still accepted, recorded as the book-count rule.
const legacy = JSON.parse(JSON.stringify(request)) as Record<string, unknown> & { pool: Record<string, unknown>[] };
delete legacy.sitVersion;
for (const p of legacy.pool) delete p.sit;
const legacyOk = checkRunRequest(legacy);
assert.ok(legacyOk.ok);
if (legacyOk.ok) { assert.equal(legacyOk.value.sitVersion, null); assert.ok(!("sit" in legacyOk.value.pool[0])); }
assert.ok(!checkRunRequest(null).ok);
assert.ok(!checkRunRequest([request]).ok);

const games = [
  { eventId: "evt-b", homeTeam: "Golden State Warriors", awayTeam: "New Orleans Pelicans", commenceTime: "2026-11-11T03:10:00.000Z" },
  { eventId: "evt-a", homeTeam: "Atlanta Hawks", awayTeam: "Los Angeles Lakers", commenceTime: "2026-11-11T00:10:00.000Z" },
];
const payload = runPayload(request, games);
assert.equal(payload.version, CONTEST_RUN_VERSION);
assert.equal(payload.sitVersion, null);
assert.equal(runPayload(timedRequest, games).sitVersion, SIT_VERSION);
assert.equal(payload.gameDate, "2026-11-10");
assert.deepEqual(payload.games.map((g) => g.eventId), ["evt-a", "evt-b"]);
assert.ok(!("eventIds" in payload) && !("date" in payload));
assert.equal(JSON.stringify(runPayload(request, [...games].reverse())), JSON.stringify(payload));   // game order cannot change the digest

// Scoring follows the contest grade. `skip` drops a player's box row; `bench` lists him as not played.
const box = (home: string, away: string, keys: string[], skip: string[] = [], bench: string[] = []) => ({
  homeTeam: home, awayTeam: away,
  players: keys.filter((k) => !skip.includes(k)).map((k, i) => ({ playerKey: k, played: !bench.includes(k), points: bench.includes(k) ? null : 20 + i })),
});
const used = new Set(payload.entries.flatMap((e) => e.players));
const aKeys = players.slice(0, 4).map((p) => p.key), bKeys = players.slice(4).map((p) => p.key);
const hawks = ["Atlanta Hawks", "Los Angeles Lakers"] as const, warriors = ["Golden State Warriors", "New Orleans Pelicans"] as const;

// Only game A is final: an entry with a game-B player is unknown, never scored as 0.
const partial = scoreRun(payload, [box(...hawks, aKeys)]);
for (const [i, e] of payload.entries.entries()) {
  assert.equal(partial.entries[i].total === null, e.players.some((k) => bKeys.includes(k)));
}
assert.equal(partial.graded, payload.entries.every((e) => e.players.every((k) => aKeys.includes(k))));

// Both final. One used player has no row in his finished game and another sat: both score 0 and are flagged.
const usedA = aKeys.filter((k) => used.has(k)), usedB = bKeys.filter((k) => used.has(k));
assert.ok(usedA.length > 0, "the fixture run should use a game-A player");
const missing = usedA[0], sat = usedB[0] ?? null;
const done = scoreRun(payload, [box(...hawks, aKeys, [missing]), box(...warriors, bKeys, [], sat ? [sat] : [])]);
assert.ok(done.graded);
for (const [i, e] of payload.entries.entries()) {
  assert.deepEqual(done.entries[i].players.map((r) => r.key), e.players);
  assert.equal(done.entries[i].total, done.entries[i].players.reduce((a, r) => a + (r.points ?? 0), 0));
  for (const r of done.entries[i].players) {
    if (r.key === missing || r.key === sat) assert.deepEqual({ points: r.points, dnp: r.dnp }, { points: 0, dnp: true });
    else assert.ok((r.points ?? 0) >= 20 && !r.dnp);
  }
}
assert.equal(done.best, Math.max(...done.entries.map((e) => e.total ?? 0)));
assert.equal(done.baselineBest, Math.max(...done.baseline.map((e) => e.total ?? 0)));

// Two box scores for one matchup, or a name listed twice in his game, is unknown rather than a guess.
const twice = scoreRun(payload, [box(...hawks, aKeys), box(...hawks, aKeys), box(...warriors, bKeys)]);
assert.ok(twice.entries.flatMap((e) => e.players).filter((r) => aKeys.includes(r.key)).every((r) => r.points === null));
const dupName = scoreRun(payload, [box(...hawks, [...aKeys, missing]), box(...warriors, bKeys)]);
assert.ok(dupName.entries.flatMap((e) => e.players).filter((r) => r.key === missing).every((r) => r.points === null));
assert.ok(!dupName.graded);

// The odds feed and ESPN can spell a team differently (LA Clippers); teamKey bridges it.
const clippers = runPayload(request, [{ ...games[1], homeTeam: "Los Angeles Clippers" }, games[0]]);
assert.ok(scoreRun(clippers, [box("LA Clippers", "Los Angeles Lakers", aKeys), box(...warriors, bKeys)]).graded);

console.log("test-nba-contest-runs: all assertions passed");
