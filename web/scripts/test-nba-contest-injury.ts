import assert from "node:assert/strict";
import { injuryTeamKey, playerInjuries, type InjuryReport } from "../src/lib/nba-contest-injury";
import { SIT_INJURY_VERSION, SIT_VERSION, optimizeContest, sitProb, type ContestPlayer } from "../src/lib/nba-contest-optimizer";

const DATE = "2026-11-12";
const games = [
  { eventId: "e1", homeTeam: "Washington Wizards", awayTeam: "Detroit Pistons" },
  { eventId: "e2", homeTeam: "Phoenix Suns", awayTeam: "LA Clippers" },
];
const state = (player_key: string, status: string, home: string, away: string, game_date = DATE) =>
  ({ game_date, player_key, home_key: injuryTeamKey(home), away_key: injuryTeamKey(away), status, reason: "Injury/Illness - test" });
const report: InjuryReport = {
  sha: "abc", publishedAt: "2026-11-12T23:45:00.000Z", observedAt: "2026-11-12T23:52:00.000Z", sourceUrl: "https://ak-static.cms.nba.com/referee/injury/x.pdf",
  states: [
    state("jalenduren", "Out", "Washington Wizards", "Detroit Pistons"),
    state("cadecunningham", "Questionable", "Washington Wizards", "Detroit Pistons"),
    state("devinbooker", "Probable", "Phoenix Suns", "Los Angeles Clippers"),
    state("kawhileonard", "Out", "Phoenix Suns", "Los Angeles Clippers", "2026-11-13"),     // tomorrow's game: not tonight's status
    state("twin", "Out", "Washington Wizards", "Detroit Pistons"), state("twin", "Available", "Washington Wizards", "Detroit Pistons"),
  ],
  submissions: [
    { team: "Washington Wizards", game_date: DATE, submission: "submitted" }, { team: "Detroit Pistons", game_date: DATE, submission: "submitted" },
    { team: "Phoenix Suns", game_date: DATE, submission: "submitted" }, { team: "LA Clippers", game_date: DATE, submission: "not_yet_submitted" },
  ],
};
const quoted = [
  { key: "jalenduren", eventId: "e1" }, { key: "cadecunningham", eventId: "e1" }, { key: "kyshawngeorge", eventId: "e1" }, { key: "twin", eventId: "e1" },
  { key: "devinbooker", eventId: "e2" }, { key: "kawhileonard", eventId: "e2" }, { key: "stranger", eventId: "e9" },
];
const got = playerInjuries(DATE, report, quoted, games);
assert.deepEqual(got.get("jalenduren"), { status: "Out", reason: "Injury/Illness - test", teamsSubmitted: true });
assert.equal(got.get("cadecunningham")?.status, "Questionable");
assert.deepEqual(got.get("kyshawngeorge"), { status: null, reason: null, teamsSubmitted: true });           // not listed, both teams filed
assert.equal(got.get("twin")?.status, "unverified");                                                          // two rows: neither cleared nor dropped
assert.deepEqual(got.get("devinbooker"), { status: "Probable", reason: "Injury/Illness - test", teamsSubmitted: false });   // the Clippers have not filed
assert.equal(got.get("kawhileonard")?.status, null);                                                          // his Out row is for another date
assert.equal(got.has("stranger"), false);                                                                    // a game the page does not know
assert.equal(playerInjuries(DATE, null, quoted, games).size, 0);
assert.equal(injuryTeamKey("LA Clippers"), injuryTeamKey("Los Angeles Clippers"));

// In the builder: Out and Doubtful leave the pool, Questionable stays at 43%, a fresh report clears the rest.
const player = (key: string, line: number, extra: Partial<ContestPlayer> = {}): ContestPlayer => ({
  key, player: key.toUpperCase(), eventId: "e1", team: "A", line, pOver: 0.5, books: 5, leadHours: 3,
  curve: [{ point: line - 8.5, p: 0.9 }, { point: line, p: 0.5 }, { point: line + 7.5, p: 0.15 }, { point: line + 12.5, p: 0.05 }], teamsSubmitted: true, ...extra,
});
const slate = [player("a", 33.5, { injury: "Out" }), player("b", 30.5, { injury: "Questionable" }), player("c", 28.5), player("d", 26.5, { injury: "Doubtful" }),
  player("e", 25.5, { injury: "Probable" }), player("f", 24.5, { teamsSubmitted: false }), player("g", 23.5), player("h", 22.5)];
const run = optimizeContest(slate, { sims: 800, entries: 3, reportLeadHours: 0.3 });
assert.equal(run.sitVersion, SIT_INJURY_VERSION);
assert.deepEqual(run.excluded, [{ key: "a", player: "A", line: 33.5, status: "Out" }, { key: "d", player: "D", line: 26.5, status: "Doubtful" }]);
assert.deepEqual(run.pool.map((p) => p.key), ["b", "c", "e", "f", "g", "h"]);
assert.deepEqual(run.sit, [0.43, 0.003, 0.003, sitProb(5, 3), 0.003, 0.003]);       // f: his team has not filed, so the lead-time rate stands
assert.ok(run.entries.every((e) => !e.players.includes("a") && !e.players.includes("d")));
// A report read long before the lock still drops and still flags, but clears nobody.
const stale = optimizeContest(slate, { sims: 800, entries: 3, reportLeadHours: 7.5 });
assert.equal(stale.excluded.length, 2);
assert.deepEqual(stale.sit, [0.43, 0.021, 0.021, 0.021, 0.021, 0.021]);
// No report: nobody is dropped on a status the page never read, and the lead-time rule is named.
const none = optimizeContest(slate, { sims: 800, entries: 3 });
assert.equal(none.sitVersion, SIT_VERSION);
assert.equal(none.excluded.length, 0);
assert.equal(none.pool.length, 8);
// Too few players left after the report is a plain error, not an empty run.
assert.throws(() => optimizeContest(slate.slice(0, 4), { reportLeadHours: 0.3 }), /at least three/);

console.log("test-nba-contest-injury: all assertions passed");
