import assert from "node:assert/strict";
import { buildBoard, consensus, bookLines, devigOver, nameKey, playerKey, teamKey } from "../src/lib/nba-top-scorers";

// Name keys ignore accents, punctuation and suffixes.
assert.equal(nameKey("Nikola Jokić"), nameKey("Nikola Jokic"));
assert.equal(nameKey("Jaren Jackson Jr."), nameKey("Jaren Jackson"));
assert.equal(teamKey("LA Clippers"), teamKey("Los Angeles Clippers"));
assert.equal(playerKey("Herb Jones"), playerKey("Herbert Jones"));
assert.notEqual(playerKey("Herb Williams"), playerKey("Herbert Williams"));

// De-vig: a symmetric -110/-110 pair is 50%; invalid prices give null.
assert.equal(devigOver(-110, -110), 0.5);
assert.equal(devigOver(-110, null), null);
assert.equal(devigOver(-50, 120), null);

// Consensus line is the modal line; P(over) only from books at that line.
const lines = bookLines({
  draftkings: { line: 27.5, over: -115, under: -105 },
  fanduel: { line: 27.5, over: -110, under: -110 },
  betmgm: { line: 28.5, over: 120, under: -150 },
  pinnacle: { line: null, over: -110, under: -110 },
});
assert.equal(lines.length, 3);
const c = consensus(lines)!;
assert.equal(c.line, 27.5);
assert.ok(c.pOver! > 0.5 && c.pOver! < 0.52, `pOver ${c.pOver}`);
assert.equal(consensus([]), null);

// Board: ranked by line, team matched only when last season's team is in the game.
const base = { commenceTime: "2026-10-21T23:30:00Z", capturedAt: "2026-10-21T15:00:00Z" };
const board = buildBoard(
  [
    { ...base, eventId: "e1", homeTeam: "Denver Nuggets", awayTeam: "Los Angeles Clippers", player: "Nikola Jokic", books: { draftkings: { line: 28.5, over: -110, under: -110 } } },
    { ...base, eventId: "e1", homeTeam: "Denver Nuggets", awayTeam: "Los Angeles Clippers", player: "James Harden", books: { draftkings: { line: 21.5, over: -110, under: -110 } } },
    { ...base, eventId: "e2", homeTeam: "Boston Celtics", awayTeam: "New York Knicks", player: "Traded Guy", books: { fanduel: { line: 31.5, over: -110, under: -110 } } },
    { ...base, eventId: "e2", homeTeam: "Boston Celtics", awayTeam: "New York Knicks", player: "No Lines", books: {} },
  ],
  [
    { name: "Nikola Jokić", position: "C", ppg: 29.6, games: 70, teamName: "Denver Nuggets", teamAbbrev: "DEN" },
    { name: "James Harden", ppg: 22.8, games: 79, teamName: "LA Clippers", teamAbbrev: "LAC" },
    { name: "Traded Guy", ppg: 18, games: 60, teamName: "Utah Jazz", teamAbbrev: "UTA" },
  ],
  [
    { name: "Denver Nuggets", abbreviation: "DEN" },
    { name: "LA Clippers", abbreviation: "LAC" },
  ],
);
assert.deepEqual(board.scorers.map((s) => s.player), ["Traded Guy", "Nikola Jokic", "James Harden"]);
// Team rank counts within one team in one game; an unmatched player has none.
assert.deepEqual(board.scorers.map((s) => s.teamRank), [null, 1, 1]);
assert.equal(board.scorers[1].position, "C");
assert.equal(board.scorers[1].opponentAbbrev, "LAC");
assert.equal(board.scorers[2].opponentAbbrev, "DEN");
assert.deepEqual(board.scorers.map((s) => s.rank), [1, 2, 3]);
const jokic = board.scorers[1];
assert.equal(jokic.team, "Denver Nuggets");
assert.equal(jokic.opponent, "Los Angeles Clippers");
assert.ok(Math.abs(jokic.vsLastSeason! - (28.5 - 29.6)) < 1e-9);
assert.equal(board.scorers[2].teamAbbrev, "LAC");
// A player whose last-season team is not in this game gets no team, but keeps his PPG.
assert.equal(board.scorers[0].team, null);
assert.equal(board.scorers[0].lastSeasonPpg, 18);
assert.equal(board.games.length, 2);
assert.equal(board.games.find((g) => g.eventId === "e1")!.lineSum, 50);

// Ambiguous name keys match nobody.
const amb = buildBoard(
  [{ ...base, eventId: "e3", homeTeam: "A", awayTeam: "B", player: "Same Name", books: { dk: { line: 10, over: -110, under: -110 } } }],
  [
    { name: "Same Name", ppg: 10, games: 1, teamName: "A", teamAbbrev: "A" },
    { name: "Same Name Jr.", ppg: 20, games: 1, teamName: "B", teamAbbrev: "B" },
  ],
);
assert.equal(amb.scorers[0].lastSeasonPpg, null);

// A verified provider alias resolves the season reference, but cannot choose
// between two canonical identities with the same alias.
const herbProps = [{ ...base, eventId: "h", homeTeam: "A", awayTeam: "B", player: "Herb Jones", books: { dk: { line: 8.5, over: -110, under: -110 } } }];
const herbStats = [{ name: "Herbert Jones", ppg: 10, games: 60, teamName: "A", teamAbbrev: "A" }];
assert.equal(buildBoard(herbProps, herbStats).scorers[0].lastSeasonPpg, 10);
assert.equal(buildBoard(herbProps, [...herbStats, { ...herbStats[0], name: "Herb Jones" }]).scorers[0].lastSeasonPpg, null);

console.log("test-nba-top-scorers: all assertions passed");
