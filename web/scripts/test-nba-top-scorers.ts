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

// This season's team (latest box-score game) wins over last season's; last
// season's is used only for a player with no game yet; an ambiguous name or a
// latest team not in this game places nobody.
const cur = (athleteId: string, name: string, team: string, gameDate: string) => ({ athleteId, name, team, gameDate });
const moved = buildBoard(
  [
    { ...base, eventId: "m", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "LeBron James", books: { dk: { line: 18.5, over: -110, under: -110 } } },
    { ...base, eventId: "m", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "Tyrese Maxey", books: { dk: { line: 28.5, over: -110, under: -110 } } },
    { ...base, eventId: "m", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "Rookie Guy", books: { dk: { line: 9.5, over: -110, under: -110 } } },
    { ...base, eventId: "m", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "Just Traded", books: { dk: { line: 12.5, over: -110, under: -110 } } },
    { ...base, eventId: "m", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "Twin Name", books: { dk: { line: 7.5, over: -110, under: -110 } } },
    { ...base, eventId: "c", homeTeam: "Denver Nuggets", awayTeam: "Los Angeles Clippers", player: "Clip Guy", books: { dk: { line: 15.5, over: -110, under: -110 } } },
  ],
  [
    { name: "LeBron James", ppg: 24.4, games: 70, teamName: "Los Angeles Lakers", teamAbbrev: "LAL" },
    { name: "Tyrese Maxey", ppg: 28.3, games: 70, teamName: "Philadelphia 76ers", teamAbbrev: "PHI" },
    { name: "Rookie Guy", ppg: 9, games: 10, teamName: "Charlotte Hornets", teamAbbrev: "CHA" },
    { name: "Just Traded", ppg: 12, games: 60, teamName: "Charlotte Hornets", teamAbbrev: "CHA" },
  ],
  [
    { name: "Philadelphia 76ers", abbreviation: "PHI" },
    { name: "Charlotte Hornets", abbreviation: "CHA" },
    { name: "Denver Nuggets", abbreviation: "DEN" },
    { name: "LA Clippers", abbreviation: "LAC" },
  ],
  [],
  [
    cur("1", "LeBron James", "Philadelphia 76ers", "2026-10-30"),
    cur("2", "Tyrese Maxey", "Philadelphia 76ers", "2026-10-30"),
    cur("3", "Just Traded", "Utah Jazz", "2026-10-29"),
    cur("4", "Twin Name", "Philadelphia 76ers", "2026-10-30"),
    cur("5", "Twin Name", "Charlotte Hornets", "2026-10-29"),
    cur("6", "Clip Guy", "LA Clippers", "2026-10-30"),
  ],
);
const by = (name: string) => moved.scorers.find((s) => s.player === name)!;
assert.equal(by("LeBron James").team, "Philadelphia 76ers");
assert.equal(by("LeBron James").teamSource, "current");
assert.equal(by("LeBron James").teamAbbrev, "PHI");
assert.equal(by("LeBron James").teamAsOf, "2026-10-30");
assert.equal(by("LeBron James").opponentAbbrev, "CHA");
// Team rank now counts LeBron among the 76ers: Maxey 1, LeBron 2.
assert.equal(by("Tyrese Maxey").teamRank, 1);
assert.equal(by("LeBron James").teamRank, 2);
// Last season's PPG still shown for a player who changed teams.
assert.equal(by("LeBron James").lastSeasonPpg, 24.4);
// No game yet this season: last season's team, marked prior.
assert.equal(by("Rookie Guy").team, "Charlotte Hornets");
assert.equal(by("Rookie Guy").teamSource, "prior");
assert.equal(by("Rookie Guy").teamAsOf, null);
// Latest team is not in this game: no team, never the stale prior-season one.
assert.equal(by("Just Traded").team, null);
assert.equal(by("Just Traded").teamSource, null);
assert.equal(by("Just Traded").teamRank, null);
// Two athletes with one name key: place neither.
assert.equal(by("Twin Name").team, null);
// ESPN's "LA Clippers" matches The Odds API's "Los Angeles Clippers".
assert.equal(by("Clip Guy").team, "Los Angeles Clippers");
assert.equal(by("Clip Guy").teamAbbrev, "LAC");

// Position and this season's scoring come from the box scores; a player with no
// game played this season has no this-season PPG, never a zero.
const season = buildBoard(
  [
    { ...base, eventId: "s", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "LeBron James", books: { dk: { line: 18.5, over: -110, under: -110 } } },
    { ...base, eventId: "s", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "Only DNPs", books: { dk: { line: 6.5, over: -110, under: -110 } } },
    { ...base, eventId: "s", homeTeam: "Charlotte Hornets", awayTeam: "Philadelphia 76ers", player: "Rookie Guy", books: { dk: { line: 9.5, over: -110, under: -110 } } },
  ],
  [
    { name: "LeBron James", position: null, ppg: 24.4, games: 70, teamName: "Los Angeles Lakers", teamAbbrev: "LAL" },
    { name: "Rookie Guy", position: "G", ppg: 9, games: 10, teamName: "Charlotte Hornets", teamAbbrev: "CHA" },
  ],
  [],
  [],
  [
    { athleteId: "1", name: "LeBron James", team: "Philadelphia 76ers", gameDate: "2026-10-30", position: "F", games: 5, ppg: 16.4 },
    { athleteId: "2", name: "Only DNPs", team: "Charlotte Hornets", gameDate: "2026-10-30", position: "C", games: 0, ppg: null },
  ],
);
const sBy = (name: string) => season.scorers.find((s) => s.player === name)!;
assert.equal(sBy("LeBron James").position, "F");
assert.equal(sBy("LeBron James").thisSeasonPpg, 16.4);
assert.equal(sBy("LeBron James").thisSeasonGames, 5);
assert.equal(sBy("LeBron James").lastSeasonPpg, 24.4);
assert.equal(sBy("Only DNPs").thisSeasonPpg, null);
assert.equal(sBy("Only DNPs").thisSeasonGames, 0);
assert.equal(sBy("Only DNPs").position, "C");
// No box score yet: no this-season numbers; position falls back to last season's.
assert.equal(sBy("Rookie Guy").thisSeasonPpg, null);
assert.equal(sBy("Rookie Guy").thisSeasonGames, 0);
assert.equal(sBy("Rookie Guy").position, "G");

console.log("test-nba-top-scorers: all assertions passed");
