import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { cfbObservations, cfbStudyTriggers, matchAdapterStudyIds, nbaObservations,
  nflObservations, qualifyingStudyMatches, type CfbStudySelection } from "../src/lib/daily-study-matches";
import { grade } from "../src/lib/nba-market-studies";
import { gradeGame } from "../src/lib/nfl/market-studies";
import { studyRegistry } from "../src/lib/study-registry";

const prior = "2026-11-10";
const selected = "2026-11-14";
const start = selected + "T23:00:00Z";
const known = selected + "T16:00:00Z";
const entry = selected + "T18:00:00Z";
const make = (id: string, date: string, margin: number | null, finalObservedAt: string | undefined = known) => ({
  id: "NBA-01" as const, cohort: "Under 4", market: "ATS" as const, side: "Home -3", margin,
  observedAt: date === selected ? entry : prior + "T20:00:00Z",
  game: { sport: "NBA" as const, id, date, start: date + "T23:00:00Z", finalObservedAt, home: "Home", away: "Away" },
});
const past = Array.from({ length: 20 }, (_, i) => make("p" + i, prior, i < 13 ? 1 : -1));
const target = make("target", selected, 99);
const rows = [...past, past[0], make("push", prior, 0),
  make("same-day", selected, -1), make("late-final", prior, -1, start),
  make("unknown-final", prior, -1, ""), make("future", "2026-11-15", -1), target];
const matches = qualifyingStudyMatches(rows, selected, Date.parse(start));
assert.equal(matches.find(row => row.game.id === "target")?.rate, 0.65);
assert.deepEqual(matches[0].record, { wins: 13, losses: 7, pushes: 1 });
assert.equal(qualifyingStudyMatches(rows, selected, Date.parse(entry)).length, 0);
assert.equal(qualifyingStudyMatches([...rows, make("extra-loss", prior, -1)], selected, Date.parse(start)).length, 0);
assert.equal(qualifyingStudyMatches([make("push", prior, 0), target], selected, Date.parse(start)).length, 0);

const game = { id: "nba", date: selected, home: "Home", away: "Away", start, finalObservedAt: null,
  cutoff: selected + "T22:30:00Z", homeScore: 104, awayScore: 101, homeBackToBack: false,
  awayBackToBack: true, homePriorVenue: null, awayPriorVenue: "Home" };
const nba = grade({ ...game, captures: [
  { id: "1", observedAt: entry, marketStart: start, books: { pinnacle: { spread: 2, total: 204 } } },
  { id: "2", observedAt: selected + "T19:00:00Z", marketStart: start, books: { pinnacle: { spread: -3, total: 205 } } },
] });
const nbaSignals = nbaObservations(nba);
assert.equal(nbaSignals.find(row => row.id === "NBA-04")?.cohort, "Favorite changed or opened pick’em · favorite");
assert.equal(nbaSignals.find(row => row.id === "NBA-01" && row.market === "ATS")?.margin, 0);
assert.equal(nbaSignals.find(row => row.id === "NBA-01" && row.market === "Outright win")?.margin, 3);
assert.ok(nbaSignals.some(row => row.id === "NBA-11" && row.cohort.includes("same venue")));

const nfl = gradeGame({ id: "nfl", week: 11, date: selected, kickoff: start, boundary: start, home: "Home", away: "Away",
  homeScore: 21, awayScore: 24, homeRest: 7, awayRest: 5, captures: [
    { id: "1", observedAt: entry, books: { pinnacle: {
      ml_home: 150, ml_away: -170, spread_home: 3, spread_away: -3, spread_home_price: -110, spread_away_price: -110,
    } } },
  ] }, Date.parse(start));
assert.equal(nflObservations(nfl).find(row => row.id === "NFL-10")?.side, "Away");
assert.equal(Math.abs(nflObservations(nfl).find(row => row.id === "NFL-01")!.margin!), 0);

const cfb: CfbStudySelection = { id: "cfb", date: prior, start: prior + "T23:00:00Z", home: "Home", away: "Away",
  trigger: "T3_small_fav_dog_spread", frozenAt: prior + "T20:00:00Z", selection: "Away +3", spread: -3,
  total: 50, homeMl: -160, awayMl: 140, homeScore: 26, awayScore: 23, finalObservedAt: known };
assert.equal(cfbObservations([cfb])[0].margin, 0);
assert.equal(cfbObservations([{ ...cfb, trigger: "T4_big_fav_over", total: 48 }])[0].margin, 1);

const registryIds = Object.keys(studyRegistry);
for (const sport of ["nba", "nfl"]) {
  const source = readFileSync(new URL("../src/app/" + sport + "/market-studies/page.tsx", import.meta.url), "utf8");
  const published = [...new Set(source.match(new RegExp(sport.toUpperCase() + "-\\d+", "g")))].sort();
  assert.deepEqual(published, registryIds.filter(id => id.startsWith(sport.toUpperCase() + "-")).sort(),
    "Add every published study to the shared registry with a matching or diagnostic policy");
}
const cfbSource = readFileSync(new URL("../../model/cfb_pattern_watch.py", import.meta.url), "utf8");
const triggerBlock = cfbSource.split("TRIGGERS = {")[1].split(/\r?\n}/)[0];
assert.deepEqual([...triggerBlock.matchAll(/"(T\d+_\w+)":/g)].map(match => match[1]).sort(), cfbStudyTriggers.sort(),
  "Connect each new registered CFB trigger to the shared adapter");
assert.deepEqual([...matchAdapterStudyIds].sort(), Object.entries(studyRegistry)
  .filter(([, value]) => value.mode === "match").map(([id]) => id).sort());
console.log("65% cutoff, temporal boundaries, market settlement, and registry parity passed.");
