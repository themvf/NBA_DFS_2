import assert from "node:assert/strict";
import { grade } from "../src/lib/nba-market-studies";
import { nbaObservations, type Observation } from "../src/lib/daily-study-matches";
import { studySeries, studyPerformance } from "../src/lib/study-performance";

const game = {
  id: "nba-1", home: "Home", away: "Away", date: "2026-11-15", start: "2026-11-16T00:00:00Z",
  cutoff: "2026-11-15T23:30:00Z", homeScore: 109, awayScore: 127,
  homeBackToBack: false, awayBackToBack: false, homePriorVenue: null, awayPriorVenue: null,
  captures: [
    { id: "entry", observedAt: "2026-11-15T23:00:00Z", marketStart: "2026-11-16T00:00:00Z",
      books: { pinnacle: { spread: 8, home_price: 102, away_price: -112, total: 235, over_price: 105, under_price: -120 } } },
    { id: "late", observedAt: "2026-11-15T23:45:00Z", marketStart: "2026-11-16T00:00:00Z",
      books: { pinnacle: { spread: 9, home_price: 122, away_price: -132, total: 237, over_price: -120, under_price: 105 } } },
  ],
};
const observations = nbaObservations(grade(game));
const entry = observations.find(row => row.cohort === "Over 235+ paper trial")!;
assert.equal(entry.quoteId, "entry");
assert.equal(entry.price, 105);
assert.equal(entry.margin, 1);
assert.equal(observations.find(row => row.id === "NBA-01" && row.market === "ATS")?.price, -132);

const row = (id: string, margin: number | null, price?: number): Observation =>
  ({ ...entry, game: { ...entry.game, id }, margin, price });
const base = [entry, row("loss", -1, -110), row("push", 0, -110), row("unpriced", 3), row("pending", null, 150)];
const rejected = { ...row("at-start", 3, 200), observedAt: entry.game.start };
const groups = studySeries([...base, entry, rejected], game.date);
assert.equal(groups.length, 1);
assert.equal(groups[0].rows.length, 5);
const stats = studyPerformance(groups[0].rows);
assert.deepEqual([stats.wins, stats.losses, stats.pushes, stats.pending, stats.priced, stats.missingPrice], [2, 1, 1, 1, 3, 1]);
assert.equal(stats.hitRate, 2 / 3);
assert.equal(stats.pricedHitRate, 0.5);
assert.ok(Math.abs(stats.roi! - 0.05 / 3) < 0.00001);
assert.equal(stats.range, null);
assert.equal(studySeries(base, "2026-11-14").length, 0);
console.log("Study performance uses exact entry prices and separates pending, unpriced, pushes, and decided outcomes.");

