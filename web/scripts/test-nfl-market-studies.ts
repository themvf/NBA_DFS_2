import assert from "node:assert/strict";
import { gradeGame, summarize, type Game } from "../src/lib/nfl/market-studies";

const kickoff = "2026-11-15T18:00:00Z";
const book = { spread_home: -3, spread_away: 3, spread_home_price: -110, spread_away_price: 100,
  total_line: 45, over: -115, under: 105, ml_home: -150, ml_away: 130 };
const game: Game = {
  id: "2026_11_BUF_MIA", date: "2026-11-15", week: 11, home: "MIA", away: "BUF",
  homeRest: 7, awayRest: 10, homeScore: 24, awayScore: 21, kickoff,
  boundary: "2026-11-15T17:55:00Z",
  captures: [
    { id: "1", observedAt: "2026-11-15T16:59:59Z", books: { pinnacle: book, other: { ...book, over: -110, total_line: 44.5, spread_home_price: -105 } } },
    { id: "2", observedAt: "2026-11-15T17:00:00Z", books: { pinnacle: { ...book, spread_home: -2.5, spread_away: 2.5, total_line: 44.5 }, other: book } },
    { id: "3", observedAt: "2026-11-15T17:54:00Z", books: { pinnacle: { total_line: 46 } } },
    { id: "4", observedAt: "2026-11-15T17:55:00Z", books: { pinnacle: book } },
    { id: "5", observedAt: kickoff, books: { pinnacle: book } },
    { id: "6", observedAt: "2026-11-15T17:52:00Z", books: { pinnacle: { ...book, last_update: kickoff } } },
  ],
};
const time = Date.parse("2026-11-16T18:00:00Z");
const row = gradeGame(game, time);
assert.equal(row.spreadQuotes.early?.quote.id, "1");
assert.equal(row.totalQuotes.last?.quote.id, "2");
assert.equal(row.spreadQuotes.last?.quote.id, "2");
assert.equal(gradeGame(game, Date.parse("2026-11-15T17:00:00Z")).spreadQuotes.last?.quote.id, "1");
assert.equal(gradeGame({ ...game, boundary: null }, time).spreadQuotes.last, undefined);
const summary = summarize([row]);
assert.deepEqual(summary.coverage, { games: 1, final: 1, spread: 1, total: 1, moneyline: 1 });
assert.equal(summary.spread.find(item => item.label === "Under 3 · favorite")?.wins, 1);
assert.equal(summary.spread.find(item => item.label === "Under 3 · underdog")?.losses, 1);
assert.equal(summary.movement.find(item => item.label === "Favorite shrank · favorite")?.wins, 1);
assert.equal(summary.totals[0].pushes, 1);
assert.equal(summary.totals[2].wins, 1);
assert.ok(Math.abs(summary.totals[2].units - 100 / 115) < 1e-10);
assert.deepEqual(summary.shopping.spread, { compared: 2, better: 1 });
assert.deepEqual(summary.shopping.total, { compared: 0, better: 0 });
assert.equal(summary.calibration.paired, 1);
assert.equal(summary.calibration.pinnacleBrier, summary.calibration.consensusBrier);
assert.deepEqual(summary.timing[0], { market: "spread", n: 1, first: 0, last: 0.5 });

const tied = summarize([gradeGame({ ...game, awayScore: 24 }, time)]);
assert.equal(tied.moneyline[0].pushes, 1);
assert.equal(tied.calibration.n, 0);
const pending = summarize([gradeGame({ ...game, homeScore: null, awayScore: null }, time)]);
assert.equal(pending.coverage.final, 0);
assert.equal(pending.coverage.spread, 1);
assert.equal(pending.spread.reduce((wins, row) => wins + row.wins, 0), 0);

console.log("NFL Market Studies settlement, fixed cutoffs, coverage, and paired comparisons passed.");
