import assert from "node:assert/strict";
import {
  bookFairProbability, buildNhlMarket, describeMove, marketSnapshot, nhlFreshnessTargetMinutes, tapeSeries,
  type NhlGameTape,
} from "../src/lib/nhl-market";

const close = (a: number | null, b: number, tol = 1e-9) => assert.ok(a != null && Math.abs(a - b) < tol, `${a} != ${b}`);

// Vig removal is proportional over one book's own pair.
const dk = { ml_home: -150, ml_away: 130, spread_home: -1.5, spread_home_price: 170, spread_away: 1.5, spread_away_price: -205,
  total_line: 6.5, over: -110, under: -110, last_update: "2026-09-29T20:00:00Z" };
close(bookFairProbability(dk, "moneyline", "home"), 0.6 / (0.6 + 100 / 230));
close(bookFairProbability(dk, "total", "over"), 0.5);
assert.ok(bookFairProbability(dk, "puckline", "home")! < 0.4);
// A puck-line pair whose lines are not mirror images is not a pair.
assert.equal(bookFairProbability({ ...dk, spread_away: 2.5 }, "puckline", "home"), null);
// |American| < 100 is not a valid price.
assert.equal(bookFairProbability({ ...dk, ml_home: 50 }, "moneyline", "home"), null);

// Price consensus only among books at the consensus line: a 5.5 book never
// blends into the 6.5 over price.
const books = {
  draftkings: dk,
  fanduel: { ...dk, over: -125, under: 105 },
  betmgm: { ...dk, total_line: 5.5, over: -200, under: 160 },
};
const snap = marketSnapshot(books, "total", "over");
assert.equal(snap.line, 6.5);
assert.equal(snap.marketBooks, 3);
assert.equal(snap.lineBooks, 2);
close(snap.probability, 0.5); // lower median of {0.5, ~0.538}; betmgm's 5.5 excluded

// A line move is reported as a line move, never as a price change across bets.
const open = marketSnapshot({ draftkings: { ...dk, total_line: 5.5 } }, "total", "over");
assert.equal(describeMove("total", open, snap), "line 5.5 → 6.5");
const priceOnly = marketSnapshot({ draftkings: { ...dk, over: -130, under: 110 } }, "total", "over");
assert.match(describeMove("total", snap, priceOnly), /^\+\d+\.\dpp$/);

// The price chart gaps books that are off the current consensus line.
const game: NhlGameTape = {
  homeTeam: "Carolina Hurricanes", awayTeam: "Florida Panthers", homeAbbrev: "CAR", awayAbbrev: "FLA",
  commenceTime: "2026-09-29T21:00:00Z", latestCapturedAt: "2026-09-29T20:58:00Z",
  openingBooks: books, currentBooks: books, closingBooks: null,
  history: [{ capturedAt: "2026-09-29T15:00:00Z", books }, { capturedAt: "2026-09-29T21:00:00Z", books }],
};
const view = buildNhlMarket(game, "total", "over", "price", "2026-09-29T21:00:00Z");
assert.equal(view.history[0].values.betmgm, null);
assert.equal(view.history[0].values.draftkings, 50);
assert.equal(view.books.find((row) => row.key === "betmgm")?.atConsensus, false);
assert.equal(view.currentLabel, "6.5 · OVER 50.0%");
const lineView = buildNhlMarket(game, "total", "over", "line", "2026-09-29T21:00:00Z");
assert.equal(lineView.history[0].values.betmgm, 5.5);
assert.equal(lineView.percentage, false);
assert.equal(buildNhlMarket(game, "puckline", "home", "price", game.latestCapturedAt!).currentLabel.startsWith("CAR -1.5 · "), true);
// Freshness: DraftKings updated 20:00, captured 20:58; at 21:00 the book update is an hour old.
assert.equal(view.books.find((row) => row.key === "draftkings")?.fresh, false);

// Sparklines stop at the scheduled start.
assert.equal(tapeSeries(game.history, game.commenceTime, "total").length, 1);

// Freshness budgets follow nhl-dense-v1 worst cases; nothing is owed before T-1200m.
assert.equal(nhlFreshnessTargetMinutes(30), 40);
assert.equal(nhlFreshnessTargetMinutes(150), 210);
assert.equal(nhlFreshnessTargetMinutes(1300), null);

console.log("NHL market views passed: vig removal, same-line consensus, line vs price moves, gaps, freshness.");

// A single capture is both open and current; its move is unobserved, not 0.0.
const single = buildNhlMarket({ ...game, history: game.history.slice(0, 1) }, "moneyline", "home", "price", game.latestCapturedAt!);
assert.equal(single.move, "Awaiting two captures");
console.log("Single-capture move is reported as awaiting, not flat.");
