import assert from "node:assert/strict";
import { sportsbookTrails, marketTrail, normalizeMlbDate, quote, summarizeSignals, type MlbCapture, type MlbTerminalSignal } from "../src/lib/mlb-terminal";
assert.equal(normalizeMlbDate("2026-02-30", new Date("2026-09-06T02:00:00Z")), "2026-09-05");
assert.equal(normalizeMlbDate("2026-09-06"), "2026-09-06");
assert.equal(quote({ spread_home: -1.5, spread_price: 120 }, "run_line", "home")?.price, 120);
assert.equal(quote({ spread_home: -1.5, spread_price: 120 }, "run_line", "away"), null);
assert.equal(quote({ over: -110, under: -110, over_line: 8, under_line: 9 }, "total", "over")?.fair, null);
const book = { ml_home: -110, ml_away: -110 };
const h: MlbCapture[] = [
  { id: 1, capturedAt: "2026-09-06T12:00:00Z", books: { draftkings: book, polymarket: { ml_home: -900, ml_away: 700 } } },
  { id: 2, capturedAt: "2026-09-06T12:30:00Z", books: { draftkings: book, fanduel: { ml_home: -900, ml_away: 700 } } },
];
const trail = marketTrail(h, "moneyline", "home");
assert.deepEqual(trail.books, ["draftkings"]);
assert.equal(trail.points[0].value, trail.points[1].value);
assert.equal(marketTrail([h[0]], "moneyline", "home").points.length, 1);
assert.equal(marketTrail([], "total", "over").points.length, 0);
const pricedHistory: MlbCapture[] = [
  { id: 1, capturedAt: "2026-09-06T12:00:00Z", books: { draftkings: { total_line: 8, over: -110, under: -110 } } },
  { id: 2, capturedAt: "2026-09-06T12:30:00Z", books: { draftkings: { total_line: 8.5, over: -110, under: -110 } } },
  { id: 3, capturedAt: "2026-09-06T13:00:00Z", books: { draftkings: { total_line: 8.5, over: -130, under: 110 } } },
];
const priceTrail = marketTrail(pricedHistory, "total", "over", "draftkings", true);
assert.equal(priceTrail.points.length, 2); // Different total is never compared as price movement.
assert.ok(priceTrail.points[1].value > priceTrail.points[0].value);
assert.equal(marketTrail(pricedHistory, "total", "over", "", true).points.length, 0);
const signal: MlbTerminalSignal = { id: 1, matchupId: 1, date: "2026-09-06", matchup: "A @ B", type: "steam", side: "home", observedAt: "2026-09-06T12:00:00Z", outcome: "won", details: { dk_decimal: 2.5 }, grade: {}, clvPp: 7 };
const rows = summarizeSignals([signal, { ...signal, id: 2, outcome: "lost" }, { ...signal, id: 3, outcome: "void", grade: { settlement_reason: "push" } }, { ...signal, id: 4, outcome: "void" }, { ...signal, id: 5, outcome: null }]);
assert.equal(rows[0].wins, 1); assert.equal(rows[0].losses, 1); assert.equal(rows[0].pushes, 1); assert.equal(rows[0].voids, 1); assert.equal(rows[0].pending, 1);
assert.equal(rows[0].units, .5); assert.equal(rows[0].priced, 3); assert.equal(rows[0].clv.length, 0);
assert.equal(summarizeSignals([{ ...signal, grade: { close_cohort: "verified_clv_v1" } }])[0].clv[0], 7);
assert.equal(summarizeSignals([{ ...signal, type: "pinnacle_polymarket_delta", grade: { close_cohort: "verified_clv_v1" } }])[0].clv.length, 0);
assert.equal(summarizeSignals([signal, { ...signal, details: { signal_version: "new" } }]).length, 2);
assert.equal(summarizeSignals([
  { ...signal, type: "mlb_total_steam", outcome: null, details: { market: "total", signal_version: "v1" } },
  { ...signal, type: "mlb_total_steam", details: { market: "total", signal_version: "v1" }, grade: { verified_clv: .5, clv_unit: "runs" } },
]).length, 1);
console.log("MLB terminal date, quote, matched-book history and scorecard checks passed");

const books = sportsbookTrails(h, "moneyline", "home");
assert.deepEqual(books.map((series) => series.key), ["draftkings", "fanduel"]);
assert.equal(books[1].points[0].value, null, "A book arriving later must leave a gap");
assert.notEqual(books[0].points[1].value, books[1].points[1].value, "Book disagreement must remain visible");
assert.equal(sportsbookTrails([h[1]], "moneyline", "home")[1].color, books[1].color);
assert.deepEqual(sportsbookTrails(pricedHistory, "total", "over")[0].points.map((p) => p.value), [8, 8.5, 8.5]);
assert.equal(sportsbookTrails([{ ...h[0], books: { draftkings: { ml_home: -110 } } }], "moneyline", "home").length, 0, "Unpaired prices cannot become fair probabilities");
assert.equal(sportsbookTrails([], "run_line", "away").length, 0);
