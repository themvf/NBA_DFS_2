import assert from "node:assert/strict";
import { buildMovementInsights, comparableTrail, cfbIntelligenceEvents, tennisIntelligenceEvents, type IntelligenceEvent } from "../src/lib/movement-intelligence";
import type { LineAlertRow } from "../src/db/queries";

const now = Date.parse("2026-09-05T18:00:00Z");
function event(id = 1): IntelligenceEvent {
  const captures = [20, 10, 0].map((minutes, i) => ({ time: now - minutes * 60_000, books: { a: -2.5 - i * .5, b: -2.5 - i * .5, c: -3 - i * .5 } }));
  return { id, home: `Home ${id}`, away: `Away ${id}`, start: now + 60 * 60_000, completed: false, markets: { spread: captures, total: captures, moneyline: captures } };
}
function alert(overrides: Partial<LineAlertRow> = {}): LineAlertRow {
  return { matchupId: 1, createdAt: new Date(now).toISOString(), matchup: "Away @ Home", commenceTime: new Date(now + 3600000).toISOString(), alertType: "spread_steam", side: "home", alertProb: null, sharpProb: null, details: { market: "spread", books_moved: 4, interval_delta: -1 }, clvPp: null, outcome: null, origin: "prospective", ...overrides };
}
const select = (signals: LineAlertRow[], events = [event()]) => buildMovementInsights(events, signals, now);
assert.equal(select([alert()])[0].selection, "Home 1");
assert.equal(select([alert()])[0].metric, "1.0 pts");
assert.equal(select([alert({ side: "away" })])[0].selection, "Away 1");
assert.equal(select([alert({ alertType: "total_steam", side: "under", details: { market: "total" } })])[0].side, "under");

// Never impose a three/four-item data cap; the UI alone limits the collapsed view.
const five = [1, 2, 3, 4, 5].map(id => event(id));
assert.equal(select(five.map(e => alert({ matchupId: e.id })), five).length, 5);
assert.equal(select([alert(), alert({ alertType: "key_cross" })]).length, 1);
assert.equal(select([alert(), alert({ details: { market: "total" }, side: "over" })]).length, 2);

// The latest signal governs direction even if an older one had more support.
const old = alert({ createdAt: new Date(now - 600_000).toISOString() });
const reversed = alert({ alertType: "reversal", side: "away", details: { market: "spread", reversal_leg: 1.5 } });
assert.equal(select([old, reversed])[0].selection, "Away 1");
assert.equal(select([old, reversed])[0].label, "REVERSAL");
assert.equal(select([alert(), reversed])[0].label, "MIXED DIRECTION");

// Recent ingestion must not make an old trigger current; malformed/future data
// and settled, retrospective, wrong-market, started or completed games stay out.
for (const bad of [
  alert({ details: { trigger_capture_at: new Date(now - 31 * 60_000).toISOString() } }),
  alert({ details: { trigger_capture_at: "bad timestamp" } }),
  alert({ createdAt: new Date(now + 60_000).toISOString() }),
  alert({ origin: "retrospective" }), alert({ details: { origin: "retrospective" } }),
  alert({ outcome: "won" }), alert({ alertType: "dk_value" }), alert({ alertType: "pinnacle_divergence" }),
  alert({ details: { market: "player_total_games" } }), alert({ side: "over" }),
]) assert.equal(select([bad]).length, 0);
assert.equal(select([alert()], [{ ...event(), start: now }]).length, 0);
assert.equal(select([alert()], [{ ...event(), completed: true }]).length, 0);
assert.equal(select([alert()], [{ ...event(), start: NaN }]).length, 0);
assert.equal(select([alert()], [{ ...event(), markets: { spread: [{ time: now - 31 * 60_000, books: { a: -3 } }] } }]).length, 0);
assert.equal(buildMovementInsights([event()], [alert()], NaN).length, 0);

// Book churn cannot generate a chart, and post-start/future points cannot leak in.
assert.equal(comparableTrail([{ time: now - 60000, books: { a: -3, b: -4 } }, { time: now, books: { c: -5, d: -6 } }], now, now + 1).length, 0);
const trail = comparableTrail([...event().markets.spread!, { time: now + 60000, books: { a: -10, b: -10, c: -10 } }], now, now + 1);
assert.equal(trail.length, 3);
assert.equal(trail[2].value, -3.5);
const ranked = select([alert({ matchupId: 1, details: { books_moved: 3 } }), alert({ matchupId: 2, details: { books_moved: 5 } })], [event(1), event(2)]);
assert.equal(ranked[0].matchupId, 2);
assert.equal(select([alert({ details: { overlap_books: ["a", "a", "b"] } })])[0].support, 2);

// Adapters preserve the market basis. ML uses paired no-vig probability, not
// an arithmetic average of American odds, and unavailable tennis markets stay out.
const cfb = cfbIntelligenceEvents([{ matchupId: 1, homeTeam: "H", awayTeam: "A", commenceTime: new Date(now + 3600000).toISOString(), completed: false, history: [{ capturedAt: new Date(now).toISOString(), books: { a: { ml_home: -150, ml_away: 130 } } }] }]);
assert.ok(Math.abs(cfb[0].markets.moneyline![0].books.a - (.6 / (.6 + 100 / 230))) < 1e-10);
const tennis = tennisIntelligenceEvents([{ id: 1, homePlayer: "H", awayPlayer: "A", commenceTime: new Date(now + 3600000).toISOString(), completionStatus: "scheduled", winner: null }], [{ matchupId: 1, trail: [{ capturedAt: new Date(now).toISOString(), homeProb: .6, bookHomeProbs: { a: .6, b: .61 } }] }]);
assert.equal(tennis[0].completed, false);
assert.equal(tennis[0].markets.total, undefined);
assert.equal(buildMovementInsights(tennis, [alert({ alertType: "walking", details: { market: "moneyline", drift_pp: 3 } })], now)[0].metric, "3.0 pp");
console.log("Movement intelligence checks passed: grouping, ranking, direction, expiry, provenance, market routing, and comparable trails.");
