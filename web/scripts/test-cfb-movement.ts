import assert from "node:assert/strict";
import { classifyCfbSignal, movementKind, movementSeries, movementSignals } from "../src/lib/cfb-movement";
import type { CfbTerminalRow, LineAlertRow } from "../src/db/queries";

assert.equal(movementKind("spread_steam"), "steam");
assert.equal(movementKind("total_walking"), "walk");
assert.equal(movementKind("reversal"), "reversal");
assert.equal(movementKind("dk_value"), null);
assert.equal(movementKind("gap_repricing"), "gap");
assert.equal(movementKind("cumulative_move"), "cumulative");
const gapHistory = { history: [
  { historyId: 1, capturedAt: "2026-09-24T19:01:00Z", books: {} },
  { historyId: 2, capturedAt: "2026-09-25T19:01:00Z", books: {} },
] } as Pick<CfbTerminalRow, "history">;
const legacySteam: LineAlertRow = {
  matchupId: 1, createdAt: "2026-09-25T19:01:00Z", matchup: "Hawaii @ Wyoming",
  commenceTime: "2026-09-26T19:00:00Z", alertType: "steam", side: "home",
  alertProb: null, sharpProb: null, details: { signal_version: "cfb-lines-v1", trigger_history_id: 2 },
  clvPp: null, outcome: null, origin: "prospective",
};
const legacyWalk = { ...legacySteam, alertType: "walking" };
assert.equal(classifyCfbSignal(legacySteam, gapHistory).alertType, "gap_repricing");
assert.equal(classifyCfbSignal(legacyWalk, gapHistory).alertType, "cumulative_move");
assert.equal(classifyCfbSignal({ ...legacySteam, details: { signal_version: "cfb-moneyline-v2", trigger_history_id: 2 } }, gapHistory).alertType, "steam");
const oldAt = "2026-09-25T18:45:00Z", newAt = "2026-09-25T19:00:00Z";
const quote = (home: number, away: number, updated: string) => ({ ml_home: home, ml_away: away, last_update: updated });
const rapidHistory = { history: [
  { historyId: 1, capturedAt: oldAt, books: Object.fromEntries(["draftkings", "fanduel", "betmgm"].map(key => [key, quote(-110, -110, oldAt)])) },
  { historyId: 2, capturedAt: newAt, books: Object.fromEntries(["draftkings", "fanduel", "betmgm"].map(key => [key, quote(-125, 105, newAt)])) },
] } as Pick<CfbTerminalRow, "history">;
assert.equal(classifyCfbSignal(legacySteam, rapidHistory).alertType, "steam");
const game: Pick<CfbTerminalRow, "commenceTime" | "history"> = {
  commenceTime: "2026-09-05T19:00:00Z",
  // Book keys must be in the six-book policy (src/lib/sportsbook-policy.ts);
  // movementSeries ignores any other book since 2026-09-07.
  history: [
    { capturedAt: "2026-09-05T18:30:00Z", books: { draftkings: { spread_home: -4, total_line: 51 } } },
    { capturedAt: "2026-09-05T18:00:00Z", books: { draftkings: { spread_home: -3, total_line: 50 }, fanduel: { spread_home: -2.5 } } },
    { capturedAt: "2026-09-05T19:00:00Z", books: { draftkings: { spread_home: -8 } } },
    { capturedAt: "2026-09-05T18:15:00Z", books: { draftkings: { spread_home: null } } },
  ],
};
assert.deepEqual(movementSeries(game, "spread").map((p) => p.value), [-3, -4]);
assert.deepEqual(movementSeries(game, "total").map((p) => p.value), [50, 51]);
assert.deepEqual(movementSeries({ ...game, history: [] }, "spread"), []);
assert.equal(movementSeries({ ...game, history: game.history.slice(0, 1) }, "spread").length, 1);
const signals = [
  { matchupId: 1, alertType: "spread_steam", createdAt: "2026-09-05T18:00:00Z" },
  { matchupId: 2, alertType: "reversal", createdAt: "2026-09-05T18:00:00Z" },
  { matchupId: 1, alertType: "dk_value", createdAt: "2026-09-05T18:00:00Z" },
] as LineAlertRow[];
assert.equal(movementSignals(signals, 1).length, 1);
assert.equal(movementSignals(signals, 3).length, 0);
console.log("CFB movement checks passed");
