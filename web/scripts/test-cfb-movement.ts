import assert from "node:assert/strict";
import { movementKind, movementSeries, movementSignals } from "../src/lib/cfb-movement";
import type { CfbTerminalRow, LineAlertRow } from "../src/db/queries";

assert.equal(movementKind("spread_steam"), "steam");
assert.equal(movementKind("total_walking"), "walk");
assert.equal(movementKind("reversal"), "reversal");
assert.equal(movementKind("dk_value"), null);
const game: Pick<CfbTerminalRow, "commenceTime" | "history"> = {
  commenceTime: "2026-09-05T19:00:00Z",
  history: [
    { capturedAt: "2026-09-05T18:30:00Z", books: { a: { spread_home: -4, total_line: 51 } } },
    { capturedAt: "2026-09-05T18:00:00Z", books: { a: { spread_home: -3, total_line: 50 }, b: { spread_home: -2.5 } } },
    { capturedAt: "2026-09-05T19:00:00Z", books: { a: { spread_home: -8 } } },
    { capturedAt: "2026-09-05T18:15:00Z", books: { a: { spread_home: null } } },
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
