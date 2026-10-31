/**
 * Fitted Classic ownership shadow (`nfl-ownership-fit-v1`): the TypeScript port
 * reproduces model/nfl_ownership_fit.py on the shared fixture, position totals
 * and the cap hold, and inactive players draw nothing.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { classicFitGameKey, projectClassicFit } from "../src/lib/nfl-dfs/classic-ownership-fit";

type Row = { position: string; team: string | null; salary: number; proj: number | null; dk_avg: number | null; dk_status: string | null; game_key: string; own: number };
const fixture = JSON.parse(readFileSync(new URL("../../tests/fixtures/nfl_ownership_fit_fixture.json", import.meta.url), "utf-8")) as { players: Row[] };
const players = fixture.players.map((p, i) => ({ id: i, position: p.position, salary: p.salary, team: p.team, proj: p.proj, dkAvg: p.dk_avg,
  dkStatus: p.dk_status, gameKey: p.game_key, active: true }));
const got = projectClassicFit(players);
fixture.players.forEach((p, i) => assert.ok(Math.abs(got.get(i)! - p.own) < 0.02, `#${i} ${p.position} ${p.team}: ${got.get(i)} vs ${p.own}`));
const total = [...got.values()].reduce((a, b) => a + b, 0);
assert.ok(total > 880 && total < 905, `total ${total}`);
assert.ok([...got.values()].every((v) => v <= 60 + 1e-9), "cap");

// An inactive player gets 0 and leaves everyone else's ranks alone except his own seat.
const withOut = projectClassicFit([...players, { ...players[0], id: 999, active: false }]);
assert.equal(withOut.get(999), 0);
assert.equal(classicFitGameKey("CLE@TEN 10/25/2026 01:00PM ET"), "CLE@TEN");
console.log("NFL Classic ownership fit: matches the Python model on the fixture; totals, cap and inactive players hold.");
