/**
 * Fitted Showdown ownership (`nfl-ownership-fit-showdown-v1`): the TypeScript
 * port reproduces model/nfl_showdown_ownership_fit.py on the shared fixture, and
 * the slot budgets and caps hold.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { projectShowdownFit } from "../src/lib/nfl-dfs/showdown-ownership-fit";
import { projectOwnershipPrior } from "../src/lib/nfl-dfs/ownership-prior";

type Row = { position: string; salary: number; pts: number; team: string | null; dk_status: string | null; flex: number; captain: number };
const fixture = JSON.parse(readFileSync(new URL("../../tests/fixtures/nfl_showdown_ownership_fit_fixture.json", import.meta.url), "utf-8")) as { players: Row[] };
const got = projectShowdownFit(fixture.players.map((p, i) => ({ id: i, position: p.position, salary: p.salary, team: p.team, dkStatus: p.dk_status, pts: p.pts, isOut: false })));
fixture.players.forEach((p, i) => {
  const r = got.get(i)!;
  assert.ok(Math.abs(r.flex - p.flex) < 0.02, `flex #${i} ${p.position}: ${r.flex} vs ${p.flex}`);
  assert.ok(Math.abs(r.captain - p.captain) < 0.02, `captain #${i} ${p.position}: ${r.captain} vs ${p.captain}`);
});
const flex = [...got.values()].reduce((t, v) => t + v.flex, 0), cpt = [...got.values()].reduce((t, v) => t + v.captain, 0);
assert.ok(Math.abs(flex - 500) < 0.01 && Math.abs(cpt - 100) < 0.01, `budgets ${flex} / ${cpt}`);
assert.ok([...got.values()].every((v) => v.captain <= 50 + 1e-9 && v.flex + v.captain <= 95 + 1e-9), "caps");

// Through the prior: an out player and a player with no points draw nothing.
const base = fixture.players.slice(0, 10).map((p, i) => ({ dkPlayerId: i, position: p.position, salary: p.salary, projection: p.pts, dkAvg: null,
  isOut: false, dkStatus: p.dk_status, captainSalary: p.salary * 1.5, team: p.team }));
const res = projectOwnershipPrior([...base, { ...base[0], dkPlayerId: 98, isOut: true }, { ...base[1], dkPlayerId: 99, projection: null }], "showdown");
assert.equal(res.version, "nfl-ownership-fit-showdown-v1");
for (const id of [98, 99]) assert.equal(res.players.find((p) => p.dkPlayerId === id)!.ownPct, 0);
console.log("NFL Showdown ownership fit: matches the Python model on the fixture; budgets and caps hold.");
