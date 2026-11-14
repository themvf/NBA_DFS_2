/**
 * Showdown field model (`nfl-ownership-field-showdown-v2`): the TypeScript port
 * reproduces model/nfl_showdown_field_model.py on the shared fixture, the slot
 * totals hold, and the rules around it (who is ownable, when the lines count as
 * in, what a kicker and a defense are read from) behave as written.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { crowdPoints, projectShowdownField, seatPoints, showdownHasLines, FIELD_KEEP_LINEUPS, FIELD_MIN_FULL_ANCHORS, FIELD_TOP_PLAYERS, NFL_OWNERSHIP_FIELD_SHOWDOWN_VERSION,
  type ShowdownFieldPlayer } from "../src/lib/nfl-dfs/showdown-field-model";
import model from "../src/data/nfl-showdown-field-model-v2.json";

type Row = { position: string; team: string | null; salary: number; captain_salary: number | null; is_out: boolean; pts: number | null;
  anchor: "full" | "td_only" | null; team_implied: number | null; opponent_implied: number | null; captain: number; flex: number };
const fixture = JSON.parse(readFileSync(new URL("../../tests/fixtures/nfl_showdown_field_model_fixture.json", import.meta.url), "utf-8")) as { players: Row[] };
const pool: ShowdownFieldPlayer[] = fixture.players.map((p, i) => ({ id: i, position: p.position, team: p.team, salary: p.salary, captainSalary: p.captain_salary,
  isOut: p.is_out, pts: p.pts, anchor: p.anchor, teamImplied: p.team_implied, opponentImplied: p.opponent_implied }));

assert.equal(model.version, NFL_OWNERSHIP_FIELD_SHOWDOWN_VERSION);
const got = projectShowdownField(pool);
assert.ok(got, "the fixture slate forms lineups");
fixture.players.forEach((p, i) => {
  const r = got.shares.get(i)!;
  assert.ok(Math.abs(r.captain - p.captain) < 0.02, `captain #${i} ${p.position}: ${r.captain} vs ${p.captain}`);
  assert.ok(Math.abs(r.flex - p.flex) < 0.02, `flex #${i} ${p.position}: ${r.flex} vs ${p.flex}`);
});
const values = [...got.shares.values()];
const captain = values.reduce((t, v) => t + v.captain, 0), flex = values.reduce((t, v) => t + v.flex, 0);
assert.ok(Math.abs(captain - 100) < 1e-6 && Math.abs(flex - 500) < 1e-6, `slot totals ${captain} / ${flex}`);
assert.ok(values.every((v) => v.captain >= 0 && v.flex >= 0 && v.captain + v.flex <= 100 + 1e-9), "one seat per lineup");
assert.ok(got.lined && got.keptPlayers === FIELD_TOP_PLAYERS && got.lineups === FIELD_KEEP_LINEUPS, `${got.keptPlayers} players, ${got.lineups} lineups`);
assert.equal(values.filter((v) => v.captain + v.flex > 0).length, FIELD_TOP_PLAYERS);

// Out players, players with no projection and players with no captain price draw nothing.
fixture.players.forEach((p, i) => {
  if (p.is_out || !((p.pts ?? 0) > 0) || !p.captain_salary) assert.deepEqual(got.shares.get(i), { captain: 0, flex: 0 }, `#${i} is not ownable`);
});
const star = pool.map((p, i) => ({ p, own: got.shares.get(i)! })).sort((a, b) => b.own.flex + b.own.captain - (a.own.flex + a.own.captain))[0];
const without = projectShowdownField(pool.map((p) => (p.id === star.p.id ? { ...p, isOut: true } : p)))!;
assert.deepEqual(without.shares.get(star.p.id), { captain: 0, flex: 0 });
const again = [...without.shares.values()];
assert.ok(Math.abs(again.reduce((t, v) => t + v.captain, 0) - 100) < 1e-6 && Math.abs(again.reduce((t, v) => t + v.flex, 0) - 500) < 1e-6, "his ownership goes to the others");

// A kicker is read off his team's Vegas points, a defense off the opponent's; neither reads our projection.
const kicker = pool.find((p) => p.position === "K" && !p.isOut)!, defense = pool.find((p) => p.position === "DST" && !p.isOut)!;
assert.equal(crowdPoints({ ...kicker, pts: 1 }, true), crowdPoints({ ...kicker, pts: 20 }, true));
assert.ok(crowdPoints({ ...kicker, teamImplied: 28 }, true) > crowdPoints({ ...kicker, teamImplied: 17 }, true));
assert.ok(crowdPoints({ ...defense, opponentImplied: 28 }, true) < crowdPoints({ ...defense, opponentImplied: 17 }, true));
assert.equal(crowdPoints({ ...kicker, teamImplied: null }, true), model.kicker.intercept + model.kicker.slope * 21);

// A missing set of lines counts against a player only once the slate's lines are in.
const skill: ShowdownFieldPlayer = { id: 900, position: "WR", team: "AAA", salary: 5000, captainSalary: 7500, isOut: false, pts: 10, anchor: "td_only", teamImplied: null, opponentImplied: null };
assert.equal(crowdPoints(skill, true), 10 * model.fringe.td_only);
assert.equal(crowdPoints({ ...skill, anchor: null }, true), 10 * model.fringe.none);
assert.equal(crowdPoints({ ...skill, anchor: "full" }, true), 10);
assert.equal(crowdPoints(skill, false), 10);
assert.ok(showdownHasLines(pool));
const unlined = pool.map((p) => ({ ...p, anchor: null }));
assert.ok(!showdownHasLines(unlined));
// The rule is twelve ownable players with a full set: eleven is the slate two days out, when starters are still unposted.
const firstFull = (n: number) => pool.map((p, i) => (p.anchor === "full" && pool.slice(0, i).filter((x) => x.anchor === "full" && !x.isOut && (x.pts ?? 0) > 0).length >= n ? { ...p, anchor: null } : p));
assert.equal(FIELD_MIN_FULL_ANCHORS, 12);
assert.ok(!showdownHasLines(firstFull(FIELD_MIN_FULL_ANCHORS - 1)), "eleven full sets are not enough");
assert.ok(showdownHasLines(firstFull(FIELD_MIN_FULL_ANCHORS)), "twelve full sets are enough");
assert.equal(projectShowdownField(firstFull(FIELD_MIN_FULL_ANCHORS - 1))!.lined, false, "one short, a missing set is not held against anyone");
const early = projectShowdownField(unlined)!;
assert.ok(!early.lined && Math.abs([...early.shares.values()].reduce((t, v) => t + v.flex, 0) - 500) < 1e-6);

// The low-end term: a seat under the threshold loses the penalty for each point under it; nothing changes at or above it.
const { threshold, penalty } = model.low_end;
assert.ok(threshold > 0 && penalty > 0, "the low-end term is fitted and positive");
assert.equal(seatPoints(threshold), threshold);
assert.equal(seatPoints(threshold + 6), threshold + 6);
assert.ok(Math.abs(seatPoints(threshold - 1.5) - (threshold - 1.5 - penalty * 1.5)) < 1e-12);
// A cheap player the crowd expects little from is owned less than a straight-line value would give him:
// dropping him further under the threshold costs him more than the same drop costs a player above it.
const kept = pool.map((p, i) => ({ p, own: got.shares.get(i)! })).filter((x) => x.own.flex > 0 && x.p.position !== "K" && x.p.position !== "DST");
const punt = kept.filter((x) => crowdPoints(x.p, true) < threshold).sort((a, b) => b.own.flex - a.own.flex)[0];
assert.ok(punt, "the fixture slate has a kept skill player under the threshold");
const lower = projectShowdownField(pool.map((p) => (p.id === punt.p.id ? { ...p, pts: (p.pts as number) * 0.8 } : p)))!;
assert.ok(lower.shares.get(punt.p.id)!.flex < punt.own.flex, "a lower crowd projection under the threshold lowers his flex ownership");

// A pool that cannot form a lineup returns null rather than a guess.
assert.equal(projectShowdownField(pool.filter((p) => !p.isOut && (p.pts ?? 0) > 0).slice(0, 5)), null);
assert.equal(projectShowdownField(pool.map((p) => ({ ...p, team: "ONE" }))), null);

console.log(`NFL Showdown field model: matches the Python model on the fixture (${got.keptPlayers} players, ${got.lineups} lineups); totals and rules hold.`);
