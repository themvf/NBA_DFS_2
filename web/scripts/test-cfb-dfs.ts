/**
 * CFB DFS: the salary reader, the lineup rules, and the builder, against the
 * real 2026-09-25 DraftKings CFB Classic file (fixture) plus edge cases.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { parseCfbKickoff, parseCfbSalaryCsv } from "../src/lib/cfb-dfs/salary-csv";
import { cfbBuildRuleProblems, cfbLineupProblems, cfbUploadCsv, DEFAULT_CFB_SETTINGS, optimizeCfbLineups, type CfbPoolPlayer } from "../src/lib/cfb-dfs/optimizer";

const csv = readFileSync(new URL("./fixtures/cfb-dk-salaries-2026-09-25.csv", import.meta.url), "utf8");
const slate = parseCfbSalaryCsv(csv);
assert.equal(slate.players.length, 235);
assert.deepEqual(slate.games.map((g) => g.game), ["NAVY@UAB", "NW@IU", "CLEM@CAL"]);
assert.equal(parseCfbKickoff("NW@IU 09/25/2026 08:00PM ET"), "2026-09-26T00:00:00.000Z");
assert.equal(slate.firstKickoff, "2026-09-25T23:00:00.000Z");
const woodson = slate.players.find((p) => p.name === "Braxton Woodson")!;
assert.equal(woodson.status, "Q"); assert.equal(woodson.opponent, "UAB");

// An NFL file must be refused, not coerced.
assert.throws(() => parseCfbSalaryCsv("Position,Name + ID,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame\nTE,A (1),A,1,TE/FLEX,4000,KC@BUF 09/27/2026 01:00PM ET,KC,5"), /QB, RB and WR/);
assert.throws(() => parseCfbSalaryCsv("Position,Name + ID,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame\nQB,A (1),A,1,QB,4000,KC@BUF 09/27/2026 01:00PM ET,KC,5"), /S-FLEX/);

// Builder: use DK's own averages as projections, just to exercise the rules.
const pool: CfbPoolPlayer[] = slate.players.filter((p) => !["OUT", "O", "D"].includes(p.status))
  .map((p) => ({ dkId: p.dkId, name: p.name, position: p.position, team: p.team, game: p.game, salary: p.salary, proj: p.dkAvg }));
const result = optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 20 });
assert.equal(result.lineups.length, 20, result.stoppedEarly ?? "");
for (const lineup of result.lineups) {
  assert.deepEqual(cfbLineupProblems(lineup.slots.map((s) => s.player)), [], `lineup ${lineup.lineupNumber}`);
  assert.deepEqual(lineup.slots.map((s) => s.slot), ["QB", "RB", "RB", "WR", "WR", "WR", "FLEX", "S-FLEX"]);
  for (const s of lineup.slots) {
    const ok = s.slot === "S-FLEX" ? true : s.slot === "FLEX" ? s.player.position !== "QB" : s.player.position === s.slot;
    assert.ok(ok, `${s.player.position} in ${s.slot}`);
  }
}
const keys = result.lineups.map((l) => l.slots.map((s) => s.player.dkId).sort().join());
assert.equal(new Set(keys).size, 20, "no duplicate lineups");
for (let i = 0; i < result.lineups.length; i += 1) for (let j = 0; j < i; j += 1) {
  const a = new Set(keys[i].split(",")); const shared = keys[j].split(",").filter((id) => a.has(id)).length;
  assert.ok(shared <= 6, "min unique 2");
}
const counts = new Map<number, number>();
for (const l of result.lineups) for (const s of l.slots) counts.set(s.player.dkId, (counts.get(s.player.dkId) ?? 0) + 1);
assert.ok(Math.max(...counts.values()) <= 14, "70% exposure cap");

// Locks, excludes, a per-player cap.
const hoover = pool.find((p) => p.name === "Josh Hoover")!, chiles = pool.find((p) => p.name === "Aidan Chiles")!;
const controlled = optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 6, lockedIds: [hoover.dkId], excludedIds: [chiles.dkId],
  maxExposureById: { [String(woodson.dkId)]: 50 } });
assert.ok(controlled.lineups.every((l) => l.slots.some((s) => s.player.dkId === hoover.dkId)), "lock holds in every lineup");
assert.ok(controlled.lineups.every((l) => !l.slots.some((s) => s.player.dkId === chiles.dkId)), "exclude holds");
assert.ok(controlled.lineups.filter((l) => l.slots.some((s) => s.player.dkId === woodson.dkId)).length <= 3, "per-player cap 50% of 6");

// Deterministic, and the upload file has DK's slot header.
assert.deepEqual(optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 5 }).lineups.map((l) => l.projection),
  optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 5 }).lineups.map((l) => l.projection));
assert.equal(cfbUploadCsv(result.lineups).split("\n")[0], "QB,RB,RB,WR,WR,WR,FLEX,S-FLEX");

// A single lineup under a 70% cap still builds (the cap rounds up to 1, not down to 0),
// and an explicit 0% cap still keeps a player out.
assert.equal(optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 1 }).lineups.length, 1);
const zeroed = optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 3, maxExposureById: { [String(hoover.dkId)]: 0 } });
assert.ok(zeroed.lineups.every((l) => !l.slots.some((s) => s.player.dkId === hoover.dkId)), "0% means never");

// Tournament rules: 2 QBs, a teammate for every QB, a bring-back for every QB.
const rules = { requireTwoQbs: true, stackQb: true, bringBack: true };
const stacked = optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 20, ...rules });
assert.equal(stacked.lineups.length, 20, stacked.stoppedEarly ?? "");
for (const lineup of stacked.lineups) {
  const players = lineup.slots.map((s) => s.player);
  assert.deepEqual(cfbLineupProblems(players), [], `legal ${lineup.lineupNumber}`);
  assert.deepEqual(cfbBuildRuleProblems(players, rules), [], `rules ${lineup.lineupNumber}`);
  assert.equal(lineup.slots.find((s) => s.slot === "S-FLEX")!.player.position, "QB", "second QB fills SUPER FLEX");
}
// Each rule alone, and the rules off (the default) still allow a one-QB lineup.
for (const only of [{ requireTwoQbs: true }, { stackQb: true }, { bringBack: true }]) {
  const r = optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 8, ...only });
  const flags = { requireTwoQbs: false, stackQb: false, bringBack: false, ...only };
  for (const l of r.lineups) assert.deepEqual(cfbBuildRuleProblems(l.slots.map((s) => s.player), flags), [], JSON.stringify(only));
}
assert.deepEqual(cfbBuildRuleProblems([pool.find((p) => p.name === "Josh Hoover")!, ...pool.filter((p) => p.team === "CAL" && p.position !== "QB").slice(0, 7)], { requireTwoQbs: false, stackQb: true, bringBack: false }),
  ["Josh Hoover has no teammate"], "the checker catches an unstacked QB");

console.log("CFB DFS: salary reader, lineup rules, locks/excludes/caps and export all hold on the real 2026-09-25 slate.");
