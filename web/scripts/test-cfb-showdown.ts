import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { parseCfbSalaryCsv } from "../src/lib/cfb-dfs/salary-csv";
import { optimizeCfbLineups, cfbUploadCsv } from "../src/lib/cfb-dfs/optimizer";
import { DEFAULT_CFB_SETTINGS, cfbAssignedLineupProblems, type CfbPoolPlayer } from "../src/lib/cfb-dfs/settings";
import { exportCfbDkEntries } from "../src/lib/cfb-dfs/entry-export";
import { parseLobby } from "../src/lib/cfb-dfs/live-status";

const header = "Position,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame";
const rows = Array.from({ length: 10 }, (_, i) => {
  const pos = i < 2 ? "QB" : i < 5 ? "RB" : "WR";
  const team = i % 2 ? "UAB" : "NAVY";
  const salary = 10000 - i * 700;
  return [`${pos},Player ${i},${100 + i},FLEX,${salary},NAVY@UAB 11/01/2026 08:00PM ET,${team},${25 - i}`,
    `${pos},Player ${i},${200 + i},CPT,${salary * 1.5},NAVY@UAB 11/01/2026 08:00PM ET,${team},${25 - i}`];
}).flat();
const csv = [header, ...rows].join("\n");
const parsed = parseCfbSalaryCsv(csv, "showdown");
assert.equal(parsed.mode, "showdown");
assert.equal(parsed.players.length, 10);
assert.equal(parsed.players[0].captainId, 200);
assert.throws(() => parseCfbSalaryCsv(csv), /S-FLEX/);
assert.throws(() => parseCfbSalaryCsv(csv.replace(',CPT,15000,', ',CPT,14000,'), "showdown"), /Captain/);
assert.throws(() => parseCfbSalaryCsv([header, ...rows.slice(1)].join("\n"), "showdown"), /both|pairs/);
assert.throws(() => parseCfbSalaryCsv(csv.replace(',101,FLEX,', ',100,FLEX,'), "showdown"), /appears twice/);
const pool: CfbPoolPlayer[] = parsed.players.map((p) => ({ ...p, proj: p.dkAvg }));
const settings = { ...DEFAULT_CFB_SETTINGS, mode: "showdown" as const, nLineups: 1, minSalary: 0, randomness: 0, maxExposure: 1 };
const result = optimizeCfbLineups(pool, settings);
assert.equal(result.lineups.length, 1, result.stoppedEarly ?? "");
const best = result.lineups[0];
assert.deepEqual(cfbAssignedLineupProblems(best), []);
// Independent exhaustive oracle: select every six-player subset and every possible captain.
let optimum = -Infinity;
for (let mask = 0; mask < 1 << pool.length; mask++) {
  const picked = pool.filter((_, i) => mask & (1 << i));
  if (picked.length !== 6 || new Set(picked.map((p) => p.team)).size !== 2) continue;
  for (const captain of picked) {
    const salary = picked.reduce((n, p) => n + p.salary, 0) + captain.salary * 0.5;
    if (salary <= 50000) optimum = Math.max(optimum, picked.reduce((n, p) => n + p.proj, 0) + captain.proj * 0.5);
  }
}
assert.equal(best.projection, optimum);
const exported = cfbUploadCsv([best]).split("\n");
assert.equal(exported[0], "CPT,UTIL,UTIL,UTIL,UTIL,UTIL");
assert.equal(Number(exported[1].split(",")[0]), best.slots[0].player.captainId);
const template = "Entry ID,Contest Name,Contest ID,Entry Fee,CPT,FLEX,FLEX,FLEX,FLEX,FLEX\n1,Example,2,$1,,,,,,";
assert.ok(exportCfbDkEntries(template, [best]).csv.includes(`(${best.slots[0].player.captainId})`));
assert.throws(() => exportCfbDkEntries(template.replace("CPT", "QB"), [best]), /entries file/);
const portfolio = optimizeCfbLineups(pool, { ...settings, nLineups: 4, maxExposure: 0.75, minUnique: 1, lockedIds: [109], excludedIds: [100] });
assert.ok(portfolio.lineups.length > 0);
for (const l of portfolio.lineups) {
  assert.deepEqual(cfbAssignedLineupProblems(l), []);
  assert.ok(l.slots.some((s) => s.player.dkId === 109));
  assert.ok(l.slots.every((s) => s.player.dkId !== 100));
}
for (const p of pool.filter((p) => p.dkId !== 109)) assert.ok(portfolio.lineups.filter((l) => l.slots.some((s) => s.player.dkId === p.dkId)).length <= 3);
assert.throws(() => optimizeCfbLineups(pool, { ...settings, lockedIds: [999] }), /unavailable/);
assert.throws(() => cfbUploadCsv([{ ...best, slots: best.slots.map((s, i) => i === 1 ? { ...s, player: best.slots[0].player } : s) }]), /distinct/);
assert.equal(parseLobby({ DraftGroups: [{ DraftGroupId: 1, ContestTypeId: 94 }, { DraftGroupId: 2, ContestTypeId: 95 }] }, "showdown")[0].draftGroupId, 2);
console.log("CFB Showdown: parser, exhaustive optimum, legality, portfolio controls, Captain export, and mode isolation passed.");

assert.equal(parseCfbSalaryCsv(csv.replaceAll("WR,Player 9", "K,Player 9"), "showdown").players.at(-1)?.position, "K");

// Real DraftKings CFB Showdown file (draft group 155789, OSU @ USC, 2026-10-31):
// non-Captain rows are written "UTIL", kickers are listed, Captain costs exactly 1.5x.
const real = parseCfbSalaryCsv(readFileSync(new URL("./fixtures/cfb-dk-showdown-salaries-2026-10-31-osu-usc.csv", import.meta.url), "utf8"), "showdown");
assert.equal(real.players.length, 82);
assert.equal(real.players.filter((p) => p.position === "K").length, 6);
assert.ok(real.players.every((p) => p.captainId && p.captainSalary === p.salary * 1.5));
const realPool: CfbPoolPlayer[] = real.players.filter((p) => p.position !== "K" && p.status !== "OUT").map((p) => ({ ...p, proj: p.dkAvg }));
const realRun = optimizeCfbLineups(realPool, { ...settings, nLineups: 3, minUnique: 1, maxExposure: 1 });
assert.equal(realRun.lineups.length, 3, realRun.stoppedEarly ?? "");
for (const l of realRun.lineups) assert.deepEqual(cfbAssignedLineupProblems(l), []);
assert.equal(cfbUploadCsv(realRun.lineups).split("\n")[0], "CPT,UTIL,UTIL,UTIL,UTIL,UTIL");
console.log("CFB Showdown: real DraftKings file parsed and optimized.");
