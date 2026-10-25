/**
 * Ghost-lineup factorial (nfl-ghost-factorial-v1): design, arm settings,
 * projection reverts, the system slate, and a timed build of real arms on
 * the frozen week-5 inputs. Offline: no database.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { optimizeNflLineups } from "../src/app/dfs/nfl/nfl-optimizer";
import { buildDesign, ghostDesign, GHOST_ARMS, GHOST_FACTORS, GHOST_LINEUPS, houseLevels, maxCrossCorrelation } from "../src/lib/nfl-dfs/ghost/design";
import { armProjection, buildArm, CEILING_CAP_OFF } from "../src/lib/nfl-dfs/ghost/arm";
import { houseSettings } from "../src/lib/nfl-dfs/ghost/house";
import { freezeTiming, FRESH_LINES_DEADLINE_MINUTES, FRESH_LINES_MINUTES } from "../src/lib/nfl-dfs/ghost/freeze-timing";
import { CAPTAIN_ID_OFFSET, dkGameInfo, systemSlate, type PoolRow } from "../src/lib/nfl-dfs/ghost/system-slate";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Raw = Record<string, any>;

// ---- Design --------------------------------------------------------------
/** The frozen designs. A change here is a new version (docs/nfl-ghost-factorial-study.md). */
const PINNED = { classic: "2a2eb6debe8a", showdown: "8c96dd79697c" };
for (const format of ["classic", "showdown"] as const) {
  const d = ghostDesign(format);
  assert.equal(d.digest.slice(0, 12), PINNED[format], `${format}: design digest is pinned`);
  assert.equal(d.arms.length, GHOST_ARMS + 1);
  assert.deepEqual(d.arms[0], { arm: 0, house: true, levels: houseLevels(format) }, "arm 0 is the house");
  assert.deepEqual(buildDesign(format), d, `${format}: deterministic`);
  const keys = new Set(d.arms.map(a => JSON.stringify(a.levels)));
  assert.equal(keys.size, GHOST_ARMS + 1, `${format}: no repeated arm and none equal to the house`);
  for (const f of GHOST_FACTORS[format]) {
    const counts = f.levels.map(l => d.arms.slice(1).filter(a => a.levels[f.key] === l).length);
    assert.ok(Math.max(...counts) - Math.min(...counts) <= 1, `${format} ${f.key}: balanced ${counts}`);
  }
  const columns = GHOST_FACTORS[format].map(f => d.arms.slice(1).map(a => f.levels.indexOf(a.levels[f.key])));
  assert.ok(maxCrossCorrelation(GHOST_FACTORS[format], columns) <= 0.12, `${format}: near-orthogonal (${d.maxCorrelation})`);
  const two = GHOST_FACTORS[format].map((f, j) => ({ f, j })).filter(x => x.f.levels.length === 2);
  assert.equal(maxCrossCorrelation(two.map(x => x.f), two.map(x => columns[x.j])), 0, `${format}: two-level factors exactly orthogonal`);
}

// ---- Projection reverts --------------------------------------------------
const dist = (mean: number) => ({ mean, p10: mean / 2, median: mean, p90: mean * 2, boom: null });
const anchored = { ourProj: 20, floor: 10, median: 20, ceiling: 40, marketAnchor: { before: dist(15) }, absenceCommit: null, supersededAbsenceCommit: { before: dist(8) } };
const committed = { ourProj: 12, floor: 6, median: 12, ceiling: 24, absenceCommit: { before: dist(5) } };
const plain = { ourProj: 9, floor: 4, median: 9, ceiling: 18 };
assert.equal(armProjection(anchored, "anchored", true).ourProj, 20);
assert.equal(armProjection(anchored, "anchored", false).ourProj, 20, "the line carries the news");
assert.equal(armProjection(anchored, "model", true).ourProj, 15);
assert.equal(armProjection(anchored, "model", false).ourProj, 8, "both reverted, via the overridden commit");
assert.equal(armProjection(committed, "anchored", false).ourProj, 5);
assert.equal(armProjection(committed, "model", true).ourProj, 12);
assert.equal(armProjection(plain, "model", false).ourProj, 9);

// ---- System slate --------------------------------------------------------
assert.equal(dkGameInfo("SEA", "DEN", "2026-10-16T00:15:00Z"), "SEA@DEN 10/15/2026 08:15PM ET", "the CSV's own Game Info");
assert.equal(dkGameInfo("CIN", "MIA", "2026-10-11T17:00:00Z"), "CIN@MIA 10/11/2026 01:00PM ET");
const pool: PoolRow[] = [
  { dk_pid: 10, name: "Sam Darnold", position: "QB", team: "SEA", opponent: "DEN", salary: 9800, status: null, is_disabled: false, ppg: 18.2 },
  { dk_pid: 11, name: "Bo Nix", position: "QB", team: "DEN", opponent: "SEA", salary: 9600, status: null, is_disabled: false, ppg: 19.1 },
  { dk_pid: 12, name: "Zach Charbonnet", position: "RB", team: "SEA", opponent: "DEN", salary: 6000, status: "O", is_disabled: false, ppg: 9 },
  { dk_pid: 13, name: "Jason Myers", position: "K", team: "SEA", opponent: "DEN", salary: 5000, status: null, is_disabled: false, ppg: null },
];
const games = [{ gameId: 1, away: "SEA", home: "DEN", kickoff: "2026-10-16T00:15:00Z" }];
const sd = systemSlate("showdown", pool, games);
assert.equal(sd.players.length, 4);
const darnold = sd.players.find(p => p.name === "Sam Darnold")!;
assert.deepEqual(darnold.captain, { dkPlayerId: CAPTAIN_ID_OFFSET + 10, salary: 14700 }, "captain = 1.5x flex");
assert.equal(darnold.gameInfo, "SEA@DEN 10/15/2026 08:15PM ET");
assert.equal(darnold.homeAway, "away"); assert.equal(darnold.opponent, "DEN"); assert.equal(darnold.avgFptsDk, 18.2);
assert.equal(sd.players.find(p => p.name === "Zach Charbonnet")!.isOut, true, "pool O = out");
assert.ok(sd.warnings.some(w => /no DraftKings average/.test(w)), "a missing average is said, not zeroed");
assert.throws(() => systemSlate("showdown", [], games), /no usable players/);
assert.throws(() => systemSlate("showdown", [...pool, { ...pool[0], dk_pid: 99, team: "KC", opponent: "LV" }], [...games, { gameId: 2, away: "KC", home: "LV", kickoff: "2026-10-16T00:15:00Z" }]), /one game/);
const classicPool = [...pool.filter(p => p.position !== "K"), { dk_pid: 20, name: "Patrick Mahomes", position: "QB", team: "KC", opponent: "LV", salary: 7000, status: null, is_disabled: false, ppg: 20 }];
const cl = systemSlate("classic", classicPool, [...games, { gameId: 2, away: "KC", home: "LV", kickoff: "2026-10-11T20:25:00Z" }]);
assert.deepEqual(cl.games, ["KC@LV", "SEA@DEN"]);
assert.deepEqual(cl.players.find(p => p.name === "Zach Charbonnet")!.rosterPositions, ["RB", "FLEX"]);
assert.equal(cl.players.find(p => p.name === "Sam Darnold")!.captain, null);

// ---- Arms on the frozen week-5 Classic -----------------------------------
const archived = JSON.parse(gunzipSync(readFileSync("../artifacts/nfl-classic-attribution-2026-10-13/inputs/2026-w5-classic.json.gz")).toString());
const frozen: Raw[] = archived.players.map((p: Raw) => { const q = { ...p }; delete q.actual; delete q.actualSource; return q; });
const house = houseSettings("classic", { favoriteTeam: null, underdogTeam: null });
assert.equal(house.nLineups, GHOST_LINEUPS.classic);
assert.equal(house.buildShape, "winners"); assert.equal(house.defensiveAdjustments?.mode ?? "off", "off");
assert.deepEqual(house.lockedPlayerIds, []); assert.deepEqual(house.exposurePolicies ?? [], []);

const d = ghostDesign("classic");
const h = buildArm("classic", frozen, house, d.arms[0].levels);
assert.equal(h.settings.buildShape, "standard", "the preset is resolved once, never reapplied");
assert.equal(h.settings.maxExposure, 0.55); assert.equal(h.settings.minUnique, 4); assert.equal(h.settings.bringBackShare, 0.5);
assert.ok(h.settings.maxQbExposure != null && h.settings.maxDstVsOffenseShare === 0.05);
assert.equal(h.settings.minSalary, 48_100);
const offArm = d.arms.find(a => a.levels.teWrRules === false && a.levels.rbDstRules === false && a.levels.qbCap === false)!;
const off = buildArm("classic", frozen, house, offArm.levels);
assert.equal(off.ruleExcludedIds.length, 0, "rules off: no pool exclusions");
assert.equal(off.settings.maxQbExposure, undefined);
assert.ok(h.ruleExcludedIds.length > 0, "house applies the pool rules");
const cap = d.arms.find(a => a.levels.ceilingCap === "off")!;
assert.equal(buildArm("classic", frozen, house, cap.levels).settings.maxCeilingMultiple, CEILING_CAP_OFF);

const timed: number[] = [];
for (const arm of [d.arms[0], d.arms[1], offArm]) {
  const b = buildArm("classic", frozen, house, arm.levels);
  const t = Date.now();
  const r = optimizeNflLineups(b.players, b.settings);
  timed.push(Date.now() - t);
  assert.ok(r.lineups.length > 0, `arm ${arm.arm} built`);
}
console.log(`Classic arm build times (40 lineups): ${timed.map(ms => `${(ms / 1000).toFixed(1)}s`).join(", ")}`);

// ---- Showdown arm ---------------------------------------------------------
// The owner's real week-6 pregame snapshot (the week-4 archives predate enough games for the observed-history rule).
const sdArchive = JSON.parse(gunzipSync(readFileSync("../artifacts/nfl-ghost-factorial-v1/fixtures/2026-w6-SEA_at_DEN.json.gz")).toString());
const sdFrozen: Raw[] = sdArchive.players.map((p: Raw) => { const q = { ...p }; delete q.actual; delete q.actualSource; return q; });
const sdHouse = houseSettings("showdown", { favoriteTeam: sdArchive.favoriteTeam, underdogTeam: sdArchive.underdogTeam });
const sdDesign = ghostDesign("showdown");
const sh = buildArm("showdown", sdFrozen, sdHouse, sdDesign.arms[0].levels);
assert.equal(sh.settings.excludeKickers, true); assert.equal(sh.settings.forbidFourTwoSplit, true);
assert.equal(sh.settings.maxCaptainExposure, 0.3); assert.equal(sh.settings.archetypeMode, "standard");
const kick = sdDesign.arms.find(a => a.levels.kickers === "allowed" && a.levels.captainPositions === "any")!;
const sk = buildArm("showdown", sdFrozen, sdHouse, kick.levels);
assert.equal(sk.settings.excludeKickers, false); assert.equal(sk.settings.captainPositions, undefined);
const st = Date.now();
const sr = optimizeNflLineups(sh.players, sh.settings);
if (!sr.lineups.length) console.log("showdown warnings:", sr.warnings.slice(0, 8));
assert.ok(sr.lineups.length > 0);
console.log(`Showdown house build (20 lineups): ${((Date.now() - st) / 1000).toFixed(1)}s, ${sr.lineups.length} built`);
// The freeze waits for the pregame prop capture, then freezes regardless at the deadline.
assert.equal(freezeTiming(100, false), "wait");
assert.equal(freezeTiming(68, false), "wait");
assert.equal(freezeTiming(68, true), "freeze");
assert.equal(freezeTiming(100, true), "freeze");
assert.equal(freezeTiming(FRESH_LINES_DEADLINE_MINUTES, false), "freeze_stale");
assert.equal(freezeTiming(25, false), "freeze_stale");
assert.ok(FRESH_LINES_MINUTES >= 75, "fresh-lines window must cover the 60-75 minute pregame capture");

console.log("Ghost factorial: pinned designs, balance and orthogonality, projection reverts, system slate, arm settings and real builds passed.");
