import assert from "node:assert/strict";
import {
  capFormatOf, describeNflFormat, detectSingleStat, isSingleStatFormat, NflDkCsvError, parseNflDkSalaryCsv,
  SINGLE_STAT_ROSTER_SIZE, singleStatOf,
} from "../src/lib/nfl-dfs/dk-salary-csv";
import { generateNflCandidates, nflSlots, validateNflLineup } from "../src/lib/nfl-dfs/lineups";
import { countNflDkEntryRows, exportNflDkEntries, exportNflDkLineups, NFL_DK_LINEUP_HEADERS } from "../src/lib/nfl-dfs/entry-export";
import {
  DEFAULT_SINGLE_STAT_SETTINGS, optimizeSingleStat, projectSingleStat, projectSingleStatOwnership,
  SINGLE_STAT_MAX_OWN_PCT, SINGLE_STAT_OWNERSHIP_BUDGET, statFromLine, type SingleStatPlayer,
} from "../src/lib/nfl-dfs/single-stat";
import type { NflGeneratedLineup, NflOptimizerPlayer } from "../src/app/dfs/nfl/nfl-optimizer";

// Column layout verbatim from DraftKings' Single Stat exports (2026-10-29):
// no Salary column, every row FLEX.
const HEADER = "﻿Position,Name + ID,Name,ID,Roster Position,Game Info,TeamAbbrev,AvgPointsPerGame,Status";
const csv = (rows: string[]) => [HEADER, ...rows].join("\n");

// ── Parsing ──────────────────────────────────────────────────────────────

// A one-game Total Yards file, detected from the averages alone.
{
  const slate = parseNflDkSalaryCsv(csv([
    "WR,Christian Watson (1),Christian Watson,1,FLEX,CAR@GB 10/29/2026 08:15PM ET,GB,85,",
    "RB,Chuba Hubbard (2),Chuba Hubbard,2,FLEX,CAR@GB 10/29/2026 08:15PM ET,CAR,95.17,",
    "TE,Tucker Kraft (3),Tucker Kraft,3,FLEX,CAR@GB 10/29/2026 08:15PM ET,GB,43.14,",
    "RB,Jonathon Brooks (4),Jonathon Brooks,4,FLEX,CAR@GB 10/29/2026 08:15PM ET,CAR,36.5,IR",
  ]));
  assert.equal(slate.format, "single_stat_yards");
  assert.equal(singleStatOf(slate.format), "yards");
  assert.equal(isSingleStatFormat(slate.format), true);
  assert.deepEqual(slate.games, ["CAR@GB"]);
  assert.equal(slate.players.length, 4);
  assert.equal(slate.players.every((p) => p.salary === 0 && p.captain === null), true, "no salaries, no captain purchases");
  assert.equal(slate.players.find((p) => p.name === "Jonathon Brooks")!.isOut, true);
  assert.equal(slate.players.find((p) => p.name === "Chuba Hubbard")!.opponent, "GB");
  assert.equal(slate.warnings.some((w) => /Only 1 game/.test(w)), false, "the Classic 2-game rule does not apply");
  assert.deepEqual(nflSlots(slate.format), ["FLEX", "FLEX", "FLEX"]);
  assert.equal(capFormatOf(slate.format, slate.games), "showdown", "one game: the live-pool key is the single-game one");
  assert.equal(describeNflFormat(slate.format, slate.games), "Total Yards · CAR@GB");
}

// A multi-game Touchdowns file, detected from the averages; the Classic
// positions outside RB/WR/TE are only a warning.
{
  const slate = parseNflDkSalaryCsv(csv([
    "RB,Jahmyr Gibbs (1),Jahmyr Gibbs,1,FLEX,MIN@DET 11/01/2026 01:00PM ET,DET,1.5,Q",
    "WR,Puka Nacua (2),Puka Nacua,2,FLEX,LAC@LAR 11/01/2026 04:05PM ET,LAR,0.6,",
    "TE,Brock Bowers (3),Brock Bowers,3,FLEX,LV@NYJ 11/01/2026 01:00PM ET,LV,0.6,Q",
  ]));
  assert.equal(slate.format, "single_stat_touchdowns");
  assert.equal(capFormatOf(slate.format, slate.games), "classic");
  assert.equal(describeNflFormat(slate.format, slate.games), "Touchdowns · 3 games");
}

// The file name decides when the averages are silent, and a conflict is an error.
assert.equal(detectSingleStat([0, 0, null], "DKSalaries_Single Stat Touchdowns.csv"), "touchdowns");
assert.equal(detectSingleStat([null], "DKSalaries_Total Yards_Showdown.csv"), "yards");
assert.equal(detectSingleStat([85, 12.5], "DKSalaries_Total Yards_Showdown.csv"), "yards");
assert.throws(() => detectSingleStat([85, 12.5], "DKSalaries_Single Stat Touchdowns.csv"), /file name says Touchdowns/);
assert.throws(() => detectSingleStat([0, 0], "DKSalaries.csv"), NflDkCsvError);

// A no-salary file that is NOT all-FLEX is refused rather than guessed.
assert.throws(() => parseNflDkSalaryCsv(csv([
  "QB,A B (1),A B,1,QB,KC@BUF 09/07/2026 08:20PM ET,KC,20,",
])), /not a DraftKings Single Stat export/);

// A salary-cap file still needs its salaries.
{
  const slate = parseNflDkSalaryCsv([
    "Position,Name + ID,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame",
    "QB,A B (1),A B,1,QB,7000,KC@BUF 09/07/2026 08:20PM ET,KC,20",
    "RB,C D (2),C D,2,RB/FLEX,,DAL@PHI 09/07/2026 04:25PM ET,DAL,15",
  ].join("\n"));
  assert.equal(slate.format, "classic");
  assert.equal(slate.players.length, 1);
  assert.ok(slate.warnings.some((w) => /Missing or zero salary/.test(w)));
}

// ── Lineup rules ─────────────────────────────────────────────────────────

const pool = parseNflDkSalaryCsv(csv([
  "RB,Chuba Hubbard (1),Chuba Hubbard,1,FLEX,CAR@GB 10/29/2026 08:15PM ET,CAR,95.17,",
  "RB,Josh Jacobs (2),Josh Jacobs,2,FLEX,CAR@GB 10/29/2026 08:15PM ET,GB,91,",
  "WR,Tetairoa McMillan (3),Tetairoa McMillan,3,FLEX,CAR@GB 10/29/2026 08:15PM ET,CAR,85.17,",
  "WR,Christian Watson (4),Christian Watson,4,FLEX,CAR@GB 10/29/2026 08:15PM ET,GB,85,",
  "TE,Tucker Kraft (5),Tucker Kraft,5,FLEX,CAR@GB 10/29/2026 08:15PM ET,GB,43.14,",
  "RB,Jonathon Brooks (6),Jonathon Brooks,6,FLEX,CAR@GB 10/29/2026 08:15PM ET,CAR,36.5,IR",
]));
{
  // Three players from ONE team is legal (no team or game rule), and the cap is moot.
  const { key, salary } = validateNflLineup(pool, [{ slot: "FLEX", playerId: 1 }, { slot: "FLEX", playerId: 3 }, { slot: "FLEX", playerId: 2 }]);
  assert.equal(salary, 0);
  assert.equal(key, "single_stat:1,2,3");
  assert.throws(() => validateNflLineup(pool, [{ slot: "FLEX", playerId: 1 }, { slot: "FLEX", playerId: 1 }, { slot: "FLEX", playerId: 2 }]), /Duplicate/);
  assert.throws(() => validateNflLineup(pool, [{ slot: "FLEX", playerId: 1 }, { slot: "FLEX", playerId: 2 }, { slot: "FLEX", playerId: 6 }]), /Ineligible/, "an IR player cannot be rostered");
  assert.throws(() => validateNflLineup(pool, [{ slot: "FLEX", playerId: 1 }, { slot: "FLEX", playerId: 2 }]), /Expected 3/);
  const candidates = generateNflCandidates(pool, { count: 5, seed: 7 });
  assert.equal(candidates.lineups.length, 5);
  assert.equal(new Set(candidates.lineups.map((l) => validateNflLineup(pool, l).key)).size, 5);
}

// ── Stat projection ──────────────────────────────────────────────────────

assert.equal(statFromLine("yards", { rushing_yards: 60.5, receiving_yards: 20 }), 80.5);
assert.equal(statFromLine("touchdowns", { rushing_tds: 0.4, receiving_tds: 0.2 }), 0.6000000000000001);
assert.equal(statFromLine("touchdowns", { scrimmage_tds: 0.55 }), 0.55, "the books' anchor carries one scrimmage figure");
assert.equal(statFromLine("yards", { receptions: 4 }), null, "no yardage fields means no projection, not zero");
{
  const base = { position: "RB", isOut: false, projectionStatus: "historical", ourProj: 15, floorFpts: 6, ceilingFpts: 27, avgFptsDk: 95.17 };
  const model = projectSingleStat({ ...base, statMeans: { rushing_yards: 70, receiving_yards: 18 } }, "yards");
  assert.equal(model.source, "model"); assert.equal(model.projected, 88); assert.equal(model.model, 88); assert.equal(model.market, null);
  assert.equal(model.p10, 88 * 6 / 15); assert.equal(model.p90, 88 * 27 / 15);
  const anchored = projectSingleStat({ ...base, statMeans: { rushing_yards: 70, receiving_yards: 18 },
    marketAnchor: { statLine: { rushing_yards: 61, receiving_yards: 14.5 } } }, "yards");
  assert.equal(anchored.source, "market"); assert.equal(anchored.projected, 75.5); assert.equal(anchored.model, 88, "the model's number stays visible");
  const scaled = projectSingleStat({ ...base, statMeans: { rushing_yards: 40, receiving_yards: 10 }, absenceCommit: { scale: 1.5 } }, "yards");
  assert.equal(scaled.projected, 75, "a starter-out scale applies to the model stat");
  assert.match(scaled.note!, /scaled ×1.50/);
  const out = projectSingleStat({ ...base, isOut: true, statMeans: { rushing_yards: 70 } }, "yards");
  assert.equal(out.projected, 0, "ruled out projects zero");
  const none = projectSingleStat({ ...base, statMeans: {} }, "yards");
  assert.equal(none.projected, null); assert.equal(none.source, "none");
  // A position-prior row carries the position's mean stat line; it is not his.
  const prior = projectSingleStat({ ...base, ourProj: null, projectionStatus: "position_prior", statMeans: { rushing_yards: 45 } }, "yards");
  assert.equal(prior.projected, null); assert.match(prior.note!, /position average/);
  const priorAnchored = projectSingleStat({ ...base, ourProj: null, projectionStatus: "position_prior", statMeans: { rushing_yards: 45 },
    marketAnchor: { statLine: { rushing_yards: 30, receiving_yards: 5 } } }, "yards");
  assert.equal(priorAnchored.projected, 35, "the books' line stands on its own");
}

// ── Ownership prior ──────────────────────────────────────────────────────
{
  const prior = projectSingleStatOwnership([
    { dkPlayerId: 1, isOut: false, dkAvg: 95, projected: 90 },
    { dkPlayerId: 2, isOut: false, dkAvg: 91, projected: 85 },
    { dkPlayerId: 3, isOut: false, dkAvg: 85, projected: 80 },
    { dkPlayerId: 4, isOut: false, dkAvg: 43, projected: 40 },
    { dkPlayerId: 5, isOut: false, dkAvg: 12, projected: 10 },
    { dkPlayerId: 6, isOut: true, dkAvg: 36, projected: 0 },
    { dkPlayerId: 7, isOut: false, dkAvg: null, projected: null },
    // A real pool has dozens of mid players; without them the 300% budget caps everyone.
    ...Array.from({ length: 12 }, (_, i) => ({ dkPlayerId: 10 + i, isOut: false, dkAvg: 15 + i * 2, projected: 14 + i * 2 })),
  ]);
  const total = [...prior.ownPct.values()].reduce((a, b) => a + b, 0);
  assert.ok(Math.abs(total + prior.unallocatedPct - SINGLE_STAT_OWNERSHIP_BUDGET) < 0.1, `sums to the budget (${total})`);
  assert.equal(prior.ownPct.get(6), 0, "out players draw nothing");
  assert.equal(prior.ownPct.get(7), 0, "nothing to draft on draws nothing");
  assert.ok(prior.ownPct.get(1)! <= SINGLE_STAT_MAX_OWN_PCT + 1e-9);
  assert.ok(prior.ownPct.get(1)! > prior.ownPct.get(4)! && prior.ownPct.get(4)! > prior.ownPct.get(5)!, "ordered by the field's number");
}

// ── Optimizer ────────────────────────────────────────────────────────────

const players: SingleStatPlayer[] = [
  { dkPlayerId: 1, name: "Hubbard", position: "RB", team: "CAR", opponent: "GB", gameKey: "CAR@GB", isOut: false, projected: 90, p10: 40, p90: 140, ownPct: 60, source: "market" },
  { dkPlayerId: 2, name: "Jacobs", position: "RB", team: "GB", opponent: "CAR", gameKey: "CAR@GB", isOut: false, projected: 85, p10: 38, p90: 130, ownPct: 55, source: "market" },
  { dkPlayerId: 3, name: "McMillan", position: "WR", team: "CAR", opponent: "GB", gameKey: "CAR@GB", isOut: false, projected: 80, p10: 30, p90: 125, ownPct: 50, source: "model" },
  { dkPlayerId: 4, name: "Watson", position: "WR", team: "GB", opponent: "CAR", gameKey: "CAR@GB", isOut: false, projected: 70, p10: 25, p90: 120, ownPct: 20, source: "model" },
  { dkPlayerId: 5, name: "Kraft", position: "TE", team: "GB", opponent: "CAR", gameKey: "CAR@GB", isOut: false, projected: 45, p10: 15, p90: 80, ownPct: 10, source: "model" },
  { dkPlayerId: 6, name: "Lloyd", position: "RB", team: "GB", opponent: "CAR", gameKey: "CAR@GB", isOut: false, projected: 30, p10: 10, p90: 60, ownPct: 5, source: "model" },
  { dkPlayerId: 7, name: "Brooks", position: "RB", team: "CAR", opponent: "GB", gameKey: "CAR@GB", isOut: true, projected: 0, p10: null, p90: null, ownPct: 0, source: "model" },
  { dkPlayerId: 8, name: "Nobody", position: "WR", team: "CAR", opponent: "GB", gameKey: "CAR@GB", isOut: false, projected: null, p10: null, p90: null, ownPct: null, source: "none" },
];
const settings = { ...DEFAULT_SINGLE_STAT_SETTINGS, lockedPlayerIds: [] as number[], excludedPlayerIds: [] as number[] };

// Cash: the top three by projection first, then the next-best lineups that
// differ by at least one player (min unique 1 means no exact repeats).
{
  const result = optimizeSingleStat(players, { ...settings, mode: "cash", nLineups: 3, maxExposure: 1, minUnique: 1 }, "yards");
  const ids = (i: number) => result.lineups[i].players.map((p) => p.dkPlayerId);
  assert.equal(result.lineups.length, 3);
  assert.deepEqual(ids(0), [1, 2, 3]);
  assert.equal(result.lineups[0].projected, 255);
  assert.equal(result.lineups[0].floor, 108); assert.equal(result.lineups[0].ceiling, 395);
  assert.equal(result.lineups[0].ownershipSum, 165);
  assert.deepEqual(ids(1), [1, 2, 4]); assert.deepEqual(ids(2), [1, 3, 4]);
  assert.equal(result.leverageApplied, false);
  assert.deepEqual(result.warnings, []);
  assert.deepEqual(result.excluded.map((e) => e.dkPlayerId).sort(), [7, 8]);
  const one = optimizeSingleStat(players, { ...settings, mode: "cash", nLineups: 3, maxExposure: 1, minUnique: 3 }, "yards");
  assert.equal(one.lineups.length, 2, "min unique 3 on a six-man pool: exactly two disjoint trios exist, then it stops");
  assert.deepEqual(one.lineups[1].players.map((p) => p.dkPlayerId), [4, 5, 6]);
  assert.ok(one.warnings.some((w) => /Stopped at/.test(w)));
}

// GPP: leverage changes the choice; ownership-unknown is never read as low.
{
  const result = optimizeSingleStat(players, { ...settings, mode: "gpp", nLineups: 1, randomness: 0, leverageExponent: 1 }, "yards");
  assert.equal(result.leverageApplied, true);
  // 90×.4=36, 85×.45=38.25, 80×.5=40, 70×.8=56, 45×.9=40.5 → Watson, Kraft, McMillan.
  assert.deepEqual(result.lineups[0].players.map((p) => p.name), ["McMillan", "Watson", "Kraft"]);
  const off = optimizeSingleStat(players, { ...settings, mode: "gpp", nLineups: 1, randomness: 0, useOwnershipLeverage: false }, "yards");
  assert.deepEqual(off.lineups[0].players.map((p) => p.dkPlayerId), [1, 2, 3]);
  assert.equal(off.leverageApplied, false);
}

// Exposure cap, min unique, locks and excludes hold; the same seed reproduces the run.
{
  const run = () => optimizeSingleStat(players, { ...settings, mode: "gpp", nLineups: 6, maxExposure: 0.5, minUnique: 2, randomness: 0.1, seed: 11,
    lockedPlayerIds: [5], excludedPlayerIds: [1] }, "yards");
  const a = run(), b = run();
  assert.deepEqual(a.lineups.map((l) => l.players.map((p) => p.dkPlayerId)), b.lineups.map((l) => l.players.map((p) => p.dkPlayerId)), "deterministic");
  assert.ok(a.lineups.length >= 2);
  const counts = new Map<number, number>();
  for (const l of a.lineups) {
    assert.equal(l.players.length, SINGLE_STAT_ROSTER_SIZE);
    assert.ok(l.players.some((p) => p.dkPlayerId === 5), "the lock is in every lineup");
    assert.ok(!l.players.some((p) => p.dkPlayerId === 1), "the exclude is in none");
    for (const p of l.players) counts.set(p.dkPlayerId, (counts.get(p.dkPlayerId) ?? 0) + 1);
  }
  for (const [id, n] of counts) if (id !== 5) assert.ok(n <= Math.floor(0.5 * 6), `${id} appears ${n} times under a 50% cap`);
  for (let i = 0; i < a.lineups.length; i += 1) for (let j = i + 1; j < a.lineups.length; j += 1) {
    const x = new Set(a.lineups[i].players.map((p) => p.dkPlayerId));
    const differ = a.lineups[j].players.filter((p) => !x.has(p.dkPlayerId)).length;
    assert.ok(differ >= 2, `lineups ${i + 1} and ${j + 1} share too many players`);
  }
  assert.throws(() => optimizeSingleStat(players, { ...settings, lockedPlayerIds: [7] }, "yards"), /ruled out/);
  assert.throws(() => optimizeSingleStat(players, { ...settings, lockedPlayerIds: [8] }, "yards"), /no total yards projection/);
  assert.throws(() => optimizeSingleStat(players, { ...settings, lockedPlayerIds: [2], excludedPlayerIds: [2] }, "yards"), /both locked and excluded/);
  assert.throws(() => optimizeSingleStat(players, { ...settings, minUnique: 4 }, "yards"), /Min unique/);
}

// ── Export ───────────────────────────────────────────────────────────────
{
  assert.deepEqual(NFL_DK_LINEUP_HEADERS.single_stat_yards, ["FLEX", "FLEX", "FLEX"]);
  const player = (id: number, name: string): NflOptimizerPlayer => ({ id, dkPlayerId: id, captainDkPlayerId: null, name, position: "RB", team: "CAR", opponent: "GB",
    gameKey: "CAR@GB", salary: 0, captainSalary: null, isOut: false, projectionStatus: "historical", ourProj: 80, floorFpts: 40, ceilingFpts: 120, boomRate: null,
    avgFptsDk: 90, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null });
  const lineup: NflGeneratedLineup = { lineupNumber: 1, playerIds: [1, 2, 3], totalSalary: 0, projectedFpts: 240, floorFpts: 120, ceilingFpts: 360, projectedOwnership: null,
    stackSummary: { quarterback: null, passCatchers: [], bringBack: null },
    slots: [player(1, "Chuba Hubbard"), player(2, "Josh Jacobs"), player(3, "Tetairoa McMillan")].map((p) => ({ slot: "FLEX", player: p, salary: 0, multiplier: 1, projection: 80, projectionSource: "our" as const })) };
  const rosters = exportNflDkLineups("single_stat_yards", [lineup]);
  assert.equal(rosters, "FLEX,FLEX,FLEX\r\nChuba Hubbard (1),Josh Jacobs (2),Tetairoa McMillan (3)\r\n");
  const template = "Entry ID,Contest Name,Contest ID,Entry Fee,FLEX,FLEX,FLEX\n4001,Single Stat $1,55,$1,,,\n4002,Single Stat $1,55,$1,,,\n";
  assert.equal(countNflDkEntryRows(template), 2);
  const entries = exportNflDkEntries(template, [lineup]);
  assert.match(entries, /4001,Single Stat \$1,55,\$1,Chuba Hubbard \(1\),Josh Jacobs \(2\),Tetairoa McMillan \(3\)/);
  assert.match(entries, /4002,Single Stat \$1,55,\$1,,,/, "an entry with no lineup is left untouched");
  assert.throws(() => exportNflDkEntries("Entry ID,Contest Name,Contest ID,Entry Fee,FLEX,FLEX\n1,x,1,$1,,\n", [lineup]), /has 2 FLEX columns/);
  const repeated = { ...lineup, slots: [lineup.slots[0], lineup.slots[0], lineup.slots[1]] };
  assert.throws(() => exportNflDkLineups("single_stat_yards", [repeated]), /repeats a player/);
}

console.log("nfl single stat: all assertions passed");
