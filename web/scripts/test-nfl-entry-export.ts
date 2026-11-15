/**
 * Entry-file export (2026-11-14): lineup types spread across contests, no
 * entry left blank, single-entry contests get the run's top lineup.
 */
import assert from "node:assert/strict";
import type { NflGeneratedLineup, NflOptimizerPlayer } from "../src/app/dfs/nfl/nfl-optimizer";
import { describeNflDkEntryFile, entryAssignmentNote, exportNflDkEntries, isSingleEntryContest, keepBestLineups, lineupShapeKey, planNflDkEntries, topNflLineup } from "../src/lib/nfl-dfs/entry-export";
import { runNflPreExportQa } from "../src/lib/nfl-dfs/pre-export-qa";

const player = (id: number): NflOptimizerPlayer => ({ id, dkPlayerId: id, captainDkPlayerId: null, name: `P${id}`, position: "RB", team: "CAR", opponent: "GB",
  gameKey: "CAR@GB", salary: 0, captainSalary: null, isOut: false, projectionStatus: "historical", ourProj: 10, floorFpts: 5, ceilingFpts: 20, boomRate: null,
  avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null });

/** A Single Stat lineup (three FLEX) is the simplest legal roster; the assignment logic does not depend on the format. */
function lineup(n: number, archetype: string, projected = 100 - n, ownership: number | null = 50): NflGeneratedLineup {
  const ids = [3 * n + 1, 3 * n + 2, 3 * n + 3];
  return { lineupNumber: n, playerIds: ids, totalSalary: 0, projectedFpts: projected, floorFpts: 0, ceilingFpts: 0, projectedOwnership: ownership,
    stackSummary: { quarterback: null, passCatchers: [], bringBack: null },
    archetype: { id: archetype, label: archetype, fadedPlayerIds: [], fadedPlayerNames: [], beneficiariesSatisfied: [] },
    slots: ids.map((id) => ({ slot: "FLEX", player: player(id), salary: 0, multiplier: 1, projection: 10, projectionSource: "our" as const })) } as NflGeneratedLineup;
}

const HEADER = "Entry ID,Contest Name,Contest ID,Entry Fee,FLEX,FLEX,FLEX";
function file(rows: Array<[contest: string, id: string, cells?: string]>): string {
  return [HEADER, ...rows.map(([name, cid, cells], i) => `${9000 + i},${name},${cid},$1,${cells ?? ",,"}`)].join("\n") + "\n";
}
/** Entry ID -> lineup number written into that row (null when the row holds something else). */
function written(csv: string): Map<string, number | null> {
  const out = new Map<string, number | null>();
  for (const line of csv.trim().split("\r\n").slice(1)) {
    const cells = line.split(",");
    const m = /\((\d+)\)/.exec(cells[4] ?? "");
    out.set(cells[0], m ? (Number(m[1]) - 1) / 3 : null);
  }
  return out;
}

// --- Single-entry detection ---
for (const name of ["NFL $5 Single Entry", "NFL Showdown [Single Entry] $3", "NFL $10 SE Classic", "NFL $3 Single-Entry"]) assert.ok(isSingleEntryContest(name), name);
for (const name of ["NFL $20 Millionaire [150 Entry Max]", "NFL Showdown $5 SEA vs NE", "NFL $1 3-Entry Max", "Seattle Special"]) assert.ok(!isSingleEntryContest(name), name);

// --- Fix 1: each contest gets a proportional mix of lineup types ---
{
  // Optimizer order: 6 standard builds first, then 2 fades, then 2 onslaughts (grouped, as it returns them).
  const lineups = [0, 1, 2, 3, 4, 5].map((n) => lineup(n, "standard_ceiling")).concat([6, 7].map((n) => lineup(n, "single_chalk_fade")), [8, 9].map((n) => lineup(n, "favorite_onslaught")));
  const rows: Array<[string, string]> = [...Array(5)].map(() => ["NFL $0.50 mini-MAX", "101"] as [string, string])
    .concat([...Array(5)].map(() => ["NFL $3 Flea Flicker", "202"] as [string, string]));
  const csv = exportNflDkEntries(file(rows), lineups);
  const got = written(csv);
  const used = [...got.values()];
  assert.equal(new Set(used).size, 10, "each lineup used once");
  assert.ok(used.every((n) => n != null), "every row filled");
  const contestTypes = (ids: string[]) => ids.map((id) => lineups[got.get(id)!].archetype!.id);
  const first = contestTypes(["9000", "9001", "9002", "9003", "9004"]);
  const second = contestTypes(["9005", "9006", "9007", "9008", "9009"]);
  for (const types of [first, second]) {
    assert.equal(types.filter((t) => t === "standard_ceiling").length, 3, `${types}`);
    assert.equal(types.filter((t) => t === "single_chalk_fade").length, 1, `${types}`);
    assert.equal(types.filter((t) => t === "favorite_onslaught").length, 1, `${types}`);
  }
  assert.equal(exportNflDkEntries(file(rows), lineups), csv, "deterministic");
  // Uneven contests: 7 rows and 3 rows still get a share of the rare type.
  const uneven = [...Array(7)].map(() => ["NFL A", "1"] as [string, string]).concat([...Array(3)].map(() => ["NFL B", "2"] as [string, string]));
  const g2 = written(exportNflDkEntries(file(uneven), lineups));
  const bTypes = ["9007", "9008", "9009"].map((id) => lineups[g2.get(id)!].archetype!.id);
  assert.ok(bTypes.some((t) => t !== "standard_ceiling"), `small contest gets a non-standard build: ${bTypes}`);
  // A file with no contest columns (older test fixtures) is one contest.
  const plain = `Entry ID,Entry Fee,FLEX,FLEX,FLEX\n1,$1,,,\n2,$1,,,\n`;
  assert.equal(written(exportNflDkEntries(plain, [lineup(0, "a"), lineup(1, "b")])).size, 2);
}

// --- Fix 2: never leave an entry blank ---
{
  const rows = [...Array(4)].map(() => ["NFL $1 Contest", "55"] as [string, string]);
  const three = [0, 1, 2].map((n) => lineup(n, "standard_ceiling"));
  assert.doesNotThrow(() => exportNflDkEntries(file(rows), three), "fewer lineups than entries is written anyway (owner, 2026-11-15)");
  assert.match(planNflDkEntries(file(rows), three).note, /The entry file has 4 entries for 3 lineups\..*written anyway/);
  const five = [0, 1, 2, 3, 4].map((n) => lineup(n, "standard_ceiling"));
  assert.match(planNflDkEntries(file(rows), five).note, /room for 4 lineups.*the 1 lowest-projected lineup was left out/);
  const shape = describeNflDkEntryFile(file(rows));
  const qa = runNflPreExportQa({ format: "showdown", requestedLineups: 3, lineups: three.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: 1, slots: [] })),
    entryFile: { shape, note: entryAssignmentNote(shape, three) } });
  const check = qa.checks.find((c) => c.id === "entry_rows")!;
  assert.equal(check.severity, "warning");
  assert.equal(check.overridable, false, "a blank entry cannot be overridden");
  assert.equal(check.detail, "The entry file has 4 entries for 3 lineups. Build 4 lineups, or remove entries from the file.");
  const overridden = runNflPreExportQa({ format: "showdown", requestedLineups: 3, lineups: [], entryFile: { shape, note: "" } },
    [{ checkId: "entry_rows", reason: "x", user: "u", at: "now", rulesetVersion: "v", runId: "r" }]);
  assert.ok(overridden.checks.some((c) => c.id === "entry_rows" && !c.passed && c.severity === "warning"), "override does not clear it");
  // A row that already holds a lineup is not blank: it keeps its lineup, and empty rows are filled first.
  const prefilled = file([["NFL $1 Contest", "55", "Old A,Old B,Old C"], ["NFL $1 Contest", "55"], ["NFL $1 Contest", "55"], ["NFL $1 Contest", "55"]]);
  const got = written(exportNflDkEntries(prefilled, three));
  assert.equal(got.get("9000"), null, "the prefilled row keeps its lineup");
  assert.deepEqual(["9001", "9002", "9003"].map((id) => got.get(id) != null), [true, true, true]);
  const pshape = describeNflDkEntryFile(prefilled);
  const pcheck = runNflPreExportQa({ format: "showdown", requestedLineups: 3, lineups: [], entryFile: { shape: pshape, note: "" } }).checks.find((c) => c.id === "entry_rows")!;
  // built=0 here: 3 empty rows and no lineups is still a blocker.
  assert.equal(pcheck.severity, "warning");
}

// --- Fix 3: single-entry contests get the top lineup and use none from the pool ---
{
  const lineups = [lineup(0, "standard_ceiling", 150, 80), lineup(1, "standard_ceiling", 160, 60), lineup(2, "single_chalk_fade", 160, 90), lineup(3, "double_fade", 120, 10)];
  assert.equal(topNflLineup(lineups)!.lineupNumber, 2, "most projected points, then highest ownership");
  const rows: Array<[string, string]> = [["NFL $20 Millionaire [150 Entry Max]", "1"], ["NFL $20 Millionaire [150 Entry Max]", "1"],
    ["NFL $5 Single Entry", "2"], ["NFL $3 Flea Flicker", "3"], ["NFL $3 Flea Flicker", "3"], ["NFL $10 [Single Entry] Classic", "4"]];
  const plan = planNflDkEntries(file(rows), lineups);
  const got = written(plan.csv);
  assert.equal(got.get("9002"), 2);
  assert.equal(got.get("9005"), 2, "the same top lineup may go into several single-entry contests");
  const multi = ["9000", "9001", "9003", "9004"].map((id) => got.get(id));
  assert.deepEqual([...multi].sort(), [0, 1, 2, 3], "multi-entry rows each get their own lineup, the top one included");
  assert.match(plan.note, /Each of the 2 contests gets a mix of the 3 lineup types/);
  assert.match(plan.note, /2 single-entry contests get your top lineup \(#2, 160\.0 projected\)/);
  // Single-entry rows do not count against the lineups: 4 multi rows + 2 SE rows with 4 lineups is complete.
  const shape = describeNflDkEntryFile(file(rows));
  assert.deepEqual([shape.rows, shape.singleEntryRows, shape.multiEntryRows, shape.emptyMultiEntryRows], [6, 2, 4, 4]);
  const check = runNflPreExportQa({ format: "showdown", requestedLineups: 4, lineups: lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: 1, slots: [] })),
    entryFile: { shape, note: plan.note } }).checks.find((c) => c.id === "entry_rows")!;
  assert.equal(check.passed, true);
  assert.match(check.detail, /All 6 entries in the file get a lineup\. Each of the 2 contests/);
}


// --- 2026-11-15: constructions spread over contests; with more lineups than entries the weakest of each type go ---
{
  const sdShape = (teams: string[]) => lineupShapeKey({ slots: teams.map((team, i) => ({ slot: i === 0 ? "CPT" : "FLEX", player: { team } })), stackSummary: { quarterback: null, passCatchers: [], bringBack: null } } as never);
  const split = (heavy: string, other: string, k: number) => [...Array(k).fill(heavy), ...Array(6 - k).fill(other)];
  assert.equal(sdShape(split("DET", "NE", 4)), "4-2 DET");
  assert.equal(sdShape(split("NE", "DET", 5)), "5-1 NE");
  assert.equal(sdShape(split("NE", "DET", 3)), "3-3");
  const stacked = (n: number, catchers: number, bring: boolean, type = "standard_ceiling", projected = 100) => ({ ...lineup(n, type, projected),
    stackSummary: { quarterback: "QB", passCatchers: Array(catchers).fill("WR"), bringBack: bring ? "Opp" : null } }) as NflGeneratedLineup;
  assert.equal(lineupShapeKey(stacked(0, 2, true)), "QB+2 bring-back");
  assert.equal(lineupShapeKey(stacked(0, 1, false)), "QB+1");
  assert.equal(lineupShapeKey(lineup(0, "standard_ceiling")), "no stack");

  // One lineup type, three constructions in blocks (as a build returns them): 8, 4 and 4, into two contests of 8.
  const run = [...Array(8).keys()].map((n) => stacked(n, 1, true))
    .concat([...Array(4).keys()].map((n) => stacked(8 + n, 2, false)), [...Array(4).keys()].map((n) => stacked(12 + n, 2, true)));
  const rows = [...Array(16).keys()].map((i): [string, string] => [i < 8 ? "Contest A" : "Contest B", i < 8 ? "1" : "2"]);
  const plan = planNflDkEntries(file(rows), run);
  const perContest: Record<string, Record<string, number>> = { "Contest A": {}, "Contest B": {} };
  const csvRows = plan.csv.trim().split("\r\n").slice(1);
  for (const [entry, n] of written(plan.csv)) {
    const contest = csvRows.find((line) => line.startsWith(`${entry},`))!.split(",")[1];
    const shape = lineupShapeKey(run[n!]);
    perContest[contest][shape] = (perContest[contest][shape] ?? 0) + 1;
  }
  const even = { "QB+1 bring-back": 4, "QB+2": 2, "QB+2 bring-back": 2 };
  assert.deepEqual(perContest["Contest A"], even, JSON.stringify(perContest));
  assert.deepEqual(perContest["Contest B"], even, JSON.stringify(perContest));
  assert.match(plan.note, /and of the 3 constructions \(stack shapes\)/);

  // Ranked cut: 6 standard + 4 fades, room for 5: 3 standard and 2 fades stay, the highest-projected of each.
  const mixed = [...Array(6).keys()].map((n) => lineup(n, "standard_ceiling", 100 + n)).concat([...Array(4).keys()].map((n) => lineup(6 + n, "single_chalk_fade", 90 + n)));
  assert.deepEqual(keepBestLineups(mixed, 5).map((l) => l.lineupNumber), [3, 4, 5, 8, 9], "top 3 of the standard builds and top 2 of the fades, in run order");
  assert.equal(keepBestLineups(mixed, 10).length, 10);
  assert.equal(keepBestLineups(mixed, 0).length, 0);
  // Through the file: 10 lineups, 5 entries, the five kept are the ones written.
  const cut = planNflDkEntries(file([...Array(5).keys()].map((): [string, string] => ["Contest A", "1"])), mixed);
  assert.deepEqual([...written(cut.csv).values()].sort((a, b) => a! - b!), [3, 4, 5, 8, 9]);
  assert.match(cut.note, /the 5 lowest-projected lineups were left out, keeping each lineup type's share/);
}

console.log("nfl entry export: all assertions passed");
