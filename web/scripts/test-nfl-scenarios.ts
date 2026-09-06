import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, readdirSync, rmdirSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { compareNflCandidates, nflDigest } from "../src/lib/nfl-dfs/experiment";
import { generateNflCandidates, validateNflLineup } from "../src/lib/nfl-dfs/lineups";
import { emptyNflStats, independentNflAblation, prepareNflScenarios, scoreNflLineupDraws,
  summarizeNflDraws, validateNflScenarioStats } from "../src/lib/nfl-dfs/scenarios";
import { nflDemoBank, nflDemoSlate } from "./fixtures/nfl-scenario-demo";

const close = (a: number, b: number) => assert.ok(Math.abs(a - b) < 1e-9, `${a} != ${b}`);

// Weighted inverse-CDF quantiles, inclusive score target, and no fake precision at zero hits.
{
  const summary = summarizeNflDraws([-4, 10, 30], [.2, .6, .2], 10);
  close(summary.mean, 11.2); close(summary.p10, -4); close(summary.p50, 10); close(summary.p90, 30);
  close(summary.targetProbability, .8); assert.equal(summary.monteCarlo95, null);
  const zeroHits = summarizeNflDraws([0, 0, 0, 0], [1, 1, 1, 1], 1, true);
  assert.ok(zeroHits.monteCarlo95![1] > 0);
  assert.throws(() => summarizeNflDraws([1, 2], [1, 2], 1, true), /equal weights/);
  assert.throws(() => summarizeNflDraws([NaN, 1], [1, 1], 1), /Invalid/);
}

for (const format of ["classic", "showdown"] as const) {
  const slate = nflDemoSlate(format);
  const search = generateNflCandidates(slate, { count: 100, seed: 42 });
  assert.equal(search.status, "complete");
  assert.deepEqual(search, generateNflCandidates(slate, { count: 100, seed: 42 }));
  assert.equal(new Set(search.lineups.map((l) => validateNflLineup(slate, l).key)).size, 100);
  const first = structuredClone(search.lineups[0]);
  const restricted = structuredClone(first);
  const assigned = new Set<number>();
  for (const entry of restricted) {
    const player = slate.players.find((p) => !assigned.has(p.dkPlayerId)
      && (format === "showdown" ? p.teamAbbrev === slate.teams[0] : p.gameKey === slate.games[0])
      && (format === "showdown" || (entry.slot === "FLEX" ? ["RB", "WR", "TE"].includes(p.position) : p.position === entry.slot)))!;
    entry.playerId = player.dkPlayerId; assigned.add(player.dkPlayerId);
  }
  assert.throws(() => validateNflLineup(slate, restricted), format === "classic" ? /two games/ : /both teams/);
  const duplicate = structuredClone(first);
  duplicate[1].playerId = duplicate[0].playerId;
  assert.throws(() => validateNflLineup(slate, duplicate), /Duplicate/);
  const missing = structuredClone(first); missing[0].playerId = -1;
  assert.throws(() => validateNflLineup(slate, missing), /Unknown/);
  const outSlate = structuredClone(slate);
  outSlate.players.find((p) => p.dkPlayerId === first[0].playerId)!.isOut = true;
  assert.throws(() => validateNflLineup(outSlate, first), /Ineligible/);
  const expensive = structuredClone(slate);
  expensive.players.forEach((p) => { p.salary = 50_000; if (p.captain) p.captain.salary = 50_000; });
  assert.throws(() => validateNflLineup(expensive, first), /salary cap/);
  const short = generateNflCandidates(expensive, { count: 100, seed: 42, maxNodes: 20 });
  assert.equal(short.status, "search-limit"); assert.equal(short.lineups.length, 0); assert.ok(short.nodes <= 20);

  const selection = nflDemoBank(slate, 100, 80, "selection");
  const evaluation = nflDemoBank(slate, 200, 80, "evaluation");
  assert.deepEqual(selection, nflDemoBank(slate, 100, 80, "selection"));
  const prepared = prepareNflScenarios(slate, selection);
  const ablated = independentNflAblation(prepared, 13);
  for (const id of prepared.playerIds) assert.deepEqual([...ablated.scores[id]].sort((a, b) => a - b), [...prepared.scores[id]].sort((a, b) => a - b));
  assert.notEqual(nflDigest(prepared.scores), nflDigest(ablated.scores));
  const report = compareNflCandidates({ slate, candidates: search.lineups, selection, evaluation, target: 100, ablationSeed: 14 });
  assert.deepEqual(report, compareNflCandidates({ slate, candidates: search.lineups, selection, evaluation, target: 100, ablationSeed: 14 }));
  assert.equal(report.candidates.length, 100);
  const changedEvaluation = structuredClone(evaluation);
  for (const scenario of changedEvaluation.scenarios) {
    for (const p of slate.players.filter((p) => p.position === "QB")) scenario.stats[p.dkPlayerId].passYds = 999;
  }
  // Deliberately invalid football accounting here isolates the scorer's no-evaluation-selection-leak contract.
  const changedReport = compareNflCandidates({ slate, candidates: search.lineups, selection, evaluation: changedEvaluation, target: 100, ablationSeed: 14 });
  assert.deepEqual(report.selected.map((r) => r.candidateKey), changedReport.selected.map((r) => r.candidateKey));
  assert.notDeepEqual(report.selected.map((r) => r.evaluation), changedReport.selected.map((r) => r.evaluation));
  assert.throws(() => compareNflCandidates({ slate, candidates: search.lineups, selection, evaluation: selection, target: 100, ablationSeed: 14 }), /separate/);
  assert.throws(() => compareNflCandidates({ slate, candidates: [first, first], selection, evaluation, target: 100, ablationSeed: 14 }), /Duplicate canonical/);
  const bad = structuredClone(selection);
  delete bad.scenarios[0].stats[slate.players[0].dkPlayerId];
  assert.throws(() => prepareNflScenarios(slate, bad), /complete underlying player pool/);
  const duplicateDraw = structuredClone(selection);
  duplicateDraw.scenarios[1].id = duplicateDraw.scenarios[0].id;
  assert.throws(() => prepareNflScenarios(slate, duplicateDraw), /Duplicate scenario/);
  const badDst = structuredClone(selection);
  delete badDst.scenarios[0].stats[slate.players.find((p) => p.position === "DST")!.dkPlayerId].pointsAllowed;
  assert.throws(() => prepareNflScenarios(slate, badDst), /exactly/);
  const leaked = { ...selection, inputsCapturedAt: "2026-09-06T00:00:00Z" };
  assert.throws(() => prepareNflScenarios(slate, leaked), /after decision/);
  const weighted = structuredClone(selection); weighted.sampling = "weighted"; weighted.scenarios[0].weight = 3;
  assert.throws(() => independentNflAblation(prepareNflScenarios(slate, weighted), 42), /equal weights/);

  // Synthetic game events reconcile passing/receiving totals, interceptions, and (Showdown) points allowed.
  for (const scenario of selection.scenarios) {
    for (const team of slate.teams) {
      const players = slate.players.filter((p) => p.teamAbbrev === team);
      const qb = scenario.stats[players.find((p) => p.position === "QB")!.dkPlayerId];
      close(qb.passYds!, players.reduce((sum, p) => sum + (scenario.stats[p.dkPlayerId].recYds ?? 0), 0));
      close(qb.passTds!, players.reduce((sum, p) => sum + (scenario.stats[p.dkPlayerId].recTds ?? 0), 0));
      const opponent = players[0].opponent;
      const opponentDst = slate.players.find((p) => p.teamAbbrev === opponent && p.position === "DST")!;
      close(qb.interceptions!, scenario.stats[opponentDst.dkPlayerId].dstInterceptions!);
      if (format === "showdown") {
        const k = scenario.stats[players.find((p) => p.position === "K")!.dkPlayerId];
        const offensiveTds = players.reduce((sum, p) => sum + (scenario.stats[p.dkPlayerId].rushTds ?? 0) + (scenario.stats[p.dkPlayerId].recTds ?? 0), 0);
        const pointsAllowed = offensiveTds * 6 + k.extraPointsMade! + 3 * (k.fgMade0to39! + k.fgMade40to49! + k.fgMade50Plus!);
        close(pointsAllowed, scenario.stats[opponentDst.dkPlayerId].pointsAllowed!);
      }
    }
  }
  if (format === "showdown") {
    const permuted = structuredClone(first);
    [permuted[1], permuted[2]] = [permuted[2], permuted[1]];
    assert.equal(validateNflLineup(slate, first).key, validateNflLineup(slate, permuted).key);
    const differentCaptain = structuredClone(first);
    [differentCaptain[0].playerId, differentCaptain[1].playerId] = [differentCaptain[1].playerId, differentCaptain[0].playerId];
    assert.notEqual(validateNflLineup(slate, first).key, validateNflLineup(slate, differentCaptain).key);
    const scores = scoreNflLineupDraws(slate, first, prepared);
    const swapped = scoreNflLineupDraws(slate, differentCaptain, prepared);
    scores.forEach((score, i) => close(swapped[i] - score, .5 * (prepared.scores[first[1].playerId][i] - prepared.scores[first[0].playerId][i])));
  }

  // Actual CLI file path, including Showdown salary-row collapse and exclusive report writes.
  const directory = mkdtempSync(join(tmpdir(), "nfl-scenario-test-"));
  try {
    const csvRows = ["Position,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame,Status"];
    for (const player of slate.players) {
      csvRows.push([player.position, player.name, player.dkPlayerId, player.rosterPositions.join("/"), player.salary,
        player.gameKey, player.teamAbbrev, "", ""].join(","));
      if (player.captain) csvRows.push([player.position, player.name, player.captain.dkPlayerId, "CPT", player.captain.salary,
        player.gameKey, player.teamAbbrev, "", ""].join(","));
    }
    writeFileSync(join(directory, "salary.csv"), csvRows.join("\n"));
    writeFileSync(join(directory, "selection.json"), JSON.stringify(selection));
    writeFileSync(join(directory, "evaluation.json"), JSON.stringify(evaluation));
    const args = ["--import", "tsx", fileURLToPath(new URL("./compare-nfl-scenarios.ts", import.meta.url)),
      "--salary-csv", join(directory, "salary.csv"), "--selection-bank", join(directory, "selection.json"),
      "--evaluation-bank", join(directory, "evaluation.json"), "--target", "100", "--count", "3", "--output", join(directory, "report.json")];
    const run = spawnSync(process.execPath, args, { encoding: "utf8" });
    assert.equal(run.status, 0, run.stderr);
    const saved = readFileSync(join(directory, "report.json"), "utf8");
    assert.equal(JSON.parse(saved).candidates.length, 3);
    const overwrite = spawnSync(process.execPath, args, { encoding: "utf8" });
    assert.equal(overwrite.status, 1); assert.match(overwrite.stderr, /EEXIST/);
    assert.equal(readFileSync(join(directory, "report.json"), "utf8"), saved);
  } finally {
    for (const name of readdirSync(directory)) unlinkSync(join(directory, name));
    rmdirSync(directory);
  }
}

// Known counterexample: sum of player P90 is not a complete-lineup P90.
{
  const slate = nflDemoSlate("showdown");
  const lineup = generateNflCandidates(slate, { count: 1, seed: 2 }).lineups[0];
  const bank = nflDemoBank(slate, 8, 10, "anticorrelated-fixture");
  for (let i = 0; i < 10; i++) {
    for (const player of slate.players) bank.scenarios[i].stats[player.dkPlayerId] = emptyNflStats(player.position);
  }
  const prepared = prepareNflScenarios(slate, bank);
  // Controlled final-score columns isolate quantile aggregation, not a football model.
  prepared.playerIds.forEach((id) => { prepared.scores[id] = Array(10).fill(0); });
  prepared.scores[lineup[0].playerId] = [0, 0, 0, 0, 0, 20, 20, 20, 20, 20];
  prepared.scores[lineup[1].playerId] = [30, 30, 30, 30, 30, 0, 0, 0, 0, 0];
  const draws = scoreNflLineupDraws(slate, lineup, prepared);
  assert.deepEqual(draws, Array(10).fill(30));
  close(summarizeNflDraws(draws, prepared.weights, 40).p90, 30);
  close(summarizeNflDraws(draws, prepared.weights, 40).targetProbability, 0);
  const sumP90 = 1.5 * summarizeNflDraws(prepared.scores[lineup[0].playerId], prepared.weights, 40).p90
    + summarizeNflDraws(prepared.scores[lineup[1].playerId], prepared.weights, 40).p90;
  close(sumP90, 60);
  prepared.playerIds.forEach((id) => { prepared.scores[id] = Array(10).fill(.2); });
  const boundaryDraws = scoreNflLineupDraws(slate, lineup, prepared);
  assert.deepEqual(boundaryDraws, Array(10).fill(1.3));
  close(summarizeNflDraws(boundaryDraws, prepared.weights, 1.3).targetProbability, 1);
}

{
  const slate = nflDemoSlate();
  const bank = nflDemoBank(slate, 8, 2, "negative");
  const dst = slate.players.find((p) => p.position === "DST")!;
  bank.scenarios.forEach((s) => { s.stats[dst.dkPlayerId] = { ...emptyNflStats("DST"), pointsAllowed: 40 }; });
  assert.deepEqual(prepareNflScenarios(slate, bank).scores[dst.dkPlayerId], [-4, -4]);
  assert.throws(() => validateNflScenarioStats("QB", { ...emptyNflStats("QB"), passTds: .5 }), /integer/);
  assert.throws(() => validateNflScenarioStats("QB", { ...emptyNflStats("QB"), interceptions: -1 }), /Negative/);
  validateNflScenarioStats("QB", { ...emptyNflStats("QB"), rushYds: -3 });
}

console.log("nfl scenarios: all assertions passed (both formats, 100 candidates, aligned scoring, input validation, replay and accounting)");
