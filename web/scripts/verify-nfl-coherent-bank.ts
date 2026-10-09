import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { prepareNflScenarios, scoreNflLineupDraws } from "../src/lib/nfl-dfs/scenarios";
import { validateNflLineup, type NflLineup } from "../src/lib/nfl-dfs/lineups";
import { scoreNflStatLine } from "../src/lib/nfl-dfs/scoring";

const [path, output] = process.argv.slice(2);
if (!path || !output) throw new Error("Supply bank input and verification output paths");
const source = JSON.parse(readFileSync(path, "utf8"));
const summaries = [];
let showdownVerification = null;
for (const [index, key] of ["selection", "evaluation"].entries()) {
  const bank = prepareNflScenarios(source.slate, source[key]);
  let maximumScoringMeanDifference = 0;
  for (const marginal of source.diagnostics[index].player_marginals) {
    const values = bank.scores[marginal.dkPlayerId];
    if (!values) continue;
    const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
    maximumScoringMeanDifference = Math.max(maximumScoringMeanDifference, Math.abs(mean - marginal.researchMean));
  }
  assert.ok(maximumScoringMeanDifference < 1e-8, "Python canonical scoring and TypeScript scoring must agree");
  if (source.slate.format === "showdown") {
    const kicker = source.slate.players.find((p: any) => p.position === "K" && p.captain);
    assert.ok(kicker, "Actual saved Showdown branch must model a kicker");
    const others = source.slate.players.filter((p: any) => p.dkPlayerId !== kicker.dkPlayerId).sort((a: any, b: any) => a.salary - b.salary);
    const flex = others.slice(0, 5);
    if (flex.every((p: any) => p.teamAbbrev === kicker.teamAbbrev)) flex[4] = others.find((p: any) => p.teamAbbrev !== kicker.teamAbbrev);
    const lineup: NflLineup = [{ slot: "CPT", playerId: kicker.dkPlayerId }, ...flex.map((p: any) => ({ slot: "FLEX" as const, playerId: p.dkPlayerId }))];
    const legal = validateNflLineup(source.slate, lineup);
    assert.equal(legal.salary, kicker.captain.salary + flex.reduce((sum: number, p: any) => sum + p.salary, 0));
    const scores = scoreNflLineupDraws(source.slate, lineup, bank);
    scores.forEach((score, i) => {
      const stats = source[key].scenarios[i].stats;
      const manual = 1.5 * scoreNflStatLine("K", stats[kicker.dkPlayerId]) + flex.reduce((sum: number, p: any) => sum + scoreNflStatLine(p.position, stats[p.dkPlayerId]), 0);
      assert.ok(Math.abs(score - manual) < 1e-9, "Captain player points must multiply exactly once");
    });
    showdownVerification = { status: "saved_slate_mechanics_passed", sourceStatus: source.status, kicker: kicker.name,
      salary: legal.salary, entries: lineup, captainMultiplier: 1.5, liveRecommendation: false };
  }
  const pairs = [];
  const marginalMeans = new Map<number, number>(source.diagnostics[index].player_marginals.map((p: any) => [p.dkPlayerId, p.researchMean]));
  for (const team of source.slate.teams) {
    const pool = source.optimizerPlayers.filter((p: any) => p.team === team);
    const qb = pool.filter((p: any) => p.position === "QB").sort((a: any, b: any) => (marginalMeans.get(b.dkPlayerId) ?? 0) - (marginalMeans.get(a.dkPlayerId) ?? 0))[0];
    const receiver = pool.filter((p: any) => ["WR", "TE"].includes(p.position)).sort((a: any, b: any) => b.ourProj - a.ourProj)[0];
    if (!qb || !receiver) continue;
    const x = bank.scores[qb.dkPlayerId], y = bank.scores[receiver.dkPlayerId];
    const mx = x.reduce((a, b) => a + b, 0) / x.length, my = y.reduce((a, b) => a + b, 0) / y.length;
    const cov = x.reduce((sum, value, i) => sum + (value - mx) * (y[i] - my), 0);
    const denominator = Math.sqrt(x.reduce((sum, value) => sum + (value - mx) ** 2, 0) * y.reduce((sum, value) => sum + (value - my) ** 2, 0));
    pairs.push({ team, quarterback: qb.name, receiver: receiver.name, correlation: denominator ? cov / denominator : null });
  }
  summaries.push({ stream: key, players: bank.playerIds.length, draws: bank.scenarioIds.length,
    quarterbackChoice: "largest research QB mean in the declared conditional starter state; baseline population-prior backups are not assumed starters",
    maximumScoringMeanDifference, quarterbackReceiverDependence: pairs,
    positiveVariablePairs: pairs.filter((p) => p.correlation !== null && p.correlation > 0).length,
    variablePairs: pairs.filter((p) => p.correlation !== null).length });
}
assert.notEqual(source.selection.runId, source.evaluation.runId);
assert.notEqual(source.selection.seed, source.evaluation.seed);
assert.notEqual(source.selection.streamId, source.evaluation.streamId);
const result = { status: "mechanical_checks_passed_not_predictive_qualification", model: source.version,
  baselineRunId: source.audit.productionRunId, registrationHash: source.manifest.sources.registration_sha256, streams: summaries, showdownVerification };
writeFileSync(output, JSON.stringify(result, null, 2));
console.log(JSON.stringify(result));
