import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { prepareNflScenarios, scoreNflLineupDraws } from "../src/lib/nfl-dfs/scenarios";
import { validateNflLineup, type NflLineup } from "../src/lib/nfl-dfs/lineups";
import { scoreNflStatLine } from "../src/lib/nfl-dfs/scoring";
import { selectPortfolio } from "../src/lib/nfl-dfs/portfolio-selection";

const path = process.argv[2];
if (!path) throw new Error("Supply a PBP script bank path");
const source = JSON.parse(readFileSync(path, "utf8"));
assert.equal(source.authority, "shadow_only");
assert.equal(source.productionChanged, false);
assert.notEqual(source.selection.seed, source.evaluation.seed);
assert.notEqual(source.selection.runId, source.evaluation.runId);
const players = source.slate.players as Array<{ dkPlayerId: number; position: "QB" | "RB" | "WR" | "TE" | "K" | "DST"; teamAbbrev: string; salary: number; captain: { salary: number } | null }>;
const captain = players.filter((p) => p.captain).sort((a, b) => a.captain!.salary - b.captain!.salary)[0];
const rest = players.filter((p) => p.dkPlayerId !== captain.dkPlayerId).sort((a, b) => a.salary - b.salary);
const flex = rest.slice(0, 5);
if (flex.every((p) => p.teamAbbrev === captain.teamAbbrev)) flex[4] = rest.find((p) => p.teamAbbrev !== captain.teamAbbrev)!;
const lineup: NflLineup = [{ slot: "CPT", playerId: captain.dkPlayerId }, ...flex.map((p) => ({ slot: "FLEX" as const, playerId: p.dkPlayerId }))];
validateNflLineup(source.slate, lineup);
const prepared = {} as Record<"selection" | "evaluation", ReturnType<typeof prepareNflScenarios>>;
for (const key of ["selection", "evaluation"] as const) {
  const bank = prepareNflScenarios(source.slate, source[key]);
  prepared[key] = bank;
  const scores = scoreNflLineupDraws(source.slate, lineup, bank);
  for (let i = 0; i < scores.length; i++) {
    const stats = source[key].scenarios[i].stats;
    const manual = 1.5 * scoreNflStatLine(captain.position, stats[captain.dkPlayerId])
      + flex.reduce((sum, p) => sum + scoreNflStatLine(p.position, stats[p.dkPlayerId]), 0);
    assert.ok(Math.abs(scores[i] - manual) < 1e-9);
  }
  console.log(JSON.stringify({ stream: key, scenarios: scores.length, sampleLineupMean: scores.reduce((a, b) => a + b, 0) / scores.length,
    meanMatchDistance: source.diagnostics[key].mean_match_distance }));
}
const candidates: NflLineup[] = players.filter((p) => p.captain).slice(0, 3).map((p) => {
  const choices = players.filter((other) => other.dkPlayerId !== p.dkPlayerId).sort((a, b) => a.salary - b.salary);
  const flexPlayers = choices.slice(0, 5);
  if (flexPlayers.every((other) => other.teamAbbrev === p.teamAbbrev)) flexPlayers[4] = choices.find((other) => other.teamAbbrev !== p.teamAbbrev)!;
  return [{ slot: "CPT", playerId: p.dkPlayerId }, ...flexPlayers.map((other) => ({ slot: "FLEX" as const, playerId: other.dkPlayerId }))];
});
const portfolio = selectPortfolio(source.slate, candidates, prepared.selection, prepared.evaluation,
  { count: 2, target: 10, objective: "max_mean_best_lineup" });
assert.equal(portfolio.selected.length, 2);
console.log(JSON.stringify({ portfolioCandidates: candidates.length, selected: portfolio.selected.length,
  selectorVersion: portfolio.selectorVersion }));
