import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { getPickemEvidence } from "../src/db/pickem-evidence";
import { getNflPickemSlate } from "../src/db/queries";
import { comparePickemDefensiveForecasts } from "../src/lib/nfl/pickem-defensive";

async function main() {
  const evidence = await getPickemEvidence(2026);
  const baseline = await getNflPickemSlate(2026, evidence);
  const comparison = comparePickemDefensiveForecasts(baseline, evidence, evidence.loadedAt);
  const game = baseline.games.find(g => g.week === 3 && g.homeAbbrev === "CHI" && g.awayAbbrev === "PHI");
  assert.ok(game, "Monday game must exist");
  const pair = comparison.comparisons[game.gameId];
  assert.equal(game.provenance, "market_ml_novig");
  assert.equal(pair.applied, true, pair.reason);
  assert.equal(pair.baselineHome, game.pHome);
  assert.notEqual(pair.baselinePlusDefenseHome, pair.baselineHome);
  assert.equal(comparison.slate.games.find(g => g.gameId === game.gameId)!.pHome, pair.baselinePlusDefenseHome);
  assert.ok(evidence.games[game.gameId].matchup?.input.model?.definitionId.startsWith("pickem_matchup_combined:"),
    "Normal reader must choose the preferred combined arm rather than the latest standalone arm");
  const receipt = { checkedAt: evidence.loadedAt, gameId: game.gameId,
    matchup: "PHI@CHI", ...pair, forecastId: evidence.games[game.gameId].matchup?.forecastId };
  await writeFile("../artifacts/nfl-matchup-implementation/2026-09-28/normal-pickem-repair-verification.json",
    JSON.stringify(receipt, null, 2) + "\n");
  console.log(JSON.stringify(receipt));
}
main().catch(error => { console.error(error); process.exitCode = 1; });
