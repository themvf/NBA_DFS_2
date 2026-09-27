import assert from "node:assert/strict";
import { getPickemEvidence } from "../src/db/pickem-evidence";
import { getNflPickemSlate } from "../src/db/queries";
async function main() {
  const evidence = await getPickemEvidence(2026);
  const slate = await getNflPickemSlate(2026, evidence);
  assert.deepEqual(evidence.warnings, []);
  let snapshots = 0;
  for (const game of slate.games) for (const team of evidence.games[game.gameId]?.pfr ?? []) {
    assert.ok(team.games.length <= 4);
    for (const prior of team.games) if (prior.capturedAt) {
      assert.ok(Date.parse(prior.capturedAt) < Date.parse(game.kickoff!));
      assert.ok(Date.parse(prior.capturedAt) <= Date.parse(evidence.loadedAt));
      snapshots++;
    }
  }
  const bucs = slate.games.find(g => g.week === 3 && g.homeAbbrev === "TB")!;
  const pfr = evidence.games[bucs.gameId].pfr!;
  assert.equal(pfr.length, 2);
  assert.ok(pfr.every(t => t.games.length === 2 && t.games.every(g => !g.missing.length)));
  const baker = pfr.find(t => t.team === "TB")!.games.flatMap(g => g.players)
    .filter(p => p.team === "TB" && p.section === "passing_advanced");
  assert.equal(baker.reduce((sum,p) => sum + (p.stats.times_sacked ?? 0),0), 7);
  console.log(JSON.stringify({snapshots, matchup:"MIN at TB", teams:pfr.map(t=>({team:t.team,games:t.games.length})), bakerSacks:7}));
}
main().catch(e=>{console.error(e);process.exit(1);});
