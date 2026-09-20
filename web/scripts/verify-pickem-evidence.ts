/** Read real evidence and validate time boundaries. Does not create cards or news. */
import assert from "node:assert/strict";
import { getPickemEvidence } from "../src/db/pickem-evidence";
import { getNflPickemSlate } from "../src/db/queries";
import { timestamp } from "../src/lib/nfl/pickem-evidence";

async function main() {
  const season = Number(process.argv[2] ?? 2026);
  const [evidence, slate] = await Promise.all([getPickemEvidence(season), getNflPickemSlate(season)]);
  for (const g of slate.games) {
    const e = evidence.games[g.gameId];
    if (!e) continue;
    for (const q of [e.opening, e.latest]) {
      if (!q) continue;
      assert.ok(timestamp(q.capturedAt) < timestamp(g.kickoff));
      assert.ok(timestamp(q.capturedAt) <= timestamp(evidence.loadedAt));
    }
    for (const n of e.news) {
      assert.ok(timestamp(n.publishedAt ?? n.observedAt) < timestamp(g.kickoff));
    }
    assert.ok(e.performance.every(p => p.week < g.week));
  }
  console.log(JSON.stringify({ season, games: Object.keys(evidence.games).length, warnings: evidence.warnings,
    quoteCoverage: Object.values(evidence.games).filter(g => g.latest).length,
    newsCoverage: Object.values(evidence.games).filter(g => g.news.length).length,
    performanceCoverage: Object.values(evidence.games).filter(g => g.performance.length).length,
    examples: slate.games.filter(g => g.week === 2 && ["DEN", "HOU", "ATL"].includes(g.homeAbbrev)).map(g => ({
      game: `${g.awayAbbrev} at ${g.homeAbbrev}`, pHome: g.pHome, computedAt: g.computedAt,
      ...evidence.games[g.gameId],
    })),
  }, null, 2));
  assert.deepEqual(evidence.warnings, [], "All existing evidence feeds should be readable");
}
main().catch(error => { console.error(error); process.exitCode = 1; });
