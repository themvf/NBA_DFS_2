import assert from "node:assert/strict";
import { sql } from "drizzle-orm";
import { db } from "../src/db";
import { ensurePickemTables } from "../src/db/ensure-schema";
import { getNflPickemSlate, getPickemLedger } from "../src/db/queries";
import { freezePickemRecommendation, savePickemNews } from "../src/app/nfl/pickem/actions";
import { evOptimalEntry } from "../src/lib/nfl/pickem-strategy";
import { timestamp } from "../src/lib/nfl/pickem-evidence";

async function main() {
  await ensurePickemTables();
  const slate = await getNflPickemSlate(2026);
  const week = slate.weeks.find(w => slate.games.filter(g => g.week === w).length > 0 &&
    slate.games.filter(g => g.week === w).every(g => timestamp(g.kickoff) > Date.now() + 86400_000));
  assert.ok(week, "Need a future week for isolated integration test");
  const games = slate.games.filter(g => g.week === week);
  const baseline = evOptimalEntry(games.map(g => ({ ...g, fieldHomePct: null })), "confidence");
  const pool = await db.execute(sql`INSERT INTO pickem_pools (name, season, format, pool_entries)
    VALUES (${`__evidence_test_${Date.now()}`}, 2026, 'confidence', 50) RETURNING id`);
  const poolId = Number(pool.rows[0].id);
  try {
    const input: Parameters<typeof freezePickemRecommendation>[0] = {
      poolId, season: 2026, week, format: "confidence", objective: "ev", poolEntries: 50,
      sims: 0, modelVersion: "evidence-integration-test", baselineExpectedPoints: 0,
      recommendedExpectedPoints: 0, baselinePrizeShare: 0, recommendedPrizeShare: 0,
      fieldModel: {}, deviations: [], games: games.map((g, i) => ({ ...g,
        baselinePickHome: baseline.pickHome[i], baselineConfidence: baseline.confidence[i],
        recommendedPickHome: baseline.pickHome[i], recommendedConfidence: baseline.confidence[i],
        fieldHomeShare: 0.6, fieldSource: "observed", scenario: i === 0 ? { pHome: 0.4, reason: "Test availability assumption" } : null,
      })),
    };
    const result = await freezePickemRecommendation(input);
    assert.equal(result.ok, true, JSON.stringify(result));
    const cards = (await getPickemLedger(2026)).filter(r => r.poolId === poolId);
    assert.equal(cards.length, 1);
    assert.equal(cards[0].games.length, games.length);
    assert.ok(cards[0].games.every(g => g.evidence?.version === 1));
    const first = cards[0].games.find(g => g.gameId === games[0].gameId)!;
    assert.equal(first.pHome, games[0].pHome, "Scenario cannot overwrite forecast");
    assert.equal(first.evidence?.scenario?.pHome, 0.4);
    assert.equal(first.evidence?.probabilityComputedAt, games[0].computedAt);
    assert.ok(cards[0].games.every(g => g.evidence?.latest?.capturedAt));
    const invalid = await freezePickemRecommendation({ ...input, games: input.games.map((g, i) => i ? g : { ...g, pHome: NaN }) });
    assert.equal(invalid.ok, false);
    const duplicate = await freezePickemRecommendation({ ...input, games: input.games.map((g, i) => i === 1 ? input.games[0] : g) });
    assert.equal(duplicate.ok, false);
    const emptyScenario = await freezePickemRecommendation({ ...input, games: input.games.map((g, i) => i ? g : { ...g, scenario: { pHome: 0.4, reason: "" } }) });
    assert.equal(emptyScenario.ok, false);
    const started = slate.games.find(g => timestamp(g.kickoff) <= Date.now());
    if (started) assert.equal((await freezePickemRecommendation({ ...input, week: started.week })).ok, false);
    const invalidNews = await savePickemNews({ gameId: games[0].gameId, team: games[0].homeAbbrev,
      category: "quarterback", headline: "Must not be stored", detail: "", source: "test",
      url: "javascript:alert(1)", publishedAt: new Date().toISOString(), status: "reported" });
    assert.equal(invalidNews.ok, false);
    assert.equal((await getPickemLedger(2026)).filter(r => r.poolId === poolId).length, 1, "Refusals leave no partial cards");
    const second = await freezePickemRecommendation(input);
    assert.equal(second.ok, true, JSON.stringify(second));
    const history = (await getPickemLedger(2026)).filter(r => r.poolId === poolId);
    assert.equal(history.length, 2);
    assert.equal(history.filter(r => r.supersededBy == null).length, 1);
    assert.equal(history.find(r => r.id === cards[0].id)?.games[0].evidence?.recordedAt, cards[0].games[0].evidence?.recordedAt);
    console.log("Freeze integration passed: atomic complete cards, immutable evidence, scenario isolation, invalid/stale/late refusals and superseding.");
  } finally {
    await db.execute(sql`DELETE FROM pickem_pools WHERE id = ${poolId}`);
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
