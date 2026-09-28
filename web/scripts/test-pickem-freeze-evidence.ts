import assert from "node:assert/strict";
import { sql } from "drizzle-orm";
import { db } from "../src/db";
import { ensurePickemTables } from "../src/db/ensure-schema";
import { getNflPickemSlate, getPickemLedger } from "../src/db/queries";
import { getPickemEvidence } from "../src/db/pickem-evidence";
import { freezePickemRecommendation, savePickemNews } from "../src/app/nfl/pickem/actions";
import { selectPickemDefensiveForecasts } from "../src/lib/nfl/pickem-defensive";
import { evOptimalEntry } from "../src/lib/nfl/pickem-strategy";
import { timestamp } from "../src/lib/nfl/pickem-evidence";
import { EMPTY_POOL_CONFIG } from "../src/lib/nfl/pickem-contest";

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
    const defensiveEvidence = await getPickemEvidence(2026);
    const defensiveSlate = selectPickemDefensiveForecasts(
      await getNflPickemSlate(2026, defensiveEvidence), defensiveEvidence, "experimental", defensiveEvidence.loadedAt);
    const defensiveGames = defensiveSlate.slate.games.filter(g => g.week === week);
    const defensiveBaseline = evOptimalEntry(defensiveGames.map(g => ({ ...g, fieldHomePct: null })), "confidence");
    const second = await freezePickemRecommendation({ ...input, defensiveMode: "experimental",
      games: defensiveGames.map((g, i) => ({ ...g,
        baselinePickHome: defensiveBaseline.pickHome[i], baselineConfidence: defensiveBaseline.confidence[i],
        recommendedPickHome: defensiveBaseline.pickHome[i], recommendedConfidence: defensiveBaseline.confidence[i],
        fieldHomeShare: .6, fieldSource: "observed" as const })) });
    assert.equal(second.ok, true, JSON.stringify(second));
    const history = (await getPickemLedger(2026)).filter(r => r.poolId === poolId);
    assert.equal(history.length, 2);
    assert.equal(history.filter(r => r.supersededBy == null).length, 1);
    assert.equal(history.find(r => r.supersededBy == null)?.fieldModel.defensiveMode, "experimental");
    assert.ok(Array.isArray(history.find(r => r.supersededBy == null)?.fieldModel.defensiveAdjustedGameIds));
    assert.equal(history.find(r => r.id === cards[0].id)?.games[0].evidence?.recordedAt, cards[0].games[0].evidence?.recordedAt);
    const partialWeek=slate.weeks.find(w=>{
      const gs=slate.games.filter(g=>g.week===w);
      return gs.some(g=>g.completed) && gs.some(g=>timestamp(g.kickoff)>Date.now()) && gs.every(g=>g.completed || timestamp(g.kickoff)>Date.now());
    });
    if(partialWeek != null) {
      const whole=slate.games.filter(g=>g.week===partialWeek), remaining=whole.filter(g=>timestamp(g.kickoff)>Date.now());
      const entry=evOptimalEntry(remaining.map(g=>({...g,fieldHomePct:null})),"straight");
      const config={...EMPTY_POOL_CONFIG,entries:3,weeklyPayouts:[100],gameTieRule:"half" as const,prizeTieRule:"split" as const,lockRule:"per_game" as const,
        settledWeek:{capturedAt:new Date().toISOString(),gameIds:whole.filter(g=>g.completed).map(g=>g.gameId),ownPoints:0,rivalPoints:[0,0]}};
      const partialInput={...input,week:partialWeek,format:"straight" as const,poolEntries:3,
        fieldModel:{favoriteBias:1.3,chalkFraction:.25,contestComparison:{config}},
        games:remaining.map((g,i)=>({...g,baselinePickHome:entry.pickHome[i],recommendedPickHome:entry.pickHome[i],baselineConfidence:1,recommendedConfidence:1,fieldHomeShare:.6,fieldSource:"observed" as const}))};
      const partial=await freezePickemRecommendation(partialInput);
      assert.equal(partial.ok,true,JSON.stringify(partial));
      const saved=(await getPickemLedger(2026)).find(r=>r.poolId===poolId && r.week===partialWeek)!;
      assert.equal(saved.games.length,remaining.length);
      assert.ok(saved.games.every(g=>remaining.some(r=>r.gameId===g.gameId)));
      for (const row of saved.games) {
        const pair = row.evidence?.defensiveComparison;
        assert.ok(pair, "Saved cards must retain both projection numbers");
        assert.equal(pair.baselinePlusDefenseHome, row.pHome);
        if (pair.applied) {
          assert.ok(row.evidence?.matchup?.candidate);
          assert.equal(row.evidence.matchup.candidate.homeConditional, row.pHome);
          assert.equal(row.evidence.matchup.input.baseline.homeConditional, pair.baselineHome);
          assert.equal(row.evidence.matchup.input.baseline.marketCapturedAt, row.evidence.latest?.capturedAt);
        }
      }
      const invalid=await freezePickemRecommendation({...partialInput,fieldModel:{contestComparison:{config:{...config,settledWeek:null}}}});
      assert.equal(invalid.ok,false,"Missing locked-game score evidence must reject midweek freeze");
    }
    console.log("Freeze integration passed: atomic complete cards, immutable evidence, scenario isolation, invalid/stale/late refusals and superseding.");
  } finally {
    await db.execute(sql`DELETE FROM pickem_pools WHERE id = ${poolId}`);
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
