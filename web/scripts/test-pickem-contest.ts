import assert from "node:assert/strict";
import { resolveField, simulateWorld, evaluateEntry, evOptimalEntry, flipCost, type PickemGame } from "../src/lib/nfl/pickem-strategy";
import { compareContestCards, EMPTY_POOL_CONFIG, splitPayout, validatePoolConfig, type PoolConfig } from "../src/lib/nfl/pickem-contest";
import { gradeRecommendation } from "../src/lib/nfl/pickem-grading";
const g = (id: number, pHome = .6, share: number | null = null): PickemGame => ({ gameId: id, week: 1,
  homeAbbrev: "H", awayAbbrev: "A", pHome, pTie: .01, provenance: "market", kickoff: "2099-09-27T17:00:00Z",
  completed: false, homeWon: null, fieldHomePct: share });
const model = { favoriteBias: 1.3, skillSigma: .35, chalkFraction: .25 };
const near = (a: number, b: number) => assert.ok(Math.abs(a - b) < 1e-10, `${a} != ${b}`);
for (const probability of [.4, .6]) for (const share of [0, .1, .6, 1]) {
  const field = resolveField([g(1, probability, share)], model, 101);
  const recovered = (field.chalkRivals * Number(probability >= .5) + field.noisyRivals * field.nonChalkShares[0]) / 100;
  near(recovered, share);
}
assert.equal(resolveField([g(1, .6, 0)], model, 101).chalkRivals, 0);
assert.ok(resolveField([g(1, .6, 0)], model, 101).warnings.some(s => s.includes("reduced")));
const all = g(1, .6, .6); all.fieldObservation = { population: "all", entryCount: 5,
  observedOwnHomePick: true, capturedAt: "2026-09-27T10:00:00Z", source: "pool" };
near(resolveField([all], model, 5).shares[0].share, .5);
// Changing candidate sides cannot change this frozen observed own pick.
const frozen = JSON.stringify(all.fieldObservation); evOptimalEntry([all], "straight");
assert.equal(JSON.stringify(all.fieldObservation), frozen);
all.fieldObservation.observedOwnHomePick = false;
near(resolveField([all], model, 5).shares[0].share, .75);
assert.equal(resolveField([all], model, 1).rivalCount, 0);
assert.equal(resolveField([g(1, .6, 1)], { ...model, chalkFraction: 1 }, 5).noisyRivals, 0);
assert.throws(() => resolveField([{ ...all, fieldHomePct: 0, fieldObservation: { ...all.fieldObservation!, observedOwnHomePick: true } }], model, 5));
const tieGame = { ...g(1), pTie: 1 };
const tieWorld = simulateWorld([tieGame], "straight", model, { sims: 100, poolEntries: 5, tiePoints: .5 });
const tieEval = evaluateEntry([tieGame], evOptimalEntry([tieGame], "straight"), tieWorld);
near(tieEval.meanScore, .5); near(tieEval.prizeShare, .2); near(flipCost([tieGame], evOptimalEntry([tieGame], "straight"), 0), 0);
near(flipCost([{ ...g(1, .52), pTie: .1 }], evOptimalEntry([g(1, .52)], "straight"), 0), .036);
near(splitPayout(10, [11, 10, 10, 9], [100, 60, 30, 0]).payout, 30);
near(splitPayout(10, [], [100]).payout, 100);
const unknown = compareContestCards([g(1)], [], EMPTY_POOL_CONFIG, model);
assert.equal(unknown.candidates.length, 0); assert.equal(unknown.baseline.weeklyPayout, null);
assert.throws(() => validatePoolConfig({ ...EMPTY_POOL_CONFIG, entries: 1.5 }));
const config: PoolConfig = { ...EMPTY_POOL_CONFIG, entries: 3, gameTieRule: "zero", prizeTieRule: "split",
  lockRule: "first_kickoff", weeklyPayouts: [100], seasonPayouts: [1000], ownScore: 10, rivalScores: [10, 11],
  remainingWeeks: 0, sameCard: true, standingsCapturedAt: "2026-09-27T10:00:00Z" };
const report = compareContestCards([g(1, .52, .9), g(2, .7, .9)], [], config, model, { sims: 500 });
assert.equal(report.candidates.length, 3); assert.notEqual(report.selectionSeed, report.evaluationSeed);
for (const c of report.candidates) {
  near(c.evaluation.combinedPayout!, c.evaluation.weeklyPayout! + c.evaluation.seasonPayout!);
  assert.ok(c.monteCarlo95[0] <= c.pairedPayoutGain && c.monteCarlo95[1] >= c.pairedPayoutGain);
  assert.ok(c.entry.confidence.every(x => x === 1));
}
assert.deepEqual(report, compareContestCards([g(1, .52, .9), g(2, .7, .9)], [], config, model, { sims: 500 }));
assert.throws(() => compareContestCards([g(1)], [], config, model, { selectionSeed: 1, evaluationSeed: 1 }));
const noFuture = compareContestCards([g(1)], [], { ...config, remainingWeeks: 1 }, model, { sims: 100 });
assert.equal(noFuture.eligibility.weekly, true); assert.equal(noFuture.eligibility.season, false);
const midweek=compareContestCards([{...g(1),completed:true}],[],config,model,{sims:100});
assert.equal(midweek.candidates.length,0);assert.equal(midweek.baseline.weeklyPayout,null);
assert.ok(midweek.warnings.some(s=>s.includes("Midweek")));
const played={...g(9),completed:true,kickoff:"2026-09-01T17:00:00Z",homeWon:true};
const partialConfig:PoolConfig={...config,lockRule:"per_game",settledWeek:{capturedAt:"2026-09-02T00:00:00Z",gameIds:[9],ownPoints:1,rivalPoints:[0,1]}};
assert.throws(()=>validatePoolConfig({...partialConfig,settledWeek:{...partialConfig.settledWeek!,ownPoints:.3}}),/scoring rule/);
const partial=compareContestCards([played,g(10,.6)],[],partialConfig,model,{sims:100});
assert.equal(partial.eligibility.weekly,true);near(partial.baseline.expectedCorrect,1+.99*.6);
assert.ok(partial.candidates.every(c=>c.entry.pickHome[0]===true),"Settled picks cannot be optimized again");
const inProgress=compareContestCards([{...played,completed:false},g(10)],[],partialConfig,model,{sims:100});
assert.equal(inProgress.eligibility.weekly,false);
const future = compareContestCards([g(1)], [{ ...g(2), week: 2 }], { ...config, remainingWeeks: 1 }, model, { sims: 100 });
assert.equal(future.eligibility.season, true); assert.deepEqual(future.futureGameIds, [2]);
assert.ok(future.warnings.some(s => s.includes("Future weeks")));
const tied = gradeRecommendation([{gameId:1,pHome:.6,pTie:.1,isTie:true,tiePoints:.5,provenance:"fixture",homeWon:null,
  baselinePickHome:true,baselineConfidence:1,recommendedPickHome:false,recommendedConfidence:1}]);
assert.equal(tied.gamesGraded,1);assert.equal(tied.complete,true);assert.equal(tied.baselinePoints,.5);assert.equal(tied.recommendedPoints,.5);
assert.equal(tied.threeWayGames,1);near(tied.threeWayLogLoss!,-Math.log(.1));assert.equal(tied.brier,null);
console.log("Pick'em contest tests passed: population/chalk recovery, ties, payouts, missing rules, independent evaluation, season scenarios.");
