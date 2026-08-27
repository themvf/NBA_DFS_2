import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { queryRows } from "../src/db/query-result";
import { buildSnakeSlots, nextControlledPick, picksUntilControlled } from "../src/lib/fantasy-football/draft-engine";
import { computeAvailabilityOdds } from "../src/lib/fantasy-football/availability-odds";
import { recommendPlayers } from "../src/lib/fantasy-football/recommendations";
import { fantasyBadgeClass } from "../src/lib/fantasy-football/badge-style";
import { filterFantasyRankings } from "../src/lib/fantasy-football/ranking-filters";
import { formatPriorSeasonFantasyPoints } from "../src/lib/fantasy-football/prior-season-finish";
import type { FantasyRankingRow } from "../src/db/queries-fantasy-football";
import { buildProjectionExplanation } from "../src/lib/fantasy-football/projection-explanation";
import { buildBestBallDraftBoard, canAddBestBallPlayer, getBestBallRosterStatus, parseBestBallDraftState } from "../src/lib/fantasy-football/best-ball";
import { buildRosterCorrelationBadges } from "../src/lib/fantasy-football/teammate-correlation-badge";
import { getYahooMarketSignal, scoreDraftKingsBestBallLine, selectBestBallLineup, simulateShadowBestBallCandidates, type ShadowBestBallPlayer } from "../src/lib/fantasy-football/best-ball-simulation";
import type { TeammateCorrelationRow } from "../src/db/queries-fantasy-football";
import {
  bestBallAdvisorDraftSignature,
  buildBestBallAdvisorProviderSnapshot,
  buildBestBallAdvisorSnapshot,
  enrichBestBallAdvisorResult,
  getValidatedBestBallAdvisorOutput,
  validateBestBallAdvisorOutput,
} from "../src/lib/fantasy-football/ai-draft-advisor";

assert.deepEqual(queryRows<{ id: number }>({ rows: [{ id: 3 }] }), [{ id: 3 }]);
assert.deepEqual(queryRows<{ id: number }>([{ id: 2 }]), [{ id: 2 }]);
assert.deepEqual(queryRows<{ id: number }>({}), []);
assert.equal(formatPriorSeasonFantasyPoints(241.6, 3, 1), "241.6 (3)");
assert.equal(formatPriorSeasonFantasyPoints(118.5, 7, 2), "118.5 (T7)");
assert.equal(formatPriorSeasonFantasyPoints(90, null, null), "90.0");
assert.equal(formatPriorSeasonFantasyPoints(null, 1, 1), "—");

const slots = buildSnakeSlots(4, 3);
assert.deepEqual(slots.map((slot) => slot.teamSlot), [1, 2, 3, 4, 4, 3, 2, 1, 1, 2, 3, 4]);
assert.equal(nextControlledPick(3, 2, 4, 3), 7);
assert.equal(picksUntilControlled(3, 2, 4, 3), 4);

const recommendations = recommendPlayers([
  { playerId: 1, position: "RB", ourRank: 12, ecr: 15, adp: 25, projectedPoints: 250, tier: 2, confidence: 0.8 },
  { playerId: 2, position: "QB", ourRank: 8, ecr: 8, adp: 8, projectedPoints: 330, tier: 1, confidence: 0.9 },
], ["QB"], 10);
assert.equal(recommendations[0].playerId, 1);
assert.equal(recommendations[0].adpDelta, 13);

// availability-odds.ts -- computeAvailabilityOdds
assert.equal(computeAvailabilityOdds({ adp: null, adpStdev: 2, adpSampleSize: 50 }, { currentPick: 1, targetPick: 10, teamCount: 12 }), null);
assert.equal(computeAvailabilityOdds({ adp: 10, adpStdev: null, adpSampleSize: 50 }, { currentPick: 1, targetPick: 10, teamCount: 12 }), null);
assert.equal(computeAvailabilityOdds({ adp: 10, adpStdev: 2, adpSampleSize: 50 }, { currentPick: 5, targetPick: 3, teamCount: 12 }), null);

const wideSample = computeAvailabilityOdds({ adp: 10, adpStdev: 2, adpSampleSize: 100 }, { currentPick: 1, targetPick: 10, teamCount: 12 });
assert.ok(wideSample);
assert.equal(wideSample!.adjustedAdp, 10);
assert.equal(wideSample!.adjustedStdev, 2);
assert.equal(wideSample!.sampleSize, 100);
assert.equal(wideSample!.confidence, "high");
assert.ok(wideSample!.probability > 0.4 && wideSample!.probability < 0.65, `expected ~0.56, got ${wideSample!.probability}`);

// Round-preserving rescale: adp=10/stdev=2 in a 12-team draft becomes
// adp=5.5/stdev=1 in a 6-team draft (half the picks per round).
const rescaled = computeAvailabilityOdds({ adp: 10, adpStdev: 2, adpSampleSize: 100 }, { currentPick: 1, targetPick: 6, teamCount: 6 });
assert.ok(rescaled);
assert.equal(rescaled!.adjustedAdp, 5.5);
assert.equal(rescaled!.adjustedStdev, 1);

// Thin real-draft samples widen the effective stdev instead of reporting FFC's raw (noisy) value.
const thinSample = computeAvailabilityOdds({ adp: 10, adpStdev: 2, adpSampleSize: 5 }, { currentPick: 1, targetPick: 10, teamCount: 12 });
assert.ok(thinSample);
assert.ok(Math.abs(thinSample!.adjustedStdev - 4.898979485566356) < 1e-9);
assert.equal(thinSample!.confidence, "low");

const noSampleInfo = computeAvailabilityOdds({ adp: 10, adpStdev: 2, adpSampleSize: null }, { currentPick: 1, targetPick: 10, teamCount: 12 });
assert.ok(noSampleInfo);
assert.equal(noSampleInfo!.confidence, "low");
assert.ok(Math.abs(noSampleInfo!.adjustedStdev - 3.2) < 1e-9);

// Conditioning on "already survived to currentPick": asking about the
// current pick itself is certainty.
const sameTarget = computeAvailabilityOdds({ adp: 10, adpStdev: 2, adpSampleSize: 100 }, { currentPick: 10, targetPick: 10, teamCount: 12 });
assert.ok(sameTarget);
assert.equal(sameTarget!.probability, 1);

// A top-4-ADP player is essentially certain to be gone by pick 40.
const longGone = computeAvailabilityOdds({ adp: 2, adpStdev: 1, adpSampleSize: 100 }, { currentPick: 1, targetPick: 40, teamCount: 12 });
assert.ok(longGone);
assert.ok(longGone!.probability < 0.02);

// recommendations.ts now scores urgency from real availability odds when supplied.
const withLowRisk = recommendPlayers([
  { playerId: 9, position: "RB", ourRank: 12, ecr: 15, adp: 25, projectedPoints: 250, tier: 2, confidence: 0.8, availabilityProbability: 0.95 },
], [], 10)[0];
const withHighRisk = recommendPlayers([
  { playerId: 9, position: "RB", ourRank: 12, ecr: 15, adp: 25, projectedPoints: 250, tier: 2, confidence: 0.8, availabilityProbability: 0.05 },
], [], 10)[0];
assert.ok(withHighRisk.score > withLowRisk.score, "a player at greater risk of being lost should score higher urgency");
assert.ok(withHighRisk.explanation.includes("5% available at your pick"));
assert.match(fantasyBadgeClass({ code: "NFL_TOP_10_TARGETS", class: "fact" }), /blue/);
assert.match(fantasyBadgeClass({ code: "NFL_TOP_10_RUSH_TDS", class: "fact" }), /orange/);
assert.match(fantasyBadgeClass({ code: "TEAM_TARGET_LEADER", class: "fact" }), /cyan/);
assert.match(fantasyBadgeClass({ code: "INJURY", class: "risk" }), /red/);
assert.match(fantasyBadgeClass({ code: "TOP_3_POSITION_POINTS", class: "fact" }), /yellow/);
const filterRows = [
  { playerId: 1, name: "A.J. Brown", position: "WR", team: "NE" },
  { playerId: 2, name: "Bijan Robinson", position: "RB", team: "ATL" },
] as FantasyRankingRow[];
assert.deepEqual(filterFantasyRankings(filterRows, { name: "brown", position: "WR", team: "NE" }).map((row) => row.playerId), [1]);
assert.equal(filterFantasyRankings(filterRows, { name: "", position: "RB", team: "" }).length, 1);
const explanation = buildProjectionExplanation({
  method: "history_regression",
  season_inputs: [{ season: 2025, ppg: 20.61, weight: 0.75 }],
  weighted_history_ppg: 17.79,
  position_prior_ppg: 14,
  regression_prior_games: 4,
  regressed_ppg: 17.06,
  expected_games_before_injury: 15.9,
  role_factor: 1,
  base_points_before_injury: 271.25,
  injury_factor: 1,
  final_points: 271.25,
  not_modeled: ["current teammates", "future schedule"],
});
assert.equal(explanation.method, "Weighted history + regression");
assert.ok(explanation.lines.includes("2025: 20.6 PPG × 75%"));
assert.ok(explanation.lines.includes("17.06 PPG × 15.90 games × 1.00 role = 271.3"));
assert.deepEqual(explanation.notModeled, ["current teammates", "future schedule"]);
const baselineExplanation = buildProjectionExplanation({
  method: "history_regression",
  weighted_history_ppg: 20.2,
  position_prior_ppg: 8,
  regression_prior_games: 4,
  regression_sample_games: 51,
  regressed_ppg: 19.31,
  baseline_games: 17,
  expected_games_after_injury: 16,
  role_factor: 1,
  base_points_before_injury: 328.27,
  final_points: 328.27,
  injury_factor: 1,
  availability_adjustment_applied_to_baseline: false,
});
assert.ok(baselineExplanation.lines.includes("Regression: 51 historical games + 4 position-prior games at 8.0 PPG → 19.3 PPG"));
assert.ok(baselineExplanation.lines.includes("19.31 PPG × 17.00 games × 1.00 role = 328.3"));
assert.ok(baselineExplanation.lines.includes("Availability estimate: 16.0 active games (modeled separately; not deducted from this baseline)"));
const rookieExplanation = buildProjectionExplanation({
  method: "rookie_prior",
  draft_number: 25,
  rookie_prior_points: 197,
  role_factor: 0.78,
  regressed_ppg: null,
  base_points_before_injury: 153.66,
  baseline_games: 17,
  expected_games_after_injury: 17,
  final_points: 153.66,
  availability_adjustment_applied_to_baseline: false,
});
assert.ok(rookieExplanation.lines.includes("NFL draft selection: #25"));
assert.ok(rookieExplanation.lines.includes("Position + draft-capital prior: 197.0 points"));
assert.ok(rookieExplanation.lines.includes("197.0 prior × 0.78 depth-chart role = 153.7"));
assert.equal(rookieExplanation.lines.some((line) => line.includes("0.00 PPG")), false);
const validBestBallRoster = [
  ...Array.from({ length: 3 }, (_, index) => ({ playerId: index + 1, position: "QB", team: index ? "BUF" : "NE" })),
  ...Array.from({ length: 6 }, (_, index) => ({ playerId: index + 10, position: "RB", team: "ATL" })),
  ...Array.from({ length: 8 }, (_, index) => ({ playerId: index + 20, position: "WR", team: "LAR" })),
  ...Array.from({ length: 3 }, (_, index) => ({ playerId: index + 30, position: "TE", team: "KC" })),
];
assert.equal(getBestBallRosterStatus(validBestBallRoster).valid, true);
assert.equal(getBestBallRosterStatus(validBestBallRoster.slice(0, 19)).valid, false);
assert.equal(canAddBestBallPlayer(validBestBallRoster, { playerId: 99, position: "WR", team: "SEA" }), false);
assert.equal(canAddBestBallPlayer([], { playerId: 100, position: "K", team: "DAL" }), false);
const bestBallSlots = buildSnakeSlots(12, 20);
assert.equal(bestBallSlots.length, 240);
assert.deepEqual(bestBallSlots.slice(0, 14).map((slot) => slot.teamSlot), [1,2,3,4,5,6,7,8,9,10,11,12,12,11]);
assert.deepEqual(bestBallSlots.filter((slot) => slot.teamSlot === 1).slice(0, 4).map((slot) => slot.overallPick), [1,24,25,48]);
const draftBoard = buildBestBallDraftBoard(Array.from({ length: 14 }, (_, index) => 1001 + index));
assert.equal(draftBoard.length, 20);
assert.equal(draftBoard[0].length, 12);
assert.deepEqual(draftBoard[0][0], { round: 1, pickInRound: 1, teamSlot: 1, overallPick: 1, playerId: 1001 });
assert.deepEqual(draftBoard[1][11], { round: 2, pickInRound: 1, teamSlot: 12, overallPick: 13, playerId: 1013 });
assert.deepEqual(draftBoard[1][10], { round: 2, pickInRound: 2, teamSlot: 11, overallPick: 14, playerId: 1014 });
assert.equal(draftBoard[1][9].playerId, null);
assert.deepEqual(parseBestBallDraftState('{"userSlot":7,"playerIds":[4,5,5,6]}'), { userSlot: 7, playerIds: [4,5,6], cpuEnabled: false });
assert.deepEqual(parseBestBallDraftState('bad-json'), { userSlot: 1, playerIds: [], cpuEnabled: false });
// cpuEnabled round-trips, and non-boolean values fall back to off rather than crashing.
assert.deepEqual(parseBestBallDraftState('{"userSlot":2,"playerIds":[],"cpuEnabled":true}'), { userSlot: 2, playerIds: [], cpuEnabled: true });
assert.deepEqual(parseBestBallDraftState('{"userSlot":2,"playerIds":[],"cpuEnabled":1}'), { userSlot: 2, playerIds: [], cpuEnabled: false });

// DraftKings scoring includes full PPR and each yardage bonus independently.
assert.equal(scoreDraftKingsBestBallLine({
  passingYards: 300, passingTouchdowns: 2, interceptions: 1,
  rushingYards: 100, rushingTouchdowns: 1,
  receptions: 5, receivingYards: 100, receivingTouchdowns: 1,
  returnTouchdowns: 1, fumblesLost: 1, twoPointConversions: 1,
}), 72);
assert.ok(Math.abs(scoreDraftKingsBestBallLine({ passingYards: 299, rushingYards: 99, receivingYards: 99 }) - 31.76) < 1e-9);

const lineupPlayers: ShadowBestBallPlayer[] = [
  { playerId: 201, name: "QB", position: "QB", team: "A", byeWeek: null, projectedPoints: 300, projectionLow: 280, projectionHigh: 320, expectedGames: 17, confidence: 0.9 },
  ...Array.from({ length: 3 }, (_, index) => ({ playerId: 210 + index, name: `RB${index}`, position: "RB", team: "B", byeWeek: null, projectedPoints: 220, projectionLow: 190, projectionHigh: 250, expectedGames: 17, confidence: 0.8 })),
  ...Array.from({ length: 4 }, (_, index) => ({ playerId: 220 + index, name: `WR${index}`, position: "WR", team: "C", byeWeek: null, projectedPoints: 220, projectionLow: 190, projectionHigh: 250, expectedGames: 17, confidence: 0.8 })),
  ...Array.from({ length: 2 }, (_, index) => ({ playerId: 230 + index, name: `TE${index}`, position: "TE", team: "D", byeWeek: null, projectedPoints: 180, projectionLow: 150, projectionHigh: 210, expectedGames: 17, confidence: 0.8 })),
];
const lineupScores = new Map(lineupPlayers.map((player, index) => [player.playerId, 30 - index]));
const selectedLineup = selectBestBallLineup(lineupPlayers, lineupScores);
assert.equal(selectedLineup.countedPlayerIds.length, 8);
assert.ok(selectedLineup.countedPlayerIds.includes(212), "the third RB should win FLEX over lower-scoring WR/TE reserves");
assert.ok(!selectedLineup.countedPlayerIds.includes(223), "the fourth WR should remain on the bench");

const shadowCandidate = { playerId: 299, name: "Candidate", position: "WR", team: "E", byeWeek: 8, projectedPoints: 250, projectionLow: 220, projectionHigh: 280, expectedGames: 16, confidence: 0.75, ourRank: 24, yahooXRank: 41.5, yahooAdp: 38.2 } satisfies ShadowBestBallPlayer;
const shadow = simulateShadowBestBallCandidates({ roster: lineupPlayers, candidates: [shadowCandidate], iterations: 80 });
assert.equal(shadow.model, "shadow-v0-v1.6-points");
assert.equal(shadow.iterations, 80);
assert.equal(shadow.candidates.length, 1);
assert.ok(shadow.candidates[0].marginalCountedPoints >= 0);
assert.ok(shadow.candidates[0].expectedCountedWeeks >= 0 && shadow.candidates[0].expectedCountedWeeks <= 17);
assert.equal(shadow.candidates[0].yahooRankGap, 17.5);
assert.equal(shadow.candidates[0].yahooMarketSignal, "major-discount");
assert.deepEqual(getYahooMarketSignal(20, 26), { gap: 6, signal: "discount" });
assert.deepEqual(getYahooMarketSignal(20, 17), { gap: -3, signal: "fair" });
assert.deepEqual(getYahooMarketSignal(20, 12), { gap: -8, signal: "premium" });
assert.deepEqual(getYahooMarketSignal(null, 12), { gap: null, signal: "unavailable" });

function advisorRow(overrides: Partial<FantasyRankingRow> & Pick<FantasyRankingRow, "playerId" | "name" | "position">): FantasyRankingRow {
  return {
    team: null, rookie: false, byeWeek: null, injuryStatus: null, ecr: null, positionRank: null,
    ourRank: null, tier: null, adp: null, adpStdev: null, adpHigh: null, adpLow: null, adpSampleSize: null,
    projectionLow: null, projectionHigh: null, rankMin: null, rankMax: null, rankStd: null,
    dkBestBallAdp: null, dkBestBallRank: null, dkBestBallDraftPct: null, dkBestBallDraftGroupId: null,
    dkBestBallCapturedAt: null,
    yahooXRank: null, yahooAdp: null, yahooSourceOrder: null, yahooCapturedAt: null,
    projectedPoints: null, fantasyProsProjectedPoints: null,
    fantasyProsProjectionFetchedAt: null, fantasyProsProjectionUpdatedAt: null, ourProjectedPoints: null,
    games2025: null, fantasyPoints2025: null, positionFinish2025: null,
    positionFinishTieCount2025: null, projectionDetails: null, expectedGames: null, confidence: null,
    indicators: [], ...overrides,
  };
}
const advisorRows = [
  advisorRow({ playerId: 1, name: "Team One Pick", position: "WR", team: "BUF", byeWeek: 7, ourRank: 1, positionRank: 1, adp: 1, ourProjectedPoints: 300, fantasyProsProjectedPoints: 298, games2025: 17, fantasyPoints2025: 290, confidence: 0.9 }),
  advisorRow({ playerId: 2, name: "My First Pick", position: "QB", team: "BAL", byeWeek: 8, ourRank: 2, positionRank: 1, adp: 2, ourProjectedPoints: 350, fantasyProsProjectedPoints: 340, games2025: 17, fantasyPoints2025: 345, confidence: 0.9 }),
  advisorRow({ playerId: 3, name: "Available Runner", position: "RB", team: "ATL", byeWeek: 5, ourRank: 3, positionRank: 1, adp: 4, adpStdev: 1.5, adpSampleSize: 40, ourProjectedPoints: 280, fantasyProsProjectedPoints: 275, games2025: 17, fantasyPoints2025: 270, confidence: 0.8 }),
  advisorRow({ playerId: 4, name: "Available Receiver", position: "WR", team: "LAR", byeWeek: 9, ourRank: 4, positionRank: 2, adp: 5, ourProjectedPoints: 270, fantasyProsProjectedPoints: 268, games2025: 16, fantasyPoints2025: 260, confidence: 0.8 }),
  advisorRow({ playerId: 5, name: "Available Tight End", position: "TE", team: "KC", byeWeek: 10, ourRank: 5, positionRank: 1, adp: 6, ourProjectedPoints: 240, fantasyProsProjectedPoints: 235, games2025: 17, fantasyPoints2025: 230, confidence: 0.75 }),
];
const advisorSnapshot = buildBestBallAdvisorSnapshot(advisorRows, { rankingSetId: 42, userSlot: 2, playerIds: [1, 2] });
assert.equal(advisorSnapshot.projectionModel, "ff-independent-v1.6");
assert.equal(advisorSnapshot.draft.currentOverallPick, 3);
assert.equal(advisorSnapshot.draft.targetOverallPick, 23);
assert.equal(advisorSnapshot.draft.picksUntilUser, 20);
assert.deepEqual(advisorSnapshot.userRoster.map((player) => player.playerId), [2]);
assert.deepEqual(advisorSnapshot.candidates.map((player) => player.playerId), [3, 4, 5]);
assert.deepEqual(advisorSnapshot.candidates.map((player) => player.candidateKey), ["C01", "C02", "C03"]);
// Candidate 3 (adp=4, well ahead of the target pick 23) has real FFC
// variance data -> a computed, near-zero survival probability. Candidates
// 4/5 have no adpStdev in this fixture -> the field stays honestly null
// rather than guessing.
assert.equal(advisorSnapshot.candidates[0].availabilityAtNextPickPct, 0);
assert.equal(advisorSnapshot.candidates[0].availabilitySampleSize, 40);
assert.equal(advisorSnapshot.candidates[1].availabilityAtNextPickPct, null);
assert.equal(advisorSnapshot.candidates[2].availabilityAtNextPickPct, null);
const providerSnapshotJson = JSON.stringify(buildBestBallAdvisorProviderSnapshot(advisorSnapshot));
assert.doesNotMatch(providerSnapshotJson, /"playerIds?"/);
assert.match(providerSnapshotJson, /"candidateKey":"C01"/);
assert.deepEqual(advisorSnapshot.userByeWeeks.QB, [8]);
const advisorOutput = validateBestBallAdvisorOutput({
  recommendedCandidateKey: " c01 ",
  confidence: 0.82,
  whyNow: "Best combination of projection and availability.",
  rosterFit: "Adds the first running back without duplicating the quarterback bye.",
  evidence: ["V1.6 projects 280 points.", "ADP is 4."],
  risks: "Role uncertainty remains.",
  alternatives: [{ candidateKey: "C02", reason: "Receiver value." }, { candidateKey: "C03", reason: "Tight-end value." }],
  strategyUntilNextTurn: "Watch the running-back tier.",
  whatWouldChange: "A confirmed role change.",
}, advisorSnapshot);
assert.equal(advisorOutput.recommendedPlayerId, 3);
assert.equal(advisorOutput.confidence, 82);
assert.deepEqual(advisorOutput.risks, ["Role uncertainty remains."]);
assert.equal(enrichBestBallAdvisorResult(advisorOutput, advisorSnapshot).recommendation.name, "Available Runner");
assert.equal(validateBestBallAdvisorOutput({
  recommendation: {
    candidate_key: "Available Runner",
    confidence: 0.82,
    why_now: advisorOutput.whyNow,
    roster_fit: advisorOutput.rosterFit,
    evidence: advisorOutput.evidence,
    risk: advisorOutput.risks[0],
    alternatives: [{ candidate_key: "candidate C2", reason: "Receiver value." }, { player_name: "Available Tight End", reason: "Tight-end value." }],
    strategy_until_next_turn: advisorOutput.strategyUntilNextTurn,
    what_would_change: advisorOutput.whatWouldChange,
  },
}, advisorSnapshot).recommendedPlayerId, 3);
assert.equal(validateBestBallAdvisorOutput({
  advisablePicks: [{ player: "Available Receiver", reason: "Strong value." }, { player: "Available Tight End", reason: "Position value." }],
  winnerPick: { player: "Available Runner", explanation: "Best live-board fit." },
}, advisorSnapshot).recommendedPlayerId, 3);
const looseAdvisorOutput = validateBestBallAdvisorOutput({
  selection: { primary: "Available Runner" },
  explanation: "Best available player for this pick.",
  strategy: "Reassess after the next pick.",
  caveats: "Role may change.",
}, advisorSnapshot);
assert.equal(looseAdvisorOutput.recommendedPlayerId, 3);
assert.equal(looseAdvisorOutput.confidenceProvided, false);
assert.equal(looseAdvisorOutput.alternatives.length, 2);
assert.equal(validateBestBallAdvisorOutput({
  recommendedPicks: [
    { player_name: "Available Runner", rationale: "Best available player." },
    { player_name: "Available Receiver" },
    { player_name: "Available Tight End" },
  ],
}, advisorSnapshot).recommendedPlayerId, 3);
assert.throws(() => validateBestBallAdvisorOutput({ recommendedCandidateKey: "C99" }, advisorSnapshot), /no longer legal or available/);
async function testAdvisorCorrection() {
  let advisorAttempts = 0;
  const correctedAdvisor = await getValidatedBestBallAdvisorOutput(advisorSnapshot, async (correction) => {
    advisorAttempts += 1;
    if (!correction) return { recommendedCandidateKey: "C99" };
    assert.match(correction.validationError, /no longer legal or available/);
    return {
      recommendedCandidateKey: "C01", confidence: 80, whyNow: "Corrected legal choice.",
      rosterFit: "Adds a running back.", evidence: ["V1.6 points", "ADP"], risks: ["Role risk"],
      alternatives: [{ candidateKey: "C02", reason: "Receiver." }, { candidateKey: "C03", reason: "Tight end." }],
      strategyUntilNextTurn: "Watch tiers.", whatWouldChange: "New role data.",
    };
  });
  assert.equal(advisorAttempts, 2);
  assert.equal(correctedAdvisor.retried, true);
  assert.equal(correctedAdvisor.output.recommendedPlayerId, 3);
  await assert.rejects(
    () => getValidatedBestBallAdvisorOutput(advisorSnapshot, async () => ({ recommendedCandidateKey: "C99" })),
    /rechecked the live board.*no longer legal or available/,
  );
}
assert.notEqual(
  bestBallAdvisorDraftSignature({ rankingSetId: 42, userSlot: 2, playerIds: [1, 2] }),
  bestBallAdvisorDraftSignature({ rankingSetId: 42, userSlot: 2, playerIds: [1, 2, 3] }),
);
const correlationRankings = [
  { playerId: 1, name: "Josh Allen", position: "QB", team: "BUF" },
  { playerId: 2, name: "Keon Coleman", position: "WR", team: "BUF" },
  { playerId: 3, name: "Khalil Shakir", position: "WR", team: "BUF" },
  { playerId: 4, name: "Bijan Robinson", position: "RB", team: "ATL" },
] as FantasyRankingRow[];
const correlationRows: TeammateCorrelationRow[] = [
  { playerAId: 1, playerBId: 2, relationshipType: "QB_WR", sampleWeeks: 13, shrunkCorrelation: 0.38 },
  { playerAId: 1, playerBId: 3, relationshipType: "QB_WR", sampleWeeks: 13, shrunkCorrelation: 0.05 },
];
const rosterBadges = buildRosterCorrelationBadges(correlationRankings, [1], correlationRows, new Map(correlationRankings.map((player) => [player.playerId, player.name])));
assert.equal(rosterBadges.size, 1);
assert.equal(rosterBadges.get(2)?.code, "TEAMMATE_STACK");
assert.equal(rosterBadges.get(2)?.label, "+0.38 w/ Allen");
assert.equal(rosterBadges.has(3), false); // 0.05 is below the 0.15 threshold
assert.equal(rosterBadges.has(1), false); // never badges a player already on the roster
assert.equal(buildRosterCorrelationBadges(correlationRankings, [], correlationRows, new Map()).size, 0);
const negativeBadges = buildRosterCorrelationBadges(
  correlationRankings,
  [4],
  [{ playerAId: 2, playerBId: 4, relationshipType: "RB_WR", sampleWeeks: 8, shrunkCorrelation: -0.22 }],
  new Map(correlationRankings.map((player) => [player.playerId, player.name])),
);
assert.equal(negativeBadges.get(2)?.code, "TEAMMATE_OFFSET");
assert.equal(negativeBadges.get(2)?.label, "-0.22 w/ Robinson");

const advisorActionSource = readFileSync(new URL("../src/app/fantasy-football/best-ball/advisor-actions.ts", import.meta.url), "utf8");
assert.doesNotMatch(advisorActionSource, /NEXT_PUBLIC_(OPENAI|DEEPSEEK)/);
const advisorEnvSource = readFileSync(new URL("../src/lib/fantasy-football/ai-draft-advisor-env.ts", import.meta.url), "utf8");
assert.match(advisorEnvSource, /OPENAI_API_KEY/);
assert.match(advisorEnvSource, /OPENAI_API/);
void testAdvisorCorrection().then(() => console.log("fantasy-football tests passed")).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
